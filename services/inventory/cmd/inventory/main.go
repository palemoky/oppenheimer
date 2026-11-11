// Command inventory 是 inventory 服务的入口。
package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/palemoky/Oppenheimer/api/eventx"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/Oppenheimer/pkg/app"
	"github.com/palemoky/Oppenheimer/pkg/grpcx"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/lock"
	"github.com/palemoky/Oppenheimer/pkg/logx"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/pkg/redisx"
	"github.com/palemoky/Oppenheimer/pkg/stockgate"
	"github.com/palemoky/Oppenheimer/pkg/worker"
	"github.com/palemoky/Oppenheimer/services/inventory"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/reconcile"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/service"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/store"
)

const serviceName = "inventory"

// kafkaSetupTimeout 是启动时连接 Kafka、建 topic 的时限。broker 不可达时尽快失败退出，
// 交给编排系统重启，而不是无限期卡在启动阶段。
const kafkaSetupTimeout = 30 * time.Second

// redisSetupTimeout 是启动时连接 Redis 的时限，理由同 kafkaSetupTimeout。
const redisSetupTimeout = 10 * time.Second

// reconcileLockTTL 是对账任务 leader 锁的过期时间。持有者崩溃后最多这么久由别的实例接手；
// 比对账间隔长，正常情况下一直由同一个实例持有，连续轮数的计数不会因为换人而中断。
const reconcileLockTTL = 15 * time.Second

type config struct {
	httpAddr     string
	grpcAddr     string
	mysqlDSN     string
	kafkaBrokers []string
	kafkaPrefix  string
	redisAddr    string
	redisPrefix  string

	reconcileInterval time.Duration
	reconcileRounds   int
}

func loadConfig() (config, error) {
	cfg := config{
		httpAddr:     cmp.Or(os.Getenv("HTTP_ADDR"), ":8082"),
		grpcAddr:     cmp.Or(os.Getenv("GRPC_ADDR"), ":9082"),
		mysqlDSN:     cmp.Or(os.Getenv("MYSQL_DSN"), "root:oppenheimer@tcp(127.0.0.1:3306)/inventory_db"),
		kafkaBrokers: splitList(cmp.Or(os.Getenv("KAFKA_BROKERS"), "127.0.0.1:9092")),
		kafkaPrefix:  os.Getenv("KAFKA_PREFIX"),
		redisAddr:    cmp.Or(os.Getenv("REDIS_ADDR"), "127.0.0.1:6379"),
		redisPrefix:  os.Getenv("REDIS_PREFIX"),

		reconcileInterval: reconcile.DefaultInterval,
		reconcileRounds:   reconcile.DefaultRounds,
	}
	if v := os.Getenv("GATE_RECONCILE_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return config{}, fmt.Errorf("GATE_RECONCILE_INTERVAL: invalid duration %q", v)
		}
		cfg.reconcileInterval = d
	}
	if v := os.Getenv("GATE_RECONCILE_ROUNDS"); v != "" {
		n, err := strconv.Atoi(v)
		// 少于 2 轮等于看到一次偏差就修，失去了区分瞬时偏差的意义。
		if err != nil || n < 2 {
			return config{}, fmt.Errorf("GATE_RECONCILE_ROUNDS: want an integer >= 2, got %q", v)
		}
		cfg.reconcileRounds = n
	}
	return cfg, nil
}

// splitList 按逗号切分并去掉空白和空项。
func splitList(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func main() {
	logger := logx.New(serviceName, os.Stdout, logx.LevelFromEnv())
	slog.SetDefault(logger)

	cfg, err := loadConfig()
	if err != nil {
		logger.Error("invalid config", "error", err)
		os.Exit(1)
	}
	if err := run(context.Background(), logger, cfg); err != nil {
		logger.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, cfg config) error {
	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: cfg.mysqlDSN})
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}
	// 在 app.Run 返回、所有组件停止之后再关，在途请求还要用连接池。
	defer db.Close()

	if err := mysqlx.Migrate(ctx, db, inventory.Migrations, inventory.MigrationsDir); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	rctx, cancel := context.WithTimeout(ctx, redisSetupTimeout)
	rdb, err := redisx.Open(rctx, redisx.Config{Addr: cfg.redisAddr})
	cancel()
	if err != nil {
		return fmt.Errorf("open redis: %w", err)
	}
	// 和 db 一样等所有组件停止后再关：消费者处理最后几条事件、gRPC 处理在途的 WarmUp 都要用它。
	// defer 按注册的逆序执行，所以它在 db.Close 之前、leader.Close 之后执行。
	defer rdb.Close()

	st := store.New(db)
	gate := stockgate.New(rdb, cfg.redisPrefix)
	svc := service.New(st, gate, logger)

	reconciler, leader, err := newReconciler(logger, cfg, rdb, gate, st)
	if err != nil {
		return err
	}
	defer func() {
		// 主动释放 leader 锁，让别的实例马上接手对账，而不是等锁过期。
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if err := leader.Close(ctx); err != nil {
			logger.Warn("release reconcile leader lock", "error", err)
		}
	}()

	consumer, err := newOrderEventConsumer(ctx, logger, cfg, svc)
	if err != nil {
		return err
	}

	// 先绑定两个端口，任何一个冲突都在开始服务前暴露。
	httpServer, err := app.ListenHTTP(ctx, "http", cfg.httpAddr, app.HealthHandler(serviceName))
	if err != nil {
		return err
	}
	grpcServer, err := grpcx.Listen(ctx, logger, "grpc", cfg.grpcAddr, func(s *grpc.Server) {
		inventoryv1.RegisterInventoryServiceServer(s, svc)
	})
	if err != nil {
		return err
	}
	logger.Info("listening", "http_addr", httpServer.Addr().String(), "grpc_addr", grpcServer.Addr().String())

	// app.Run 按相反顺序停止：
	//   1. 对账最先停。它只是周期性地修正，少跑一轮没有影响；先停掉，就不会在后面的组件
	//      收尾时（消费者最后几条事件在改 MySQL 和闸门）读到半途的状态去修正。
	//   2. 消费者其次，在途事件处理完（MySQL 和闸门两步都做完或返回错误）、位点提交后再往下走。
	//   3. 然后 gRPC、HTTP。
	// Redis 和 MySQL 的连接在 app.Run 返回、所有组件都停了之后才关（见上面的 defer）。
	return app.Run(ctx, logger, httpServer, grpcServer, consumer, reconciler)
}

// newReconciler 创建对账任务。多个实例只有持有 leader 锁的那个在对账，理由见 reconcile.NewLoop。
func newReconciler(logger *slog.Logger, cfg config, rdb redis.Scripter, gate *stockgate.Gate, st *store.Store) (*worker.Loop, *lock.Leader, error) {
	locker, err := lock.New(rdb, lock.Options{TTL: reconcileLockTTL, KeyPrefix: cfg.redisPrefix, Logger: logger})
	if err != nil {
		return nil, nil, fmt.Errorf("new locker: %w", err)
	}
	leader := locker.Leader("inventory:gate-reconcile")
	r := reconcile.New(gate, st, reconcile.Config{
		Interval: cfg.reconcileInterval,
		Rounds:   cfg.reconcileRounds,
		Logger:   logger,
	})
	loop, err := reconcile.NewLoop(r, leader)
	if err != nil {
		return nil, nil, fmt.Errorf("new reconcile loop: %w", err)
	}
	return loop, leader, nil
}

// newOrderEventConsumer 确保 order.events 及其死信 topic 存在，并创建库存服务的消费者。
//
// topic 由发布方（订单服务）和消费方各自声明：谁先启动都不会因为 topic 不存在而失败，
// EnsureTopics 对已存在的 topic 不做任何修改。
func newOrderEventConsumer(ctx context.Context, logger *slog.Logger, cfg config, svc *service.Service) (*kafkax.Consumer, error) {
	ctx, cancel := context.WithTimeout(ctx, kafkaSetupTimeout)
	defer cancel()

	kcfg := kafkax.Config{Brokers: cfg.kafkaBrokers, ClientID: serviceName}
	topic := eventx.Topic(cfg.kafkaPrefix, eventx.OrderEvents)

	admin, err := kafkax.NewClient(ctx, kcfg)
	if err != nil {
		return nil, fmt.Errorf("connect kafka: %w", err)
	}
	defer admin.Close()
	err = kafkax.EnsureTopics(ctx, admin,
		kafkax.TopicSpec{Name: topic, Partitions: eventx.Partitions},
		kafkax.TopicSpec{Name: topic + eventx.DLQSuffix, Partitions: eventx.Partitions},
	)
	if err != nil {
		return nil, fmt.Errorf("ensure topics: %w", err)
	}

	consumer, err := kafkax.NewConsumer(ctx, kafkax.ConsumerConfig{
		Config:    kcfg,
		Group:     eventx.Group(cfg.kafkaPrefix, eventx.GroupInventoryOrderEvents),
		Topics:    []string{topic},
		Logger:    logger,
		DLQSuffix: eventx.DLQSuffix,
		// 重试次数远大于默认的 5 次。处理一条事件要先改 MySQL 再改 Redis，Redis 短暂不可用时
		// MySQL 那一步已经提交；这时如果很快放弃、把消息送进死信，闸门里的占用就没人归还了，
		// 而且对账看不出来（恒等式两边都没变）。所以宁可让分区停住等 Redis 恢复：
		// 60 次、间隔上限 5 秒，大约能撑过 5 分钟的故障，之后才进死信等人工重放。
		MaxAttempts: 60,
		RetryMax:    5 * time.Second,
	}, svc.OrderEventHandler(service.DefaultEventTimeout))
	if err != nil {
		return nil, fmt.Errorf("new order event consumer: %w", err)
	}
	return consumer, nil
}
