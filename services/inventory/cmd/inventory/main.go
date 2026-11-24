// Command inventory 是 inventory 服务的入口。
package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/palemoky/oppenheimer/api/eventx"
	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/cache"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/fault"
	"github.com/palemoky/oppenheimer/pkg/grpcx"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/lock"
	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/pkg/redisx"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/pkg/worker"
	"github.com/palemoky/oppenheimer/services/inventory"
	"github.com/palemoky/oppenheimer/services/inventory/internal/apply"
	"github.com/palemoky/oppenheimer/services/inventory/internal/reconcile"
	"github.com/palemoky/oppenheimer/services/inventory/internal/service"
	"github.com/palemoky/oppenheimer/services/inventory/internal/store"
)

const serviceName = "inventory"

// kafkaSetupTimeout 是启动时连接 Kafka、建 topic 的时限。broker 不可达时尽快失败退出，
// 交给编排系统重启，而不是无限期卡在启动阶段。
const kafkaSetupTimeout = 30 * time.Second

// redisSetupTimeout 是启动时探测 Redis 的时限。到时还连不上也照样启动（redisx.OpenLazy），
// 它只是不让一个卡住的连接拖住启动。
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
	// cacheRedisAddr 为空表示复用 redisAddr 的客户端。
	cacheRedisAddr string
	// stockCacheTTL 为 0 表示关闭 GetStock 缓存。
	stockCacheTTL time.Duration

	reconcileInterval time.Duration
	reconcileRounds   int

	// 库存增量的应用（ADR-0018），见 apply.Config。
	applyInterval time.Duration
	applyBatch    int

	// eventConcurrency 是 order.events 的消费者每个分区的并发度，见 defaultEventConcurrency。
	eventConcurrency int

	// drainDelay 是收到退出信号后摘流的等待，见 app.Options.DrainDelay。
	drainDelay time.Duration
}

// defaultEventConcurrency 是 ORDER_EVENTS_CONCURRENCY 的默认值：order.events 的消费者在一个分区内同时处理的消息数。
//
// 原来是分区内串行。处理一条事件是一个小事务（确认或释放预占）加一次闸门调用，在集群的盘上一次提交约 40 毫秒，
// 4 个分区合计每秒只能处理约 100 条，和负载无关。3b 第 1 轮把支付出结果修通之后，每单多出一条“已支付”的事件，
// 这里立刻成了最窄的地方：积压涨到 2 万条（docs/notes/3b-rounds/round-1-replica-flush-and-payment.md 第 5 节）。
// 支付的处理器是同一类问题、同一种修法。
//
// 可以并发的依据（kafkax.ConsumerConfig.Concurrency 的条件）：消息的 key 是订单号，同一个订单的事件仍然串行；
// 处理一条事件只碰这个订单自己的预占行，对 SKU 的改动是往 stock_pending 里追加增量，不等 sku_stock 的行锁（ADR-0018）；
// 闸门那一步是 Redis 里的原子脚本。Confirm、Release 以订单号幂等，重复投递变多不影响结果。
// 不同订单同时确认、释放同一个 SKU 早就会发生（4 个分区、2 个副本，还有 gRPC 的 Release），
// 有并发测试（TestHandleOrderEventConcurrentWithOnline）。
// 8 × 4 个分区 = 最多 32 条在途，连接池默认 50 个，还要留给 Reserve。
const defaultEventConcurrency = 8

func loadConfig() (config, error) {
	cfg := config{
		httpAddr:     cmp.Or(os.Getenv("HTTP_ADDR"), ":8082"),
		grpcAddr:     cmp.Or(os.Getenv("GRPC_ADDR"), ":9082"),
		mysqlDSN:     cmp.Or(os.Getenv("MYSQL_DSN"), "root:oppenheimer@tcp(127.0.0.1:3306)/inventory_db"),
		kafkaBrokers: splitList(cmp.Or(os.Getenv("KAFKA_BROKERS"), "127.0.0.1:9092")),
		kafkaPrefix:  os.Getenv("KAFKA_PREFIX"),
		redisAddr:    cmp.Or(os.Getenv("REDIS_ADDR"), "127.0.0.1:6379"),
		redisPrefix:  os.Getenv("REDIS_PREFIX"),

		cacheRedisAddr: os.Getenv("CACHE_REDIS_ADDR"),
		stockCacheTTL:  time.Second,

		reconcileInterval: reconcile.DefaultInterval,
		reconcileRounds:   reconcile.DefaultRounds,

		applyInterval: apply.DefaultInterval,
		applyBatch:    apply.DefaultBatch,

		eventConcurrency: defaultEventConcurrency,
	}
	if v := os.Getenv("ORDER_EVENTS_CONCURRENCY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return config{}, fmt.Errorf("ORDER_EVENTS_CONCURRENCY: want a positive integer, got %q", v)
		}
		cfg.eventConcurrency = n
	}
	if v := os.Getenv("STOCK_APPLY_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return config{}, fmt.Errorf("STOCK_APPLY_INTERVAL: invalid duration %q", v)
		}
		cfg.applyInterval = d
	}
	if v := os.Getenv("STOCK_APPLY_BATCH"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > store.MaxApplyBatch {
			return config{}, fmt.Errorf("STOCK_APPLY_BATCH: want an integer in 1..%d, got %q", store.MaxApplyBatch, v)
		}
		cfg.applyBatch = n
	}
	if v := os.Getenv("STOCK_CACHE_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return config{}, fmt.Errorf("STOCK_CACHE_TTL: want a non-negative duration, got %q", v)
		}
		cfg.stockCacheTTL = d
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
	d, err := app.DrainDelayFromEnv()
	if err != nil {
		return config{}, err
	}
	cfg.drainDelay = d
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
	// 链路和指标（设计文档 1d 第 8 节）。最先装配、最后关闭：其他组件停止时产生的 span 也要发出去。
	// 没设 OTEL_EXPORTER_OTLP_ENDPOINT 时不导出，只让日志带上 trace id。
	shutdownTelemetry, err := otelx.Setup(ctx, serviceName, logger)
	if err != nil {
		return err
	}
	defer otelx.Shutdown(ctx, shutdownTelemetry, logger)

	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: cfg.mysqlDSN})
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}
	// 在 app.RunWith 返回、所有组件停止之后再关，在途请求还要用连接池。
	defer db.Close()

	if err := mysqlx.Migrate(ctx, db, inventory.Migrations, inventory.MigrationsDir); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	rctx, cancel := context.WithTimeout(ctx, redisSetupTimeout)
	// 闸门的 Redis 此刻连不上也照样启动（设计文档 1d 的 6.10）：WarmUp 返回降级错误，订单事件的消费者
	// 做完 MySQL 那一步后在闸门那一步重试到 Redis 恢复；Reserve、GetStock 不依赖它。
	// Redis 故障期间重启或发布的实例要能回来承担这些。
	rdb, err := redisx.OpenLazy(rctx, redisx.Config{Addr: cfg.redisAddr}, logger)
	cancel()
	if err != nil {
		return fmt.Errorf("open redis: %w", err)
	}
	// 和 db 一样等所有组件停止后再关：消费者处理最后几条事件、gRPC 处理在途的 WarmUp 都要用它。
	// defer 按注册的逆序执行，所以它在 db.Close 之前、leader.Close 之后执行。
	defer rdb.Close()

	st := store.New(db)
	gate := stockgate.New(rdb, cfg.redisPrefix)
	// 降级开关（设计文档 1d 的 6.9）。现在只有缓存一个，在 newStockCache 里登记。
	switches := degrade.New(serviceName, logger)
	stockCache, closeCache, err := newStockCache(ctx, logger, cfg, rdb, switches)
	if err != nil {
		return err
	}
	defer closeCache()
	// 开关都登记完了。名字写错在这里就拒绝启动：被悄悄忽略的话，人以为关了其实没关。
	if err := switches.Apply(os.Getenv(degrade.Env)); err != nil {
		return err
	}
	svc := service.New(st, gate, logger, service.WithStockCache(stockCache, cfg.stockCacheTTL))

	reconciler, leader, err := newReconciler(logger, cfg, rdb, gate, st)
	if err != nil {
		return err
	}
	defer func() {
		// 主动释放 leader 锁，让别的实例马上接手对账，而不是等锁过期。
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if err := leader.Close(ctx); err != nil {
			logger.WarnContext(ctx, "release reconcile leader lock", "error", err)
		}
	}()

	consumer, err := newOrderEventConsumer(ctx, logger, cfg, svc)
	if err != nil {
		return err
	}

	// 把库存增量合并进 sku_stock 的后台任务（ADR-0018）。每个实例各跑一个，不用 leader 锁。
	applier, err := apply.New(st, apply.Config{Interval: cfg.applyInterval, Batch: cfg.applyBatch, Logger: logger})
	if err != nil {
		return err
	}
	applyLoop, err := apply.NewLoop(applier)
	if err != nil {
		return fmt.Errorf("new stock apply loop: %w", err)
	}
	// 未应用的增量行数，由应用者自己定期数。defer 在下面的 Drain 之后执行。
	unregisterApplyMetrics, err := applier.RegisterMetrics()
	if err != nil {
		return fmt.Errorf("register stock apply metrics: %w", err)
	}
	defer unregisterApplyMetrics()

	// 先绑定两个端口，任何一个冲突都在开始服务前暴露。
	// 内部端口：存活和就绪探测、降级开关。不经过 gateway。
	readiness := &app.Readiness{}
	admin := app.AdminMux(serviceName, readiness)
	switches.Register(admin)
	fault.Register(admin)
	httpServer, err := app.ListenHTTP(ctx, "http", cfg.httpAddr, admin)
	if err != nil {
		return err
	}
	// 调试端口（pprof）：DEBUG_ADDR 没设就不开，集群里只在回环上监听，经 kubectl port-forward 访问。
	debugServer, err := app.ListenDebug(ctx, logger)
	if err != nil {
		return err
	}
	grpcServer, err := grpcx.Listen(ctx, logger, "grpc", cfg.grpcAddr, func(s *grpc.Server) {
		inventoryv1.RegisterInventoryServiceServer(s, svc)
	})
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, "listening", "http_addr", httpServer.Addr().String(), "grpc_addr", grpcServer.Addr().String())

	// app.RunWith 按相反顺序停止：
	//   0. 收到退出信号后先摘流（设计文档 1d 的 6.11）：就绪探测和 gRPC 健康检查变成不可用，继续正常服务
	//      DRAIN_DELAY 这么久，让负载均衡把流量挪走，然后才开始下面的停止。本机默认 0。
	//   1. 对账最先停。它只是周期性地修正，少跑一轮没有影响；先停掉，就不会在后面的组件
	//      收尾时（消费者最后几条事件在改 MySQL 和闸门）读到半途的状态去修正。
	//   2. 消费者其次，在途事件处理完（MySQL 和闸门两步都做完或返回错误）、位点提交后再往下走。
	//   3. 然后 gRPC、HTTP。
	//   4. 库存增量的应用最后停：gRPC 的预占、消费者的确认和释放在收尾时还在写增量。
	//      它停了之后再收一次尾（下面的 Drain），尽量不把没应用的增量留到下次启动。留下了也没有错：
	//      读库存的地方都会把它们加上，下次启动接着应用。
	//   5. 调试端口（开了的话）最后停。
	// Redis 和 MySQL 的连接在 app.RunWith 返回、所有组件都停了之后才关（见上面的 defer）。
	runErr := app.RunWith(ctx, logger, app.Options{Readiness: readiness, DrainDelay: cfg.drainDelay},
		app.WithDebug(debugServer, applyLoop, httpServer, grpcServer, consumer, reconciler)...)
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := applier.Drain(dctx); err != nil {
		logger.WarnContext(ctx, "apply remaining stock deltas on shutdown", "error", err)
	}
	return runErr
}

// newStockCache 创建 GetStock 的查询缓存；STOCK_CACHE_TTL 为 0 时返回 nil（不缓存），
// 也不登记 cache 开关（和 order 的订单缓存一样：没有缓存就没有可关的东西，DEGRADE 里写了 cache 会拒绝启动）。
// 设了 CACHE_REDIS_ADDR（且与 REDIS_ADDR 不同）时连专用的缓存 Redis，否则复用 rdb。
// 返回的 closeFn 关闭专用客户端（复用 rdb 时什么也不做），调用方在所有组件停止后调用。
func newStockCache(ctx context.Context, logger *slog.Logger, cfg config, rdb redis.UniversalClient, switches *degrade.Set) (c *cache.Cache[*inventoryv1.GetStockResponse], closeFn func(), err error) {
	closeFn = func() {}
	if cfg.stockCacheTTL <= 0 {
		logger.InfoContext(ctx, "stock query cache disabled")
		return nil, closeFn, nil
	}
	client := rdb
	if cfg.cacheRedisAddr != "" && cfg.cacheRedisAddr != cfg.redisAddr {
		rctx, cancel := context.WithTimeout(ctx, redisSetupTimeout)
		defer cancel()
		// 缓存是可选的依赖，连不上不阻止启动：查询直接回源（cache 包里的熔断器），恢复后自动用上。
		dedicated, err := redisx.OpenLazy(rctx, redisx.Config{Addr: cfg.cacheRedisAddr}, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("open cache redis: %w", err)
		}
		client = dedicated
		closeFn = func() {
			if err := dedicated.Close(); err != nil {
				logger.WarnContext(ctx, "close cache redis", "error", err)
			}
		}
	}
	c, err = service.NewStockCache(client, cfg.redisPrefix, logger, switches)
	if err != nil {
		closeFn()
		return nil, nil, err
	}
	return c, closeFn, nil
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
		// 分区内按订单号并发，见 defaultEventConcurrency。1 就是原来的分区内串行。
		Concurrency: cfg.eventConcurrency,
		// 普通错误不设重试上限，分区停在这条消息上等依赖恢复（设计文档 1d 的 6.2、6.5，ADR-0021）。
		// 处理一条事件要先改 MySQL 再改 Redis（闸门）。闸门那一步失败时 MySQL 那一步已经提交；
		// 这时放弃、把消息送进死信，闸门里的占用就没人归还或结清了，而且对账看不出来（恒等式两边都没变），
		// 只能等人发现后手工重放。以前是 60 次、约 5 分钟，Redis 坏得更久就会这样。
		// 依赖坏了，后面的每条消息都会同样失败，挪走一条没有用；让分区停住，Redis 或 MySQL 恢复后自己接着处理。
		// 两步都幂等，重试多少次都只生效一次。
		// 死信只收消息本身有问题的：handler 标成 kafkax.Permanent 的（解不开、没有订单号、订单和预占的状态矛盾）。
		// 代价：一条 handler 没识别出来的坏消息会一直挡住它的分区，要靠消费积压的告警发现（阶段 3）。
		// RetryMax 保持 5 秒：恢复后最多 5 秒接着处理，故障期间每个分区每 5 秒才试一次，不给依赖添压力。
		MaxAttempts: math.MaxInt,
		RetryMax:    5 * time.Second,
	}, svc.OrderEventHandler(service.DefaultEventTimeout))
	if err != nil {
		return nil, fmt.Errorf("new order event consumer: %w", err)
	}
	return consumer, nil
}
