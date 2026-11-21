// Command payment 是 payment 服务的入口。
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
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/grpcx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/lock"
	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/pkg/outbox"
	"github.com/palemoky/oppenheimer/pkg/redisx"
	"github.com/palemoky/oppenheimer/services/payment"
	"github.com/palemoky/oppenheimer/services/payment/internal/consumer"
	"github.com/palemoky/oppenheimer/services/payment/internal/processor"
	"github.com/palemoky/oppenheimer/services/payment/internal/service"
	"github.com/palemoky/oppenheimer/services/payment/internal/store"
)

const serviceName = "payment"

// kafkaStartTimeout 是启动时连接 Kafka、建 topic 的时限。
const kafkaStartTimeout = 30 * time.Second

// redisSetupTimeout 是启动时探测 Redis 的时限。到时还连不上也照样启动（redisx.OpenLazy），
// 它只是不让一个卡住的连接拖住启动。
const redisSetupTimeout = 10 * time.Second

// payment 调 order（创建支付单前查订单）这一跳的时限（设计文档 1d 的 5.6）。
//
// orderHopBudget 是这一跳的预算，写在 deploy/envoy/payment.yaml 出站路由 to_order 的 timeout 上，由边车执行
// （有测试核对两处一致）。orderHopTimeout 是进程里的时限，比预算多 grpcx.SidecarMargin，只做兜底。
// 边车必须先超时，慢的 order 才会被摘除，理由见 grpcx.SidecarMargin。
const (
	orderHopBudget  = time.Second
	orderHopTimeout = orderHopBudget + grpcx.SidecarMargin
)

// 两个周期任务的 leader 锁。锁名固定，TTL 比各自的轮询间隔长：持有者崩溃后最多这么久由别的副本接手。
const (
	relayLockName      = "payment:outbox-relay"
	relayLockTTL       = 5 * time.Second
	processorLockName  = "payment:processor"
	processorLockTTL   = 15 * time.Second
	leaderCloseTimeout = 2 * time.Second
)

type config struct {
	httpAddr  string
	grpcAddr  string
	mysqlDSN  string
	workerID  int64
	orderAddr string
	// Kafka 与 outbox，见设计文档第 4 节。
	kafkaBrokers   []string
	kafkaPrefix    string
	redisAddr      string
	redisPrefix    string
	outboxInterval time.Duration
	processor      processor.Config
	// drainDelay 是收到退出信号后摘流的等待，见 app.Options.DrainDelay。
	drainDelay time.Duration
}

func main() {
	logger := logx.New(serviceName, os.Stdout, logx.LevelFromEnv())
	slog.SetDefault(logger)

	if err := run(context.Background(), logger); err != nil {
		logger.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func loadConfig() (config, error) {
	cfg := config{
		httpAddr:  cmp.Or(os.Getenv("HTTP_ADDR"), ":8083"),
		grpcAddr:  cmp.Or(os.Getenv("GRPC_ADDR"), ":9083"),
		mysqlDSN:  cmp.Or(os.Getenv("MYSQL_DSN"), "root:oppenheimer@tcp(127.0.0.1:3306)/payment_db"),
		orderAddr: cmp.Or(os.Getenv("ORDER_ADDR"), "127.0.0.1:9081"),
		kafkaBrokers: strings.FieldsFunc(cmp.Or(os.Getenv("KAFKA_BROKERS"), "127.0.0.1:9092"),
			func(r rune) bool { return r == ',' || r == ' ' }),
		kafkaPrefix: os.Getenv("KAFKA_PREFIX"),
		redisAddr:   cmp.Or(os.Getenv("REDIS_ADDR"), "127.0.0.1:6379"),
		redisPrefix: os.Getenv("REDIS_PREFIX"),
	}

	var err error
	// 没设 WORKER_ID 就从 payment 的库里租一个节点号（ADR-0028），设了就固定用它（本机、测试脚本）。
	cfg.workerID = idgen.Leased
	if v := os.Getenv("WORKER_ID"); v != "" {
		if cfg.workerID, err = strconv.ParseInt(v, 10, 64); err != nil {
			return config{}, fmt.Errorf("parse WORKER_ID: %w", err)
		}
		if cfg.workerID < 0 || cfg.workerID > idgen.MaxWorkerID {
			return config{}, fmt.Errorf("WORKER_ID must be in [0, %d], or unset to lease one", idgen.MaxWorkerID)
		}
	}
	if cfg.outboxInterval, err = durationEnv("OUTBOX_INTERVAL"); err != nil {
		return config{}, err
	}
	cfg.outboxInterval = cmp.Or(cfg.outboxInterval, 100*time.Millisecond)
	cfg.processor.TopicPrefix = cfg.kafkaPrefix
	// 以下三项设计文档的配置表没有列出，留空即用 processor 的默认值；端到端测试可借此缩短等待。
	if cfg.processor.Interval, err = durationEnv("PROCESSOR_INTERVAL"); err != nil {
		return config{}, err
	}
	if cfg.processor.ResultDelay, err = durationEnv("PAYMENT_SIMULATED_DELAY"); err != nil {
		return config{}, err
	}
	if v := os.Getenv("PROCESSOR_BATCH_SIZE"); v != "" {
		if cfg.processor.BatchSize, err = strconv.Atoi(v); err != nil || cfg.processor.BatchSize <= 0 {
			return config{}, fmt.Errorf("invalid PROCESSOR_BATCH_SIZE %q", v)
		}
	}
	if cfg.drainDelay, err = app.DrainDelayFromEnv(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func durationEnv(name string) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid %s %q", name, v)
	}
	return d, nil
}

func run(ctx context.Context, logger *slog.Logger) error {
	// 链路和指标（设计文档 1d 第 8 节）。最先装配、最后关闭：其他组件停止时产生的 span 也要发出去。
	// 没设 OTEL_EXPORTER_OTLP_ENDPOINT 时不导出，只让日志带上 trace id。
	shutdownTelemetry, err := otelx.Setup(ctx, serviceName, logger)
	if err != nil {
		return err
	}
	defer otelx.Shutdown(ctx, shutdownTelemetry, logger)

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: cfg.mysqlDSN})
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}
	defer db.Close()
	if err := mysqlx.Migrate(ctx, db, payment.Migrations, payment.MigrationsDir); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// 支付单号的节点号。租约表在 payment 自己的库里（设计文档 2c 的 5.2）。
	ids, idLease, err := idgen.Open(ctx, cfg.workerID, db, logger)
	if err != nil {
		return fmt.Errorf("open id generator: %w", err)
	}
	if idLease != nil {
		// 正常退出时由 app.Run 停（让出节点号）；这里兜的是装配中途失败的情况。Stop 可以重复调用。
		defer func() {
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()
			_ = idLease.Stop(sctx)
		}()
	}

	rctx, cancelRedis := context.WithTimeout(ctx, redisSetupTimeout)
	// Redis 此刻连不上也照样启动（设计文档 1d 的 6.10）。这里只有周期任务的 leader 锁用它，
	// 而锁都是 lock.FailOpen：Redis 不可用时每个副本都跑，正确性靠条件更新。运行中坏了能放行，
	// 启动时坏着也应该能起来（1b 留下的问题：以前这里会启动失败）。
	rdb, err := redisx.OpenLazy(rctx, redisx.Config{Addr: cfg.redisAddr}, logger)
	cancelRedis()
	if err != nil {
		return fmt.Errorf("open redis: %w", err)
	}
	// 只有锁用 Redis。defer 逆序执行：它在 leader.Close 之后关闭。
	defer rdb.Close()

	relayLeader, err := newLeader(logger, cfg, rdb, relayLockName, relayLockTTL)
	if err != nil {
		return err
	}
	procLeader, err := newLeader(logger, cfg, rdb, processorLockName, processorLockTTL)
	if err != nil {
		return err
	}
	defer func() {
		// 所有组件停止之后主动释放锁，让别的副本马上接手，而不是等锁过期。
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaderCloseTimeout)
		defer cancel()
		for name, l := range map[string]*lock.Leader{relayLockName: relayLeader, processorLockName: procLeader} {
			if err := l.Close(ctx); err != nil {
				logger.WarnContext(ctx, "release leader lock", "lock", name, "error", err)
			}
		}
	}()

	orderConn, err := grpcx.Dial(cfg.orderAddr, grpcx.WithHopTimeout(orderHopTimeout))
	if err != nil {
		return fmt.Errorf("dial order service: %w", err)
	}
	defer orderConn.Close()
	orders := orderv1.NewOrderServiceClient(orderConn)

	// 连接 Kafka（确认 broker 可达、检查死信 topic）只在启动时用这个带超时的 ctx，broker 不可达时尽快失败退出。
	startCtx, cancelStart := context.WithTimeout(ctx, kafkaStartTimeout)
	defer cancelStart()
	kafkaCfg := kafkax.Config{Brokers: cfg.kafkaBrokers, ClientID: serviceName}
	if err := ensureTopics(startCtx, kafkaCfg, cfg.kafkaPrefix); err != nil {
		return err
	}
	producer, err := kafkax.NewProducer(startCtx, kafkaCfg)
	if err != nil {
		return fmt.Errorf("create kafka producer: %w", err)
	}
	defer producer.Close()
	relay, err := outbox.NewRelay(outbox.RelayConfig{
		Name:     serviceName,
		DB:       db,
		Sender:   producer,
		Logger:   logger,
		Interval: cfg.outboxInterval,
		Guard:    lock.FailOpen(relayLeader, logger),
	})
	if err != nil {
		return fmt.Errorf("create outbox relay: %w", err)
	}

	now := func() time.Time { return time.Now().UTC() }
	st := store.New(db)
	svc := service.New(st, orders, ids, now, logger)
	cfg.processor.Now = now
	cfg.processor.Guard = lock.FailOpen(procLeader, logger)
	proc, err := processor.New(st, relay, logger, cfg.processor)
	if err != nil {
		return err
	}
	orderEvents, err := kafkax.NewConsumer(startCtx, kafkax.ConsumerConfig{
		Config: kafkaCfg,
		Group:  eventx.Group(cfg.kafkaPrefix, eventx.GroupPaymentOrderEvents),
		Topics: []string{eventx.Topic(cfg.kafkaPrefix, eventx.OrderEvents)},
		Logger: logger,
		// 普通错误不设重试上限，分区停在这条消息上等依赖恢复（设计文档 1d 的 6.2、6.5，ADR-0021）。
		// 这里处理的是订单服务对支付结果的答复：接受就标记已答复，要求退款就退款。这条答复是这笔支付
		// 得到处置的唯一途径，没有任务会重发它；默认的 5 次（合计约 1.5 秒）之后进死信，支付库抖一下
		// 就会让一笔该退的款没人退，只能等人发现后手工重放。
		// handler 返回普通错误只在支付库不可用（或超时）时；支付单还是 PENDING（ErrPaymentPending）
		// 也会自己结束：处理器会把每一笔 PENDING 的支付单做完。消息本身有问题的（解不开、没有 payment_id、
		// 支付单不存在、订单号对不上、不认识的答复）由 handler 标成 Permanent，仍然直接进死信。
		// 代价：一条 handler 没识别出来的坏消息会一直挡住它的分区，要靠消费积压的告警发现（阶段 3）。
		MaxAttempts: math.MaxInt,
	}, consumer.New(st, logger, now).Kafka(consumer.DefaultTimeout))
	if err != nil {
		return fmt.Errorf("create order events consumer: %w", err)
	}

	// 内部端口：存活和就绪探测、降级开关。不经过 gateway。
	// payment 没有可降级的功能，开关集合是空的；照样挂上 GET /degrade（返回空列表），
	// 故障注入的脚本可以对每个服务用同一种方式查询。DEGRADE 里写了任何名字都拒绝启动。
	switches := degrade.New(serviceName, logger)
	if err := switches.Apply(os.Getenv(degrade.Env)); err != nil {
		return err
	}
	readiness := &app.Readiness{}
	admin := app.AdminMux(serviceName, readiness)
	switches.Register(admin)
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
		paymentv1.RegisterPaymentServiceServer(s, svc)
	})
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, "listening", "http_addr", httpServer.Addr().String(), "grpc_addr", grpcServer.Addr().String())

	// app.RunWith 按相反顺序停止。收到退出信号后先摘流（设计文档 1d 的 6.11）：就绪探测和 gRPC 健康检查
	// 变成不可用，继续正常服务 DRAIN_DELAY 这么久，让负载均衡把流量挪走（本机默认 0）。然后
	// 先停 gRPC 不再受理新支付，再停 processor（不再产生新事件），
	// 再停消费者，然后停中继（它最后一轮会把 processor 刚写的事件发出去），健康检查最后停，
	// 调试端口（开了的话）在它之后停。
	// 节点号的租约（有的话）紧跟在健康检查之后：gRPC 停了、不再发号之后才让出。
	components := []app.Component{httpServer}
	if idLease != nil {
		components = append(components, idLease)
	}
	components = append(components, relay, orderEvents, proc, grpcServer)
	return app.RunWith(ctx, logger, app.Options{Readiness: readiness, DrainDelay: cfg.drainDelay}, app.WithDebug(debugServer, components...)...)
}

// newLeader 创建一把按 name 竞争的 leader 锁。
func newLeader(logger *slog.Logger, cfg config, rdb redis.Scripter, name string, ttl time.Duration) (*lock.Leader, error) {
	locker, err := lock.New(rdb, lock.Options{TTL: ttl, KeyPrefix: cfg.redisPrefix, Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("new locker for %s: %w", name, err)
	}
	return locker.Leader(name), nil
}

// ensureTopics 创建本服务发布和消费的 topic 及其死信 topic。
// broker 关闭了自动建 topic，消费者订阅前死信 topic 必须已存在。
func ensureTopics(ctx context.Context, kafkaCfg kafkax.Config, prefix string) error {
	client, err := kafkax.NewClient(ctx, kafkaCfg)
	if err != nil {
		return fmt.Errorf("connect kafka: %w", err)
	}
	defer client.Close()

	var specs []kafkax.TopicSpec
	for _, name := range []string{eventx.PaymentEvents, eventx.OrderEvents} {
		topic := eventx.Topic(prefix, name)
		specs = append(specs,
			kafkax.TopicSpec{Name: topic, Partitions: eventx.Partitions},
			kafkax.TopicSpec{Name: topic + eventx.DLQSuffix, Partitions: eventx.Partitions})
	}
	if err := kafkax.EnsureTopics(ctx, client, specs...); err != nil {
		return fmt.Errorf("ensure kafka topics: %w", err)
	}
	return nil
}
