// Command order 是 order 服务的入口。
package main

import (
	"cmp"
	"context"
	"errors"
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
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/app"
	"github.com/palemoky/Oppenheimer/pkg/cache"
	"github.com/palemoky/Oppenheimer/pkg/grpcx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/lock"
	"github.com/palemoky/Oppenheimer/pkg/logx"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/pkg/outbox"
	"github.com/palemoky/Oppenheimer/pkg/redisx"
	"github.com/palemoky/Oppenheimer/pkg/stockgate"
	"github.com/palemoky/Oppenheimer/services/order"
	"github.com/palemoky/Oppenheimer/services/order/internal/service"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

const serviceName = "order"

// kafkaStartTimeout 是启动时连接 Kafka、建 topic 的时限。
const kafkaStartTimeout = 30 * time.Second

// redisStartTimeout 是启动时连接 Redis 的时限。
const redisStartTimeout = 5 * time.Second

// closeDelayMaxAttempts 是延迟关单消息的最大处理次数。到期消息到达时订单按本机时钟还没过期
// （与发送方的时钟有偏差）会返回错误重试；默认的 5 次退避合计不到 2 秒，容不下秒级的时钟偏差。
// 10 次在 RetryMax 5 秒下合计约 20 秒，再长就交给 sweeper。
const closeDelayMaxAttempts = 10

// 周期任务的 leader 锁（设计文档 7.1）。持有者崩溃后锁要等 TTL 过期才能被别的副本接手，
// 这段时间任务没人跑：中继的空窗直接表现为事件延迟，取得短；其余任务本来就是低频兜底，取 15 秒。
// 正常退出时主动释放，没有空窗。
const (
	relayLockTTL = 5 * time.Second
	taskLockTTL  = 15 * time.Second

	relayLockName  = "order:outbox-relay"
	sweeperLock    = "order:sweeper"
	holdReaperLock = "order:hold-reaper"
)

// leaderReleaseTimeout 是退出时主动释放一把 leader 锁的时限。
const leaderReleaseTimeout = 2 * time.Second

type config struct {
	httpAddr       string
	grpcAddr       string
	mysqlDSN       string
	workerID       int64
	inventoryAddr  string
	paymentTimeout time.Duration
	idempotencyTTL time.Duration
	sweeper        service.SweeperConfig
	reaper         service.ReaperConfig
	kafkaBrokers   []string
	kafkaPrefix    string
	outboxInterval time.Duration
	redisAddr      string
	redisPrefix    string
	// cacheRedisAddr 是查询缓存的 Redis，不设时与 redisAddr 相同（复用同一个客户端）。
	cacheRedisAddr string
	// orderCacheTTL 为 0 表示关闭订单查询缓存。
	orderCacheTTL     time.Duration
	orderCacheNullTTL time.Duration
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
		httpAddr:      cmp.Or(os.Getenv("HTTP_ADDR"), ":8081"),
		grpcAddr:      cmp.Or(os.Getenv("GRPC_ADDR"), ":9081"),
		mysqlDSN:      cmp.Or(os.Getenv("MYSQL_DSN"), "root:oppenheimer@tcp(127.0.0.1:3306)/order_db"),
		inventoryAddr: cmp.Or(os.Getenv("INVENTORY_ADDR"), "127.0.0.1:9082"),
		kafkaPrefix:   os.Getenv("KAFKA_PREFIX"),
		redisAddr:     cmp.Or(os.Getenv("REDIS_ADDR"), "127.0.0.1:6379"),
		redisPrefix:   os.Getenv("REDIS_PREFIX"),
	}
	for b := range strings.SplitSeq(cmp.Or(os.Getenv("KAFKA_BROKERS"), "127.0.0.1:9092"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.kafkaBrokers = append(cfg.kafkaBrokers, b)
		}
	}
	if len(cfg.kafkaBrokers) == 0 {
		return cfg, errors.New("KAFKA_BROKERS has no broker addresses")
	}
	var err error
	if cfg.outboxInterval, err = envDuration("OUTBOX_INTERVAL", 100*time.Millisecond); err != nil {
		return cfg, err
	}
	if cfg.workerID, err = envInt("WORKER_ID", 0); err != nil {
		return cfg, err
	}
	if cfg.workerID < 0 || cfg.workerID > idgen.MaxWorkerID {
		return cfg, fmt.Errorf("WORKER_ID must be in [0, %d]", idgen.MaxWorkerID)
	}
	if cfg.paymentTimeout, err = envDuration("PAYMENT_TIMEOUT", service.DefaultPaymentTimeout); err != nil {
		return cfg, err
	}
	if cfg.sweeper.Interval, err = envDuration("SWEEP_INTERVAL", service.DefaultSweepInterval); err != nil {
		return cfg, err
	}
	batch, err := envInt("SWEEP_BATCH_SIZE", service.DefaultSweepBatchSize)
	if err != nil {
		return cfg, err
	}
	if batch <= 0 {
		return cfg, errors.New("SWEEP_BATCH_SIZE must be positive")
	}
	cfg.sweeper.BatchSize = int(batch)
	if cfg.sweeper.CreateTimeout, err = envDuration("CREATE_TIMEOUT", service.DefaultCreateTimeout); err != nil {
		return cfg, err
	}
	// 创建时限必须给落库时的一次推进（处理一条消息的时限内含 Reserve）留出余地，
	// 否则后台任务会和正常落库抢着处理。
	if cfg.sweeper.CreateTimeout <= service.DefaultEventTimeout {
		return cfg, fmt.Errorf("CREATE_TIMEOUT must be longer than %s", service.DefaultEventTimeout)
	}
	if cfg.idempotencyTTL, err = envDuration("IDEMPOTENCY_TTL", service.DefaultIdempotencyTTL); err != nil {
		return cfg, err
	}
	if cfg.reaper.Grace, err = envDuration("HOLD_REAP_GRACE", service.DefaultReapGrace); err != nil {
		return cfg, err
	}
	if cfg.reaper.Interval, err = envDuration("HOLD_REAP_INTERVAL", service.DefaultReapInterval); err != nil {
		return cfg, err
	}
	cfg.cacheRedisAddr = cmp.Or(os.Getenv("CACHE_REDIS_ADDR"), cfg.redisAddr)
	if cfg.orderCacheTTL, err = envDurationOrZero("ORDER_CACHE_TTL", service.DefaultOrderCacheTTL); err != nil {
		return cfg, err
	}
	if cfg.orderCacheNullTTL, err = envDuration("ORDER_CACHE_NULL_TTL", service.DefaultOrderCacheNullTTL); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func envInt(key string, def int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return d, nil
}

// envDurationOrZero 与 envDuration 相同，但允许显式设为 0（表示关闭某个功能）。
func envDurationOrZero(key string, def time.Duration) (time.Duration, error) {
	if os.Getenv(key) == "0" {
		return 0, nil
	}
	return envDuration(key, def)
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: cfg.mysqlDSN})
	if err != nil {
		return err
	}
	defer db.Close()
	if err := mysqlx.Migrate(ctx, db, order.Migrations, order.MigrationsDir); err != nil {
		return err
	}

	ids, err := idgen.New(cfg.workerID)
	if err != nil {
		return err
	}

	conn, err := grpcx.Dial(cfg.inventoryAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	rctx, rcancel := context.WithTimeout(ctx, redisStartTimeout)
	defer rcancel()
	rdb, err := redisx.Open(rctx, redisx.Config{Addr: cfg.redisAddr})
	if err != nil {
		return err
	}
	// defer 在 app.Run 返回、所有组件都停下之后执行：受理、落库、回收都用它。
	defer rdb.Close()
	gate := stockgate.New(rdb, cfg.redisPrefix)

	orderCache, closeCache, err := openOrderCache(ctx, logger, cfg, rdb)
	if err != nil {
		return err
	}
	// 和 rdb 一样在 app.Run 返回、gRPC 停下之后关闭。
	defer closeCache()

	// 锁放在闸门那个实例上（noeviction），不放缓存实例：锁的 key 被淘汰等于凭空释放。
	relayLeader, taskLockers, err := newLockers(logger, cfg, rdb)
	if err != nil {
		return err
	}
	sweeperLeader := taskLockers.Leader(sweeperLock)
	reaperLeader := taskLockers.Leader(holdReaperLock)
	// 主动释放 leader 锁，让别的副本马上接手，而不是等锁过期。defer 在 app.Run 返回、
	// 所有 Loop 都停下之后执行（停下之前释放，Loop 的下一轮 Allow 会把锁重新抢回来），
	// 并且在 rdb.Close 之前（defer 后进先出）。
	defer releaseLeaders(ctx, logger, map[string]*lock.Leader{
		relayLockName:  relayLeader,
		sweeperLock:    sweeperLeader,
		holdReaperLock: reaperLeader,
	})

	kcfg := kafkax.Config{Brokers: cfg.kafkaBrokers, ClientID: serviceName}
	ordersTopic := eventx.Topic(cfg.kafkaPrefix, eventx.OrderEvents)
	paymentsTopic := eventx.Topic(cfg.kafkaPrefix, eventx.PaymentEvents)
	submitTopic := eventx.Topic(cfg.kafkaPrefix, eventx.OrderSubmit)
	closeTopic := eventx.Topic(cfg.kafkaPrefix, eventx.OrderCloseDelay)
	// 启动阶段连 Kafka（探活、建 topic、检查死信 topic）的总时限。这些 ctx 只在构造时使用。
	kctx, kcancel := context.WithTimeout(ctx, kafkaStartTimeout)
	defer kcancel()
	if err := ensureTopics(kctx, kcfg, ordersTopic, paymentsTopic, submitTopic, closeTopic); err != nil {
		return err
	}

	producer, err := kafkax.NewProducer(kctx, kcfg)
	if err != nil {
		return err
	}
	// 最后关闭：中继停下之后才不再发送。defer 按后进先出执行，它在 app.Run 返回后运行。
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
		return err
	}

	svc := service.New(service.Deps{
		Store:      store.New(db),
		Gate:       gate,
		Sender:     producer,
		Inventory:  inventoryv1.NewInventoryServiceClient(conn),
		IDs:        ids,
		Logger:     logger,
		OrderCache: orderCache,
	}, service.Config{
		PaymentTimeout:    cfg.paymentTimeout,
		IdempotencyTTL:    cfg.idempotencyTTL,
		OrderEventsTopic:  ordersTopic,
		SubmitTopic:       submitTopic,
		CloseDelayTopic:   closeTopic,
		Relay:             relay,
		OrderCacheTTL:     cfg.orderCacheTTL,
		OrderCacheNullTTL: cfg.orderCacheNullTTL,
	})
	cfg.sweeper.Guard = lock.FailOpen(sweeperLeader, logger)
	cfg.reaper.Guard = lock.FailOpen(reaperLeader, logger)
	sweeper, err := service.NewSweeper(svc, cfg.sweeper)
	if err != nil {
		return err
	}
	reaper, err := service.NewReaper(svc, cfg.reaper)
	if err != nil {
		return err
	}

	paymentConsumer, err := kafkax.NewConsumer(kctx, kafkax.ConsumerConfig{
		Config: kcfg,
		Group:  eventx.Group(cfg.kafkaPrefix, eventx.GroupOrderPaymentEvents),
		Topics: []string{paymentsTopic},
		Logger: logger,
	}, svc.PaymentEventsHandler(service.DefaultEventTimeout))
	if err != nil {
		return err
	}
	submitConsumer, err := kafkax.NewConsumer(kctx, kafkax.ConsumerConfig{
		Config: kcfg,
		Group:  eventx.Group(cfg.kafkaPrefix, eventx.GroupOrderSubmit),
		Topics: []string{submitTopic},
		Logger: logger,
	}, svc.SubmitHandler(service.DefaultEventTimeout))
	if err != nil {
		return err
	}
	closeConsumer, err := kafkax.NewDelayConsumer(kctx, kafkax.DelayConsumerConfig{
		Config:      kcfg,
		Group:       eventx.Group(cfg.kafkaPrefix, eventx.GroupOrderCloseDelay),
		Topics:      []string{closeTopic},
		Logger:      logger,
		MaxAttempts: closeDelayMaxAttempts,
	}, svc.CloseDueHandler(service.DefaultEventTimeout))
	if err != nil {
		return err
	}

	httpServer, err := app.ListenHTTP(ctx, "http", cfg.httpAddr, app.HealthHandler(serviceName))
	if err != nil {
		return err
	}
	grpcServer, err := grpcx.Listen(ctx, logger, "grpc", cfg.grpcAddr, func(s *grpc.Server) {
		orderv1.RegisterOrderServiceServer(s, svc)
	})
	if err != nil {
		return err
	}
	logger.Info("listening", "http_addr", httpServer.Addr().String(), "grpc_addr", grpcServer.Addr().String())

	// 停止顺序与列出的顺序相反：
	//  1. sweeper、回收任务：纯后台扫描，随时可以停，下次启动接着扫。
	//  2. gRPC：不再受理新请求，并等在途的 SubmitOrder 做完。受理是“Hold → 发消息”两步，
	//     中途被打断会留下孤儿占用（只能等回收任务），所以让它们在 Redis 和生产者都还可用时走完。
	//  3. 三个消费者：支付结果、延迟关单、落库。它们写 outbox，必须在中继之前停；
	//     没处理完的消息不提交位点，下次启动（或别的副本）重新消费。
	//  4. outbox 中继：排在所有会写 outbox 的组件之后，让最后写入的事件尽量在退出前发出；
	//     没发出的也不会丢，下次启动时中继会先跑一轮。
	//  5. 健康检查最后停。
	// 生产者、Redis、数据库和库存的连接由 defer 在 app.Run 返回后关闭，此时已没有组件在用。
	// 三把 leader 锁也在 app.Run 返回后释放（先于 Redis 连接关闭）：此时中继、sweeper、回收任务都已停下。
	return app.Run(ctx, logger, httpServer, relay,
		submitConsumer, closeConsumer, paymentConsumer, grpcServer, reaper, sweeper)
}

// openOrderCache 按配置创建订单查询缓存。ORDER_CACHE_TTL=0 时缓存为 nil（关闭）。
// 缓存实例与闸门是同一个地址时复用 rdb，不另开连接池；否则新开一个客户端，由返回的 closeFn 关闭。
func openOrderCache(ctx context.Context, logger *slog.Logger, cfg config, rdb redis.Cmdable) (
	c *cache.Cache[*orderv1.Order], closeFn func(), err error,
) {
	closeFn = func() {}
	if cfg.orderCacheTTL == 0 {
		logger.Info("order query cache disabled")
		return nil, closeFn, nil
	}
	client := rdb
	if cfg.cacheRedisAddr != cfg.redisAddr {
		rctx, cancel := context.WithTimeout(ctx, redisStartTimeout)
		defer cancel()
		cdb, err := redisx.Open(rctx, redisx.Config{Addr: cfg.cacheRedisAddr})
		if err != nil {
			return nil, closeFn, fmt.Errorf("open cache redis: %w", err)
		}
		closeFn = func() { _ = cdb.Close() }
		client = cdb
	}
	c, err = service.NewOrderCache(client, cfg.redisPrefix, logger)
	if err != nil {
		closeFn()
		return nil, func() {}, err
	}
	return c, closeFn, nil
}

// newLockers 创建两个 Locker：中继一个（TTL 短），其余周期任务共用一个。
func newLockers(logger *slog.Logger, cfg config, rdb redis.Scripter) (*lock.Leader, *lock.Locker, error) {
	relayLocker, err := lock.New(rdb, lock.Options{TTL: relayLockTTL, KeyPrefix: cfg.redisPrefix, Logger: logger})
	if err != nil {
		return nil, nil, fmt.Errorf("new relay locker: %w", err)
	}
	taskLocker, err := lock.New(rdb, lock.Options{TTL: taskLockTTL, KeyPrefix: cfg.redisPrefix, Logger: logger})
	if err != nil {
		return nil, nil, fmt.Errorf("new task locker: %w", err)
	}
	return relayLocker.Leader(relayLockName), taskLocker, nil
}

// releaseLeaders 主动释放各 leader 锁。ctx 已结束（进程在退出），释放用独立的时限。
func releaseLeaders(ctx context.Context, logger *slog.Logger, leaders map[string]*lock.Leader) {
	for name, ld := range leaders {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaderReleaseTimeout)
		if err := ld.Close(rctx); err != nil {
			logger.Warn("release leader lock failed", "lock", name, "error", err)
		}
		cancel()
	}
}

// ensureTopics 创建本服务发布和消费的 topic 及其死信 topic。
// broker 关闭了自动建 topic，消费者要求死信 topic 已存在。
func ensureTopics(ctx context.Context, kcfg kafkax.Config, topics ...string) error {
	client, err := kafkax.NewClient(ctx, kcfg)
	if err != nil {
		return err
	}
	defer client.Close()
	specs := make([]kafkax.TopicSpec, 0, 2*len(topics))
	for _, t := range topics {
		specs = append(specs,
			kafkax.TopicSpec{Name: t, Partitions: eventx.Partitions},
			kafkax.TopicSpec{Name: t + eventx.DLQSuffix, Partitions: eventx.Partitions},
		)
	}
	if err := kafkax.EnsureTopics(ctx, client, specs...); err != nil {
		return fmt.Errorf("ensure topics: %w", err)
	}
	return nil
}
