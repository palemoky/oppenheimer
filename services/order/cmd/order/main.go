// Command order 是 order 服务的入口。
package main

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/palemoky/oppenheimer/api/eventx"
	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/cache"
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
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/order/internal/readmark"
	"github.com/palemoky/oppenheimer/services/order/internal/service"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
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

// defaultRedisPoolSize 是 REDIS_POOL_SIZE 的默认值：闸门 Redis 的连接池大小。
//
// 原来用 redisx 的默认值 32。受理、落库（并发度乘分区数个在途）、回收和查询共用这个池子，开环每秒 2000 个
// 下单请求时在用数一直顶在 32，每轮有几万次调用在池子上排队（设计文档 1d 的 8.18）。Redis 的连接很便宜，
// 服务端单线程处理，连接多了不会让它变慢，所以直接给够。
const defaultRedisPoolSize = 128

// defaultAdmitMaxQueue 是 ADMIT_MAX_QUEUE 的默认值：等待落库的订单数上限。
//
// 它应当约等于“落库吞吐 × 允许的等待”，允许的等待要明显短于回收宽限期（默认 1 分钟）。
// 1b 时本机实测落库约 400 单/秒（4 个分区串行处理），1 万单约排 25 秒。1d 开了分区内并发之后
// 实测是 640 到 820 单/秒（并发度 8，取决于有多少订单抢同一个 SKU，设计文档 1d 的 2.10），
// 1 万单排 12 到 16 秒，比原来更保守，所以没有改。落库吞吐再变（加分区、换机器、热门 SKU 的
// 扣减优化之后）这个值要跟着调；它是静态的，没有按实测速率自适应，理由见 1b 设计文档 6.4。
const defaultAdmitMaxQueue = 10000

// defaultAdmitMaxStale 是 ADMIT_MAX_STALE 的默认值：允许排队超时的占用个数。
// 它用来容忍零星的孤儿占用（受理方在发消息前崩溃，要等宽限期才被回收），不让一两个孤儿挡住受理；
// 真正的积压是成千上万个。
const defaultAdmitMaxStale = 100

// 周期任务的 leader 锁（设计文档 7.1）。持有者崩溃后锁要等 TTL 过期才能被别的副本接手，
// 这段时间任务没人跑：中继的空窗直接表现为事件延迟，取得短；其余任务本来就是低频兜底，取 15 秒。
// 正常退出时主动释放，没有空窗。
const (
	relayLockTTL = 5 * time.Second
	taskLockTTL  = 15 * time.Second

	// 每个库一个中继，锁名后面加库号：order:outbox-relay:0、order:outbox-relay:1。
	relayLockPrefix = "order:outbox-relay:"
	sweeperLock     = "order:sweeper"
	holdReaperLock  = "order:hold-reaper"
)

// defaultMySQLDSNs 是 MYSQL_DSNS 的默认值，第 i 个是 order_db_i。
// 两个库在两个 MySQL 实例上；它们的从库在 3312、3322，从库没有默认值（不设 MYSQL_REPLICA_DSNS 就是没有从库）。
const defaultMySQLDSNs = "root:oppenheimer@tcp(127.0.0.1:3311)/order_db_0," +
	"root:oppenheimer@tcp(127.0.0.1:3321)/order_db_1"

// order 调 inventory（Reserve、WarmUp）这一跳的时限（设计文档 1d 的 5.6）。
//
// inventoryHopBudget 是这一跳的预算，写在 deploy/envoy/order.yaml 出站路由 to_inventory 的 timeout 上，由边车执行
// （有测试核对两处一致）。取 1 秒：Reserve 是一次短事务，正常几毫秒；网关给下游的总预算是 3 秒，
// order 在 Reserve 超时之后还有时间返回明确的错误。
//
// inventoryHopTimeout 是进程里的时限，比预算多 grpcx.SidecarMargin，只做兜底（边车不在或卡住时 order 仍然放手）。
// 边车必须先超时，慢的 inventory 才会被摘除，理由见 grpcx.SidecarMargin。所以 inventory 超时时这里正常看到的是
// 边车返回的 UNAVAILABLE。
//
// 落库的消费者没有上游截止时间（它有自己的 DefaultEventTimeout），这个时限对它同样生效：
// Reserve 超时按“结果未知”原地重试。
const (
	inventoryHopBudget  = time.Second
	inventoryHopTimeout = inventoryHopBudget + grpcx.SidecarMargin
)

// archiveMaxOpenConns 是冷库的连接池上限。冷库只承接旧订单的查询，量很小；
// 本机它和 inventory、payment、商家索引共用一个 MySQL 实例（max_connections 151），不能多占。
const archiveMaxOpenConns = 10

// defaultSubmitConcurrency 是 SUBMIT_CONCURRENCY 的默认值：落库消费者在一个分区内同时处理的消息数
// （设计文档 1d 的 2.2）。
//
// 串行时落库约 400 单/秒，瓶颈是每条消息依次等两次订单库事务和一次 Reserve 的往返，MySQL 和 inventory
// 都很空闲。8 × 4 个分区 = 同时 32 单在途，低于默认的订单库连接池总和（25 × 2 个库 = 50），
// 不会让落库自己在连接池上排队。改大之前先看启动时的连接池检查（checkSubmitConcurrency）。
const defaultSubmitConcurrency = 8

// defaultMySQLMaxOpenConns 是 MYSQL_MAX_OPEN_CONNS 的默认值：每个订单库的连接池上限。
//
// 本机所有服务连同一个 MySQL，默认 max_connections 是 151。分片前 order 一个池 50，加上 inventory、
// payment 各 50，合计 150，刚好放得下。分片后 order 连两个库、每个库一个池，
// 仍用 50 就是 100，合计 200，超过上限后新连接会被拒（Too many connections）。每个库取 25，
// 两个库合计 50，与分片前持平。库分到不同实例上之后，每个池只占自己那个实例的预算，可以调大。
const defaultMySQLMaxOpenConns = 25

// relayName 是第 db 个库的 outbox 中继的名字。各库必须不同：event-id 是“中继名:outbox 行 id”，
// 行 id 是各库自己的自增列，名字相同时两个库的不同事件会有相同的 event-id，按 event-id 去重的下游会丢掉一条。
func relayName(db int) string { return serviceName + "-" + strconv.Itoa(db) }

// leaderReleaseTimeout 是退出时主动释放一把 leader 锁的时限。
const leaderReleaseTimeout = 2 * time.Second

type config struct {
	httpAddr string
	grpcAddr string
	// mysqlDSNs[i] 是第 i 个订单库（order_db_i）。
	mysqlDSNs []string
	// mysqlReplicaDSNs[i] 是 order_db_i 的从库，为空表示没有从库。
	mysqlReplicaDSNs []string
	readMode         service.ReadMode
	readPinTTL       time.Duration
	readGTIDTTL      time.Duration
	readGTIDWait     time.Duration
	// readMaxLag 是从库允许的最大延迟（ORDER_READ_MAX_LAG），从库的健康探测用它，见 service.ReplicaProber。
	readMaxLag time.Duration
	// archiveDSN 是冷库（已归档订单）的 DSN，为空表示没有冷库路径。
	archiveDSN string
	// mysqlMaxOpenConns 是每个订单库的连接池上限，预算见 defaultMySQLMaxOpenConns。
	mysqlMaxOpenConns int
	// readMaxConcurrency 是每个订单库上展示读的并发上限，见 service.DefaultReadMaxConcurrency。
	readMaxConcurrency int
	// redisPoolSize 是闸门 Redis 的连接池大小，readGateMaxConcurrency 是展示读能同时占用的连接数。
	redisPoolSize          int
	readGateMaxConcurrency int
	// archiveMaxConcurrency 是冷库读的并发上限，见 service.DefaultArchiveMaxConcurrency。
	archiveMaxConcurrency int
	// submitConcurrency 是落库消费者每个分区的并发度，见 defaultSubmitConcurrency。
	submitConcurrency int
	workerID          int64
	inventoryAddr     string
	paymentTimeout    time.Duration
	idempotencyTTL    time.Duration
	sweeper           service.SweeperConfig
	reaper            service.ReaperConfig
	backlog           stockgate.BacklogLimit
	kafkaBrokers      []string
	kafkaPrefix       string
	outboxInterval    time.Duration
	redisAddr         string
	redisPrefix       string
	// cacheRedisAddr 是查询缓存的 Redis，不设时与 redisAddr 相同（复用同一个客户端）。
	cacheRedisAddr string
	// orderCacheTTL 为 0 表示关闭订单查询缓存。
	orderCacheTTL     time.Duration
	orderCacheNullTTL time.Duration
	// drainDelay 是停止前摘流的等待（DRAIN_DELAY），见 app.Options。
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
		httpAddr:      cmp.Or(os.Getenv("HTTP_ADDR"), ":8081"),
		grpcAddr:      cmp.Or(os.Getenv("GRPC_ADDR"), ":9081"),
		inventoryAddr: cmp.Or(os.Getenv("INVENTORY_ADDR"), "127.0.0.1:9082"),
		kafkaPrefix:   os.Getenv("KAFKA_PREFIX"),
		redisAddr:     cmp.Or(os.Getenv("REDIS_ADDR"), "127.0.0.1:6379"),
		redisPrefix:   os.Getenv("REDIS_PREFIX"),
	}
	// 旧的 MYSQL_DSN 指向分片前的 order_db，不能当作任何一个分片库。还设着它说明部署配置没跟上，
	// 静默忽略会让服务连上默认的本机库，所以直接拒绝启动。
	if os.Getenv("MYSQL_DSN") != "" {
		return cfg, errors.New("MYSQL_DSN is no longer supported by order; set MYSQL_DSNS (order_db_0,order_db_1) instead")
	}
	for d := range strings.SplitSeq(cmp.Or(os.Getenv("MYSQL_DSNS"), defaultMySQLDSNs), ",") {
		cfg.mysqlDSNs = append(cfg.mysqlDSNs, strings.TrimSpace(d))
	}
	// 库数等于 DSN 的个数（ADR-0015），第 i 个是 i 号库。个数是否合法（2 的幂，不超过 slot 数）由
	// sharding.New 判断；顺序写反、个数与数据不符由启动时的 shard_meta 核对挡住。
	if slices.Contains(cfg.mysqlDSNs, "") {
		return cfg, errors.New("MYSQL_DSNS has an empty DSN")
	}
	if _, err := sharding.New(sharding.OrderLayout(len(cfg.mysqlDSNs))); err != nil {
		return cfg, fmt.Errorf("MYSQL_DSNS has %d DSNs: %w", len(cfg.mysqlDSNs), err)
	}
	if v := os.Getenv("MYSQL_REPLICA_DSNS"); v != "" {
		for d := range strings.SplitSeq(v, ",") {
			cfg.mysqlReplicaDSNs = append(cfg.mysqlReplicaDSNs, strings.TrimSpace(d))
		}
		if len(cfg.mysqlReplicaDSNs) != len(cfg.mysqlDSNs) || slices.Contains(cfg.mysqlReplicaDSNs, "") {
			return cfg, fmt.Errorf("MYSQL_REPLICA_DSNS must have one non-empty DSN per entry of MYSQL_DSNS (%d), got %d",
				len(cfg.mysqlDSNs), len(cfg.mysqlReplicaDSNs))
		}
	}
	switch m := cmp.Or(os.Getenv("ORDER_READ_MODE"), "primary"); m {
	case "primary":
		cfg.readMode = service.ReadPrimary
	case "window":
		cfg.readMode = service.ReadWindow
	case "gtid":
		cfg.readMode = service.ReadGTID
	default:
		return cfg, fmt.Errorf("ORDER_READ_MODE must be primary, window or gtid, got %q", m)
	}
	// 在这里就拒绝，而不是等 service.New：错误信息能直接指出缺的是哪个环境变量。
	if cfg.readMode != service.ReadPrimary && cfg.mysqlReplicaDSNs == nil {
		return cfg, fmt.Errorf("ORDER_READ_MODE=%s needs MYSQL_REPLICA_DSNS", cfg.readMode)
	}
	cfg.archiveDSN = strings.TrimSpace(os.Getenv("ARCHIVE_DSN"))
	maxOpen, err := envInt("MYSQL_MAX_OPEN_CONNS", defaultMySQLMaxOpenConns)
	if err != nil {
		return cfg, err
	}
	if maxOpen <= 0 {
		return cfg, errors.New("MYSQL_MAX_OPEN_CONNS must be positive")
	}
	cfg.mysqlMaxOpenConns = int(maxOpen)
	// 展示读的并发上限必须小于连接池：它的作用是给落库、支付结果这些写路径留出连接，
	// 等于或超过连接池时查询仍然能把池子占满，这个上限就形同虚设。
	readMax, err := envInt("READ_MAX_CONCURRENCY", service.DefaultReadMaxConcurrency)
	if err != nil {
		return cfg, err
	}
	if readMax < 1 || readMax >= maxOpen {
		return cfg, fmt.Errorf("READ_MAX_CONCURRENCY (%d) must be at least 1 and less than MYSQL_MAX_OPEN_CONNS (%d), "+
			"otherwise queries can take every connection and leave none for order landing", readMax, maxOpen)
	}
	cfg.readMaxConcurrency = int(readMax)
	// 闸门的 Redis 连接池和展示读能占用的份额（设计文档 1d 的 8.18）。池子是受理、落库、回收、查询共用的，
	// 查询的份额必须小于池子，否则查询仍然能把它占满。
	poolSize, err := envInt("REDIS_POOL_SIZE", defaultRedisPoolSize)
	if err != nil {
		return cfg, err
	}
	readGateMax, err := envInt("READ_GATE_MAX_CONCURRENCY", service.DefaultReadGateMaxConcurrency)
	if err != nil {
		return cfg, err
	}
	if poolSize < 2 || readGateMax < 1 || readGateMax >= poolSize {
		return cfg, fmt.Errorf("READ_GATE_MAX_CONCURRENCY (%d) must be at least 1 and less than REDIS_POOL_SIZE (%d), "+
			"otherwise queries can take every stock gate connection and stall order landing", readGateMax, poolSize)
	}
	cfg.redisPoolSize, cfg.readGateMaxConcurrency = int(poolSize), int(readGateMax)
	// 冷库上没有在线写入，上限不超过连接池即可；超过了就不起作用（多出来的查询在连接池上排队）。
	archiveMax, err := envInt("ARCHIVE_MAX_CONCURRENCY", service.DefaultArchiveMaxConcurrency)
	if err != nil {
		return cfg, err
	}
	if archiveMax < 1 || archiveMax > archiveMaxOpenConns {
		return cfg, fmt.Errorf("ARCHIVE_MAX_CONCURRENCY (%d) must be between 1 and the archive db pool size (%d)",
			archiveMax, archiveMaxOpenConns)
	}
	cfg.archiveMaxConcurrency = int(archiveMax)
	// 只校验下限。上限不在这里拒绝：它和连接池的关系取决于库数和分区数，启动时只做检查和警告
	// （checkSubmitConcurrency）。1 就是 1d 之前的分区内串行，可以用来对比或回退。
	concurrency, err := envInt("SUBMIT_CONCURRENCY", defaultSubmitConcurrency)
	if err != nil {
		return cfg, err
	}
	if concurrency < 1 {
		return cfg, errors.New("SUBMIT_CONCURRENCY must be at least 1")
	}
	cfg.submitConcurrency = int(concurrency)
	for b := range strings.SplitSeq(cmp.Or(os.Getenv("KAFKA_BROKERS"), "127.0.0.1:9092"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.kafkaBrokers = append(cfg.kafkaBrokers, b)
		}
	}
	if len(cfg.kafkaBrokers) == 0 {
		return cfg, errors.New("KAFKA_BROKERS has no broker addresses")
	}
	if cfg.outboxInterval, err = envDuration("OUTBOX_INTERVAL", 100*time.Millisecond); err != nil {
		return cfg, err
	}
	// 没设 WORKER_ID 就从第一个订单库里租一个节点号（ADR-0028）：多副本时每个进程的号不能相同，
	// 而一个 Deployment 的所有 Pod 环境变量是一样的。设了就固定用它，本机和测试脚本都是这样。
	if cfg.workerID, err = envInt("WORKER_ID", idgen.Leased); err != nil {
		return cfg, err
	}
	if cfg.workerID != idgen.Leased && (cfg.workerID < 0 || cfg.workerID > idgen.MaxWorkerID) {
		return cfg, fmt.Errorf("WORKER_ID must be in [0, %d], or unset to lease one", idgen.MaxWorkerID)
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
	// 受理端的背压，理由见 stockgate.BacklogLimit。0 表示关闭对应的一条。
	queue, err := envInt("ADMIT_MAX_QUEUE", defaultAdmitMaxQueue)
	if err != nil {
		return cfg, err
	}
	if queue < 0 {
		return cfg, errors.New("ADMIT_MAX_QUEUE must not be negative")
	}
	cfg.backlog.MaxQueue = queue
	// 排队时长默认取回收宽限期的一半：落库停住之后，受理在半个宽限期内停下。
	if cfg.backlog.MaxWait, err = envDurationOrZero("ADMIT_MAX_WAIT", cfg.reaper.Grace/2); err != nil {
		return cfg, err
	}
	if cfg.backlog.MaxWait >= cfg.reaper.Grace {
		return cfg, fmt.Errorf("ADMIT_MAX_WAIT (%s) must be shorter than HOLD_REAP_GRACE (%s), "+
			"otherwise accepted orders can be reaped while still queued", cfg.backlog.MaxWait, cfg.reaper.Grace)
	}
	stale, err := envInt("ADMIT_MAX_STALE", defaultAdmitMaxStale)
	if err != nil {
		return cfg, err
	}
	if stale < 0 {
		return cfg, errors.New("ADMIT_MAX_STALE must not be negative")
	}
	cfg.backlog.MaxStale = stale
	if cfg.readPinTTL, err = envDuration("ORDER_READ_PIN_TTL", service.DefaultReadPinTTL); err != nil {
		return cfg, err
	}
	if cfg.readGTIDTTL, err = envDuration("ORDER_READ_GTID_TTL", service.DefaultReadGTIDTTL); err != nil {
		return cfg, err
	}
	if cfg.readGTIDWait, err = envDuration("ORDER_READ_GTID_WAIT", service.DefaultReadGTIDWait); err != nil {
		return cfg, err
	}
	if cfg.readMaxLag, err = envDuration("ORDER_READ_MAX_LAG", service.DefaultReplicaMaxLag); err != nil {
		return cfg, err
	}
	// window 模式靠 pin 的时长假设从库的延迟：pin 到期之后的读交给从库。探测允许的延迟不小于 pin 的时长时，
	// 一个被判为健康的从库仍可能还没复制到 pin 已经到期的写入，读到自己刚写的数据的旧版本（设计文档 1d 的 6.7）。
	// 要把探测的间隔也算进去：延迟在两次探测之间还会继续涨，被发现时最坏已经是“最大延迟 + 一个探测间隔”。
	if worst := cfg.readMaxLag + service.DefaultReplicaProbeInterval; cfg.readMode == service.ReadWindow && worst >= cfg.readPinTTL {
		return cfg, fmt.Errorf("ORDER_READ_MAX_LAG (%s) plus the probe interval (%s) must be shorter than ORDER_READ_PIN_TTL (%s) "+
			"in window read mode, otherwise reads after the pin expires can miss the caller's own writes",
			cfg.readMaxLag, service.DefaultReplicaProbeInterval, cfg.readPinTTL)
	}
	cfg.cacheRedisAddr = cmp.Or(os.Getenv("CACHE_REDIS_ADDR"), cfg.redisAddr)
	if cfg.orderCacheTTL, err = envDurationOrZero("ORDER_CACHE_TTL", service.DefaultOrderCacheTTL); err != nil {
		return cfg, err
	}
	if cfg.orderCacheNullTTL, err = envDuration("ORDER_CACHE_NULL_TTL", service.DefaultOrderCacheNullTTL); err != nil {
		return cfg, err
	}
	if cfg.drainDelay, err = app.DrainDelayFromEnv(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// checkSubmitConcurrency 比较落库的在途上限和订单库连接池的总和，前者更大时打一条 WARN（设计文档 1d 的 2.7）。
//
// 同时在途的落库数最多是“并发度 × 分区数”，它们按订单号分散在各个订单库上；连接池的总和是
// “每个库的池 × 库数”。前者长期超过后者时，落库会在连接池上排队，加并发换不来吞吐，
// 还会和查询、中继、sweeper 抢同一个池。
//
// 只警告、不拒绝启动，因为这个比较只是粗略的上界估计：
//   - 一条落库只在两次短事务（写订单行、改成待支付）里占用连接，等 Reserve 的往返期间不占，
//     在途数超过池子不等于池子被占满；
//   - 分区数用的是 eventx.Partitions（本机建 topic 时的分区数），线上 topic 的实际分区数可能不同；
//     多个副本分摊分区时，每个进程实际持有的分区更少；
//   - 订单不一定均匀落在各个库上，均匀时每个库分到的在途数是总数除以库数，不均匀时个别库会更多。
//
// 所以阈值取“在途上限超过连接池总和”，也就是平均每个库的在途数超过它自己的池子，越过它几乎一定排队；
// 没越过也不保证不排队（不均匀、其他路径同时占用），要靠压测时看连接池的等待指标。
func checkSubmitConcurrency(logger *slog.Logger, cfg config) {
	inFlight := cfg.submitConcurrency * eventx.Partitions
	pool := cfg.mysqlMaxOpenConns * len(cfg.mysqlDSNs)
	if inFlight > pool {
		logger.Warn("submit consumer may queue on the order db connection pools: "+
			"max in-flight landings (SUBMIT_CONCURRENCY x partitions) exceed the total pool size (MYSQL_MAX_OPEN_CONNS x dbs)",
			"max_in_flight", inFlight, "submit_concurrency", cfg.submitConcurrency, "partitions", eventx.Partitions,
			"total_pool", pool, "mysql_max_open_conns", cfg.mysqlMaxOpenConns, "dbs", len(cfg.mysqlDSNs))
		return
	}
	logger.Info("submit consumer concurrency", "max_in_flight", inFlight, "submit_concurrency", cfg.submitConcurrency,
		"partitions", eventx.Partitions, "total_pool", pool)
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

	router, err := sharding.New(sharding.OrderLayout(len(cfg.mysqlDSNs)))
	if err != nil {
		return err
	}
	dbs := make([]*sql.DB, router.DBs())
	for i, dsn := range cfg.mysqlDSNs {
		db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn, MaxOpenConns: cfg.mysqlMaxOpenConns})
		if err != nil {
			return fmt.Errorf("open order db %d: %w", i, err)
		}
		defer db.Close()
		// 每个库只建自己的 slot 的表。扩容时新库的表是从旧库复制过来的（ADR-0015），这里的迁移在它上面
		// 整个重跑一遍，靠每条 DDL 可重入而成为空操作，只补上一张空的 outbox。
		if err := mysqlx.MigrateTemplate(ctx, db, order.Migrations, order.MigrationsDir,
			order.MigrationData{Slots: router.SlotsOf(i)}); err != nil {
			return fmt.Errorf("migrate order db %d: %w", i, err)
		}
		dbs[i] = db
	}
	var storeOpts []store.Option
	if cfg.mysqlReplicaDSNs != nil {
		replicas := make([]*sql.DB, router.DBs())
		for i, dsn := range cfg.mysqlReplicaDSNs {
			// 从库连不上不阻止启动（设计文档 1d 的 6.7、6.10）：它们初始被标记为不健康，展示读和 sweeper 读主库，
			// 探测（ReplicaProber）连续通过之后自动用上。所以不 ping，只有 DSN 写错才失败。
			db, err := mysqlx.OpenLazy(mysqlx.Config{DSN: dsn, MaxOpenConns: cfg.mysqlMaxOpenConns})
			if err != nil {
				return fmt.Errorf("open order db %d replica: %w", i, err)
			}
			defer db.Close()
			// 从库上不跑迁移：表结构经复制从主库过来，在从库上直接执行 DDL 会让两边分叉、打断复制。
			replicas[i] = db
		}
		storeOpts = append(storeOpts, store.WithReplicas(replicas))
	}
	// 在任何读写之前核对：DSN 列表里第 i 个库确实是 i 号库，库里记的总库数与配置一致。
	if err := store.EnsureShardMeta(ctx, dbs, router); err != nil {
		return err
	}
	st, err := store.New(dbs, router, storeOpts...)
	if err != nil {
		return err
	}
	// 冷库是可选的只读依赖（设计文档 1c 第 6 节）。一旦有订单归档过就必须配上，
	// 否则已归档的订单查不到。迁移也由 order 跑，和归档任务用的是同一套、同一把迁移锁。
	// 冷库连不上不阻止启动（设计文档 1d 的 6.8、6.10）：不 ping，迁移交给 ArchiveMigrator 在后台重试，
	// 成功之前旧订单的查询立刻返回 ARCHIVE_UNAVAILABLE，近期订单不受影响。
	var (
		cold        *store.Cold
		coldMigrate func(context.Context) error
	)
	if cfg.archiveDSN != "" {
		db, err := mysqlx.OpenLazy(mysqlx.Config{DSN: cfg.archiveDSN, MaxOpenConns: archiveMaxOpenConns})
		if err != nil {
			return fmt.Errorf("open archive db: %w", err)
		}
		defer db.Close()
		cold = store.NewCold(db)
		coldMigrate = func(ctx context.Context) error {
			return mysqlx.MigrateTemplate(ctx, db, order.ArchiveMigrations, order.ArchiveMigrationsDir,
				order.NewArchiveMigrationData())
		}
	}
	logger.InfoContext(ctx, "order archive", "configured", cold != nil)

	// 订单号的节点号。租约表放在第一个订单库（设计文档 2c 的 5.2）。
	ids, idLease, err := idgen.Open(ctx, cfg.workerID, dbs[0], logger)
	if err != nil {
		return fmt.Errorf("open id generator: %w", err)
	}
	if idLease != nil {
		// 正常退出时由 app.Run 停（让出节点号）；这里兜的是装配中途失败的情况，免得这个号白占一个租期。Stop 可以重复调用。
		defer func() {
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()
			_ = idLease.Stop(sctx)
		}()
	}

	conn, err := grpcx.Dial(cfg.inventoryAddr, grpcx.WithHopTimeout(inventoryHopTimeout))
	if err != nil {
		return err
	}
	defer conn.Close()

	rctx, rcancel := context.WithTimeout(ctx, redisStartTimeout)
	defer rcancel()
	// Redis 此刻连不上也照样启动（设计文档 1d 的 6.10）：受理会被闸门的熔断器拒绝，查询已落库的订单、
	// 支付结果、取消都不依赖它。Redis 故障期间重启或发布的实例要能回来承担这些。
	rdb, err := redisx.OpenLazy(rctx, redisx.Config{Addr: cfg.redisAddr, PoolSize: cfg.redisPoolSize}, logger)
	if err != nil {
		return err
	}
	// defer 在 app.Run 返回、所有组件都停下之后执行：受理、落库、回收都用它。
	defer rdb.Close()
	gate := stockgate.New(rdb, cfg.redisPrefix)

	// 降级开关（设计文档 1d 的 6.9）。各组件把自己的开关登记上来；全部登记完之后再读 DEGRADE 里的初始值。
	switches := degrade.New(serviceName, logger)
	orderCache, closeCache, err := openOrderCache(ctx, logger, cfg, rdb, switches)
	if err != nil {
		return err
	}
	// 和 rdb 一样在 app.Run 返回、gRPC 停下之后关闭。
	defer closeCache()

	// 锁放在闸门那个实例上（noeviction），不放缓存实例：锁的 key 被淘汰等于凭空释放。
	relayLocker, taskLockers, err := newLockers(logger, cfg, rdb)
	if err != nil {
		return err
	}
	sweeperLeader := taskLockers.Leader(sweeperLock)
	reaperLeader := taskLockers.Leader(holdReaperLock)
	leaders := map[string]*lock.Leader{
		sweeperLock:    sweeperLeader,
		holdReaperLock: reaperLeader,
	}
	// 每个库的中继各有一把 leader 锁：两个库的中继互不依赖，可以由不同副本持有。
	relayLeaders := make([]*lock.Leader, router.DBs())
	for i := range relayLeaders {
		name := relayLockPrefix + strconv.Itoa(i)
		relayLeaders[i] = relayLocker.Leader(name)
		leaders[name] = relayLeaders[i]
	}
	// 主动释放 leader 锁，让别的副本马上接手，而不是等锁过期。defer 在 app.Run 返回、
	// 所有 Loop 都停下之后执行（停下之前释放，Loop 的下一轮 Allow 会把锁重新抢回来），
	// 并且在 rdb.Close 之前（defer 后进先出）。
	defer releaseLeaders(ctx, logger, leaders)

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
	// 每个库一个中继，名字各不相同（见 relayName）。同一订单的事件都在它所在的库里，
	// 经过同一个中继、同一个消息 key 发到同一个分区，仍然有序。
	relays := make([]*outbox.Relay, router.DBs())
	kickers := make([]service.Kicker, router.DBs())
	for i := range relays {
		r, err := outbox.NewRelay(outbox.RelayConfig{
			Name:     relayName(i),
			DB:       dbs[i],
			Sender:   producer,
			Logger:   logger,
			Interval: cfg.outboxInterval,
			Guard:    lock.FailOpen(relayLeaders[i], logger),
		})
		if err != nil {
			return fmt.Errorf("new outbox relay for db %d: %w", i, err)
		}
		relays[i], kickers[i] = r, r
	}

	logger.InfoContext(ctx, "order read mode", "read_mode", cfg.readMode.String(), "replicas", st.HasReplicas())
	svc, err := service.New(service.Deps{
		Store:      st,
		Gate:       gate,
		Sender:     producer,
		Inventory:  inventoryv1.NewInventoryServiceClient(conn),
		IDs:        ids,
		Logger:     logger,
		OrderCache: orderCache,
		// 标记放在闸门那个实例上（noeviction）：被淘汰的标记等于提前到期。
		Marks:       readmark.New(rdb, cfg.redisPrefix),
		Cold:        cold,
		ColdMigrate: coldMigrate,
		Degrade:     switches,
	}, service.Config{
		PaymentTimeout:    cfg.paymentTimeout,
		IdempotencyTTL:    cfg.idempotencyTTL,
		OrderEventsTopic:  ordersTopic,
		SubmitTopic:       submitTopic,
		CloseDelayTopic:   closeTopic,
		Relays:            kickers,
		OrderCacheTTL:     cfg.orderCacheTTL,
		OrderCacheNullTTL: cfg.orderCacheNullTTL,
		Backlog:           cfg.backlog,
		ReadMode:          cfg.readMode,
		ReadPinTTL:        cfg.readPinTTL,
		ReadGTIDTTL:       cfg.readGTIDTTL,
		ReadGTIDWait:      cfg.readGTIDWait,
		// 设计文档 1d 的 5.7：展示读对每个订单库、读冷库各有并发上限，满了答 RESOURCE_EXHAUSTED。
		ReadMaxConcurrency:     cfg.readMaxConcurrency,
		ReadGateMaxConcurrency: cfg.readGateMaxConcurrency,
		GateFailureSpan:        service.DefaultGateFailureSpan,
		ArchiveMaxConcurrency:  cfg.archiveMaxConcurrency,
	})
	if err != nil {
		return err
	}
	// 展示读名额的在用数和拒绝数。defer 在 app.Run 返回、gRPC 停下之后执行，那时已没有展示读在用名额。
	unregisterLimitMetrics, err := svc.RegisterLimitMetrics()
	if err != nil {
		return fmt.Errorf("register read limit metrics: %w", err)
	}
	defer unregisterLimitMetrics()
	// 开关都登记完了（缓存的在 openOrderCache，其余在 service.New）。名字写错在这里就拒绝启动：
	// 被悄悄忽略的话，人以为关了其实没关。
	if err := switches.Apply(os.Getenv(degrade.Env)); err != nil {
		return err
	}
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
	// 闸门里排队的占用数，由回收任务每轮顺带读一次；只有在跑回收任务的实例（leader）上报。
	unregisterQueueMetrics, err := reaper.RegisterQueueMetrics()
	if err != nil {
		return fmt.Errorf("register stock gate queue metrics: %w", err)
	}
	defer unregisterQueueMetrics()

	paymentConsumer, err := kafkax.NewConsumer(kctx, kafkax.ConsumerConfig{
		Config: kcfg,
		Group:  eventx.Group(cfg.kafkaPrefix, eventx.GroupOrderPaymentEvents),
		Topics: []string{paymentsTopic},
		Logger: logger,
		// 支付结果不能因为订单库抖了一下就进死信：钱已经收了，这条消息是订单变成已支付的唯一途径，
		// 没有任何任务会重发它（设计文档 1b 的 5.4：长期 notified = 0 的支付单没有人扫）。
		// 默认的 5 次重试合计只有约 1.5 秒，订单库的一次主从切换、扩容时被搬的 slot 的不可用窗口
		// （ADR-0015，表被改名期间读写报“表不存在”）都比它长。所以普通错误无限重试，分区停在这里等库恢复；
		// 业务上的异常由 handler 给出明确的处置，消息本身有问题的由 handler 标成 Permanent 直接进死信，
		// 都不会走到这里的重试。代价是一条始终失败的消息会一直挡住它所在的分区，要靠积压告警发现（阶段 3）。
		MaxAttempts: math.MaxInt,
	}, svc.PaymentEventsHandler(service.DefaultEventTimeout))
	if err != nil {
		return err
	}
	// 落库消费者在分区内按 key 并发（设计文档 1d 的 2.2、2.3，ADR-0017）。能这样做的前提是：
	// 消息的 key 是订单号，kafkax 保证同一个 key 总在同一条 lane 上串行；而处理一条消息碰到的数据
	// ——闸门里这个订单的占用、订单行、订单明细、这个订单的 outbox 事件——都属于这个订单，
	// 不同订单的消息之间没有共享的可变状态，也没有先后要求。
	// 库存的行属于 SKU 而不是订单，不同订单会同时预占同一个 SKU，但那发生在 inventory 服务里，
	// 由它的行锁保护（1a 起有并发测试），不在这个消费者里。同一个用户的不同订单共用的只有
	// (user_id, idempotency_key) 唯一键，由数据库保证。
	// 代价是重启和再平衡后重复投递变成常态（提交的是连续完成的前缀），handler 本来就按至少一次设计。
	// 支付结果和延迟关单的消费者保持串行：量小，延迟消费者也不允许并发。
	checkSubmitConcurrency(logger, cfg)
	submitConsumer, err := kafkax.NewConsumer(kctx, kafkax.ConsumerConfig{
		Config:      kcfg,
		Group:       eventx.Group(cfg.kafkaPrefix, eventx.GroupOrderSubmit),
		Topics:      []string{submitTopic},
		Logger:      logger,
		Concurrency: cfg.submitConcurrency,
		// 普通错误不设重试上限，分区停在这条消息上等依赖恢复（设计文档 1d 的 6.2、6.5，ADR-0021）。
		// handler 返回普通错误只有一种含义：依赖不可用（闸门认领不了、订单库写不进去、库存服务调不通）。
		// 这时每条消息都会失败，把一条挪进死信没有用，后面的照样失败；全部挪进去之后，一次会自己恢复的
		// 故障就变成了要人逐条处理的数据问题：闸门不可用超过重试的总时长，已受理的订单认领不了、进了死信，
		// Redis 恢复后占用被回收，用户拿到过 202 的订单凭空消失（原来是 10 次、约 26 秒）。
		// 消息本身有问题的由 handler 标成 Permanent，仍然直接进死信。
		// 不设上限不等于永远重试，每条消息都有自己的终点：
		//   - 闸门恢复后认领成功，继续落库；已经过了支付时限的直接置为失败。
		//   - 订单行写进去之后一直预占不了的，sweeper 在创建超时后把它置为失败，下一次重试看到终态就结束。
		//   - 占用被回收或归还了的，下一次重试认领不到，丢弃。
		// 代价：一条 handler 没识别出来的坏消息会一直挡住它的分区，要靠消费积压的告警发现（阶段 3）。
		MaxAttempts: math.MaxInt,
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

	// 内部端口：存活和就绪探测、降级开关。不经过 gateway。
	readiness := &app.Readiness{}
	admin := app.AdminMux(serviceName, readiness)
	switches.Register(admin)
	httpServer, err := app.ListenHTTP(ctx, "http", cfg.httpAddr, admin)
	if err != nil {
		return err
	}
	grpcServer, err := grpcx.Listen(ctx, logger, "grpc", cfg.grpcAddr, func(s *grpc.Server) {
		orderv1.RegisterOrderServiceServer(s, svc)
	})
	if err != nil {
		return err
	}
	readLimitReporter := svc.ReadLimitReporter(service.DefaultReadLimitReportInterval)
	// 读路径降级的两个后台任务（设计文档 1d 的 6.7、6.8）：从库的健康探测（只在配了从库时），冷库的后台迁移（只在配了冷库时）。
	var background []app.Component
	if st.HasReplicas() {
		prober, err := svc.ReplicaProber(service.DefaultReplicaProbeInterval, cfg.readMaxLag)
		if err != nil {
			return err
		}
		background = append(background, prober)
	}
	if m := svc.ArchiveMigrator(); m != nil {
		background = append(background, m)
	}
	logger.InfoContext(ctx, "listening", "http_addr", httpServer.Addr().String(), "grpc_addr", grpcServer.Addr().String())

	// 停止顺序与列出的顺序相反：
	//  0. 收到退出信号后先摘流（设计文档 1d 的 6.11）：就绪探测和 gRPC 健康检查变成不可用，继续正常服务
	//     DRAIN_DELAY 这么久，让负载均衡把流量挪走，然后才开始下面的停止。本机默认 0。
	//  1. sweeper、回收任务、从库探测、冷库迁移：纯后台任务，随时可以停，下次启动接着做。
	//     从库探测停了之后健康标记停在最后的状态，冷库迁移停了之后冷库保持不可用；此后 gRPC 还在处理
	//     在途请求，它们按这两个状态读，都是安全的一侧（最多是从库刚坏而没被发现，请求路径上读出错会立刻标记）。
	//  2. gRPC：不再受理新请求，并等在途的 SubmitOrder 做完。受理是“Hold → 发消息”两步，
	//     中途被打断会留下孤儿占用（只能等回收任务），所以让它们在 Redis 和生产者都还可用时走完。
	//  3. 三个消费者：支付结果、延迟关单、落库。它们写 outbox，必须在中继之前停；
	//     没处理完的消息不提交位点，下次启动（或别的副本）重新消费。
	//  4. 各库的 outbox 中继：排在所有会写 outbox 的组件之后，让最后写入的事件尽量在退出前发出；
	//     没发出的也不会丢，下次启动时中继会先跑一轮。
	//  5. 健康检查最后停。
	// 生产者、Redis、数据库和库存的连接由 defer 在 app.Run 返回后关闭，此时已没有组件在用。
	// 全部 leader 锁（每个中继一把、sweeper、回收任务）也在 app.Run 返回后释放（先于 Redis 连接关闭）：
	// 此时所有中继、sweeper、回收任务都已停下。
	// 汇总日志在 gRPC 停下之后才停（停止顺序与列表相反），停止时把最后一段的拒绝次数也记下来。
	// 节点号的租约紧跟在健康检查之后：它比 gRPC 晚停，受理还在进行时一直能发号，gRPC 停了之后才让出。
	components := []app.Component{httpServer}
	if idLease != nil {
		components = append(components, idLease)
	}
	components = append(components, readLimitReporter)
	for _, r := range relays {
		components = append(components, r)
	}
	components = append(components, submitConsumer, closeConsumer, paymentConsumer, grpcServer, reaper, sweeper)
	components = append(components, background...)
	return app.RunWith(ctx, logger, app.Options{Readiness: readiness, DrainDelay: cfg.drainDelay}, components...)
}

// openOrderCache 按配置创建订单查询缓存。ORDER_CACHE_TTL=0 时缓存为 nil（关闭）。
// 缓存实例与闸门是同一个地址时复用 rdb，不另开连接池；否则新开一个客户端，由返回的 closeFn 关闭。
func openOrderCache(ctx context.Context, logger *slog.Logger, cfg config, rdb redis.Cmdable, switches *degrade.Set) (
	c *cache.Cache[*orderv1.Order], closeFn func(), err error,
) {
	closeFn = func() {}
	if cfg.orderCacheTTL == 0 {
		logger.InfoContext(ctx, "order query cache disabled")
		return nil, closeFn, nil
	}
	client := rdb
	if cfg.cacheRedisAddr != cfg.redisAddr {
		rctx, cancel := context.WithTimeout(ctx, redisStartTimeout)
		defer cancel()
		// 缓存是可选的依赖，连不上不阻止启动：查询直接回源（cache 包里的熔断器），恢复后自动用上。
		cdb, err := redisx.OpenLazy(rctx, redisx.Config{Addr: cfg.cacheRedisAddr}, logger)
		if err != nil {
			return nil, closeFn, fmt.Errorf("open cache redis: %w", err)
		}
		closeFn = func() { _ = cdb.Close() }
		client = cdb
	}
	c, err = service.NewOrderCache(client, cfg.redisPrefix, logger, switches)
	if err != nil {
		closeFn()
		return nil, func() {}, err
	}
	return c, closeFn, nil
}

// newLockers 创建两个 Locker：各库的中继共用一个（TTL 短），其余周期任务共用一个。
func newLockers(logger *slog.Logger, cfg config, rdb redis.Scripter) (*lock.Locker, *lock.Locker, error) {
	relayLocker, err := lock.New(rdb, lock.Options{TTL: relayLockTTL, KeyPrefix: cfg.redisPrefix, Logger: logger})
	if err != nil {
		return nil, nil, fmt.Errorf("new relay locker: %w", err)
	}
	taskLocker, err := lock.New(rdb, lock.Options{TTL: taskLockTTL, KeyPrefix: cfg.redisPrefix, Logger: logger})
	if err != nil {
		return nil, nil, fmt.Errorf("new task locker: %w", err)
	}
	return relayLocker, taskLocker, nil
}

// releaseLeaders 主动释放各 leader 锁。ctx 已结束（进程在退出），释放用独立的时限。
func releaseLeaders(ctx context.Context, logger *slog.Logger, leaders map[string]*lock.Leader) {
	for name, ld := range leaders {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaderReleaseTimeout)
		if err := ld.Close(rctx); err != nil {
			logger.WarnContext(ctx, "release leader lock failed", "lock", name, "error", err)
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
