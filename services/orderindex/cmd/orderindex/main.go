// Command orderindex 是订单索引服务的入口：消费 CDC 发出的订单行变更写入商家索引表，并提供商家维度的订单查询。
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

	"google.golang.org/grpc"

	"github.com/palemoky/oppenheimer/api/eventx"
	orderindexv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/orderindex/v1"
	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/grpcx"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/services/orderindex"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/indexer"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/service"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

const serviceName = "orderindex"

// kafkaStartTimeout 是启动时连接 Kafka、建 topic 的时限。
const kafkaStartTimeout = 30 * time.Second

// defaultMySQLDSNs 是 MYSQL_DSNS 的默认值，第 i 个是 merchant_index_db_i。
// 两个索引库都放在现有的 mysql 实例上（设计文档 1c 4.9）。
const defaultMySQLDSNs = "root:oppenheimer@tcp(127.0.0.1:3306)/merchant_index_db_0," +
	"root:oppenheimer@tcp(127.0.0.1:3306)/merchant_index_db_1"

// defaultMySQLMaxOpenConns 是 MYSQL_MAX_OPEN_CONNS 的默认值：每个索引库的连接池上限。
//
// 需求：消费者每个分区一个 worker，一次处理一批，一批在每个库上最多占一条连接（一个事务，两个库并行写），
// 所以写入同时在途的连接最多是 eventx.Partitions = 4 条每库。再加上 gRPC 的列表查询，都是短的范围扫描。
// 10 留出了查询的余量；写入不会在池上排队。
//
// 预算：本机两个索引库和 inventory_db、payment_db、订单冷库在同一个 MySQL 实例上，max_connections 400。
// 各服务的池上限：inventory 50、payment 50（mysqlx 默认）、order 的冷库 10、archiver 的冷库 4，
// 加上本服务 2 × 10 = 20，合计 134（按批之前本服务是 2 × 25，合计 164）。
// 分区数变多时写入在途的连接随之增加，按“分区数 + 查询的并发”调大。
const defaultMySQLMaxOpenConns = 10

// defaultIndexMaxBatch 是 INDEX_MAX_BATCH 的默认值：消费者一批最多处理多少条消息（设计文档 1d 2.11）。
//
// 一批合成每个库一个事务、每张索引表一条多行语句，提交次数从每条一次降到每批每库一次。
// 不等凑满：没有积压时一批就是一条，延迟不变；有积压时批自然变大。
// 500 行是几十毫秒的事务，锁持有时间和重做的代价（一批失败会逐条回退）都还小。
const defaultIndexMaxBatch = 500

// maxIndexMaxBatch 是 INDEX_MAX_BATCH 的上限：一条多行语句最多 store.MaxStatementRows（6553）行，
// 由 MySQL 预处理语句 65535 个占位符、每行 10 列决定。一批的行全落在同一张表上时也能用一条语句写完。
// store 对超过它的行会再切成几条语句，所以这不是正确性的限制，而是再大也没有收益：
// 事务更长、持锁更久、失败后逐条回退的代价更大，也更容易碰到 indexer.DefaultWriteTimeout。
const maxIndexMaxBatch = store.MaxStatementRows

type config struct {
	httpAddr          string
	grpcAddr          string
	mysqlDSNs         []string
	mysqlMaxOpenConns int
	// indexMaxBatch 是消费者一批最多处理的消息数，见 defaultIndexMaxBatch。
	indexMaxBatch int
	kafkaBrokers  []string
	kafkaPrefix   string
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
		httpAddr:    cmp.Or(os.Getenv("HTTP_ADDR"), ":8084"),
		grpcAddr:    cmp.Or(os.Getenv("GRPC_ADDR"), ":9084"),
		kafkaPrefix: os.Getenv("KAFKA_PREFIX"),
	}
	for d := range strings.SplitSeq(cmp.Or(os.Getenv("MYSQL_DSNS"), defaultMySQLDSNs), ",") {
		cfg.mysqlDSNs = append(cfg.mysqlDSNs, strings.TrimSpace(d))
	}
	// 分片布局见 sharding.MerchantIndexLayout，是代码里的常量，不随配置变。
	if len(cfg.mysqlDSNs) != sharding.MerchantIndexDBs || slices.Contains(cfg.mysqlDSNs, "") {
		return cfg, fmt.Errorf("MYSQL_DSNS must have exactly %d non-empty DSNs (merchant_index_db_0 .. merchant_index_db_%d), got %d",
			sharding.MerchantIndexDBs, sharding.MerchantIndexDBs-1, len(cfg.mysqlDSNs))
	}
	maxOpen, err := envInt("MYSQL_MAX_OPEN_CONNS", defaultMySQLMaxOpenConns)
	if err != nil {
		return cfg, err
	}
	if maxOpen <= 0 {
		return cfg, errors.New("MYSQL_MAX_OPEN_CONNS must be positive")
	}
	cfg.mysqlMaxOpenConns = int(maxOpen)
	// INDEX_CONCURRENCY（分区内按 key 并发）已被按批处理取代，两者在 kafkax 里互斥。还设着它说明部署配置没跟上，
	// 静默忽略会让人以为调的是并发度，所以直接拒绝启动。
	if os.Getenv("INDEX_CONCURRENCY") != "" {
		return cfg, errors.New("INDEX_CONCURRENCY is no longer supported by orderindex; set INDEX_MAX_BATCH instead")
	}
	maxBatch, err := envInt("INDEX_MAX_BATCH", defaultIndexMaxBatch)
	if err != nil {
		return cfg, err
	}
	if maxBatch < 1 || maxBatch > maxIndexMaxBatch {
		return cfg, fmt.Errorf("INDEX_MAX_BATCH must be within [1, %d], got %d", maxIndexMaxBatch, maxBatch)
	}
	cfg.indexMaxBatch = int(maxBatch)
	for b := range strings.SplitSeq(cmp.Or(os.Getenv("KAFKA_BROKERS"), "127.0.0.1:9092"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.kafkaBrokers = append(cfg.kafkaBrokers, b)
		}
	}
	if len(cfg.kafkaBrokers) == 0 {
		return cfg, errors.New("KAFKA_BROKERS has no broker addresses")
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

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	router, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil {
		return err
	}
	dbs := make([]*sql.DB, router.DBs())
	for i, dsn := range cfg.mysqlDSNs {
		db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn, MaxOpenConns: cfg.mysqlMaxOpenConns})
		if err != nil {
			return fmt.Errorf("open merchant index db %d: %w", i, err)
		}
		defer db.Close()
		// 每个库只建自己的 slot 的表。
		if err := mysqlx.MigrateTemplate(ctx, db, orderindex.Migrations, orderindex.MigrationsDir,
			orderindex.MigrationData{Slots: router.SlotsOf(i)}); err != nil {
			return fmt.Errorf("migrate merchant index db %d: %w", i, err)
		}
		dbs[i] = db
	}
	st, err := store.New(dbs, router)
	if err != nil {
		return err
	}
	ix, err := indexer.New(st, logger)
	if err != nil {
		return err
	}
	svc, err := service.New(st)
	if err != nil {
		return err
	}

	kcfg := kafkax.Config{Brokers: cfg.kafkaBrokers, ClientID: serviceName}
	topic := eventx.Topic(cfg.kafkaPrefix, eventx.CDCOrders)
	kctx, kcancel := context.WithTimeout(ctx, kafkaStartTimeout)
	defer kcancel()
	if err := ensureTopics(kctx, kcfg, topic); err != nil {
		return err
	}
	consumer, err := kafkax.NewBatchConsumer(kctx, kafkax.BatchConsumerConfig{
		Config: kcfg,
		Group:  eventx.Group(cfg.kafkaPrefix, eventx.GroupOrderIndexCDCOrders),
		Topics: []string{topic},
		Logger: logger,
		// 无限重试，不进死信（设计文档 1c 4.8）：表结构不符的消息是毒消息，消费要停在这里持续报错，
		// 不能跳过；跳过就丢了这个订单的这个版本，而后面的版本可能很久都不来。写库失败同理，等库恢复。
		// kafkax 对普通错误的处理是“原地重试 MaxAttempts 次后进死信、分区继续”，所以取最大值让它永远不到上限；
		// 退避封顶在 RetryMax（默认 5 秒），日志里每 5 秒一条重试告警。
		// 只有反序列化失败（kafkax.Permanent）会直接进死信，理由见 indexer.Handler。
		//
		// 按批处理时“停住”是这样发生的：一批里有一条坏消息，整批返回错误、什么都没写（或只写成了一个库）；
		// kafkax 把这一批退回逐条处理，坏消息之前的各自写成功、位点前进；坏的那条原地无限重试，
		// 分区停在它前面，它和它之后的消息都不会被跳过，也不会再取下一批。和逐条处理时的停住完全一样。
		// 库暂时不可用时也走这条路：逐条回退后第一条原地重试到库恢复，之后这一批剩下的逐条写完，再恢复按批。
		MaxAttempts: math.MaxInt,
		MaxBatch:    cfg.indexMaxBatch,
	}, ix.Handler(indexer.DefaultWriteTimeout))
	if err != nil {
		return err
	}
	reporter := indexer.NewReporter(ix.Stats(), logger, indexer.DefaultReportInterval)

	// 内部端口：存活和就绪探测。就绪状态由 app.RunWith 维护（设计文档 1d 的 6.11）。
	readiness := &app.Readiness{}
	drainDelay, err := app.DrainDelayFromEnv()
	if err != nil {
		return err
	}
	httpServer, err := app.ListenHTTP(ctx, "http", cfg.httpAddr, app.AdminMux(serviceName, readiness))
	if err != nil {
		return err
	}
	grpcServer, err := grpcx.Listen(ctx, logger, "grpc", cfg.grpcAddr, func(s *grpc.Server) {
		orderindexv1.RegisterOrderIndexServiceServer(s, svc)
	})
	if err != nil {
		return err
	}
	logger.Info("listening", "http_addr", httpServer.Addr().String(), "grpc_addr", grpcServer.Addr().String())

	// 收到退出信号后先摘流：就绪探测和 gRPC 健康检查变成不可用，继续正常服务 DRAIN_DELAY 这么久（本机默认 0），
	// 然后按与列出的顺序相反的顺序停止：先停 gRPC（等在途查询做完），再停消费者（处理完在途消息、提交位点），
	// 然后汇总日志输出最后一段，健康检查最后停。数据库连接由 defer 在 app.RunWith 返回后关闭。
	return app.RunWith(ctx, logger, app.Options{Readiness: readiness, DrainDelay: drainDelay},
		httpServer, reporter, consumer, grpcServer)
}

// ensureTopics 创建本服务消费的 topic 及其死信 topic。broker 关闭了自动建 topic，消费者要求死信 topic 已存在。
//
// 变更 topic 由 CDC 发布，这里也按同样的规格（压缩保留，见 eventx.CDCOrders）声明一次，
// 让两个服务的启动顺序无关：已存在时不动。
func ensureTopics(ctx context.Context, kcfg kafkax.Config, topic string) error {
	client, err := kafkax.NewClient(ctx, kcfg)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := kafkax.EnsureTopics(ctx, client,
		kafkax.TopicSpec{Name: topic, Partitions: eventx.Partitions, Configs: map[string]string{"cleanup.policy": "compact"}},
		kafkax.TopicSpec{Name: topic + eventx.DLQSuffix, Partitions: eventx.Partitions},
	); err != nil {
		return fmt.Errorf("ensure topics: %w", err)
	}
	return nil
}
