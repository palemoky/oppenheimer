// Command payment 是 payment 服务的入口。
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

	"google.golang.org/grpc"

	"github.com/palemoky/Oppenheimer/api/eventx"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/Oppenheimer/pkg/app"
	"github.com/palemoky/Oppenheimer/pkg/grpcx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/logx"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/pkg/outbox"
	"github.com/palemoky/Oppenheimer/services/payment"
	"github.com/palemoky/Oppenheimer/services/payment/internal/consumer"
	"github.com/palemoky/Oppenheimer/services/payment/internal/processor"
	"github.com/palemoky/Oppenheimer/services/payment/internal/service"
	"github.com/palemoky/Oppenheimer/services/payment/internal/store"
)

const serviceName = "payment"

// kafkaStartTimeout 是启动时连接 Kafka、建 topic 的时限。
const kafkaStartTimeout = 30 * time.Second

type config struct {
	httpAddr  string
	grpcAddr  string
	mysqlDSN  string
	workerID  int64
	orderAddr string
	// Kafka 与 outbox，见设计文档第 4 节。
	kafkaBrokers   []string
	kafkaPrefix    string
	outboxInterval time.Duration
	processor      processor.Config
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
	}

	var err error
	if cfg.workerID, err = strconv.ParseInt(cmp.Or(os.Getenv("WORKER_ID"), "0"), 10, 64); err != nil {
		return config{}, fmt.Errorf("parse WORKER_ID: %w", err)
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
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ids, err := idgen.New(cfg.workerID)
	if err != nil {
		return fmt.Errorf("create id generator: %w", err)
	}

	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: cfg.mysqlDSN})
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}
	defer db.Close()
	if err := mysqlx.Migrate(ctx, db, payment.Migrations, payment.MigrationsDir); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	orderConn, err := grpcx.Dial(cfg.orderAddr)
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
	})
	if err != nil {
		return fmt.Errorf("create outbox relay: %w", err)
	}

	now := func() time.Time { return time.Now().UTC() }
	st := store.New(db)
	svc := service.New(st, orders, ids, now, logger)
	cfg.processor.Now = now
	proc, err := processor.New(st, relay, logger, cfg.processor)
	if err != nil {
		return err
	}
	orderEvents, err := kafkax.NewConsumer(startCtx, kafkax.ConsumerConfig{
		Config: kafkaCfg,
		Group:  eventx.Group(cfg.kafkaPrefix, eventx.GroupPaymentOrderEvents),
		Topics: []string{eventx.Topic(cfg.kafkaPrefix, eventx.OrderEvents)},
		Logger: logger,
	}, consumer.New(st, logger, now).Kafka(consumer.DefaultTimeout))
	if err != nil {
		return fmt.Errorf("create order events consumer: %w", err)
	}

	httpServer, err := app.ListenHTTP(ctx, "http", cfg.httpAddr, app.HealthHandler(serviceName))
	if err != nil {
		return err
	}
	grpcServer, err := grpcx.Listen(ctx, logger, "grpc", cfg.grpcAddr, func(s *grpc.Server) {
		paymentv1.RegisterPaymentServiceServer(s, svc)
	})
	if err != nil {
		return err
	}
	logger.Info("listening", "http_addr", httpServer.Addr().String(), "grpc_addr", grpcServer.Addr().String())

	// app.Run 按相反顺序停止：先停 gRPC 不再受理新支付，再停 processor（不再产生新事件），
	// 再停消费者，然后停中继（它最后一轮会把 processor 刚写的事件发出去），健康检查最后停。
	return app.Run(ctx, logger, httpServer, relay, orderEvents, proc, grpcServer)
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
