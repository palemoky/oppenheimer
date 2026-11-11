// Command inventory 是 inventory 服务的入口。
package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/palemoky/Oppenheimer/api/eventx"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/Oppenheimer/pkg/app"
	"github.com/palemoky/Oppenheimer/pkg/grpcx"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/logx"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/services/inventory"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/service"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/store"
)

const serviceName = "inventory"

// kafkaSetupTimeout 是启动时连接 Kafka、建 topic 的时限。broker 不可达时尽快失败退出，
// 交给编排系统重启，而不是无限期卡在启动阶段。
const kafkaSetupTimeout = 30 * time.Second

type config struct {
	httpAddr     string
	grpcAddr     string
	mysqlDSN     string
	kafkaBrokers []string
	kafkaPrefix  string
}

func loadConfig() config {
	return config{
		httpAddr:     cmp.Or(os.Getenv("HTTP_ADDR"), ":8082"),
		grpcAddr:     cmp.Or(os.Getenv("GRPC_ADDR"), ":9082"),
		mysqlDSN:     cmp.Or(os.Getenv("MYSQL_DSN"), "root:oppenheimer@tcp(127.0.0.1:3306)/inventory_db"),
		kafkaBrokers: splitList(cmp.Or(os.Getenv("KAFKA_BROKERS"), "127.0.0.1:9092")),
		kafkaPrefix:  os.Getenv("KAFKA_PREFIX"),
	}
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

	if err := run(context.Background(), logger, loadConfig()); err != nil {
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

	svc := service.New(store.New(db), logger)

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

	// app.Run 按相反顺序停止：消费者最先停，在途事件处理完、位点提交后再关 gRPC 和 HTTP。
	return app.Run(ctx, logger, httpServer, grpcServer, consumer)
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
	}, svc.OrderEventHandler(service.DefaultEventTimeout))
	if err != nil {
		return nil, fmt.Errorf("new order event consumer: %w", err)
	}
	return consumer, nil
}
