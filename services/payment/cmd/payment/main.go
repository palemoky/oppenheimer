// Command payment 是 payment 服务的入口。
package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/Oppenheimer/pkg/app"
	"github.com/palemoky/Oppenheimer/pkg/grpcx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/logx"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/services/payment"
	"github.com/palemoky/Oppenheimer/services/payment/internal/processor"
	"github.com/palemoky/Oppenheimer/services/payment/internal/service"
	"github.com/palemoky/Oppenheimer/services/payment/internal/store"
)

const serviceName = "payment"

type config struct {
	httpAddr  string
	grpcAddr  string
	mysqlDSN  string
	workerID  int64
	orderAddr string
	processor processor.Config
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
	}

	var err error
	if cfg.workerID, err = strconv.ParseInt(cmp.Or(os.Getenv("WORKER_ID"), "0"), 10, 64); err != nil {
		return config{}, fmt.Errorf("parse WORKER_ID: %w", err)
	}
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

	now := func() time.Time { return time.Now().UTC() }
	st := store.New(db)
	svc := service.New(st, orders, ids, now, logger)
	cfg.processor.Now = now
	proc := processor.New(st, orders, logger, cfg.processor)

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

	// app.Run 按相反顺序停止：先停 gRPC 不再受理新支付，再停 processor，健康检查最后停。
	return app.Run(ctx, logger, httpServer, proc, grpcServer)
}
