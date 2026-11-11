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
	"time"

	"google.golang.org/grpc"

	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/app"
	"github.com/palemoky/Oppenheimer/pkg/grpcx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/logx"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/services/order"
	"github.com/palemoky/Oppenheimer/services/order/internal/service"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

const serviceName = "order"

type config struct {
	httpAddr       string
	grpcAddr       string
	mysqlDSN       string
	workerID       int64
	inventoryAddr  string
	paymentTimeout time.Duration
	sweeper        service.SweeperConfig
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
	}
	var err error
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
	if cfg.sweeper.SettleGrace, err = envDuration("SETTLE_GRACE", service.DefaultSettleGrace); err != nil {
		return cfg, err
	}
	// 创建时限必须给在线路径的一次 Reserve 留出余地，否则后台任务会和正常下单抢着处理。
	if cfg.sweeper.CreateTimeout <= grpcx.DefaultCallTimeout {
		return cfg, fmt.Errorf("CREATE_TIMEOUT must be longer than %s", grpcx.DefaultCallTimeout)
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

	svc := service.New(store.New(db), inventoryv1.NewInventoryServiceClient(conn), ids, logger,
		service.Config{PaymentTimeout: cfg.paymentTimeout})

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

	// 停止顺序与此相反：先停后台任务，再停 gRPC，最后停健康检查。
	return app.Run(ctx, logger, httpServer, grpcServer, service.NewSweeper(svc, cfg.sweeper))
}
