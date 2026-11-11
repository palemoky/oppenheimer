// Command inventory 是 inventory 服务的入口。
package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"

	"google.golang.org/grpc"

	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/Oppenheimer/pkg/app"
	"github.com/palemoky/Oppenheimer/pkg/grpcx"
	"github.com/palemoky/Oppenheimer/pkg/logx"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/services/inventory"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/service"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/store"
)

const serviceName = "inventory"

type config struct {
	httpAddr string
	grpcAddr string
	mysqlDSN string
}

func loadConfig() config {
	return config{
		httpAddr: cmp.Or(os.Getenv("HTTP_ADDR"), ":8082"),
		grpcAddr: cmp.Or(os.Getenv("GRPC_ADDR"), ":9082"),
		mysqlDSN: cmp.Or(os.Getenv("MYSQL_DSN"), "root:oppenheimer@tcp(127.0.0.1:3306)/inventory_db"),
	}
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

	return app.Run(ctx, logger, httpServer, grpcServer)
}
