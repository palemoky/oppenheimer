// Command inventory 是 inventory 服务的入口。
package main

import (
	"cmp"
	"context"
	"log/slog"
	"os"

	"github.com/palemoky/Oppenheimer/pkg/app"
	"github.com/palemoky/Oppenheimer/pkg/logx"
)

const serviceName = "inventory"

func main() {
	logger := logx.New(serviceName, os.Stdout, logx.LevelFromEnv())
	slog.SetDefault(logger)

	if err := run(context.Background(), logger); err != nil {
		logger.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	addr := cmp.Or(os.Getenv("HTTP_ADDR"), ":8082")

	httpServer, err := app.ListenHTTP(ctx, "http", addr, app.HealthHandler(serviceName))
	if err != nil {
		return err
	}
	logger.Info("listening", "addr", httpServer.Addr().String())

	return app.Run(ctx, logger, httpServer)
}
