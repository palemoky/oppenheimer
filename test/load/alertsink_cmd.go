package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/test/load/internal/alertsink"
)

// alertsinkServiceName 是接收端在链路和指标里的服务名。
const alertsinkServiceName = "alertsink"

// alertsinkCmd 运行常驻的 Alertmanager webhook 接收端，直到收到 SIGINT 或 SIGTERM。
func alertsinkCmd(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("alertsink", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var listen, logLevel string
	fs.StringVar(&listen, "listen", ":8080", "address to listen on")
	fs.StringVar(&logLevel, "log-level", "", "log level: debug, info, warn, error (default $"+logx.LevelEnv+", else info)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if logLevel == "" {
		logLevel = os.Getenv(logx.LevelEnv)
	}
	logger := logx.New(alertsinkServiceName, stderr, logx.ParseLevel(logLevel))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdown, err := otelx.Setup(ctx, alertsinkServiceName, logger)
	if err != nil {
		logger.ErrorContext(ctx, "telemetry setup failed", "error", err)
		return 1
	}
	// 最先注册、最后执行：停止时最后一次指标也要发出去。
	defer otelx.Shutdown(ctx, shutdown, logger)

	if err := alertsink.Run(ctx, alertsink.Config{Listen: listen, Logger: logger}); err != nil {
		logger.ErrorContext(ctx, "alertsink failed", "error", fmt.Errorf("run: %w", err))
		return 1
	}
	return 0
}
