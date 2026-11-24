package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/services/recon/internal/totals"
)

// watchTotalsCommand 是常驻核对的子命令：
//
//	recon watch-totals [-interval 30s] [-window 30m] [-skip 1m] [-query-timeout 10s] [-drill-timeout 10s]
//
// 每隔 interval 按笔数和金额核对一次最近 window 里的支付（internal/totals 的包注释写了规则），结果报成指标，
// 有差异时每一轮打一条 Error 日志；同一分钟连续两轮对不上时逐笔比一次那一分钟，把是哪几笔支付打进日志。
// 它不退出：库连不上、查询超时都只记一次失败，等下一轮。
//
// 环境变量：MYSQL_DSNS（订单库的主库，与 order 服务相同）、PAYMENT_DSN，以及 OpenTelemetry 的那几个
// （没设 OTEL_EXPORTER_OTLP_ENDPOINT 时不导出，只有日志）。
const watchTotalsCommand = "watch-totals"

// watchConnectTimeout 是启动时连上各个库的时限。和一次性的对账不同，这里连不上就退出让 Kubernetes 重启：
// 启动时的配置错误（DSN 写错）应该马上暴露，而不是变成一个永远在“失败一轮”的进程。
const watchConnectTimeout = 30 * time.Second

func watchTotals(logger *slog.Logger, args []string) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runWatchTotals(ctx, logger, args); err != nil {
		logger.ErrorContext(ctx, "watch-totals failed", "error", err)
		return exitError
	}
	return exitClean
}

func runWatchTotals(ctx context.Context, logger *slog.Logger, args []string) error {
	var cfg totals.Config
	fs := flag.NewFlagSet(watchTotalsCommand, flag.ContinueOnError)
	fs.DurationVar(&cfg.Interval, "interval", 30*time.Second, "time between checks")
	fs.DurationVar(&cfg.Window, "window", 30*time.Minute, "check payments created within this long before now")
	fs.DurationVar(&cfg.Skip, "skip", time.Minute, "leave out payments created within this long before now")
	fs.DurationVar(&cfg.QueryTimeout, "query-timeout", 10*time.Second, "time limit of one check")
	fs.DurationVar(&cfg.DrillTimeout, "drill-timeout", 10*time.Second, "time limit of one per-payment drill-down into a mismatching minute")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	if cfg.Interval <= 0 || cfg.Window <= 0 || cfg.Skip <= 0 || cfg.QueryTimeout <= 0 || cfg.DrillTimeout <= 0 {
		return errors.New("-interval, -window, -skip, -query-timeout and -drill-timeout must be positive")
	}

	shutdownTelemetry, err := otelx.Setup(ctx, serviceName, logger)
	if err != nil {
		return err
	}
	// 关闭用不跟着停止信号取消的 ctx：收到 SIGTERM 之后还要把最后一批指标推出去。
	defer otelx.Shutdown(context.WithoutCancel(ctx), shutdownTelemetry, logger)

	orderDSNs := splitDSNs(os.Getenv("MYSQL_DSNS"))
	paymentDSN := os.Getenv("PAYMENT_DSN")
	if len(orderDSNs) == 0 || paymentDSN == "" {
		return errors.New("MYSQL_DSNS and PAYMENT_DSN are required")
	}
	router, err := sharding.New(sharding.OrderLayout(len(orderDSNs)))
	if err != nil {
		return fmt.Errorf("order shard layout: %w", err)
	}

	connectCtx, cancel := context.WithTimeout(ctx, watchConnectTimeout)
	defer cancel()
	// 每个库同一时刻只有一条查询在跑，连接给两个就够。
	open := func(name, dsn string) (*sql.DB, error) {
		if dsn == "" {
			return nil, fmt.Errorf("%s has an empty DSN", name)
		}
		db, err := mysqlx.Open(connectCtx, mysqlx.Config{DSN: dsn, MaxOpenConns: 2})
		if err != nil {
			return nil, fmt.Errorf("connect %s: %w", name, err)
		}
		return db, nil
	}
	cfg.Sources.Router = router
	for i, dsn := range orderDSNs {
		db, err := open(fmt.Sprintf("order_%d", i), dsn)
		if err != nil {
			return err
		}
		defer db.Close() //nolint:errcheck // 进程退出时关闭，失败没有可做的事
		cfg.Sources.Orders = append(cfg.Sources.Orders, db)
	}
	pay, err := open("payment", paymentDSN)
	if err != nil {
		return err
	}
	defer pay.Close() //nolint:errcheck // 同上
	cfg.Sources.Payment = pay
	cfg.Logger = logger

	w, err := totals.NewWatcher(cfg)
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, "watching payment totals", "interval", cfg.Interval.String(), "window", cfg.Window.String(),
		"skip", cfg.Skip.String(), "order_dbs", len(orderDSNs))
	return w.Run(ctx)
}
