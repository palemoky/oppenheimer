package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/services/recon/internal/stock"
)

// watchStockCommand 是常驻核对库存的子命令：
//
//	recon watch-stock [-interval 1m] [-full-every 1h] [-reserved-window 2h] [-stuck-after 1h]
//
// 在库存库的一个一致性快照里核对计数对不对得上流水（internal/stock 的包注释写了规则），结果报成指标，
// 有差异时每一轮打 Error 日志。它不退出：库连不上、查询超时都只记一次失败，等下一轮。
//
// 环境变量：INVENTORY_DSN（库存库的主库），以及 OpenTelemetry 的那几个。
const watchStockCommand = "watch-stock"

func watchStock(logger *slog.Logger, args []string) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runWatchStock(ctx, logger, args); err != nil {
		logger.ErrorContext(ctx, "watch-stock failed", "error", err)
		return exitError
	}
	return exitClean
}

func runWatchStock(ctx context.Context, logger *slog.Logger, args []string) error {
	cfg := stock.Config{Logger: logger}
	fs := flag.NewFlagSet(watchStockCommand, flag.ContinueOnError)
	fs.DurationVar(&cfg.Interval, "interval", time.Minute, "time between checks")
	fs.DurationVar(&cfg.FullEvery, "full-every", time.Hour, "how often a check also scans the whole ledger (sold counters, stuck reservations)")
	fs.DurationVar(&cfg.ReservedWindow, "reserved-window", 2*time.Hour, "quick checks look at reservations created within this long before now; must be much longer than the payment deadline")
	fs.DurationVar(&cfg.StuckAfter, "stuck-after", time.Hour, "a reservation still RESERVED after this long counts as stuck")
	fs.DurationVar(&cfg.QuickTimeout, "quick-timeout", 5*time.Second, "time limit of a quick check")
	fs.DurationVar(&cfg.FullTimeout, "full-timeout", 2*time.Minute, "time limit of a full check")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	if cfg.Interval <= 0 || cfg.FullEvery <= 0 || cfg.ReservedWindow <= 0 || cfg.StuckAfter <= 0 || cfg.QuickTimeout <= 0 || cfg.FullTimeout <= 0 {
		return errors.New("all durations must be positive")
	}
	dsn := strings.TrimSpace(os.Getenv("INVENTORY_DSN"))
	if dsn == "" {
		return errors.New("INVENTORY_DSN is required")
	}

	shutdownTelemetry, err := otelx.Setup(ctx, serviceName, logger)
	if err != nil {
		return err
	}
	// 关闭用不跟着停止信号取消的 ctx：收到 SIGTERM 之后还要把最后一批指标推出去。
	defer otelx.Shutdown(context.WithoutCancel(ctx), shutdownTelemetry, logger)

	connectCtx, cancel := context.WithTimeout(ctx, watchConnectTimeout)
	defer cancel()
	// 同一时刻只有一个快照事务在跑，连接给两个就够。
	db, err := mysqlx.Open(connectCtx, mysqlx.Config{DSN: dsn, MaxOpenConns: 2})
	if err != nil {
		return fmt.Errorf("connect inventory: %w", err)
	}
	defer db.Close() //nolint:errcheck // 进程退出时关闭，失败没有可做的事
	cfg.DB = db

	w, err := stock.NewWatcher(cfg)
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, "watching stock counters against the reservation ledger", "interval", cfg.Interval.String(),
		"full_every", cfg.FullEvery.String(), "reserved_window", cfg.ReservedWindow.String(), "stuck_after", cfg.StuckAfter.String())
	return w.Run(ctx)
}
