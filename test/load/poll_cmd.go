package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/poller"
)

// pollCmd 只对最近受理的订单发查询（GET /api/v1/orders/{id}），用来在下单压力之外叠加读压力。
//
//	load poll -orders-from tmp/load/ledger.jsonl -c 64 -interval 10ms -duration 30s
func pollCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("poll", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cfg      poller.Config
		jsonPath string
		logLevel string
	)
	fs.StringVar(&cfg.BaseURL, "addr", "http://127.0.0.1:8080", "gateway base URL")
	fs.StringVar(&cfg.OrdersFrom, "orders-from", "", "orders to query: a ledger written by load run (followed while it grows), or lines of \"order_id user_id\" (required)")
	fs.IntVar(&cfg.Concurrency, "c", 50, "number of workers; each waits -interval between queries")
	fs.DurationVar(&cfg.Interval, "interval", 10*time.Millisecond, "wait between two queries of one worker (rate is about c/interval)")
	fs.DurationVar(&cfg.Duration, "duration", 30*time.Second, "how long to poll")
	fs.DurationVar(&cfg.RequestTimeout, "req-timeout", 5*time.Second, "timeout of a single HTTP request")
	fs.IntVar(&cfg.Recent, "recent", 2000, "query a random one of this many most recently accepted orders")
	fs.DurationVar(&cfg.Refresh, "refresh", time.Second, "how often to re-read -orders-from")
	fs.Uint64Var(&cfg.Seed, "seed", 0, "random seed, 0 picks one")
	fs.StringVar(&jsonPath, "json", "", "also write the report as JSON to this file")
	fs.StringVar(&logLevel, "log-level", "info", "log level: debug, info, warn, error")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		fmt.Fprintf(stderr, "invalid -log-level: %v\n", err)
		return 2
	}
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: level}))
	cfg.Logger = logger
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "invalid flags: %v\n", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := poller.Run(ctx, cfg)
	if err != nil {
		logger.Error("load poll failed", "error", err)
		return 1
	}
	code := 0
	if err := report.WriteText(stdout); err != nil {
		logger.Error("print report failed", "error", err)
	}
	if jsonPath != "" {
		if err := writeJSON(jsonPath, report); err != nil {
			logger.Error("write json report failed", "error", err)
			code = 1
		}
	}
	return code
}
