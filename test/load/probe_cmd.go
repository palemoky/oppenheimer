package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/test/load/internal/probe"
)

// probeServiceName 是探针在链路和指标里的服务名。
const probeServiceName = "probe"

// probeCmd 运行常驻的探针，直到收到 SIGINT 或 SIGTERM。指标经 OTLP 导出，地址读 OTEL_EXPORTER_OTLP_ENDPOINT，
// 没设时不导出、探针照常跑（只有日志）。
func probeCmd(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cfg      probe.Config
		skus     string
		logLevel string
	)
	fs.StringVar(&cfg.BaseURL, "addr", "http://127.0.0.1:8080", "gateway (ingress) base URL")
	fs.StringVar(&cfg.Host, "host", "", "Host header to send, empty keeps the one from -addr")
	fs.DurationVar(&cfg.Interval, "interval", time.Second, "interval between cycle starts (open loop: a slow cycle does not delay the next)")
	fs.IntVar(&cfg.MaxInFlight, "max-in-flight", 20, "max cycles in flight; a cycle due while the limit is reached is skipped and counted")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", 2*time.Second, "timeout of a single HTTP request")
	fs.DurationVar(&cfg.LandTimeout, "land-timeout", 30*time.Second, "give up waiting for an order to leave creating after this long")
	fs.DurationVar(&cfg.PayTimeout, "pay-timeout", 30*time.Second, "give up waiting for an order to become paid after this long")
	fs.IntVar(&cfg.CancelEvery, "cancel-every", 10, "every n-th cycle cancels instead of paying, 0 never cancels")
	fs.IntVar(&cfg.ListEvery, "list-every", 5, "every n-th cycle also lists the user's orders, 0 never lists")
	fs.StringVar(&skus, "skus", "", "skus used in turn: comma-separated ids and inclusive ranges, e.g. 900001-900010,900020 (required)")
	fs.Int64Var(&cfg.UserBase, "user-base", probe.DefaultUserBase, "first user id; users are [user-base, user-base+user-count), inside the load range [2^39, 2^40)")
	fs.Int64Var(&cfg.UserCount, "user-count", probe.DefaultUserCount, "number of user ids, used in turn")
	fs.DurationVar(&cfg.Drain, "drain", 10*time.Second, "on stop, max time to wait for cycles in flight")
	fs.DurationVar(&cfg.SummaryInterval, "summary", time.Minute, "interval of the summary log line, 0 disables")
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
	logger := logx.New(probeServiceName, stderr, logx.ParseLevel(logLevel))

	ids, err := parseIDRanges(skus)
	if err != nil {
		fmt.Fprintf(stderr, "invalid -skus: %v\n", err)
		return 2
	}
	cfg.SKUs = ids
	cfg.Logger = logger
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "invalid flags: %v\n", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdown, err := otelx.Setup(ctx, probeServiceName, logger)
	if err != nil {
		logger.ErrorContext(ctx, "telemetry setup failed", "error", err)
		return 1
	}
	// 最先注册、最后执行：停止时最后一次指标也要发出去。
	defer otelx.Shutdown(ctx, shutdown, logger)

	if err := probe.Run(ctx, cfg); err != nil {
		logger.ErrorContext(ctx, "probe failed", "error", err)
		return 1
	}
	return 0
}

// maxRangeIDs 是一个范围最多展开的个数，挡住写错的范围（少写一个连字符两边的位数）。
const maxRangeIDs = 100000

// parseIDRanges 在 parseIDs 的逗号分隔之外认 a-b（闭区间）。
func parseIDRanges(s string) ([]int64, error) {
	var ids []int64
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, ok := strings.Cut(part, "-")
		if !ok {
			one, err := parseIDs(part)
			if err != nil {
				return nil, err
			}
			ids = append(ids, one...)
			continue
		}
		a, errA := strconv.ParseInt(strings.TrimSpace(lo), 10, 64)
		b, errB := strconv.ParseInt(strings.TrimSpace(hi), 10, 64)
		if errA != nil || errB != nil || a <= 0 || b < a || b-a >= maxRangeIDs {
			return nil, fmt.Errorf("invalid range %q", part)
		}
		for id := a; id <= b; id++ {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil, errors.New("empty list")
	}
	return ids, nil
}
