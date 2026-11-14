// Command load 是订单系统的压测工具。
//
//	load run [flags]      对网关压测，输出指标和客户端账本
//	load verify [flags]       用账本和服务端数据对账（7.3.3 节）
//	load wait-quiet [flags]   等系统静止（7.3.2 节第 3 步）
//	load warmup [flags]       通过库存服务把 SKU 装载进闸门
//	load seed [flags]         直接向订单主库灌历史订单（1c 设计 5.3）
//	load seed-report [flags]  灌数之后量分片均匀度和查询延迟，只读（1c 设计 5.3）
//
// 设计见 docs/design/1b-high-concurrency.md 的 7.3.1 节。
package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
	"github.com/palemoky/oppenheimer/test/load/internal/runner"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

const usage = `usage: load <command> [flags]

commands:
  run      run the load test against the gateway
  verify       reconcile a ledger against server data (design 7.3.3)
  wait-quiet   wait until outboxes, consumer lag, unfinished orders and gate holds are all zero
  warmup       load skus into the stock gate through the inventory service
  seed         write historical orders straight into the order primaries (design 1c 5.3)
  seed-report  measure shard balance and query latency after seeding, read only

run "load <command> -h" for the flags of a command.
`

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "run":
		return runCmd(args[1:], stdout, stderr)
	case "verify":
		return verifyCmd(args[1:], stdout, stderr)
	case "wait-quiet":
		return waitQuietCmd(args[1:], stdout, stderr)
	case "warmup":
		return warmupCmd(args[1:], stdout, stderr)
	case "seed":
		return seedCmd(args[1:], stdout, stderr)
	case "seed-report":
		return seedReportCmd(args[1:], stdout, stderr)
	case "-h", "-help", "--help", "help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 2
	}
}

func runCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cfg        runner.Config
		skus       string
		quantity   int
		ledgerPath string
		jsonPath   string
		logLevel   string
	)
	fs.StringVar(&cfg.BaseURL, "addr", "http://127.0.0.1:8080", "gateway base URL")
	fs.IntVar(&cfg.Concurrency, "c", 50, "number of virtual users")
	fs.DurationVar(&cfg.Duration, "duration", 0, "stop starting new orders after this long (exclusive with -orders)")
	fs.Int64Var(&cfg.Orders, "orders", 0, "total number of orders to submit (exclusive with -duration)")
	fs.StringVar(&skus, "skus", "1001,1002", "comma-separated sku ids, one picked at random per order")
	fs.IntVar(&quantity, "qty", 1, "quantity per order")
	fs.Float64Var(&cfg.PayRatio, "pay", 0.5, "fraction of landed orders to pay")
	fs.Float64Var(&cfg.CancelRatio, "cancel", 0.2, "fraction of landed orders to cancel; the rest are left to time out")
	fs.DurationVar(&cfg.PollInterval, "poll-interval", 100*time.Millisecond, "interval between order queries")
	fs.DurationVar(&cfg.PollTimeout, "poll-timeout", 30*time.Second, "give up polling one order after this long (per stage)")
	fs.DurationVar(&cfg.RequestTimeout, "req-timeout", 5*time.Second, "timeout of a single HTTP request")
	fs.IntVar(&cfg.SubmitAttempts, "submit-attempts", 5, "max submit requests per order, retries reuse the idempotency key")
	fs.DurationVar(&cfg.RetryBackoff, "retry-backoff", 200*time.Millisecond, "wait between submit retries")
	fs.DurationVar(&cfg.RejectBackoff, "reject-backoff", 0, "wait after a rejected submit before the next order")
	fs.Int64Var(&cfg.Users, "users", 100000, "user ids are drawn from [1, users]")
	fs.Uint64Var(&cfg.Seed, "seed", 0, "random seed, 0 picks one")
	fs.StringVar(&cfg.RunID, "run-id", "", "idempotency key prefix (default derived from start time)")
	fs.DurationVar(&cfg.ProgressInterval, "progress", 5*time.Second, "progress log interval, 0 disables")
	fs.StringVar(&ledgerPath, "ledger", "tmp/load/ledger.jsonl", "client ledger output (JSON Lines)")
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

	ids, err := parseIDs(skus)
	if err != nil {
		fmt.Fprintf(stderr, "invalid -skus: %v\n", err)
		return 2
	}
	cfg.SKUs = ids
	if quantity < 1 || quantity > 1<<31-1 {
		fmt.Fprintln(stderr, "invalid -qty")
		return 2
	}
	cfg.Quantity = int32(quantity)
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "invalid flags: %v\n", err)
		return 2
	}

	if dir := filepath.Dir(ledgerPath); dir != "" {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			logger.Error("create ledger directory failed", "error", err)
			return 1
		}
	}
	led, err := ledger.Create(ledgerPath)
	if err != nil {
		logger.Error("create ledger failed", "error", err)
		return 1
	}

	// 第一次 Ctrl-C 让压测停下并把已有内容刷盘；stop 之后再按一次恢复默认行为，直接退出。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()

	report, runErr := runner.Run(ctx, cfg, led)
	if err := led.Close(); err != nil && runErr == nil {
		runErr = err
	}
	code := 0
	if report != nil {
		if err := report.WriteText(stdout); err != nil {
			logger.Error("print report failed", "error", err)
		}
		if jsonPath != "" {
			if err := writeJSON(jsonPath, report); err != nil {
				logger.Error("write json report failed", "error", err)
				code = 1
			}
		}
		if report.Interrupted {
			code = 130
		}
	}
	if runErr != nil {
		logger.Error("load run failed", "error", runErr)
		return 1
	}
	logger.Info("ledger written", "path", ledgerPath)
	return code
}

func parseIDs(s string) ([]int64, error) {
	var ids []int64
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("invalid id %q", part)
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, errors.New("empty list")
	}
	return ids, nil
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v, json.Deterministic(true))
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		return fmt.Errorf("write report %s: %w", path, err)
	}
	return nil
}
