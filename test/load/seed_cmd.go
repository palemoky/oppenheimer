package main

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/test/load/internal/seed"
	"github.com/palemoky/oppenheimer/test/load/internal/seedreport"
)

// defaultSeedOrderDSNs 与 scripts/run-local.sh 里 order 用的主库一致（两个分片各自的主库端口）。
const defaultSeedOrderDSNs = "root:oppenheimer@tcp(127.0.0.1:3311)/order_db_0,root:oppenheimer@tcp(127.0.0.1:3321)/order_db_1"

// openDBs 按顺序打开逗号分隔的 DSN。interpolate 为 true 时在客户端拼参数：多行 INSERT 一次往返，
// 不用为每条不同长度的语句做一次服务端预处理。
func openDBs(ctx context.Context, list, what string, interpolate bool, maxConns int) ([]*sql.DB, func(), error) {
	var dbs []*sql.DB
	closeAll := func() {
		for _, db := range slices.Backward(dbs) {
			_ = db.Close()
		}
	}
	for i, dsn := range splitList(list) {
		if interpolate {
			cfg, err := mysql.ParseDSN(dsn)
			if err != nil {
				closeAll()
				return nil, nil, fmt.Errorf("parse %s dsn %d: %w", what, i, err)
			}
			cfg.InterpolateParams = true
			dsn = cfg.FormatDSN()
		}
		db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn, MaxOpenConns: maxConns})
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("open %s db %d: %w", what, i, err)
		}
		dbs = append(dbs, db)
	}
	return dbs, closeAll, nil
}

func marshalReport(rep *seedreport.Report) ([]byte, error) {
	b, err := json.Marshal(rep, json.Deterministic(true))
	if err != nil {
		return nil, fmt.Errorf("encode report: %w", err)
	}
	return append(b, '\n'), nil
}

func parseDate(s string) (time.Time, error) {
	t, err := time.ParseInLocation(time.DateOnly, s, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("want YYYY-MM-DD: %w", err)
	}
	return t, nil
}

// ---------- seed ----------

func seedCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		p                  seed.Params
		dsns, from, to     string
		rate               float64
		batch              int
		progress, deadline time.Duration
	)
	fs.StringVar(&dsns, "order-dsns", defaultSeedOrderDSNs, "comma-separated order primary DB DSNs; the i-th is order_db_i")
	fs.Int64Var(&p.Total, "orders", 5_000_000, "total number of orders")
	fs.Uint64Var(&p.Seed, "seed", 1, "global random seed")
	fs.StringVar(&from, "from", idgen.Epoch.Format(time.DateOnly), "first day (UTC, inclusive), not before idgen.Epoch")
	fs.StringVar(&to, "to", time.Now().UTC().Format(time.DateOnly),
		"last day (UTC, exclusive); defaults to today, so pass it explicitly when resuming on a later day")
	fs.Float64Var(&rate, "rate", 0, "max orders per second over all slots, 0 means unlimited")
	fs.Uint64Var(&p.Users, "users", 2_000_000, "number of users, drawn by zipf")
	fs.Float64Var(&p.UserS, "user-zipf-s", 1.05, "zipf s of users (> 1, larger is more skewed)")
	fs.Float64Var(&p.UserV, "user-zipf-v", 10, "zipf v of users (>= 1, larger flattens the head)")
	fs.Uint64Var(&p.Merchants, "merchants", 2_000, "number of merchants, drawn by zipf")
	fs.Float64Var(&p.MerchantS, "merchant-zipf-s", 1.3, "zipf s of merchants")
	fs.Float64Var(&p.MerchantV, "merchant-zipf-v", 1, "zipf v of merchants")
	fs.IntVar(&batch, "batch", seed.DefaultBatchOrders, "orders per transaction (a transaction always holds whole slot-day chunks)")
	fs.DurationVar(&progress, "progress", 5*time.Second, "progress interval, 0 disables")
	fs.DurationVar(&deadline, "timeout", 0, "stop at a chunk boundary after this long, 0 means no limit")
	if ok, code := parseFlags(fs, args); !ok {
		return code
	}
	logger := newLogger(stderr)
	var err error
	if p.From, err = parseDate(from); err != nil {
		fmt.Fprintf(stderr, "invalid -from: %v\n", err)
		return 2
	}
	if p.To, err = parseDate(to); err != nil {
		fmt.Fprintf(stderr, "invalid -to: %v\n", err)
		return 2
	}
	p.DBs = len(splitList(dsns))
	if err := p.Validate(); err != nil {
		fmt.Fprintf(stderr, "invalid flags: %v\n", err)
		return 2
	}

	// 第一次 Ctrl-C 让各 slot 写完手上的事务后停下；之后再按一次恢复默认行为，直接退出。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop()
	}()
	if deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, deadline)
		defer cancel()
	}

	octx, ocancel := context.WithTimeout(ctx, 30*time.Second)
	dbs, closeDBs, err := openDBs(octx, dsns, "order", true, 8)
	ocancel()
	if err != nil {
		logger.Error("connect failed", "error", err)
		return 1
	}
	defer closeDBs()

	fmt.Fprintf(stdout, "seed: %s\n", p.Fingerprint())
	res, err := seed.Run(ctx, seed.Config{DBs: dbs, Params: p, Rate: rate, BatchOrders: batch, Progress: progress, Out: stdout})
	if res != nil {
		rate := float64(res.Orders) / res.Elapsed.Seconds()
		fmt.Fprintf(stdout, "seed: wrote %d orders and %d items in %s (%.0f orders/s); chunks %d skipped + %d done of %d; %d orders were already there\n",
			res.Orders, res.Items, res.Elapsed.Round(time.Millisecond), rate,
			res.ChunksSkipped, res.ChunksDone, res.ChunksTotal, res.OrdersBefore)
	}
	switch {
	case errors.Is(err, seed.ErrParamsMismatch), errors.Is(err, seed.ErrTablesMissing):
		fmt.Fprintf(stderr, "%v\n", err)
		return 1
	case err != nil:
		logger.Error("seed failed", "error", err)
		return 1
	case res.Interrupted:
		fmt.Fprintln(stdout, "seed: interrupted at a chunk boundary; rerun with the same flags to resume")
		return 130
	}
	fmt.Fprintln(stdout, "seed: complete")
	return 0
}

// ---------- seed-report ----------

func seedReportCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("seed-report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cfg                seedreport.Config
		orderDSNs, idxDSNs string
		archiveDSN         string
		jsonPath           string
		timeout            time.Duration
	)
	fs.StringVar(&orderDSNs, "order-dsns", defaultSeedOrderDSNs, "comma-separated order DB DSNs; the i-th is order_db_i")
	fs.StringVar(&idxDSNs, "merchant-index-dsns", "",
		"optional, comma-separated merchant index DB DSNs, the i-th is merchant_index_db_i; when empty merchant stats and queries are skipped")
	fs.StringVar(&archiveDSN, "archive-dsn", "",
		"optional, order_archive_db (cold archive) DSN; when empty the report has no cold archive section")
	fs.IntVar(&cfg.Runs, "runs", 2000, "runs of each query class")
	fs.IntVar(&cfg.CompareRuns, "compare-runs", 20, "runs of each side of the offset vs cursor comparison")
	fs.IntVar(&cfg.DeepPage, "page", 500, "page number used as the deep page")
	fs.IntVar(&cfg.PageSize, "page-size", 20, "rows per page")
	fs.IntVar(&cfg.TopN, "top", 5, "number of heavy users and big merchants")
	fs.BoolVar(&cfg.Analyze, "analyze", false, "ANALYZE LOCAL TABLE every table first (including the cold tables) so the sizes are fresh; without it sizes are InnoDB persistent stats and may be stale")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "random seed for query parameters")
	fs.StringVar(&jsonPath, "json", "", "also write the report as JSON to this file (- for stdout instead of the text)")
	fs.DurationVar(&timeout, "timeout", 30*time.Minute, "overall timeout")
	if ok, code := parseFlags(fs, args); !ok {
		return code
	}
	if cfg.Runs < 1 || cfg.CompareRuns < 1 || cfg.DeepPage < 1 || cfg.PageSize < 1 || cfg.TopN < 1 {
		fmt.Fprintln(stderr, "invalid flags: -runs, -compare-runs, -page, -page-size and -top must be positive")
		return 2
	}
	logger := newLogger(stderr)
	cfg.Log = stderr

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var err error
	var closeOrder, closeIdx func()
	if cfg.OrderDBs, closeOrder, err = openDBs(ctx, orderDSNs, "order", false, 16); err != nil {
		logger.Error("connect failed", "error", err)
		return 1
	}
	defer closeOrder()
	if cfg.IndexDBs, closeIdx, err = openDBs(ctx, idxDSNs, "merchant index", false, 4); err != nil {
		logger.Error("connect failed", "error", err)
		return 1
	}
	defer closeIdx()
	archiveDBs, closeArchive, err := openDBs(ctx, archiveDSN, "archive", false, 4)
	if err != nil {
		logger.Error("connect failed", "error", err)
		return 1
	}
	defer closeArchive()
	if len(archiveDBs) > 1 {
		fmt.Fprintln(stderr, "-archive-dsn takes one DSN")
		return 2
	}
	if len(archiveDBs) == 1 {
		cfg.ArchiveDB = archiveDBs[0]
	}
	if len(cfg.OrderDBs) == 0 {
		fmt.Fprintln(stderr, "-order-dsns is empty")
		return 2
	}

	rep, err := seedreport.Generate(ctx, cfg)
	if err != nil {
		logger.Error("seed report failed", "error", err)
		return 1
	}
	if jsonPath == "-" {
		b, err := marshalReport(rep)
		if err != nil {
			logger.Error("encode report failed", "error", err)
			return 1
		}
		_, _ = stdout.Write(b)
		return 0
	}
	if err := rep.WriteText(stdout); err != nil {
		logger.Error("print report failed", "error", err)
		return 1
	}
	if jsonPath != "" {
		if err := writeJSON(jsonPath, rep); err != nil {
			logger.Error("write json report failed", "error", err)
			return 1
		}
	}
	return 0
}
