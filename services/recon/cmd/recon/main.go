// Command recon 是对账工具：在系统运行中比对几份应当一致的数据，定位到不一致的行（设计文档 1d 第 7 节，ADR-0022）。
//
// 环境变量（只有被选中的对账对象用到的库才是必填的）：
//
//	MYSQL_DSNS           必填，订单库的主库，逗号分隔，与 order 服务相同
//	MYSQL_REPLICA_DSNS   可选，与 MYSQL_DSNS 一一对应的从库；配了就从从库读订单（replica 必填）
//	ARCHIVE_DSN          可选，冷库；配了各对的订单侧包含已归档的订单（archive 必填）
//	MERCHANT_INDEX_DSNS  index 必填，商家索引库，逗号分隔，与 orderindex 相同
//	PAYMENT_DSN          payment 必填
//	INVENTORY_DSN        stock 必填
//
// 报告的文本打到标准输出，日志（JSON）打到标准错误。
// 退出码：0 没有差异；1 有确认的差异（修复之后仍有的也算）；2 参数或运行错误。
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
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/pkg/merkle"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/services/recon/internal/recon"
)

const (
	serviceName = "recon"
	// connectTimeout 是连上全部库的时限。
	connectTimeout = 30 * time.Second

	exitClean = 0
	exitDiff  = 1
	exitError = 2
)

type options struct {
	pairs         []string
	pairsSet      bool
	settle        time.Duration
	since         time.Duration
	recheck       time.Duration
	maxRows       uint64
	mode          recon.Mode
	repair        bool
	snapshotOut   string
	against       string
	jsonPath      string
	maxExamples   int
	dbConcurrency int
	timeout       time.Duration
}

func main() {
	logger := logx.New(serviceName, os.Stderr, logx.LevelFromEnv())
	slog.SetDefault(logger)

	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		logger.Error("bad arguments", "error", err)
		os.Exit(exitError)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	if opts.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.timeout)
		defer cancel()
	}
	code, err := run(ctx, logger, opts, os.Stdout)
	stop()
	if err != nil {
		logger.Error("recon failed", "error", err)
		code = exitError
	}
	os.Exit(code) //nolint:gocritic // stop 已经调用；cancel 只是释放计时器，进程退出时无所谓
}

func parseFlags(args []string) (options, error) {
	var (
		o     options
		pairs string
		mode  string
	)
	fs := flag.NewFlagSet(serviceName, flag.ContinueOnError)
	fs.StringVar(&pairs, "pairs", "replica,index,payment,stock", "comma separated pairs to compare: "+strings.Join(recon.PairNames, ", ")+" (archive must be given explicitly)")
	fs.DurationVar(&o.settle, "settle", 30*time.Minute, "compare only orders created before now minus this; 0 means no upper bound")
	fs.DurationVar(&o.since, "since", 0, "incremental run: compare only orders created within this long before now, e.g. 26h; 0 means no lower bound (full run). Not for the archive pair")
	fs.DurationVar(&o.recheck, "recheck", recon.DefaultRecheck, "wait this long and re-read the differences; 0 skips the recheck")
	fs.Uint64Var(&o.maxRows, "max-rows", recon.DefaultMaxRows, "compare row by row the differing nodes with at most this many rows on each side")
	fs.StringVar(&mode, "mode", string(recon.ModeTree), "tree (compare digests first) or full (read both sides entirely, the baseline)")
	fs.BoolVar(&o.repair, "repair", false, "repair the confirmed differences of repairable comparisons (the merchant index)")
	fs.StringVar(&o.snapshotOut, "snapshot-out", "", "archive only: save the trees to this file instead of comparing")
	fs.StringVar(&o.against, "against", "", "archive only: compare against the trees saved in this file")
	fs.StringVar(&o.jsonPath, "json", "", "also write the report as JSON to this file")
	fs.IntVar(&o.maxExamples, "max-examples", recon.DefaultMaxExamples, "examples per class of difference in the report")
	fs.IntVar(&o.dbConcurrency, "db-concurrency", recon.DefaultDBConcurrency, "statements running at the same time on one database")
	fs.DurationVar(&o.timeout, "timeout", 0, "give up after this long; 0 means no limit (SIGINT always stops)")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "pairs" {
			o.pairsSet = true
		}
	})
	for p := range strings.SplitSeq(pairs, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		if !slices.Contains(recon.PairNames, p) {
			return o, fmt.Errorf("unknown pair %q", p)
		}
		if !slices.Contains(o.pairs, p) {
			o.pairs = append(o.pairs, p)
		}
	}
	o.mode = recon.Mode(mode)
	if o.mode != recon.ModeTree && o.mode != recon.ModeFull {
		return o, fmt.Errorf("unknown mode %q", mode)
	}
	switch {
	case o.settle < 0, o.recheck < 0, o.timeout < 0, o.since < 0:
		return o, errors.New("-settle, -since, -recheck and -timeout must not be negative")
	case o.since > 0 && o.since <= o.settle:
		return o, errors.New("-since must be longer than -settle, otherwise the window is empty")
	case o.since > 0 && (slices.Contains(o.pairs, recon.PairArchive) || o.against != "" || o.snapshotOut != ""):
		return o, errors.New("-since cannot be used with the archive pair: a saved tree covers everything up to its bound")
	case o.maxExamples <= 0, o.dbConcurrency <= 0, o.maxRows == 0:
		return o, errors.New("-max-examples, -db-concurrency and -max-rows must be positive")
	case o.snapshotOut != "" && o.against != "":
		return o, errors.New("-snapshot-out and -against cannot be used together")
	}
	if o.snapshotOut != "" || o.against != "" {
		if !o.pairsSet {
			o.pairs = []string{recon.PairArchive}
		}
		if !slices.Equal(o.pairs, []string{recon.PairArchive}) {
			return o, errors.New("-snapshot-out and -against apply only to -pairs archive")
		}
	} else if slices.Contains(o.pairs, recon.PairArchive) {
		return o, errors.New("pair archive needs -snapshot-out (before archiving) or -against (after)")
	}
	if len(o.pairs) == 0 {
		return o, errors.New("no pairs selected")
	}
	return o, nil
}

func run(ctx context.Context, logger *slog.Logger, o options, stdout io.Writer) (int, error) {
	// 快照文件先读：-against 时水位线取文件里记的那个。
	var (
		bound   int64
		against map[string]*merkle.Tree
	)
	now := time.Now()
	if o.settle > 0 {
		bound = idgen.LowerBound(now.Add(-o.settle))
	}
	var since int64
	if o.since > 0 {
		since = idgen.LowerBound(now.Add(-o.since))
	}
	if o.against != "" {
		f, err := os.Open(o.against)
		if err != nil {
			return exitError, fmt.Errorf("open snapshot: %w", err)
		}
		snap, trees, err := recon.ReadSnapshot(f)
		_ = f.Close()
		if err != nil {
			return exitError, err
		}
		bound, against = snap.Bound, trees
		logger.Info("loaded snapshot", "path", o.against, "created_at", snap.CreatedAt, "bound", bound, "trees", len(trees))
	}

	topo, closeAll, err := connect(ctx, o)
	defer closeAll()
	if err != nil {
		return exitError, err
	}
	var pairs []recon.Pair
	for _, name := range o.pairs {
		p, err := recon.Build(name, topo)
		if err != nil {
			return exitError, err
		}
		pairs = append(pairs, p)
	}
	lim := recon.NewLimiter(o.dbConcurrency)

	if o.snapshotOut != "" {
		return exitClean, snapshot(ctx, logger, pairs, bound, lim, o.snapshotOut, stdout)
	}

	var repairer *recon.Repairer
	if o.repair && slices.Contains(o.pairs, recon.PairIndex) {
		if repairer, err = recon.NewRepairer(topo); err != nil {
			return exitError, err
		}
	}
	report := &recon.Report{StartedAt: time.Now().UTC()}
	for _, p := range pairs {
		for _, c := range p.Comparisons {
			logger.Info("comparing", "comparison", c.Name)
			r, err := recon.Compare(ctx, c, recon.Options{
				Mode: o.mode, Since: since, Bound: bound, MaxRows: o.maxRows, Recheck: o.recheck, MaxExamples: o.maxExamples,
				Limiter: lim, Against: against, Repairer: repairer, Logger: logger,
			})
			if err != nil {
				return exitError, fmt.Errorf("compare %s: %w", c.Name, err)
			}
			report.Results = append(report.Results, r)
		}
	}
	report.FinishedAt = time.Now().UTC()
	if err := report.WriteText(stdout); err != nil {
		return exitError, fmt.Errorf("write report: %w", err)
	}
	if o.jsonPath != "" {
		if err := writeJSON(o.jsonPath, report); err != nil {
			return exitError, err
		}
	}
	if report.Outstanding() > 0 {
		return exitDiff, nil
	}
	return exitClean, nil
}

func snapshot(ctx context.Context, logger *slog.Logger, pairs []recon.Pair, bound int64, lim *recon.Limiter, path string, stdout io.Writer) error {
	snap := &recon.SnapshotFile{CreatedAt: time.Now().UTC(), Bound: bound}
	for _, p := range pairs {
		for _, c := range p.Comparisons {
			t, rep, err := recon.Snapshot(ctx, c, bound, lim)
			if err != nil {
				return fmt.Errorf("snapshot %s: %w", c.Name, err)
			}
			if err := snap.Add(c.Name, t); err != nil {
				return err
			}
			logger.Info("snapshot taken", "comparison", c.Name, "rows", rep.Rows, "statements", rep.Statements)
			fmt.Fprintf(stdout, "%s: rows=%d root=%s leaves=%d statements=%d\n", c.Name, rep.Rows, rep.Root, t.Len(), rep.Statements)
		}
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	if err := snap.Write(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}
	fmt.Fprintf(stdout, "snapshot written to %s (bound %d)\n", path, bound)
	return nil
}

func writeJSON(path string, r *recon.Report) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create json report: %w", err)
	}
	if err := json.MarshalWrite(f, r); err != nil {
		_ = f.Close()
		return fmt.Errorf("write json report: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close json report: %w", err)
	}
	return nil
}

// connect 只连被选中的对账对象用到的库。缺必填的库时留空，由 recon.Build 报错。
func connect(ctx context.Context, o options) (recon.Topology, func(), error) {
	var (
		topo recon.Topology
		dbs  []*recon.DB
	)
	closeAll := func() {
		for _, d := range dbs {
			_ = d.Conn.Close()
		}
	}
	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	// 每个库上同时最多 db-concurrency 条语句，再留两个给修复和复查之外的零星语句。
	maxConns := o.dbConcurrency + 2
	open := func(name, dsn string) (*recon.DB, error) {
		conn, err := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn, MaxOpenConns: maxConns})
		if err != nil {
			return nil, fmt.Errorf("connect %s: %w", name, err)
		}
		d := &recon.DB{Name: name, Conn: conn}
		dbs = append(dbs, d)
		return d, nil
	}
	openList := func(env, prefix string) ([]*recon.DB, error) {
		var out []*recon.DB
		for i, dsn := range splitDSNs(os.Getenv(env)) {
			if dsn == "" {
				return nil, fmt.Errorf("%s has an empty DSN", env)
			}
			d, err := open(fmt.Sprintf("%s_%d", prefix, i), dsn)
			if err != nil {
				return nil, err
			}
			out = append(out, d)
		}
		return out, nil
	}
	uses := func(p string) bool { return slices.Contains(o.pairs, p) }

	var err error
	if topo.OrderPrimaries, err = openList("MYSQL_DSNS", "order"); err != nil {
		return topo, closeAll, err
	}
	if len(topo.OrderPrimaries) == 0 {
		return topo, closeAll, errors.New("MYSQL_DSNS is required")
	}
	if topo.OrderReplicas, err = openList("MYSQL_REPLICA_DSNS", "order_replica"); err != nil {
		return topo, closeAll, err
	}
	if dsn := strings.TrimSpace(os.Getenv("ARCHIVE_DSN")); dsn != "" {
		if topo.Archive, err = open("archive", dsn); err != nil {
			return topo, closeAll, err
		}
	}
	if uses(recon.PairIndex) {
		if topo.MerchantIndex, err = openList("MERCHANT_INDEX_DSNS", "merchant_index"); err != nil {
			return topo, closeAll, err
		}
	}
	if dsn := strings.TrimSpace(os.Getenv("PAYMENT_DSN")); dsn != "" && uses(recon.PairPayment) {
		if topo.Payment, err = open("payment", dsn); err != nil {
			return topo, closeAll, err
		}
	}
	if dsn := strings.TrimSpace(os.Getenv("INVENTORY_DSN")); dsn != "" && uses(recon.PairStock) {
		if topo.Inventory, err = open("inventory", dsn); err != nil {
			return topo, closeAll, err
		}
	}
	return topo, closeAll, nil
}

// splitDSNs 按逗号拆分，空串返回 nil。
func splitDSNs(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	var out []string
	for d := range strings.SplitSeq(v, ",") {
		out = append(out, strings.TrimSpace(d))
	}
	return out
}
