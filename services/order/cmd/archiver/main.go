// Command archiver 把热库里足够旧的终态订单搬到冷库，跑完就退出（设计文档 1c 第 6 节）。
//
// 环境变量：
//
//	MYSQL_DSNS          必填，订单库的 DSN，逗号分隔，第 i 个是 order_db_i（与 order 服务相同）
//	MYSQL_REPLICA_DSNS  可选，与 MYSQL_DSNS 一一对应的从库；配了就在每批删除后等从库追上
//	ARCHIVE_DSN         必填，冷库
//
// 报告的摘要打到标准输出，日志（JSON）打到标准错误。出错或被中断时退出码非 0。
package main

import (
	"context"
	"database/sql"
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

	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/order/internal/archive"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

const (
	serviceName = "archiver"
	// startTimeout 是连库、核对 shard_meta、冷库迁移的总时限。
	startTimeout = time.Minute
	// maxOpenConns：每个库同时只有一个 slot 在跑，一批里的语句是串行的，用不了几个连接。
	maxOpenConns = 4
)

type options struct {
	olderThan time.Duration
	batch     int
	rate      int
	dryRun    bool
	reclaim   bool
	jsonPath  string
}

type config struct {
	dsns        []string
	replicaDSNs []string
	archiveDSN  string
}

func main() {
	logger := logx.New(serviceName, os.Stderr, logx.LevelFromEnv())
	slog.SetDefault(logger)

	var opts options
	flag.DurationVar(&opts.olderThan, "older-than", archive.DefaultArchiveAge,
		"archive terminal orders accepted before now minus this (minimum "+archive.MinArchiveAge.String()+")")
	flag.IntVar(&opts.batch, "batch", archive.DefaultBatchSize, "orders per batch")
	flag.IntVar(&opts.rate, "rate", 0, "max orders per second per database, 0 means unlimited")
	flag.BoolVar(&opts.dryRun, "dry-run", false, "only count, do not move anything")
	flag.BoolVar(&opts.reclaim, "reclaim", false, "only rebuild the hot tables to reclaim space, do not archive")
	flag.StringVar(&opts.jsonPath, "json", "", "also write the report as JSON to this file")
	flag.Parse()

	// 收到停止信号时取消 ctx：归档做完手上这一批、空间回收做完手上这张表再退出，仍然输出报告。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger, opts); err != nil {
		logger.Error("archiver failed", "error", err)
		stop()
		os.Exit(1) //nolint:gocritic // stop 已经显式调用，没有其他需要执行的 defer
	}
}

func loadConfig() (config, error) {
	var cfg config
	cfg.dsns = splitDSNs(os.Getenv("MYSQL_DSNS"))
	if cfg.dsns == nil {
		return cfg, errors.New("MYSQL_DSNS is required")
	}
	if slices.Contains(cfg.dsns, "") {
		return cfg, errors.New("MYSQL_DSNS has an empty DSN")
	}
	if v := os.Getenv("MYSQL_REPLICA_DSNS"); v != "" {
		cfg.replicaDSNs = splitDSNs(v)
		if len(cfg.replicaDSNs) != len(cfg.dsns) || slices.Contains(cfg.replicaDSNs, "") {
			return cfg, fmt.Errorf("MYSQL_REPLICA_DSNS must have one non-empty DSN per entry of MYSQL_DSNS (%d), got %d",
				len(cfg.dsns), len(cfg.replicaDSNs))
		}
	}
	cfg.archiveDSN = strings.TrimSpace(os.Getenv("ARCHIVE_DSN"))
	if cfg.archiveDSN == "" {
		return cfg, errors.New("ARCHIVE_DSN is required")
	}
	return cfg, nil
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

func run(ctx context.Context, logger *slog.Logger, opts options) error {
	if opts.dryRun && opts.reclaim {
		return errors.New("-dry-run and -reclaim cannot be used together")
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	router, err := sharding.New(sharding.OrderLayout(len(cfg.dsns)))
	if err != nil {
		return fmt.Errorf("MYSQL_DSNS has %d DSNs: %w", len(cfg.dsns), err)
	}

	st, cold, closeAll, err := open(ctx, cfg, router)
	defer closeAll()
	if err != nil {
		return err
	}

	// 同一时刻只让一个归档任务动数据。两个任务同时跑不会丢订单（写冷是幂等的、只做加法，
	// 删热以锁内比对为前提），但其中一个读回冷库时会看到另一个写进去的更新内容，
	// 报 ErrColdMismatch 并停下，是一次误报。只数不搬的 -dry-run 不用互斥。
	if !opts.dryRun {
		release, err := cold.LockArchiver(ctx)
		if err != nil {
			return err
		}
		defer release()
	}

	a, err := archive.New(st, cold, archive.Config{
		OlderThan: opts.olderThan,
		BatchSize: opts.batch,
		Rate:      opts.rate,
		DryRun:    opts.dryRun,
		Logger:    logger,
	})
	if err != nil {
		return err
	}

	var (
		report any
		runErr error
	)
	if opts.reclaim {
		rep, err := a.Reclaim(ctx)
		printReclaim(os.Stdout, rep)
		report, runErr = rep, err
	} else {
		rep, err := a.Run(ctx)
		printReport(os.Stdout, rep)
		report, runErr = rep, err
	}
	if opts.jsonPath != "" {
		if err := writeJSON(opts.jsonPath, report); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}
	return runErr
}

// open 连上所有库并做启动检查。返回的 closeAll 总是可以调用，出错时也要调用。
func open(ctx context.Context, cfg config, router *sharding.Router) (*store.Store, *store.Cold, func(), error) {
	ctx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()
	var opened []*sql.DB
	closeAll := func() {
		for _, db := range opened {
			_ = db.Close()
		}
	}
	connect := func(name, dsn string) (*sql.DB, error) {
		db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn, MaxOpenConns: maxOpenConns})
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", name, err)
		}
		opened = append(opened, db)
		return db, nil
	}

	// 热库不跑迁移：表结构归 order 服务管，归档任务只在 shard_meta 核对通过后读写。
	dbs := make([]*sql.DB, router.DBs())
	for i, dsn := range cfg.dsns {
		db, err := connect(fmt.Sprintf("order db %d", i), dsn)
		if err != nil {
			return nil, nil, closeAll, err
		}
		dbs[i] = db
	}
	// 核对 DSN 列表里第 i 个库确实是 i 号库。DSN 写反时归档任务会去错误的库上找 slot 的表，
	// 表不存在只是报错；核对让它在动手之前就停下。
	if err := store.EnsureShardMeta(ctx, dbs, router); err != nil {
		return nil, nil, closeAll, err
	}
	var storeOpts []store.Option
	if cfg.replicaDSNs != nil {
		replicas := make([]*sql.DB, router.DBs())
		for i, dsn := range cfg.replicaDSNs {
			db, err := connect(fmt.Sprintf("order db %d replica", i), dsn)
			if err != nil {
				return nil, nil, closeAll, err
			}
			replicas[i] = db
		}
		storeOpts = append(storeOpts, store.WithReplicas(replicas))
	}
	st, err := store.New(dbs, router, storeOpts...)
	if err != nil {
		return nil, nil, closeAll, err
	}

	coldDB, err := connect("archive db", cfg.archiveDSN)
	if err != nil {
		return nil, nil, closeAll, err
	}
	// 冷库的迁移与 order 服务是同一套、同一把迁移锁，谁先启动谁建表。
	if err := mysqlx.MigrateTemplate(ctx, coldDB, order.ArchiveMigrations, order.ArchiveMigrationsDir,
		order.NewArchiveMigrationData()); err != nil {
		return nil, nil, closeAll, fmt.Errorf("migrate archive db: %w", err)
	}
	return st, store.NewCold(coldDB), closeAll, nil
}

func printReport(w io.Writer, rep *archive.Report) {
	if rep == nil {
		return
	}
	mode := "archive"
	if rep.DryRun {
		mode = "dry run"
	}
	fmt.Fprintf(w, "%s: cutoff %s (order_id < %d), %s\n", mode, rep.Cutoff.Format(time.RFC3339), rep.Below,
		time.Duration(rep.ElapsedMS)*time.Millisecond)
	for _, s := range rep.Slots {
		fmt.Fprintf(w, "  slot %d (db %d): %s, %s\n", s.Slot, s.DB, formatCounts(rep.DryRun, s.Counts),
			time.Duration(s.ElapsedMS)*time.Millisecond)
	}
	fmt.Fprintf(w, "  total: %s\n", formatCounts(rep.DryRun, rep.Total))
}

func formatCounts(dryRun bool, c archive.Counts) string {
	if dryRun {
		return fmt.Sprintf("archivable %d, non-terminal %d", c.Archivable, c.NonTerminal)
	}
	return fmt.Sprintf("scanned %d, archived %d, skipped %d, gone %d, non-terminal %d, batches %d",
		c.Scanned, c.Archived, c.Skipped, c.Gone, c.NonTerminal, c.Batches)
}

func printReclaim(w io.Writer, rep *archive.ReclaimReport) {
	if rep == nil {
		return
	}
	fmt.Fprintf(w, "reclaim: %d tables, %s\n", len(rep.Tables), time.Duration(rep.ElapsedMS)*time.Millisecond)
	for _, t := range rep.Tables {
		fmt.Fprintf(w, "  %s (db %d): %d -> %d bytes, %s\n", t.Table, t.DB, t.BeforeBytes, t.AfterBytes,
			time.Duration(t.ElapsedMS)*time.Millisecond)
	}
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil { //nolint:gosec // 报告不含敏感信息，路径来自操作者的命令行参数
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}
