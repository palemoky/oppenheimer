package main

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/palemoky/oppenheimer/api/eventx"
	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/services/recon/internal/changes"
)

// watchChangesCommand 是跟着 CDC 的变更核对派生数据的常驻子命令：
//
//	recon watch-changes [-delay 10s] [-recheck-every 5s] [-lag-after 2m] [-check-timeout 5s]
//
// 它用自己的消费组读订单的行变更（和商家索引的消费者读同一个 topic，互不影响位点），每条变更提交满 delay 之后
// 去派生端按主键读那一行，用行的版本号判定（internal/changes 的包注释写了规则），结果报成指标，
// 不一致和落后各打一条日志。
//
// 环境变量：
//
//	KAFKA_BROKERS        默认 127.0.0.1:9092
//	KAFKA_PREFIX         topic 和消费组的环境前缀，与别的服务相同
//	MYSQL_REPLICA_DSNS   订单库的从库，逗号分隔，个数和顺序与 order 服务的 MYSQL_DSNS 一致；配了才核对从库
//	MERCHANT_INDEX_DSNS  商家索引库，与 orderindex 相同；配了才核对商家索引
//	ARCHIVE_DSN          冷库；配了才核对归档
//	HTTP_ADDR            内部端口（/healthz、/degrade），默认 :8086
//	DEGRADE              开关的初始值，例如 changes_check=off
//
// 三个库至少配一个。还有 OpenTelemetry 的那几个。
const watchChangesCommand = "watch-changes"

// switchChangesCheck 是核对的开关。关掉时只消费、不查库：位点照常往前走，recon.changes.verified_through 不动。
// 给过载时用：核对是每条变更几次主键点查，分摊在从库和索引库上。
const switchChangesCheck = "changes_check"

func watchChanges(logger *slog.Logger, args []string) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runWatchChanges(ctx, logger, args); err != nil {
		logger.ErrorContext(ctx, "watch-changes failed", "error", err)
		return exitError
	}
	return exitClean
}

func runWatchChanges(ctx context.Context, logger *slog.Logger, args []string) error {
	cfg := changes.Config{Logger: logger}
	fs := flag.NewFlagSet(watchChangesCommand, flag.ContinueOnError)
	fs.DurationVar(&cfg.Delay, "delay", 10*time.Second, "check a change this long after it was committed; only affects how many rechecks are needed")
	fs.DurationVar(&cfg.RecheckEvery, "recheck-every", 5*time.Second, "time between rechecks of changes the derived data had not caught up with")
	fs.DurationVar(&cfg.LagAfter, "lag-after", 2*time.Minute, "report a change as lagging when the derived data is still behind this long after the first check")
	fs.DurationVar(&cfg.CheckTimeout, "check-timeout", 5*time.Second, "time limit of checking one batch against one target")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	if cfg.Delay <= 0 || cfg.RecheckEvery <= 0 || cfg.LagAfter <= 0 || cfg.CheckTimeout <= 0 {
		return errors.New("all durations must be positive")
	}
	var brokers []string
	for b := range strings.SplitSeq(cmp.Or(os.Getenv("KAFKA_BROKERS"), "127.0.0.1:9092"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		return errors.New("KAFKA_BROKERS has no broker addresses")
	}
	prefix := os.Getenv("KAFKA_PREFIX")

	shutdownTelemetry, err := otelx.Setup(ctx, serviceName, logger)
	if err != nil {
		return err
	}
	// 关闭用不跟着停止信号取消的 ctx：收到 SIGTERM 之后还要把最后一批指标推出去。
	defer otelx.Shutdown(context.WithoutCancel(ctx), shutdownTelemetry, logger)

	connectCtx, cancel := context.WithTimeout(ctx, watchConnectTimeout)
	defer cancel()
	// 派生端和别的常驻核对不同：这里连不上不挡住启动的话，核对会在第一批上停住，效果一样，
	// 但启动时就失败能马上暴露写错的 DSN。库是必需的依赖，不是可选的。
	var closers []func()
	defer func() {
		for _, c := range closers {
			c()
		}
	}()
	open := func(name, dsn string) (*sql.DB, error) {
		if strings.TrimSpace(dsn) == "" {
			return nil, fmt.Errorf("%s has an empty DSN", name)
		}
		// 各分区的批和复查会同时查；点查很快，连接不用多。
		db, err := mysqlx.Open(connectCtx, mysqlx.Config{DSN: strings.TrimSpace(dsn), MaxOpenConns: 8})
		if err != nil {
			return nil, fmt.Errorf("connect %s: %w", name, err)
		}
		closers = append(closers, func() { _ = db.Close() })
		return db, nil
	}
	openAll := func(name string, dsns []string) ([]*sql.DB, error) {
		dbs := make([]*sql.DB, len(dsns))
		for i, dsn := range dsns {
			var err error
			if dbs[i], err = open(fmt.Sprintf("%s_%d", name, i), dsn); err != nil {
				return nil, err
			}
		}
		return dbs, nil
	}

	var names []string
	if dsns := splitDSNs(os.Getenv("MERCHANT_INDEX_DSNS")); len(dsns) > 0 {
		router, err := sharding.New(sharding.MerchantIndexLayout())
		if err != nil {
			return fmt.Errorf("merchant index shard layout: %w", err)
		}
		if router.DBs() != len(dsns) {
			return fmt.Errorf("MERCHANT_INDEX_DSNS has %d databases, the layout has %d", len(dsns), router.DBs())
		}
		dbs, err := openAll("merchant_index", dsns)
		if err != nil {
			return err
		}
		t, err := changes.NewIndexTarget(dbs, router)
		if err != nil {
			return err
		}
		cfg.Targets = append(cfg.Targets, t)
		names = append(names, t.Name())
	}
	if dsns := splitDSNs(os.Getenv("MYSQL_REPLICA_DSNS")); len(dsns) > 0 {
		router, err := sharding.New(sharding.OrderLayout(len(dsns)))
		if err != nil {
			return fmt.Errorf("order shard layout: %w", err)
		}
		dbs, err := openAll("order_replica", dsns)
		if err != nil {
			return err
		}
		t, err := changes.NewReplicaTarget(dbs, router)
		if err != nil {
			return err
		}
		cfg.Targets = append(cfg.Targets, t)
		names = append(names, t.Name())
	}
	if dsn := strings.TrimSpace(os.Getenv("ARCHIVE_DSN")); dsn != "" {
		db, err := open("archive", dsn)
		if err != nil {
			return err
		}
		t, err := changes.NewArchiveTarget(db)
		if err != nil {
			return err
		}
		cfg.Targets = append(cfg.Targets, t)
		names = append(names, t.Name())
	}
	if len(cfg.Targets) == 0 {
		return errors.New("at least one of MERCHANT_INDEX_DSNS, MYSQL_REPLICA_DSNS and ARCHIVE_DSN is required")
	}

	switches := degrade.New(serviceName, logger)
	sw := switches.Switch(switchChangesCheck, "row changes are consumed but derived data is not read; recon.changes.verified_through stops moving")
	if err := switches.Apply(os.Getenv(degrade.Env)); err != nil {
		return fmt.Errorf("apply %s: %w", degrade.Env, err)
	}
	cfg.Off = sw.Off

	checker, err := changes.New(cfg)
	if err != nil {
		return err
	}
	consumer, err := kafkax.NewBatchConsumer(connectCtx, kafkax.BatchConsumerConfig{
		Brokers:  brokers,
		ClientID: serviceName,
		Group:    eventx.Group(prefix, eventx.GroupReconCDCOrders),
		Topics:   []string{eventx.Topic(prefix, eventx.CDCOrders)},
		Logger:   logger,
		// 派生端读不到时停在那一批上等它恢复，不进死信：核对程序把消息送进死信没有意义，
		// 往前走则等于把“读不到”当成了“核对过”。解不开的消息在 handler 里已经跳过了，不会走到这里。
		MaxAttempts: math.MaxInt,
	}, checker.Handler())
	if err != nil {
		return err
	}

	mux := app.AdminMux(serviceName, nil)
	switches.Register(mux)
	httpServer, err := app.ListenHTTP(ctx, "http", cmp.Or(os.Getenv("HTTP_ADDR"), ":8086"), mux)
	if err != nil {
		return err
	}

	logger.InfoContext(ctx, "watching row changes against derived data", "targets", strings.Join(names, ","),
		"delay", cfg.Delay.String(), "recheck_every", cfg.RecheckEvery.String(), "lag_after", cfg.LagAfter.String())
	return app.Run(ctx, logger, httpServer, newRecheckLoop(checker), consumer)
}

// recheckLoop 把 Checker.Run（复查和瞬时值）包成 app.Component。
type recheckLoop struct {
	checker *changes.Checker
	stop    chan struct{} // Stop 关闭它
	done    chan struct{} // Start 返回时关闭
	once    sync.Once
}

func newRecheckLoop(checker *changes.Checker) *recheckLoop {
	return &recheckLoop{checker: checker, stop: make(chan struct{}), done: make(chan struct{})}
}

func (r *recheckLoop) Name() string { return "recheck" }

func (r *recheckLoop) Start(ctx context.Context) error {
	defer close(r.done)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-r.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	return r.checker.Run(ctx)
}

// Stop 可以在 Start 之前或者和它同时调用。Start 一直没有被调用时等到 ctx 到期。
func (r *recheckLoop) Stop(ctx context.Context) error {
	r.once.Do(func() { close(r.stop) })
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
