package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/redisx"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
	"github.com/palemoky/oppenheimer/test/load/internal/verify"
)

// sourceFlags 是 verify 和 wait-quiet 共用的数据源参数。
type sourceFlags struct {
	inventoryDSN, orderDSNs, paymentDSN string
	redisAddr, redisPrefix              string
	kafkaBrokers, kafkaPrefix           string
	// merchantIndexDSNs 可选：给了才对账商家索引（verify 的第 20 条）。
	merchantIndexDSNs string
	// archiveDSN 可选：给了才对账冷库（verify 的第 21 条，并让第 20 条把冷库的订单算进订单侧）。只有 verify 注册这个参数。
	archiveDSN string
}

func (f *sourceFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.inventoryDSN, "inventory-dsn", "root:oppenheimer@tcp(127.0.0.1:3306)/inventory_db", "inventory_db DSN")
	fs.StringVar(&f.orderDSNs, "order-dsns", "root:oppenheimer@tcp(127.0.0.1:3306)/order_db_0,root:oppenheimer@tcp(127.0.0.1:3306)/order_db_1",
		"comma-separated order DB DSNs; the i-th is order_db_i")
	fs.StringVar(&f.paymentDSN, "payment-dsn", "root:oppenheimer@tcp(127.0.0.1:3306)/payment_db", "payment_db DSN")
	fs.StringVar(&f.redisAddr, "redis-addr", "127.0.0.1:6379", "stock gate redis address")
	fs.StringVar(&f.redisPrefix, "redis-prefix", "", "REDIS_PREFIX of the services")
	fs.StringVar(&f.kafkaBrokers, "kafka-brokers", "127.0.0.1:9092", "comma-separated kafka brokers")
	fs.StringVar(&f.kafkaPrefix, "kafka-prefix", "", "KAFKA_PREFIX of the services")
	fs.StringVar(&f.merchantIndexDSNs, "merchant-index-dsns", "",
		"optional, comma-separated merchant index DB DSNs, the i-th is merchant_index_db_i; when empty verify check 20 is skipped")
}

// open 连上全部数据源，返回的 close 关闭它们。
func (f *sourceFlags) open(ctx context.Context) (verify.Sources, func(), error) {
	var closers []func()
	closeAll := func() {
		for _, c := range slices.Backward(closers) {
			c()
		}
	}
	fail := func(err error) (verify.Sources, func(), error) {
		closeAll()
		return verify.Sources{}, func() {}, err
	}
	src := verify.Sources{RedisPrefix: f.redisPrefix, KafkaPrefix: f.kafkaPrefix}
	for _, d := range []struct {
		name string
		dsn  string
		dst  **sql.DB
	}{
		{"inventory", f.inventoryDSN, &src.InventoryDB},
		{"payment", f.paymentDSN, &src.PaymentDB},
	} {
		db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: d.dsn, MaxOpenConns: 4})
		if err != nil {
			return fail(fmt.Errorf("open %s db: %w", d.name, err))
		}
		closers = append(closers, func() { _ = db.Close() })
		*d.dst = db
	}
	for i, dsn := range splitList(f.orderDSNs) {
		db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn, MaxOpenConns: 4})
		if err != nil {
			return fail(fmt.Errorf("open order db %d: %w", i, err))
		}
		closers = append(closers, func() { _ = db.Close() })
		src.OrderDBs = append(src.OrderDBs, db)
	}
	if len(src.OrderDBs) == 0 {
		return fail(errors.New("-order-dsns is empty"))
	}
	for i, dsn := range splitList(f.merchantIndexDSNs) {
		db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn, MaxOpenConns: 4})
		if err != nil {
			return fail(fmt.Errorf("open merchant index db %d: %w", i, err))
		}
		closers = append(closers, func() { _ = db.Close() })
		src.MerchantIndexDBs = append(src.MerchantIndexDBs, db)
	}
	if f.archiveDSN != "" {
		db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: f.archiveDSN, MaxOpenConns: 4})
		if err != nil {
			return fail(fmt.Errorf("open archive db: %w", err))
		}
		closers = append(closers, func() { _ = db.Close() })
		src.ArchiveDB = db
	}
	rdb, err := redisx.Open(ctx, redisx.Config{Addr: f.redisAddr, ReadTimeout: 5 * time.Second})
	if err != nil {
		return fail(err)
	}
	closers = append(closers, func() { _ = rdb.Close() })
	src.Redis = rdb

	src.KafkaBrokers = splitList(f.kafkaBrokers)
	kc, err := kafkax.NewClient(ctx, kafkax.Config{Brokers: src.KafkaBrokers, ClientID: "load-verify"})
	if err != nil {
		return fail(err)
	}
	closers = append(closers, kc.Close)
	src.Kafka = kc
	return src, closeAll, nil
}

func splitList(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseInitial 解析 "1001=500,1002=500"。
func parseInitial(s string) (map[int64]int64, error) {
	out := map[int64]int64{}
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("%q is not sku=quantity", part)
		}
		sku, err1 := strconv.ParseInt(k, 10, 64)
		qty, err2 := strconv.ParseInt(v, 10, 64)
		if err1 != nil || err2 != nil || sku <= 0 || qty < 0 {
			return nil, fmt.Errorf("invalid %q", part)
		}
		if _, dup := out[sku]; dup {
			return nil, fmt.Errorf("sku %d listed twice", sku)
		}
		out[sku] = qty
	}
	if len(out) == 0 {
		return nil, errors.New("empty list")
	}
	return out, nil
}

func newLogger(stderr io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func parseFlags(fs *flag.FlagSet, args []string) (ok bool, code int) {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return false, 0
		}
		return false, 2
	}
	return true, 0
}

// ---------- verify ----------

func verifyCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		sf                          sourceFlags
		ledgerPath, initial, invLog string
		timeout                     time.Duration
		archiveSettled              bool
	)
	sf.register(fs)
	fs.StringVar(&sf.archiveDSN, "archive-dsn", "",
		"optional, order_archive_db (cold archive) DSN; when empty verify check 21 is skipped and check 20 compares the index with the hot dbs only")
	fs.BoolVar(&archiveSettled, "archive-settled", true,
		"the archive job has finished: an order id in both the hot and the cold db fails check 21; false only reports the count")
	fs.StringVar(&ledgerPath, "ledger", "tmp/load/ledger.jsonl", "client ledger written by run")
	fs.StringVar(&initial, "initial", "", "initial stock of every sku, e.g. 1001=500,1002=500 (required)")
	fs.StringVar(&invLog, "inventory-log", "", "inventory service log file, checked for gate corrections")
	fs.DurationVar(&timeout, "timeout", 2*time.Minute, "overall timeout")
	if ok, code := parseFlags(fs, args); !ok {
		return code
	}
	logger := newLogger(stderr)
	initStock, err := parseInitial(initial)
	if err != nil {
		fmt.Fprintf(stderr, "invalid -initial: %v\n", err)
		return 2
	}

	entries, err := readLedger(ledgerPath, logger)
	if err != nil {
		logger.Error("read ledger failed", "error", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	src, closeSrc, err := sf.open(ctx)
	if err != nil {
		logger.Error("connect failed", "error", err)
		return 1
	}
	defer closeSrc()
	src.InventoryLog = invLog

	snap, err := verify.Collect(ctx, src, initStock)
	if err != nil {
		logger.Error("collect failed", "error", err)
		return 1
	}
	res := verify.Check(verify.Input{Snapshot: snap, Ledger: entries, ArchiveInFlight: !archiveSettled})
	printResult(stdout, res, snap, len(entries), archiveSettled)
	if res.Failed() {
		return 1
	}
	return 0
}

// readLedger 读账本。进程被强杀时最后一行可能不完整，坏行记 warn 并跳过。
func readLedger(path string, logger *slog.Logger) ([]ledger.Entry, error) {
	f, err := os.Open(path) //nolint:gosec // 路径来自命令行参数
	if err != nil {
		return nil, fmt.Errorf("open ledger: %w", err)
	}
	defer f.Close()
	var out []ledger.Entry
	for e, err := range ledger.Read(f) {
		if err != nil {
			logger.Warn("skipping bad ledger line", "error", err)
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

func printResult(w io.Writer, res *verify.Result, snap *verify.Snapshot, ledgerLines int, archiveSettled bool) {
	c := res.Counts
	p := func(format string, args ...any) { fmt.Fprintf(w, format+"\n", args...) }
	p("== verify summary ==")
	p("ledger lines: %d", ledgerLines)
	p("orders in the order dbs: %d %v", len(snap.Orders), sortedCounts(c.OrdersByStatus))
	p("payments: %d", len(snap.Payments))
	p("accepted order ids (201/202): %d, in the order dbs: %d", c.Accepted, c.AcceptedInDB)
	p("[15] accepted, not in the order dbs, no hold (reaped): %d (of which the flow later got another order: %d)",
		c.Reaped, c.ReapedResubmitted)
	p("[16] unknown outcomes: %v; 5xx: %v", sortedCounts(c.Unknown), sortedCounts(c.HTTP5xx))
	p("submit gave up without an order id (some attempt unknown/5xx): %d, found in the order dbs by (user_id, key): %v",
		c.GaveUp, sortedCounts(c.GaveUpFound))
	p("orders whose idempotency key is not in the ledger: %d", c.OrdersWithoutFlow)
	p("gate reconciliation log: corrected %d, capped %d", c.Corrections, c.Capped)
	p("dead letter messages: %d", c.DLQ)
	for _, sku := range slices.Sorted(maps.Keys(snap.Stock)) {
		st, g := snap.Stock[sku], snap.Gate[sku]
		p("sku %d: initial %d available %d reserved %d sold %d | gate avail %d held %d limit %d",
			sku, snap.Initial[sku], st.Available, st.Reserved, st.Sold, g.Avail, g.Held, g.Limit)
	}
	var skipped []string
	if mi := c.MerchantIndex; mi != nil {
		p("[20] merchant index: %d orders with a merchant, %d index rows; missing %d, extra %d, mismatched %d, misplaced %d",
			mi.Orders, mi.Rows, mi.Missing, mi.Extra, mi.Mismatch, mi.Misplaced)
	} else {
		p("[20] merchant index: skipped (no -merchant-index-dsns)")
		skipped = append(skipped, "20")
	}
	if a := snap.Archive; a != nil {
		p("[21] cold archive: %d orders in the cold db (%d in the hot dbs); non-final %d, too new %d, also in the hot dbs %d, without items %d, "+
			"orphan items %d, orphan payment results %d; hot orphan items %d, hot orphan payment results %d (archive settled: %t)",
			a.ColdOrders, a.HotOrders, a.NonFinal.N, a.TooNew.N, a.Both.N, a.NoItems.N,
			a.ItemOrphans.N, a.PaymentOrphans.N, a.HotItemOrphans.N, a.HotPaymentOrphans.N, archiveSettled)
	} else {
		p("[21] cold archive: skipped (no -archive-dsn)")
		skipped = append(skipped, "21")
	}
	var pendingRows int64
	for _, ps := range snap.Pending {
		pendingRows += ps.Rows
	}
	p("[22] stock_pending: %d rows for %d skus not applied to sku_stock (the sku lines above are sku_stock plus these deltas)",
		pendingRows, len(snap.Pending))
	for _, n := range res.Notes {
		p("note: %s", n)
	}
	if !res.Failed() {
		if len(skipped) > 0 {
			p("RESULT: PASS (checks 1-%d, skipped %s)", verify.CheckStockPending, strings.Join(skipped, ", "))
		} else {
			p("RESULT: PASS (checks 1-%d)", verify.CheckStockPending)
		}
		return
	}
	p("== violations (%d) ==", len(res.Violations))
	for _, v := range res.Violations {
		p("%s", v)
	}
	p("RESULT: FAIL, failed checks %v", res.FailedChecks())
}

func sortedCounts[K ~string](m map[K]int) string {
	keys := slices.Sorted(maps.Keys(m))
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

// ---------- wait-quiet ----------

func waitQuietCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("wait-quiet", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		sf                sourceFlags
		timeout, interval time.Duration
	)
	sf.register(fs)
	fs.DurationVar(&timeout, "timeout", 2*time.Minute, "give up after this long")
	fs.DurationVar(&interval, "interval", time.Second, "probe interval")
	if ok, code := parseFlags(fs, args); !ok {
		return code
	}
	logger := newLogger(stderr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	octx, ocancel := context.WithTimeout(ctx, 30*time.Second)
	src, closeSrc, err := sf.open(octx)
	ocancel()
	if err != nil {
		logger.Error("connect failed", "error", err)
		return 1
	}
	defer closeSrc()

	elapsed, last, err := verify.WaitQuiet(ctx, src, timeout, interval, logger)
	if err == nil {
		fmt.Fprintf(stdout, "quiet after %s\n", elapsed.Round(100*time.Millisecond))
		return 0
	}
	fmt.Fprintf(stdout, "NOT QUIET after %s: %v\n", elapsed.Round(100*time.Millisecond), err)
	problems := last.Problems()
	if len(problems) == 0 {
		fmt.Fprintln(stdout, "  (no successful probe)")
	}
	for _, pr := range problems {
		fmt.Fprintf(stdout, "  - %s\n", pr)
	}
	return 1
}

// ---------- warmup ----------

// warmupCmd 调库存服务的 WarmUp 把 SKU 装载进闸门，并确认闸门里已经有了。
func warmupCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("warmup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		addr, skus, redisAddr, redisPrefix string
		timeout                            time.Duration
	)
	fs.StringVar(&addr, "inventory-addr", "127.0.0.1:9082", "inventory gRPC address")
	fs.StringVar(&skus, "skus", "", "comma-separated sku ids (required)")
	fs.StringVar(&redisAddr, "redis-addr", "127.0.0.1:6379", "stock gate redis address")
	fs.StringVar(&redisPrefix, "redis-prefix", "", "REDIS_PREFIX of the services")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "overall timeout")
	if ok, code := parseFlags(fs, args); !ok {
		return code
	}
	logger := newLogger(stderr)
	ids, err := parseIDs(skus)
	if err != nil {
		fmt.Fprintf(stderr, "invalid -skus: %v\n", err)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := warmup(ctx, addr, ids, redisAddr, redisPrefix); err != nil {
		logger.Error("warmup failed", "error", err)
		return 1
	}
	fmt.Fprintf(stdout, "gate loaded: %v\n", ids)
	return 0
}

func warmup(ctx context.Context, addr string, ids []int64, redisAddr, redisPrefix string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial inventory: %w", err)
	}
	defer conn.Close()
	resp, err := inventoryv1.NewInventoryServiceClient(conn).WarmUp(ctx, &inventoryv1.WarmUpRequest{SkuIds: ids})
	if err != nil {
		return fmt.Errorf("warm up: %w", err)
	}
	if m := resp.GetMissing(); len(m) > 0 {
		return fmt.Errorf("skus not in inventory: %v", m)
	}
	rdb, err := redisx.Open(ctx, redisx.Config{Addr: redisAddr})
	if err != nil {
		return err
	}
	defer rdb.Close()
	return checkLoaded(ctx, rdb, redisPrefix, ids)
}

func checkLoaded(ctx context.Context, rdb redis.UniversalClient, prefix string, ids []int64) error {
	gate := stockgate.New(rdb, prefix)
	for _, id := range ids {
		if _, ok, err := gate.Stock(ctx, id); err != nil {
			return fmt.Errorf("read gate sku %d: %w", id, err)
		} else if !ok {
			return fmt.Errorf("sku %d not loaded in the gate after warm up", id)
		}
	}
	return nil
}
