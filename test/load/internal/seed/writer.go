package seed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// ErrParamsMismatch 表示 seed_progress 里已有用另一组参数灌入的记录。
var ErrParamsMismatch = errors.New("seed: progress was recorded with different parameters")

// ErrTablesMissing 表示订单表不存在：表由 order 服务启动时的迁移创建，造数工具不建表。
var ErrTablesMissing = errors.New("seed: order tables missing")

// progressDDL 是进度表。每个订单库一张，每完成一个“slot 加一天”的块，在写入这个块的同一个事务里记一行。
const progressDDL = `CREATE TABLE IF NOT EXISTS seed_progress (
    slot        INT          NOT NULL,
    day         DATE         NOT NULL,
    orders      INT          NOT NULL COMMENT '这个块写入的订单数',
    items       INT          NOT NULL COMMENT '这个块写入的明细数',
    params      VARCHAR(512) NOT NULL COMMENT '参数指纹，见 seed.Params.Fingerprint',
    finished_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (slot, day)
) ENGINE = InnoDB`

// DefaultBatchOrders 是一个事务里的目标订单数。
const DefaultBatchOrders = 1000

// Config 配置一次造数。
type Config struct {
	// DBs 是订单主库，DBs[i] 是 order_db_i。
	DBs    []*sql.DB
	Params Params
	// Rate 是每秒写入的订单数上限，0 表示不限。
	Rate float64
	// BatchOrders 是一个事务里的目标订单数，默认 DefaultBatchOrders。
	// 事务总是包含整数个块：块比它大时一个事务就是一个块（分几条 INSERT），块比它小时攒几个块再提交。
	BatchOrders int
	// Progress 是打印进度的间隔，0 表示不打印。
	Progress time.Duration
	// Out 接收进度行，nil 表示丢弃。
	Out io.Writer

	// afterCommit 在每个事务提交之后调用，供测试在中途取消。
	afterCommit func()
}

// Result 是一次运行的结果。
type Result struct {
	// ChunksTotal 是全部块数（slot 数乘以天数）。
	ChunksTotal int
	// ChunksSkipped 是运行开始时已经完成、被跳过的块。
	ChunksSkipped int
	// ChunksDone 是本次运行完成的块。
	ChunksDone int
	// OrdersBefore 是运行开始前已经写入的订单数。
	OrdersBefore int64
	// Orders、Items 是本次运行写入的行数。
	Orders, Items int64
	Elapsed       time.Duration
	// Interrupted 表示收到取消、在块边界停下，没有全部完成。
	Interrupted bool
}

// Complete 报告全部块是否都已完成。
func (r *Result) Complete() bool { return r.ChunksSkipped+r.ChunksDone == r.ChunksTotal }

type chunkKey struct {
	slot int
	day  string // YYYY-MM-DD
}

// Run 执行造数，直到全部完成、ctx 被取消或出错。ctx 被取消时，正在写的事务照常提交，然后停下：
// 进度总是停在块边界上。返回的 Result 在出错和取消时也有效。
func Run(ctx context.Context, cfg Config) (*Result, error) {
	p := cfg.Params
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("seed: invalid params: %w", err)
	}
	if len(cfg.DBs) != p.DBs {
		return nil, fmt.Errorf("seed: got %d databases, params say %d", len(cfg.DBs), p.DBs)
	}
	router, err := sharding.New(sharding.OrderLayout(p.DBs))
	if err != nil {
		return nil, err
	}
	batch := cfg.BatchOrders
	if batch <= 0 {
		batch = DefaultBatchOrders
	}
	out := cfg.Out
	if out == nil {
		out = io.Discard
	}

	done := map[chunkKey]bool{}
	res := &Result{ChunksTotal: router.Slots() * p.Days()}
	fp := p.Fingerprint()
	for i, db := range cfg.DBs {
		if err := checkTables(ctx, db, i, router); err != nil {
			return nil, err
		}
		if _, err := db.ExecContext(ctx, progressDDL); err != nil {
			return nil, fmt.Errorf("seed: create seed_progress in db %d: %w", i, err)
		}
		n, err := loadProgress(ctx, db, i, fp, done)
		if err != nil {
			return nil, err
		}
		res.OrdersBefore += n
	}
	res.ChunksSkipped = len(done)

	var (
		orders, items, chunks atomic.Int64
		lim                   = newLimiter(cfg.Rate)
		start                 = time.Now()
	)
	// 某个 slot 出错时让其他 slot 也停下。stop 只在块边界生效，与外部取消一样。
	stopCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)

	var wg sync.WaitGroup
	errs := make([]error, router.Slots())
	for slot := range router.Slots() {
		w := &slotWriter{
			db: cfg.DBs[router.DB(slot)], slot: slot, p: &p, fp: fp, batch: batch, lim: lim,
			orders: &orders, items: &items, chunks: &chunks, afterCommit: cfg.afterCommit,
		}
		wg.Go(func() {
			if err := w.run(stopCtx, done); err != nil {
				errs[slot] = fmt.Errorf("slot %d: %w", slot, err)
				stop(err)
			}
		})
	}

	finished := make(chan struct{})
	var pwg sync.WaitGroup
	if cfg.Progress > 0 {
		pwg.Go(func() {
			t := time.NewTicker(cfg.Progress)
			defer t.Stop()
			for {
				select {
				case <-finished:
					return
				case <-t.C:
					printProgress(out, &p, res.OrdersBefore, orders.Load(), items.Load(), time.Since(start))
				}
			}
		})
	}
	wg.Wait()
	close(finished)
	pwg.Wait()

	res.Orders, res.Items, res.ChunksDone = orders.Load(), items.Load(), int(chunks.Load())
	res.Elapsed = time.Since(start)
	if err := errors.Join(errs...); err != nil {
		return res, err
	}
	res.Interrupted = !res.Complete()
	return res, nil
}

func printProgress(w io.Writer, p *Params, before, orders, items int64, elapsed time.Duration) {
	rate := float64(orders) / elapsed.Seconds()
	doneTotal := before + orders
	eta := "?"
	if rate > 0 {
		eta = (time.Duration(float64(p.Total-doneTotal)/rate) * time.Second).Round(time.Second).String()
	}
	fmt.Fprintf(w, "seed: %d/%d orders (%.1f%%), %d items this run, %.0f orders/s, eta %s\n",
		doneTotal, p.Total, 100*float64(doneTotal)/float64(p.Total), items, rate, eta)
}

// checkTables 先看 shard_meta：已有记录时确认它和 DSN 的顺序、个数相符；
// 再确认本库上每个 slot 的订单表和明细表都在，且已有 00002 加的列。
func checkTables(ctx context.Context, db *sql.DB, idx int, router *sharding.Router) error {
	var dbIndex, dbCount int
	err := db.QueryRowContext(ctx, "SELECT db_index, db_count FROM shard_meta WHERE id = 1").Scan(&dbIndex, &dbCount)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case isTableMissing(err): // 服务还没升到有 shard_meta 的版本
	case err != nil:
		return fmt.Errorf("seed: read shard_meta of db %d: %w", idx, err)
	case dbIndex != idx || dbCount != router.DBs():
		return fmt.Errorf("seed: db %d says it is db %d of %d, but %d dsns were given in this order; check the dsn list",
			idx, dbIndex, dbCount, router.DBs())
	}
	for _, slot := range router.SlotsOf(idx) {
		for _, t := range []string{sharding.Table("orders", slot), sharding.Table("order_items", slot)} {
			var n int
			if err := db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", t,
			).Scan(&n); err != nil {
				return fmt.Errorf("seed: check table %s in db %d: %w", t, idx, err)
			}
			if n == 0 {
				return fmt.Errorf("%w: table %s not found in db %d; start the order service once so it runs its migrations",
					ErrTablesMissing, t, idx)
			}
		}
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ?
			   AND column_name IN ('merchant_id', 'version')`, sharding.Table("orders", slot),
		).Scan(&n); err != nil {
			return fmt.Errorf("seed: check columns in db %d: %w", idx, err)
		}
		if n != 2 {
			return fmt.Errorf("%w: %s lacks merchant_id/version in db %d; start the order service once so it runs its migrations",
				ErrTablesMissing, sharding.Table("orders", slot), idx)
		}
	}
	return nil
}

// isTableMissing 判断 err 是否为 MySQL 的 1146（表不存在）。
func isTableMissing(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1146
}

// loadProgress 读本库的进度，把完成的块加进 done，返回已写入的订单数。
// 有任何一行的参数指纹与 fp 不同就拒绝：同一批库里不能混着两组参数的数据。
func loadProgress(ctx context.Context, db *sql.DB, idx int, fp string, done map[chunkKey]bool) (int64, error) {
	others, err := otherFingerprints(ctx, db, fp)
	if err != nil {
		return 0, fmt.Errorf("seed: read seed_progress of db %d: %w", idx, err)
	}
	if len(others) > 0 {
		return 0, fmt.Errorf("%w: db %d has rows from\n  %s\nthis run is\n  %s\n"+
			"rerun with the recorded parameters to resume, or clear the seeded orders and seed_progress first",
			ErrParamsMismatch, idx, strings.Join(others, "\n  "), fp)
	}

	rows, err := db.QueryContext(ctx, "SELECT slot, DATE_FORMAT(day, '%Y-%m-%d'), orders FROM seed_progress")
	if err != nil {
		return 0, fmt.Errorf("seed: read seed_progress of db %d: %w", idx, err)
	}
	defer rows.Close()
	var total int64
	for rows.Next() {
		var (
			k chunkKey
			n int64
		)
		if err := rows.Scan(&k.slot, &k.day, &n); err != nil {
			return 0, fmt.Errorf("seed: scan seed_progress: %w", err)
		}
		done[k] = true
		total += n
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("seed: read seed_progress of db %d: %w", idx, err)
	}
	return total, nil
}

// otherFingerprints 返回 seed_progress 里与 fp 不同的参数指纹。
func otherFingerprints(ctx context.Context, db *sql.DB, fp string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT DISTINCT params FROM seed_progress")
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	var others []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		if s != fp {
			others = append(others, s)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return others, nil
}

// slotWriter 按日期顺序写一个 slot 的全部块。每个 slot 一个，各自一个协程。
type slotWriter struct {
	db    *sql.DB
	slot  int
	p     *Params
	fp    string
	batch int
	lim   *limiter

	orders, items, chunks *atomic.Int64
	afterCommit           func()
}

type chunk struct {
	day    time.Time
	orders []Order
}

func (w *slotWriter) run(ctx context.Context, done map[chunkKey]bool) error {
	var (
		pending []chunk
		n       int
	)
	for d := range w.p.Days() {
		dayStart := w.p.DayStart(d)
		if done[chunkKey{w.slot, dayStart.Format(time.DateOnly)}] {
			continue
		}
		generated, err := w.p.GenerateChunk(w.slot, d)
		if err != nil {
			return err
		}
		pending = append(pending, chunk{day: dayStart, orders: generated})
		if n += len(generated); n >= w.batch {
			if stop, err := w.flush(ctx, pending); stop || err != nil {
				return err
			}
			pending, n = pending[:0], 0
		}
	}
	if len(pending) > 0 {
		_, err := w.flush(ctx, pending)
		return err
	}
	return nil
}

// flush 在一个事务里写入若干个完整的块和它们的进度行。ctx 已取消时什么都不写，返回 stop。
// 事务本身不受 ctx 取消的影响：开始了就写完提交，保证进度停在块边界。
func (w *slotWriter) flush(ctx context.Context, chunks []chunk) (stop bool, err error) {
	var nOrders, nItems int
	for _, c := range chunks {
		nOrders += len(c.orders)
		for _, o := range c.orders {
			nItems += len(o.Items)
		}
	}
	if err := w.lim.wait(ctx, nOrders); err != nil {
		return true, nil //nolint:nilerr // 取消是正常的停止
	}
	if ctx.Err() != nil {
		return true, nil
	}
	txCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	tx, err := w.db.BeginTx(txCtx, nil)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	var all []Order
	for _, c := range chunks {
		all = append(all, c.orders...)
	}
	for part := range slices.Chunk(all, w.batch) {
		if err := insertOrders(txCtx, tx, w.slot, part); err != nil {
			return false, err
		}
	}
	if err := insertProgress(txCtx, tx, w.slot, w.fp, chunks); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	w.orders.Add(int64(nOrders))
	w.items.Add(int64(nItems))
	w.chunks.Add(int64(len(chunks)))
	if w.afterCommit != nil {
		w.afterCommit()
	}
	return false, nil
}

const orderInsertColumns = `(order_id, user_id, merchant_id, version, status, total_amount, currency, idempotency_key, request_hash,
	payment_id, fail_reason, expire_at, paid_at, created_at, updated_at)`

func nullInt(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// insertOrders 用两条多行 INSERT 写入订单和它们的明细。表名只来自 sharding.Table。
func insertOrders(ctx context.Context, tx *sql.Tx, slot int, orders []Order) error {
	if len(orders) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO " + sharding.Table("orders", slot) + " " + orderInsertColumns + " VALUES ")
	args := make([]any, 0, len(orders)*15)
	nItems := 0
	for i, o := range orders {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		args = append(args, o.OrderID, o.UserID, nullInt(o.MerchantID), o.Version, o.Status, o.TotalAmount, o.Currency,
			o.IdempotencyKey, o.RequestHash, nullInt(o.PaymentID), nullString(o.FailReason),
			o.ExpireAt, nullTime(o.PaidAt), o.CreatedAt, o.UpdatedAt)
		nItems += len(o.Items)
	}
	if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
		return fmt.Errorf("insert orders into slot %d: %w", slot, err)
	}

	b.Reset()
	b.WriteString("INSERT INTO " + sharding.Table("order_items", slot) + " (order_id, sku_id, quantity, unit_price, created_at) VALUES ")
	args = make([]any, 0, nItems*5)
	first := true
	for _, o := range orders {
		for _, it := range o.Items {
			if !first {
				b.WriteString(", ")
			}
			first = false
			b.WriteString("(?, ?, ?, ?, ?)")
			args = append(args, o.OrderID, it.SKUID, it.Quantity, it.UnitPrice, o.CreatedAt)
		}
	}
	if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
		return fmt.Errorf("insert order items into slot %d: %w", slot, err)
	}
	return nil
}

func insertProgress(ctx context.Context, tx *sql.Tx, slot int, fp string, chunks []chunk) error {
	var b strings.Builder
	b.WriteString("INSERT INTO seed_progress (slot, day, orders, items, params) VALUES ")
	args := make([]any, 0, len(chunks)*5)
	for i, c := range chunks {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(?, ?, ?, ?, ?)")
		n := 0
		for _, o := range c.orders {
			n += len(o.Items)
		}
		args = append(args, slot, c.day.Format(time.DateOnly), len(c.orders), n, fp)
	}
	if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
		return fmt.Errorf("record progress of slot %d: %w", slot, err)
	}
	return nil
}

// limiter 把所有 slot 合计的写入速率限制在 rate 单/秒以内。零值和 rate <= 0 都不限速。
type limiter struct {
	mu       sync.Mutex
	interval time.Duration // 每单的间隔
	next     time.Time     // 下一单最早可以开始的时间
}

func newLimiter(rate float64) *limiter {
	if rate <= 0 {
		return &limiter{}
	}
	return &limiter{interval: time.Duration(float64(time.Second) / rate)}
}

// wait 为 n 单预约时间并等到预约的时刻。ctx 取消时返回错误。
func (l *limiter) wait(ctx context.Context, n int) error {
	if l.interval == 0 || n == 0 {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	at := l.next
	l.next = l.next.Add(time.Duration(n) * l.interval)
	l.mu.Unlock()

	d := time.Until(at)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
