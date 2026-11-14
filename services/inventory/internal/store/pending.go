package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
)

// MaxApplyBatch 是 ApplyPending 一次最多取的增量行数。它们的 id 会进 IN 列表。
const MaxApplyBatch = 5000

// StuckSKU 是这一轮没能应用的 SKU 及原因。
type StuckSKU struct {
	SKUID int64
	// Rows 是这一轮取到的、属于它的增量行数，都留在表里。
	Rows int
	// Reason 是给人看的原因。
	Reason string
}

// ApplyResult 是一轮 ApplyPending 的结果。
type ApplyResult struct {
	// Fetched 是取到的增量行数；等于 limit 说明可能还有，调用方应当接着再来一轮。
	Fetched int
	// Applied 是已经合并进 sku_stock 并删除的行数。
	Applied int
	// SKUs 是被更新的 sku_stock 行数。Applied / SKUs 就是合并的倍数。
	SKUs int
	// Stuck 是真正对不上的 SKU，按 sku_id 升序：把它的全部未应用增量加上去之后仍有一个数为负
	// （闸门放行的数量超过了 MySQL 里有的），或者 sku_stock 里没有这一行。它们的增量原样留在表里，要人来处理。
	Stuck []StuckSKU
	// Deferred 是这一轮没有应用、但账其实是平的 SKU：取到的这部分增量先后不全，下一轮再试。见 ApplyPending。
	Deferred []StuckSKU
	// OldestAge 是取到的最老一行从写入到现在的时间，Fetched 为 0 时为 0。用来观察应用的延迟。
	OldestAge time.Duration
}

// ApplyPending 把 stock_pending 里最老的一批增量合并进 sku_stock：按 SKU 求和，每个 SKU 一条更新，
// 同一个事务里删掉这批增量。exclude 里的 SKU 这一轮不取（调用方用它绕开卡住的 SKU）。
//
// 这是 ADR-0018 的核心。它取代了预占、确认、释放里对 sku_stock 的逐笔条件更新：
// 热点行从“每个订单改一次”变成“每批改一次”。
//
// 恰好一次：更新 sku_stock 和删除增量在一个事务里，要么都生效要么都不生效。
// 两个应用者同时运行（多副本）也安全：后一个在前一个锁住的增量行上等待，等到时这些行已经被删掉，
// 它读到的是后面的行。用普通的 FOR UPDATE，不用 SKIP LOCKED：同一个订单的增量有先后（先预占 +n，
// 后释放 -n），跳过被锁的行会让后面的经常先被应用。不跳过也不能完全保证先后（见下面第 1 种情况），
// 只是让它变得少见；正确性不依赖先后，靠的是下面“被拒绝时重算”的处理。
//
// 两个应用者偶尔会互相死锁（一个拿着后面的行、回头要锁同一个 SKU 更早的行，另一个正相反），
// 数据库回滚其中一个并返回死锁错误，调用方重试即可。
//
// 为什么“取当前看得见的行、应用、删掉”，而不是记一个“已应用到哪个 id”的水位：自增 id 的分配顺序和
// 事务提交的顺序不一致，id 小的事务可能后提交，水位已经越过它，它就永远不会被应用。
//
// 隔离级别用 READ COMMITTED：REPEATABLE READ 下带 LIMIT 的加锁读在行数不足时会一直扫到索引末尾，
// 锁住末尾的间隙，而所有新增量都插在末尾，预占、确认、释放会被这个事务挡到它提交为止。
//
// 某个 SKU 这一批的合计会让 available、reserved 或 sold 变成负数时（CHECK 约束拒绝），有两种可能：
//
//  1. 取到的增量先后不全。按 id 顺序加锁扫描并不是一个瞬间的快照：扫描在某一行上等锁的时候，
//     一个更早分配了 id、更晚提交的事务会把行插在已经扫过的位置后面。于是这一批里有某个订单的释放
//     （-n 预占），却没有它的预占（+n 预占，id 更小，在游标后面）。账是平的，只是这一批不全。
//     这和“不能用自增 id 做水位”是同一个原因：id 的分配顺序不是提交顺序。
//  2. 真的对不上：库存闸门放行的数量超过了 MySQL 里有的，或者恒等式已经被破坏。
//
// 所以被拒绝时不直接下结论，先把这个 SKU 的**全部**未应用增量都锁住、合计后再试一次：
// 这一批里出现的释放，它的预占在这条语句开始之前就已经提交了，一定在里面。能应用就全部应用。
// 还是被拒绝时，用一次快照读算出它的真实值（sku_stock 加全部增量；普通的一致性读是真正的快照）：
// 有负数就是第 2 种，记入 Stuck；没有负数说明第二次加锁扫描又遇到了第 1 种情况，记入 Deferred，下一轮再来。
// 两种情况下这个 SKU 的增量都原样留在表里（保持先后），其他 SKU 照常。SKU 不存在时记入 Stuck。
func (s *Store) ApplyPending(ctx context.Context, limit int, exclude []int64, now time.Time) (ApplyResult, error) {
	if limit <= 0 || limit > MaxApplyBatch {
		return ApplyResult{}, fmt.Errorf("store: apply batch of %d, want 1..%d", limit, MaxApplyBatch)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ApplyResult{}, fmt.Errorf("apply pending: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			// 回滚失败通常是连接已断，服务端会自行回滚；不盖过原始错误。
			_ = tx.Rollback()
		}
	}()

	bySKU, oldest, fetched, err := lockPending(ctx, tx, limit, exclude)
	if err != nil {
		return ApplyResult{}, err
	}
	res := ApplyResult{Fetched: fetched}
	if res.Fetched == 0 {
		return res, nil
	}
	res.OldestAge = max(now.Sub(oldest), 0)

	// 按 sku_id 升序更新：两个应用者同时运行时以同样的顺序锁 sku_stock 的行，不会互相死锁。
	var applied []int64
	for _, sku := range slices.Sorted(maps.Keys(bySKU)) {
		d := bySKU[sku]
		ids, outcome, reason, err := applySKU(ctx, tx, sku, d)
		if err != nil {
			return ApplyResult{}, err
		}
		switch outcome {
		case skuStuck:
			res.Stuck = append(res.Stuck, StuckSKU{SKUID: sku, Rows: len(d.ids), Reason: reason})
		case skuDeferred:
			res.Deferred = append(res.Deferred, StuckSKU{SKUID: sku, Rows: len(d.ids), Reason: reason})
		case skuApplied:
			applied = append(applied, ids...)
			res.SKUs++
		}
	}

	// 一个 SKU 整个重读之后，要删的行数可能超过这一批的上限，分段删，每段的占位符数量有界。
	for chunk := range slices.Chunk(applied, MaxApplyBatch) {
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		r, err := tx.ExecContext(ctx, "DELETE FROM stock_pending WHERE id IN ("+placeholders(len(chunk))+")", args...) //nolint:gosec // 只拼接占位符
		if err != nil {
			return ApplyResult{}, fmt.Errorf("apply pending: delete: %w", err)
		}
		n, err := r.RowsAffected()
		if err != nil {
			return ApplyResult{}, fmt.Errorf("apply pending: rows affected: %w", err)
		}
		// 这些行在本事务里被锁着，一定都还在。删掉的行数对不上说明有别的路径在删这张表，
		// 那样增量可能被应用了却没删、或者没应用就被删了，宁可回滚报错。
		if int(n) != len(chunk) {
			return ApplyResult{}, fmt.Errorf("apply pending: deleted %d rows, locked %d", n, len(chunk))
		}
	}
	res.Applied = len(applied)
	if err := tx.Commit(); err != nil {
		return ApplyResult{}, fmt.Errorf("apply pending: commit: %w", err)
	}
	committed = true
	// 按 sku_id 升序处理的，Stuck 和 Deferred 本来就有序。
	return res, nil
}

// skuOutcome 是 applySKU 对一个 SKU 的结论。
type skuOutcome int

const (
	skuApplied skuOutcome = iota
	skuStuck
	skuDeferred
)

// applySKU 把一个 SKU 的增量加到 sku_stock 上。返回已经应用、可以删除的增量行 id。
// 逻辑见 ApplyPending 的说明：先用这一批的合计，被 CHECK 拒绝就改用这个 SKU 全部未应用增量的合计。
func applySKU(ctx context.Context, tx *sql.Tx, sku int64, batch *pendingDelta) (ids []int64, outcome skuOutcome, reason string, err error) {
	ok, exists, err := addToStock(ctx, tx, sku, batch)
	if err != nil {
		return nil, 0, "", err
	}
	if !exists {
		return nil, skuStuck, "sku_stock row does not exist", nil
	}
	if ok {
		return batch.ids, skuApplied, "", nil
	}

	// 这一批的合计加不上去。把这个 SKU 的全部未应用增量锁住再算一次。
	all, err := lockPendingOfSKU(ctx, tx, sku)
	if err != nil {
		return nil, 0, "", err
	}
	if ok, _, err = addToStock(ctx, tx, sku, all); err != nil {
		return nil, 0, "", err
	}
	if ok {
		return all.ids, skuApplied, "", nil
	}

	// 全部加上也不行。看真实值：普通的一致性读，是一个真正的快照，没有“游标后面”的问题。
	var available, reserved, sold int64
	if err := tx.QueryRowContext(ctx, `
		SELECT s.available + COALESCE(p.da, 0), s.reserved + COALESCE(p.dr, 0), s.sold + COALESCE(p.ds, 0)
		  FROM sku_stock s
		  LEFT JOIN (SELECT sku_id, SUM(d_available) AS da, SUM(d_reserved) AS dr, SUM(d_sold) AS ds
		               FROM stock_pending WHERE sku_id = ? GROUP BY sku_id) p ON p.sku_id = s.sku_id
		 WHERE s.sku_id = ?`, sku, sku).Scan(&available, &reserved, &sold); err != nil {
		return nil, 0, "", fmt.Errorf("apply pending: effective stock of sku %d: %w", sku, err)
	}
	if available < 0 || reserved < 0 || sold < 0 {
		return nil, skuStuck, fmt.Sprintf("stock with all pending deltas applied is negative: available %d, reserved %d, sold %d",
			available, reserved, sold), nil
	}
	return nil, skuDeferred, "deltas seen out of order, the stock itself balances", nil
}

// addToStock 把 d 的合计加到 sku_stock 的一行上。ok 为 false 表示被 CHECK 约束拒绝（某个数会变负），
// 这时只有这条语句被回滚，事务还在。exists 为 false 表示没有这一行。
func addToStock(ctx context.Context, tx *sql.Tx, sku int64, d *pendingDelta) (ok, exists bool, err error) {
	// 合计可能三项都是零（一批里同一个订单先预占后释放），也照常执行，这样“SKU 不存在”仍然能被发现。
	r, err := tx.ExecContext(ctx,
		`UPDATE sku_stock SET available = available + ?, reserved = reserved + ?, sold = sold + ? WHERE sku_id = ?`,
		d.available, d.reserved, d.sold, sku)
	if mysqlx.IsCheckViolation(err) {
		return false, true, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("apply pending: update sku_stock %d: %w", sku, err)
	}
	n, err := r.RowsAffected()
	if err != nil {
		return false, false, fmt.Errorf("apply pending: rows affected: %w", err)
	}
	if n > 0 {
		return true, true, nil
	}
	// 受影响行数是“实际修改的行数”（mysqlx.Open 固定），值没变时是 0，分不出“没变”和“不存在”。
	// 合计全为零时单独确认一次行是否存在；其余情况下 0 行就是不存在。
	if d.available != 0 || d.reserved != 0 || d.sold != 0 {
		return false, false, nil
	}
	var one int
	switch err := tx.QueryRowContext(ctx, `SELECT 1 FROM sku_stock WHERE sku_id = ?`, sku).Scan(&one); {
	case err == nil:
		return true, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, false, nil
	default:
		return false, false, fmt.Errorf("apply pending: select sku_stock %d: %w", sku, err)
	}
}

// lockPendingOfSKU 锁住一个 SKU 的全部未应用增量并合计。
func lockPendingOfSKU(ctx context.Context, tx *sql.Tx, sku int64) (*pendingDelta, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT id, d_available, d_reserved, d_sold FROM stock_pending WHERE sku_id = ? ORDER BY id FOR UPDATE`, sku)
	if err != nil {
		return nil, fmt.Errorf("apply pending: select deltas of sku %d: %w", sku, err)
	}
	defer rows.Close()
	d := &pendingDelta{}
	for rows.Next() {
		var id, da, dr, ds int64
		if err := rows.Scan(&id, &da, &dr, &ds); err != nil {
			return nil, fmt.Errorf("apply pending: scan: %w", err)
		}
		d.available += da
		d.reserved += dr
		d.sold += ds
		d.ids = append(d.ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("apply pending: iterate: %w", err)
	}
	return d, nil
}

// pendingDelta 是一批增量里属于一个 SKU 的部分：三个数的合计和这些行的 id。
type pendingDelta struct {
	available, reserved, sold int64
	ids                       []int64
}

// lockPending 按 id 顺序取最老的 limit 行增量并加行锁，按 SKU 合并。oldest 是其中最早的写入时间。
func lockPending(ctx context.Context, tx *sql.Tx, limit int, exclude []int64) (bySKU map[int64]*pendingDelta, oldest time.Time, fetched int, err error) {
	query := "SELECT id, sku_id, d_available, d_reserved, d_sold, created_at FROM stock_pending"
	args := make([]any, 0, len(exclude)+1)
	if len(exclude) > 0 {
		query += " WHERE sku_id NOT IN (" + placeholders(len(exclude)) + ")" //nolint:gosec // 只拼接占位符
		for _, id := range exclude {
			args = append(args, id)
		}
	}
	query += " ORDER BY id LIMIT ? FOR UPDATE"
	args = append(args, limit)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, time.Time{}, 0, fmt.Errorf("apply pending: select: %w", err)
	}
	defer rows.Close()

	bySKU = map[int64]*pendingDelta{}
	for rows.Next() {
		var (
			id, sku    int64
			da, dr, ds int64
			created    time.Time
		)
		if err := rows.Scan(&id, &sku, &da, &dr, &ds, &created); err != nil {
			return nil, time.Time{}, 0, fmt.Errorf("apply pending: scan: %w", err)
		}
		d := bySKU[sku]
		if d == nil {
			d = &pendingDelta{}
			bySKU[sku] = d
		}
		d.available += da
		d.reserved += dr
		d.sold += ds
		d.ids = append(d.ids, id)
		if oldest.IsZero() || created.Before(oldest) {
			oldest = created
		}
		fetched++
	}
	if err := rows.Err(); err != nil {
		return nil, time.Time{}, 0, fmt.Errorf("apply pending: iterate: %w", err)
	}
	return bySKU, oldest, fetched, nil
}

// PendingBacklog 返回 stock_pending 的行数和最老一行的写入时间（没有行时为零值）。给日志和监控用。
func (s *Store) PendingBacklog(ctx context.Context) (rows int64, oldest time.Time, err error) {
	var t sql.NullTime
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(created_at) FROM stock_pending`).Scan(&rows, &t); err != nil {
		return 0, time.Time{}, fmt.Errorf("select stock_pending backlog: %w", err)
	}
	return rows, t.Time, nil
}

// placeholders 返回 n 个用逗号分隔的 "?"。
func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}
