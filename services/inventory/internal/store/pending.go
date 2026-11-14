package store

import (
	"cmp"
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
	// Stuck 是没能应用的 SKU，按 sku_id 升序。它们的增量原样留在表里。
	Stuck []StuckSKU
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
// 后释放 -n），跳过被锁的行会让后面的先应用，reserved 出现一瞬间的负数而撞上 CHECK 约束。
//
// 为什么“取当前看得见的行、应用、删掉”，而不是记一个“已应用到哪个 id”的水位：自增 id 的分配顺序和
// 事务提交的顺序不一致，id 小的事务可能后提交，水位已经越过它，它就永远不会被应用。
//
// 隔离级别用 READ COMMITTED：REPEATABLE READ 下带 LIMIT 的加锁读在行数不足时会一直扫到索引末尾，
// 锁住末尾的间隙，而所有新增量都插在末尾，预占、确认、释放会被这个事务挡到它提交为止。
//
// 某个 SKU 的更新会让 available、reserved 或 sold 变成负数时（CHECK 约束拒绝），不应用它：
// 这说明库存闸门放行的数量超过了 MySQL 里有的，或者恒等式已经被破坏，需要人来看。
// 它的全部增量原样留下（保持先后），其他 SKU 照常，并在结果里报告。SKU 不存在时同样处理。
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
		reason := ""
		// 三项之和为零的增量加起来可能三项都是零（一批里同一个订单先预占后释放），也照常执行：
		// 这样“SKU 不存在”仍然能被发现，而值没有变化的更新代价很小。
		r, err := tx.ExecContext(ctx,
			`UPDATE sku_stock SET available = available + ?, reserved = reserved + ?, sold = sold + ? WHERE sku_id = ?`,
			d.available, d.reserved, d.sold, sku)
		switch {
		case mysqlx.IsCheckViolation(err):
			// 语句失败只回滚这一条语句，事务还在，可以继续处理其他 SKU。
			reason = fmt.Sprintf("applying (available %+d, reserved %+d, sold %+d) would make a counter negative",
				d.available, d.reserved, d.sold)
		case err != nil:
			return ApplyResult{}, fmt.Errorf("apply pending: update sku_stock %d: %w", sku, err)
		default:
			// 受影响行数是“实际修改的行数”（mysqlx.Open 固定），值没变时是 0，分不出“没变”和“不存在”。
			// 增量全为零时单独确认一次行是否存在；其余情况下 0 行就是不存在。
			n, err := r.RowsAffected()
			if err != nil {
				return ApplyResult{}, fmt.Errorf("apply pending: rows affected: %w", err)
			}
			if n == 0 {
				exists := false
				if d.available == 0 && d.reserved == 0 && d.sold == 0 {
					var one int
					switch err := tx.QueryRowContext(ctx, `SELECT 1 FROM sku_stock WHERE sku_id = ?`, sku).Scan(&one); {
					case err == nil:
						exists = true
					case !errors.Is(err, sql.ErrNoRows):
						return ApplyResult{}, fmt.Errorf("apply pending: select sku_stock %d: %w", sku, err)
					}
				}
				if !exists {
					reason = "sku_stock row does not exist"
				}
			}
		}
		if reason != "" {
			res.Stuck = append(res.Stuck, StuckSKU{SKUID: sku, Rows: len(d.ids), Reason: reason})
			continue
		}
		applied = append(applied, d.ids...)
		res.SKUs++
	}

	if len(applied) > 0 {
		args := make([]any, len(applied))
		for i, id := range applied {
			args[i] = id
		}
		r, err := tx.ExecContext(ctx, "DELETE FROM stock_pending WHERE id IN ("+placeholders(len(applied))+")", args...) //nolint:gosec // 只拼接占位符
		if err != nil {
			return ApplyResult{}, fmt.Errorf("apply pending: delete: %w", err)
		}
		n, err := r.RowsAffected()
		if err != nil {
			return ApplyResult{}, fmt.Errorf("apply pending: rows affected: %w", err)
		}
		// 这些行在本事务里被锁着，一定都还在。删掉的行数对不上说明有别的路径在删这张表，
		// 那样增量可能被应用了却没删、或者没应用就被删了，宁可回滚报错。
		if int(n) != len(applied) {
			return ApplyResult{}, fmt.Errorf("apply pending: deleted %d rows, locked %d", n, len(applied))
		}
		res.Applied = len(applied)
	}
	if err := tx.Commit(); err != nil {
		return ApplyResult{}, fmt.Errorf("apply pending: commit: %w", err)
	}
	committed = true
	slices.SortFunc(res.Stuck, func(a, b StuckSKU) int { return cmp.Compare(a.SKUID, b.SKUID) })
	return res, nil
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
