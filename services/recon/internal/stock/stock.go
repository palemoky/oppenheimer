// Package stock 核对库存的计数对不对得上流水（设计文档 3e 的 4.2、ADR-0039）。
//
// 库存库里有两样东西说的是同一件事：
//
//   - 计数：sku_stock 的 reserved、sold，加上 stock_pending 里还没应用的增量；
//   - 流水：reservation（每单一行，RESERVED、CONFIRMED、RELEASED）和 reservation_item（每单每个 SKU 的数量）。
//
// 应当成立的等式，按 SKU：
//
//	预占  reserved（含没应用的增量） = 状态是 RESERVED 的预占的数量合计
//	已售  sold（含没应用的增量）     = 状态是 CONFIRMED 的预占的数量合计
//
// # 为什么不需要“等一会儿再比”
//
// 这些表在同一个库里，而且库存服务改预占的状态和记库存增量是在同一个事务里做的（Reserve 一个事务里插入
// 头记录、明细和增量；确认、释放在一个事务里改状态并记增量）。所以在一个一致性快照里读，等式两边看到的是
// 同一时刻，没有在途的量：相等就是相等，差一件也是真的差。这里不设容差，也不复查。
//
// # 它管什么，不管什么
//
// 总量守恒（available + reserved + sold = stocked）由 sku_stock 的 CHECK 保证，不在这里查。这里查的是另一类
// 错：增量每一行自己是平的（三个数之和为 0），所以“同一条增量被应用了两次”“丢了一条增量”“改了状态没记增量”
// 都不破坏总量，只会让计数和流水对不上。
//
// 它发现不了流水本身记错了（预占的数量记成了别的数）：那要和订单的明细比，是逐行对账 stock 那一对的事。
// 闸门（Redis）和 MySQL 之间由库存服务自己的 reconcile 管。
//
// # 两种轮次
//
// RESERVED 的预占都是新的：订单在支付时限内要么付款（确认）要么关单（释放）。reservation 的主键是订单号，
// 订单号的高位是时间，所以“最近一段”是主键上的范围扫描。快的一轮只扫这一段，核对预占那条等式。
//
// 已售那条等式要对全部 CONFIRMED 求和，是一次全表扫描，流水只增不减，它随订单总量变慢。完整的一轮才做它，
// 顺带数出“比时限老得多、却还是 RESERVED”的预占：那是卡住了，没有被确认也没有被释放。快的一轮看不到
// 窗口之外的 RESERVED，有卡住的预占时它会报出预占那条等式的差值，完整的一轮说明原因。
package stock

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
)

// 与 services/inventory 的迁移文件里的注释一致（测试用真实的表结构核对）。
const (
	statusReserved  = 1
	statusConfirmed = 2
)

// 等式的名字，指标的标签。
const (
	CheckReserved = "reserved"
	CheckSold     = "sold"
)

// Diff 是一个 SKU 上对不上的一条等式。
type Diff struct {
	SKU int64
	// Counter 是计数一侧（sku_stock 加没应用的增量），Ledger 是流水一侧。
	Counter int64
	Ledger  int64
	// NoStockRow 表示流水里有这个 SKU 而 sku_stock 里没有这一行。
	NoStockRow bool
}

// Result 是一轮的结果。
type Result struct {
	// At 是这一轮开始的时刻。
	At time.Time
	// Full 表示这是完整的一轮：Sold、Stuck、OldestStuck 只在完整的一轮里有意义。
	Full bool
	// SKUs 是 sku_stock 的行数。
	SKUs int
	// Reserved 是预占那条等式对不上的 SKU，按 SKU 排序。
	Reserved []Diff
	// Sold 是已售那条等式对不上的 SKU。
	Sold []Diff
	// Stuck 是比 StuckAfter 更老、仍是 RESERVED 的预占的个数，OldestStuck 是其中最老的订单号。
	Stuck       int64
	OldestStuck int64
}

// Quantity 返回一组差异的数量的绝对值之和。
func Quantity(diffs []Diff) int64 {
	var n int64
	for _, d := range diffs {
		if d.Counter > d.Ledger {
			n += d.Counter - d.Ledger
		} else {
			n += d.Ledger - d.Counter
		}
	}
	return n
}

// Options 是一轮的参数。
type Options struct {
	// Now 是这一轮的时刻。
	Now time.Time
	// ReservedWindow 是预占那条等式往回看多久。要比支付时限长得多：窗口之外的 RESERVED 当成卡住的。
	ReservedWindow time.Duration
	// Full 为真时做完整的一轮。
	Full bool
	// StuckAfter 是 RESERVED 的预占多老算卡住，只在完整的一轮里用。
	StuckAfter time.Duration
}

// Collect 在库存库的一个一致性快照里核对一轮。
//
// 快照用可重复读的只读事务：InnoDB 在事务的第一条读语句上建立读视图，之后的语句都看到那一刻的数据。
// 一致性读不加锁，不挡库存服务的写入；但事务开着的时候它之后的旧版本回收不了，所以调用方要给 ctx 设时限。
func Collect(ctx context.Context, db *sql.DB, opts Options) (Result, error) {
	res := Result{At: opts.Now, Full: opts.Full}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return res, fmt.Errorf("stock: begin snapshot: %w", err)
	}
	// 只读事务，提交和回滚没有区别；回滚不会因为 ctx 已经取消而留下事务。
	defer func() { _ = tx.Rollback() }()

	counters, err := readCounters(ctx, tx)
	if err != nil {
		return res, err
	}
	res.SKUs = len(counters)

	since := idgen.LowerBound(opts.Now.Add(-opts.ReservedWindow))
	recent, err := readLedger(ctx, tx, statusReserved, since)
	if err != nil {
		return res, err
	}
	res.Reserved = compare(counters, recent, func(c counter) int64 { return c.reserved })

	if opts.Full {
		sold, err := readLedger(ctx, tx, statusConfirmed, 0)
		if err != nil {
			return res, err
		}
		res.Sold = compare(counters, sold, func(c counter) int64 { return c.sold })

		var oldest sql.NullInt64
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*), MIN(order_id) FROM reservation WHERE status = ? AND order_id < ?`,
			statusReserved, idgen.LowerBound(opts.Now.Add(-opts.StuckAfter))).Scan(&res.Stuck, &oldest); err != nil {
			return res, fmt.Errorf("stock: count stuck reservations: %w", err)
		}
		res.OldestStuck = oldest.Int64
	}
	return res, nil
}

type counter struct{ reserved, sold int64 }

// readCounters 读每个 SKU 的计数，加上还没应用的增量。
func readCounters(ctx context.Context, tx *sql.Tx) (map[int64]counter, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT s.sku_id, s.reserved + COALESCE(p.d_reserved, 0), s.sold + COALESCE(p.d_sold, 0)
		  FROM sku_stock s
		  LEFT JOIN (SELECT sku_id, SUM(d_reserved) AS d_reserved, SUM(d_sold) AS d_sold
		               FROM stock_pending GROUP BY sku_id) p ON p.sku_id = s.sku_id`)
	if err != nil {
		return nil, fmt.Errorf("stock: read counters: %w", err)
	}
	defer rows.Close() //nolint:errcheck // 读完之后的关闭失败没有可做的事，rows.Err 已经查过
	out := make(map[int64]counter)
	for rows.Next() {
		var (
			sku int64
			c   counter
		)
		if err := rows.Scan(&sku, &c.reserved, &c.sold); err != nil {
			return nil, fmt.Errorf("stock: scan counters: %w", err)
		}
		out[sku] = c
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("stock: read counters: %w", err)
	}
	return out, nil
}

// readLedger 按 SKU 汇总某个状态的预占的数量。since 是订单号的下界，0 表示不设。
func readLedger(ctx context.Context, tx *sql.Tx, status int, since int64) (map[int64]int64, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT i.sku_id, SUM(i.quantity)
		  FROM reservation r
		  JOIN reservation_item i ON i.order_id = r.order_id
		 WHERE r.status = ? AND r.order_id >= ?
		 GROUP BY i.sku_id`, status, since)
	if err != nil {
		return nil, fmt.Errorf("stock: read ledger (status %d): %w", status, err)
	}
	defer rows.Close() //nolint:errcheck // 同上
	out := make(map[int64]int64)
	for rows.Next() {
		var sku, qty int64
		if err := rows.Scan(&sku, &qty); err != nil {
			return nil, fmt.Errorf("stock: scan ledger: %w", err)
		}
		out[sku] = qty
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("stock: read ledger (status %d): %w", status, err)
	}
	return out, nil
}

// compare 逐个 SKU 比计数和流水，返回对不上的，按 SKU 排序。
func compare(counters map[int64]counter, ledger map[int64]int64, pick func(counter) int64) []Diff {
	var diffs []Diff
	for sku, c := range counters {
		if got, want := pick(c), ledger[sku]; got != want {
			diffs = append(diffs, Diff{SKU: sku, Counter: got, Ledger: want})
		}
	}
	for sku, qty := range ledger {
		if _, ok := counters[sku]; !ok && qty != 0 {
			diffs = append(diffs, Diff{SKU: sku, Ledger: qty, NoStockRow: true})
		}
	}
	slices.SortFunc(diffs, func(a, b Diff) int { return cmp.Compare(a.SKU, b.SKU) })
	return diffs
}

// ErrConfig 表示参数不对。
var ErrConfig = errors.New("stock: invalid configuration")
