package totals

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// 下钻：合计只知道哪一分钟、哪一组差了多少，这里把那一分钟两边的每一笔读出来逐笔比，找出是哪几笔。
//
// 比的就是合计加起来的那两个集合（同一段支付单号、同样的分组），所以合计报了差异，这里一定找得到具体的支付单，
// 除非差异在两次读之间自己消失了。一段是一分钟的支付单，两边各一次按主键范围的读取。
//
// 逐笔的规则和合计的两条规则是同一件事，只是“封口”细到了每一笔：
//
//   - 支付一侧在某一组里的，订单一侧必须有同一组的记录（包含关系，不用等）。
//   - 订单一侧在某一组里的，支付一侧不在这一组：这张支付单还会变（没出结果，或者答复没到）时是在途，不算；
//     已经定了（出了结果并且已答复），或者支付库里根本没有这一笔，才算。
//   - 两边在同一组里，金额必须相等。
//
// 读的顺序和合计一样：先支付库、后订单库。

// FindingKind 是一笔差异的种类。
type FindingKind string

const (
	// FindingPaymentOnly 支付一侧在这一组里，订单一侧没有同一组的记录。
	FindingPaymentOnly FindingKind = "payment_only"
	// FindingOrderOnly 订单一侧在这一组里，支付一侧已经定了却不在这一组（或者没有这一笔）。
	FindingOrderOnly FindingKind = "order_only"
	// FindingAmount 两边在同一组里，金额不同。
	FindingAmount FindingKind = "amount"
)

// PaymentRow 是支付库里的一笔。
type PaymentRow struct {
	OrderID  int64
	Status   int64
	Notified bool
	Amount   int64
}

// OrderRow 是订单库里的一条支付结果。
type OrderRow struct {
	OrderID     int64
	Succeeded   bool
	Disposition int64
	Amount      int64
}

// Finding 是下钻找到的一笔对不上的支付。
type Finding struct {
	PaymentID int64
	Kind      FindingKind
	// Group 是在哪一组里对不上。两边各在一个不同的组里时（一边接受、一边退款）记的是支付一侧的那一组。
	Group Group
	// Payment、Order 是两边各自的那一行，没有时是 nil。
	Payment *PaymentRow
	Order   *OrderRow
}

// OrderID 返回这笔支付属于哪个订单，优先取支付一侧记的。
func (f Finding) OrderID() int64 {
	if f.Payment != nil {
		return f.Payment.OrderID
	}
	if f.Order != nil {
		return f.Order.OrderID
	}
	return 0
}

// Drill 逐笔比较起始时刻为 start 的那一段，返回对不上的支付，按支付单号排序。
func Drill(ctx context.Context, src Sources, start time.Time) ([]Finding, error) {
	if src.Payment == nil || len(src.Orders) == 0 || src.Router == nil {
		return nil, errors.New("totals: payment database, order databases and router are all required")
	}
	lo := idgen.LowerBound(start) / idsPerBucket * idsPerBucket
	hi := lo + idsPerBucket
	pay, err := paymentRows(ctx, src.Payment, lo, hi)
	if err != nil {
		return nil, err
	}
	ord := map[int64]OrderRow{}
	for i, db := range src.Orders {
		if err := orderRows(ctx, db, src.Router.SlotsOf(i), lo, hi, ord); err != nil {
			return nil, fmt.Errorf("totals: order database %d: %w", i, err)
		}
	}
	return diffRows(pay, ord), nil
}

// diffRows 逐笔套用规则。
func diffRows(pay map[int64]PaymentRow, ord map[int64]OrderRow) []Finding {
	ids := make([]int64, 0, len(pay)+len(ord))
	for id := range pay {
		ids = append(ids, id)
	}
	for id := range ord {
		if _, ok := pay[id]; !ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)

	var out []Finding
	for _, id := range ids {
		p, hasPay := pay[id]
		o, hasOrd := ord[id]
		f := Finding{PaymentID: id}
		if hasPay {
			f.Payment = &p
		}
		if hasOrd {
			f.Order = &o
		}
		pg, inPay := payGroup(p.Status, p.Notified) // 没有这一行时是零值，不属于任何一组
		og, inOrd := orderGroup(o.Succeeded, o.Disposition)
		switch {
		case inPay && (!inOrd || og != pg):
			f.Kind, f.Group = FindingPaymentOnly, pg
		case inOrd && !inPay:
			if hasPay && (p.Status == paymentPending || !p.Notified) {
				continue // 在途：订单已经处置，支付单还没出结果或者答复没到
			}
			f.Kind, f.Group = FindingOrderOnly, og
		case inPay && p.Amount != o.Amount: // 到这里 inOrd 且同组
			f.Kind, f.Group = FindingAmount, pg
		default:
			continue
		}
		out = append(out, f)
	}
	return out
}

func paymentRows(ctx context.Context, db *sql.DB, lo, hi int64) (map[int64]PaymentRow, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT payment_id, order_id, status, notified, amount FROM payments WHERE payment_id >= ? AND payment_id < ?`, lo, hi)
	if err != nil {
		return nil, fmt.Errorf("totals: query payments: %w", err)
	}
	defer rows.Close() //nolint:errcheck // 只读查询，关闭失败没有可做的事
	out := map[int64]PaymentRow{}
	for rows.Next() {
		var (
			id int64
			r  PaymentRow
		)
		if err := rows.Scan(&id, &r.OrderID, &r.Status, &r.Notified, &r.Amount); err != nil {
			return nil, fmt.Errorf("totals: scan payments: %w", err)
		}
		out[id] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("totals: query payments: %w", err)
	}
	return out, nil
}

// orderRows 把一个订单库上各个 slot 里这一段的支付结果读进 out。
func orderRows(ctx context.Context, db *sql.DB, slots []int, lo, hi int64, out map[int64]OrderRow) error {
	if len(slots) == 0 {
		return nil
	}
	var (
		parts []string
		args  []any
	)
	for _, slot := range slots {
		// 表名来自 slot 号（整数），不是外部输入；值全部走参数。
		parts = append(parts, "SELECT payment_id, order_id, succeeded, disposition, amount FROM "+
			sharding.Table("payment_results", slot)+" WHERE payment_id >= ? AND payment_id < ?")
		args = append(args, lo, hi)
	}
	rows, err := db.QueryContext(ctx, strings.Join(parts, " UNION ALL "), args...) //nolint:gosec // 拼进去的只有按 slot 号生成的表名
	if err != nil {
		return fmt.Errorf("query payment_results: %w", err)
	}
	defer rows.Close() //nolint:errcheck // 只读查询，关闭失败没有可做的事
	for rows.Next() {
		var (
			id int64
			r  OrderRow
		)
		if err := rows.Scan(&id, &r.OrderID, &r.Succeeded, &r.Disposition, &r.Amount); err != nil {
			return fmt.Errorf("scan payment_results: %w", err)
		}
		out[id] = r
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("query payment_results: %w", err)
	}
	return nil
}
