// Package totals 用“笔数和金额合计”核对钱：支付服务认为订单已经接受的支付，和订单服务记下的“已接受”的支付结果，
// 按支付单号分成一分钟一段，两边各算一个笔数和一个金额，逐段比（设计文档 3c 第 8 节“钱的笔数和金额”）。
//
// 它和 recon 的逐行对账是两种工具：这里每一轮只有几条聚合查询，可以每半分钟跑一次、把结果报成指标，
// 回答的是“钱现在对不对得上”；对不上时它只知道是哪一分钟、差几笔、差多少钱，是哪一笔要用逐行对账去找。
//
// # 为什么不需要“等一会儿再比”
//
// 跨库的两份数据不在一个事务里，总有一些在途。常见的做法是只比若干分钟之前的数据，靠时间盖住在途的量；
// 那样容差设短了误报，设长了发现得晚。这里用支付单上已有的两个状态把在途的部分精确地分出来：
//
//   - 支付单 status = SUCCEEDED 且 notified = 1：订单服务已经答复“接受”。订单服务是先在自己的事务里写下
//     payment_results（disposition = ACCEPTED），事务提交之后答复才发得出去（outbox），所以这样的支付单
//     在订单一侧一定已经有记录了。答复是“要求退款”时，支付单在同一条 UPDATE 里变成 REFUNDED 并标记 notified，
//     不会出现“SUCCEEDED、notified = 1、订单却没接受”的中间状态。
//   - 所以任何时刻都有：支付一侧“成功且已答复”的集合 ⊆ 订单一侧“已接受”的集合。多出来的那部分就是答复还在路上的。
//   - 一段里没有 PENDING 的、也没有 notified = 0 的支付单时，这一段不会再变（支付单号是按时间递增分配的，
//     旧的一段不会再进新的支付单），在途为零，两边必须相等。这样的段叫“封口”的。
//
// 由此有两条规则，都不带时间容差：
//
//  1. 封口的段：两边的笔数和金额必须相等。
//  2. 没封口的段：支付一侧不能比订单一侧多（笔数和金额都是）。
//
// 读的顺序是先支付库、后订单库：先读到的是较早的快照，包含关系对更晚的订单库快照只会更宽松，不会因为
// 两次查询之间又处理了几笔而误报。
//
// # 它发现不了的
//
// 合计相等不等于逐笔相等：同一段里一笔多 100 分、另一笔少 100 分，合计看不出来（笔数也相等）。
// 这是合计类对账固有的限制，靠逐行对账（recon 的 payment 这一对）兜底。它也只看支付和订单两个系统之间，
// 订单库内部“已接受的支付结果”和“订单是已支付”的一致由同一个事务保证，不在这里核对。
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

// BucketWidth 是一段的宽度。支付单号的高位是毫秒时间戳，按单号分段就是按支付单的创建时间分段。
const BucketWidth = time.Minute

// idsPerBucket 是一段跨过多少个单号：单号 DIV 它就是段号。
var idsPerBucket = idgen.LowerBound(idgen.Epoch.Add(BucketWidth))

// 与各自迁移文件里的注释、proto 的枚举数值一致（测试用真实的表结构核对）。
const (
	paymentPending   = 1 // payments.status
	paymentSucceeded = 2
	dispositionOK    = 1 // payment_results.disposition：ACCEPTED
)

// Sum 是一组支付的笔数和金额合计（分）。
type Sum struct {
	Count  int64
	Amount int64
}

func (s Sum) add(o Sum) Sum { return Sum{Count: s.Count + o.Count, Amount: s.Amount + o.Amount} }

// payBucket 是支付一侧的一段。
type payBucket struct {
	// Accepted 是成功且订单已答复的支付单。
	Accepted Sum
	// Open 是还会变的支付单数：PENDING 的，或者出了结果、订单还没答复的。为 0 时这一段封口。
	Open int64
}

// Sources 是要读的库。
type Sources struct {
	Payment *sql.DB
	// Orders 是订单库的主库，个数和顺序与 order 服务的 MYSQL_DSNS 一致。读主库：从库有复制延迟，
	// 会把“还没复制到”读成“订单一侧少了”。每一轮只有一条按主键范围的聚合，对主库的压力很小。
	Orders []*sql.DB
	Router *sharding.Router
}

// Violation 是一段没有满足规则。
type Violation struct {
	// Start 是这一段的起始时刻（支付单的创建时间落在 [Start, Start+BucketWidth)）。
	Start time.Time
	// Closed 为 true 时违反的是规则 1（封口的段两边不等），否则是规则 2（没封口的段支付一侧多了）。
	Closed  bool
	Payment Sum
	Order   Sum
}

// Result 是一轮核对的结果。
type Result struct {
	// 核对的范围：支付单创建于 [From, To)。
	From, To time.Time
	// 封口的段上两边各自的合计。没有差异时两者相等。
	ClosedPayment, ClosedOrder Sum
	// InFlight 是没封口的段上订单一侧比支付一侧多出的笔数：订单已经接受、答复还没到支付服务的。
	InFlight int64
	// OpenBuckets 是没封口的段数，OldestOpen 是其中最早的一段的起始时刻（没有时是零值）。
	// 一段长时间不封口，说明有支付单一直出不了结果，或者订单的答复一直到不了。
	OpenBuckets int
	OldestOpen  time.Time
	// MismatchCount、MismatchAmount 是各个违反规则的段上差的笔数和金额（取绝对值后相加）。
	// 两个数分开报：只有金额不等时笔数的差是 0。
	MismatchCount  int64
	MismatchAmount int64
	Violations     []Violation
}

// OK 报告这一轮有没有发现差异。
func (r Result) OK() bool { return len(r.Violations) == 0 }

// Window 返回 now 时要核对的单号范围 [lo, hi)，两端都落在段的边界上。
//
// 最新的 skip 不看：支付单号是各个支付实例用自己的时钟分配的，时钟差几百毫秒时，“当前这一段”里
// 还可能进新的支付单；刚创建的支付单本来也在途。skip 取一分钟足够，它只是让最新的一两段晚一点才被看到。
func Window(now time.Time, window, skip time.Duration) (lo, hi int64) {
	hiBucket := idgen.LowerBound(now.Add(-skip)) / idsPerBucket
	loBucket := max(hiBucket-int64(window/BucketWidth), 0)
	return loBucket * idsPerBucket, hiBucket * idsPerBucket
}

// Collect 读两边并核对单号在 [lo, hi) 里的支付。先读支付库，再读订单库（见包注释）。
func Collect(ctx context.Context, src Sources, lo, hi int64) (Result, error) {
	if src.Payment == nil || len(src.Orders) == 0 || src.Router == nil {
		return Result{}, errors.New("totals: payment database, order databases and router are all required")
	}
	if src.Router.DBs() != len(src.Orders) {
		return Result{}, fmt.Errorf("totals: router has %d databases, %d order databases given", src.Router.DBs(), len(src.Orders))
	}
	pay, err := paymentSide(ctx, src.Payment, lo, hi)
	if err != nil {
		return Result{}, err
	}
	ord := map[int64]Sum{}
	for i, db := range src.Orders {
		part, err := orderSide(ctx, db, src.Router.SlotsOf(i), lo, hi)
		if err != nil {
			return Result{}, fmt.Errorf("totals: order database %d: %w", i, err)
		}
		// 同一笔支付只属于一个订单、一个分片，各库的结果按段相加。
		for b, s := range part {
			ord[b] = ord[b].add(s)
		}
	}
	r := compare(pay, ord)
	r.From, r.To = idgen.Time(lo), idgen.Time(hi)
	return r, nil
}

// paymentSide 按段读支付库：成功且已答复的笔数和金额，以及还会变的支付单数。
//
// 按主键的范围扫，范围是最近的一个窗口；一条语句，不分批。
func paymentSide(ctx context.Context, db *sql.DB, lo, hi int64) (map[int64]payBucket, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT payment_id DIV ? AS b,
		        CAST(COALESCE(SUM(status = ? AND notified = 1), 0) AS SIGNED),
		        CAST(COALESCE(SUM(IF(status = ? AND notified = 1, amount, 0)), 0) AS SIGNED),
		        CAST(COALESCE(SUM(status = ? OR notified = 0), 0) AS SIGNED)
		   FROM payments
		  WHERE payment_id >= ? AND payment_id < ?
		  GROUP BY b`,
		idsPerBucket, paymentSucceeded, paymentSucceeded, paymentPending, lo, hi)
	if err != nil {
		return nil, fmt.Errorf("totals: query payments: %w", err)
	}
	defer rows.Close() //nolint:errcheck // 只读查询，关闭失败没有可做的事
	out := map[int64]payBucket{}
	for rows.Next() {
		var (
			b  int64
			pb payBucket
		)
		if err := rows.Scan(&b, &pb.Accepted.Count, &pb.Accepted.Amount, &pb.Open); err != nil {
			return nil, fmt.Errorf("totals: scan payments: %w", err)
		}
		out[b] = pb
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("totals: query payments: %w", err)
	}
	return out, nil
}

// orderSide 按段读一个订单库上各个 slot 的 payment_results：支付成功且订单接受了的笔数和金额。
//
// payment_results 的主键是 payment_id，每个 slot 一次按主键范围的扫描，合成一条语句。
func orderSide(ctx context.Context, db *sql.DB, slots []int, lo, hi int64) (map[int64]Sum, error) {
	if len(slots) == 0 {
		return nil, nil
	}
	var (
		parts []string
		args  []any
	)
	for _, slot := range slots {
		// 表名来自 slot 号（整数），不是外部输入；值全部走参数。
		parts = append(parts, "SELECT payment_id DIV ? AS b, COUNT(*) AS c, COALESCE(SUM(amount), 0) AS a FROM "+
			sharding.Table("payment_results", slot)+
			" WHERE payment_id >= ? AND payment_id < ? AND succeeded = 1 AND disposition = ? GROUP BY b")
		args = append(args, idsPerBucket, lo, hi, dispositionOK)
	}
	query := "SELECT b, CAST(SUM(c) AS SIGNED), CAST(SUM(a) AS SIGNED) FROM (" + //nolint:gosec // 拼进去的只有按 slot 号生成的表名
		strings.Join(parts, " UNION ALL ") + ") t GROUP BY b"
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query payment_results: %w", err)
	}
	defer rows.Close() //nolint:errcheck // 只读查询，关闭失败没有可做的事
	out := map[int64]Sum{}
	for rows.Next() {
		var (
			b int64
			s Sum
		)
		if err := rows.Scan(&b, &s.Count, &s.Amount); err != nil {
			return nil, fmt.Errorf("scan payment_results: %w", err)
		}
		out[b] = s
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query payment_results: %w", err)
	}
	return out, nil
}

// compare 逐段套用两条规则。两边任何一边有数的段都要看：支付库里一笔都没有、订单库里却有“已接受”的段，
// 按封口处理（支付一侧是 0），也就是订单接受了一笔支付服务没有的支付。
func compare(pay map[int64]payBucket, ord map[int64]Sum) Result {
	buckets := make([]int64, 0, len(pay)+len(ord))
	for b := range pay {
		buckets = append(buckets, b)
	}
	for b := range ord {
		if _, ok := pay[b]; !ok {
			buckets = append(buckets, b)
		}
	}
	slices.Sort(buckets)

	var r Result
	for _, b := range buckets {
		p, o := pay[b], ord[b]
		start := idgen.Time(b * idsPerBucket)
		if p.Open == 0 {
			r.ClosedPayment = r.ClosedPayment.add(p.Accepted)
			r.ClosedOrder = r.ClosedOrder.add(o)
			if p.Accepted != o {
				r.MismatchCount += abs(p.Accepted.Count - o.Count)
				r.MismatchAmount += abs(p.Accepted.Amount - o.Amount)
				r.Violations = append(r.Violations, Violation{Start: start, Closed: true, Payment: p.Accepted, Order: o})
			}
			continue
		}
		r.OpenBuckets++
		if r.OldestOpen.IsZero() {
			r.OldestOpen = start // buckets 是升序的，第一个没封口的就是最早的
		}
		// 规则 2：只算支付一侧多出来的部分。订单一侧多是正常的在途。
		overCount := max(p.Accepted.Count-o.Count, 0)
		overAmount := max(p.Accepted.Amount-o.Amount, 0)
		if overCount > 0 || overAmount > 0 {
			r.MismatchCount += overCount
			r.MismatchAmount += overAmount
			r.Violations = append(r.Violations, Violation{Start: start, Payment: p.Accepted, Order: o})
		}
		r.InFlight += max(o.Count-p.Accepted.Count, 0)
	}
	return r
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
