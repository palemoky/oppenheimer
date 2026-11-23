package totals

import (
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/payment"
)

// ---------- compare：两条规则 ----------

func TestCompare(t *testing.T) {
	t.Parallel()
	// 段号随便取，只要互不相同、升序有意义。
	const b0, b1, b2 = 1000, 1001, 1002
	start := func(b int64) time.Time { return idgen.Time(b * idsPerBucket) }

	tests := []struct {
		name string
		pay  map[int64]payBucket
		ord  map[int64]Sum
		want Result
	}{
		{
			name: "封口的段两边相等",
			pay:  map[int64]payBucket{b0: {Accepted: Sum{3, 300}}, b1: {Accepted: Sum{2, 500}}},
			ord:  map[int64]Sum{b0: {3, 300}, b1: {2, 500}},
			want: Result{ClosedPayment: Sum{5, 800}, ClosedOrder: Sum{5, 800}},
		},
		{
			name: "两边都没有数据",
			want: Result{},
		},
		{
			// 支付服务认为订单接受了，订单库里没有这一笔：钱收了，订单没有记。
			name: "封口的段支付一侧多一笔",
			pay:  map[int64]payBucket{b0: {Accepted: Sum{3, 300}}},
			ord:  map[int64]Sum{b0: {2, 200}},
			want: Result{
				ClosedPayment: Sum{3, 300}, ClosedOrder: Sum{2, 200},
				MismatchCount: 1, MismatchAmount: 100,
				Violations: []Violation{{Start: start(b0), Closed: true, Payment: Sum{3, 300}, Order: Sum{2, 200}}},
			},
		},
		{
			// 订单记了“已接受”，支付一侧这笔却不是“成功且已答复”（例如那边已经退款了）。封口之后才算差异。
			name: "封口的段订单一侧多一笔",
			pay:  map[int64]payBucket{b0: {Accepted: Sum{2, 200}}},
			ord:  map[int64]Sum{b0: {3, 300}},
			want: Result{
				ClosedPayment: Sum{2, 200}, ClosedOrder: Sum{3, 300},
				MismatchCount: 1, MismatchAmount: 100,
				Violations: []Violation{{Start: start(b0), Closed: true, Payment: Sum{2, 200}, Order: Sum{3, 300}}},
			},
		},
		{
			// 笔数相等、金额不等：同一笔支付两边记的金额不一样。两个数分开报。
			name: "封口的段只有金额不等",
			pay:  map[int64]payBucket{b0: {Accepted: Sum{2, 200}}},
			ord:  map[int64]Sum{b0: {2, 250}},
			want: Result{
				ClosedPayment: Sum{2, 200}, ClosedOrder: Sum{2, 250},
				MismatchAmount: 50,
				Violations:     []Violation{{Start: start(b0), Closed: true, Payment: Sum{2, 200}, Order: Sum{2, 250}}},
			},
		},
		{
			// 订单已经接受、答复还没到支付服务：没封口，订单一侧多，是正常的在途，不是差异，也不进封口的合计。
			name: "没封口的段订单一侧多是在途",
			pay:  map[int64]payBucket{b0: {Accepted: Sum{1, 100}}, b1: {Accepted: Sum{1, 100}, Open: 2}},
			ord:  map[int64]Sum{b0: {1, 100}, b1: {3, 300}},
			want: Result{
				ClosedPayment: Sum{1, 100}, ClosedOrder: Sum{1, 100},
				InFlight: 2, OpenBuckets: 1, OldestOpen: start(b1),
			},
		},
		{
			// 没封口也不允许支付一侧多：“成功且已答复”的每一笔，订单一侧一定先记下了。
			name: "没封口的段支付一侧多",
			pay:  map[int64]payBucket{b1: {Accepted: Sum{3, 300}, Open: 1}},
			ord:  map[int64]Sum{b1: {2, 200}},
			want: Result{
				OpenBuckets: 1, OldestOpen: start(b1),
				MismatchCount: 1, MismatchAmount: 100,
				Violations: []Violation{{Start: start(b1), Payment: Sum{3, 300}, Order: Sum{2, 200}}},
			},
		},
		{
			// 支付库里这一段一笔都没有，订单库里却有“已接受”的：订单接受了一笔支付服务没有的支付。
			name: "只有订单一侧有的段按封口处理",
			pay:  map[int64]payBucket{b0: {Accepted: Sum{1, 100}}},
			ord:  map[int64]Sum{b0: {1, 100}, b2: {1, 999}},
			want: Result{
				ClosedPayment: Sum{1, 100}, ClosedOrder: Sum{2, 1099},
				MismatchCount: 1, MismatchAmount: 999,
				Violations: []Violation{{Start: start(b2), Closed: true, Payment: Sum{}, Order: Sum{1, 999}}},
			},
		},
		{
			// 只有 PENDING 的支付单（还没出结果）：没封口，两边都是 0，不是差异。
			name: "只有待出结果的支付单",
			pay:  map[int64]payBucket{b2: {Open: 4}, b1: {Open: 1}},
			want: Result{OpenBuckets: 2, OldestOpen: start(b1)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := compare(tt.pay, tt.ord)
			if !equalResult(got, tt.want) {
				t.Errorf("compare:\n got  %+v\n want %+v", got, tt.want)
			}
			if got.OK() != (len(tt.want.Violations) == 0) {
				t.Errorf("OK() = %v with %d violations", got.OK(), len(got.Violations))
			}
		})
	}
}

func equalResult(a, b Result) bool {
	if a.ClosedPayment != b.ClosedPayment || a.ClosedOrder != b.ClosedOrder || a.InFlight != b.InFlight ||
		a.OpenBuckets != b.OpenBuckets || !a.OldestOpen.Equal(b.OldestOpen) ||
		a.MismatchCount != b.MismatchCount || a.MismatchAmount != b.MismatchAmount ||
		len(a.Violations) != len(b.Violations) {
		return false
	}
	for i := range a.Violations {
		x, y := a.Violations[i], b.Violations[i]
		if !x.Start.Equal(y.Start) || x.Closed != y.Closed || x.Payment != y.Payment || x.Order != y.Order {
			return false
		}
	}
	return true
}

func TestWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 22, 12, 30, 45, 0, time.UTC)
	lo, hi := Window(now, 30*time.Minute, time.Minute)
	// 最新的一分钟不看：上界落在 12:29:00，窗口往回 30 段。
	if got, want := idgen.Time(hi), time.Date(2026, 11, 22, 12, 29, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("hi = %s, want %s", got, want)
	}
	if got, want := idgen.Time(lo), time.Date(2026, 11, 22, 11, 59, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("lo = %s, want %s", got, want)
	}
	if lo%idsPerBucket != 0 || hi%idsPerBucket != 0 {
		t.Errorf("window [%d, %d) is not aligned to buckets of %d ids", lo, hi, idsPerBucket)
	}
}

// ---------- Collect：对着真实的表结构 ----------

// env 是一套和真实环境同构的库：两个订单库（各有自己的 slot）加一个支付库，用各服务自己的迁移建表。
// 这里核对的是 SQL 和表结构、状态的数值对得上；规则本身在 TestCompare 里测。
type env struct {
	t      *testing.T
	src    Sources
	base   time.Time
	serial int64
}

func newEnv(t *testing.T) *env {
	t.Helper()
	r, err := sharding.New(sharding.OrderLayout(2))
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, base: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)}
	e.src.Router = r
	for i := range 2 {
		e.src.Orders = append(e.src.Orders, testdb.NewTemplate(t, order.Migrations, order.MigrationsDir, order.MigrationData{Slots: r.SlotsOf(i)}))
	}
	e.src.Payment = testdb.New(t, payment.Migrations, payment.MigrationsDir)
	return e
}

// 一笔支付在支付一侧的状态。
const (
	payPendingRow  = iota // 还没出结果
	payUnacked            // 成功，订单还没答复
	payAcceptedRow        // 成功，订单答复了接受
	payRefundedRow        // 订单要求退款，已退
	payFailedAcked        // 失败，订单已知悉
)

// 同一笔支付在订单一侧的记录。
const (
	orderNone        = iota // 没有记录
	orderAccepted           // 记了“接受”
	orderRefund             // 记了“要求退款”
	orderFailedKnown        // 支付失败，记了知悉
)

// add 造一笔支付：minute 是它创建在第几分钟（决定它在哪一段），返回支付单号。
// payState 为负表示支付库里没有这一笔。
func (e *env) add(minute int, amount int64, payState, orderState int) int64 {
	e.t.Helper()
	e.serial++
	// 同一分钟里的各笔错开几毫秒；订单号的基因决定它落在哪个 slot、哪个库，让两个库都有数据。
	at := e.base.Add(time.Duration(minute)*time.Minute + time.Duration(e.serial)*7*time.Millisecond)
	orderID := idgen.LowerBound(at) | e.serial<<6 | (e.serial & 63)
	paymentID := idgen.LowerBound(at) | e.serial<<6

	if payState >= 0 {
		status, notified := 1, 0
		switch payState {
		case payUnacked:
			status = 2
		case payAcceptedRow:
			status, notified = 2, 1
		case payRefundedRow:
			status, notified = 4, 1
		case payFailedAcked:
			status, notified = 3, 1
		}
		testdb.MustExec(e.t, e.src.Payment,
			"INSERT INTO payments (payment_id, order_id, user_id, status, amount, currency, outcome, notified) VALUES (?, ?, 1, ?, ?, 'CNY', 1, ?)",
			paymentID, orderID, status, amount, notified)
	}
	if orderState != orderNone {
		succeeded, disposition := 1, 1
		switch orderState {
		case orderRefund:
			disposition = 2
		case orderFailedKnown:
			succeeded = 0
		}
		slot := e.src.Router.ForOrder(orderID)
		testdb.MustExec(e.t, e.src.Orders[e.src.Router.DB(slot)],
			"INSERT INTO "+sharding.Table("payment_results", slot)+" (payment_id, order_id, succeeded, amount, disposition) VALUES (?, ?, ?, ?, ?)",
			paymentID, orderID, succeeded, amount, disposition)
	}
	return paymentID
}

func (e *env) collect(fromMinute, toMinute int) Result {
	e.t.Helper()
	lo := idgen.LowerBound(e.base.Add(time.Duration(fromMinute) * time.Minute))
	hi := idgen.LowerBound(e.base.Add(time.Duration(toMinute) * time.Minute))
	r, err := Collect(e.t.Context(), e.src, lo, hi)
	if err != nil {
		e.t.Fatalf("collect: %v", err)
	}
	return r
}

func TestCollectConsistent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// 第 0 分钟：各种已经走完的状态，只有“成功且接受”的进合计。
	for i := range 20 {
		e.add(0, 100+int64(i), payAcceptedRow, orderAccepted)
	}
	e.add(0, 7000, payRefundedRow, orderRefund)      // 订单要求退款：两边都不算
	e.add(0, 8000, payFailedAcked, orderFailedKnown) // 支付失败：两边都不算
	// 第 1 分钟：在途的。订单接受了、答复没到（在途 2 笔）；成功了、订单还没处理；还没出结果。
	e.add(1, 500, payAcceptedRow, orderAccepted)
	e.add(1, 600, payUnacked, orderAccepted)
	e.add(1, 700, payUnacked, orderAccepted)
	e.add(1, 800, payUnacked, orderNone)
	e.add(1, 900, payPendingRow, orderNone)
	// 第 5 分钟：窗口之外，再怎么不一致也不该被看到。
	e.add(5, 123456, payAcceptedRow, orderNone)

	r := e.collect(0, 3)
	// 20 笔，金额 100..119。
	wantClosed := Sum{Count: 20, Amount: 20*100 + 190}
	if !r.OK() {
		t.Fatalf("violations on consistent data: %+v", r.Violations)
	}
	if r.ClosedPayment != wantClosed || r.ClosedOrder != wantClosed {
		t.Errorf("closed totals: payment %+v, order %+v, want both %+v", r.ClosedPayment, r.ClosedOrder, wantClosed)
	}
	if r.InFlight != 2 || r.OpenBuckets != 1 || !r.OldestOpen.Equal(e.base.Add(time.Minute)) {
		t.Errorf("in flight %d, open buckets %d, oldest open %s; want 2, 1, %s",
			r.InFlight, r.OpenBuckets, r.OldestOpen, e.base.Add(time.Minute))
	}
	if !r.From.Equal(e.base) || !r.To.Equal(e.base.Add(3*time.Minute)) {
		t.Errorf("window [%s, %s)", r.From, r.To)
	}
}

func TestCollectFindsMissingOrderRecord(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.add(0, 100, payAcceptedRow, orderAccepted)
	// 支付服务认为订单接受了，订单库里没有记录。
	e.add(0, 250, payAcceptedRow, orderNone)

	r := e.collect(0, 2)
	if r.OK() || r.MismatchCount != 1 || r.MismatchAmount != 250 {
		t.Fatalf("mismatch count %d amount %d, violations %+v; want 1, 250", r.MismatchCount, r.MismatchAmount, r.Violations)
	}
	v := r.Violations[0]
	if !v.Closed || !v.Start.Equal(e.base) || v.Payment != (Sum{2, 350}) || v.Order != (Sum{1, 100}) {
		t.Errorf("violation %+v", v)
	}
}

func TestCollectFindsAcceptedButRefunded(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.add(0, 100, payAcceptedRow, orderAccepted)
	// 订单记了接受，支付一侧这笔却退款了：订单当已支付，钱已经退回去。
	e.add(0, 400, payRefundedRow, orderAccepted)
	// 订单接受了一笔支付库里根本没有的支付，在另一段里。
	e.add(1, 900, -1, orderAccepted)

	r := e.collect(0, 2)
	if len(r.Violations) != 2 || r.MismatchCount != 2 || r.MismatchAmount != 1300 {
		t.Fatalf("mismatch count %d amount %d, violations %+v; want 2 violations, 2, 1300", r.MismatchCount, r.MismatchAmount, r.Violations)
	}
}

func TestCollectFindsAmountDifference(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.add(0, 100, payAcceptedRow, orderAccepted)
	// 同一笔支付，支付库里的金额被改了。笔数相等，只有金额差。
	testdb.MustExec(t, e.src.Payment, "UPDATE payments SET amount = 130 WHERE payment_id = ?", id)

	r := e.collect(0, 1)
	if r.OK() || r.MismatchCount != 0 || r.MismatchAmount != 30 {
		t.Fatalf("mismatch count %d amount %d; want 0, 30", r.MismatchCount, r.MismatchAmount)
	}
}

func TestCollectPaymentAheadInOpenBucket(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// 同一段里还有没出结果的支付单（没封口），但有一笔“成功且已答复”在订单库里找不到：不用等封口就是差异。
	e.add(0, 100, payPendingRow, orderNone)
	e.add(0, 300, payAcceptedRow, orderNone)

	r := e.collect(0, 1)
	if r.OK() || r.MismatchCount != 1 || r.MismatchAmount != 300 || r.Violations[0].Closed {
		t.Fatalf("mismatch count %d amount %d, violations %+v; want 1, 300, not closed", r.MismatchCount, r.MismatchAmount, r.Violations)
	}
}

func TestCollectRejectsBadSources(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	src := e.src
	src.Orders = src.Orders[:1] // 路由是 2 个库的
	if _, err := Collect(t.Context(), src, 0, idsPerBucket); err == nil {
		t.Error("Collect accepted a router with a different number of databases")
	}
	if _, err := Collect(t.Context(), Sources{}, 0, idsPerBucket); err == nil {
		t.Error("Collect accepted empty sources")
	}
}

// 表里没有任何数据时两条查询都要能跑（空的聚合、空的 UNION）。
func TestCollectEmpty(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r := e.collect(0, 30)
	if !r.OK() || r.ClosedPayment != (Sum{}) || r.OpenBuckets != 0 {
		t.Errorf("result on empty databases: %+v", r)
	}
}
