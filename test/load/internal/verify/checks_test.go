package verify

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
)

// world 构造一个自洽的系统终态：SKU 1001 初始 10 件，订单 1 付清 2 件、订单 2 取消、
// 订单 3 关闭、订单 4 创建失败（无预占）、订单 5 支付失败后关闭。账本与之一致。
func world() Input {
	s := &Snapshot{
		Initial: map[int64]int64{1001: 10},
		Stock:   map[int64]SKUStock{1001: {SKUID: 1001, Available: 8, Sold: 2}},
		Reservations: map[int64]Reservation{
			1: {OrderID: 1, Status: ReservationConfirmed, Items: map[int64]int64{1001: 2}},
			2: {OrderID: 2, Status: ReservationReleased, Items: map[int64]int64{1001: 1}},
			3: {OrderID: 3, Status: ReservationReleased, Items: map[int64]int64{1001: 1}},
			5: {OrderID: 5, Status: ReservationReleased, Items: map[int64]int64{1001: 1}},
		},
		Orders: map[int64]Order{
			1: {OrderID: 1, UserID: 11, Status: OrderPaid, TotalAmount: 200, Currency: "CNY", IdempotencyKey: "k1", PaymentID: 101, Items: map[int64]int64{1001: 2}},
			2: {OrderID: 2, UserID: 12, Status: OrderCancelled, TotalAmount: 100, Currency: "CNY", IdempotencyKey: "k2", Items: map[int64]int64{1001: 1}},
			3: {OrderID: 3, UserID: 13, Status: OrderClosed, TotalAmount: 100, Currency: "CNY", IdempotencyKey: "k3", Items: map[int64]int64{1001: 1}},
			4: {OrderID: 4, UserID: 14, Status: OrderFailed, Currency: "CNY", IdempotencyKey: "k4", Items: map[int64]int64{1001: 1}},
			5: {OrderID: 5, UserID: 15, Status: OrderClosed, TotalAmount: 100, Currency: "CNY", IdempotencyKey: "k5", Items: map[int64]int64{1001: 1}},
		},
		Payments: []Payment{
			{PaymentID: 101, OrderID: 1, UserID: 11, Status: PaymentSucceeded, Amount: 200, Currency: "CNY", Notified: true},
			{PaymentID: 105, OrderID: 5, UserID: 15, Status: PaymentFailed, Amount: 100, Currency: "CNY", Notified: true},
		},
		Gate:          map[int64]GateStock{1001: {Avail: 8, Held: 0, Limit: 0}},
		Bought:        map[int64]map[int64]int64{1001: {11: 2}},
		Holds:         map[int64]bool{},
		OutboxPending: map[string]int64{"order": 0, "payment": 0},
	}
	s.OrdersByIdem = map[IdemRef]int64{}
	for id, o := range s.Orders {
		s.OrdersByIdem[IdemRef{UserID: o.UserID, Key: o.IdempotencyKey}] = id
	}
	var led []ledger.Entry
	add := func(e ledger.Entry) { e.Time = time.Unix(0, 0); led = append(led, e) }
	for _, id := range []int64{1, 2, 3, 4, 5} {
		o := s.Orders[id]
		add(ledger.Entry{
			Op: ledger.OpSubmit, OrderID: strconv.FormatInt(id, 10), UserID: o.UserID, SKUID: 1001,
			IdempotencyKey: o.IdempotencyKey, Attempt: 1, HTTPStatus: 202, OrderStatus: "creating", Outcome: ledger.OutcomeOK,
		})
	}
	add(ledger.Entry{
		Op: ledger.OpPollFinal, Stage: ledger.StageSettle, OrderID: "1", UserID: 11, IdempotencyKey: "k1",
		HTTPStatus: 200, OrderStatus: "paid", Outcome: ledger.OutcomeOK,
	})
	add(ledger.Entry{
		Op: ledger.OpCancel, OrderID: "2", UserID: 12, IdempotencyKey: "k2",
		HTTPStatus: 200, OrderStatus: "cancelled", Outcome: ledger.OutcomeOK,
	})
	return Input{Snapshot: s, Ledger: led}
}

func failedChecks(t *testing.T, in Input) []int {
	t.Helper()
	res := Check(in)
	for _, v := range res.Violations {
		t.Logf("violation: %s", v)
	}
	return res.FailedChecks()
}

func TestConsistentWorldPasses(t *testing.T) {
	in := world()
	res := Check(in)
	if res.Failed() {
		t.Fatalf("unexpected violations: %v", res.Violations)
	}
	if res.Counts.Accepted != 5 || res.Counts.AcceptedInDB != 5 || res.Counts.Reaped != 0 {
		t.Fatalf("counts = %+v", res.Counts)
	}
}

func TestEachCheckDetectsItsViolation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(in *Input)
		want   []int
	}{
		{"1 total differs from initial", func(in *Input) { in.Snapshot.Initial[1001] = 11 }, []int{1}},
		{"1 sku without initial", func(in *Input) {
			in.Snapshot.Stock[2002] = SKUStock{SKUID: 2002, Available: 1}
		}, []int{1}},
		{"2 reserved left", func(in *Input) {
			in.Snapshot.Stock[1001] = SKUStock{SKUID: 1001, Available: 7, Reserved: 1, Sold: 2}
			in.Snapshot.Gate[1001] = GateStock{Avail: 8}
		}, []int{2}},
		{"3 negative", func(in *Input) {
			in.Snapshot.Initial[1001] = 9
			in.Snapshot.Stock[1001] = SKUStock{SKUID: 1001, Available: 8, Reserved: -1, Sold: 2}
			in.Snapshot.Gate[1001] = GateStock{Avail: 7}
		}, []int{2, 3}},
		{"4 sold differs from paid orders", func(in *Input) {
			in.Snapshot.Stock[1001] = SKUStock{SKUID: 1001, Available: 7, Sold: 3}
			in.Snapshot.Gate[1001] = GateStock{Avail: 7}
		}, []int{4}},
		{"5 paid order with released reservation", func(in *Input) {
			r := in.Snapshot.Reservations[1]
			r.Status = ReservationReleased
			in.Snapshot.Reservations[1] = r
		}, []int{5}},
		{"5 cancelled order with confirmed reservation", func(in *Input) {
			r := in.Snapshot.Reservations[2]
			r.Status = ReservationConfirmed
			in.Snapshot.Reservations[2] = r
		}, []int{5}},
		{"5 failed order with reserved reservation", func(in *Input) {
			in.Snapshot.Reservations[4] = Reservation{OrderID: 4, Status: ReservationReserved, Items: map[int64]int64{1001: 1}}
		}, []int{5}},
		{"5 order not terminal", func(in *Input) {
			o := in.Snapshot.Orders[3]
			o.Status = OrderPendingPayment
			in.Snapshot.Orders[3] = o
		}, []int{5}},
		{"6 reservation without order", func(in *Input) {
			in.Snapshot.Reservations[99] = Reservation{OrderID: 99, Status: ReservationConfirmed, Items: map[int64]int64{1001: 1}}
		}, []int{6}},
		{"7 paid order amount mismatch", func(in *Input) { in.Snapshot.Payments[0].Amount = 199 }, []int{7}},
		{"7 paid order without payment", func(in *Input) { in.Snapshot.Payments = in.Snapshot.Payments[1:] }, []int{7}},
		{"8 succeeded payment for closed order", func(in *Input) {
			in.Snapshot.Payments[1].Status = PaymentSucceeded
		}, []int{8, 9}},
		{"8 succeeded payment without order", func(in *Input) {
			in.Snapshot.Payments = append(in.Snapshot.Payments, Payment{PaymentID: 900, OrderID: 900, Status: PaymentSucceeded, Notified: true})
		}, []int{8}},
		{"9 pending payment on closed order", func(in *Input) {
			in.Snapshot.Payments[1].Status = PaymentPending
			in.Snapshot.Payments[1].Notified = false
		}, []int{9}},
		{"10 not notified", func(in *Input) { in.Snapshot.Payments[1].Notified = false }, []int{10}},
		{"11 gate drift", func(in *Input) { in.Snapshot.Gate[1001] = GateStock{Avail: 9} }, []int{11}},
		{"11 held left", func(in *Input) { in.Snapshot.Gate[1001] = GateStock{Avail: 7, Held: 1} }, []int{11}},
		{"11 claimed hold left", func(in *Input) {
			in.Snapshot.Claimed = []int64{1}
			in.Snapshot.Holds[1] = true
		}, []int{11}},
		{"12 bought counter not decremented", func(in *Input) { in.Snapshot.Bought[1001][12] = 1 }, []int{12}},
		{"12 bought counter missing", func(in *Input) { delete(in.Snapshot.Bought[1001], 11) }, []int{12}},
		{"13 client saw paid", func(in *Input) {
			in.Ledger = append(in.Ledger, ledger.Entry{
				Op: ledger.OpPollFinal, OrderID: "3", UserID: 13, IdempotencyKey: "k3",
				OrderStatus: "paid", Outcome: ledger.OutcomeOK,
			})
		}, []int{13}},
		{"14 cancel 200 but closed", func(in *Input) {
			in.Ledger = append(in.Ledger, ledger.Entry{
				Op: ledger.OpCancel, OrderID: "3", UserID: 13, IdempotencyKey: "k3",
				HTTPStatus: 200, OrderStatus: "cancelled", Outcome: ledger.OutcomeOK,
			})
		}, []int{14}},
		{"15 accepted, not in db, hold still there", func(in *Input) {
			in.Ledger = append(in.Ledger, ledger.Entry{
				Op: ledger.OpSubmit, OrderID: "77", UserID: 17, IdempotencyKey: "k7",
				HTTPStatus: 202, OrderStatus: "creating", Outcome: ledger.OutcomeOK,
			})
			in.Snapshot.Holds[77] = true
		}, []int{11, 15}},
		{"17 outbox", func(in *Input) { in.Snapshot.OutboxPending["payment"] = 3 }, []int{17}},
		{"18 dlq", func(in *Input) {
			in.Snapshot.DLQ = []DLQMessage{{Topic: "x.order.events.dlq", Headers: map[string]string{"dlq-error": "boom"}}}
		}, []int{18}},
		{"19 correction in log", func(in *Input) {
			in.Snapshot.Corrections = []string{`{"msg":"stock gate deviation corrected"}`}
		}, []int{CheckLogCorrections}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := world()
			tc.mutate(&in)
			if got := failedChecks(t, in); !slices.Equal(got, tc.want) {
				t.Fatalf("failed checks = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLedgerCounts(t *testing.T) {
	in := world()
	e := func(e ledger.Entry) { in.Ledger = append(in.Ledger, e) }
	// 被回收后用同一个幂等键重新提交，拿到了订单 6。
	in.Snapshot.Orders[6] = Order{OrderID: 6, UserID: 16, Status: OrderFailed, Currency: "CNY", IdempotencyKey: "k6", Items: map[int64]int64{1001: 1}}
	in.Snapshot.OrdersByIdem[IdemRef{UserID: 16, Key: "k6"}] = 6
	e(ledger.Entry{Op: ledger.OpSubmit, OrderID: "60", UserID: 16, IdempotencyKey: "k6", HTTPStatus: 202, Outcome: ledger.OutcomeOK})
	e(ledger.Entry{Op: ledger.OpSubmit, OrderID: "6", UserID: 16, IdempotencyKey: "k6", HTTPStatus: 202, Outcome: ledger.OutcomeOK})
	// 被回收、流程就此结束。
	e(ledger.Entry{Op: ledger.OpSubmit, OrderID: "61", UserID: 18, IdempotencyKey: "k8", HTTPStatus: 202, Outcome: ledger.OutcomeOK})
	// 提交一直结果未知，服务端其实建了订单 7。
	in.Snapshot.Orders[7] = Order{OrderID: 7, UserID: 19, Status: OrderFailed, Currency: "CNY", IdempotencyKey: "k9", Items: map[int64]int64{1001: 1}}
	in.Snapshot.OrdersByIdem[IdemRef{UserID: 19, Key: "k9"}] = 7
	e(ledger.Entry{Op: ledger.OpSubmit, UserID: 19, IdempotencyKey: "k9", Outcome: ledger.OutcomeUnknown})
	e(ledger.Entry{Op: ledger.OpSubmit, UserID: 19, IdempotencyKey: "k9", HTTPStatus: 504, Outcome: ledger.OutcomeHTTPError})
	// 提交 5xx 后放弃，服务端没有订单。
	e(ledger.Entry{Op: ledger.OpSubmit, UserID: 20, IdempotencyKey: "k10", HTTPStatus: 503, Outcome: ledger.OutcomeHTTPError})
	// 只被拒绝过的流程不算放弃。
	e(ledger.Entry{Op: ledger.OpSubmit, UserID: 21, IdempotencyKey: "k11", HTTPStatus: 409, Outcome: ledger.OutcomeRejected})
	// 支付结果未知。
	e(ledger.Entry{Op: ledger.OpPay, OrderID: "1", UserID: 11, IdempotencyKey: "k1", Outcome: ledger.OutcomeUnknown})

	res := Check(in)
	if res.Failed() {
		t.Fatalf("unexpected violations: %v", res.Violations)
	}
	c := res.Counts
	if c.Accepted != 8 || c.AcceptedInDB != 6 || c.Reaped != 2 || c.ReapedResubmitted != 1 {
		t.Errorf("accepted %d in db %d reaped %d resubmitted %d", c.Accepted, c.AcceptedInDB, c.Reaped, c.ReapedResubmitted)
	}
	if c.Unknown[ledger.OpSubmit] != 1 || c.Unknown[ledger.OpPay] != 1 || c.HTTP5xx[ledger.OpSubmit] != 2 {
		t.Errorf("unknown %v 5xx %v", c.Unknown, c.HTTP5xx)
	}
	if c.GaveUp != 2 || c.GaveUpFound["FAILED"] != 1 || len(c.GaveUpFound) != 1 {
		t.Errorf("gave up %d found %v", c.GaveUp, c.GaveUpFound)
	}
}

func TestQuietProblems(t *testing.T) {
	q := QuietState{
		OutboxPending: map[string]int64{"order": 0, "payment": 2}, Lag: map[string]int64{"g1": 0, "g2": 5},
		LagErr: map[string]string{"g3": "boom"}, CreatingOrders: 1, ClaimedHolds: 3,
	}
	got := strings.Join(q.Problems(), "|")
	want := "payment outbox has 2 unpublished rows|consumer group g3 lag unknown: boom|consumer group g2 lag 5|" +
		"1 orders in CREATING|3 holds in gate:claimed"
	if got != want {
		t.Fatalf("problems = %q\nwant       %q", got, want)
	}
	if p := (QuietState{Lag: map[string]int64{"g": 0}}).Problems(); len(p) != 0 {
		t.Fatalf("zero state problems = %v", p)
	}
}

func TestScanLog(t *testing.T) {
	log := strings.Join([]string{
		`{"level":"INFO","msg":"stock gate deviation observed","sku_id":1}`,
		`{"level":"ERROR","msg":"stock gate deviation corrected","sku_id":1,"deviation":2}`,
		`{"level":"WARN","msg":"stock gate deviation persists, correction capped","sku_id":2}`,
		`not json`,
	}, "\n")
	corrected, capped, err := scanLog(strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	if len(corrected) != 1 || !strings.Contains(corrected[0], `"deviation":2`) || len(capped) != 1 {
		t.Fatalf("corrected %v capped %v", corrected, capped)
	}
}
