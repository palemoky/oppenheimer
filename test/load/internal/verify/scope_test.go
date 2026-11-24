package verify

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/pkg/testredis"
	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
)

func TestNewScope(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		skus      []int64
		entries   []ledger.Entry
		wantSKUs  []int64
		wantIDs   []int64
		wantFlows []IdemRef
	}{
		{name: "empty", wantSKUs: []int64{}, wantIDs: []int64{}, wantFlows: []IdemRef{}},
		{
			name: "dedup and sort, every op contributes its order id",
			skus: []int64{7002, 7001, 7002},
			entries: []ledger.Entry{
				{Op: ledger.OpSubmit, OrderID: "30", UserID: 2, IdempotencyKey: "b"},
				{Op: ledger.OpPollFinal, OrderID: "30", UserID: 2, IdempotencyKey: "b"},
				{Op: ledger.OpSubmit, OrderID: "", UserID: 1, IdempotencyKey: "a", Outcome: ledger.OutcomeUnknown},
				{Op: ledger.OpCancel, OrderID: "10", UserID: 1, IdempotencyKey: "c"},
				{Op: ledger.OpSubmit, OrderID: "not-a-number", UserID: 1, IdempotencyKey: "c"},
				{Op: ledger.OpSubmit, OrderID: "-5", UserID: 0, IdempotencyKey: "d"},
			},
			wantSKUs:  []int64{7001, 7002},
			wantIDs:   []int64{10, 30},
			wantFlows: []IdemRef{{1, "a"}, {1, "c"}, {2, "b"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sc := NewScope(tt.skus, tt.entries)
			if !slices.Equal(sc.SKUs, tt.wantSKUs) && len(sc.SKUs)+len(tt.wantSKUs) > 0 {
				t.Errorf("skus = %v, want %v", sc.SKUs, tt.wantSKUs)
			}
			if !slices.Equal(sc.OrderIDs, tt.wantIDs) && len(sc.OrderIDs)+len(tt.wantIDs) > 0 {
				t.Errorf("order ids = %v, want %v", sc.OrderIDs, tt.wantIDs)
			}
			if !slices.Equal(sc.Flows, tt.wantFlows) && len(sc.Flows)+len(tt.wantFlows) > 0 {
				t.Errorf("flows = %v, want %v", sc.Flows, tt.wantFlows)
			}
			if sc.Empty() != (len(tt.skus) == 0) {
				t.Errorf("empty = %t with skus %v", sc.Empty(), tt.skus)
			}
		})
	}
}

func TestChunksAndPlaceholders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		n, size int
		want    []int // 每段的长度
	}{
		{0, 3, nil},
		{1, 3, []int{1}},
		{3, 3, []int{3}},
		{7, 3, []int{3, 3, 1}},
		{1001, 500, []int{500, 500, 1}},
		{2, 0, []int{1, 1}},
	}
	for _, tt := range tests {
		s := make([]int64, tt.n)
		for i := range s {
			s[i] = int64(i)
		}
		var got []int
		var joined []int64
		for c := range chunks(s, tt.size) {
			got = append(got, len(c))
			joined = append(joined, c...)
			if ph := placeholders(len(c)); strings.Count(ph, "?") != len(c) || strings.Count(ph, ",") != len(c)-1 {
				t.Errorf("placeholders(%d) = %q", len(c), ph)
			}
		}
		if !slices.Equal(got, tt.want) || !slices.Equal(joined, s) {
			t.Errorf("chunks(%d, %d) sizes %v, want %v (concatenation equal: %t)", tt.n, tt.size, got, tt.want, slices.Equal(joined, s))
		}
	}
}

// scopeEnv 是一套临时的库和闸门，以及往里写数据的帮手。订单号由 idgen 以用户号为分片键生成，
// 和真实的订单一样能按订单号路由到 slot（限定范围的采集靠这个点查）。
type scopeEnv struct {
	t      *testing.T
	src    Sources
	gate   *stockgate.Gate
	router *sharding.Router
	ids    *idgen.Generator
}

func newScopeEnv(t *testing.T) *scopeEnv {
	t.Helper()
	rdb, prefix := testredis.New(t)
	router, err := sharding.New(sharding.OrderLayout(2))
	if err != nil {
		t.Fatal(err)
	}
	gen, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	return &scopeEnv{
		t: t,
		src: Sources{
			InventoryDB: newDB(t, "inventory"), OrderDBs: newOrderDBs(t), PaymentDB: newDB(t, "payment"),
			Redis: rdb, RedisPrefix: prefix,
		},
		gate: stockgate.New(rdb, prefix), router: router, ids: gen,
	}
}

func (e *scopeEnv) newOrderID(user int64) int64 {
	id, err := e.ids.Next(user)
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *scopeEnv) stock(sku, avail, reserved, sold int64) {
	exec(e.t, e.src.InventoryDB, "INSERT INTO sku_stock (sku_id, name, unit_price, available, reserved, sold) VALUES (?, 'x', 100, ?, ?, ?)",
		sku, avail, reserved, sold)
}

// order 写订单和明细；paymentID 为 0 表示 NULL。
func (e *scopeEnv) order(id, user int64, status int, key string, paymentID int64, items map[int64]int64) {
	slot := e.router.ForOrder(id)
	db := e.src.OrderDBs[e.router.DB(slot)]
	var total int64
	for _, q := range items {
		total += 100 * q
	}
	var pid any
	if paymentID != 0 {
		pid = paymentID
	}
	exec(e.t, db, fmt.Sprintf(`INSERT INTO %s (order_id, user_id, status, total_amount, currency, idempotency_key, request_hash, payment_id, expire_at)
		VALUES (?, ?, ?, ?, 'CNY', ?, REPEAT('a', 32), ?, NOW(6))`, sharding.Table("orders", slot)), id, user, status, total, key, pid)
	for sku, q := range items {
		exec(e.t, db, fmt.Sprintf("INSERT INTO %s (order_id, sku_id, quantity, unit_price) VALUES (?, ?, ?, 100)", sharding.Table("order_items", slot)),
			id, sku, q)
	}
}

func (e *scopeEnv) reservation(id int64, status int, items map[int64]int64) {
	exec(e.t, e.src.InventoryDB, "INSERT INTO reservation (order_id, status) VALUES (?, ?)", id, status)
	for sku, q := range items {
		exec(e.t, e.src.InventoryDB, "INSERT INTO reservation_item (order_id, sku_id, quantity, unit_price, currency) VALUES (?, ?, ?, 100, 'CNY')",
			id, sku, q)
	}
}

func (e *scopeEnv) payment(paymentID, orderID, user int64, status int, amount int64, notified bool) {
	exec(e.t, e.src.PaymentDB, `INSERT INTO payments (payment_id, order_id, user_id, status, amount, currency, outcome, notified)
		VALUES (?, ?, ?, ?, ?, 'CNY', 1, ?)`, paymentID, orderID, user, status, amount, notified)
}

func (e *scopeEnv) load(sku, avail int64) {
	if _, err := e.gate.Load(e.t.Context(), sku, avail, 0); err != nil {
		e.t.Fatal(err)
	}
}

func (e *scopeEnv) hold(orderID, user, sku int64) {
	res, err := e.gate.Hold(e.t.Context(), stockgate.HoldRequest{
		OrderID: orderID, UserID: user, IdempotencyKey: fmt.Sprintf("hold-%d", orderID), RequestHash: make([]byte, 32),
		Items: []stockgate.Item{{SKUID: sku, Quantity: 1}}, Now: time.Now(), IdempotencyTTL: time.Hour,
	})
	if err != nil || res.Outcome != stockgate.Held {
		e.t.Fatalf("hold %d: %v %v", orderID, res, err)
	}
}

// scopeRun 是“这一轮压测”的数据和它的账本。
type scopeRun struct {
	initial          map[int64]int64
	ledger           []ledger.Entry
	a, b, c, d, eord int64 // eord 是“重试用尽、没拿到订单号”的流程在库里的订单
}

const scopeSKU1, scopeSKU2, otherSKU = 7001, 7002, 8001

// seedRun 写入一轮压测的数据，本身是对得上账的：
//   - A 付清（7001×2），B 取消（7002×1），C 关闭（7001×1，客户端没收到响应，不在账本里，只能经预占明细找到），
//   - D 受理了但没落库、也没有占用（被回收），
//   - E 失败（7002×1，没有预占），账本里它的提交结果未知、没有订单号，只能按 (user_id, 幂等键) 找到。
func seedRun(e *scopeEnv) scopeRun {
	r := scopeRun{initial: map[int64]int64{scopeSKU1: 10, scopeSKU2: 10}}
	r.a, r.b, r.c, r.d, r.eord = e.newOrderID(101), e.newOrderID(102), e.newOrderID(103), e.newOrderID(104), e.newOrderID(105)

	e.stock(scopeSKU1, 8, 0, 2)
	e.stock(scopeSKU2, 10, 0, 0)

	e.order(r.a, 101, OrderPaid, "kA", 9001, map[int64]int64{scopeSKU1: 2})
	e.reservation(r.a, ReservationConfirmed, map[int64]int64{scopeSKU1: 2})
	e.payment(9001, r.a, 101, PaymentSucceeded, 200, true)

	e.order(r.b, 102, OrderCancelled, "kB", 0, map[int64]int64{scopeSKU2: 1})
	e.reservation(r.b, ReservationReleased, map[int64]int64{scopeSKU2: 1})

	e.order(r.c, 103, OrderClosed, "kC", 0, map[int64]int64{scopeSKU1: 1})
	e.reservation(r.c, ReservationReleased, map[int64]int64{scopeSKU1: 1})

	e.order(r.eord, 105, OrderFailed, "kE", 0, map[int64]int64{scopeSKU2: 1})

	e.load(scopeSKU1, 8)
	e.load(scopeSKU2, 10)
	if err := e.src.Redis.HSet(e.t.Context(), e.src.RedisPrefix+"gate:bought:7001", "101", 2).Err(); err != nil {
		e.t.Fatal(err)
	}

	id := func(v int64) string { return strconv.FormatInt(v, 10) }
	r.ledger = []ledger.Entry{
		{Op: ledger.OpSubmit, OrderID: id(r.a), UserID: 101, IdempotencyKey: "kA", Attempt: 1, HTTPStatus: 202, OrderStatus: "creating", Outcome: ledger.OutcomeOK},
		{Op: ledger.OpPollFinal, OrderID: id(r.a), UserID: 101, IdempotencyKey: "kA", HTTPStatus: 200, OrderStatus: "paid", Outcome: ledger.OutcomeOK},
		{Op: ledger.OpSubmit, OrderID: id(r.b), UserID: 102, IdempotencyKey: "kB", Attempt: 1, HTTPStatus: 202, OrderStatus: "creating", Outcome: ledger.OutcomeOK},
		{Op: ledger.OpCancel, OrderID: id(r.b), UserID: 102, IdempotencyKey: "kB", HTTPStatus: 200, OrderStatus: "cancelled", Outcome: ledger.OutcomeOK},
		{Op: ledger.OpSubmit, OrderID: id(r.d), UserID: 104, IdempotencyKey: "kD", Attempt: 1, HTTPStatus: 202, OrderStatus: "creating", Outcome: ledger.OutcomeOK},
		{Op: ledger.OpSubmit, UserID: 105, IdempotencyKey: "kE", Attempt: 1, Outcome: ledger.OutcomeUnknown},
	}
	return r
}

// seedOther 写入范围外的数据：读进来的话每一样都会让 Check 报错或改变计数。
func seedOther(e *scopeEnv) {
	// 别的 SKU，有预占中的数量（第 2 条）和没应用的增量（第 22 条）。
	e.stock(otherSKU, 50, 1, 0)
	exec(e.t, e.src.InventoryDB, "INSERT INTO stock_pending (sku_id, d_available, d_reserved, d_sold, order_id) VALUES (?, -1, 1, 0, 1)", otherSKU)
	// 别的订单：待支付（第 5 条），预占中，支付单处理中。用户 101 和 A 同一个用户、不同的幂等键：
	// 按 (user_id, 幂等键) 找订单不能把它带进来。
	x := e.newOrderID(201)
	e.order(x, 201, OrderPendingPayment, "kX", 0, map[int64]int64{otherSKU: 1})
	e.reservation(x, ReservationReserved, map[int64]int64{otherSKU: 1})
	e.payment(9100, x, 201, PaymentPending, 100, false)
	z := e.newOrderID(101)
	e.order(z, 101, OrderPendingPayment, "kZ", 0, map[int64]int64{otherSKU: 1})
	// 造数的订单：用户号在 seededUserBase 之上，状态是 CREATING（第 5 条）。
	seeded := seededUserBase + 7
	e.order(e.newOrderID(seeded), seeded, OrderCreating, "kA", 0, map[int64]int64{otherSKU: 1})
	// 闸门里别的 SKU 和一笔遗留的占用（第 11 条）。
	e.load(otherSKU, 50)
	e.hold(e.newOrderID(202), 202, otherSKU)
}

func TestCollectScopedReadsOnlyTheRun(t *testing.T) {
	e := newScopeEnv(t)
	run := seedRun(e)
	seedOther(e)
	ctx := t.Context()
	sc := NewScope([]int64{scopeSKU1, scopeSKU2}, run.ledger)

	snap, err := CollectScoped(ctx, e.src, run.initial, sc)
	if err != nil {
		t.Fatal(err)
	}
	keys := func(m any) []int64 {
		v := reflect.ValueOf(m)
		out := make([]int64, 0, v.Len())
		for _, k := range v.MapKeys() {
			out = append(out, k.Int())
		}
		slices.Sort(out)
		return out
	}
	sorted := func(ids ...int64) []int64 { return slices.Sorted(slices.Values(ids)) }
	skus := []int64{scopeSKU1, scopeSKU2}
	for _, c := range []struct {
		what      string
		got, want []int64
	}{
		{"stock", keys(snap.Stock), skus},
		{"raw stock", keys(snap.RawStock), skus},
		{"pending", keys(snap.Pending), []int64{}},
		{"orders", keys(snap.Orders), sorted(run.a, run.b, run.c, run.eord)},
		{"reservations", keys(snap.Reservations), sorted(run.a, run.b, run.c)},
		{"gate", keys(snap.Gate), skus},
		{"bought", keys(snap.Bought), skus},
		{"holds", keys(snap.Holds), []int64{}},
	} {
		if !slices.Equal(c.got, c.want) {
			t.Errorf("%s = %v, want %v", c.what, c.got, c.want)
		}
	}
	if len(snap.Payments) != 1 || snap.Payments[0].PaymentID != 9001 {
		t.Errorf("payments = %+v", snap.Payments)
	}
	if len(snap.Unclaimed) != 0 || len(snap.Claimed) != 0 {
		t.Errorf("unclaimed %v claimed %v", snap.Unclaimed, snap.Claimed)
	}
	if !maps.Equal(snap.Bought[scopeSKU1], map[int64]int64{101: 2}) || len(snap.Bought[scopeSKU2]) != 0 {
		t.Errorf("bought = %v", snap.Bought)
	}
	if o := snap.Orders[run.a]; o.Status != OrderPaid || o.PaymentID != 9001 || !maps.Equal(o.Items, map[int64]int64{scopeSKU1: 2}) {
		t.Errorf("order A = %+v", o)
	}
	if snap.MerchantIndex != nil || snap.Archive != nil || snap.DLQ != nil || snap.Corrections != nil || snap.Capped != nil {
		t.Errorf("scoped collect read a source it should skip: %+v", snap)
	}

	// 这一轮本身对得上账：限定范围后范围外的数据不该带来任何违规。
	res := Check(Input{Snapshot: snap, Ledger: run.ledger})
	if res.Failed() {
		t.Fatalf("violations: %v", res.Violations)
	}
	c := res.Counts
	if c.Accepted != 3 || c.AcceptedInDB != 2 || c.Reaped != 1 || c.GaveUp != 1 || c.GaveUpFound["FAILED"] != 1 || c.OrdersWithoutFlow != 1 {
		t.Errorf("counts = %+v", c)
	}

	// 对照：不限定范围时范围外的数据会让 Check 报错（证明上面的通过不是因为这些数据没写进去）。
	full, err := Collect(ctx, e.src, run.initial)
	if err != nil {
		t.Fatal(err)
	}
	if got := Check(Input{Snapshot: full, Ledger: run.ledger}).FailedChecks(); !slices.Contains(got, 2) || !slices.Contains(got, 5) ||
		!slices.Contains(got, 11) || !slices.Contains(got, CheckStockPending) {
		t.Errorf("unscoped failed checks = %v, want 2, 5, 11 and 22 among them", got)
	}

	// 把范围内一个 SKU 的已售数改大一个：库存不再守恒，必须报出来。
	exec(t, e.src.InventoryDB, "UPDATE sku_stock SET sold = sold + 1, stocked = available + reserved + sold WHERE sku_id = ?", scopeSKU1)
	snap, err = CollectScoped(ctx, e.src, run.initial, sc)
	if err != nil {
		t.Fatal(err)
	}
	res = Check(Input{Snapshot: snap, Ledger: run.ledger})
	found := false
	for _, v := range res.Violations {
		found = found || v.Check == 1 && v.Subject == skuSubj(scopeSKU1)
	}
	if !found {
		t.Errorf("sold + 1 on sku %d not reported by check 1: %v", scopeSKU1, res.Violations)
	}
}

// 库里只有这一轮的数据时，限定范围和不限定范围的采集让 Check 得出同样的结果。
// 数据里放了一笔遗留的占用（D 受理了、没落库，占用还在），让结果不是“全部通过”这种平凡的情况。
func TestCollectScopedMatchesCollect(t *testing.T) {
	e := newScopeEnv(t)
	run := seedRun(e)
	e.hold(run.d, 104, scopeSKU1)
	ctx := t.Context()

	full, err := Collect(ctx, e.src, run.initial)
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := CollectScoped(ctx, e.src, run.initial, NewScope([]int64{scopeSKU1, scopeSKU2}, run.ledger))
	if err != nil {
		t.Fatal(err)
	}
	if !scoped.Holds[run.d] || !slices.Equal(scoped.Unclaimed, []int64{run.d}) || len(scoped.Claimed) != 0 {
		t.Errorf("scoped holds %v unclaimed %v claimed %v", scoped.Holds, scoped.Unclaimed, scoped.Claimed)
	}
	want := Check(Input{Snapshot: full, Ledger: run.ledger})
	got := Check(Input{Snapshot: scoped, Ledger: run.ledger})
	if !want.Failed() {
		t.Fatal("the leftover hold should fail checks 11 and 15")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("scoped result differs from the unscoped one:\nscoped   %+v\nunscoped %+v", got, want)
	}
}
