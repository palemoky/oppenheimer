package service

import (
	"database/sql"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/palemoky/Oppenheimer/api/eventx"
	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	eventv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/event/v1"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/services/order"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

// 测试用商品。
const (
	skuA       = 1 // 单价 100
	skuB       = 2 // 单价 250
	skuScarce  = 3 // 只有 1 件
	skuHuge    = 4 // 单价接近 int64 上限的一半
	skuMissing = 99

	priceA = 100
	priceB = 250
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeRelay 记录 Kick 的次数。
type fakeRelay struct{ kicks atomic.Int64 }

func (r *fakeRelay) Kick() { r.kicks.Add(1) }

// 测试用的 topic 名，带前缀以确认 Service 用的是配置的 topic 而不是默认值。
const testEventsTopic = "test." + eventx.OrderEvents

type env struct {
	db      *sql.DB
	store   *store.Store
	inv     *fakeInventory
	clock   *fakeClock
	relay   *fakeRelay
	svc     *Service
	sweeper *Sweeper

	// consumed 是 consumeEvents 已经处理到的 outbox 行数；simulated 是它代替库存服务发起的调用次数。
	mu        sync.Mutex
	consumed  int
	simulated map[string]int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testdb.New(t, order.Migrations, order.MigrationsDir)
	inv := newFakeInventory()
	inv.addSKU(skuA, priceA, 1000)
	inv.addSKU(skuB, priceB, 1000)
	inv.addSKU(skuScarce, 10, 1)
	inv.addSKU(skuHuge, 1<<62, 1000)

	ids, err := idgen.New(0)
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{t: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
	logger := slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug}))
	st := store.New(db)
	relay := &fakeRelay{}
	svc := New(st, inv, ids, logger, Config{Clock: clock.Now, OrderEventsTopic: testEventsTopic, Relay: relay})
	sweeper, err := NewSweeper(svc, SweeperConfig{})
	if err != nil {
		t.Fatal(err)
	}
	e := &env{
		db:        db,
		store:     st,
		inv:       inv,
		clock:     clock,
		relay:     relay,
		svc:       svc,
		sweeper:   sweeper,
		simulated: map[string]int{},
	}
	// 1b 起订单服务只同步调用 Reserve，确认和释放全部交给库存服务消费事件完成。
	// 每个测试结束时检查这一点，任何一条路径偷偷调回库存都会被发现。
	t.Cleanup(func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		for _, m := range []string{"Confirm", "Release"} {
			if n := inv.callCount(m) - e.simulated[m]; n != 0 {
				t.Errorf("order service called inventory.%s %d times, want 0", m, n)
			}
		}
	})
	return e
}

// consumeEvents 扮演库存服务：按顺序消费 outbox 里新增的事件，
// paid 确认预占，cancelled、closed、failed 释放预占。用来验证“事件发出去之后库存不会泄漏”。
func (e *env) consumeEvents(t *testing.T) {
	t.Helper()
	evs := e.allEvents(t)
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ev := range evs[e.consumed:] {
		var err error
		switch ev.GetBody().(type) {
		case *eventv1.OrderEvent_Paid:
			e.simulated["Confirm"]++
			_, err = e.inv.Confirm(t.Context(), &inventoryv1.ConfirmRequest{OrderId: ev.GetOrderId()})
		case *eventv1.OrderEvent_Cancelled, *eventv1.OrderEvent_Closed, *eventv1.OrderEvent_Failed:
			e.simulated["Release"]++
			_, err = e.inv.Release(t.Context(), &inventoryv1.ReleaseRequest{OrderId: ev.GetOrderId()})
		}
		if err != nil {
			t.Errorf("inventory rejected event for order %d: %v", ev.GetOrderId(), err)
		}
	}
	e.consumed = len(evs)
}

func items(pairs ...int64) []*orderv1.CreateOrderItem {
	var out []*orderv1.CreateOrderItem
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &orderv1.CreateOrderItem{SkuId: pairs[i], Quantity: int32(pairs[i+1])})
	}
	return out
}

func createReq(userID int64, key string, it []*orderv1.CreateOrderItem) *orderv1.CreateOrderRequest {
	return &orderv1.CreateOrderRequest{UserId: userID, IdempotencyKey: key, Items: it}
}

// mustPending 下一个 skuA×2 的订单并确认它进入待支付。
func (e *env) mustPending(t *testing.T, userID int64, key string) *orderv1.Order {
	t.Helper()
	resp, err := e.svc.CreateOrder(t.Context(), createReq(userID, key, items(skuA, 2)))
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if got := resp.GetOrder().GetStatus(); got != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
		t.Fatalf("status = %s, want PENDING_PAYMENT", got)
	}
	return resp.GetOrder()
}

func (e *env) load(t *testing.T, orderID int64) *store.Order {
	t.Helper()
	o, err := e.store.GetOrder(t.Context(), orderID)
	if err != nil {
		t.Fatalf("load order %d: %v", orderID, err)
	}
	return o
}

func (e *env) loadByKey(t *testing.T, userID int64, key string) *store.Order {
	t.Helper()
	o, err := e.store.GetOrderByIdempotencyKey(t.Context(), userID, key)
	if err != nil {
		t.Fatalf("load order by key: %v", err)
	}
	return o
}

func (e *env) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.db.QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func paymentEvent(orderID, paymentID, amount int64, succeeded bool) *eventv1.PaymentEvent {
	return &eventv1.PaymentEvent{
		PaymentId: paymentID,
		OrderId:   orderID,
		UserId:    1,
		Body: &eventv1.PaymentEvent_Completed{Completed: &eventv1.PaymentCompleted{
			Succeeded: succeeded,
			Amount:    &commonv1.Money{Amount: amount, Currency: "CNY"},
		}},
	}
}

// pay 投递一条支付结果，返回 outbox 里对这笔支付的答复。答复必须恰好有一条。
func (e *env) pay(t *testing.T, orderID, paymentID, amount int64, succeeded bool) (*eventv1.PaymentResultHandled, error) {
	t.Helper()
	return e.deliver(t, paymentEvent(orderID, paymentID, amount, succeeded))
}

func (e *env) deliver(t *testing.T, ev *eventv1.PaymentEvent) (*eventv1.PaymentResultHandled, error) {
	t.Helper()
	if err := e.svc.HandlePaymentEvent(t.Context(), ev); err != nil {
		return nil, err
	}
	return e.reply(t, ev.GetPaymentId()), nil
}

// reply 返回 outbox 里对 paymentID 的唯一一条答复。
func (e *env) reply(t *testing.T, paymentID int64) *eventv1.PaymentResultHandled {
	t.Helper()
	var found []*eventv1.PaymentResultHandled
	for _, ev := range e.allEvents(t) {
		if h := ev.GetPaymentResultHandled(); h != nil && h.GetPaymentId() == paymentID {
			found = append(found, h)
		}
	}
	if len(found) != 1 {
		t.Fatalf("payment %d has %d replies in outbox, want 1", paymentID, len(found))
	}
	return found[0]
}

// allEvents 按写入顺序返回 outbox 里的全部 OrderEvent，并检查 topic 和 key。
func (e *env) allEvents(t *testing.T) []*eventv1.OrderEvent {
	t.Helper()
	rows, err := e.db.QueryContext(t.Context(), "SELECT topic, msg_key, payload FROM outbox ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var evs []*eventv1.OrderEvent
	for rows.Next() {
		var (
			topic        string
			key, payload []byte
		)
		if err := rows.Scan(&topic, &key, &payload); err != nil {
			t.Fatal(err)
		}
		ev, err := eventx.UnmarshalOrderEvent(payload)
		if err != nil {
			t.Fatal(err)
		}
		if topic != testEventsTopic {
			t.Errorf("outbox topic = %q, want %q", topic, testEventsTopic)
		}
		if want := strconv.FormatInt(ev.GetOrderId(), 10); string(key) != want {
			t.Errorf("outbox key = %q, want %q", key, want)
		}
		evs = append(evs, ev)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return evs
}

// eventKinds 按写入顺序返回某个订单的事件种类。
func (e *env) eventKinds(t *testing.T, orderID int64) []string {
	t.Helper()
	var kinds []string
	for _, ev := range e.allEvents(t) {
		if ev.GetOrderId() != orderID {
			continue
		}
		switch ev.GetBody().(type) {
		case *eventv1.OrderEvent_Paid:
			kinds = append(kinds, "paid")
		case *eventv1.OrderEvent_Cancelled:
			kinds = append(kinds, "cancelled")
		case *eventv1.OrderEvent_Closed:
			kinds = append(kinds, "closed")
		case *eventv1.OrderEvent_Failed:
			kinds = append(kinds, "failed")
		case *eventv1.OrderEvent_PaymentResultHandled:
			kinds = append(kinds, "handled")
		default:
			kinds = append(kinds, "unknown")
		}
	}
	return kinds
}

func (e *env) wantEvents(t *testing.T, orderID int64, want ...string) {
	t.Helper()
	if got := e.eventKinds(t, orderID); !slices.Equal(got, want) {
		t.Fatalf("order %d events = %v, want %v", orderID, got, want)
	}
}

func (e *env) outboxRows(t *testing.T) int {
	t.Helper()
	return e.count(t, "SELECT COUNT(*) FROM outbox")
}

func wantErr(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	if errx.Code(err) != code || errx.Reason(err) != reason {
		t.Fatalf("err = %v (code %s, reason %q), want code %s reason %q",
			err, errx.Code(err), errx.Reason(err), code, reason)
	}
}

func wantStatus(t *testing.T, o *store.Order, st orderv1.OrderStatus) {
	t.Helper()
	if o.Status != st {
		t.Fatalf("order %d: status=%s, want %s", o.OrderID, o.Status, st)
	}
}

func wantStock(t *testing.T, inv *fakeInventory, sku, available, reserved, sold int64) {
	t.Helper()
	s := inv.stock(sku)
	if s.available != available || s.reserved != reserved || s.sold != sold {
		t.Fatalf("sku %d stock = %d/%d/%d, want %d/%d/%d (available/reserved/sold)",
			sku, s.available, s.reserved, s.sold, available, reserved, sold)
	}
}

func refOf(o *orderv1.Order) store.OrderRef {
	return store.OrderRef{OrderID: o.GetOrderId(), UserID: o.GetUserId()}
}
