package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"

	"github.com/palemoky/Oppenheimer/api/eventx"
	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	eventv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/event/v1"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/stockgate"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/pkg/testredis"
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
const (
	testEventsTopic = "test." + eventx.OrderEvents
	testSubmitTopic = "test." + eventx.OrderSubmit
	testCloseTopic  = "test." + eventx.OrderCloseDelay
)

// sendFault 决定一次 Send 的结果。persist[i] 为 true 的记录照样“写进去”（被记下来），
// 然后返回 errs。用来模拟“发送失败但消息其实到了”。
type sendFault struct {
	persist []bool
	errs    []error
	// after 在记录之后、返回之前调用，用来在“消息已到、发送方还没拿到结果”的窗口里制造并发。
	after func(recs []kafkax.Record)
}

var errSend = errors.New("fake: produce failed")

// fakeSender 是内存里的 Kafka：记下发出的每条记录，可以按顺序注入失败。
type fakeSender struct {
	mu     sync.Mutex
	recs   []kafkax.Record
	sends  int
	faults []sendFault
}

func (f *fakeSender) inject(fs ...sendFault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults = append(f.faults, fs...)
}

func (f *fakeSender) Send(ctx context.Context, recs ...kafkax.Record) ([]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.sends++
	var ft *sendFault
	if len(f.faults) > 0 {
		ft = &f.faults[0]
		f.faults = f.faults[1:]
	}
	if ft == nil {
		f.recs = append(f.recs, recs...)
		f.mu.Unlock()
		return make([]error, len(recs)), nil
	}
	for i, r := range recs {
		if i < len(ft.persist) && ft.persist[i] {
			f.recs = append(f.recs, r)
		}
	}
	f.mu.Unlock()
	if ft.after != nil {
		ft.after(recs)
	}
	errs := make([]error, len(recs))
	copy(errs, ft.errs)
	return errs, errors.Join(errs...)
}

func (f *fakeSender) sendCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sends
}

// records 返回发往 topic 的全部记录。
func (f *fakeSender) records(topic string) []kafkax.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []kafkax.Record
	for _, r := range f.recs {
		if r.Topic == topic {
			out = append(out, r)
		}
	}
	return out
}

type env struct {
	db      *sql.DB
	store   *store.Store
	rdb     *redis.Client
	prefix  string
	gate    *stockgate.Gate
	sender  *fakeSender
	inv     *fakeInventory
	clock   *fakeClock
	relay   *fakeRelay
	svc     *Service
	sweeper *Sweeper
	reaper  *Reaper

	// consumed 是 consumeEvents 已经处理到的 outbox 行数；simulated 是它代替库存服务发起的调用次数。
	mu        sync.Mutex
	consumed  int
	simulated map[string]int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testdb.New(t, order.Migrations, order.MigrationsDir)
	rdb, prefix := testredis.New(t)
	gate := stockgate.New(rdb, prefix)
	inv := newFakeInventory(gate)
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
	sender := &fakeSender{}
	svc := New(Deps{Store: st, Gate: gate, Sender: sender, Inventory: inv, IDs: ids, Logger: logger}, Config{
		Clock:            clock.Now,
		OrderEventsTopic: testEventsTopic,
		SubmitTopic:      testSubmitTopic,
		CloseDelayTopic:  testCloseTopic,
		Relay:            relay,
	})
	sweeper, err := NewSweeper(svc, SweeperConfig{})
	if err != nil {
		t.Fatal(err)
	}
	reaper, err := NewReaper(svc, ReaperConfig{})
	if err != nil {
		t.Fatal(err)
	}
	e := &env{
		db:        db,
		store:     st,
		rdb:       rdb,
		prefix:    prefix,
		gate:      gate,
		sender:    sender,
		inv:       inv,
		clock:     clock,
		relay:     relay,
		svc:       svc,
		sweeper:   sweeper,
		reaper:    reaper,
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
// paid 确认预占后结清占用，cancelled、closed、failed 释放预占后归还占用（先 MySQL 后 Redis）。
// 用来验证“事件发出去之后库存不会泄漏”。
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
			if _, err = e.inv.Confirm(t.Context(), &inventoryv1.ConfirmRequest{OrderId: ev.GetOrderId()}); err == nil {
				_, err = e.gate.Settle(t.Context(), ev.GetOrderId())
			}
		case *eventv1.OrderEvent_Cancelled, *eventv1.OrderEvent_Closed, *eventv1.OrderEvent_Failed:
			e.simulated["Release"]++
			if _, err = e.inv.Release(t.Context(), &inventoryv1.ReleaseRequest{OrderId: ev.GetOrderId()}); err == nil {
				_, err = e.gate.Return(t.Context(), ev.GetOrderId())
			}
		}
		if err != nil {
			t.Errorf("inventory rejected event for order %d: %v", ev.GetOrderId(), err)
		}
	}
	e.consumed = len(evs)
}

func items(pairs ...int64) []*orderv1.SubmitOrderItem {
	var out []*orderv1.SubmitOrderItem
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &orderv1.SubmitOrderItem{SkuId: pairs[i], Quantity: int32(pairs[i+1])})
	}
	return out
}

func submitReq(userID int64, key string, it []*orderv1.SubmitOrderItem) *orderv1.SubmitOrderRequest {
	return &orderv1.SubmitOrderRequest{UserId: userID, IdempotencyKey: key, Items: it}
}

// submit 受理一个下单请求，要求成功并返回 CREATING 的订单。
func (e *env) submit(t *testing.T, userID int64, key string, it []*orderv1.SubmitOrderItem) *orderv1.Order {
	t.Helper()
	resp, err := e.svc.SubmitOrder(t.Context(), submitReq(userID, key, it))
	if err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}
	return resp.GetOrder()
}

// submitMessage 返回受理时为 orderID 发出的 order.submit 消息（作为消费者收到的 kafkax.Message）。
func (e *env) submitMessage(t *testing.T, orderID int64) kafkax.Message {
	t.Helper()
	for _, r := range e.sender.records(testSubmitTopic) {
		if string(r.Key) == strconv.FormatInt(orderID, 10) {
			return kafkax.Message{Topic: r.Topic, Key: r.Key, Value: r.Value, Headers: r.Headers}
		}
	}
	t.Fatalf("no order.submit message for order %d", orderID)
	return kafkax.Message{}
}

// land 扮演 order.submit 的消费者处理 orderID 的消息一次，经过 Kafka 适配层（解码、时限、死信判断）。
func (e *env) land(t *testing.T, orderID int64) error {
	t.Helper()
	return e.svc.SubmitHandler(0)(t.Context(), e.submitMessage(t, orderID))
}

func (e *env) mustLand(t *testing.T, orderID int64) {
	t.Helper()
	if err := e.land(t, orderID); err != nil {
		t.Fatalf("land order %d: %v", orderID, err)
	}
}

// mustPending 下一个 skuA×2 的订单、落库，并确认它进入待支付。
func (e *env) mustPending(t *testing.T, userID int64, key string) *orderv1.Order {
	t.Helper()
	o := e.submit(t, userID, key, items(skuA, 2))
	e.mustLand(t, o.GetOrderId())
	stored := e.load(t, o.GetOrderId())
	wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	return toProto(stored)
}

// mustCreating 下一个 skuA×1 的订单并落库，但让 Reserve 超时，订单停在 CREATING。
func (e *env) mustCreating(t *testing.T, userID int64, key string) *store.Order {
	t.Helper()
	o := e.submit(t, userID, key, items(skuA, 1))
	e.inv.inject("Reserve", fault{err: errTimeout})
	if err := e.land(t, o.GetOrderId()); !errors.Is(err, errReserveUnknown) {
		t.Fatalf("land err = %v, want reserve outcome unknown", err)
	}
	stored := e.load(t, o.GetOrderId())
	wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_CREATING)
	return stored
}

func (e *env) load(t *testing.T, orderID int64) *store.Order {
	t.Helper()
	o, err := e.store.GetOrder(t.Context(), orderID)
	if err != nil {
		t.Fatalf("load order %d: %v", orderID, err)
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

func (e *env) orderRows(t *testing.T) int {
	t.Helper()
	return e.count(t, "SELECT COUNT(*) FROM orders")
}

// wantGate 断言闸门里 sku 的可放行数量和占用数量。
func (e *env) wantGate(t *testing.T, sku, avail, held int64) {
	t.Helper()
	st, ok, err := e.gate.Stock(t.Context(), sku)
	if err != nil || !ok {
		t.Fatalf("gate stock of sku %d: loaded=%v err=%v", sku, ok, err)
	}
	if st.Avail != avail || st.Held != held {
		t.Fatalf("gate sku %d = avail %d held %d, want avail %d held %d", sku, st.Avail, st.Held, avail, held)
	}
}

// wantBalanced 断言稳定状态下的恒等式（设计文档 6.6）：闸门的 avail + held 等于库存的 available + reserved。
func (e *env) wantBalanced(t *testing.T, sku int64) {
	t.Helper()
	st, ok, err := e.gate.Stock(t.Context(), sku)
	if err != nil || !ok {
		t.Fatalf("gate stock of sku %d: loaded=%v err=%v", sku, ok, err)
	}
	s := e.inv.stock(sku)
	if st.Avail+st.Held != s.available+s.reserved {
		t.Fatalf("sku %d unbalanced: gate avail %d + held %d != inventory available %d + reserved %d",
			sku, st.Avail, st.Held, s.available, s.reserved)
	}
}

// hold 读 orderID 的占用。
func (e *env) hold(t *testing.T, orderID int64) (stockgate.HoldInfo, bool) {
	t.Helper()
	h, ok, err := e.gate.Get(t.Context(), orderID)
	if err != nil {
		t.Fatal(err)
	}
	return h, ok
}

// expireIdempotencyKeys 删掉闸门里的全部幂等键，模拟它们过了保留时长。
func (e *env) expireIdempotencyKeys(t *testing.T) {
	t.Helper()
	iter := e.rdb.Scan(t.Context(), 0, e.prefix+"gate:idem:*", 100).Iterator()
	for iter.Next(t.Context()) {
		if err := e.rdb.Del(t.Context(), iter.Val()).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
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
