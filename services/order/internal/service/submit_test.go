package service

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/palemoky/Oppenheimer/api/eventx"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/redisx"
	"github.com/palemoky/Oppenheimer/pkg/stockgate"
	"github.com/palemoky/Oppenheimer/pkg/testredis"
)

func TestSubmitValidation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	cases := map[string]*orderv1.SubmitOrderRequest{
		"zero user":     submitReq(0, "k", items(skuA, 1)),
		"empty key":     submitReq(1, "", items(skuA, 1)),
		"long key":      submitReq(1, strings.Repeat("k", 65), items(skuA, 1)),
		"no items":      submitReq(1, "k", nil),
		"zero quantity": submitReq(1, "k", items(skuA, 0)),
		"negative qty":  submitReq(1, "k", items(skuA, -1)),
		"zero sku":      submitReq(1, "k", items(0, 1)),
		"duplicate sku": submitReq(1, "k", items(skuA, 1, skuB, 1, skuA, 2)),
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := e.svc.SubmitOrder(t.Context(), req)
			wantErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
		})
	}
	if n := e.inv.callCount("WarmUp"); n != 0 {
		t.Errorf("warm up called %d times for invalid requests", n)
	}
	if e.sender.sendCount() != 0 {
		t.Errorf("sent %d times for invalid requests", e.sender.sendCount())
	}
	e.submit(t, 1, strings.Repeat("k", 64), items(skuA, 1))
}

func TestSubmitAccepted(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 7, "k1", items(skuB, 3, skuA, 2))

	if o.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CREATING || o.GetUserId() != 7 {
		t.Fatalf("order = %v", o)
	}
	// 只有商品和数量，没有价格。
	if o.GetTotal() != nil || len(o.GetItems()) != 2 ||
		o.GetItems()[0].GetSkuId() != skuA || o.GetItems()[0].GetQuantity() != 2 || o.GetItems()[0].GetUnitPrice() != nil ||
		o.GetItems()[1].GetSkuId() != skuB || o.GetItems()[1].GetQuantity() != 3 {
		t.Errorf("items/total = %v / %v", o.GetItems(), o.GetTotal())
	}
	accepted := e.clock.Now()
	if !o.GetCreateTime().AsTime().Equal(accepted) || !o.GetExpireTime().AsTime().Equal(accepted.Add(DefaultPaymentTimeout)) {
		t.Errorf("create/expire = %s / %s", o.GetCreateTime().AsTime(), o.GetExpireTime().AsTime())
	}
	// 受理只扣闸门，不写订单库、不预占。
	if n := e.orderRows(t); n != 0 {
		t.Errorf("orders = %d after submit, want 0", n)
	}
	if n := e.inv.callCount("Reserve"); n != 0 {
		t.Errorf("reserve called %d times during submit", n)
	}
	e.wantGate(t, skuA, 998, 2)
	e.wantGate(t, skuB, 997, 3)

	// 一次 Send 发出两条消息，key 都是订单号。
	if e.sender.sendCount() != 1 {
		t.Errorf("sends = %d, want 1", e.sender.sendCount())
	}
	subs := e.sender.records(testSubmitTopic)
	dues := e.sender.records(testCloseTopic)
	if len(subs) != 1 || len(dues) != 1 {
		t.Fatalf("submit/close records = %d/%d, want 1/1", len(subs), len(dues))
	}
	m, err := eventx.UnmarshalOrderSubmitted(subs[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if m.GetOrderId() != o.GetOrderId() || m.GetUserId() != 7 || m.GetIdempotencyKey() != "k1" ||
		len(m.GetRequestHash()) != 32 || len(m.GetItems()) != 2 ||
		!m.GetAcceptTime().AsTime().Equal(accepted) || !m.GetExpireTime().AsTime().Equal(o.GetExpireTime().AsTime()) {
		t.Errorf("order submitted = %v", m)
	}
	due, err := eventx.UnmarshalOrderCloseDue(dues[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if due.GetOrderId() != o.GetOrderId() || due.GetUserId() != 7 || !due.GetExpireTime().AsTime().Equal(o.GetExpireTime().AsTime()) {
		t.Errorf("close due = %v", due)
	}
	if got, want := dues[0].Headers[kafkax.DueHeader], o.GetExpireTime().AsTime().Format(time.RFC3339Nano); got != want {
		t.Errorf("due header = %q, want %q", got, want)
	}
	if key := strconv.FormatInt(o.GetOrderId(), 10); string(subs[0].Key) != key || string(dues[0].Key) != key {
		t.Errorf("keys = %q, %q", subs[0].Key, dues[0].Key)
	}
}

// 受理路径不访问 MySQL：Store 为空时照样能受理（访问了就会空指针）。
func TestSubmitDoesNotTouchMySQL(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	svc := mustNew(t, Deps{Gate: e.gate, Sender: e.sender, Inventory: e.inv, IDs: ids, Logger: e.svc.logger},
		Config{Clock: e.clock.Now, SubmitTopic: testSubmitTopic, CloseDelayTopic: testCloseTopic})
	for _, it := range [][]*orderv1.SubmitOrderItem{items(skuA, 1), items(skuScarce, 5), items(skuMissing, 1)} {
		_, err := svc.SubmitOrder(t.Context(), submitReq(1, fmt.Sprint(it), it))
		if err != nil && errx.Code(err) == codes.Internal {
			t.Fatal(err)
		}
	}
	e.wantGate(t, skuA, 999, 1)
}

func TestSubmitWarmUp(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, ok, _ := e.gate.Stock(t.Context(), skuA); ok {
		t.Fatal("sku loaded before first submit")
	}
	// 第一次遇到未装载的 SKU：预热后重试成功。一次预热请求里带上全部 SKU。
	e.submit(t, 1, "a", items(skuA, 1, skuB, 1))
	if n := e.inv.callCount("WarmUp"); n != 1 {
		t.Fatalf("warm up called %d times, want 1", n)
	}
	e.wantGate(t, skuA, 999, 1)
	e.wantGate(t, skuB, 999, 1)
	// 已装载的不再预热。
	e.submit(t, 1, "b", items(skuB, 1))
	if n := e.inv.callCount("WarmUp"); n != 1 {
		t.Fatalf("warm up called %d times, want 1", n)
	}

	// 库存服务里不存在的 SKU：同步返回 SKU_NOT_FOUND，什么都不扣、不发。
	sends := e.sender.sendCount()
	_, err := e.svc.SubmitOrder(t.Context(), submitReq(1, "missing", items(skuA, 1, skuMissing, 1)))
	wantErr(t, err, codes.NotFound, errx.ReasonSKUNotFound)
	e.wantGate(t, skuA, 999, 1)
	if e.sender.sendCount() != sends {
		t.Error("sent messages for a rejected request")
	}

	// 预热失败：UNAVAILABLE，之后用同一幂等键重试成功。
	e.inv.inject("WarmUp", fault{err: errTimeout})
	_, err = e.svc.SubmitOrder(t.Context(), submitReq(1, "scarce", items(skuScarce, 1)))
	if errx.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
	e.submit(t, 1, "scarce", items(skuScarce, 1))
	e.wantGate(t, skuScarce, 0, 1)
}

func TestSubmitRejected(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.inv.setLimit(skuB, 3)

	_, err := e.svc.SubmitOrder(t.Context(), submitReq(1, "scarce", items(skuScarce, 2)))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonInsufficientStock)
	e.wantGate(t, skuScarce, 1, 0)

	// 限购 3：先买 2 再买 2 超限；别的用户不受影响；多 SKU 订单有一个超限就都不扣。
	e.submit(t, 1, "b1", items(skuB, 2))
	_, err = e.svc.SubmitOrder(t.Context(), submitReq(1, "b2", items(skuB, 2)))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonPurchaseLimit)
	_, err = e.svc.SubmitOrder(t.Context(), submitReq(1, "b3", items(skuA, 5, skuB, 2)))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonPurchaseLimit)
	e.wantGate(t, skuA, 1000, 0)
	e.submit(t, 2, "b1", items(skuB, 3))
	e.submit(t, 1, "b4", items(skuB, 1))
	e.wantGate(t, skuB, 994, 6)

	// 幂等键复用到不同内容。
	for _, it := range [][]*orderv1.SubmitOrderItem{items(skuB, 1), items(skuA, 2), items(skuB, 2, skuA, 1)} {
		_, err := e.svc.SubmitOrder(t.Context(), submitReq(1, "b1", it))
		wantErr(t, err, codes.AlreadyExists, errx.ReasonIdempotencyKeyReused)
	}
	e.wantGate(t, skuB, 994, 6)
	if n := len(e.sender.records(testSubmitTopic)); n != 3 {
		t.Errorf("submit messages = %d, want 3", n)
	}
}

func TestSubmitIdempotentReplay(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	first := e.submit(t, 1, "k", items(skuA, 2, skuB, 1))
	// 商品顺序不同但内容相同，仍是同一个请求：同一订单、不再扣减、不再发消息。
	second := e.submit(t, 1, "k", items(skuB, 1, skuA, 2))
	if first.GetOrderId() != second.GetOrderId() || second.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CREATING {
		t.Fatalf("replay = %v, want order %d CREATING", second, first.GetOrderId())
	}
	if !second.GetExpireTime().AsTime().Equal(first.GetExpireTime().AsTime()) || len(second.GetItems()) != 2 {
		t.Errorf("replay order = %v, first = %v", second, first)
	}
	e.wantGate(t, skuA, 998, 2)
	if e.sender.sendCount() != 1 {
		t.Errorf("sends = %d, want 1", e.sender.sendCount())
	}

	// 落库之后重放返回订单的当前状态（与 GetOrder 一样，带上价格）。
	e.mustLand(t, first.GetOrderId())
	third := e.submit(t, 1, "k", items(skuA, 2, skuB, 1))
	if third.GetOrderId() != first.GetOrderId() || third.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT ||
		third.GetTotal().GetAmount() != 2*priceA+priceB {
		t.Errorf("replay after landing = %v", third)
	}
	if e.sender.sendCount() != 1 || e.inv.appliedCount("Reserve") != 1 {
		t.Errorf("sends = %d, reserves = %d, want 1, 1", e.sender.sendCount(), e.inv.appliedCount("Reserve"))
	}

	// 同一幂等键属于另一个用户时互不相干。
	other := e.submit(t, 2, "k", items(skuA, 1))
	if other.GetOrderId() == first.GetOrderId() {
		t.Error("other user got the same order")
	}
}

func TestSubmitConcurrentSameKey(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.submit(t, 9, "warm", items(skuA, 1)) // 先装载，让并发的请求都走 Hold
	const n = 16
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		ids   = map[int64]int{}
		start = make(chan struct{})
	)
	for range n {
		wg.Go(func() {
			<-start
			resp, err := e.svc.SubmitOrder(t.Context(), submitReq(1, "same", items(skuA, 2)))
			if err != nil {
				t.Errorf("SubmitOrder: %v", err)
				return
			}
			mu.Lock()
			ids[resp.GetOrder().GetOrderId()]++
			mu.Unlock()
		})
	}
	close(start)
	wg.Wait()
	if len(ids) != 1 {
		t.Fatalf("got %d distinct order ids, want 1: %v", len(ids), ids)
	}
	e.wantGate(t, skuA, 997, 3)
	if n := len(e.sender.records(testSubmitTopic)); n != 2 {
		t.Errorf("submit messages = %d, want 2", n)
	}
}

// 并发受理不超过库存：放进来的恰好是闸门里的数量，落库后 MySQL 一侧也恰好卖出这么多。
func TestSubmitConcurrentDoesNotOversell(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const stock, users = 5, 40
	e.inv.addSKU(10, 30, stock)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted []int64
		start    = make(chan struct{})
	)
	for u := range users {
		wg.Go(func() {
			<-start
			resp, err := e.svc.SubmitOrder(t.Context(), submitReq(int64(u+1), "k", items(10, 1)))
			switch {
			case err == nil:
				mu.Lock()
				accepted = append(accepted, resp.GetOrder().GetOrderId())
				mu.Unlock()
			case errx.Reason(err) != errx.ReasonInsufficientStock:
				t.Errorf("SubmitOrder: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	if len(accepted) != stock {
		t.Fatalf("accepted %d, want %d", len(accepted), stock)
	}
	e.wantGate(t, 10, 0, stock)
	for _, id := range accepted {
		e.mustLand(t, id)
		wantStatus(t, e.load(t, id), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	}
	wantStock(t, e.inv, 10, 0, stock, 0)
	e.wantBalanced(t, 10)
}

func TestSubmitSendFailure(t *testing.T) {
	t.Parallel()
	for _, persisted := range []bool{false, true} {
		t.Run(fmt.Sprintf("persisted=%v", persisted), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			e.sender.inject(sendFault{persist: []bool{persisted, persisted}, errs: []error{errSend, errSend}})
			_, err := e.svc.SubmitOrder(t.Context(), submitReq(1, "k", items(skuA, 2)))
			if errx.Code(err) != codes.Unavailable {
				t.Fatalf("err = %v, want Unavailable", err)
			}
			// 占用已归还，幂等键已删除。
			e.wantGate(t, skuA, 1000, 0)

			if persisted {
				// 消息其实到了：消费者 Claim 不到，丢弃它，不产生订单。
				lost := e.sender.records(testSubmitTopic)[0]
				if err := e.svc.SubmitHandler(0)(t.Context(), kafkax.Message{Topic: lost.Topic, Value: lost.Value}); err != nil {
					t.Fatalf("land lost message: %v", err)
				}
				if n := e.orderRows(t); n != 0 {
					t.Fatalf("orders = %d, want 0", n)
				}
			}

			// 同一幂等键重新下单：新的订单号，正常受理、落库。
			o := e.submit(t, 1, "k", items(skuA, 2))
			e.wantGate(t, skuA, 998, 2)
			e.mustLand(t, o.GetOrderId())
			wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
			if n := e.orderRows(t); n != 1 {
				t.Errorf("orders = %d, want 1", n)
			}
		})
	}
}

// 发送报错，但消息已到、消费者已经认领：受理方撤销不了，按受理成功作答，订单照常落库。
func TestSubmitSendFailureAfterClaim(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.sender.inject(sendFault{
		persist: []bool{true, true},
		errs:    []error{errSend, nil},
		after: func(recs []kafkax.Record) {
			m, err := eventx.UnmarshalOrderSubmitted(recs[0].Value)
			if err != nil {
				t.Error(err)
				return
			}
			if _, _, err := e.gate.Claim(t.Context(), m.GetOrderId(), e.clock.Now()); err != nil {
				t.Error(err)
			}
		},
	})
	o := e.submit(t, 1, "k", items(skuA, 2))
	if o.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CREATING {
		t.Fatalf("status = %s", o.GetStatus())
	}
	e.wantGate(t, skuA, 998, 2)
	e.mustLand(t, o.GetOrderId())
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	// 幂等键还指向它。
	if again := e.submit(t, 1, "k", items(skuA, 2)); again.GetOrderId() != o.GetOrderId() {
		t.Errorf("replay = %d, want %d", again.GetOrderId(), o.GetOrderId())
	}
}

// 只有延迟关单的消息没发出去：受理成功，关单交给 sweeper 兜底。
func TestSubmitCloseDelaySendFailure(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.sender.inject(sendFault{persist: []bool{true, false}, errs: []error{nil, errSend}})
	o := e.submit(t, 1, "k", items(skuA, 2))
	e.wantGate(t, skuA, 998, 2)
	if n := len(e.sender.records(testCloseTopic)); n != 0 {
		t.Fatalf("close records = %d", n)
	}
	e.mustLand(t, o.GetOrderId())
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
}

// Redis 不可用：Hold 结果未知，返回 UNAVAILABLE，不发消息。
func TestSubmitGateUnavailable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rdb, prefix := testredis.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	broken, err := redisx.Open(ctx, redisx.Config{Addr: rdb.Options().Addr})
	if err != nil {
		t.Fatal(err)
	}
	if err := broken.Close(); err != nil {
		t.Fatal(err)
	}
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	svc := mustNew(t, Deps{
		Store: e.store, Gate: stockgate.New(broken, prefix), Sender: e.sender, Inventory: e.inv, IDs: ids,
		Logger: slog.New(slog.DiscardHandler),
	}, Config{Clock: e.clock.Now})
	_, err = svc.SubmitOrder(t.Context(), submitReq(1, "k", items(skuA, 1)))
	if errx.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
	if e.sender.sendCount() != 0 {
		t.Errorf("sends = %d, want 0", e.sender.sendCount())
	}
	// 查询也是 UNAVAILABLE，而不是把“读不到占用”当成订单不存在。
	_, err = svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: 12345, UserId: 1})
	if errx.Code(err) != codes.Unavailable {
		t.Fatalf("GetOrder err = %v, want Unavailable", err)
	}
}

// 等待落库的订单积压过多时拒绝受理：返回可重试的 RESOURCE_EXHAUSTED，不扣库存、不发消息；
// 积压消化之后，同一个幂等键能正常受理。
func TestSubmitBackpressure(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	svc := mustNew(t, Deps{Store: e.store, Gate: e.gate, Sender: e.sender, Inventory: e.inv, IDs: ids, Logger: e.svc.logger},
		Config{
			Clock: e.clock.Now, OrderEventsTopic: testEventsTopic, SubmitTopic: testSubmitTopic, CloseDelayTopic: testCloseTopic,
			Backlog: stockgate.BacklogLimit{MaxWait: 30 * time.Second, MaxStale: 1},
		})
	submit := func(user int64, key string) (*orderv1.Order, error) {
		resp, err := svc.SubmitOrder(t.Context(), submitReq(user, key, items(skuA, 1)))
		return resp.GetOrder(), err
	}

	// 两单受理之后一直没有落库。
	first, err := submit(1, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := submit(2, "b"); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(31 * time.Second)

	sent := len(e.sender.records(testSubmitTopic))
	_, err = submit(3, "c")
	wantErr(t, err, codes.ResourceExhausted, errx.ReasonBacklogged)
	e.wantGate(t, skuA, 998, 2)
	if n := len(e.sender.records(testSubmitTopic)); n != sent {
		t.Fatalf("submit messages %d → %d, a backlogged request must not be sent", sent, n)
	}

	// 已经受理过的请求重试时照常拿到自己的订单。
	again, err := submit(1, "a")
	if err != nil || again.GetOrderId() != first.GetOrderId() {
		t.Fatalf("replay while backlogged: %v, %v", again.GetOrderId(), err)
	}

	// 落库一单之后只剩一个排队超时，不多于 MaxStale：被拒绝的请求用同一个幂等键重试成功。
	e.mustLand(t, first.GetOrderId())
	o, err := submit(3, "c")
	if err != nil {
		t.Fatalf("retry after the backlog drained: %v", err)
	}
	if o.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CREATING {
		t.Fatalf("status = %v, want CREATING", o.GetStatus())
	}
	e.wantGate(t, skuA, 997, 3)
}

// 落库停住、排队的订单被回收之后，受理只放进少量探测订单；消费者恢复后，即使它最先处理的是
// 占用早已被回收的消息（这条消息会被丢弃），也说明落库活着，受理随即恢复。
func TestSubmitResumesAfterLandingDropsReapedOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	svc := mustNew(t, Deps{Store: e.store, Gate: e.gate, Sender: e.sender, Inventory: e.inv, IDs: ids, Logger: e.svc.logger},
		Config{
			Clock: e.clock.Now, OrderEventsTopic: testEventsTopic, SubmitTopic: testSubmitTopic, CloseDelayTopic: testCloseTopic,
			Backlog: stockgate.BacklogLimit{MaxWait: 30 * time.Second, MaxStale: 1},
		})
	submit := func(user int64, key string) (*orderv1.Order, error) {
		resp, err := svc.SubmitOrder(t.Context(), submitReq(user, key, items(skuA, 1)))
		return resp.GetOrder(), err
	}

	// 两单受理之后消费者停住，宽限期过后被回收。
	reaped, err := submit(1, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := submit(2, "b"); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(DefaultReapGrace + time.Second)
	if st := e.reaper.RunOnce(t.Context()); st.Returned != 2 {
		t.Fatalf("reaped %d, want 2", st.Returned)
	}

	// 落库已超过 MaxWait 没有动静：只放进 MaxStale+1 个探测订单。
	for i, key := range []string{"c", "d"} {
		if _, err := submit(int64(3+i), key); err != nil {
			t.Fatalf("probe %s: %v", key, err)
		}
	}
	_, err = submit(5, "e")
	wantErr(t, err, codes.ResourceExhausted, errx.ReasonBacklogged)

	// 消费者恢复，先处理到已被回收的订单：丢弃它，但刷新了心跳。
	e.mustLand(t, reaped.GetOrderId())
	if _, ok := e.hold(t, reaped.GetOrderId()); ok {
		t.Fatal("reaped order got a hold back")
	}
	if n := e.orderRows(t); n != 0 {
		t.Fatalf("orders = %d, want 0", n)
	}
	if _, err := submit(5, "e"); err != nil {
		t.Fatalf("after landing resumed: %v", err)
	}
}

// 新 SKU 开卖瞬间的并发首单只触发一次预热，全部受理成功。
func TestSubmitConcurrentWarmUpIsCoalesced(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// 让第一次预热慢一点，其余请求一定会在它进行中到达。
	e.inv.inject("WarmUp", fault{before: func(int64) { time.Sleep(200 * time.Millisecond) }})

	const buyers = 50
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range buyers {
		wg.Go(func() {
			<-start
			if _, err := e.svc.SubmitOrder(t.Context(), submitReq(int64(i+1), "k", items(skuA, 1))); err != nil {
				t.Errorf("buyer %d: %v", i, err)
			}
		})
	}
	close(start)
	wg.Wait()
	if n := e.inv.callCount("WarmUp"); n != 1 {
		t.Fatalf("warm up called %d times, want 1", n)
	}
	e.wantGate(t, skuA, 1000-buyers, buyers)
}
