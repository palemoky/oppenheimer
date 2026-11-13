package service

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/palemoky/Oppenheimer/api/eventx"
	eventv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/event/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

func TestLandSuccess(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	sub := e.submit(t, 7, "k1", items(skuB, 3, skuA, 2))
	accepted := e.clock.Now()
	// 落库比受理晚：创建时间、支付截止时间仍以受理时为准。
	e.clock.Advance(5 * time.Second)
	e.mustLand(t, sub.GetOrderId())

	stored := e.load(t, sub.GetOrderId())
	wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if want := int64(2*priceA + 3*priceB); stored.TotalAmount != want || stored.Currency != "CNY" {
		t.Errorf("total = %d %s, want %d CNY", stored.TotalAmount, stored.Currency, want)
	}
	if !stored.CreatedAt.Equal(accepted) || !stored.ExpireAt.Equal(accepted.Add(DefaultPaymentTimeout)) {
		t.Errorf("created/expire = %s / %s", stored.CreatedAt, stored.ExpireAt)
	}
	if stored.IdempotencyKey != "k1" || stored.Items[0].UnitPrice != priceA || stored.Items[1].UnitPrice != priceB {
		t.Errorf("stored = %+v", stored)
	}
	wantStock(t, e.inv, skuA, 998, 2, 0)
	wantStock(t, e.inv, skuB, 997, 3, 0)
	// 占用被认领，留到订单终态才归还。
	if h, ok := e.hold(t, sub.GetOrderId()); !ok || !h.Claimed {
		t.Errorf("hold = %+v, %v; want claimed", h, ok)
	}
	e.wantBalanced(t, skuA)

	got, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: sub.GetOrderId(), UserId: 7})
	if err != nil || got.GetOrder().GetTotal().GetAmount() != stored.TotalAmount ||
		got.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
		t.Errorf("GetOrder = %v, %v", got, err)
	}
	// 付款、库存服务确认并结清之后，两边仍然平衡。
	if _, err := e.pay(t, sub.GetOrderId(), 1, stored.TotalAmount, true); err != nil {
		t.Fatal(err)
	}
	e.consumeEvents(t)
	wantStock(t, e.inv, skuA, 998, 0, 2)
	e.wantGate(t, skuA, 998, 0)
	e.wantBalanced(t, skuA)
}

// 同一条消息处理两次（先后或并发）：只有一个订单、一份预占。
func TestLandTwice(t *testing.T) {
	t.Parallel()
	for _, concurrent := range []bool{false, true} {
		t.Run(fmt.Sprintf("concurrent=%v", concurrent), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			o := e.submit(t, 1, "k", items(skuA, 2))
			if concurrent {
				var wg sync.WaitGroup
				for range 8 {
					wg.Go(func() {
						if err := e.land(t, o.GetOrderId()); err != nil {
							t.Errorf("land: %v", err)
						}
					})
				}
				wg.Wait()
			} else {
				e.mustLand(t, o.GetOrderId())
				e.mustLand(t, o.GetOrderId())
			}
			if n := e.orderRows(t); n != 1 {
				t.Fatalf("orders = %d, want 1", n)
			}
			if n := e.count(t, "order_items", ""); n != 1 {
				t.Errorf("order_items = %d, want 1", n)
			}
			if n := e.inv.appliedCount("Reserve"); n != 1 {
				t.Errorf("reserve applied %d times, want 1", n)
			}
			wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
			wantStock(t, e.inv, skuA, 998, 2, 0)
			e.wantGate(t, skuA, 998, 2)
		})
	}
}

// 订单走到终态、占用被归还之后，迟到的重复消息被丢弃。
func TestLandRedeliveredAfterTerminal(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	e.consumeEvents(t)
	e.wantGate(t, skuA, 1000, 0)
	e.mustLand(t, o.GetOrderId())
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	if n := e.inv.callCount("Reserve"); n != 1 {
		t.Errorf("reserve called %d times, want 1", n)
	}
}

// 占用被回收后消息才到：丢弃，不产生订单，库存不变。
func TestLandAfterReap(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 1, "k", items(skuA, 2))
	e.clock.Advance(DefaultReapGrace + time.Second)
	if st := e.reaper.RunOnce(t.Context()); st.Returned != 1 {
		t.Fatalf("reaped = %+v, want 1 returned", st)
	}
	e.mustLand(t, o.GetOrderId())
	if n := e.orderRows(t); n != 0 {
		t.Fatalf("orders = %d, want 0", n)
	}
	if n := e.inv.callCount("Reserve"); n != 0 {
		t.Errorf("reserve called %d times", n)
	}
	e.wantGate(t, skuA, 1000, 0)
	_, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: o.GetOrderId(), UserId: 1})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
}

// 幂等键在 Redis 里过期后的重放：订单库的唯一键挡住第二个订单，占用归还，幂等键补回指向原订单。
func TestLandReplayAfterIdempotencyExpiry(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	a := e.mustPending(t, 1, "k")
	e.wantGate(t, skuA, 998, 2)

	e.expireIdempotencyKeys(t)
	b := e.submit(t, 1, "k", items(skuA, 2))
	if b.GetOrderId() == a.GetOrderId() {
		t.Fatal("replay after expiry should be accepted as a new order by the gate")
	}
	e.wantGate(t, skuA, 996, 4)

	for range 2 { // 重复投递也一样
		e.mustLand(t, b.GetOrderId())
	}
	if n := e.orderRows(t); n != 1 {
		t.Fatalf("orders = %d, want 1", n)
	}
	if n := e.inv.callCount("Reserve"); n != 1 {
		t.Errorf("reserve called %d times, want 1", n)
	}
	e.wantGate(t, skuA, 998, 2)
	if _, ok := e.hold(t, b.GetOrderId()); ok {
		t.Error("hold of the replayed order still exists")
	}
	if _, ok := e.hold(t, a.GetOrderId()); !ok {
		t.Error("hold of the original order was removed")
	}
	// 幂等键已补回：再重放直接得到原订单，不再扣减。
	again := e.submit(t, 1, "k", items(skuA, 2))
	if again.GetOrderId() != a.GetOrderId() || again.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
		t.Errorf("replay = %v, want order %d", again, a.GetOrderId())
	}
	e.wantGate(t, skuA, 998, 2)
	_, err := e.svc.SubmitOrder(t.Context(), submitReq(1, "k", items(skuA, 3)))
	wantErr(t, err, codes.AlreadyExists, errx.ReasonIdempotencyKeyReused)
	_, err = e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: b.GetOrderId(), UserId: 1})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
}

// Reserve 被拒绝（闸门放进来了，MySQL 没有库存给它）：订单 FAILED 并写 failed 事件，消息处理成功。
// 库存服务消费事件后释放预占（空回滚）并归还占用。
// 防止：消费积压超过支付时限时，订单落库后进入待支付又立刻被关闭（用户没有时间支付，库存白占一轮）。
// 这样的订单应当直接失败、不预占，占用由 failed 事件归还。
func TestLandAfterPaymentDeadline(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 1, "late", items(skuA, 2))
	e.clock.Advance(DefaultPaymentTimeout + time.Second)

	e.mustLand(t, o.GetOrderId())
	stored := e.load(t, o.GetOrderId())
	wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_FAILED)
	if stored.FailReason != ReasonCreateTimeout {
		t.Errorf("fail_reason = %q, want %q", stored.FailReason, ReasonCreateTimeout)
	}
	if n := e.inv.callCount("Reserve"); n != 0 {
		t.Errorf("reserve called %d times, want 0", n)
	}
	e.wantEvents(t, o.GetOrderId(), "failed")

	// 重复投递不多写事件；库存服务消费 failed 事件后占用被归还，两边平衡。
	e.mustLand(t, o.GetOrderId())
	e.wantEvents(t, o.GetOrderId(), "failed")
	e.consumeEvents(t)
	e.wantGate(t, skuA, 1000, 0)
	e.wantBalanced(t, skuA)
}

func TestLandReserveRejected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		inject error
		reason string
	}{
		{"insufficient stock", nil, errx.ReasonInsufficientStock},
		{"sku not found", errx.New(codes.NotFound, errx.ReasonSKUNotFound, "gone"), errx.ReasonSKUNotFound},
		{"reservation released", errx.New(codes.FailedPrecondition, errx.ReasonReservationReleased, "released"), errx.ReasonReservationReleased},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			o := e.submit(t, 1, "k", items(skuA, 2))
			if tc.inject != nil {
				e.inv.inject("Reserve", fault{err: tc.inject})
			} else {
				e.inv.setAvailable(skuA, 1) // MySQL 只剩 1 件，闸门以为还有 998
			}
			e.mustLand(t, o.GetOrderId())
			stored := e.load(t, o.GetOrderId())
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_FAILED)
			if stored.FailReason != tc.reason {
				t.Errorf("fail_reason = %q, want %q", stored.FailReason, tc.reason)
			}
			e.wantEvents(t, o.GetOrderId(), "failed")
			if got := e.allEvents(t)[0].GetFailed().GetReason(); got != tc.reason {
				t.Errorf("failed event reason = %q", got)
			}

			// 重复投递、sweeper、再置一次失败都不会多写事件、多调库存。
			e.mustLand(t, o.GetOrderId())
			e.clock.Advance(time.Hour)
			e.sweeper.RunOnce(t.Context())
			ok, err := e.svc.failCreating(t.Context(), store.OrderRef{OrderID: o.GetOrderId(), UserID: 1}, "AGAIN", e.svc.now())
			if err != nil || ok {
				t.Errorf("second failCreating = %v, %v; want false, nil", ok, err)
			}
			e.wantEvents(t, o.GetOrderId(), "failed")
			if n := e.inv.callCount("Reserve"); n != 1 {
				t.Errorf("reserve called %d times, want 1", n)
			}

			// 查询和重放看到 FAILED。
			got, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: o.GetOrderId(), UserId: 1})
			if err != nil || got.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_FAILED {
				t.Errorf("GetOrder = %v, %v", got, err)
			}
			if again := e.submit(t, 1, "k", items(skuA, 2)); again.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_FAILED {
				t.Errorf("replay status = %s", again.GetStatus())
			}

			e.consumeEvents(t)
			e.wantGate(t, skuA, 1000, 0)
			if e.inv.state(o.GetOrderId()) != resReleased {
				t.Errorf("reservation state = %d, want released", e.inv.state(o.GetOrderId()))
			}
		})
	}
}

// Reserve 结果未知：返回（可重试的）错误，订单留在 CREATING；重试后成功，只预占一次。
func TestLandReserveUnknownThenRetry(t *testing.T) {
	t.Parallel()
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%v", applied), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			o := e.submit(t, 1, "k", items(skuA, 2))
			if applied {
				e.inv.inject("Reserve", fault{errAfterApply: errTimeout})
			} else {
				e.inv.inject("Reserve", fault{err: errTimeout})
			}
			err := e.land(t, o.GetOrderId())
			if err == nil || kafkax.IsPermanent(err) {
				t.Fatalf("err = %v, want a retryable error", err)
			}
			wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CREATING)
			got, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: o.GetOrderId(), UserId: 1})
			if err != nil || got.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CREATING {
				t.Errorf("GetOrder = %v, %v", got, err)
			}

			e.mustLand(t, o.GetOrderId())
			wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
			if n := e.inv.appliedCount("Reserve"); n != 1 {
				t.Errorf("reserve applied %d times, want 1", n)
			}
			wantStock(t, e.inv, skuA, 998, 2, 0)
			e.wantBalanced(t, skuA)
		})
	}
}

// Reserve 结果未知且不再重试（消息进了死信）：sweeper 按创建超时置为失败，库存最终全部归还。
// 创建超时从落库时算起：受理很久之后才落库（消费积压）的订单不会一落库就被判超时。
func TestLandReserveUnknownWithoutRetry(t *testing.T) {
	t.Parallel()
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%v", applied), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			o := e.submit(t, 1, "k", items(skuA, 2))
			e.clock.Advance(3 * DefaultCreateTimeout) // 积压
			if applied {
				e.inv.inject("Reserve", fault{errAfterApply: errTimeout})
			} else {
				e.inv.inject("Reserve", fault{err: errTimeout})
			}
			if err := e.land(t, o.GetOrderId()); err == nil {
				t.Fatal("land succeeded, want error")
			}
			id := o.GetOrderId()

			e.sweeper.RunOnce(t.Context())
			e.clock.Advance(DefaultCreateTimeout - time.Second)
			e.sweeper.RunOnce(t.Context())
			wantStatus(t, e.load(t, id), orderv1.OrderStatus_ORDER_STATUS_CREATING)

			e.clock.Advance(2 * time.Second)
			e.sweeper.RunOnce(t.Context())
			stored := e.load(t, id)
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_FAILED)
			if stored.FailReason != ReasonCreateTimeout {
				t.Errorf("fail_reason = %q", stored.FailReason)
			}
			e.wantEvents(t, id, "failed")
			e.consumeEvents(t)
			if e.inv.state(id) != resReleased {
				t.Errorf("reservation state = %d, want released", e.inv.state(id))
			}
			wantStock(t, e.inv, skuA, 1000, 0, 0)
			e.wantGate(t, skuA, 1000, 0)

			// 之后同一条消息重新投递（例如从死信重放）：占用已归还，丢弃。
			e.mustLand(t, id)
			wantStatus(t, e.load(t, id), orderv1.OrderStatus_ORDER_STATUS_FAILED)
		})
	}
}

// 落库时的 Reserve 还在路上，sweeper 按超时把订单置为失败并写 failed 事件。两种先后都不泄漏库存。
func TestLandLateReserveAfterTimeout(t *testing.T) {
	t.Parallel()
	for _, eventFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("eventFirst=%v", eventFirst), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			o := e.submit(t, 1, "k", items(skuA, 2))
			e.inv.inject("Reserve", fault{before: func(int64) {
				e.clock.Advance(DefaultCreateTimeout + time.Second)
				e.sweeper.RunOnce(t.Context())
				if eventFirst {
					e.consumeEvents(t)
				}
			}})
			e.mustLand(t, o.GetOrderId())
			stored := e.load(t, o.GetOrderId())
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_FAILED)
			if stored.FailReason != ReasonCreateTimeout {
				t.Errorf("fail_reason = %q", stored.FailReason)
			}
			e.wantEvents(t, o.GetOrderId(), "failed")
			e.consumeEvents(t)
			wantStock(t, e.inv, skuA, 1000, 0, 0)
			e.wantGate(t, skuA, 1000, 0)
		})
	}
}

// 同一订单的两次推进并发：另一次把订单置为 FAILED 并写了 failed 事件，随后本次的 Reserve 成功。
// 本次不再写事件，这份预占由库存服务消费那条 failed 事件时释放。
func TestLandReserveSucceededOnFailedOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 1, "k", items(skuA, 2))
	e.inv.inject("Reserve", fault{before: func(orderID int64) {
		ref := store.OrderRef{OrderID: orderID, UserID: 1}
		if _, err := e.svc.failCreating(t.Context(), ref, errx.ReasonInsufficientStock, e.svc.now()); err != nil {
			t.Error(err)
		}
	}})
	e.mustLand(t, o.GetOrderId())
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_FAILED)
	wantStock(t, e.inv, skuA, 998, 2, 0)
	e.wantEvents(t, o.GetOrderId(), "failed")

	e.consumeEvents(t)
	wantStock(t, e.inv, skuA, 1000, 0, 0)
	e.wantGate(t, skuA, 1000, 0)
}

// 预占明细不一致、预占结果不可用：重试也没用，进死信；订单留在 CREATING，由 sweeper 收尾。
func TestLandUnrecoverable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	mismatch := e.submit(t, 1, "mismatch", items(skuA, 1))
	e.inv.inject("Reserve", fault{err: errx.New(codes.AlreadyExists, errx.ReasonReservationMismatch, "mismatch")})
	if err := e.land(t, mismatch.GetOrderId()); !errors.Is(err, ErrUnrecoverable) || !kafkax.IsPermanent(err) {
		t.Fatalf("mismatch err = %v, want unrecoverable", err)
	}
	wantStatus(t, e.load(t, mismatch.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CREATING)

	// 单价 2^62 × 2 溢出 int64。
	overflow := e.submit(t, 1, "overflow", items(skuHuge, 2))
	// 求和溢出：2^62 + 2^62。
	e.inv.addSKU(5, 1<<62, 10)
	sum := e.submit(t, 1, "sum", items(skuHuge, 1, 5, 1))
	// 同一订单内币种不一致。
	e.inv.setCurrency(skuB, "USD")
	currency := e.submit(t, 1, "currency", items(skuA, 1, skuB, 1))
	for _, o := range []*orderv1.Order{overflow, sum, currency} {
		if err := e.land(t, o.GetOrderId()); !errors.Is(err, ErrUnrecoverable) {
			t.Fatalf("order %d err = %v, want unrecoverable", o.GetOrderId(), err)
		}
		wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CREATING)
	}

	e.clock.Advance(DefaultCreateTimeout + time.Second)
	e.sweeper.RunOnce(t.Context())
	for _, o := range []*orderv1.Order{overflow, sum, currency} {
		wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_FAILED)
		e.wantEvents(t, o.GetOrderId(), "failed")
	}
	e.consumeEvents(t)
	wantStock(t, e.inv, skuHuge, 1000, 0, 0)
	wantStock(t, e.inv, skuB, 1000, 0, 0)
	e.wantGate(t, skuHuge, 1000, 0)
}

func TestLandInvalidMessage(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	h := e.svc.SubmitHandler(0)
	if err := h(t.Context(), kafkax.Message{Value: []byte{0xff, 0xff}}); !kafkax.IsPermanent(err) {
		t.Errorf("garbage err = %v, want permanent", err)
	}
	now := timestamppb.New(e.clock.Now())
	valid := func() *eventv1.OrderSubmitted {
		return &eventv1.OrderSubmitted{
			OrderId: 1, UserId: 1, IdempotencyKey: "k", RequestHash: make([]byte, 32),
			Items:      []*eventv1.SubmittedItem{{SkuId: skuA, Quantity: 1}},
			AcceptTime: now, ExpireTime: now,
		}
	}
	for name, mutate := range map[string]func(m *eventv1.OrderSubmitted){
		"no user": func(m *eventv1.OrderSubmitted) { m.UserId = 0 },
		// 订单号 1 的基因是 1，用户 2 的基因是 2：毒消息，不重试。
		"gene mismatch": func(m *eventv1.OrderSubmitted) { m.UserId = 2 },
		// 基因只差在 slot 位以上（1 和 9 都落在 slot 1）也拒绝：idgen 嵌入的是完整的 6 位基因。
		"gene high bits": func(m *eventv1.OrderSubmitted) { m.UserId = 9 },
		"no key":         func(m *eventv1.OrderSubmitted) { m.IdempotencyKey = "" },
		"bad hash":       func(m *eventv1.OrderSubmitted) { m.RequestHash = []byte{1} },
		"no items":       func(m *eventv1.OrderSubmitted) { m.Items = nil },
		"bad qty":        func(m *eventv1.OrderSubmitted) { m.Items[0].Quantity = 0 },
		"no expire":      func(m *eventv1.OrderSubmitted) { m.ExpireTime = nil },
	} {
		m := valid()
		mutate(m)
		_, value, err := eventx.MarshalOrderSubmitted(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := h(t.Context(), kafkax.Message{Value: value}); !kafkax.IsPermanent(err) {
			t.Errorf("%s: err = %v, want permanent", name, err)
		}
	}
	if n := e.orderRows(t); n != 0 {
		t.Errorf("orders = %d", n)
	}
}

func TestAmountArithmetic(t *testing.T) {
	t.Parallel()
	if v, ok := mulAmount(math.MaxInt64/3, 3); !ok || v != math.MaxInt64/3*3 {
		t.Errorf("mulAmount max = %d, %v", v, ok)
	}
	if _, ok := mulAmount(math.MaxInt64/3+1, 3); ok {
		t.Error("mulAmount did not detect overflow")
	}
	if v, ok := mulAmount(0, 5); !ok || v != 0 {
		t.Errorf("mulAmount zero = %d, %v", v, ok)
	}
	if _, ok := addAmount(math.MaxInt64, 1); ok {
		t.Error("addAmount did not detect overflow")
	}
	if v, ok := addAmount(math.MaxInt64-1, 1); !ok || v != math.MaxInt64 {
		t.Errorf("addAmount max = %d, %v", v, ok)
	}
}

// 落库之前 GetOrder 返回 CREATING（从占用合成），别的用户查不到；落库后返回订单行。
func TestGetOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	sub := e.submit(t, 1, "k", items(skuB, 1, skuA, 2))

	got, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: sub.GetOrderId(), UserId: 1})
	if err != nil {
		t.Fatal(err)
	}
	g := got.GetOrder()
	if g.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CREATING || g.GetOrderId() != sub.GetOrderId() ||
		len(g.GetItems()) != 2 || g.GetItems()[0].GetSkuId() != skuA || g.GetItems()[0].GetQuantity() != 2 ||
		g.GetTotal() != nil || g.GetPayTime() != nil ||
		!g.GetCreateTime().AsTime().Equal(sub.GetCreateTime().AsTime()) ||
		!g.GetExpireTime().AsTime().Equal(sub.GetExpireTime().AsTime()) {
		t.Errorf("GetOrder before landing = %v, submit returned %v", g, sub)
	}
	_, err = e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: sub.GetOrderId(), UserId: 2})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	// 落库前取消：订单不可取消（与 GetOrder 的说法一致），别的用户则是不存在。
	_, err = e.svc.CancelOrder(t.Context(), cancelReq(sub.GetOrderId(), 1))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderNotCancellable)
	_, err = e.svc.CancelOrder(t.Context(), cancelReq(sub.GetOrderId(), 2))
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)

	e.mustLand(t, sub.GetOrderId())
	got, err = e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: sub.GetOrderId(), UserId: 1})
	if err != nil || got.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT ||
		got.GetOrder().GetTotal().GetAmount() != 2*priceA+priceB {
		t.Fatalf("GetOrder after landing = %v, %v", got, err)
	}
	_, err = e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: sub.GetOrderId(), UserId: 2})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	_, err = e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: 12345, UserId: 1})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	_, err = e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: 0, UserId: 1})
	wantErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
}
