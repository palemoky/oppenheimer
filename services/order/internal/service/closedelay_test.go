package service

import (
	"errors"
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
)

// closeDue 把受理时为 orderID 发出的延迟关单消息交给 handler 处理一次。
func (e *env) closeDue(t *testing.T, orderID int64) error {
	t.Helper()
	for _, r := range e.sender.records(testCloseTopic) {
		m, err := eventx.UnmarshalOrderCloseDue(r.Value)
		if err != nil {
			t.Fatal(err)
		}
		if m.GetOrderId() == orderID {
			return e.svc.CloseDueHandler(0)(t.Context(), kafkax.Message{Topic: r.Topic, Key: r.Key, Value: r.Value, Headers: r.Headers})
		}
	}
	t.Fatalf("no close delay message for order %d", orderID)
	return nil
}

func TestCloseDueExpired(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	e.clock.Advance(DefaultPaymentTimeout + time.Millisecond)
	kicks := e.relay.kicks.Load()
	if err := e.closeDue(t, o.GetOrderId()); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
	e.wantEvents(t, o.GetOrderId(), "closed")
	if e.relay.kicks.Load() <= kicks {
		t.Error("relay was not kicked after close")
	}
	// 重复投递、sweeper 再扫：不再写事件。
	if err := e.closeDue(t, o.GetOrderId()); err != nil {
		t.Fatal(err)
	}
	e.sweeper.RunOnce(t.Context())
	e.wantEvents(t, o.GetOrderId(), "closed")
	e.consumeEvents(t)
	wantStock(t, e.inv, skuA, 1000, 0, 0)
	e.wantGate(t, skuA, 1000, 0)
}

// 按本机时钟还没过期（发送方时钟快）：返回可重试的错误，不能当作不需要关而跳过。
func TestCloseDueNotYetExpired(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	e.clock.Advance(DefaultPaymentTimeout) // 恰好到截止时间：条件是 expire_at < now，还不关
	err := e.closeDue(t, o.GetOrderId())
	if !errors.Is(err, errNotYetExpired) || kafkax.IsPermanent(err) {
		t.Fatalf("err = %v, want retryable not-yet-expired", err)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	e.wantEvents(t, o.GetOrderId())

	e.clock.Advance(time.Second)
	if err := e.closeDue(t, o.GetOrderId()); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
}

// 订单已不是待支付（已支付、已取消、已失败）：什么都不做。
func TestCloseDueNotPending(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	paid := e.mustPending(t, 1, "paid")
	if _, err := e.pay(t, paid.GetOrderId(), 1, paid.GetTotal().GetAmount(), true); err != nil {
		t.Fatal(err)
	}
	cancelled := e.mustPending(t, 1, "cancelled")
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(cancelled.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	failed := e.submit(t, 1, "failed", items(skuA, 1))
	e.inv.inject("Reserve", fault{err: errx.New(codes.FailedPrecondition, errx.ReasonInsufficientStock, "no")})
	e.mustLand(t, failed.GetOrderId())

	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	outbox := e.outboxRows(t)
	for _, id := range []int64{paid.GetOrderId(), cancelled.GetOrderId(), failed.GetOrderId()} {
		if err := e.closeDue(t, id); err != nil {
			t.Fatalf("order %d: %v", id, err)
		}
	}
	wantStatus(t, e.load(t, paid.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID)
	wantStatus(t, e.load(t, cancelled.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	wantStatus(t, e.load(t, failed.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_FAILED)
	if n := e.outboxRows(t); n != outbox {
		t.Errorf("outbox rows %d -> %d, want unchanged", outbox, n)
	}
}

// 订单还不存在（没落库、被回收）或还在 CREATING：什么都不做，交给 sweeper。
func TestCloseDueNotLanded(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	notLanded := e.submit(t, 1, "a", items(skuA, 1))
	creating := e.mustCreating(t, 1, "b")
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	for _, id := range []int64{notLanded.GetOrderId(), creating.OrderID} {
		if err := e.closeDue(t, id); err != nil {
			t.Fatalf("order %d: %v", id, err)
		}
	}
	if _, err := e.store.GetOrder(t.Context(), notLanded.GetOrderId()); err == nil {
		t.Error("close due created an order row")
	}
	wantStatus(t, e.load(t, creating.OrderID), orderv1.OrderStatus_ORDER_STATUS_CREATING)
	if n := e.outboxRows(t); n != 0 {
		t.Errorf("outbox = %d, want 0", n)
	}

	// 落库晚于到期：订单进入待支付时已经过期，延迟消息早已处理过，由 sweeper 关闭。
	e.mustLand(t, notLanded.GetOrderId())
	wantStatus(t, e.load(t, notLanded.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, notLanded.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
}

// 到期关单与支付并发：都以 PENDING_PAYMENT 为前置条件，只有一方得手；支付输了要退款。
func TestCloseDueRacesPayment(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const n = 10
	orders := make([]*orderv1.Order, 0, n)
	for i := range n {
		orders = append(orders, e.mustPending(t, 1, string(rune('a'+i))))
	}
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, o := range orders {
		wg.Go(func() {
			<-start
			if err := e.closeDue(t, o.GetOrderId()); err != nil {
				t.Errorf("close: %v", err)
			}
		})
		wg.Go(func() {
			<-start
			if err := e.svc.HandlePaymentEvent(t.Context(), paymentEvent(o.GetOrderId(), int64(100+i), o.GetTotal().GetAmount(), true)); err != nil {
				t.Errorf("pay: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	for i, o := range orders {
		stored := e.load(t, o.GetOrderId())
		reply := e.reply(t, int64(100+i))
		switch stored.Status {
		case orderv1.OrderStatus_ORDER_STATUS_CLOSED:
			e.wantEvents(t, o.GetOrderId(), "closed", "handled")
			if reply.GetDisposition() != refundRequired {
				t.Errorf("order %d closed but payment %v", o.GetOrderId(), reply)
			}
		case orderv1.OrderStatus_ORDER_STATUS_PAID:
			e.wantEvents(t, o.GetOrderId(), "paid", "handled")
			if reply.GetDisposition() != accepted {
				t.Errorf("order %d paid but payment %v", o.GetOrderId(), reply)
			}
		default:
			t.Errorf("order %d status = %s", o.GetOrderId(), stored.Status)
		}
	}
}

func TestCloseDueInvalidMessage(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	h := e.svc.CloseDueHandler(0)
	if err := h(t.Context(), kafkax.Message{Value: []byte{0xff}}); !kafkax.IsPermanent(err) {
		t.Errorf("garbage err = %v", err)
	}
	if err := e.svc.HandleOrderCloseDue(t.Context(), &eventv1.OrderCloseDue{ExpireTime: timestamppb.Now()}); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("no order id err = %v", err)
	}
}
