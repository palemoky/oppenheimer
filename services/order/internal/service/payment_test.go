package service

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/palemoky/Oppenheimer/api/eventx"
	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	eventv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/event/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
)

func wantReply(t *testing.T, got *eventv1.PaymentResultHandled, err error, d eventv1.PaymentDisposition, reason string) {
	t.Helper()
	if err != nil {
		t.Fatalf("handle payment: %v", err)
	}
	if got.GetDisposition() != d || got.GetReason() != reason {
		t.Fatalf("reply = %s %q, want %s %q", got.GetDisposition(), got.GetReason(), d, reason)
	}
}

// sideEffects 是一次处理留下的全部可观察结果，用来比较“处理一次”和“处理多次”。
type sideEffects struct {
	outbox, paymentResults int
	kicks                  int64
	order                  string
}

func (e *env) sideEffects(t *testing.T, orderID int64) sideEffects {
	t.Helper()
	se := sideEffects{
		outbox:         e.outboxRows(t),
		paymentResults: e.count(t, "payment_results", ""),
		kicks:          e.kicks(),
	}
	if o, err := e.store.GetOrder(t.Context(), orderID); err == nil {
		se.order = fmt.Sprintf("%s/%d/%s/%s", o.Status, o.PaymentID, o.PaidAt, o.UpdatedAt)
	}
	return se
}

// 同一事件处理多次：答复、订单、payment_results、outbox 行数都与处理一次相同。
func TestHandlePaymentEventIdempotent(t *testing.T) {
	t.Parallel()
	type setup func(t *testing.T, e *env) (ev *eventv1.PaymentEvent, d eventv1.PaymentDisposition, reason string)
	pending := func(t *testing.T, e *env) *orderv1.Order { return e.mustPending(t, 1, "k") }
	cases := map[string]setup{
		"paid": func(t *testing.T, e *env) (*eventv1.PaymentEvent, eventv1.PaymentDisposition, string) {
			o := pending(t, e)
			return paymentEvent(o.GetOrderId(), 1, o.GetTotal().GetAmount(), true), accepted, ""
		},
		"payment failed": func(t *testing.T, e *env) (*eventv1.PaymentEvent, eventv1.PaymentDisposition, string) {
			o := pending(t, e)
			return paymentEvent(o.GetOrderId(), 1, o.GetTotal().GetAmount(), false), accepted, ""
		},
		"amount mismatch": func(t *testing.T, e *env) (*eventv1.PaymentEvent, eventv1.PaymentDisposition, string) {
			o := pending(t, e)
			return paymentEvent(o.GetOrderId(), 1, o.GetTotal().GetAmount()+1, true), refundRequired, errx.ReasonAmountMismatch
		},
		"order not found": func(*testing.T, *env) (*eventv1.PaymentEvent, eventv1.PaymentDisposition, string) {
			return paymentEvent(424242, 1, 100, true), refundRequired, errx.ReasonOrderNotFound
		},
		"order not payable": func(t *testing.T, e *env) (*eventv1.PaymentEvent, eventv1.PaymentDisposition, string) {
			o := pending(t, e)
			if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
				t.Fatal(err)
			}
			return paymentEvent(o.GetOrderId(), 1, o.GetTotal().GetAmount(), true), refundRequired, errx.ReasonOrderNotPayable
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			ev, d, reason := setup(t, e)

			reply, err := e.deliver(t, ev)
			wantReply(t, reply, err, d, reason)
			once := e.sideEffects(t, ev.GetOrderId())

			e.clock.Advance(time.Minute)
			for range 3 {
				reply, err := e.deliver(t, ev)
				wantReply(t, reply, err, d, reason)
			}
			if again := e.sideEffects(t, ev.GetOrderId()); again != once {
				t.Errorf("side effects after redelivery = %+v, want %+v", again, once)
			}
			if once.paymentResults != 1 {
				t.Errorf("payment_results = %d, want 1", once.paymentResults)
			}
		})
	}
}

func TestHandlePaymentSuccess(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	total := o.GetTotal().GetAmount()
	kicks := e.kicks()

	reply, err := e.pay(t, o.GetOrderId(), 501, total, true)
	wantReply(t, reply, err, accepted, "")
	stored := e.load(t, o.GetOrderId())
	wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PAID)
	if stored.PaymentID != 501 || !stored.PaidAt.Equal(e.clock.Now()) {
		t.Errorf("payment_id = %d, paid_at = %s", stored.PaymentID, stored.PaidAt)
	}
	// 一个事务两条 outbox 记录：paid 和答复。
	e.wantEvents(t, o.GetOrderId(), "paid", "handled")
	for _, ev := range e.allEvents(t) {
		if p := ev.GetPaid(); p != nil && p.GetPaymentId() != 501 {
			t.Errorf("paid event payment_id = %d, want 501", p.GetPaymentId())
		}
		if ev.GetUserId() != 1 || !ev.GetOccurTime().AsTime().Equal(stored.PaidAt) {
			t.Errorf("event user_id = %d, occur_time = %s", ev.GetUserId(), ev.GetOccurTime().AsTime())
		}
	}
	if e.kicks() <= kicks {
		t.Error("relay was not kicked after commit")
	}
	// 库存只预占过，确认由库存服务消费 paid 事件完成。
	wantStock(t, e.inv, skuA, 998, 2, 0)

	got, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: o.GetOrderId(), UserId: 1})
	if err != nil || got.GetOrder().GetPayTime() == nil {
		t.Errorf("GetOrder after pay = %v, %v", got, err)
	}

	// 另一笔支付再付一次：需要退款，订单不变，只多一条答复。
	for range 2 {
		reply, err = e.pay(t, o.GetOrderId(), 502, total, true)
		wantReply(t, reply, err, refundRequired, errx.ReasonOrderNotPayable)
	}
	if e.load(t, o.GetOrderId()).PaymentID != 501 {
		t.Error("payment_id overwritten by second payment")
	}
	e.wantEvents(t, o.GetOrderId(), "paid", "handled", "handled")
}

func TestHandlePaymentFailed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	reply, err := e.pay(t, o.GetOrderId(), 600, o.GetTotal().GetAmount(), false)
	wantReply(t, reply, err, accepted, "")
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if n := e.count(t, "payment_results", "payment_id = 600 AND succeeded = 0"); n != 1 {
		t.Errorf("payment_results = %d, want 1", n)
	}
	e.wantEvents(t, o.GetOrderId(), "handled")

	// 失败之后仍可用另一笔支付付清。
	reply, err = e.pay(t, o.GetOrderId(), 601, o.GetTotal().GetAmount(), true)
	wantReply(t, reply, err, accepted, "")
	e.wantEvents(t, o.GetOrderId(), "handled", "paid", "handled")
}

// 金额或币种不符：记录并要求退款，订单保持待支付，之后的正确支付仍能付清。
func TestHandlePaymentAmountMismatch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	total := o.GetTotal().GetAmount()

	for i, amount := range []*commonv1.Money{
		{Amount: total - 1, Currency: "CNY"},
		{Amount: total + 1, Currency: "CNY"},
		{Amount: total, Currency: "USD"},
	} {
		ev := paymentEvent(o.GetOrderId(), int64(700+i), 0, true)
		ev.GetCompleted().Amount = amount
		reply, err := e.deliver(t, ev)
		wantReply(t, reply, err, refundRequired, errx.ReasonAmountMismatch)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if n := e.count(t, "payment_results", "disposition = ?", int32(refundRequired)); n != 3 {
		t.Errorf("refund payment_results = %d, want 3", n)
	}
	e.wantEvents(t, o.GetOrderId(), "handled", "handled", "handled")

	reply, err := e.pay(t, o.GetOrderId(), 710, total, true)
	wantReply(t, reply, err, accepted, "")
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID)
}

// 订单不存在：成功的支付要求退款，失败的支付只需知悉；都记录下来，不卡住消费者。
func TestHandlePaymentOrderNotFound(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	reply, err := e.pay(t, 424242, 1, 100, true)
	wantReply(t, reply, err, refundRequired, errx.ReasonOrderNotFound)
	reply, err = e.pay(t, 424243, 2, 100, false)
	wantReply(t, reply, err, accepted, "")

	// 答复里的 user_id 来自支付事件（订单不存在，没有别的来源）。
	for _, ev := range e.allEvents(t) {
		if ev.GetUserId() != 1 {
			t.Errorf("reply user_id = %d, want 1", ev.GetUserId())
		}
	}
	if n := e.count(t, "payment_results", ""); n != 2 {
		t.Errorf("payment_results = %d, want 2", n)
	}
	if n := e.count(t, "orders", ""); n != 0 {
		t.Errorf("orders = %d, want 0", n)
	}
}

func TestHandlePaymentNotPayable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	// 已取消的订单。
	cancelled := e.mustPending(t, 1, "cancel")
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(cancelled.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	// 创建中的订单（预占超时）。
	creating := e.mustCreating(t, 1, "creating")

	for i, id := range []int64{cancelled.GetOrderId(), creating.OrderID} {
		reply, err := e.pay(t, id, int64(800+i), 200, true)
		wantReply(t, reply, err, refundRequired, errx.ReasonOrderNotPayable)
	}
	wantStatus(t, e.load(t, cancelled.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	wantStatus(t, e.load(t, creating.OrderID), orderv1.OrderStatus_ORDER_STATUS_CREATING)
	e.wantEvents(t, cancelled.GetOrderId(), "cancelled", "handled")
	e.wantEvents(t, creating.OrderID, "handled")
}

func TestHandlePaymentInvalidEvents(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")

	noAmount := paymentEvent(o.GetOrderId(), 1, 0, true)
	noAmount.GetCompleted().Amount = nil
	for name, ev := range map[string]*eventv1.PaymentEvent{
		"no order id":   paymentEvent(0, 1, 100, true),
		"no payment id": paymentEvent(o.GetOrderId(), 0, 100, true),
		"no amount":     noAmount,
	} {
		if err := e.svc.HandlePaymentEvent(t.Context(), ev); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("%s: err = %v, want ErrInvalidEvent", name, err)
		}
	}

	// 同一 payment_id 已记在另一个订单上：事件与已知事实矛盾，不能当作重复静默吞掉。
	if _, err := e.pay(t, o.GetOrderId(), 5, o.GetTotal().GetAmount(), false); err != nil {
		t.Fatal(err)
	}
	other := e.mustPending(t, 1, "other")
	before := e.sideEffects(t, other.GetOrderId())
	if err := e.svc.HandlePaymentEvent(t.Context(), paymentEvent(other.GetOrderId(), 5, other.GetTotal().GetAmount(), true)); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("payment of another order: err = %v, want ErrInvalidEvent", err)
	}
	if after := e.sideEffects(t, other.GetOrderId()); after != before {
		t.Errorf("side effects = %+v, want %+v", after, before)
	}

	// 不认识的事件直接跳过，什么都不写。
	if err := e.svc.HandlePaymentEvent(t.Context(), &eventv1.PaymentEvent{PaymentId: 9, OrderId: o.GetOrderId()}); err != nil {
		t.Errorf("unknown event: %v", err)
	}
	if after := e.sideEffects(t, other.GetOrderId()); after != before {
		t.Errorf("unknown event changed side effects: %+v, want %+v", after, before)
	}
}

// Kafka 适配层：解不开的消息和不合法的事件进死信，正常的事件交给消费逻辑。
func TestPaymentEventsHandler(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	h := e.svc.PaymentEventsHandler(0)

	if err := h(t.Context(), kafkax.Message{Value: []byte{0xff, 0xff}}); !kafkax.IsPermanent(err) {
		t.Errorf("garbage message: err = %v, want permanent", err)
	}
	bad, err := proto.Marshal(paymentEvent(o.GetOrderId(), 0, 100, true))
	if err != nil {
		t.Fatal(err)
	}
	if err := h(t.Context(), kafkax.Message{Value: bad}); !kafkax.IsPermanent(err) {
		t.Errorf("invalid event: err = %v, want permanent", err)
	}

	_, value, err := eventx.MarshalPaymentEvent(paymentEvent(o.GetOrderId(), 1, o.GetTotal().GetAmount(), true))
	if err != nil {
		t.Fatal(err)
	}
	if err := h(t.Context(), kafkax.Message{Value: value}); err != nil {
		t.Fatalf("valid event: %v", err)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID)
}

func TestHandlePaymentConcurrentDuplicates(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	for range 10 {
		wg.Go(func() {
			<-start
			if err := e.svc.HandlePaymentEvent(t.Context(), paymentEvent(o.GetOrderId(), 900, o.GetTotal().GetAmount(), true)); err != nil {
				t.Errorf("handle: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID)
	if n := e.count(t, "payment_results", ""); n != 1 {
		t.Errorf("payment_results = %d, want 1", n)
	}
	e.wantEvents(t, o.GetOrderId(), "paid", "handled")
	wantReply(t, e.reply(t, 900), nil, accepted, "")
}

// 订单不存在时没有订单行锁可以排队，并发的重复投递在插入 payment_results 时相撞；
// 输的一方重做事务后看到已处理的记录，不报错也不多写。
func TestHandlePaymentConcurrentDuplicatesOrderNotFound(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	for range 10 {
		wg.Go(func() {
			<-start
			if err := e.svc.HandlePaymentEvent(t.Context(), paymentEvent(424242, 900, 100, true)); err != nil {
				t.Errorf("handle: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	if n := e.count(t, "payment_results", ""); n != 1 {
		t.Errorf("payment_results = %d, want 1", n)
	}
	if n := e.outboxRows(t); n != 1 {
		t.Errorf("outbox = %d, want 1", n)
	}
	wantReply(t, e.reply(t, 900), nil, refundRequired, errx.ReasonOrderNotFound)
}

// 支付成功与取消并发：恰好一个转换成功、恰好一条状态事件，支付输了的一方得到 REFUND_REQUIRED。
// 支付结果可能重复投递，混进一次重复，结果仍然一样。
func TestPaymentRacesCancel(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const rounds = 20
	var paidWins, cancelWins int
	for i := range rounds {
		o := e.mustPending(t, 1, fmt.Sprintf("race-%d", i))
		paymentID := int64(1000 + i)
		var (
			wg        sync.WaitGroup
			start     = make(chan struct{})
			cancelErr error
		)
		for range 2 {
			wg.Go(func() {
				<-start
				if err := e.svc.HandlePaymentEvent(t.Context(), paymentEvent(o.GetOrderId(), paymentID, o.GetTotal().GetAmount(), true)); err != nil {
					t.Errorf("handle: %v", err)
				}
			})
		}
		wg.Go(func() {
			<-start
			_, cancelErr = e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1))
		})
		close(start)
		wg.Wait()
		stored := e.load(t, o.GetOrderId())
		reply := e.reply(t, paymentID)
		switch reply.GetDisposition() {
		case accepted:
			paidWins++
			wantErr(t, cancelErr, codes.FailedPrecondition, errx.ReasonOrderNotCancellable)
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PAID)
			e.wantEvents(t, o.GetOrderId(), "paid", "handled")
		case refundRequired:
			cancelWins++
			if cancelErr != nil {
				t.Fatalf("cancel won but returned %v", cancelErr)
			}
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
			e.wantEvents(t, o.GetOrderId(), "cancelled", "handled")
		default:
			t.Fatalf("disposition = %s", reply.GetDisposition())
		}
	}
	t.Logf("paid wins %d, cancel wins %d", paidWins, cancelWins)
}

// 支付成功与超时关单并发：恰好一个转换成功、恰好一条状态事件。
func TestPaymentRacesClose(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const rounds = 20
	orders := make([]*orderv1.Order, 0, rounds)
	for i := range rounds {
		orders = append(orders, e.mustPending(t, 1, fmt.Sprintf("close-%d", i)))
	}
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	now := e.svc.now()

	var paidWins int
	for i, o := range orders {
		paymentID := int64(2000 + i)
		var (
			wg    sync.WaitGroup
			start = make(chan struct{})
		)
		wg.Go(func() {
			<-start
			if err := e.svc.HandlePaymentEvent(t.Context(), paymentEvent(o.GetOrderId(), paymentID, o.GetTotal().GetAmount(), true)); err != nil {
				t.Errorf("handle: %v", err)
			}
		})
		wg.Go(func() {
			<-start
			e.sweeper.closeOrder(t.Context(), refOf(o), now)
		})
		close(start)
		wg.Wait()
		stored := e.load(t, o.GetOrderId())
		switch d := e.reply(t, paymentID).GetDisposition(); d {
		case accepted:
			paidWins++
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PAID)
			e.wantEvents(t, o.GetOrderId(), "paid", "handled")
		case refundRequired:
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_CLOSED)
			e.wantEvents(t, o.GetOrderId(), "closed", "handled")
		default:
			t.Fatalf("disposition = %s", d)
		}
	}
	t.Logf("paid wins %d of %d", paidWins, rounds)
}
