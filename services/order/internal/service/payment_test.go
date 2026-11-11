package service

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
)

const (
	accepted       = orderv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED
	refundRequired = orderv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED
)

func TestHandlePaymentSuccess(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	total := o.GetTotal().GetAmount()

	d, err := e.pay(t, o.GetOrderId(), 501, total, true)
	if err != nil || d != accepted {
		t.Fatalf("pay = %s, %v", d, err)
	}
	stored := e.load(t, o.GetOrderId())
	wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PAID, true)
	if stored.PaymentID != 501 || !stored.PaidAt.Equal(e.clock.Now()) {
		t.Errorf("payment_id = %d, paid_at = %s", stored.PaymentID, stored.PaidAt)
	}
	wantStock(t, e.inv, skuA, 998, 0, 2)

	got, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: o.GetOrderId(), UserId: 1})
	if err != nil || got.GetOrder().GetPayTime() == nil {
		t.Errorf("GetOrder after pay = %v, %v", got, err)
	}

	// 重复投递：答复一致，订单只转换一次，库存只确认一次。
	e.clock.Advance(time.Minute)
	for range 3 {
		d, err := e.pay(t, o.GetOrderId(), 501, total, true)
		if err != nil || d != accepted {
			t.Fatalf("duplicate pay = %s, %v", d, err)
		}
	}
	if !e.load(t, o.GetOrderId()).PaidAt.Equal(stored.PaidAt) {
		t.Error("paid_at changed on duplicate delivery")
	}
	if n := e.inv.appliedCount("Confirm"); n != 1 {
		t.Errorf("confirm applied %d times, want 1", n)
	}
	if n := e.count(t, "SELECT COUNT(*) FROM payment_results"); n != 1 {
		t.Errorf("payment_results = %d, want 1", n)
	}

	// 另一笔支付再付一次：需要退款，重复投递同样答复。
	for range 2 {
		d, err = e.pay(t, o.GetOrderId(), 502, total, true)
		if err != nil || d != refundRequired {
			t.Fatalf("second payment = %s, %v", d, err)
		}
	}
	if e.load(t, o.GetOrderId()).PaymentID != 501 {
		t.Error("payment_id overwritten by second payment")
	}
}

func TestHandlePaymentFailed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	for range 2 {
		d, err := e.pay(t, o.GetOrderId(), 600, o.GetTotal().GetAmount(), false)
		if err != nil || d != accepted {
			t.Fatalf("failed payment = %s, %v", d, err)
		}
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT, false)
	if n := e.count(t, "SELECT COUNT(*) FROM payment_results WHERE payment_id = 600 AND succeeded = 0"); n != 1 {
		t.Errorf("payment_results = %d, want 1", n)
	}
	// 失败之后仍可用另一笔支付付清。
	d, err := e.pay(t, o.GetOrderId(), 601, o.GetTotal().GetAmount(), true)
	if err != nil || d != accepted {
		t.Fatalf("pay after failure = %s, %v", d, err)
	}
}

func TestHandlePaymentAmountMismatch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	total := o.GetTotal().GetAmount()

	for _, amount := range []*commonv1.Money{
		{Amount: total - 1, Currency: "CNY"},
		{Amount: total + 1, Currency: "CNY"},
		{Amount: total, Currency: "USD"},
	} {
		_, err := e.svc.HandlePaymentResult(t.Context(), &orderv1.HandlePaymentResultRequest{
			OrderId: o.GetOrderId(), PaymentId: 700, Succeeded: true, Amount: amount,
		})
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonAmountMismatch)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT, false)
	if n := e.count(t, "SELECT COUNT(*) FROM payment_results"); n != 0 {
		t.Errorf("mismatch was recorded: payment_results = %d", n)
	}
	d, err := e.pay(t, o.GetOrderId(), 700, total, true)
	if err != nil || d != accepted {
		t.Fatalf("pay with correct amount = %s, %v", d, err)
	}
}

func TestHandlePaymentNotPayable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	_, err := e.pay(t, 424242, 1, 100, true)
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)

	_, err = e.svc.HandlePaymentResult(t.Context(), &orderv1.HandlePaymentResultRequest{OrderId: 1, PaymentId: 1, Succeeded: true})
	wantErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)

	// 已取消的订单。
	cancelled := e.mustPending(t, 1, "cancel")
	if _, err := e.svc.CancelOrder(t.Context(), &orderv1.CancelOrderRequest{OrderId: cancelled.GetOrderId(), UserId: 1}); err != nil {
		t.Fatal(err)
	}
	// 创建中的订单（预占超时）。
	e.inv.inject("Reserve", fault{err: errTimeout})
	if _, err := e.svc.CreateOrder(t.Context(), createReq(1, "creating", items(skuA, 1))); errx.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v", err)
	}
	creating := e.loadByKey(t, 1, "creating")

	for i, id := range []int64{cancelled.GetOrderId(), creating.OrderID} {
		paymentID := int64(800 + i)
		for range 2 {
			d, err := e.pay(t, id, paymentID, 200, true)
			if err != nil || d != refundRequired {
				t.Fatalf("pay order %d = %s, %v", id, d, err)
			}
		}
	}
	wantStatus(t, e.load(t, creating.OrderID), orderv1.OrderStatus_ORDER_STATUS_CREATING, false)
	if n := e.inv.callCount("Confirm"); n != 0 {
		t.Errorf("confirm called %d times, want 0", n)
	}
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
			d, err := e.pay(t, o.GetOrderId(), 900, o.GetTotal().GetAmount(), true)
			if err != nil || d != accepted {
				t.Errorf("pay = %s, %v", d, err)
			}
		})
	}
	close(start)
	wg.Wait()
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID, true)
	if n := e.count(t, "SELECT COUNT(*) FROM payment_results"); n != 1 {
		t.Errorf("payment_results = %d, want 1", n)
	}
	if n := e.inv.appliedCount("Confirm"); n != 1 {
		t.Errorf("confirm applied %d times, want 1", n)
	}
	wantStock(t, e.inv, skuA, 998, 0, 2)
}

// 支付成功与取消并发：恰好一个转换成功，支付输了的一方得到 REFUND_REQUIRED。
func TestPaymentRacesCancel(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const rounds = 20
	var paidWins, cancelWins int
	for i := range rounds {
		o := e.mustPending(t, 1, fmt.Sprintf("race-%d", i))
		var (
			wg          sync.WaitGroup
			start       = make(chan struct{})
			disposition orderv1.PaymentDisposition
			payErr      error
			cancelErr   error
		)
		wg.Go(func() {
			<-start
			disposition, payErr = e.pay(t, o.GetOrderId(), int64(1000+i), o.GetTotal().GetAmount(), true)
		})
		wg.Go(func() {
			<-start
			_, cancelErr = e.svc.CancelOrder(t.Context(), &orderv1.CancelOrderRequest{OrderId: o.GetOrderId(), UserId: 1})
		})
		close(start)
		wg.Wait()
		if payErr != nil {
			t.Fatalf("pay: %v", payErr)
		}
		stored := e.load(t, o.GetOrderId())
		switch disposition {
		case accepted:
			paidWins++
			wantErr(t, cancelErr, codes.FailedPrecondition, errx.ReasonOrderNotCancellable)
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PAID, true)
		case refundRequired:
			cancelWins++
			if cancelErr != nil {
				t.Fatalf("cancel lost to refund but returned %v", cancelErr)
			}
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_CANCELLED, true)
		default:
			t.Fatalf("disposition = %s", disposition)
		}
	}
	// 每轮一笔 2 件：付成的进 sold，取消的回 available。
	wantStock(t, e.inv, skuA, 1000-int64(2*paidWins), 0, int64(2*paidWins))
	t.Logf("paid wins %d, cancel wins %d", paidWins, cancelWins)
}

// 支付成功与超时关单并发：恰好一个转换成功。
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
		var (
			wg          sync.WaitGroup
			start       = make(chan struct{})
			disposition orderv1.PaymentDisposition
			payErr      error
		)
		wg.Go(func() {
			<-start
			disposition, payErr = e.pay(t, o.GetOrderId(), int64(2000+i), o.GetTotal().GetAmount(), true)
		})
		wg.Go(func() {
			<-start
			e.sweeper.closeOrder(t.Context(), o.GetOrderId(), now)
		})
		close(start)
		wg.Wait()
		if payErr != nil {
			t.Fatalf("pay: %v", payErr)
		}
		stored := e.load(t, o.GetOrderId())
		switch disposition {
		case accepted:
			paidWins++
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PAID, true)
		case refundRequired:
			wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_CLOSED, true)
		default:
			t.Fatalf("disposition = %s", disposition)
		}
	}
	wantStock(t, e.inv, skuA, 1000-int64(2*paidWins), 0, int64(2*paidWins))
	t.Logf("paid wins %d of %d", paidWins, rounds)
}
