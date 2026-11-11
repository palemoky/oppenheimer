package service

import (
	"sync"
	"testing"

	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
)

func cancelReq(orderID, userID int64) *orderv1.CancelOrderRequest {
	return &orderv1.CancelOrderRequest{OrderId: orderID, UserId: userID}
}

func TestCancelOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")

	// 别人的订单等同于不存在，也不会被取消。
	_, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 2))
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	_, err = e.svc.CancelOrder(t.Context(), cancelReq(31337, 1))
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)

	for range 3 {
		resp, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1))
		if err != nil || resp.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
			t.Fatalf("cancel = %v, %v", resp, err)
		}
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CANCELLED, true)
	if n := e.inv.appliedCount("Release"); n != 1 {
		t.Errorf("release applied %d times, want 1", n)
	}
	if n := e.inv.callCount("Release"); n != 1 {
		t.Errorf("release called %d times, want 1 (repeat cancels must not settle again)", n)
	}
	wantStock(t, e.inv, skuA, 1000, 0, 0)
}

func TestCancelOrderNotCancellable(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	paid := e.mustPending(t, 1, "paid")
	if _, err := e.pay(t, paid.GetOrderId(), 1, paid.GetTotal().GetAmount(), true); err != nil {
		t.Fatal(err)
	}
	_, err := e.svc.CancelOrder(t.Context(), cancelReq(paid.GetOrderId(), 1))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderNotCancellable)
	wantStatus(t, e.load(t, paid.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID, true)

	e.inv.inject("Reserve", fault{err: errTimeout})
	if _, err := e.svc.CreateOrder(t.Context(), createReq(1, "creating", items(skuA, 1))); errx.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	creating := e.loadByKey(t, 1, "creating")
	_, err = e.svc.CancelOrder(t.Context(), cancelReq(creating.OrderID, 1))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderNotCancellable)

	_, err = e.svc.CancelOrder(t.Context(), cancelReq(0, 1))
	wantErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
}

func TestCancelOrderConcurrent(t *testing.T) {
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
			if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
				t.Errorf("cancel: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	// 只有完成转换的那一次去结清。
	if n := e.inv.callCount("Release"); n != 1 {
		t.Errorf("release called %d times, want 1", n)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CANCELLED, true)
	wantStock(t, e.inv, skuA, 1000, 0, 0)
}
