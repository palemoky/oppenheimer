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
	if n := e.outboxRows(t); n != 0 {
		t.Fatalf("outbox = %d after failed cancels, want 0", n)
	}

	kicks := e.kicks()
	for range 3 {
		resp, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1))
		if err != nil || resp.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
			t.Fatalf("cancel = %v, %v", resp, err)
		}
	}
	stored := e.load(t, o.GetOrderId())
	wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	// 重复取消只有第一次完成转换，只有它写事件、通知中继。
	e.wantEvents(t, o.GetOrderId(), "cancelled")
	if got := e.kicks() - kicks; got != 1 {
		t.Errorf("relay kicked %d times, want 1", got)
	}
	ev := e.allEvents(t)[0]
	if ev.GetUserId() != 1 || !ev.GetOccurTime().AsTime().Equal(stored.UpdatedAt) {
		t.Errorf("event user_id = %d, occur_time = %s, want 1, %s", ev.GetUserId(), ev.GetOccurTime().AsTime(), stored.UpdatedAt)
	}
	// 释放由库存服务消费 cancelled 事件完成，订单服务不再同步调用。
	wantStock(t, e.inv, skuA, 998, 2, 0)
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
	wantStatus(t, e.load(t, paid.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID)
	e.wantEvents(t, paid.GetOrderId(), "paid", "handled")

	creating := e.mustCreating(t, 1, "creating")
	_, err = e.svc.CancelOrder(t.Context(), cancelReq(creating.OrderID, 1))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderNotCancellable)
	e.wantEvents(t, creating.OrderID)

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
	// 只有完成转换的那一次写事件。
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	e.wantEvents(t, o.GetOrderId(), "cancelled")
}
