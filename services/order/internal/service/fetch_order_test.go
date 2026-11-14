package service

import (
	"testing"

	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
)

// 本文件测试 fetch 的读顺序：先读占用，再决定要不要读订单库（设计文档 1d 的 2.4）。

func (e *env) getOrder(t *testing.T, orderID, userID int64) (*orderv1.Order, error) {
	t.Helper()
	resp, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: orderID, UserId: userID})
	return resp.GetOrder(), err
}

// 排队等落库的订单（占用未认领）被查询时不读订单库：把订单库全部关掉，照样答 CREATING。
// 这是“轮询不和落库抢连接”的依据。
func TestQueuedOrderIsAnsweredWithoutTheOrderDB(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 1, "k", items(skuA, 2))
	for _, db := range e.dbs {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	for range 3 {
		got, err := e.getOrder(t, o.GetOrderId(), 1)
		if err != nil {
			t.Fatalf("GetOrder of a queued order with the order dbs closed: %v", err)
		}
		if got.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CREATING || len(got.GetItems()) != 1 {
			t.Fatalf("got %s with %d items, want CREATING synthesized from the hold", got.GetStatus(), len(got.GetItems()))
		}
	}
	// 别人的订单仍然等同于不存在，判断归属也不需要订单库。
	_, err := e.getOrder(t, o.GetOrderId(), 2)
	wantErr(t, err, codes.NotFound, "ORDER_NOT_FOUND")
}

// 已认领的占用（订单已落库、还没到终态）：以订单行为准，不能答成 CREATING。
func TestClaimedOrderIsReadFromTheOrderDB(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	if h, ok := e.hold(t, o.GetOrderId()); !ok || !h.Claimed {
		t.Fatalf("hold = %+v, %v; want a claimed hold on a pending order", h, ok)
	}
	got, err := e.getOrder(t, o.GetOrderId(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT || got.GetTotal().GetAmount() == 0 {
		t.Fatalf("got %s total %d, want PENDING_PAYMENT with the reserved total", got.GetStatus(), got.GetTotal().GetAmount())
	}
}

// Redis 不可用时：已落库的订单照常能查到；查不到行的订单报不可用，而不是不存在
// （分不清它是在排队还是不存在，不能答 404，更不能把“不存在”写进缓存）。
func TestGateDownStillServesLandedOrders(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	landed := e.mustPending(t, 1, "landed")
	queued := e.submit(t, 1, "queued", items(skuA, 1))
	if err := e.rdb.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := e.getOrder(t, landed.GetOrderId(), 1)
	if err != nil {
		t.Fatalf("GetOrder of a landed order with redis down: %v", err)
	}
	if got.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
		t.Fatalf("got %s, want PENDING_PAYMENT", got.GetStatus())
	}
	for _, id := range []int64{queued.GetOrderId(), 424242} {
		_, err := e.getOrder(t, id, 1)
		// 降级错误（不是 UNAVAILABLE，见 errx.Degraded）：分不清它是在排队还是不存在。
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonStockGateUnavailable)
	}
}
