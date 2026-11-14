package service

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// 扩容切换时，被搬走的 slot 的表在旧库上被改名作围栏（ADR-0015），这个 slot 上的读写得到“表不存在”。
// 这些测试守着窗口期间的行为：消息必须留在队列里重试，不能进死信，不能改任何状态；
// 表恢复（演练里是路由切到新库）后重试成功。其他 slot 不受影响。

// fence 把 slot 的三张表改名，返回恢复的函数。一条 RENAME TABLE 对多张表是原子的。
func (e *env) fence(t *testing.T, slot int) (restore func()) {
	t.Helper()
	db := e.dbs[e.router.DB(slot)]
	rename := func(from, to string) {
		t.Helper()
		var parts []string
		for _, base := range []string{"orders", "order_items", "payment_results"} {
			parts = append(parts, from+sharding.Table(base, slot)+" TO "+to+sharding.Table(base, slot))
		}
		stmt := strings.Join(parts, ", ")
		testdb.MustExec(t, db, "RENAME TABLE "+stmt)
	}
	rename("", "moved_")
	return func() { rename("moved_", "") }
}

// retryable 断言 err 是消费者会原地重试的错误：不是 nil，也没有被标成进死信。
func retryable(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s succeeded while its shard is fenced", what)
	}
	if kafkax.IsPermanent(err) || errors.Is(err, ErrInvalidEvent) || errors.Is(err, ErrUnrecoverable) {
		t.Fatalf("%s would go to the dead letter topic: %v", what, err)
	}
}

func TestLandRetriesWhileShardFenced(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const fenced, other = 2, 1 // 用户号即 slot 号
	moved := e.submit(t, fenced, "k", items(skuA, 2))
	kept := e.submit(t, other, "k", items(skuA, 1))

	restore := e.fence(t, fenced)
	for range 3 {
		retryable(t, "landing", e.land(t, moved.GetOrderId()))
	}
	// 没有预占，占用还在：重试时还能认领到，回收任务在宽限期内也不会动它。
	if n := e.inv.appliedCount("Reserve"); n != 0 {
		t.Errorf("reserve applied %d times while fenced, want 0", n)
	}
	if _, ok := e.hold(t, moved.GetOrderId()); !ok {
		t.Fatal("hold of the fenced order is gone")
	}
	// 接口返回可重试的错误，而不是“订单不存在”或内部错误。
	_, err := e.svc.CancelOrder(t.Context(), cancelReq(moved.GetOrderId(), fenced))
	wantErr(t, err, codes.Unavailable, "")
	_, err = e.svc.ListUserOrders(t.Context(), &orderv1.ListUserOrdersRequest{UserId: fenced})
	wantErr(t, err, codes.Unavailable, "")

	// 别的 slot 照常。
	e.mustLand(t, kept.GetOrderId())
	wantStatus(t, e.load(t, kept.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)

	restore()
	e.mustLand(t, moved.GetOrderId())
	wantStatus(t, e.load(t, moved.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if n := e.inv.appliedCount("Reserve"); n != 2 {
		t.Errorf("reserve applied %d times, want 2", n)
	}
	e.wantBalanced(t, skuA)
}

// 支付结果是订单变成已支付的唯一途径：窗口期间必须重试，不能给出任何处置（尤其不能当成“订单不存在”去退款）。
func TestPaymentResultRetriesWhileShardFenced(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const fenced = 2
	o := e.mustPending(t, fenced, "k")
	amount := o.GetTotal().GetAmount()

	restore := e.fence(t, fenced)
	before := e.outboxRows(t)
	for range 3 {
		_, err := e.pay(t, o.GetOrderId(), 9001, amount, true)
		retryable(t, "payment result", err)
	}
	if n := e.outboxRows(t); n != before {
		t.Errorf("outbox grew from %d to %d while fenced", before, n)
	}

	restore()
	reply, err := e.pay(t, o.GetOrderId(), 9001, amount, true)
	if err != nil {
		t.Fatal(err)
	}
	if reply.GetDisposition() != eventv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED {
		t.Errorf("disposition = %v, want ACCEPTED", reply.GetDisposition())
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID)
}
