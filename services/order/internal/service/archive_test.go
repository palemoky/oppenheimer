package service

import (
	"testing"
	"time"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/services/order/internal/archive"
)

// 本文件测试冷热归档对在线路径的影响（设计文档 1c 第 6.6 节）。归档任务本身在 internal/archive 里测。

func newColdEnv(t *testing.T) *env {
	t.Helper()
	return newEnvWith(t, envOptions{cold: true})
}

// age 把服务的时钟拨到“现有的订单号都已经老到可能归档”之后。
// 测试里的订单号用真实时钟生成，服务用 fakeClock，所以以真实的现在为基准。
func (e *env) age(t *testing.T) {
	t.Helper()
	e.clock.Advance(time.Now().Add(archive.MinHotAge + time.Hour).Sub(e.clock.Now()))
}

// moveToCold 扮演归档任务：把订单的三张表的行原样搬到冷库，再从热库删掉。
// 测试用的热库和冷库在同一个 MySQL 实例上，所以直接跨库 INSERT ... SELECT。
func (e *env) moveToCold(t *testing.T, orderID int64) {
	t.Helper()
	var coldName string
	if err := e.coldDB.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&coldName); err != nil {
		t.Fatal(err)
	}
	hot := e.dbOf(orderID)
	exec := func(q string) {
		t.Helper()
		if _, err := hot.ExecContext(t.Context(), q, orderID); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	const orderCols = `order_id, user_id, merchant_id, version, status, total_amount, currency, idempotency_key, request_hash,
		payment_id, fail_reason, expire_at, paid_at, created_at, updated_at`
	const itemCols = "order_id, sku_id, quantity, unit_price, created_at"
	const payCols = "order_id, payment_id, succeeded, amount, disposition, created_at"
	exec("INSERT INTO `" + coldName + "`.orders_archive (" + orderCols + ") SELECT " + orderCols + " FROM " + e.table("orders", orderID) + " WHERE order_id = ?")
	exec("INSERT INTO `" + coldName + "`.order_items_archive (" + itemCols + ") SELECT " + itemCols + " FROM " + e.table("order_items", orderID) + " WHERE order_id = ?")
	exec("INSERT INTO `" + coldName + "`.payment_results_archive (" + payCols + ") SELECT " + payCols + " FROM " + e.table("payment_results", orderID) + " WHERE order_id = ?")
	exec("DELETE FROM " + e.table("payment_results", orderID) + " WHERE order_id = ?")
	exec("DELETE FROM " + e.table("order_items", orderID) + " WHERE order_id = ?")
	exec("DELETE FROM " + e.table("orders", orderID) + " WHERE order_id = ?")
}

// replies 返回 outbox 里对 paymentID 的全部答复。
func (e *env) replies(t *testing.T, paymentID int64) []*eventv1.PaymentResultHandled {
	t.Helper()
	var found []*eventv1.PaymentResultHandled
	for _, ev := range e.allEvents(t) {
		if h := ev.GetPaymentResultHandled(); h != nil && h.GetPaymentId() == paymentID {
			found = append(found, h)
		}
	}
	return found
}

// wantNoHotRows 检查在线路径没有在热库里为已归档的订单留下任何行。
func (e *env) wantNoHotRows(t *testing.T) {
	t.Helper()
	for _, base := range []string{"orders", "order_items", "payment_results"} {
		if n := e.count(t, base, ""); n != 0 {
			t.Errorf("hot %s has %d rows, want 0", base, n)
		}
	}
}

// 已支付的订单归档之后，重放它的支付结果：不能要求退款。
// 没有冷库路径时，热库里订单和处理记录都没有了，这条消息会走“订单不存在 → 退款”。
func TestPaymentReplayAfterArchiveIsNoOp(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	o := e.mustPending(t, 1, "k")
	ev := paymentEvent(o.GetOrderId(), 7, o.GetTotal().GetAmount(), true)
	got, err := e.deliver(t, ev)
	wantReply(t, got, err, accepted, "")
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID)

	e.age(t)
	e.moveToCold(t, o.GetOrderId())
	outbox := e.outboxRows(t)

	for range 3 {
		if err := e.svc.HandlePaymentEvent(t.Context(), ev); err != nil {
			t.Fatalf("replay after archive: %v", err)
		}
	}
	if n := e.outboxRows(t); n != outbox {
		t.Errorf("outbox grew from %d to %d on replay, want no new events", outbox, n)
	}
	if rs := e.replies(t, 7); len(rs) != 1 || rs[0].GetDisposition() != accepted {
		t.Errorf("replies to payment 7 = %v, want the single original ACCEPTED", rs)
	}
	e.wantNoHotRows(t)
}

// 已归档的订单上来了一笔没处理过的支付：成功的要求退款，失败的只需知悉。
// 答复不留记录，重复投递得出同样的答复。
func TestNewPaymentOnArchivedOrder(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	o := e.mustPending(t, 1, "k")
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	e.age(t)
	e.moveToCold(t, o.GetOrderId())

	for i := range 2 {
		if err := e.svc.HandlePaymentEvent(t.Context(), paymentEvent(o.GetOrderId(), 8, o.GetTotal().GetAmount(), true)); err != nil {
			t.Fatalf("delivery %d of succeeded payment: %v", i, err)
		}
		if err := e.svc.HandlePaymentEvent(t.Context(), paymentEvent(o.GetOrderId(), 9, o.GetTotal().GetAmount(), false)); err != nil {
			t.Fatalf("delivery %d of failed payment: %v", i, err)
		}
	}
	rs := e.replies(t, 8)
	if len(rs) != 2 {
		t.Fatalf("succeeded payment has %d replies, want one per delivery (2)", len(rs))
	}
	for _, r := range rs {
		if r.GetDisposition() != refundRequired || r.GetReason() != errx.ReasonOrderNotPayable {
			t.Errorf("reply to succeeded payment = %s %q, want REFUND_REQUIRED %q", r.GetDisposition(), r.GetReason(), errx.ReasonOrderNotPayable)
		}
	}
	rs = e.replies(t, 9)
	if len(rs) != 2 {
		t.Fatalf("failed payment has %d replies, want 2", len(rs))
	}
	for _, r := range rs {
		if r.GetDisposition() != accepted {
			t.Errorf("reply to failed payment = %s, want ACCEPTED", r.GetDisposition())
		}
	}
	// 答复带的是订单自己的 user_id（事件的 key 和分区由它决定）。
	for _, ev := range e.allEvents(t) {
		if ev.GetPaymentResultHandled() != nil && (ev.GetOrderId() != o.GetOrderId() || ev.GetUserId() != 1) {
			t.Errorf("reply event for order %d user %d, want order %d user 1", ev.GetOrderId(), ev.GetUserId(), o.GetOrderId())
		}
	}
	e.wantNoHotRows(t)
}

// 冷库里也没有的订单，仍然按“订单不存在”处理：老订单号和新订单号都一样。
func TestPaymentForUnknownOrderWithColdStore(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	e.age(t)
	oldID := idgen.LowerBound(e.clock.Now().Add(-2*archive.MinHotAge)) | 1
	newID := idgen.LowerBound(e.clock.Now()) | 1
	if !archive.MaybeCold(oldID, e.clock.Now()) || archive.MaybeCold(newID, e.clock.Now()) {
		t.Fatal("test setup: ids are not on the expected sides of MinHotAge")
	}
	for i, id := range []int64{oldID, newID} {
		got, err := e.pay(t, id, int64(20+i), 100, true)
		wantReply(t, got, err, refundRequired, errx.ReasonOrderNotFound)
	}
}

// 冷库查不了的时候不给答复，返回错误让消费者重试。不能在不知道订单是否归档的情况下要求退款。
func TestPaymentForOldOrderFailsWhenColdStoreIsDown(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	o := e.mustPending(t, 1, "k")
	ev := paymentEvent(o.GetOrderId(), 7, o.GetTotal().GetAmount(), true)
	if _, err := e.deliver(t, ev); err != nil {
		t.Fatal(err)
	}
	e.age(t)
	e.moveToCold(t, o.GetOrderId())
	outbox := e.outboxRows(t)
	if err := e.coldDB.Close(); err != nil {
		t.Fatal(err)
	}

	if err := e.svc.HandlePaymentEvent(t.Context(), ev); err == nil {
		t.Fatal("replay with the cold store down returned nil, want an error")
	}
	if n := e.outboxRows(t); n != outbox {
		t.Errorf("outbox grew from %d to %d, want no reply while the cold store is down", outbox, n)
	}
}

// 订单号老到可能已归档的受理消息不再落库：热库里不会重新出现一个旧订单号。
func TestStaleSubmissionIsNotInserted(t *testing.T) {
	t.Parallel()
	e := newEnv(t) // 这条规则不依赖是否配置了冷库
	o := e.submit(t, 1, "k", items(skuA, 2))
	e.age(t)

	for range 2 {
		e.mustLand(t, o.GetOrderId())
	}
	if n := e.orderRows(t); n != 0 {
		t.Fatalf("hot orders = %d after landing a stale submission, want 0", n)
	}
	if n := e.outboxRows(t); n != 0 {
		t.Errorf("outbox = %d, want 0: a dropped submission emits nothing", n)
	}
	// 占用没有被认领，留给回收任务。
	if h, ok := e.hold(t, o.GetOrderId()); !ok || h.Claimed {
		t.Errorf("hold = %+v, %v; want still present and unclaimed", h, ok)
	}
}
