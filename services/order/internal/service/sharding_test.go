package service

import (
	"database/sql"
	"testing"
	"time"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 一批连续的用户号下单、落库后，8 张订单表的行数相同，且每个订单都在它的用户所在的 slot。
func TestOrdersSpreadEvenlyAcrossSlots(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const perSlot = 3
	for user := int64(1); user <= testSlots*perSlot; user++ {
		o := e.submit(t, user, "k", items(skuA, 1))
		e.mustLand(t, o.GetOrderId())
		if got, want := e.router.ForOrder(o.GetOrderId()), e.router.ForUser(user); got != want {
			t.Fatalf("order %d of user %d is in slot %d, user is in slot %d", o.GetOrderId(), user, got, want)
		}
		if n := e.countSlot(t, "orders", e.router.ForUser(user), "order_id = ? AND user_id = ?", o.GetOrderId(), user); n != 1 {
			t.Fatalf("order %d not found in its user's slot", o.GetOrderId())
		}
	}
	for slot := range e.router.Slots() {
		if n := e.countSlot(t, "orders", slot, ""); n != perSlot {
			t.Errorf("orders_%d has %d rows, want %d", slot, n, perSlot)
		}
	}
}

// 事务提交后只踢事务所在库的中继。
func TestKickOnlyRelayOfOrdersDB(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for _, user := range []int64{1, 2} { // 用户 1 在库 1，用户 2 在库 0
		o := e.mustPending(t, user, "k")
		db := e.router.DB(e.router.ForUser(user))
		before := make([]int64, len(e.relays))
		for i, r := range e.relays {
			before[i] = r.kicks.Load()
		}
		if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), user)); err != nil {
			t.Fatal(err)
		}
		for i, r := range e.relays {
			want := int64(0)
			if i == db {
				want = 1
			}
			if got := r.kicks.Load() - before[i]; got != want {
				t.Errorf("user %d (db %d): relay %d kicked %d times, want %d", user, db, i, got, want)
			}
		}
	}
	// eventsByDB 顺带检查了每条事件都写在订单所在库的 outbox 里。
	if n := len(e.allEvents(t)); n != 2 {
		t.Errorf("events = %d, want 2", n)
	}
}

// sweeper 一轮扫完所有 slot。
func TestSweeperScansAllSlots(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var orders []*orderv1.Order
	for user := int64(1); user <= testSlots; user++ {
		orders = append(orders, e.mustPending(t, user, "k"))
	}
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	e.sweeper.RunOnce(t.Context())
	for _, o := range orders {
		wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
		e.wantEvents(t, o.GetOrderId(), "closed")
	}
	e.consumeEvents(t)
	e.wantBalanced(t, skuA)
}

// 一个库不可用时，sweeper 记日志、跳过它的 slot，另一个库上的订单照常处理。
func TestSweeperSkipsUnavailableDB(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	inDB0 := e.mustPending(t, 2, "k") // slot 2，库 0
	inDB1 := e.mustPending(t, 1, "k") // slot 1，库 1
	e.clock.Advance(DefaultPaymentTimeout + time.Second)

	half, err := store.New([]*sql.DB{e.dbs[0], brokenDB(t)}, e.router)
	if err != nil {
		t.Fatal(err)
	}
	svc := mustNew(t, Deps{Store: half, Gate: e.gate, Sender: e.sender, Inventory: e.inv, IDs: nil, Logger: e.svc.logger}, Config{
		Clock:            e.clock.Now,
		OrderEventsTopic: testEventsTopic,
		Relays:           e.kickers(),
	})
	sw, err := NewSweeper(svc, SweeperConfig{})
	if err != nil {
		t.Fatal(err)
	}
	sw.RunOnce(t.Context())

	wantStatus(t, e.load(t, inDB0.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
	wantStatus(t, e.load(t, inDB1.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)

	// 库恢复后（用正常的 sweeper）补上。
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, inDB1.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
	for _, o := range []*orderv1.Order{inDB0, inDB1} {
		e.wantEvents(t, o.GetOrderId(), "closed")
	}
}

// 一个库挂起（连得上但不响应）时，它上面的每个 slot 最多耗 SlotTimeout，之后照常扫后面的 slot，
// 另一个库上过期的订单被关闭。与上一个测试的区别：那里查询立刻失败，这里查询会一直等。
func TestSweeperSlotTimeoutSkipsHungDB(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	inDB0 := e.mustPending(t, 2, "k") // slot 2，库 0
	inDB1 := e.mustPending(t, 1, "k") // slot 1，库 1
	e.clock.Advance(DefaultPaymentTimeout + time.Second)

	// 挂起的库放在库 0：slot 0 最先扫，能验证超时之后后面的 slot 仍被扫到。
	// 连接池只有一个连接且被占住，sweeper 的查询拿不到连接，只能等到 ctx 超时。
	hung, _ := testdb.Empty(t)
	hung.SetMaxOpenConns(1)
	held, err := hung.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	half, err := store.New([]*sql.DB{hung, e.dbs[1]}, e.router)
	if err != nil {
		t.Fatal(err)
	}
	svc := mustNew(t, Deps{Store: half, Gate: e.gate, Sender: e.sender, Inventory: e.inv, IDs: nil, Logger: e.svc.logger}, Config{
		Clock:            e.clock.Now,
		OrderEventsTopic: testEventsTopic,
		Relays:           e.kickers(),
	})
	const slotTimeout = 100 * time.Millisecond
	sw, err := NewSweeper(svc, SweeperConfig{SlotTimeout: slotTimeout})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	sw.RunOnce(t.Context())
	// 库 0 上有 4 个 slot，每个最多耗一个 SlotTimeout；留足余量，只排除“卡住不返回”。
	if elapsed, limit := time.Since(start), 20*slotTimeout*time.Duration(e.router.Slots()); elapsed > limit {
		t.Errorf("RunOnce took %s with a hung db, want under %s", elapsed, limit)
	}

	wantStatus(t, e.load(t, inDB1.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
	wantStatus(t, e.load(t, inDB0.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	e.wantEvents(t, inDB1.GetOrderId(), "closed")
}

// 按订单号定位：订单号所在的 slot 里没有就是不存在，不去别的 slot 找。
// 构造一个基因指向另一个 slot 的订单号（换掉低 6 位），查不到。
func TestGetOrderOnlyLooksInOrdersSlot(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	forged := o.GetOrderId()&^63 | 2 // 同一个时间戳和序号，基因改成 2
	if _, err := e.store.GetOrder(t.Context(), forged); err == nil {
		t.Fatalf("GetOrder(%d) found an order, want not found", forged)
	}
	_, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: forged, UserId: 1})
	if err == nil {
		t.Fatalf("GetOrder via service found forged order %d", forged)
	}
}
