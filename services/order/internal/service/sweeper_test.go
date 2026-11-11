package service

import (
	"context"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
)

func TestSweeperClosesExpiredOrders(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	expired := e.mustPending(t, 1, "old")
	e.clock.Advance(time.Minute)
	fresh := e.mustPending(t, 1, "new")

	// 刚好到截止时间还不关。
	e.clock.Advance(DefaultPaymentTimeout - time.Minute)
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, expired.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT, false)

	e.clock.Advance(time.Second)
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, expired.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED, true)
	wantStatus(t, e.load(t, fresh.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT, false)
	wantStock(t, e.inv, skuA, 998, 2, 0)

	// 关闭后取消不了，再付款要退款。
	_, err := e.svc.CancelOrder(t.Context(), cancelReq(expired.GetOrderId(), 1))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderNotCancellable)
	d, err := e.pay(t, expired.GetOrderId(), 1, expired.GetTotal().GetAmount(), true)
	if err != nil || d != refundRequired {
		t.Fatalf("pay closed order = %s, %v", d, err)
	}
}

func TestSweeperBatchSize(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	sw := NewSweeper(e.svc, SweeperConfig{BatchSize: 2})
	for _, key := range []string{"a", "b", "c"} {
		e.mustPending(t, 1, key)
	}
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	sw.RunOnce(t.Context())
	if n := e.count(t, "SELECT COUNT(*) FROM orders WHERE status = ?", int32(orderv1.OrderStatus_ORDER_STATUS_CLOSED)); n != 2 {
		t.Fatalf("closed = %d after one round, want 2", n)
	}
	sw.RunOnce(t.Context())
	if n := e.count(t, "SELECT COUNT(*) FROM orders WHERE status = ?", int32(orderv1.OrderStatus_ORDER_STATUS_CLOSED)); n != 3 {
		t.Fatalf("closed = %d after two rounds, want 3", n)
	}
}

// 结清调用失败后由后台任务在宽限期后补上；补结清重复执行无害。
func TestSweeperResettlesAfterFailure(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	cancelled := e.mustPending(t, 1, "cancel")
	e.inv.inject("Release", fault{err: errTimeout})
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(cancelled.GetOrderId(), 1)); err != nil {
		t.Fatalf("cancel must succeed even if settle fails: %v", err)
	}
	wantStatus(t, e.load(t, cancelled.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CANCELLED, false)

	paid := e.mustPending(t, 1, "paid")
	// 确认实际成功但返回超时。
	e.inv.inject("Confirm", fault{errAfterApply: errTimeout})
	if d, err := e.pay(t, paid.GetOrderId(), 1, paid.GetTotal().GetAmount(), true); err != nil || d != accepted {
		t.Fatalf("pay = %s, %v", d, err)
	}
	wantStatus(t, e.load(t, paid.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID, false)

	// 宽限期内不补。
	e.sweeper.RunOnce(t.Context())
	if n := e.inv.callCount("Release"); n != 1 {
		t.Errorf("release called %d times within grace, want 1", n)
	}

	e.clock.Advance(DefaultSettleGrace + time.Second)
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, cancelled.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CANCELLED, true)
	wantStatus(t, e.load(t, paid.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID, true)
	wantStock(t, e.inv, skuA, 998, 0, 2)

	// 已结清的不再处理；直接重复结清也不改变库存。
	e.clock.Advance(time.Hour)
	e.sweeper.RunOnce(t.Context())
	if err := e.svc.settle(t.Context(), cancelled.GetOrderId(), orderv1.OrderStatus_ORDER_STATUS_CANCELLED); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.settle(t.Context(), paid.GetOrderId(), orderv1.OrderStatus_ORDER_STATUS_PAID); err != nil {
		t.Fatal(err)
	}
	if e.inv.appliedCount("Release") != 1 || e.inv.appliedCount("Confirm") != 1 {
		t.Errorf("applied release=%d confirm=%d, want 1 each",
			e.inv.appliedCount("Release"), e.inv.appliedCount("Confirm"))
	}
	wantStock(t, e.inv, skuA, 998, 0, 2)
}

// 库存返回不可能的组合时保持未结清，不影响同一轮里的其他订单。
func TestSweeperLeavesInvariantViolationUnsettled(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	broken := e.mustPending(t, 1, "broken")
	e.inv.inject("Confirm",
		fault{err: errx.New(codes.FailedPrecondition, errx.ReasonReservationReleased, "released")},
		fault{err: errx.New(codes.NotFound, errx.ReasonReservationNotFound, "not found")},
	)
	if d, err := e.pay(t, broken.GetOrderId(), 1, broken.GetTotal().GetAmount(), true); err != nil || d != accepted {
		t.Fatalf("pay = %s, %v", d, err)
	}
	healthy := e.mustPending(t, 1, "healthy")
	e.inv.inject("Release", fault{err: errTimeout})
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(healthy.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}

	e.clock.Advance(DefaultSettleGrace + time.Second)
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, broken.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID, false)
	wantStatus(t, e.load(t, healthy.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CANCELLED, true)
	if n := e.inv.callCount("Confirm"); n != 2 {
		t.Errorf("confirm called %d times, want 2", n)
	}
}

func TestSweeperStartStop(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	e.clock.Advance(DefaultPaymentTimeout + time.Second)

	sw := NewSweeper(e.svc, SweeperConfig{Interval: 5 * time.Millisecond})
	errc := make(chan error, 1)
	go func() { errc <- sw.Start(t.Context()) }()

	deadline := time.Now().Add(5 * time.Second)
	for e.load(t, o.GetOrderId()).Status != orderv1.OrderStatus_ORDER_STATUS_CLOSED {
		if time.Now().After(deadline) {
			t.Fatal("sweeper did not close the expired order")
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := sw.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("Start: %v", err)
	}
	// 重复 Stop 无害。
	if err := sw.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

// 在第一次触发之前停止：不访问数据库，Start 立即返回。
func TestSweeperStopBeforeFirstTick(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		svc := New(nil, nil, nil, slog.New(slog.DiscardHandler), Config{})
		sw := NewSweeper(svc, SweeperConfig{Interval: time.Hour})
		errc := make(chan error, 1)
		go func() { errc <- sw.Start(t.Context()) }()
		synctest.Wait()

		time.Sleep(59 * time.Minute)
		if err := sw.Stop(t.Context()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if err := <-errc; err != nil {
			t.Fatalf("Start: %v", err)
		}
	})
}

func TestSweeperStartReturnsOnContextCancel(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		svc := New(nil, nil, nil, slog.New(slog.DiscardHandler), Config{})
		sw := NewSweeper(svc, SweeperConfig{Interval: time.Hour})
		ctx, cancel := context.WithCancel(t.Context())
		errc := make(chan error, 1)
		go func() { errc <- sw.Start(ctx) }()
		synctest.Wait()
		cancel()
		if err := <-errc; err != nil {
			t.Fatalf("Start: %v", err)
		}
	})
}
