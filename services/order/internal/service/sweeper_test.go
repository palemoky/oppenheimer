package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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
	wantStatus(t, e.load(t, expired.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	e.wantEvents(t, expired.GetOrderId())

	e.clock.Advance(time.Second)
	kicks := e.relay.kicks.Load()
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, expired.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
	wantStatus(t, e.load(t, fresh.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	e.wantEvents(t, expired.GetOrderId(), "closed")
	e.wantEvents(t, fresh.GetOrderId())
	if e.relay.kicks.Load() <= kicks {
		t.Error("relay was not kicked after close")
	}
	// 库存的释放由库存服务消费 closed 事件完成，这里只预占过。
	wantStock(t, e.inv, skuA, 996, 4, 0)

	// 再扫一轮、直接再关一次：条件更新不成立，不再写事件。
	e.sweeper.RunOnce(t.Context())
	e.sweeper.closeOrder(t.Context(), refOf(expired), e.svc.now())
	e.wantEvents(t, expired.GetOrderId(), "closed")

	// 关闭后取消不了；很久之后才到的付款要退款。
	_, err := e.svc.CancelOrder(t.Context(), cancelReq(expired.GetOrderId(), 1))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderNotCancellable)
	e.clock.Advance(24 * time.Hour)
	reply, err := e.pay(t, expired.GetOrderId(), 1, expired.GetTotal().GetAmount(), true)
	wantReply(t, reply, err, refundRequired, errx.ReasonOrderNotPayable)
	e.wantEvents(t, expired.GetOrderId(), "closed", "handled")
}

func TestSweeperBatchSize(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	sw, err := NewSweeper(e.svc, SweeperConfig{BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
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
	if n := e.outboxRows(t); n != 3 {
		t.Errorf("outbox = %d, want 3", n)
	}
}

// 多个副本同时扫描：每个订单只关一次、只有一条 closed 事件。
func TestSweeperConcurrentReplicas(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const orders = 10
	for i := range orders {
		e.mustPending(t, 1, fmt.Sprintf("k%d", i))
	}
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	var wg sync.WaitGroup
	for range 4 {
		sw, err := NewSweeper(e.svc, SweeperConfig{})
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() { sw.RunOnce(t.Context()) })
	}
	wg.Wait()
	if n := e.count(t, "SELECT COUNT(*) FROM orders WHERE status = ?", int32(orderv1.OrderStatus_ORDER_STATUS_CLOSED)); n != orders {
		t.Fatalf("closed = %d, want %d", n, orders)
	}
	if n := e.outboxRows(t); n != orders {
		t.Errorf("outbox = %d, want %d", n, orders)
	}
}

func TestSweeperStartStop(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	e.clock.Advance(DefaultPaymentTimeout + time.Second)

	sw, err := NewSweeper(e.svc, SweeperConfig{Interval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
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
	e.wantEvents(t, o.GetOrderId(), "closed")
}

// 在第一次触发之前停止：不访问数据库，Start 立即返回。
func TestSweeperStopBeforeFirstTick(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		svc := New(nil, nil, nil, slog.New(slog.DiscardHandler), Config{})
		sw, err := NewSweeper(svc, SweeperConfig{Interval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
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
		sw, err := NewSweeper(svc, SweeperConfig{Interval: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
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
