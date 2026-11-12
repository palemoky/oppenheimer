package service

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
)

// 未认领的旧占用被归还（并删除幂等键），宽限期内的和已认领的不动。
func TestReaperReturnsOrphans(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	orphan := e.submit(t, 1, "orphan", items(skuA, 1))
	claimed := e.submit(t, 1, "claimed", items(skuA, 2))
	e.mustLand(t, claimed.GetOrderId())
	e.clock.Advance(DefaultReapGrace - time.Second)
	fresh := e.submit(t, 1, "fresh", items(skuA, 4))
	e.clock.Advance(2 * time.Second)

	st := e.reaper.RunOnce(t.Context())
	if st.Returned != 1 || st.Claimed != 0 || st.Stuck != 0 {
		t.Fatalf("stats = %+v, want 1 returned and nothing else", st)
	}
	if _, ok := e.hold(t, orphan.GetOrderId()); ok {
		t.Error("orphan hold still exists")
	}
	if h, ok := e.hold(t, claimed.GetOrderId()); !ok || !h.Claimed {
		t.Error("claimed hold was touched")
	}
	if _, ok := e.hold(t, fresh.GetOrderId()); !ok {
		t.Error("hold within grace period was reaped")
	}
	e.wantGate(t, skuA, 994, 6)

	// 再跑一轮：没有新的可回收。
	if st := e.reaper.RunOnce(t.Context()); st.Returned != 0 {
		t.Errorf("second round returned %d", st.Returned)
	}
	// 被回收的订单的幂等键已删除，同一幂等键可以重新下单。
	again := e.submit(t, 1, "orphan", items(skuA, 1))
	if again.GetOrderId() == orphan.GetOrderId() {
		t.Error("reaped order id came back")
	}
	e.wantGate(t, skuA, 993, 7)
}

// 回收与落库并发：同一笔占用只有一方得手。回收得手的订单不落库，落库得手的占用不被回收；
// 库存不多不少，两边始终平衡。
func TestReaperRacesLanding(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const n = 30
	ids := make([]int64, 0, n)
	for i := range n {
		ids = append(ids, e.submit(t, int64(i+1), "k", items(skuA, 1)).GetOrderId())
	}
	e.clock.Advance(DefaultReapGrace + time.Second)

	var (
		wg, landers sync.WaitGroup
		start       = make(chan struct{})
		done        = make(chan struct{})
	)
	// 回收任务反复扫，落库错开开始，让两种先后都出现。
	for range 3 {
		wg.Go(func() {
			<-start
			for {
				e.reaper.RunOnce(t.Context())
				select {
				case <-done:
					return
				default:
				}
			}
		})
	}
	for i, id := range ids {
		landers.Go(func() {
			<-start
			time.Sleep(time.Duration(i) * 500 * time.Microsecond)
			if err := e.land(t, id); err != nil {
				t.Errorf("land %d: %v", id, err)
			}
		})
	}
	close(start)
	landers.Wait()
	close(done)
	wg.Wait()

	landed := 0
	for _, id := range ids {
		_, held := e.hold(t, id)
		o, err := e.store.GetOrder(t.Context(), id)
		switch {
		case err == nil:
			landed++
			wantStatus(t, o, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
			if !held {
				t.Errorf("order %d landed but its hold was reaped", id)
			}
		case held:
			t.Errorf("order %d neither landed nor reaped", id)
		}
	}
	t.Logf("landed %d of %d", landed, n)
	wantStock(t, e.inv, skuA, int64(1000-landed), int64(landed), 0)
	e.wantGate(t, skuA, int64(1000-landed), int64(landed))
	e.wantBalanced(t, skuA)
}

// 防止：已认领的占用把孤儿挡在回收任务的一批之外。待支付订单的占用要活到支付时限结束，
// 它们比孤儿更早受理、数量超过一批时，孤儿仍然必须被回收。
func TestReaperNotStarvedByClaimedHolds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r, err := NewReaper(e.svc, ReaperConfig{BatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		e.mustPending(t, 1, strconv.Itoa(i))
	}
	orphan := e.submit(t, 1, "orphan", items(skuA, 1))
	e.clock.Advance(DefaultReapGrace + time.Second)

	if st := r.RunOnce(t.Context()); st.Returned != 1 || st.Stuck != 0 {
		t.Fatalf("stats = %+v, want the orphan returned", st)
	}
	if _, ok := e.hold(t, orphan.GetOrderId()); ok {
		t.Fatal("orphan behind claimed holds was not reaped")
	}
}

// 防止：认领之后卡住的占用无声无息地留在闸门里。超过支付时限两倍还没结束的已认领占用要被报告，
// 但不能被自动归还（不知道 MySQL 里有没有预占）。
func TestReaperReportsStuckClaimedHolds(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 1, "stuck", items(skuA, 2))
	if _, ok, err := e.svc.gate.Claim(t.Context(), o.GetOrderId()); err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}

	e.clock.Advance(2*DefaultPaymentTimeout - time.Second)
	if st := e.reaper.RunOnce(t.Context()); st.Stuck != 0 {
		t.Fatalf("stats = %+v, want nothing stuck yet", st)
	}
	e.clock.Advance(2 * time.Second)
	if st := e.reaper.RunOnce(t.Context()); st.Stuck != 1 || st.Returned != 0 {
		t.Fatalf("stats = %+v, want 1 stuck and none returned", st)
	}
	if h, ok := e.hold(t, o.GetOrderId()); !ok || !h.Claimed {
		t.Fatal("stuck hold was touched")
	}
}

func TestReaperStartStop(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 1, "k", items(skuA, 1))
	e.clock.Advance(DefaultReapGrace + time.Second)
	r, err := NewReaper(e.svc, ReaperConfig{Interval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- r.Start(t.Context()) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := e.hold(t, o.GetOrderId()); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reaper did not return the orphan hold")
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := r.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.wantGate(t, skuA, 1000, 0)
}
