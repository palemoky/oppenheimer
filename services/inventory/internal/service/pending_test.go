package service

// 本文件是 ADR-0018（预占、确认、释放只记增量，后台的应用者合并进 sku_stock）在 Service 入口上的测试，
// 对应设计文档 1d 的 3.5。合并本身的正确性（恰好一次、多个应用者、先后、卡住与恢复）在 internal/apply 里测，
// 这里不重复；这里测的是真实入口在“应用落后”的各种时机下行为都对。

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/inventory/internal/apply"
	"github.com/palemoky/oppenheimer/services/inventory/internal/reconcile"
)

// TestConcurrentReservesDoNotWaitOnStockRow：并发预占、确认、释放同一个 SKU 时不再等 sku_stock 的行锁。
//
// 不对耗时做断言（测试机不稳定），而是直接证明“不碰那一行”：另开一个事务把 sku_stock 的这一行
// 以排他锁锁住、不提交，在锁被持有期间跑完 64 个并发预占和随后的确认、释放。以前它们的
// UPDATE sku_stock 会在这把锁上等到超时；现在必须全部在锁释放之前完成。
func TestConcurrentReservesDoNotWaitOnStockRow(t *testing.T) {
	e := newEnv(t)
	const orders = 64
	e.seed(t, 1, 100, 1000)

	lock, err := e.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback() }()
	var one int
	if err := lock.QueryRowContext(t.Context(), `SELECT 1 FROM sku_stock WHERE sku_id = 1 FOR UPDATE`).Scan(&one); err != nil {
		t.Fatal(err)
	}

	// 远小于 innodb_lock_wait_timeout：真在行锁上等的话，会在这里以超时失败，而不是挂住整个测试。
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	errs := make([]error, orders)
	var wg sync.WaitGroup
	for i := range orders {
		wg.Go(func() { _, errs[i] = e.reserve(ctx, int64(1000+i), 1, 1) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("reserve order %d while sku_stock row is locked: %v", 1000+i, err)
		}
	}
	if n := e.pendingRows(t, 0); n != orders {
		t.Fatalf("%d pending rows, want %d", n, orders)
	}
	// 一半确认、一半释放，同样在锁被持有期间。
	for i := range orders {
		wg.Go(func() {
			if i%2 == 0 {
				errs[i] = e.confirm(ctx, int64(1000+i))
			} else {
				errs[i] = e.release(ctx, int64(1000+i))
			}
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("settle order %d while sku_stock row is locked: %v", 1000+i, err)
		}
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}

	if n := e.pendingRows(t, 0); n != 2*orders {
		t.Fatalf("%d pending rows, want %d", n, 2*orders)
	}
	e.assertRaw(t, 1, 1000, 0, 0)
	e.assertStock(t, 1, 1000-orders/2, 0, orders/2)
	e.applyAll(t)
	e.assertRaw(t, 1, 1000-orders/2, 0, orders/2)
}

// TestWarmUpLoadsEffectiveStock：预热装载的是真实的可售数（sku_stock 加上未应用的增量）。
// 只看 sku_stock 的话会装多，闸门会放行 MySQL 里已经没有的数量，这是会超卖的方向。
func TestWarmUpLoadsEffectiveStock(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 100) // 预热时增量还没应用
	e.seed(t, 2, 100, 100) // 对照：应用之后再预热

	// 每个 SKU：4 个订单各预占 10，其中一个释放、一个确认。可售 100 - 40 + 10 = 70。
	for sku := int64(1); sku <= 2; sku++ {
		base := sku * 100
		for i := range int64(4) {
			_, err := e.reserve(t.Context(), base+i, sku, 10)
			mustOK(t, err)
		}
		mustOK(t, e.release(t.Context(), base))
		mustOK(t, e.confirm(t.Context(), base+1))
	}
	e.assertRaw(t, 1, 100, 0, 0) // 前提：确实还没应用

	e.warmUp(t, 1)
	e.assertGate(t, 1, 70, 0)

	e.applyAll(t)
	e.assertRaw(t, 2, 70, 20, 10)
	e.warmUp(t, 2)
	e.assertGate(t, 2, 70, 0)
}

// newReconciler 返回一个使用真实时钟的对账器。连续几轮之间远小于间隔，不会被当成跳过了轮次。
func (e *env) newReconciler(t *testing.T) *reconcile.Reconciler {
	t.Helper()
	return reconcile.New(e.gate, e.store, reconcile.Config{
		Interval: time.Minute,
		Logger:   slog.New(slog.NewTextHandler(t.Output(), nil)),
	})
}

// TestReconcileWithUnappliedDeltas：闸门和 MySQL 实际是平的，只是 MySQL 这边有一批没应用的增量
// （包括确认，它改变 available + reserved），对账不产生任何修正。反过来，有未应用增量时
// 真实的偏差仍然被发现，并修正到正确的值。
func TestReconcileWithUnappliedDeltas(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 100)
	e.warmUp(t, 1)
	r := e.newReconciler(t)

	// 5 个订单各受理并预占 2；2 个支付、1 个取消，2 个在途。全部经真实的事件路径（MySQL 一步加闸门一步）。
	for i := range int64(5) {
		if got := e.hold(t, 10+i, 10+i, 1, 2); got != stockgate.Held {
			t.Fatalf("hold = %v", got)
		}
		_, err := e.reserve(t.Context(), 10+i, 1, 2)
		mustOK(t, err)
	}
	mustOK(t, e.svc.HandleOrderEvent(t.Context(), paidEvent(10)))
	mustOK(t, e.svc.HandleOrderEvent(t.Context(), paidEvent(11)))
	mustOK(t, e.svc.HandleOrderEvent(t.Context(), cancelledEvent(12)))

	// 前提：增量都没应用。只看 sku_stock 的话 available + reserved = 100，而闸门是 92 + 4 = 96，
	// 偏差 -4 会在第 3 轮被“修正”成 avail 96，多放 4 件。
	e.assertRaw(t, 1, 100, 0, 0)
	e.assertGate(t, 1, 92, 4)
	for i := range 6 {
		if err := r.Round(t.Context()); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		e.assertGate(t, 1, 92, 4)
	}

	// 真实的偏差：有人把 MySQL 的 available 直接改小了 5（模拟盘点、手工修数据），增量仍未应用。
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET available = available - 5 WHERE sku_id = 1`)
	e.totals[1] -= 5 // 入库总量随之变了，结束时的检查按新总量核对
	for range 3 {
		if err := r.Round(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	// 真实的可售 92 - 5 = 87，闸门修到 87，偏差归零。
	e.assertGate(t, 1, 87, 4)
	if d := e.deviation(t, 1); d != 0 {
		t.Errorf("deviation after correction = %d, want 0", d)
	}
	// 应用之后再对几轮，仍然什么都不改。
	e.applyAll(t)
	for range 4 {
		if err := r.Round(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	e.assertGate(t, 1, 87, 4)
}

// TestLifecycleWithLaggingApplier：预占 →（应用或不应用）→ 确认或释放 →（应用或不应用），
// 每一步 GetStock 返回的数都对；最后应用一遍，sku_stock 与“每一步都立刻应用”的结果相同。
func TestLifecycleWithLaggingApplier(t *testing.T) {
	for _, settle := range []string{"confirm", "release"} {
		for _, applyAfterReserve := range []bool{false, true} {
			for _, applyAfterSettle := range []bool{false, true} {
				name := fmt.Sprintf("%s/apply_after_reserve=%v/apply_after_settle=%v", settle, applyAfterReserve, applyAfterSettle)
				t.Run(name, func(t *testing.T) {
					e := newEnv(t)
					e.seed(t, 1, 100, 10)
					e.seed(t, 2, 100, 10)
					// 一个订单两个 SKU，另有一个不相干的订单一直挂着预占，让“应用”不是从空表开始。
					_, err := e.reserve(t.Context(), 9, 1, 1)
					mustOK(t, err)

					_, err = e.reserve(t.Context(), 7, 1, 3, 2, 4)
					mustOK(t, err)
					e.assertStock(t, 1, 6, 4, 0)
					e.assertStock(t, 2, 6, 4, 0)
					if applyAfterReserve {
						e.applyAll(t)
						e.assertRaw(t, 1, 6, 4, 0)
						e.assertRaw(t, 2, 6, 4, 0)
					} else {
						e.assertRaw(t, 1, 10, 0, 0)
					}

					// 每一步都立刻应用时，结束后 sku_stock 应当是的值。
					want1, want2 := [3]int64{9, 1, 0}, [3]int64{10, 0, 0}
					if settle == "confirm" {
						mustOK(t, e.confirm(t.Context(), 7))
						want1, want2 = [3]int64{6, 1, 3}, [3]int64{6, 0, 4}
					} else {
						mustOK(t, e.release(t.Context(), 7))
					}
					e.assertStock(t, 1, want1[0], want1[1], want1[2])
					e.assertStock(t, 2, want2[0], want2[1], want2[2])
					if applyAfterSettle {
						e.applyAll(t)
						e.assertRaw(t, 1, want1[0], want1[1], want1[2])
					}

					e.applyAll(t)
					e.assertRaw(t, 1, want1[0], want1[1], want1[2])
					e.assertRaw(t, 2, want2[0], want2[1], want2[2])
					e.assertStock(t, 1, want1[0], want1[1], want1[2])
					e.assertStock(t, 2, want2[0], want2[1], want2[2])
				})
			}
		}
	}
}

// TestNoExtraDeltas：空回滚、重复请求、被状态机拒绝的请求都不产生增量。每次状态变化恰好一份增量，
// 是“应用之后数是对的”的前提，而它只靠预占记录的状态机保证（Pend* 自己不去重）。
func TestNoExtraDeltas(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 100)
	ctx := t.Context()
	assertPending := func(step string, orderID int64, want int) {
		t.Helper()
		if n := e.pendingRows(t, orderID); n != want {
			t.Errorf("%s: order %d has %d pending rows, want %d", step, orderID, n, want)
		}
	}

	// 空回滚：释放先于预占到达，留下“已释放”标记，之后的预占被拒绝。
	mustOK(t, e.release(ctx, 1))
	mustOK(t, e.svc.HandleOrderEvent(ctx, failedEvent(1)))
	_, err := e.reserve(ctx, 1, 1, 3)
	assertErr(t, err, codes.FailedPrecondition, errx.ReasonReservationReleased)
	assertPending("empty rollback", 1, 0)

	// 重复预占、重复确认（gRPC 和事件混着来）、确认之后的释放被拒绝。
	for range 3 {
		_, err := e.reserve(ctx, 2, 1, 3)
		mustOK(t, err)
	}
	assertPending("repeated reserve", 2, 1)
	mustOK(t, e.confirm(ctx, 2))
	mustOK(t, e.confirm(ctx, 2))
	mustOK(t, e.svc.HandleOrderEvent(ctx, paidEvent(2)))
	assertPending("repeated confirm", 2, 2)
	assertErr(t, e.release(ctx, 2), codes.FailedPrecondition, errx.ReasonReservationConfirmed)
	assertPending("release after confirm", 2, 2)

	// 重复释放、释放之后的确认和预占被拒绝。
	_, err = e.reserve(ctx, 3, 1, 4)
	mustOK(t, err)
	mustOK(t, e.release(ctx, 3))
	mustOK(t, e.svc.HandleOrderEvent(ctx, cancelledEvent(3)))
	mustOK(t, e.release(ctx, 3))
	assertPending("repeated release", 3, 2)
	assertErr(t, e.confirm(ctx, 3), codes.FailedPrecondition, errx.ReasonReservationReleased)
	_, err = e.reserve(ctx, 3, 1, 4)
	assertErr(t, err, codes.FailedPrecondition, errx.ReasonReservationReleased)
	assertPending("confirm or reserve after release", 3, 2)

	// 并发的重复：同一个订单 10 个并发确认、10 个并发释放（不同订单），各只多一份增量。
	for _, id := range []int64{4, 5} {
		_, err := e.reserve(ctx, id, 1, 1)
		mustOK(t, err)
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if err := e.confirm(ctx, 4); err != nil {
				t.Errorf("concurrent confirm: %v", err)
			}
		})
		wg.Go(func() {
			if err := e.release(ctx, 5); err != nil {
				t.Errorf("concurrent release: %v", err)
			}
		})
	}
	wg.Wait()
	assertPending("concurrent confirm", 4, 2)
	assertPending("concurrent release", 5, 2)

	// 订单 2 确认 3，订单 3 释放，订单 4 确认 1，订单 5 释放。
	e.assertStock(t, 1, 96, 0, 4)
}

// TestApplyLoopAsComponent：应用者作为真的周期任务运行，期间持续经 Service 预占、确认、释放；
// 停止写入后在几秒内追平。再验证循环停止后留下的增量由 Drain 收尾（cmd/inventory 停止时的顺序）。
func TestApplyLoopAsComponent(t *testing.T) {
	e := newEnv(t)
	const (
		skus    = 3
		total   = 10000
		writers = 6
		orders  = 40 // 每个写入方
	)
	for sku := int64(1); sku <= skus; sku++ {
		e.seed(t, sku, 100, total)
	}
	a, err := apply.New(e.store, apply.Config{
		Interval: 5 * time.Millisecond,
		Batch:    16,
		Logger:   slog.New(slog.NewTextHandler(t.Output(), nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	loop, err := apply.NewLoop(a)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- loop.Start(t.Context()) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		if err := loop.Stop(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("stop: %v", err)
		}
		if err := <-done; err != nil {
			t.Errorf("start: %v", err)
		}
	}
	t.Cleanup(stop)

	// 每个订单：预占 2，然后三分之一确认、其余释放。
	var sold [skus + 1]atomic.Int64
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range orders {
				id := int64(w*orders + i + 1)
				sku := id%skus + 1
				if _, err := e.reserve(t.Context(), id, sku, 2); err != nil {
					t.Errorf("reserve %d: %v", id, err)
					return
				}
				var err error
				if id%3 == 0 {
					err = e.confirm(t.Context(), id)
					sold[sku].Add(2)
				} else {
					err = e.release(t.Context(), id)
				}
				if err != nil {
					t.Errorf("settle %d: %v", id, err)
					return
				}
			}
		})
	}
	wg.Wait()

	waitDrained := func(step string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for e.pendingRows(t, 0) != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("%s: %d pending rows not applied in time", step, e.pendingRows(t, 0))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitDrained("loop")
	for sku := int64(1); sku <= skus; sku++ {
		s := sold[sku].Load()
		e.assertRaw(t, sku, total-s, 0, s)
		e.assertStock(t, sku, total-s, 0, s)
	}

	// 循环停止之后还有写入（停止时 gRPC 和消费者的收尾），留下的增量由 Drain 应用完。
	stop()
	for i := range int64(5) {
		_, err := e.reserve(t.Context(), 10000+i, 1, 1)
		mustOK(t, err)
	}
	if n := e.pendingRows(t, 0); n != 5 {
		t.Fatalf("%d pending rows after the loop stopped, want 5", n)
	}
	if err := a.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := e.pendingRows(t, 0); n != 0 || len(a.Stuck()) != 0 {
		t.Fatalf("after drain: %d pending rows, stuck %v", n, a.Stuck())
	}
	s := sold[1].Load()
	e.assertRaw(t, 1, total-s-5, 5, s)
}

// ---------- 发现的问题（见报告）：真实的可售数可以是负的 ----------
//
// ADR-0018 之前 sku_stock 有 CHECK 约束，读到的 available 不可能是负数。现在 GetStock 返回的是
// “sku_stock 加上未应用的增量”，闸门放行多了（MySQL 一侧超卖、SKU 卡住）时它如实为负。
// 下面两个测试钉住两个按“available 不为负”写的读者在这种情况下的行为。

// TestWarmUpOversoldSKU：SKU 在 MySQL 一侧已经超卖（卡住），这时需要重新装载闸门（例如 Redis 丢了数据）。
// 应当装载 0（一件都不能再卖），而不是报“闸门不可用”让调用方无休止地重试。
func TestWarmUpOversoldSKU(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 2)
	for i := range int64(5) {
		_, err := e.reserve(t.Context(), 10+i, 1, 1)
		mustOK(t, err)
	}
	e.assertStock(t, 1, -3, 5, 0)
	e.expectStuck(1)

	resp, err := e.svc.WarmUp(t.Context(), &inventoryv1.WarmUpRequest{SkuIds: []int64{1}})
	if err != nil {
		t.Fatalf("WarmUp of an oversold sku = %v; want it loaded with avail 0", err)
	}
	if len(resp.GetMissing()) != 0 {
		t.Fatalf("missing = %v", resp.GetMissing())
	}
	e.assertGate(t, 1, 0, 0)
}

// TestReconcileOversoldSKU：闸门被装多了（装载错误、有人直接改了数），放行的订单超过了 MySQL 里有的。
// 这时对账是唯一能把闸门收紧的机制：偏差稳定之后应当把 avail 修到 0，不再继续放行。
func TestReconcileOversoldSKU(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 2)
	// 闸门装成了 10（错误），MySQL 只有 2。
	if _, err := e.gate.Load(t.Context(), 1, 10, 0); err != nil {
		t.Fatal(err)
	}
	for i := range int64(5) {
		if got := e.hold(t, 10+i, 10+i, 1, 1); got != stockgate.Held {
			t.Fatalf("hold = %v", got)
		}
		_, err := e.reserve(t.Context(), 10+i, 1, 1)
		mustOK(t, err)
	}
	e.assertStock(t, 1, -3, 5, 0)
	e.assertGate(t, 1, 5, 5)
	e.expectStuck(1)

	r := e.newReconciler(t)
	var roundErr error
	for range 4 {
		if err := r.Round(t.Context()); err != nil {
			roundErr = err
		}
	}
	if roundErr != nil {
		t.Errorf("reconcile round on an oversold sku: %v", roundErr)
	}
	// 偏差 (5 + 5) - (-3 + 5) = 8；按增量修是 avail 5 - 8 = -3，截到 0。
	e.assertGate(t, 1, 0, 5)
}
