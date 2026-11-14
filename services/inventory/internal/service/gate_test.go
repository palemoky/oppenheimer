package service

import (
	"errors"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/pkg/testredis"
)

func (e *env) setLimit(t *testing.T, skuID, limit int64) {
	t.Helper()
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET purchase_limit = ? WHERE sku_id = ?`, limit, skuID)
}

func (e *env) warmUp(t *testing.T, skuIDs ...int64) []int64 {
	t.Helper()
	resp, err := e.svc.WarmUp(t.Context(), &inventoryv1.WarmUpRequest{SkuIds: skuIDs})
	mustOK(t, err)
	return resp.GetMissing()
}

func (e *env) gateStock(t *testing.T, skuID int64) stockgate.Stock {
	t.Helper()
	st, loaded, err := e.gate.Stock(t.Context(), skuID)
	mustOK(t, err)
	if !loaded {
		t.Fatalf("sku %d not loaded into gate", skuID)
	}
	return st
}

func (e *env) assertGate(t *testing.T, skuID, avail, held int64) {
	t.Helper()
	if st := e.gateStock(t, skuID); st.Avail != avail || st.Held != held {
		t.Errorf("gate sku %d = (avail %d, held %d), want (%d, %d)", skuID, st.Avail, st.Held, avail, held)
	}
}

// hold 模拟订单服务的受理：用订单号做幂等键，在闸门里扣一个 SKU。
func (e *env) hold(t *testing.T, orderID, userID, skuID int64, qty int32) stockgate.Outcome {
	t.Helper()
	res, err := e.gate.Hold(t.Context(), stockgate.HoldRequest{
		OrderID: orderID, UserID: userID, IdempotencyKey: "k" + strconv.FormatInt(orderID, 10),
		RequestHash: []byte("h"), Items: []stockgate.Item{{SKUID: skuID, Quantity: qty}},
		Now: time.Now(), IdempotencyTTL: time.Hour,
	})
	if err != nil {
		t.Errorf("hold order %d: %v", orderID, err)
	}
	return res.Outcome
}

// deviation 返回 (avail + held) - (available + reserved)，见设计文档 6.6 的恒等式。
func (e *env) deviation(t *testing.T, skuID int64) int64 {
	t.Helper()
	g := e.gateStock(t, skuID)
	m := e.stock(t.Context(), t, skuID)
	return (g.Avail + g.Held) - (m.GetAvailable() + m.GetReserved())
}

// breakGate 把闸门换成一个已关闭的客户端，模拟 Redis 不可用；返回恢复函数。
func (e *env) breakGate(t *testing.T) (restore func()) {
	t.Helper()
	dead := redis.NewClient(&redis.Options{Addr: os.Getenv(testredis.AddrEnv)})
	_ = dead.Close()
	good := e.svc.gate
	e.svc.gate = stockgate.New(dead, "unused:")
	return func() { e.svc.gate = good }
}

func TestWarmUp(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	e.seed(t, 2, 100, 5)
	e.setLimit(t, 1, 2)

	// 重复的 SKU 只装载一次，不存在的 SKU 进 missing 而不报错。
	if missing := e.warmUp(t, 2, 1, 99, 1); !slices.Equal(missing, []int64{99}) {
		t.Fatalf("missing = %v, want [99]", missing)
	}
	if st := e.gateStock(t, 1); st != (stockgate.Stock{Avail: 10, Held: 0, Limit: 2}) {
		t.Errorf("gate sku 1 = %+v", st)
	}
	if st := e.gateStock(t, 2); st != (stockgate.Stock{Avail: 5, Held: 0, Limit: 0}) {
		t.Errorf("gate sku 2 = %+v", st)
	}

	// 已装载之后数量由 Hold、Return 维护：再次预热不能用 MySQL 的值覆盖还没落到 MySQL 的占用，
	// 但限购是配置，跟着 MySQL 更新。
	if got := e.hold(t, 7, 1, 1, 2); got != stockgate.Held {
		t.Fatalf("hold = %v", got)
	}
	e.setLimit(t, 1, 5)
	e.warmUp(t, 1)
	if st := e.gateStock(t, 1); st != (stockgate.Stock{Avail: 8, Held: 2, Limit: 5}) {
		t.Errorf("gate sku 1 after second warm up = %+v", st)
	}

	for name, ids := range map[string][]int64{"empty": nil, "zero": {1, 0}, "negative": {-3}} {
		t.Run(name, func(t *testing.T) {
			_, err := e.svc.WarmUp(t.Context(), &inventoryv1.WarmUpRequest{SkuIds: ids})
			assertErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
		})
	}
}

// TestWarmUpConcurrent 让多个预热与受理同时进行：装载只发生一次，之后的预热不覆盖已经扣掉的数量。
func TestWarmUpConcurrent(t *testing.T) {
	e := newEnv(t)
	const stock, warmers, buyers = 20, 16, 30
	e.seed(t, 1, 100, stock)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		held int64
	)
	for range warmers {
		wg.Go(func() {
			if _, err := e.svc.WarmUp(t.Context(), &inventoryv1.WarmUpRequest{SkuIds: []int64{1}}); err != nil {
				t.Errorf("WarmUp: %v", err)
			}
		})
	}
	for i := range buyers {
		wg.Go(func() {
			// 还没装载时得到 NotLoaded，这里不重试：要验证的是装载之后的扣减不会被后来的预热抹掉。
			if e.hold(t, int64(1000+i), int64(i+1), 1, 1) == stockgate.Held {
				mu.Lock()
				held++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	e.assertGate(t, 1, stock-held, held)
	if d := e.deviation(t, 1); d != 0 {
		t.Errorf("deviation = %d, want 0", d)
	}
}

func TestHandleOrderEventGate(t *testing.T) {
	cases := []struct {
		name string
		ev   func(int64) *eventv1.OrderEvent
		// 处理之后 SKU 1：MySQL 的 (available, sold)，闸门的 avail。held 总是 0。
		available, sold, avail int64
	}{
		{"paid", paidEvent, 7, 3, 7},
		{"cancelled", cancelledEvent, 10, 0, 10},
		{"closed", closedEvent, 10, 0, 10},
		{"failed", failedEvent, 10, 0, 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.seed(t, 1, 100, 10)
			e.setLimit(t, 1, 3)
			e.warmUp(t, 1)
			if got := e.hold(t, 7, 1, 1, 3); got != stockgate.Held {
				t.Fatalf("hold = %v", got)
			}
			_, err := e.reserve(t.Context(), 7, 1, 3)
			mustOK(t, err)

			for range 2 {
				mustOK(t, e.svc.HandleOrderEvent(t.Context(), c.ev(7)))
				e.assertStock(t, 1, c.available, 0, c.sold)
				e.assertGate(t, 1, c.avail, 0)
			}
			if _, exists, err := e.gate.Get(t.Context(), 7); err != nil || exists {
				t.Errorf("hold of order 7 exists = %v, err = %v; want gone", exists, err)
			}
			if d := e.deviation(t, 1); d != 0 {
				t.Errorf("deviation = %d, want 0", d)
			}
			// 限购 3：归还时用户的已购数量跟着退回，可以再买 3 个；结清时不退，再买一个就超限。
			want := stockgate.Held
			if c.sold > 0 {
				want = stockgate.LimitExceeded
			}
			if got := e.hold(t, 8, 1, 1, 3); got != want {
				t.Errorf("hold after %s = %v, want %v", c.name, got, want)
			}
		})
	}
}

// TestHandleOrderEventGateEmptyRollback：Reserve 没有成功（超时、SKU 不存在、多商家）的订单失败时，
// （ADR-0018 之前还有“MySQL 库存不足”，现在 Reserve 不再因此失败。）
// MySQL 只留下空回滚标记，闸门里受理时扣的数量照样归还。
func TestHandleOrderEventGateEmptyRollback(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	e.warmUp(t, 1)
	if got := e.hold(t, 7, 1, 1, 4); got != stockgate.Held {
		t.Fatalf("hold = %v", got)
	}
	for range 2 {
		mustOK(t, e.svc.HandleOrderEvent(t.Context(), failedEvent(7)))
	}
	e.assertStock(t, 1, 10, 0, 0)
	e.assertGate(t, 1, 10, 0)
}

func TestHandleOrderEventGateUnavailable(t *testing.T) {
	cases := []struct {
		name            string
		ev              func(int64) *eventv1.OrderEvent
		available, sold int64
		// 恢复后闸门的 avail。
		avail int64
	}{
		{"paid", paidEvent, 7, 3, 7},
		{"cancelled", cancelledEvent, 10, 0, 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.seed(t, 1, 100, 10)
			e.warmUp(t, 1)
			if got := e.hold(t, 7, 1, 1, 3); got != stockgate.Held {
				t.Fatalf("hold = %v", got)
			}
			_, err := e.reserve(t.Context(), 7, 1, 3)
			mustOK(t, err)

			restore := e.breakGate(t)
			err = e.svc.HandleOrderEvent(t.Context(), c.ev(7))
			if err == nil || kafkax.IsPermanent(err) || !errors.Is(err, errGate) {
				t.Fatalf("HandleOrderEvent with redis down = %v, want retryable gate error", err)
			}
			restore()
			// MySQL 那一步已经提交，闸门还是原样。
			e.assertStock(t, 1, c.available, 0, c.sold)
			e.assertGate(t, 1, 7, 3)

			// 消费者重试：MySQL 幂等重放，闸门补上。
			mustOK(t, e.svc.HandleOrderEvent(t.Context(), c.ev(7)))
			e.assertStock(t, 1, c.available, 0, c.sold)
			e.assertGate(t, 1, c.avail, 0)
		})
	}
}

// TestHandleOrderEventImpossibleKeepsGate：进死信的组合在 MySQL 那一步就返回，不动闸门。
// 订单与库存已经矛盾，闸门这时归还或结清都是在猜，留给人工处理时一起判断。
func TestHandleOrderEventImpossibleKeepsGate(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, e *env)
		ev    *eventv1.OrderEvent
	}{
		{
			// 只动 MySQL（内部的 release），模拟闸门还没跟上的状态。
			name:  "paid but released",
			setup: func(t *testing.T, e *env) { mustOK(t, e.svc.release(t.Context(), 7)) },
			ev:    paidEvent(7),
		},
		{
			name:  "cancelled but confirmed",
			setup: func(t *testing.T, e *env) { mustOK(t, e.svc.confirm(t.Context(), 7)) },
			ev:    cancelledEvent(7),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.seed(t, 1, 100, 10)
			e.warmUp(t, 1)
			if got := e.hold(t, 7, 1, 1, 3); got != stockgate.Held {
				t.Fatalf("hold = %v", got)
			}
			_, err := e.reserve(t.Context(), 7, 1, 3)
			mustOK(t, err)
			c.setup(t, e)

			if err := e.svc.HandleOrderEvent(t.Context(), c.ev); !kafkax.IsPermanent(err) {
				t.Fatalf("HandleOrderEvent = %v, want permanent", err)
			}
			e.assertGate(t, 1, 7, 3)
			if _, exists, err := e.gate.Get(t.Context(), 7); err != nil || !exists {
				t.Errorf("hold of order 7 exists = %v, err = %v; want kept", exists, err)
			}
		})
	}
}

// TestGRPCConfirmReleaseUseGate：排障用的 gRPC 接口与事件走同一条路径，闸门失败时返回可重试的 Unavailable。
func TestGRPCConfirmReleaseUseGate(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	e.warmUp(t, 1)
	for _, id := range []int64{7, 8} {
		if got := e.hold(t, id, id, 1, 2); got != stockgate.Held {
			t.Fatalf("hold = %v", got)
		}
		_, err := e.reserve(t.Context(), id, 1, 2)
		mustOK(t, err)
	}

	restore := e.breakGate(t)
	if err := e.confirm(t.Context(), 7); status.Code(err) != codes.Unavailable {
		t.Fatalf("Confirm with redis down = %v, want Unavailable", err)
	}
	restore()
	mustOK(t, e.confirm(t.Context(), 7))
	mustOK(t, e.release(t.Context(), 8))

	e.assertStock(t, 1, 8, 0, 2)
	e.assertGate(t, 1, 8, 0)
}

// TestGateIdentityLifecycle 走完订单的完整生命周期，每一步之后检查恒等式
// avail + held == available + reserved。只有“Confirm 已提交、Settle 还没执行”这一步两边不等，
// 而且差值恰好是这笔订单的数量。
func TestGateIdentityLifecycle(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 20)
	e.warmUp(t, 1)

	check := func(step string, want int64) {
		t.Helper()
		if d := e.deviation(t, 1); d != want {
			t.Errorf("%s: deviation = %d, want %d", step, d, want)
		}
	}
	check("load", 0)

	// 订单 1：受理 → 预占 → 支付。
	e.hold(t, 1, 1, 1, 3)
	check("hold 1", 0)
	_, err := e.reserve(t.Context(), 1, 1, 3)
	mustOK(t, err)
	check("reserve 1", 0)
	mustOK(t, e.svc.confirm(t.Context(), 1)) // 只做 MySQL 那一步，观察中间状态
	check("confirm 1 before settle", 3)
	mustOK(t, e.svc.HandleOrderEvent(t.Context(), paidEvent(1)))
	check("paid 1", 0)

	// 订单 2：受理 → 预占 → 取消。
	e.hold(t, 2, 2, 1, 4)
	check("hold 2", 0)
	_, err = e.reserve(t.Context(), 2, 1, 4)
	mustOK(t, err)
	check("reserve 2", 0)
	mustOK(t, e.svc.release(t.Context(), 2))
	check("release 2 before return", 0)
	mustOK(t, e.svc.HandleOrderEvent(t.Context(), cancelledEvent(2)))
	check("cancelled 2", 0)

	// 订单 3：受理后没能预占就失败（空回滚）。
	e.hold(t, 3, 3, 1, 5)
	check("hold 3", 0)
	mustOK(t, e.svc.HandleOrderEvent(t.Context(), failedEvent(3)))
	check("failed 3", 0)

	e.assertStock(t, 1, 17, 0, 3)
	e.assertGate(t, 1, 17, 0)
}
