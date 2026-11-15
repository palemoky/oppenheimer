package stockgate_test

import (
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/pkg/testredis"
)

const ttl = time.Hour

type env struct {
	t      *testing.T
	gate   *stockgate.Gate
	rdb    redis.UniversalClient
	prefix string
	nextID atomic.Int64
}

func newEnv(t *testing.T) *env {
	rdb, prefix := testredis.New(t)
	e := &env{t: t, gate: stockgate.New(rdb, prefix), rdb: rdb, prefix: prefix}
	e.nextID.Store(1000)
	return e
}

func (e *env) load(sku, avail, limit int64) {
	e.t.Helper()
	if _, err := e.gate.Load(e.t.Context(), sku, avail, limit); err != nil {
		e.t.Fatal(err)
	}
}

// hold 用新的订单号和以订单号为名的幂等键受理一次。
func (e *env) hold(user int64, items ...stockgate.Item) stockgate.HoldResult {
	e.t.Helper()
	id := e.nextID.Add(1)
	return e.holdAs(id, user, "k"+strconv.FormatInt(id, 10), "h", items...)
}

func (e *env) holdAs(order, user int64, key, hash string, items ...stockgate.Item) stockgate.HoldResult {
	e.t.Helper()
	res, err := e.gate.Hold(e.t.Context(), stockgate.HoldRequest{
		OrderID: order, UserID: user, IdempotencyKey: key, RequestHash: []byte(hash),
		Items: items, Now: time.Now(), IdempotencyTTL: ttl,
	})
	if err != nil {
		// Errorf 而不是 Fatal：这个辅助函数也在并发的 goroutine 里用，Fatal 只能在测试的 goroutine 里调用。
		e.t.Errorf("hold order %d: %v", order, err)
	}
	return res
}

// parallel 用固定数量的 worker 并发执行 n 次 fn。
// 不为每次调用起一个 goroutine：几百个 goroutine 同时新建 Redis 连接，在带竞态检测的慢环境里会超时，
// 那测的是连接池而不是脚本的原子性。
func parallel(n int, fn func(i int)) {
	const workers = 16
	var wg sync.WaitGroup
	var next atomic.Int64
	for range workers {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				fn(i)
			}
		})
	}
	wg.Wait()
}

func (e *env) stock(sku int64) stockgate.Stock {
	e.t.Helper()
	st, ok, err := e.gate.Stock(e.t.Context(), sku)
	if err != nil {
		e.t.Fatal(err)
	}
	if !ok {
		e.t.Fatalf("sku %d not loaded", sku)
	}
	return st
}

func (e *env) assertStock(sku, avail, held int64) {
	e.t.Helper()
	if st := e.stock(sku); st.Avail != avail || st.Held != held {
		e.t.Fatalf("sku %d: avail=%d held=%d, want avail=%d held=%d", sku, st.Avail, st.Held, avail, held)
	}
}

func item(sku int64, qty int32) stockgate.Item { return stockgate.Item{SKUID: sku, Quantity: qty} }

func must[T any](t *testing.T) func(T, error) T {
	return func(v T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

// 防止：并发受理时扣多了（超卖的源头）或扣少了。库存 50，200 个请求各买 1 个，恰好 50 个成功。
func TestHoldNeverOverdraws(t *testing.T) {
	e := newEnv(t)
	e.load(1, 50, 0)

	var held, rejected atomic.Int64
	parallel(200, func(i int) {
		switch res := e.hold(int64(i+1), item(1, 1)); res.Outcome {
		case stockgate.Held:
			held.Add(1)
		case stockgate.Insufficient:
			rejected.Add(1)
		default:
			t.Errorf("unexpected outcome %v", res.Outcome)
		}
	})

	if held.Load() != 50 || rejected.Load() != 150 {
		t.Fatalf("held=%d rejected=%d, want 50 and 150", held.Load(), rejected.Load())
	}
	e.assertStock(1, 0, 50)
}

// 防止：多 SKU 订单扣了一半。第二个 SKU 不够时，第一个也不能扣，也不能留下占用和幂等键。
func TestHoldIsAllOrNothing(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)
	e.load(2, 1, 0)

	res := e.holdAs(7, 100, "key", "h", item(1, 3), item(2, 2))
	if res.Outcome != stockgate.Insufficient || res.SKUID != 2 {
		t.Fatalf("result = %+v, want Insufficient on sku 2", res)
	}
	e.assertStock(1, 10, 0)
	e.assertStock(2, 1, 0)
	if _, ok, _ := e.gate.Get(t.Context(), 7); ok {
		t.Fatal("rejected hold left a hold record")
	}
	// 幂等键没有被占住：同一个键换一个买得到的数量可以成功。
	if res := e.holdAs(8, 100, "key", "h2", item(1, 3), item(2, 1)); res.Outcome != stockgate.Held {
		t.Fatalf("retry after rejection = %+v, want Held", res)
	}
	e.assertStock(1, 7, 3)
	e.assertStock(2, 0, 1)
}

// 防止：没装载的 SKU 被当成库存为 0（应当让调用方去装载），或被凭空创建。
func TestHoldNotLoaded(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)
	res := e.hold(100, item(1, 1), item(2, 1))
	if res.Outcome != stockgate.NotLoaded || res.SKUID != 2 {
		t.Fatalf("result = %+v, want NotLoaded on sku 2", res)
	}
	e.assertStock(1, 10, 0)
	if _, ok, _ := e.gate.Stock(t.Context(), 2); ok {
		t.Fatal("sku 2 was created by a rejected hold")
	}
}

// 防止：限购不生效；归还后额度不恢复；支付后额度被错误地恢复。
func TestPurchaseLimit(t *testing.T) {
	e := newEnv(t)
	e.load(1, 100, 2)

	first := e.hold(100, item(1, 2))
	if first.Outcome != stockgate.Held {
		t.Fatalf("first = %+v", first)
	}
	if res := e.hold(100, item(1, 1)); res.Outcome != stockgate.LimitExceeded {
		t.Fatalf("over limit = %+v, want LimitExceeded", res)
	}
	// 别的用户不受影响。
	if res := e.hold(200, item(1, 2)); res.Outcome != stockgate.Held {
		t.Fatalf("other user = %+v, want Held", res)
	}

	// 取消后额度恢复。
	must[bool](t)(e.gate.Return(t.Context(), first.OrderID))
	second := e.hold(100, item(1, 2))
	if second.Outcome != stockgate.Held {
		t.Fatalf("after return = %+v, want Held", second)
	}
	// 支付后额度不恢复：已经买到了。
	must[bool](t)(e.gate.Settle(t.Context(), second.OrderID))
	if res := e.hold(100, item(1, 1)); res.Outcome != stockgate.LimitExceeded {
		t.Fatalf("after settle = %+v, want LimitExceeded", res)
	}
}

// 防止：同一个幂等键扣两次；内容不同的复用没有被识别出来。
func TestIdempotencyKey(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)

	if res := e.holdAs(7, 100, "key", "h", item(1, 3)); res.Outcome != stockgate.Held || res.OrderID != 7 {
		t.Fatalf("first = %+v", res)
	}
	// 重试用了新的订单号，但幂等键相同：返回第一次的订单号，不再扣。
	if res := e.holdAs(8, 100, "key", "h", item(1, 3)); res.Outcome != stockgate.Duplicate || res.OrderID != 7 {
		t.Fatalf("duplicate = %+v, want Duplicate of order 7", res)
	}
	if res := e.holdAs(9, 100, "key", "other", item(1, 1)); res.Outcome != stockgate.KeyReused || res.OrderID != 7 {
		t.Fatalf("reused = %+v, want KeyReused of order 7", res)
	}
	// 幂等键按用户隔离。
	if res := e.holdAs(10, 200, "key", "h", item(1, 3)); res.Outcome != stockgate.Held {
		t.Fatalf("other user same key = %+v, want Held", res)
	}
	e.assertStock(1, 4, 6)

	// 并发的重复请求只有一个扣到。
	var heldCount atomic.Int64
	parallel(20, func(i int) {
		if res := e.holdAs(int64(100+i), 300, "same", "h", item(1, 1)); res.Outcome == stockgate.Held {
			heldCount.Add(1)
		}
	})
	if heldCount.Load() != 1 {
		t.Fatalf("concurrent duplicates held %d times, want 1", heldCount.Load())
	}
	e.assertStock(1, 3, 7)
}

// 防止：归还执行多次加回多次（库存凭空变多）。这是占用记录存在的理由。
func TestReturnIsIdempotent(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)
	e.load(2, 10, 0)
	res := e.hold(100, item(1, 3), item(2, 4))
	e.assertStock(1, 7, 3)

	var returned atomic.Int64
	parallel(10, func(int) {
		ok, err := e.gate.Return(t.Context(), res.OrderID)
		if err != nil {
			t.Error(err)
		}
		if ok {
			returned.Add(1)
		}
	})
	if returned.Load() != 1 {
		t.Fatalf("returned %d times, want 1", returned.Load())
	}
	e.assertStock(1, 10, 0)
	e.assertStock(2, 10, 0)

	// 没有受理过的订单号：什么都不做。
	if ok := must[bool](t)(e.gate.Return(t.Context(), 424242)); ok {
		t.Fatal("return of unknown order reported true")
	}
	e.assertStock(1, 10, 0)
}

// 防止：已支付的订单被归还（卖掉的又加回来）；结清把数量加了回去。
func TestSettleThenReturn(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)
	res := e.hold(100, item(1, 3))

	if ok := must[bool](t)(e.gate.Settle(t.Context(), res.OrderID)); !ok {
		t.Fatal("settle reported false")
	}
	e.assertStock(1, 7, 0)
	if ok := must[bool](t)(e.gate.Settle(t.Context(), res.OrderID)); ok {
		t.Fatal("second settle reported true")
	}
	if ok := must[bool](t)(e.gate.Return(t.Context(), res.OrderID)); ok {
		t.Fatal("return after settle reported true")
	}
	e.assertStock(1, 7, 0)
}

// 防止：回收和落库同时得手。同一笔占用，认领和“未认领才归还”并发，只能有一方成功：
// 认领成功则数量不加回，回收成功则认领必须失败。
func TestClaimAndReapAreMutuallyExclusive(t *testing.T) {
	e := newEnv(t)
	e.load(1, 1000, 0)

	for range 100 {
		res := e.hold(100, item(1, 1))
		before := e.stock(1)

		var claimed bool
		var reap stockgate.ReapResult
		var wg sync.WaitGroup
		wg.Go(func() {
			_, ok, err := e.gate.Claim(t.Context(), res.OrderID, time.Now())
			if err != nil {
				t.Error(err)
			}
			claimed = ok
		})
		wg.Go(func() {
			r, err := e.gate.ReturnIfUnclaimed(t.Context(), res.OrderID)
			if err != nil {
				t.Error(err)
			}
			reap = r
		})
		wg.Wait()

		after := e.stock(1)
		switch {
		case claimed && reap == stockgate.ReapClaimed:
			if after != before {
				t.Fatalf("claimed hold changed stock: %+v -> %+v", before, after)
			}
			// 清理，让下一轮从干净的状态开始。
			must[bool](t)(e.gate.Return(t.Context(), res.OrderID))
		case !claimed && reap == stockgate.ReapReturned:
			if after.Avail != before.Avail+1 || after.Held != before.Held-1 {
				t.Fatalf("reaped hold: %+v -> %+v", before, after)
			}
		default:
			t.Fatalf("claimed=%v reap=%v: both or neither won", claimed, reap)
		}
	}
}

// 防止：认领不幂等（重复投递的消息第二次认领失败，订单被丢掉）；Get 改变了认领状态。
func TestClaimAndGet(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)
	e.load(2, 10, 0)
	at := time.UnixMilli(1_700_000_000_123).UTC()
	_, err := e.gate.Hold(t.Context(), stockgate.HoldRequest{
		OrderID: 7, UserID: 100, IdempotencyKey: "k", RequestHash: []byte("h"),
		Items: []stockgate.Item{item(1, 2), item(2, 3)}, Now: at, IdempotencyTTL: ttl,
	})
	if err != nil {
		t.Fatal(err)
	}

	info, ok, err := e.gate.Get(t.Context(), 7)
	if err != nil || !ok {
		t.Fatalf("Get = %v, %v", ok, err)
	}
	want := []stockgate.Item{item(1, 2), item(2, 3)}
	if info.UserID != 100 || info.Claimed || !info.At.Equal(at) || !slices.Equal(info.Items, want) {
		t.Fatalf("Get = %+v", info)
	}
	for range 2 {
		info, ok, err = e.gate.Claim(t.Context(), 7, time.Now())
		if err != nil || !ok || !info.Claimed || !slices.Equal(info.Items, want) {
			t.Fatalf("Claim = %+v, %v, %v", info, ok, err)
		}
	}
	if r := must[stockgate.ReapResult](t)(e.gate.ReturnIfUnclaimed(t.Context(), 7)); r != stockgate.ReapClaimed {
		t.Fatalf("reap claimed hold = %v, want ReapClaimed", r)
	}
	if _, ok, _ := e.gate.Claim(t.Context(), 999, time.Now()); ok {
		t.Fatal("claim of unknown order succeeded")
	}
}

// 防止：没落库的订单被归还后，幂等键还指着那个不存在的订单，用户重试永远得到一个查不到的订单号。
// 而已落库的订单归还后，幂等键必须保留，重放才能得到同一个订单。
func TestAbortFreesIdempotencyKeyButReturnKeepsIt(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)

	e.holdAs(7, 100, "aborted", "h", item(1, 1))
	must[bool](t)(e.gate.Abort(t.Context(), 7))
	if res := e.holdAs(8, 100, "aborted", "h", item(1, 1)); res.Outcome != stockgate.Held || res.OrderID != 8 {
		t.Fatalf("after abort = %+v, want a fresh Held", res)
	}

	e.holdAs(9, 100, "reaped", "h", item(1, 1))
	must[stockgate.ReapResult](t)(e.gate.ReturnIfUnclaimed(t.Context(), 9))
	if res := e.holdAs(10, 100, "reaped", "h", item(1, 1)); res.Outcome != stockgate.Held {
		t.Fatalf("after reap = %+v, want a fresh Held", res)
	}

	e.holdAs(11, 100, "returned", "h", item(1, 1))
	must[bool](t)(e.gate.Return(t.Context(), 11))
	if res := e.holdAs(12, 100, "returned", "h", item(1, 1)); res.Outcome != stockgate.Duplicate || res.OrderID != 11 {
		t.Fatalf("after return = %+v, want Duplicate of order 11", res)
	}
}

// 防止：幂等键过期后补回的映射不生效。
func TestPointIdempotency(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)
	if err := e.gate.PointIdempotency(t.Context(), 100, "old", 55, []byte("h"), ttl); err != nil {
		t.Fatal(err)
	}
	if res := e.holdAs(56, 100, "old", "h", item(1, 1)); res.Outcome != stockgate.Duplicate || res.OrderID != 55 {
		t.Fatalf("result = %+v, want Duplicate of order 55", res)
	}
	e.assertStock(1, 10, 0)
}

// 防止：回收任务拿不到旧的占用，或把还在宽限期内的也拿出来。
func TestHoldsBefore(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)
	base := time.UnixMilli(1_700_000_000_000)
	for i, id := range []int64{1, 2, 3} {
		_, err := e.gate.Hold(t.Context(), stockgate.HoldRequest{
			OrderID: id, UserID: 100, IdempotencyKey: strconv.FormatInt(id, 10), RequestHash: []byte("h"),
			Items: []stockgate.Item{item(1, 1)}, Now: base.Add(time.Duration(i) * time.Minute), IdempotencyTTL: ttl,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	got := must[[]int64](t)(e.gate.HoldsBefore(t.Context(), base.Add(2*time.Minute), 10))
	if !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("HoldsBefore = %v, want [1 2]", got)
	}
	if got := must[[]int64](t)(e.gate.HoldsBefore(t.Context(), base.Add(time.Hour), 1)); !slices.Equal(got, []int64{1}) {
		t.Fatalf("HoldsBefore limit 1 = %v, want [1]", got)
	}
	// 归还后不再出现。
	must[bool](t)(e.gate.Return(t.Context(), 1))
	if got := must[[]int64](t)(e.gate.HoldsBefore(t.Context(), base.Add(time.Hour), 10)); !slices.Equal(got, []int64{2, 3}) {
		t.Fatalf("HoldsBefore after return = %v, want [2 3]", got)
	}

	// 认领后从待回收的列表移到已认领的列表：已认领的占用要活到订单结束，
	// 留在原列表里会一直排在最前面，把后面的孤儿挡在回收任务的一批之外。
	for range 2 { // 重复认领不改变结果
		if _, ok, err := e.gate.Claim(t.Context(), 2, time.Now()); err != nil || !ok {
			t.Fatalf("Claim = %v, %v", ok, err)
		}
	}
	if got := must[[]int64](t)(e.gate.HoldsBefore(t.Context(), base.Add(time.Hour), 10)); !slices.Equal(got, []int64{3}) {
		t.Fatalf("HoldsBefore after claim = %v, want [3]", got)
	}
	if got := must[[]int64](t)(e.gate.ClaimedBefore(t.Context(), base.Add(time.Hour), 10)); !slices.Equal(got, []int64{2}) {
		t.Fatalf("ClaimedBefore = %v, want [2]", got)
	}
	// 按受理时间而不是认领时间排：订单 2 是在 base+1m 受理的。
	if got := must[[]int64](t)(e.gate.ClaimedBefore(t.Context(), base.Add(time.Minute), 10)); len(got) != 0 {
		t.Fatalf("ClaimedBefore(base+1m) = %v, want none", got)
	}
	// 已认领的占用结束后，从已认领的列表里消失。
	must[bool](t)(e.gate.Settle(t.Context(), 2))
	if got := must[[]int64](t)(e.gate.ClaimedBefore(t.Context(), base.Add(time.Hour), 10)); len(got) != 0 {
		t.Fatalf("ClaimedBefore after settle = %v, want none", got)
	}
}

// Queued 数的是背压比较的那个队列：受理加一个未认领的，认领把它移到已认领，结束（回收、结清、归还）把它去掉；
// 被背压拒绝的受理不进队列。
func TestQueued(t *testing.T) {
	e := newEnv(t)
	want := func(unclaimed, claimed int64) {
		t.Helper()
		u, c, err := e.gate.Queued(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if u != unclaimed || c != claimed {
			t.Fatalf("Queued = %d unclaimed, %d claimed; want %d, %d", u, c, unclaimed, claimed)
		}
	}
	want(0, 0)
	e.load(1, 100, 0)
	now := time.Now()
	ids := e.admit(now, 5, stockgate.BacklogLimit{})
	want(5, 0)
	for _, id := range ids[:3] {
		e.claim(id, now)
	}
	e.claim(ids[0], now) // 重复认领不重复计数
	want(2, 3)

	// 背压按同一个数拒绝：上限取现在的未认领数，下一个受理被拒，拒绝时报的也是这个数。
	if res := e.holdAt(now, stockgate.BacklogLimit{MaxQueue: 2}); res.Outcome != stockgate.Backlogged || res.Queued != 2 {
		t.Fatalf("hold at the queue limit = %+v, want Backlogged with 2 queued", res)
	}
	want(2, 3)

	e.reap(ids[3:4])
	must[bool](t)(e.gate.Settle(t.Context(), ids[0]))
	must[bool](t)(e.gate.Return(t.Context(), ids[1]))
	want(1, 1)
}

// 防止：重复装载用 MySQL 的值覆盖闸门里的数量，抹掉还没落到 MySQL 的占用。
func TestLoadOnlyOnce(t *testing.T) {
	e := newEnv(t)
	if ok := must[bool](t)(e.gate.Load(t.Context(), 1, 10, 0)); !ok {
		t.Fatal("first load reported false")
	}
	e.hold(100, item(1, 4))
	if ok := must[bool](t)(e.gate.Load(t.Context(), 1, 10, 5)); ok {
		t.Fatal("second load reported true")
	}
	st := e.stock(1)
	if st.Avail != 6 || st.Held != 4 || st.Limit != 5 {
		t.Fatalf("stock after reload = %+v, want avail=6 held=4 limit=5", st)
	}
	if got := must[[]int64](t)(e.gate.SKUs(t.Context())); !slices.Equal(got, []int64{1}) {
		t.Fatalf("SKUs = %v", got)
	}
}

// 防止：对账修正把闸门调得比 MySQL 的可售还大，或调成负数；修正抹掉并发的扣减。
func TestAdjust(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)

	adjust := func(delta, maxAvail int64) int64 {
		t.Helper()
		v, ok, err := e.gate.Adjust(t.Context(), 1, delta, maxAvail)
		if err != nil || !ok {
			t.Errorf("Adjust = %v, %v", ok, err)
		}
		return v
	}
	if v := adjust(5, 100); v != 15 {
		t.Fatalf("adjust +5 = %d, want 15", v)
	}
	if v := adjust(50, 20); v != 20 {
		t.Fatalf("adjust capped = %d, want 20", v)
	}
	if v := adjust(-100, 20); v != 0 {
		t.Fatalf("adjust floored = %d, want 0", v)
	}
	if _, ok, err := e.gate.Adjust(t.Context(), 2, 1, 10); err != nil || ok {
		t.Fatalf("adjust unloaded sku = %v, %v; want false", ok, err)
	}

	// 增量修正与并发的扣减互不影响：从 100 开始，50 个扣减和一次 -10 的修正，最后一定是 40。
	adjust(100, 100)
	var wg sync.WaitGroup
	wg.Go(func() { parallel(50, func(i int) { e.hold(int64(i+1), item(1, 1)) }) })
	wg.Go(func() { adjust(-10, 1000) })
	wg.Wait()
	e.assertStock(1, 40, 50)
}

// 防止：Redis 丢过数据后，旧订单的归还把还没重新装载的 SKU 凭空创建出来（之后会被当作已装载）。
func TestReturnDoesNotResurrectMissingSKU(t *testing.T) {
	e := newEnv(t)
	e.load(1, 10, 0)
	res := e.hold(100, item(1, 3))
	if err := e.rdb.Del(t.Context(), e.prefix+"gate:sku:1").Err(); err != nil {
		t.Fatal(err)
	}
	if ok := must[bool](t)(e.gate.Return(t.Context(), res.OrderID)); !ok {
		t.Fatal("return reported false")
	}
	if _, ok, _ := e.gate.Stock(t.Context(), 1); ok {
		t.Fatal("return recreated the sku key")
	}
}

// 防止：参数不合法的请求进了脚本。
func TestHoldValidates(t *testing.T) {
	e := newEnv(t)
	good := stockgate.HoldRequest{
		OrderID: 1, UserID: 1, IdempotencyKey: "k", Items: []stockgate.Item{item(1, 1)},
		Now: time.Now(), IdempotencyTTL: ttl,
	}
	for name, mutate := range map[string]func(*stockgate.HoldRequest){
		"no order":   func(r *stockgate.HoldRequest) { r.OrderID = 0 },
		"no user":    func(r *stockgate.HoldRequest) { r.UserID = 0 },
		"no key":     func(r *stockgate.HoldRequest) { r.IdempotencyKey = "" },
		"no items":   func(r *stockgate.HoldRequest) { r.Items = nil },
		"no ttl":     func(r *stockgate.HoldRequest) { r.IdempotencyTTL = 0 },
		"bad qty":    func(r *stockgate.HoldRequest) { r.Items = []stockgate.Item{item(1, 0)} },
		"bad sku id": func(r *stockgate.HoldRequest) { r.Items = []stockgate.Item{item(0, 1)} },
	} {
		req := good
		mutate(&req)
		if _, err := e.gate.Hold(t.Context(), req); err == nil {
			t.Errorf("%s: Hold succeeded, want error", name)
		}
	}
}

// 积压的背压：排队超过 MaxWait 的未认领占用多于 MaxStale 个时拒绝受理，闸门不变。
func TestHoldBacklog(t *testing.T) {
	e := newEnv(t)
	e.load(1, 100, 0)
	start := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	limit := stockgate.BacklogLimit{MaxWait: 30 * time.Second, MaxStale: 2}
	hold := func(at time.Time, key string, backlog stockgate.BacklogLimit) stockgate.HoldResult {
		t.Helper()
		res, err := e.gate.Hold(t.Context(), stockgate.HoldRequest{
			OrderID: e.nextID.Add(1), UserID: 7, IdempotencyKey: key, RequestHash: []byte("h"),
			Items: []stockgate.Item{{SKUID: 1, Quantity: 1}}, Now: at, IdempotencyTTL: ttl, Backlog: backlog,
		})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	avail := func() int64 {
		t.Helper()
		st, _, err := e.gate.Stock(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		return st.Avail
	}

	// 落库在 start 还活着（心跳），这个测试只看第二条：排队超时的个数。
	if _, _, err := e.gate.Claim(t.Context(), 999_999, start); err != nil {
		t.Fatal(err)
	}
	// 三笔占用在 start 受理，之后一直没被认领。
	var first stockgate.HoldResult
	for i := range 3 {
		res := hold(start, "old"+strconv.Itoa(i), limit)
		if res.Outcome != stockgate.Held {
			t.Fatalf("old %d: %v", i, res.Outcome)
		}
		if i == 0 {
			first = res
		}
	}

	// 排队时长恰好等于 MaxWait 不算超时（截止是开区间）。
	if res := hold(start.Add(30*time.Second), "at-limit", limit); res.Outcome != stockgate.Held {
		t.Fatalf("at the limit: %v, want HELD", res.Outcome)
	}
	// 再晚 1 毫秒，三笔都超时，多于 MaxStale=2。
	late := start.Add(30*time.Second + time.Millisecond)
	before := avail()
	res := hold(late, "late", limit)
	if res.Outcome != stockgate.Backlogged || res.Queued != 3 {
		t.Fatalf("late: %+v, want BACKLOGGED with 3 stale", res)
	}
	if got := avail(); got != before {
		t.Fatalf("avail changed from %d to %d on a backlogged hold", before, got)
	}
	// 被拒绝时没有写幂等键：同一个键稍后可以正常受理。
	if res := hold(late, "late", stockgate.BacklogLimit{}); res.Outcome != stockgate.Held {
		t.Fatalf("retry of a backlogged key without limit: %v, want HELD", res.Outcome)
	}

	// 已受理过的请求重试时仍然得到原来的订单，不会因为积压被拒绝。
	if res := hold(late, "old0", limit); res.Outcome != stockgate.Duplicate || res.OrderID != first.OrderID {
		t.Fatalf("duplicate while backlogged: %+v", res)
	}
	// 卖完的请求得到“卖完”，而不是“稍后重试”。
	if res, err := e.gate.Hold(t.Context(), stockgate.HoldRequest{
		OrderID: e.nextID.Add(1), UserID: 7, IdempotencyKey: "huge", RequestHash: []byte("h"),
		Items: []stockgate.Item{{SKUID: 1, Quantity: 1000}}, Now: late, IdempotencyTTL: ttl, Backlog: limit,
	}); err != nil || res.Outcome != stockgate.Insufficient {
		t.Fatalf("sold out while backlogged: %+v, %v", res, err)
	}

	// 认领之后不再算在队列里：认领一笔，剩下两笔超时，不多于 MaxStale。
	if _, ok, err := e.gate.Claim(t.Context(), first.OrderID, late); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if res := hold(late, "after-claim", limit); res.Outcome != stockgate.Held {
		t.Fatalf("after claim: %v, want HELD", res.Outcome)
	}
}

func TestHoldRejectsNegativeBacklogLimit(t *testing.T) {
	e := newEnv(t)
	e.load(1, 100, 0)
	for _, b := range []stockgate.BacklogLimit{{MaxQueue: -1}, {MaxWait: -time.Second}, {MaxWait: time.Second, MaxStale: -1}} {
		_, err := e.gate.Hold(t.Context(), stockgate.HoldRequest{
			OrderID: e.nextID.Add(1), UserID: 7, IdempotencyKey: "k", RequestHash: []byte("h"),
			Items: []stockgate.Item{{SKUID: 1, Quantity: 1}}, Now: time.Now(), IdempotencyTTL: ttl, Backlog: b,
		})
		if err == nil {
			t.Errorf("backlog %+v: want error", b)
		}
	}
}

// 队列长度上限：未认领的占用达到 MaxQueue 时拒绝，队列永远不超过 MaxQueue；认领一笔就空出一个位置。
func TestHoldQueueLimit(t *testing.T) {
	e := newEnv(t)
	e.load(1, 100, 0)
	limit := stockgate.BacklogLimit{MaxQueue: 3}
	now := time.Now()
	hold := func(key string) stockgate.HoldResult {
		t.Helper()
		res, err := e.gate.Hold(t.Context(), stockgate.HoldRequest{
			OrderID: e.nextID.Add(1), UserID: 7, IdempotencyKey: key, RequestHash: []byte("h"),
			Items: []stockgate.Item{{SKUID: 1, Quantity: 1}}, Now: now, IdempotencyTTL: ttl, Backlog: limit,
		})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	var first int64
	for i := range 3 {
		res := hold("q" + strconv.Itoa(i))
		if res.Outcome != stockgate.Held {
			t.Fatalf("hold %d: %v", i, res.Outcome)
		}
		if i == 0 {
			first = res.OrderID
		}
	}
	if res := hold("q3"); res.Outcome != stockgate.Backlogged || res.Queued != 3 {
		t.Fatalf("4th hold: %+v, want BACKLOGGED with 3 queued", res)
	}
	if _, ok, err := e.gate.Claim(t.Context(), first, time.Now()); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if res := hold("q3"); res.Outcome != stockgate.Held {
		t.Fatalf("after claim: %v, want HELD", res.Outcome)
	}
}

// 并发受理时队列长度上限仍然严格成立：脚本里的检查和写入是原子的。
func TestHoldQueueLimitConcurrent(t *testing.T) {
	e := newEnv(t)
	e.load(1, 1000, 0)
	const maxQueue = 50
	var held atomic.Int64
	parallel(300, func(i int) {
		res, err := e.gate.Hold(t.Context(), stockgate.HoldRequest{
			OrderID: e.nextID.Add(1), UserID: int64(i + 1), IdempotencyKey: "c", RequestHash: []byte("h"),
			Items: []stockgate.Item{{SKUID: 1, Quantity: 1}}, Now: time.Now(), IdempotencyTTL: ttl,
			Backlog: stockgate.BacklogLimit{MaxQueue: maxQueue},
		})
		if err != nil {
			t.Errorf("hold: %v", err)
			return
		}
		if res.Outcome == stockgate.Held {
			held.Add(1)
		}
	})
	if n := held.Load(); n != maxQueue {
		t.Fatalf("held %d, want exactly %d", n, maxQueue)
	}
	if n, err := e.rdb.ZCard(t.Context(), e.prefix+"gate:holds").Result(); err != nil || n != maxQueue {
		t.Fatalf("queue length %d (%v), want %d", n, err, maxQueue)
	}
}

// ---------- 落库心跳（第三条背压） ----------

var stallLimit = stockgate.BacklogLimit{MaxWait: 30 * time.Second, MaxStale: 2}

// holdAt 在 at 时刻用新的订单号和幂等键受理一件 SKU 1。
func (e *env) holdAt(at time.Time, backlog stockgate.BacklogLimit) stockgate.HoldResult {
	e.t.Helper()
	id := e.nextID.Add(1)
	res, err := e.gate.Hold(e.t.Context(), stockgate.HoldRequest{
		OrderID: id, UserID: 7, IdempotencyKey: "k" + strconv.FormatInt(id, 10), RequestHash: []byte("h"),
		Items: []stockgate.Item{item(1, 1)}, Now: at, IdempotencyTTL: ttl, Backlog: backlog,
	})
	if err != nil {
		e.t.Errorf("hold order %d: %v", id, err)
	}
	return res
}

// admit 在 at 时刻连续受理最多 n 次，返回被受理的订单号；遇到 Backlogged 就停下。
func (e *env) admit(at time.Time, n int, backlog stockgate.BacklogLimit) []int64 {
	e.t.Helper()
	var ids []int64
	for range n {
		res := e.holdAt(at, backlog)
		switch res.Outcome {
		case stockgate.Held:
			ids = append(ids, res.OrderID)
		case stockgate.Backlogged:
			return ids
		default:
			e.t.Fatalf("hold: %+v", res)
		}
	}
	return ids
}

func (e *env) claim(orderID int64, at time.Time) bool {
	e.t.Helper()
	_, ok, err := e.gate.Claim(e.t.Context(), orderID, at)
	if err != nil {
		e.t.Fatalf("claim %d: %v", orderID, err)
	}
	return ok
}

func (e *env) reap(ids []int64) {
	e.t.Helper()
	for _, id := range ids {
		if r := must[stockgate.ReapResult](e.t)(e.gate.ReturnIfUnclaimed(e.t.Context(), id)); r != stockgate.ReapReturned {
			e.t.Fatalf("reap %d = %v, want ReapReturned", id, r)
		}
	}
}

func (e *env) heartbeat() (int64, bool) {
	e.t.Helper()
	v, err := e.rdb.Get(e.t.Context(), e.prefix+"gate:claim_at").Int64()
	if errors.Is(err, redis.Nil) {
		return 0, false
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return v, true
}

// stall 让落库在 start 还活着、之后停住：受理一批，时间推过 MaxWait 后被第二条拒绝，
// 回收任务把它们还掉。返回此刻的时间。
func (e *env) stall(start time.Time) time.Time {
	e.t.Helper()
	e.claim(1, start)
	queued := e.admit(start, 10, stallLimit)
	if len(queued) != 10 {
		e.t.Fatalf("live landing admitted %d, want 10", len(queued))
	}
	now := start.Add(stallLimit.MaxWait + time.Second)
	if res := e.holdAt(now, stallLimit); res.Outcome != stockgate.Backlogged {
		e.t.Fatalf("stale queue: %+v, want BACKLOGGED", res)
	}
	e.reap(queued)
	return now
}

// 防止：落库停住时，回收任务把排队超时的占用还掉后受理重新打开，新订单再排到超时、再被回收，循环下去。
// 心跳不会被回收抹掉：每个宽限期最多放进 MaxStale+1 个探测订单。
func TestHoldStalledLandingDoesNotCycle(t *testing.T) {
	e := newEnv(t)
	e.load(1, 1000, 0)
	now := e.stall(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))

	for cycle := range 5 {
		probes := e.admit(now, 50, stallLimit)
		if int64(len(probes)) != stallLimit.MaxStale+1 {
			t.Fatalf("cycle %d: admitted %d while landing is stalled, want %d", cycle, len(probes), stallLimit.MaxStale+1)
		}
		before := e.stock(1)
		res := e.holdAt(now, stallLimit)
		if res.Outcome != stockgate.Backlogged || res.Queued != stallLimit.MaxStale+1 {
			t.Fatalf("cycle %d: %+v, want BACKLOGGED with %d queued", cycle, res, stallLimit.MaxStale+1)
		}
		if after := e.stock(1); after != before {
			t.Fatalf("backlogged hold changed stock: %+v -> %+v", before, after)
		}
		now = now.Add(stallLimit.MaxWait + time.Second)
		e.reap(probes)
	}
	e.assertStock(1, 1000, 0)
}

// 落库恢复：一次认领刷新心跳后受理恢复正常。占用不存在的认领也算，
// 恢复后的消费者最先消费到的往往正是已被回收的订单的消息。
func TestHoldResumesAfterClaim(t *testing.T) {
	for name, claimLive := range map[string]bool{"hold exists": true, "hold reaped": false} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.load(1, 1000, 0)
			now := e.stall(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
			probes := e.admit(now, 50, stallLimit)
			if int64(len(probes)) != stallLimit.MaxStale+1 {
				t.Fatalf("stalled landing admitted %d, want %d", len(probes), stallLimit.MaxStale+1)
			}

			target := int64(424242) // 已被回收或从未存在
			if claimLive {
				target = probes[0]
			}
			now = now.Add(time.Second)
			if ok := e.claim(target, now); ok != claimLive {
				t.Fatalf("claim %d = %v, want %v", target, ok, claimLive)
			}
			if got := e.admit(now, 20, stallLimit); len(got) != 20 {
				t.Fatalf("after claim admitted %d, want 20", len(got))
			}
		})
	}
}

// 冷启动：没有心跳、队列为空，第一批照常受理，直到队列超过 MaxStale。
func TestHoldColdStart(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	e := newEnv(t)
	e.load(1, 1000, 0)
	if got := e.admit(now, 50, stallLimit); int64(len(got)) != stallLimit.MaxStale+1 {
		t.Fatalf("cold start admitted %d, want %d", len(got), stallLimit.MaxStale+1)
	}
	if _, ok := e.heartbeat(); ok {
		t.Fatal("Hold wrote the heartbeat")
	}

	// MaxStale 为 0 也不会因为没有心跳而永远关闭：放进一个探测订单。
	e2 := newEnv(t)
	e2.load(1, 1000, 0)
	if got := e2.admit(now, 50, stockgate.BacklogLimit{MaxWait: time.Second}); len(got) != 1 {
		t.Fatalf("cold start with MaxStale=0 admitted %d, want 1", len(got))
	}
}

// 心跳只往前走；Get 不刷新心跳；重复认领、认领不存在的占用都推进心跳。
func TestHeartbeat(t *testing.T) {
	e := newEnv(t)
	e.load(1, 1000, 0)
	t0 := time.UnixMilli(1_700_000_000_000)
	res := e.holdAt(t0, stockgate.BacklogLimit{})

	if _, ok, err := e.gate.Get(t.Context(), res.OrderID); err != nil || !ok {
		t.Fatalf("Get = %v, %v", ok, err)
	}
	if _, ok, err := e.gate.Get(t.Context(), 999); err != nil || ok {
		t.Fatalf("Get unknown = %v, %v", ok, err)
	}
	if _, ok := e.heartbeat(); ok {
		t.Fatal("Get wrote the heartbeat")
	}

	later := t0.Add(time.Minute)
	if !e.claim(res.OrderID, later) {
		t.Fatal("claim failed")
	}
	for _, at := range []time.Time{t0, later.Add(-time.Millisecond)} {
		e.claim(999, at)
		e.claim(res.OrderID, at)
		if v, ok := e.heartbeat(); !ok || v != later.UnixMilli() {
			t.Fatalf("heartbeat after an earlier claim = %d, %v; want %d", v, ok, later.UnixMilli())
		}
	}

	latest := later.Add(time.Second)
	e.claim(res.OrderID, latest) // 已认领过
	if v, _ := e.heartbeat(); v != latest.UnixMilli() {
		t.Fatalf("heartbeat after reclaim = %d, want %d", v, latest.UnixMilli())
	}
	e.claim(999, latest.Add(time.Second)) // 占用不存在
	if v, _ := e.heartbeat(); v != latest.Add(time.Second).UnixMilli() {
		t.Fatalf("heartbeat after claiming a missing hold = %d, want %d", v, latest.Add(time.Second).UnixMilli())
	}
	if _, _, err := e.gate.Get(t.Context(), res.OrderID); err != nil {
		t.Fatal(err)
	}
	if v, _ := e.heartbeat(); v != latest.Add(time.Second).UnixMilli() {
		t.Fatalf("Get moved the heartbeat to %d", v)
	}
}

// MaxWait 为 0 时不看心跳，只有队列长度上限生效。
func TestHoldNoMaxWaitIgnoresHeartbeat(t *testing.T) {
	e := newEnv(t)
	e.load(1, 1000, 0)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	if got := e.admit(now, 20, stockgate.BacklogLimit{MaxStale: 0}); len(got) != 20 {
		t.Fatalf("MaxWait=0 admitted %d, want 20", len(got))
	}
	if got := e.admit(now, 20, stockgate.BacklogLimit{MaxQueue: 25}); len(got) != 5 {
		t.Fatalf("MaxQueue=25 admitted %d more, want 5", len(got))
	}
}

// 并发受理和认领：心跳最终是最大的认领时间，库存恒等式成立，每个受理的订单恰好在一个队列里。
func TestHoldAndClaimConcurrent(t *testing.T) {
	e := newEnv(t)
	const total = 1000
	e.load(1, total, 0)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	first := e.nextID.Load() + 1

	var held, backlogged, claimed, maxBeat atomic.Int64
	parallel(600, func(i int) {
		if i%2 == 0 {
			switch res := e.holdAt(now, stallLimit); res.Outcome {
			case stockgate.Held:
				held.Add(1)
			case stockgate.Backlogged:
				backlogged.Add(1)
			default:
				t.Errorf("hold: %+v", res)
			}
			return
		}
		// 认领一个可能已受理、可能还没受理的订单号，时间乱序。
		at := now.Add(time.Duration(i%37) * time.Millisecond)
		_, ok, err := e.gate.Claim(t.Context(), first+int64(i/2), at)
		if err != nil {
			t.Errorf("claim: %v", err)
			return
		}
		if ok {
			claimed.Add(1)
		}
		for {
			cur := maxBeat.Load()
			if at.UnixMilli() <= cur || maxBeat.CompareAndSwap(cur, at.UnixMilli()) {
				break
			}
		}
	})

	if held.Load()+backlogged.Load() != 300 {
		t.Fatalf("held=%d backlogged=%d, want 300 in total", held.Load(), backlogged.Load())
	}
	e.assertStock(1, total-held.Load(), held.Load())
	queued := must[int64](t)(e.rdb.ZCard(t.Context(), e.prefix+"gate:holds").Result())
	inClaimed := must[int64](t)(e.rdb.ZCard(t.Context(), e.prefix+"gate:claimed").Result())
	if queued+inClaimed != held.Load() || inClaimed != claimed.Load() {
		t.Fatalf("holds=%d claimed=%d, want %d in total and %d claimed", queued, inClaimed, held.Load(), claimed.Load())
	}
	if v, ok := e.heartbeat(); !ok || v != maxBeat.Load() {
		t.Fatalf("heartbeat = %d, %v; want %d", v, ok, maxBeat.Load())
	}
}
