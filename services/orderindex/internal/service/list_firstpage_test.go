//go:build faults

package service

import (
	"context"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	orderindexv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/orderindex/v1"
	"github.com/palemoky/oppenheimer/pkg/fault"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

// 这个文件只在带 faults 标签的构建里编译。它钉住的是开关 f05 的行为（答案在 chaos/scenarios/ 里，这里不写）。
//
// 两个测试都不并行：开关和 firstPages 是包级状态。顶层的非并行测试全部跑完之后，
// service_test.go 里那些 t.Parallel 的测试才开始，那时开关已经被 Cleanup 关掉了。

// 开关关着时和平时的构建一样（每次都查库）；开着时没有并发交错的顺序请求照常返回。
func TestFirstPageSwitch(t *testing.T) {
	svc, st := newService(t)
	const merchant = 9101
	first := func() []int64 {
		t.Helper()
		resp, err := svc.ListMerchantOrders(t.Context(), &orderindexv1.ListMerchantOrdersRequest{MerchantId: merchant})
		if err != nil {
			t.Fatal(err)
		}
		var ids []int64
		for _, o := range resp.GetOrders() {
			ids = append(ids, o.GetOrderId())
		}
		return ids
	}

	put(t, st, merchant, 1, orderv1.OrderStatus_ORDER_STATUS_PAID)
	if got := first(); !slices.Equal(got, []int64{1}) {
		t.Fatalf("switch off: got %v, want [1]", got)
	}
	put(t, st, merchant, 2, orderv1.OrderStatus_ORDER_STATUS_PAID)
	if got := first(); !slices.Equal(got, []int64{2, 1}) {
		t.Fatalf("switch off: a new row must show up at once, got %v, want [2 1]", got)
	}

	if err := fault.Set("f05", true, time.Minute); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fault.Set("f05", false, 0) })

	// 顺序的请求不会交错，不会挂住（挂住的话由 go test 的总超时报出来）。
	for range 50 {
		if got := first(); !slices.Equal(got, []int64{2, 1}) {
			t.Fatalf("switch on: got %v, want [2 1]", got)
		}
	}
	// 带游标的翻页不走缓存。
	resp, err := svc.ListMerchantOrders(t.Context(), &orderindexv1.ListMerchantOrdersRequest{MerchantId: merchant, BeforeOrderId: 2})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(resp.GetOrders()); n != 1 {
		t.Fatalf("switch on, second page: %d orders, want 1", n)
	}
}

// 开着时，一个商家正在查库、同一个商家的另一个请求进来等它，两边互相等住，之后所有请求都挂住。
//
// 用的是测试自己的 pageCache 和不动的时钟，不碰包级的 firstPages：卡住的 goroutine 留在这个缓存上
// （sync.Mutex 等不到就永远等，没法从外面解开），测试结束时它们还在，但只有这几个，不影响别的测试。
// 先后顺序靠 goroutine 的调用栈确认（某个 goroutine 停在某个函数里的 Mutex.Lock），不靠 sleep 赌时序。
func TestFirstPageInterleaving(t *testing.T) {
	// 时钟不动：这里的交错不靠过期。
	at := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return at }
	c := newPageCache(time.Second, 1000, now)

	loading := make(chan struct{})
	release := make(chan struct{})
	slowLoad := func(context.Context, store.ListQuery) ([]store.Row, error) {
		close(loading)
		<-release
		return []store.Row{{MerchantID: 1, OrderID: 1}}, nil
	}
	fastLoad := func(_ context.Context, q store.ListQuery) ([]store.Row, error) {
		return []store.Row{{MerchantID: q.MerchantID, OrderID: 2}}, nil
	}
	get := func(merchant int64, load func(context.Context, store.ListQuery) ([]store.Row, error)) <-chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = c.get(context.Background(), store.ListQuery{MerchantID: merchant, Limit: 21}, load)
		}()
		return done
	}

	// 没有交错时一切正常：查两次，第二次用缓存。
	for range 2 {
		select {
		case <-get(2, fastLoad):
		case <-time.After(5 * time.Second):
			t.Fatal("a lone request did not finish")
		}
	}

	// 1. 商家 1 的第一页正在查库（持有商家 1 的锁）。
	a := get(1, slowLoad)
	<-loading
	// 2. 商家 1 的另一个请求进来，等商家 1 的锁（持有整个缓存的锁）。
	b := get(1, fastLoad)
	waitForStack(t, "(*pageCache).lockMerchant")
	// 3. 商家 1 查完了，要记进缓存，等整个缓存的锁。
	close(release)
	waitForStack(t, "(*pageCache).keep")

	// 两边都不会再返回。
	select {
	case <-a:
		t.Fatal("the loading request returned; want it stuck")
	case <-b:
		t.Fatal("the waiting request returned; want it stuck")
	case <-time.After(500 * time.Millisecond):
	}
	// 之后来的任何请求，哪个商家都一样，也挂住（停在 lockMerchant 里等整个缓存的锁）。
	select {
	case <-get(3, fastLoad):
		t.Fatal("a later request returned; want it stuck")
	case <-time.After(300 * time.Millisecond):
	}
}

// waitForStack 等到有一个 goroutine 停在 fn 里的 sync.Mutex.Lock 上，最多等 5 秒。
func waitForStack(t *testing.T, fn string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		for g := range strings.SplitSeq(string(buf[:n]), "\n\n") {
			if strings.Contains(g, "sync.(*Mutex).Lock") && strings.Contains(g, fn) &&
				strings.Index(g, "sync.(*Mutex).Lock") < strings.Index(g, fn) {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no goroutine is blocked on a mutex in %s", fn)
}
