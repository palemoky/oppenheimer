package e2e

// 网关的限流（ADR-0019）。scripts/e2e.sh 以 RATE_LIMIT=on、RATE_LIMIT_IP=off 启动网关，按用户加接口的限额是默认值：
// createOrder 每秒 5 个、桶容量 10；getOrder 每秒 20 个、桶容量 40。
// 按 IP 的限额关掉了（所有用例都从 127.0.0.1 来），所以这里测的只有按用户的那一维。
//
// 用例不并行：时间敏感的断言留出了余量，但独占系统让它们更稳。

import (
	"encoding/json/v2"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	// createOrder 的默认限额：桶容量和每秒补充的令牌数。
	rlBurst        = 10
	rlRefillPerSec = 5
	rlRequests     = 30
)

// rlUser 返回一个随机的大用户号，不与别的用例的用户（从时间戳派生）和归档用例（[1<<40, 2<<40)）的用户相撞。
func rlUser() int64 { return 1<<41 + rand.Int64N(1<<40) }

// rlResult 是一次下单请求的结果。
type rlResult struct {
	key        string
	status     int
	code       string // 错误码，429 和别的非 2xx 都有
	retryAfter string // 429 的 Retry-After 头原文
	orderID    string // 202 的订单号
	body       string // 非 202 时留着，失败信息里用
	err        error
}

// rlCreate 以指定的幂等键下一单。不调用 t.Fatal，可在 goroutine 里用。
func rlCreate(t testing.TB, user, sku int64, key string) rlResult {
	res := rlResult{key: key}
	r, err := call(t.Context(), http.MethodPost, "/api/v1/orders", user, key, createOrderBody(sku, 1))
	if err != nil {
		res.err = fmt.Errorf("create order (key %s): %w", key, err)
		return res
	}
	res.status = r.status
	if r.status == http.StatusAccepted {
		var o order
		if err := json.Unmarshal(r.body, &o); err != nil {
			res.err = fmt.Errorf("decode accepted order (key %s): %w: %s", key, err, r.body)
			return res
		}
		res.orderID = o.OrderID
		return res
	}
	res.code = r.errorCode()
	res.retryAfter = r.header.Get("Retry-After")
	res.body = string(r.body)
	return res
}

// retryAfterSeconds 解析 Retry-After：必须是不小于 1 的整数秒。
func retryAfterSeconds(v string) (int, error) {
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("Retry-After %q is not an integer: %w", v, err)
	}
	if n < 1 {
		return 0, fmt.Errorf("Retry-After = %d, want >= 1", n)
	}
	return n, nil
}

// listOrderIDs 返回用户的订单号（一页，limit=100）。遇到 429 或非 200 返回 ok=false，让调用方继续轮询。
func listOrderIDs(t testing.TB, user int64) (ids []string, ok bool) {
	t.Helper()
	r := mustCall(t, http.MethodGet, "/api/v1/orders?limit=100", user, "", nil)
	if r.status != http.StatusOK {
		return nil, false
	}
	var page orderSummaryPage
	r.decode(t, &page)
	if page.NextCursor != "" {
		t.Fatalf("list orders: more than 100 orders for a user that placed about %d", rlRequests)
	}
	for _, o := range page.Orders {
		ids = append(ids, o.OrderID)
	}
	return ids, true
}

// TestRateLimitCreateOrder 验证按用户加接口的限流：超出的请求得到 429 RATE_LIMITED 和 Retry-After，
// 用户之间、接口之间互不影响，等 Retry-After 之后恢复，而且被限流的请求没有任何效果。
func TestRateLimitCreateOrder(t *testing.T) {
	user, other := rlUser(), rlUser()
	sku := newSKU(t, 1000)

	// ---------- 1. 一个用户尽快连发 30 个下单请求，每个用不同的幂等键 ----------
	results := make([]rlResult, rlRequests)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range results {
		key := newKey()
		wg.Go(func() {
			<-start
			results[i] = rlCreate(t, user, sku, key)
		})
	}
	began := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(began)

	var accepted, limited []rlResult
	maxRetryAfter := 0
	for _, res := range results {
		switch {
		case res.err != nil:
			t.Errorf("%v", res.err)
		case res.status == http.StatusAccepted:
			accepted = append(accepted, res)
		case res.status == http.StatusTooManyRequests:
			if res.code != "RATE_LIMITED" {
				t.Errorf("429 with code %q, want RATE_LIMITED (BACKLOGGED is backpressure, not the rate limit): %s", res.code, res.body)
			}
			secs, err := retryAfterSeconds(res.retryAfter)
			if err != nil {
				t.Errorf("429 response: %v", err)
			}
			maxRetryAfter = max(maxRetryAfter, secs)
			limited = append(limited, res)
		default:
			t.Errorf("unexpected response: status %d code %q: %s", res.status, res.code, res.body)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	// 受理数不少于桶容量；不多于桶容量加这段时间里补充的令牌，再留 2 个的余量（令牌取整、时钟抖动）。
	// 时间从发出第一个请求算到最后一个返回，CI 上慢也只会放宽上限。
	upper := rlBurst + int(elapsed.Seconds()*rlRefillPerSec) + 2
	t.Logf("%d requests in %v: accepted %d (allowed %d..%d), rate limited %d, max Retry-After %ds",
		rlRequests, elapsed, len(accepted), rlBurst, upper, len(limited), maxRetryAfter)
	if len(accepted) < rlBurst || len(accepted) > upper {
		t.Fatalf("accepted %d of %d requests, want between %d and %d", len(accepted), rlRequests, rlBurst, upper)
	}
	if len(limited) == 0 {
		t.Fatalf("no request was rate limited (%d accepted in %v); the machine is too slow for this test or the limit is off", len(accepted), elapsed)
	}

	// ---------- 3. 互不影响：此时第一个用户已经被限流 ----------
	// 另一个用户下单不受影响。
	if res := rlCreate(t, other, sku, newKey()); res.err != nil || res.status != http.StatusAccepted {
		t.Fatalf("another user's order: status %d code %q err %v: %s, want 202", res.status, res.code, res.err, res.body)
	}
	// 同一个用户查询自己的订单是另一个接口，有自己的桶，不受下单被限流的影响（getOrder 在非 200 时 t.Fatal）。
	getOrder(t, user, accepted[0].orderID)

	// ---------- 2. 等 Retry-After 之后恢复 ----------
	// 最后一个 429 给出的 Retry-After 是补回一个令牌需要的时间；多等半秒的余量，到时桶里有好几个令牌。
	time.Sleep(time.Duration(maxRetryAfter)*time.Second + 500*time.Millisecond)
	recovered := rlCreate(t, user, sku, newKey())
	if recovered.err != nil || recovered.status != http.StatusAccepted {
		t.Fatalf("order after Retry-After: status %d code %q err %v: %s, want 202", recovered.status, recovered.code, recovered.err, recovered.body)
	}

	// known 是该用户所有被受理的订单号。
	known := make(map[string]bool)
	for _, res := range accepted {
		known[res.orderID] = true
	}
	known[recovered.orderID] = true

	// ---------- 4. 被限流的请求没有任何效果 ----------
	// 先等已受理的订单都落库，取一个快照：里面不能有任何不是受理响应里给出的订单。
	// 被限流的请求若走到了下单逻辑，它的订单（订单号没人拿到）此时早已落库。
	var before []string
	eventually(t, 15*time.Second, "accepted orders to show up in the user's list", func() bool {
		ids, ok := listOrderIDs(t, user)
		before = ids
		return ok && len(ids) >= len(known)
	})
	for _, id := range before {
		if !known[id] {
			t.Fatalf("order %s exists but was never returned by an accepted request: a rate-limited request reached the order logic", id)
		}
	}

	// 用被限流时用过的幂等键再提交：必须是新受理的订单（202），订单号不在快照里。
	// 如果它得到的是已经存在的订单（幂等重放），说明上一次被限流的请求其实走到了下单逻辑。
	// 令牌够 3 次，仍然遇到 429 就按 Retry-After 再等一轮。
	for _, res := range limited[:min(3, len(limited))] {
		var again rlResult
		for range 5 {
			again = rlCreate(t, user, sku, res.key)
			if again.err != nil || again.status != http.StatusTooManyRequests {
				break
			}
			secs, err := retryAfterSeconds(again.retryAfter)
			if err != nil {
				t.Fatalf("429 response: %v", err)
			}
			time.Sleep(time.Duration(secs)*time.Second + 200*time.Millisecond)
		}
		if again.err != nil {
			t.Fatal(again.err)
		}
		if again.status != http.StatusAccepted {
			t.Fatalf("resubmit with the key %s that was rate limited: status %d code %q: %s, want 202 (not a key conflict)",
				res.key, again.status, again.code, again.body)
		}
		if slices.Contains(before, again.orderID) {
			t.Fatalf("resubmit with the key %s returned the existing order %s: the rate-limited request had an effect", res.key, again.orderID)
		}
		known[again.orderID] = true
	}

	// 最终该用户的订单数恰好是受理（202）的个数：被限流的请求一个订单都没有产生。
	var final []string
	eventually(t, 15*time.Second, "all accepted orders to show up in the user's list", func() bool {
		ids, ok := listOrderIDs(t, user)
		final = ids
		return ok && len(ids) >= len(known)
	})
	if len(final) != len(known) {
		t.Errorf("user has %d orders, want exactly %d (the accepted ones)", len(final), len(known))
	}
	for _, id := range final {
		if !known[id] {
			t.Errorf("order %s exists but was never returned by an accepted request", id)
		}
	}
}
