package httpapi_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/ratelimit"
	"github.com/palemoky/oppenheimer/pkg/testredis"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi/oas"
)

// slow 是一个几乎不补充的速率：测试里时钟不动，桶用完就不会自己恢复。
const slow = 0.001

// fakeClock 是可以手动拨动的时钟，给 ratelimit.Local 用。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newLocal(clock *fakeClock) *ratelimit.Local {
	return ratelimit.NewLocal(ratelimit.LocalConfig{Now: clock.Now})
}

// newRateLimitedEnv 和 newEnv 一样，只是带上限流的配置。假的下游都会成功，并记调用次数（e.order.calls、*stockCalls）。
func newRateLimitedEnv(t *testing.T, rl *httpapi.RateLimit) (e *env, stockCalls *atomic.Int64) {
	t.Helper()
	e = newEnv(t)
	stockCalls = &atomic.Int64{}
	e.order.get = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
		return &orderv1.GetOrderResponse{Order: sampleOrder()}, nil
	}
	e.order.create = func(*orderv1.SubmitOrderRequest) (*orderv1.SubmitOrderResponse, error) {
		return &orderv1.SubmitOrderResponse{Order: sampleOrder()}, nil
	}
	e.inventory.getStock = func(context.Context, *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
		stockCalls.Add(1)
		return &inventoryv1.GetStockResponse{SkuId: 1, Available: 3}, nil
	}
	e.h = e.newHandler(rl)
	return e, stockCalls
}

// newHandler 用同一组假下游建一个新的 handler。用来在测试中途换一份限流配置，而限流器（桶）保持不变。
func (e *env) newHandler(rl *httpapi.RateLimit) http.Handler {
	return httpapi.New(httpapi.Deps{Order: e.order, Payment: e.payment, Inventory: e.inventory, OrderIndex: e.orderIndex}, httpapi.Config{
		Logger:      slog.New(slog.NewJSONHandler(e.logs, nil)),
		CallTimeout: time.Second,
		RateLimit:   rl,
	})
}

// send 发一个请求，可以指定远端地址；user 为空表示不带 X-User-Id。响应解析方式和 do 相同。
func send(t *testing.T, h http.Handler, method, path, user, remote string, hdr ...string) result {
	t.Helper()
	var body io.Reader
	if method == http.MethodPost {
		body = strings.NewReader(`{"items":[{"sku_id":"42","quantity":1}]}`)
	}
	req := httptest.NewRequestWithContext(t.Context(), method, path, body)
	if remote != "" {
		req.RemoteAddr = remote
	}
	if user != "" {
		req.Header.Set("X-User-Id", user)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	res := result{status: rec.Code, header: rec.Header(), raw: rec.Body.String()}
	if err := json.Unmarshal(rec.Body.Bytes(), &res.body); err != nil {
		t.Fatalf("response is not a JSON object: %v: %s", err, res.raw)
	}
	return res
}

func getOrder(t *testing.T, h http.Handler, user string) result {
	t.Helper()
	return send(t, h, http.MethodGet, "/api/v1/orders/1", user, "")
}

// wantRateLimited 检查 429 RATE_LIMITED 的响应：维度、Retry-After，并且响应体严格符合规范的 Error 结构。
func wantRateLimited(t *testing.T, r result, dim string, retryAfter int) {
	t.Helper()
	if r.status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body %s", r.status, r.raw)
	}
	// 按规范的类型严格解析：多出或缺少字段都失败。
	var e oas.TooManyRequests
	if err := json.Unmarshal([]byte(r.raw), &e, json.RejectUnknownMembers(true)); err != nil {
		t.Fatalf("429 body does not match the spec: %v: %s", err, r.raw)
	}
	if e.Error.Code != "RATE_LIMITED" {
		t.Errorf("code = %q, want RATE_LIMITED", e.Error.Code)
	}
	if want := "rate limit exceeded: " + dim; e.Error.Message != want {
		t.Errorf("message = %q, want %q", e.Error.Message, want)
	}
	got, err := strconv.Atoi(r.header.Get("Retry-After"))
	if err != nil || got < 1 {
		t.Fatalf("Retry-After = %q, want a positive integer", r.header.Get("Retry-After"))
	}
	if retryAfter > 0 && got != retryAfter {
		t.Errorf("Retry-After = %d, want %d", got, retryAfter)
	}
}

func wantStatus(t *testing.T, r result, status int) {
	t.Helper()
	if r.status != status {
		t.Fatalf("status = %d, want %d; body %s", r.status, status, r.raw)
	}
}

func TestRateLimitPerUser(t *testing.T) {
	clock := newFakeClock()
	// 每秒 0.5 个、桶 2：用完之后 2 秒才有下一个令牌，Retry-After 应当是 2。
	limit := ratelimit.Limit{Rate: 0.5, Burst: 2}
	e, stockCalls := newRateLimitedEnv(t, &httpapi.RateLimit{
		Shared: newLocal(clock),
		User:   map[string]ratelimit.Limit{"getOrder": limit, "getStock": limit},
	})

	for range 2 {
		wantStatus(t, getOrder(t, e.h, "7"), http.StatusOK)
	}
	wantRateLimited(t, getOrder(t, e.h, "7"), "user", 2)
	if e.order.calls != 2 {
		t.Errorf("downstream calls = %d, want 2 (the limited request must not reach it)", e.order.calls)
	}

	// 另一个用户、同一个用户的另一个接口都不受影响。
	wantStatus(t, getOrder(t, e.h, "8"), http.StatusOK)
	wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/skus/1/stock", "7", ""), http.StatusOK)
	if stockCalls.Load() != 1 {
		t.Errorf("stock calls = %d", stockCalls.Load())
	}

	// 过了 Retry-After 之后恢复一个令牌。
	clock.Advance(2 * time.Second)
	wantStatus(t, getOrder(t, e.h, "7"), http.StatusOK)
	wantRateLimited(t, getOrder(t, e.h, "7"), "user", 2)
}

func TestRateLimitedCreateOrderDoesNotCallDownstream(t *testing.T) {
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		Shared: newLocal(newFakeClock()),
		User:   map[string]ratelimit.Limit{"createOrder": {Rate: slow, Burst: 1}},
	})
	create := func() result {
		return send(t, e.h, http.MethodPost, "/api/v1/orders", "7", "", "Idempotency-Key", "k1")
	}
	wantStatus(t, create(), http.StatusAccepted)
	for range 3 {
		wantRateLimited(t, create(), "user", 0)
	}
	if e.order.calls != 1 {
		t.Errorf("downstream calls = %d, want 1", e.order.calls)
	}
}

func TestRateLimitPerRoute(t *testing.T) {
	shared := newLocal(newFakeClock())
	user := map[string]ratelimit.Limit{"getOrder": {Rate: slow, Burst: 3}}
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		Shared: shared,
		User:   user,
		Route:  map[string]ratelimit.Limit{"getOrder": {Rate: slow, Burst: 2}},
	})

	// 两个用户合起来用完了接口的总量。
	wantStatus(t, getOrder(t, e.h, "8"), http.StatusOK)
	wantStatus(t, getOrder(t, e.h, "9"), http.StatusOK)
	for range 5 {
		wantRateLimited(t, getOrder(t, e.h, "7"), "route", 0)
	}
	if e.order.calls != 2 {
		t.Errorf("downstream calls = %d, want 2", e.order.calls)
	}

	// 把接口的限制放开（同一个限流器，也就是同一组桶）：用户 7 的桶仍然是满的，
	// 说明被接口拒绝的那 5 次没有扣他的令牌。
	h := e.newHandler(&httpapi.RateLimit{Shared: shared, User: user})
	for range 3 {
		wantStatus(t, getOrder(t, h, "7"), http.StatusOK)
	}
	wantRateLimited(t, getOrder(t, h, "7"), "user", 0)
}

func TestRateLimitPerIP(t *testing.T) {
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		IP:      newLocal(newFakeClock()),
		IPLimit: ratelimit.Limit{Rate: slow, Burst: 2},
	})
	const a, b = "198.51.100.1:1000", "198.51.100.2:1000"

	// 没超限时，缺少用户的请求仍然得到原来的 401（它也消耗了地址的一个令牌）。
	r := send(t, e.h, http.MethodGet, "/api/v1/orders/1", "", a)
	wantStatus(t, r, http.StatusUnauthorized)
	if errCode(t, r) != "UNAUTHENTICATED" {
		t.Errorf("code = %s", errCode(t, r))
	}
	wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", a), http.StatusOK)
	// 同一个地址超限：带不带用户都是 429 ip，按 IP 在识别用户之前。
	wantRateLimited(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", a), "ip", 0)
	wantRateLimited(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "", a), "ip", 0)
	// 同一个地址换一个接口、换一个用户也一样：按 IP 是所有接口共用的一个桶。
	wantRateLimited(t, send(t, e.h, http.MethodGet, "/api/v1/skus/1/stock", "8", a), "ip", 0)
	// 不同的地址不受影响。
	wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", b), http.StatusOK)
	// 规范之外的路由（404）不经过限流。
	r = send(t, e.h, http.MethodGet, "/nope", "7", a)
	wantStatus(t, r, http.StatusNotFound)

	if e.order.calls != 2 {
		t.Errorf("downstream calls = %d, want 2", e.order.calls)
	}
}

func TestRateLimitIPIgnoresForwardedForFromUntrustedPeer(t *testing.T) {
	trusted, err := httpapi.ParseTrustedProxies("10.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		IP:             newLocal(newFakeClock()),
		IPLimit:        ratelimit.Limit{Rate: slow, Burst: 1},
		TrustedProxies: trusted,
	})
	// 不是代理的客户端每次写一个不同的 X-Forwarded-For，也换不到新的桶。
	wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "203.0.113.9:1", "X-Forwarded-For", "1.1.1.1"), http.StatusOK)
	wantRateLimited(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "203.0.113.9:1", "X-Forwarded-For", "2.2.2.2"), "ip", 0)
	// 经过受信任的代理来的两个不同客户端，各有各的桶。
	wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "10.0.0.1:1", "X-Forwarded-For", "198.51.100.1"), http.StatusOK)
	wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "10.0.0.1:1", "X-Forwarded-For", "198.51.100.2"), http.StatusOK)
	wantRateLimited(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "10.0.0.1:1", "X-Forwarded-For", "198.51.100.1"), "ip", 0)
}

// countingLimiter 记录被调用的次数和传进来的请求。
type countingLimiter struct {
	ratelimit.Limiter
	mu    sync.Mutex
	calls int
	last  []ratelimit.Request
}

func (c *countingLimiter) Allow(ctx context.Context, reqs ...ratelimit.Request) (ratelimit.Decision, error) {
	c.mu.Lock()
	c.calls++
	c.last = append([]ratelimit.Request(nil), reqs...)
	c.mu.Unlock()
	return c.Limiter.Allow(ctx, reqs...)
}

func TestRateLimitIPIsCheckedBeforeUser(t *testing.T) {
	shared := &countingLimiter{Limiter: newLocal(newFakeClock())}
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		IP:      newLocal(newFakeClock()),
		IPLimit: ratelimit.Limit{Rate: slow, Burst: 1},
		Shared:  shared,
		User:    map[string]ratelimit.Limit{"getOrder": {Rate: slow, Burst: 1}},
	})
	const a = "198.51.100.1:1000"
	wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", a), http.StatusOK)
	// 第二个请求同时超过了 IP 和用户的限额：报的是 ip，而且用户的桶根本没有被问到（没有被扣）。
	wantRateLimited(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", a), "ip", 0)
	if shared.calls != 1 {
		t.Errorf("per-user limiter called %d times, want 1", shared.calls)
	}
	// 换一个地址，用户的桶在第一个请求时已经用完，这次报 user。
	wantRateLimited(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "198.51.100.2:1"), "user", 0)
}

// TestRateLimitKeysUseOperationID 对规范里的每个接口发一个请求，检查中间件认出了它的 operationId，
// 并且用户的桶在前、接口的桶在后。接口的清单来自 OperationIDs（TestOperationsMatchSpec 保证它和规范一致）。
func TestRateLimitKeysUseOperationID(t *testing.T) {
	deny := &countingLimiter{Limiter: denyAll{}}
	all := map[string]ratelimit.Limit{}
	for _, id := range httpapi.OperationIDs() {
		all[id] = ratelimit.Limit{Rate: 1, Burst: 1}
	}
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{Shared: deny, User: all, Route: all})

	paths := map[string][2]string{
		"listOrders":         {http.MethodGet, "/api/v1/orders"},
		"createOrder":        {http.MethodPost, "/api/v1/orders"},
		"getOrder":           {http.MethodGet, "/api/v1/orders/1"},
		"cancelOrder":        {http.MethodPost, "/api/v1/orders/1/cancel"},
		"payOrder":           {http.MethodPost, "/api/v1/orders/1/pay"},
		"getPayment":         {http.MethodGet, "/api/v1/orders/1/payment"},
		"getStock":           {http.MethodGet, "/api/v1/skus/1/stock"},
		"listMerchantOrders": {http.MethodGet, "/api/v1/merchants/1/orders"},
	}
	for _, id := range httpapi.OperationIDs() {
		p, ok := paths[id]
		if !ok {
			t.Errorf("no sample request for operation %s; add one here", id)
			continue
		}
		wantRateLimited(t, send(t, e.h, p[0], p[1], "7", ""), "user", 0)
		got := deny.last
		if len(got) != 2 || got[0].Key != "u:7:"+id || got[1].Key != "r:"+id {
			t.Errorf("%s: limiter got %+v, want keys u:7:%s and r:%s", id, got, id, id)
		}
	}
}

// denyAll 总是拒绝第一个桶。
type denyAll struct{}

func (denyAll) Allow(context.Context, ...ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{Denied: 0, RetryAfter: 1500 * time.Millisecond}, nil
}

// failingLimiter 总是返回错误。
type failingLimiter struct{ err error }

func (f failingLimiter) Allow(context.Context, ...ratelimit.Request) (ratelimit.Decision, error) {
	return ratelimit.Decision{}, f.err
}

func TestRateLimiterErrorLetsRequestThrough(t *testing.T) {
	broken := failingLimiter{err: errors.New("boom")}
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		IP:      broken,
		IPLimit: ratelimit.Limit{Rate: 1, Burst: 1},
		Shared:  broken,
		User:    map[string]ratelimit.Limit{"getOrder": {Rate: 1, Burst: 1}},
	})
	for range 3 {
		wantStatus(t, getOrder(t, e.h, "7"), http.StatusOK)
	}
	if e.order.calls != 3 {
		t.Errorf("downstream calls = %d, want 3", e.order.calls)
	}
	if !strings.Contains(e.logs.String(), "rate limiter failed") || !strings.Contains(e.logs.String(), `"level":"WARN"`) {
		t.Errorf("expected a warning in logs: %s", e.logs.String())
	}
}

func TestRateLimiterCancelledRequestIsNotLetThrough(t *testing.T) {
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		Shared: failingLimiter{err: context.Canceled},
		User:   map[string]ratelimit.Limit{"getOrder": {Rate: 1, Burst: 1}},
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/orders/1", nil)
	req.Header.Set("X-User-Id", "7")
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	// 客户端已经走了：499，既不放行，也不当成被限流。
	if rec.Code != 499 {
		t.Errorf("status = %d for a cancelled request, want 499", rec.Code)
	}
	if e.order.calls != 0 {
		t.Errorf("downstream calls = %d, want 0", e.order.calls)
	}
}

func TestNoRateLimitByDefault(t *testing.T) {
	// Config 里不给限流器就不限：现有测试的环境都是这样。
	e, _ := newRateLimitedEnv(t, nil)
	for range 200 {
		wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "198.51.100.1:1"), http.StatusOK)
	}
	// 给了配置但所有维度都不限，也不限。
	e, _ = newRateLimitedEnv(t, &httpapi.RateLimit{
		IP:     newLocal(newFakeClock()),
		Shared: newLocal(newFakeClock()),
		User:   map[string]ratelimit.Limit{},
	})
	for range 200 {
		wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "198.51.100.1:1"), http.StatusOK)
	}
}

// unreachableRedis 返回一个连不上的 Redis 客户端（端口上没有服务，连接立刻被拒绝）。
func unreachableRedis(t *testing.T) *redis.Client {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	rdb := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true, MaxRetries: -1, DialerRetries: 1})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestRateLimitRedisUnavailableFallsBackToLocal(t *testing.T) {
	const timeout = 50 * time.Millisecond
	fb := ratelimit.NewFallback(ratelimit.FallbackConfig{
		Primary:   ratelimit.NewRedis(unreachableRedis(t), "t:"),
		Secondary: newLocal(newFakeClock()),
		Timeout:   timeout,
		Logger:    slog.New(slog.DiscardHandler),
	})
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		Shared: fb,
		User:   map[string]ratelimit.Limit{"getOrder": {Rate: slow, Burst: 2}},
	})

	timed := func() result {
		start := time.Now()
		r := getOrder(t, e.h, "7")
		if d := time.Since(start); d > timeout+200*time.Millisecond {
			t.Errorf("request took %v with Redis down, want about the Redis timeout (%v) at most", d, timeout)
		}
		return r
	}
	// 请求照常被处理，限额仍然生效（由进程内的限流器）。
	wantStatus(t, timed(), http.StatusOK)
	wantStatus(t, timed(), http.StatusOK)
	wantRateLimited(t, timed(), "user", 0)
	if got := fb.Fallbacks(); got != 3 {
		t.Errorf("fallbacks = %d, want 3", got)
	}
}

func TestRateLimitWithRedis(t *testing.T) {
	rdb, prefix := testredis.New(t)
	fb := ratelimit.NewFallback(ratelimit.FallbackConfig{
		Primary:   ratelimit.NewRedis(rdb, prefix),
		Secondary: newLocal(newFakeClock()),
		// 测试机上 Redis 偶尔慢一点不应当让用例退回进程内而失败。
		Timeout: time.Second,
		Logger:  slog.New(slog.DiscardHandler),
	})
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		Shared: fb,
		User:   map[string]ratelimit.Limit{"getOrder": {Rate: slow, Burst: 2}},
		Route:  map[string]ratelimit.Limit{"getOrder": {Rate: slow, Burst: 3}},
	})
	wantStatus(t, getOrder(t, e.h, "7"), http.StatusOK)
	wantStatus(t, getOrder(t, e.h, "7"), http.StatusOK)
	wantRateLimited(t, getOrder(t, e.h, "7"), "user", 0)
	wantStatus(t, getOrder(t, e.h, "8"), http.StatusOK)
	wantRateLimited(t, getOrder(t, e.h, "9"), "route", 0)
	if got := fb.Fallbacks(); got != 0 {
		t.Errorf("fallbacks = %d, want 0 (Redis is up)", got)
	}
	// 状态确实在 Redis 里，键是设计里写的形状。
	for _, key := range []string{prefix + "rl:u:7:getOrder", prefix + "rl:r:getOrder"} {
		if n, err := rdb.Exists(t.Context(), key).Result(); err != nil || n != 1 {
			t.Errorf("key %s exists = %d, %v", key, n, err)
		}
	}
}

func TestRateLimitReporter(t *testing.T) {
	stats := &httpapi.RateLimitStats{}
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		IP:      newLocal(newFakeClock()),
		IPLimit: ratelimit.Limit{Rate: slow, Burst: 1},
		Shared:  newLocal(newFakeClock()),
		User:    map[string]ratelimit.Limit{"getOrder": {Rate: slow, Burst: 1}},
		Stats:   stats,
	})
	wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "198.51.100.1:1"), http.StatusOK)
	for range 3 {
		wantRateLimited(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "198.51.100.1:1"), "ip", 0)
	}
	wantRateLimited(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "198.51.100.2:1"), "user", 0)

	var fallbacks atomic.Int64
	fallbacks.Store(10) // 创建之前的退回不算
	var buf strings.Builder
	var mu sync.Mutex
	logger := slog.New(slog.NewJSONHandler(&lockedWriter{w: &buf, mu: &mu}, nil))
	rep := httpapi.NewRateLimitReporter(stats, fallbacks.Load, logger, time.Hour)
	fallbacks.Add(4)

	rep.Flush(t.Context())
	mu.Lock()
	out := buf.String()
	buf.Reset()
	mu.Unlock()
	var line struct {
		Msg         string           `json:"msg"`
		DeniedTotal int64            `json:"denied_total"`
		Denied      map[string]int64 `json:"denied"`
		Fallbacks   int64            `json:"fallbacks"`
	}
	if err := json.Unmarshal([]byte(out), &line); err != nil {
		t.Fatalf("summary is not one JSON line: %v: %q", err, out)
	}
	if line.Msg != "rate limit summary" || line.DeniedTotal != 4 || line.Fallbacks != 4 ||
		line.Denied["ip.getOrder"] != 3 || line.Denied["user.getOrder"] != 1 || len(line.Denied) != 2 {
		t.Errorf("unexpected summary: %s", out)
	}

	// 计数已经清零，没有新的事情时不输出。
	rep.Flush(t.Context())
	mu.Lock()
	if buf.Len() != 0 {
		t.Errorf("empty period logged: %s", buf.String())
	}
	mu.Unlock()

	// Start 随 Stop 返回，返回前把最后一段输出掉。
	done := make(chan error, 1)
	go func() { done <- rep.Start(t.Context()) }()
	wantRateLimited(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "198.51.100.1:1"), "ip", 0)
	if err := rep.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reporter did not stop")
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(buf.String(), `"ip.getOrder":1`) {
		t.Errorf("final flush missing: %s", buf.String())
	}
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
