package probe

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/palemoky/oppenheimer/test/load/internal/gwapi"
)

// metricReader 读本包测试进程里的全部指标。全局的 MeterProvider 是进程级状态，只在 TestMain 里设一次；
// 用例都不并行，断言只看一个用例前后的差值。
var metricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	otel.SetMeterProvider(mp)
	code := m.Run()
	_ = mp.Shutdown(context.Background())
	os.Exit(code)
}

// metricValue 返回名为 name 的计数里标签匹配 want 的数据点之和；直方图返回记录次数之和。
func metricValue(t *testing.T, name string, want map[string]string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	match := func(set attribute.Set) bool {
		for k, v := range want {
			got, ok := set.Value(attribute.Key(k))
			if !ok || got.AsString() != v {
				return false
			}
		}
		return true
	}
	var v int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						v += dp.Value
					}
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						v += int64(dp.Count) //nolint:gosec // 测试里的计数很小
					}
				}
			default:
				t.Fatalf("metric %s has unexpected data type %T", name, m.Data)
			}
		}
	}
	return v
}

// snapshot 记下一组指标的当前值，delta 返回之后的增量。
type snapshot struct {
	t      *testing.T
	before map[string]int64
}

// series 是一条时间序列：指标名加标签。step 或 outcome 为空表示没有这个标签。
type series struct {
	name, step, outcome string
}

func (s series) key() string { return fmt.Sprint(s.name, "/", s.step, "/", s.outcome) }

func (s series) want() map[string]string {
	m := map[string]string{}
	if s.step != "" {
		m["step"] = s.step
	}
	if s.outcome != "" {
		m["outcome"] = s.outcome
	}
	return m
}

func steps(step, outcome string) series { return series{"probe.steps", step, outcome} }

func cycles(outcome string) series { return series{"probe.cycles", "", outcome} }

func duration(step string) series { return series{"probe.step.duration", step, ""} }

func delay(step string) series { return series{"probe.step.delay", step, ""} }

var allSeries = func() []series {
	var all []series
	for s := range numSteps {
		for o := range numOutcomes {
			all = append(all, steps(s.String(), o.String()))
		}
		all = append(all, duration(s.String()), delay(s.String()))
	}
	for c := range numCycleOutcomes {
		all = append(all, cycles(c.String()))
	}
	return all
}()

func takeSnapshot(t *testing.T) *snapshot {
	s := &snapshot{t: t, before: map[string]int64{}}
	for _, se := range allSeries {
		s.before[se.key()] = metricValue(t, se.name, se.want())
	}
	return s
}

// expect 断言列出的序列各增加了给定的值，没列出的序列都没有变化。
func (s *snapshot) expect(want map[series]int64) {
	s.t.Helper()
	wantByKey := map[string]int64{}
	for se, n := range want {
		wantByKey[se.key()] = n
	}
	for _, se := range allSeries {
		got := metricValue(s.t, se.name, se.want()) - s.before[se.key()]
		if got != wantByKey[se.key()] {
			s.t.Errorf("%s: delta %d, want %d", se.key(), got, wantByKey[se.key()])
		}
	}
}

// fakeGateway 是一个假的入口：订单在内存里，第一次查询是 creating，之后是 pending_payment，支付后是 paid。
// 各接口可以用 override 换掉。
type fakeGateway struct {
	t *testing.T

	mu       sync.Mutex
	nextID   int
	status   map[string]string
	polls    map[string]int
	calls    map[string]int
	getTimes []time.Time

	override map[string]http.HandlerFunc
}

func newFakeGateway(t *testing.T) (*fakeGateway, *httptest.Server) {
	g := &fakeGateway{
		t: t, status: map[string]string{}, polls: map[string]int{}, calls: map[string]int{},
		override: map[string]http.HandlerFunc{},
	}
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	return g, srv
}

func (g *fakeGateway) count(op string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[op]
}

func (g *fakeGateway) set(op string, h http.HandlerFunc) {
	g.mu.Lock()
	g.override[op] = h
	g.mu.Unlock()
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, `{"error":{"code":"`+code+`","message":"x"}}`)
}

// op 按路径和方法给请求起名，和 step 的名字对应。
func op(r *http.Request) (name, orderID string) {
	p := r.URL.Path
	switch {
	case strings.HasPrefix(p, "/api/v1/skus/") && strings.HasSuffix(p, "/stock"):
		return "stock", ""
	case p == gwapi.PathOrders && r.Method == http.MethodPost:
		return "create", ""
	case p == gwapi.PathOrders:
		return "list", ""
	case strings.HasSuffix(p, "/pay"):
		return "pay", strings.TrimSuffix(strings.TrimPrefix(p, gwapi.PathOrders+"/"), "/pay")
	case strings.HasSuffix(p, "/cancel"):
		return "cancel", strings.TrimSuffix(strings.TrimPrefix(p, gwapi.PathOrders+"/"), "/cancel")
	default:
		return "get", strings.TrimPrefix(p, gwapi.PathOrders+"/")
	}
}

func (g *fakeGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name, _ := op(r)
	if got := r.Header.Get(gwapi.HeaderLoadRun); got != LoadRun {
		g.t.Errorf("%s: X-Load-Run = %q, want %q", name, got, LoadRun)
	}
	user, err := strconv.ParseInt(r.Header.Get(gwapi.HeaderUserID), 10, 64)
	if err != nil || user < UserFloor || user >= UserLimit {
		g.t.Errorf("%s: user id %q outside the load range", name, r.Header.Get(gwapi.HeaderUserID))
	}
	g.mu.Lock()
	g.calls[name]++
	if name == "get" {
		g.getTimes = append(g.getTimes, time.Now())
	}
	h := g.override[name]
	g.mu.Unlock()
	if h != nil {
		h(w, r)
		return
	}
	g.serveDefault(w, r)
}

// serveDefault 是各接口的默认行为，override 里也可以调用它。
func (g *fakeGateway) serveDefault(w http.ResponseWriter, r *http.Request) {
	name, id := op(r)
	g.mu.Lock()
	defer g.mu.Unlock()
	switch name {
	case "stock":
		writeJSON(w, http.StatusOK, `{"sku_id":"1","available":100,"reserved":0,"sold":0,"unit_price":{"amount":100,"currency":"CNY"}}`)
	case "create":
		key := r.Header.Get(gwapi.HeaderIdemKey)
		if !strings.HasPrefix(key, keyPrefix) || len(key) > 64 {
			g.t.Errorf("idempotency key %q", key)
		}
		var req gwapi.CreateOrderRequest
		if err := json.UnmarshalRead(r.Body, &req); err != nil || len(req.Items) != 1 || req.Items[0].Quantity != 1 {
			g.t.Errorf("create body: %+v, %v", req, err)
		}
		g.nextID++
		oid := strconv.Itoa(1000 + g.nextID)
		g.status[oid] = gwapi.StatusCreating
		writeJSON(w, http.StatusAccepted, `{"order_id":"`+oid+`","status":"creating"}`)
	case "get":
		st, ok := g.status[id]
		if !ok {
			writeError(w, http.StatusNotFound, "NOT_FOUND")
			return
		}
		g.polls[id]++
		if st == gwapi.StatusCreating && g.polls[id] > 1 {
			st = gwapi.StatusPending
			g.status[id] = st
		}
		writeJSON(w, http.StatusOK, `{"order_id":"`+id+`","status":"`+st+`"}`)
	case "pay":
		g.status[id] = gwapi.StatusPaid
		writeJSON(w, http.StatusAccepted, `{"payment_id":"1","order_id":"`+id+`","status":"pending"}`)
	case "cancel":
		g.status[id] = gwapi.StatusCancelled
		writeJSON(w, http.StatusOK, `{"order_id":"`+id+`","status":"cancelled"}`)
	case "list":
		writeJSON(w, http.StatusOK, `{"orders":[]}`)
	}
}

func testConfig(srv *httptest.Server) Config {
	return Config{
		BaseURL:        srv.URL,
		Interval:       time.Hour,
		MaxInFlight:    20,
		RequestTimeout: 2 * time.Second,
		LandTimeout:    2 * time.Second,
		PayTimeout:     2 * time.Second,
		CancelEvery:    0,
		ListEvery:      0,
		SKUs:           []int64{1},
		UserBase:       DefaultUserBase,
		UserCount:      DefaultUserCount,
		Drain:          5 * time.Second,
		PollInitial:    5 * time.Millisecond,
		PollMax:        20 * time.Millisecond,
		Logger:         slog.New(slog.DiscardHandler),
	}
}

// runCycle 直接跑一个周期，不经过节拍。
func runCycle(t *testing.T, cfg Config, seq int64) {
	t.Helper()
	p, err := newProber(cfg)
	if err != nil {
		t.Fatalf("newProber: %v", err)
	}
	p.cycle(t.Context(), seq)
}

func TestCycleAllStepsOK(t *testing.T) {
	g, srv := newFakeGateway(t)
	cfg := testConfig(srv)
	cfg.ListEvery = 1
	s := takeSnapshot(t)
	runCycle(t, cfg, 1)
	s.expect(map[series]int64{
		steps("stock", "ok"): 1, steps("create", "ok"): 1, steps("land", "ok"): 1,
		steps("pay", "ok"): 1, steps("paid", "ok"): 1, steps("list", "ok"): 1,
		duration("stock"):  1,
		duration("create"): 1,
		duration("pay"):    1,
		duration("list"):   1,
		delay("land"):      1,
		delay("paid"):      1,
		cycles("ok"):       1,
	})
	if g.count("cancel") != 0 {
		t.Errorf("cancel called %d times, want 0", g.count("cancel"))
	}
}

func TestCycleCancelsEveryNth(t *testing.T) {
	g, srv := newFakeGateway(t)
	cfg := testConfig(srv)
	cfg.CancelEvery = 2
	cfg.ListEvery = 3
	s := takeSnapshot(t)
	runCycle(t, cfg, 2)
	s.expect(map[series]int64{
		steps("stock", "ok"): 1, steps("create", "ok"): 1, steps("land", "ok"): 1, steps("cancel", "ok"): 1,
		duration("stock"):  1,
		duration("create"): 1,
		duration("cancel"): 1,
		delay("land"):      1,
		cycles("ok"):       1,
	})
	if g.count("pay") != 0 || g.count("list") != 0 {
		t.Errorf("pay %d, list %d calls, want 0", g.count("pay"), g.count("list"))
	}
}

// 下单被拒绝时周期不完整，后面的步骤都不做；不重试。
func TestCreateNotAccepted(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		outcome string
		cycle   string
	}{
		{"rate limited", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "RATE_LIMITED")
		}, "rejected", "incomplete"},
		{"sold out", func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusConflict, gwapi.CodeInsufficientStock)
		}, "sold_out", "incomplete"},
		{"degraded", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "5")
			writeError(w, http.StatusServiceUnavailable, "FEATURE_DISABLED")
		}, "degraded", "incomplete"},
		{"unavailable", func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusServiceUnavailable, gwapi.CodeUnavailable)
		}, "error", "failed"},
		{"internal error", func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusInternalServerError, "INTERNAL")
		}, "error", "failed"},
		{"other conflict", func(w http.ResponseWriter, _ *http.Request) {
			writeError(w, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED")
		}, "error", "failed"},
		{"bad body", func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusAccepted, `not json`)
		}, "error", "failed"},
		{"request timeout", func(_ http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(500 * time.Millisecond):
			}
		}, "timeout", "failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g, srv := newFakeGateway(t)
			g.set("create", tt.handler)
			cfg := testConfig(srv)
			cfg.RequestTimeout = 100 * time.Millisecond
			s := takeSnapshot(t)
			runCycle(t, cfg, 1)
			s.expect(map[series]int64{
				steps("stock", "ok"):        1,
				duration("stock"):           1,
				steps("create", tt.outcome): 1,
				cycles(tt.cycle):            1,
			})
			if g.count("create") != 1 {
				t.Errorf("create called %d times, want 1 (no retry)", g.count("create"))
			}
			if n := g.count("get") + g.count("pay") + g.count("cancel"); n != 0 {
				t.Errorf("%d follow-up requests after a create that was not accepted", n)
			}
		})
	}
}

// 查库存坏了不挡住下单，但周期算失败。
func TestStockErrorDoesNotStopTheCycle(t *testing.T) {
	g, srv := newFakeGateway(t)
	g.set("stock", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusBadGateway, "x")
	})
	s := takeSnapshot(t)
	runCycle(t, testConfig(srv), 1)
	s.expect(map[series]int64{
		steps("stock", "error"): 1, steps("create", "ok"): 1, steps("land", "ok"): 1,
		steps("pay", "ok"): 1, steps("paid", "ok"): 1,
		duration("create"): 1,
		duration("pay"):    1,
		delay("land"):      1,
		delay("paid"):      1,
		cycles("failed"):   1,
	})
}

func TestLandTimeout(t *testing.T) {
	g, srv := newFakeGateway(t)
	g.set("get", func(w http.ResponseWriter, r *http.Request) {
		_, id := op(r)
		writeJSON(w, http.StatusOK, `{"order_id":"`+id+`","status":"creating"}`)
	})
	cfg := testConfig(srv)
	cfg.LandTimeout = 200 * time.Millisecond
	s := takeSnapshot(t)
	start := time.Now()
	runCycle(t, cfg, 1)
	if d := time.Since(start); d < cfg.LandTimeout {
		t.Errorf("cycle took %v, shorter than the land timeout", d)
	}
	s.expect(map[series]int64{
		steps("stock", "ok"): 1, steps("create", "ok"): 1, steps("land", "timeout"): 1,
		duration("stock"):  1,
		duration("create"): 1,
		cycles("failed"):   1,
	})
	if g.count("pay") != 0 {
		t.Error("paid an order that never landed")
	}
	if n := g.count("get"); n < 3 {
		t.Errorf("polled %d times, want several", n)
	}
}

func TestLandFailedIsError(t *testing.T) {
	g, srv := newFakeGateway(t)
	g.set("get", func(w http.ResponseWriter, r *http.Request) {
		_, id := op(r)
		writeJSON(w, http.StatusOK, `{"order_id":"`+id+`","status":"failed"}`)
	})
	s := takeSnapshot(t)
	runCycle(t, testConfig(srv), 1)
	s.expect(map[series]int64{
		steps("stock", "ok"): 1, steps("create", "ok"): 1, steps("land", "error"): 1,
		duration("stock"):  1,
		duration("create"): 1,
		cycles("failed"):   1,
	})
}

// 轮询收到 429 带 Retry-After 时，下一次查询要等够这么久，即使退避的间隔短得多。
func TestPollRespectsRetryAfter(t *testing.T) {
	g, srv := newFakeGateway(t)
	var once sync.Once
	g.set("get", func(w http.ResponseWriter, r *http.Request) {
		limited := false
		once.Do(func() { limited = true })
		if limited {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "READ_BUSY")
			return
		}
		g.serveDefault(w, r)
	})
	cfg := testConfig(srv)
	cfg.LandTimeout = 5 * time.Second
	s := takeSnapshot(t)
	runCycle(t, cfg, 1)

	g.mu.Lock()
	times := append([]time.Time(nil), g.getTimes...)
	g.mu.Unlock()
	// 落库等了两次查询，支付后又查了至少一次。
	if len(times) < 2 {
		t.Fatalf("got %d polls, want >= 2", len(times))
	}
	if gap := times[1].Sub(times[0]); gap < time.Second {
		t.Errorf("polled again %v after a 429 with Retry-After: 1", gap)
	}
	// 轮询中途的 429 不算这一步的结果，最后等到了就是 ok。
	s.expect(map[series]int64{
		steps("stock", "ok"): 1, steps("create", "ok"): 1, steps("land", "ok"): 1,
		steps("pay", "ok"): 1, steps("paid", "ok"): 1,
		duration("stock"): 1, duration("create"): 1, duration("pay"): 1,
		delay("land"): 1, delay("paid"): 1,
		cycles("ok"): 1,
	})
}

// Retry-After 比剩下的时限还长时不再问，按超时算。
func TestPollRetryAfterBeyondDeadline(t *testing.T) {
	g, srv := newFakeGateway(t)
	g.set("get", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "10")
		writeError(w, http.StatusTooManyRequests, "READ_BUSY")
	})
	cfg := testConfig(srv)
	cfg.LandTimeout = 300 * time.Millisecond
	s := takeSnapshot(t)
	start := time.Now()
	runCycle(t, cfg, 1)
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("cycle took %v, should give up at the land timeout", d)
	}
	if n := g.count("get"); n != 1 {
		t.Errorf("polled %d times, want 1", n)
	}
	if got := metricValue(t, "probe.steps", map[string]string{"step": "land", "outcome": "timeout"}) -
		s.before[steps("land", "timeout").key()]; got != 1 {
		t.Errorf("land timeout delta %d, want 1", got)
	}
}

func TestPaidNeverArrives(t *testing.T) {
	g, srv := newFakeGateway(t)
	g.set("pay", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusAccepted, `{"payment_id":"1","status":"pending"}`)
	})
	cfg := testConfig(srv)
	cfg.PayTimeout = 150 * time.Millisecond
	s := takeSnapshot(t)
	runCycle(t, cfg, 1)
	s.expect(map[series]int64{
		steps("stock", "ok"): 1, steps("create", "ok"): 1, steps("land", "ok"): 1,
		steps("pay", "ok"): 1, steps("paid", "timeout"): 1,
		duration("stock"):  1,
		duration("create"): 1,
		duration("pay"):    1,
		delay("land"):      1,
		cycles("failed"):   1,
	})
}

// blockStock 让查库存挂住，直到 release 被关闭或请求被取消；entered 在每次进入时收到一个值。
func blockStock(g *fakeGateway, release <-chan struct{}) <-chan struct{} {
	entered := make(chan struct{}, 100)
	g.set("stock", func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		writeJSON(w, http.StatusOK, `{"available":1}`)
	})
	return entered
}

// 在途的周期到了上限时，节拍照走，但这一拍跳过并计数。
func TestSkipsWhenInFlightLimitReached(t *testing.T) {
	g, srv := newFakeGateway(t)
	release := make(chan struct{})
	entered := blockStock(g, release)
	cfg := testConfig(srv)
	cfg.Interval = 10 * time.Millisecond
	cfg.MaxInFlight = 1
	cfg.RequestTimeout = 5 * time.Second
	s := takeSnapshot(t)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	<-entered
	time.Sleep(150 * time.Millisecond)
	cancel()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := g.count("stock"); n != 1 {
		t.Errorf("stock called %d times, want 1 (one cycle in flight)", n)
	}
	skipped := metricValue(t, "probe.cycles", map[string]string{"outcome": "skipped"}) - s.before[cycles("skipped").key()]
	if skipped < 5 {
		t.Errorf("skipped %d ticks, want >= 5", skipped)
	}
	if got := metricValue(t, "probe.cycles", map[string]string{"outcome": "ok"}) - s.before[cycles("ok").key()]; got != 1 {
		t.Errorf("ok cycles delta %d, want 1", got)
	}
}

// 收到停止信号后不再发新的周期，等在途的做完再返回。
func TestStopWaitsForCyclesInFlight(t *testing.T) {
	g, srv := newFakeGateway(t)
	release := make(chan struct{})
	entered := blockStock(g, release)
	cfg := testConfig(srv)
	cfg.RequestTimeout = 5 * time.Second
	s := takeSnapshot(t)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	<-entered
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned (%v) while a cycle was in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the cycle in flight finished")
	}
	if n := g.count("create"); n != 1 {
		t.Errorf("create called %d times, want 1", n)
	}
	if got := metricValue(t, "probe.cycles", map[string]string{"outcome": "ok"}) - s.before[cycles("ok").key()]; got != 1 {
		t.Errorf("ok cycles delta %d, want 1", got)
	}
}

// 在途的周期超过 Drain 还没做完就中断，中断的周期不计入指标（那是探针自己造成的）。
func TestStopAbortsAfterDrain(t *testing.T) {
	g, srv := newFakeGateway(t)
	release := make(chan struct{})
	defer close(release)
	entered := blockStock(g, release)
	cfg := testConfig(srv)
	cfg.RequestTimeout = 5 * time.Second
	cfg.Drain = 100 * time.Millisecond
	s := takeSnapshot(t)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	<-entered
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the drain timeout")
	}
	s.expect(nil)
}

func TestValidateUserRange(t *testing.T) {
	_, srv := newFakeGateway(t)
	for _, tt := range []struct {
		base, count int64
		ok          bool
	}{
		{DefaultUserBase, DefaultUserCount, true},
		{UserFloor, 1000, true},
		{UserFloor - 1, 10, false},
		{UserLimit - 10, 11, false},
		{UserFloor, 0, false},
	} {
		cfg := testConfig(srv)
		cfg.UserBase, cfg.UserCount = tt.base, tt.count
		if err := cfg.Validate(); (err == nil) != tt.ok {
			t.Errorf("base %d count %d: err %v, want ok=%v", tt.base, tt.count, err, tt.ok)
		}
	}
}
