package runner

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/palemoky/Oppenheimer/test/load/internal/ledger"
)

// fakeGateway 在内存里模拟网关的下单、查询、支付、取消接口。
type fakeGateway struct {
	t *testing.T

	// 行为开关，在启动前设置。
	sync201        bool   // 提交返回 201 且订单已落库
	rejectCode     string // 不为空时提交一律返回 409 和这个原因码
	landAfterPolls int    // 查询多少次之后从 creating 变成 pending_payment
	// dropSubmits 是前多少个提交请求在服务端执行（建单）之后直接断开连接。
	dropSubmits int
	// fail503First 为 true 时每个幂等键的第一次提交返回 503（不建单）。
	fail503First bool
	// loseFirstPoll 为 true 时每个订单的第一次查询返回 404 并丢掉订单（模拟受理后丢单）。
	loseFirstPoll bool
	// submitDelay 让每个提交请求先等这么久。
	submitDelay time.Duration

	mu      sync.Mutex
	nextID  int64
	orders  map[string]*fakeOrder // order_id → 订单
	byKey   map[string]*fakeOrder // user|key → 订单
	submits map[string]int        // 幂等键 → 收到的提交次数
	lost    map[string]bool       // 幂等键 → 已经丢过一次
}

type fakeOrder struct {
	id     string
	user   string
	key    string
	status string
	polls  int
	paying bool
}

func newFakeGateway(t *testing.T) *fakeGateway {
	return &fakeGateway{
		t:              t,
		landAfterPolls: 2,
		nextID:         9007199254740990, // 超过 2^53，确认订单号按字符串处理
		orders:         map[string]*fakeOrder{},
		byKey:          map[string]*fakeOrder{},
		submits:        map[string]int{},
		lost:           map[string]bool{},
	}
}

func (g *fakeGateway) start() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/orders", g.submit)
	mux.HandleFunc("GET /api/v1/orders/{id}", g.get)
	mux.HandleFunc("POST /api/v1/orders/{id}/pay", g.pay)
	mux.HandleFunc("POST /api/v1/orders/{id}/cancel", g.cancel)
	srv := httptest.NewServer(mux)
	g.t.Cleanup(srv.Close)
	return srv
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.MarshalWrite(w, v)
}

func writeErr(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": code}})
}

func (o *fakeOrder) view() map[string]any {
	return map[string]any{"order_id": o.id, "user_id": o.user, "status": o.status}
}

func (g *fakeGateway) submit(w http.ResponseWriter, r *http.Request) {
	user, key := r.Header.Get("X-User-Id"), r.Header.Get("Idempotency-Key")
	if user == "" || key == "" || len(key) > 64 {
		writeErr(w, http.StatusBadRequest, "INVALID_ARGUMENT")
		return
	}
	var req createOrderRequest
	if err := json.UnmarshalRead(r.Body, &req); err != nil || len(req.Items) != 1 || req.Items[0].SKUID <= 0 {
		writeErr(w, http.StatusBadRequest, "INVALID_ARGUMENT")
		return
	}
	if g.submitDelay > 0 {
		select {
		case <-time.After(g.submitDelay):
		case <-r.Context().Done():
			return
		}
	}

	g.mu.Lock()
	g.submits[key]++
	n := g.submits[key]
	if g.rejectCode != "" {
		g.mu.Unlock()
		writeErr(w, http.StatusConflict, g.rejectCode)
		return
	}
	if g.fail503First && n == 1 {
		g.mu.Unlock()
		writeErr(w, http.StatusServiceUnavailable, "UNAVAILABLE")
		return
	}
	o := g.byKey[user+"|"+key]
	if o == nil {
		g.nextID++
		o = &fakeOrder{id: strconv.FormatInt(g.nextID, 10), user: user, key: key, status: statusCreating}
		if g.sync201 {
			o.status = statusPending
		}
		g.orders[o.id] = o
		g.byKey[user+"|"+key] = o
	}
	view := o.view()
	drop := g.dropSubmits > 0
	if drop {
		g.dropSubmits--
	}
	g.mu.Unlock()

	if drop {
		// 订单已经建好，客户端却拿不到响应：结果未知。
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			g.t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
		return
	}
	if g.sync201 {
		writeJSON(w, http.StatusCreated, view)
		return
	}
	writeJSON(w, http.StatusAccepted, view)
}

func (g *fakeGateway) lookup(r *http.Request) *fakeOrder {
	o := g.orders[r.PathValue("id")]
	if o == nil || o.user != r.Header.Get("X-User-Id") {
		return nil
	}
	return o
}

func (g *fakeGateway) get(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	o := g.lookup(r)
	if o == nil {
		writeErr(w, http.StatusNotFound, "ORDER_NOT_FOUND")
		return
	}
	if g.loseFirstPoll && !g.lost[o.key] {
		g.lost[o.key] = true
		delete(g.orders, o.id)
		delete(g.byKey, o.user+"|"+o.key)
		writeErr(w, http.StatusNotFound, "ORDER_NOT_FOUND")
		return
	}
	o.polls++
	switch {
	case o.status == statusCreating && o.polls >= g.landAfterPolls:
		o.status = statusPending
	case o.paying:
		o.status, o.paying = statusPaid, false
	}
	writeJSON(w, http.StatusOK, o.view())
}

func (g *fakeGateway) pay(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	o := g.lookup(r)
	if o == nil {
		writeErr(w, http.StatusNotFound, "ORDER_NOT_FOUND")
		return
	}
	if o.status != statusPending {
		writeErr(w, http.StatusConflict, "ORDER_NOT_PAYABLE")
		return
	}
	o.paying = true
	writeJSON(w, http.StatusAccepted, map[string]any{"payment_id": "1", "order_id": o.id, "status": "pending"})
}

func (g *fakeGateway) cancel(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	o := g.lookup(r)
	if o == nil {
		writeErr(w, http.StatusNotFound, "ORDER_NOT_FOUND")
		return
	}
	if o.status != statusPending && o.status != "cancelled" {
		writeErr(w, http.StatusConflict, "ORDER_NOT_CANCELLABLE")
		return
	}
	o.status = "cancelled"
	writeJSON(w, http.StatusOK, o.view())
}

func (g *fakeGateway) orderCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.orders)
}

func (g *fakeGateway) submitCounts() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return maps.Clone(g.submits)
}

func testConfig(baseURL string) Config {
	return Config{
		BaseURL:        baseURL,
		Concurrency:    4,
		Orders:         20,
		SKUs:           []int64{1001, 1002},
		Quantity:       1,
		PollInterval:   time.Millisecond,
		PollTimeout:    2 * time.Second,
		RequestTimeout: 2 * time.Second,
		SubmitAttempts: 3,
		RetryBackoff:   time.Millisecond,
		Users:          1000,
		Seed:           42,
		RunID:          "t",
		Logger:         slog.New(slog.DiscardHandler),
	}
}

// runLoad 运行压测，返回报告和读回的账本。
func runLoad(ctx context.Context, t *testing.T, cfg Config) (*Report, []ledger.Entry) {
	t.Helper()
	var buf bytes.Buffer
	led := ledger.NewWriter(&buf)
	rep, err := Run(ctx, cfg, led)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := led.Close(); err != nil {
		t.Fatalf("close ledger: %v", err)
	}
	var entries []ledger.Entry
	for e, err := range ledger.Read(&buf) {
		if err != nil {
			t.Fatalf("read ledger: %v", err)
		}
		entries = append(entries, e)
	}
	return rep, entries
}

func count(entries []ledger.Entry, op ledger.Op, stage ledger.Stage, outcome ledger.Outcome) int {
	n := 0
	for _, e := range entries {
		if e.Op == op && e.Stage == stage && e.Outcome == outcome {
			n++
		}
	}
	return n
}

func TestAsyncSubmitPollPay(t *testing.T) {
	g := newFakeGateway(t)
	srv := g.start()
	cfg := testConfig(srv.URL)
	cfg.PayRatio = 1

	rep, entries := runLoad(t.Context(), t, cfg)

	for _, k := range []string{"started", "accepted", "landed", "paid"} {
		if rep.Orders[k] != 20 {
			t.Errorf("orders[%s] = %d, want 20 (orders: %v)", k, rep.Orders[k], rep.Orders)
		}
	}
	if rep.Orders["accepted_sync"] != 0 {
		t.Errorf("accepted_sync = %d, want 0", rep.Orders["accepted_sync"])
	}
	if rep.Accepted.Count != 20 || rep.Landed.Count != 20 || rep.Accepted.Latency.Count != 20 {
		t.Errorf("accepted %d landed %d latency samples %d, want 20 each",
			rep.Accepted.Count, rep.Landed.Count, rep.Accepted.Latency.Count)
	}
	if rep.Accepted.PerSecond <= 0 || rep.Landed.PerSecond <= 0 {
		t.Errorf("throughput accepted %.2f landed %.2f, want > 0", rep.Accepted.PerSecond, rep.Landed.PerSecond)
	}
	if rep.SettledStatus[statusPaid] != 20 {
		t.Errorf("settled status = %v, want 20 paid", rep.SettledStatus)
	}
	if rep.PollRequests == 0 || rep.PollErrors != 0 {
		t.Errorf("polls %d errors %d", rep.PollRequests, rep.PollErrors)
	}

	if n := count(entries, ledger.OpSubmit, "", ledger.OutcomeOK); n != 20 {
		t.Errorf("submit ok lines = %d, want 20", n)
	}
	if n := count(entries, ledger.OpPollFinal, ledger.StageLanding, ledger.OutcomeOK); n != 20 {
		t.Errorf("landing ok lines = %d, want 20", n)
	}
	if n := count(entries, ledger.OpPay, "", ledger.OutcomeOK); n != 20 {
		t.Errorf("pay ok lines = %d, want 20", n)
	}
	if n := count(entries, ledger.OpPollFinal, ledger.StageSettle, ledger.OutcomeOK); n != 20 {
		t.Errorf("settle ok lines = %d, want 20", n)
	}
	if len(entries) != 80 {
		t.Errorf("ledger lines = %d, want 80", len(entries))
	}
	for _, e := range entries {
		if e.OrderID == "" || e.IdempotencyKey == "" || e.UserID < 1 || e.UserID > 1000 || e.Quantity != 1 {
			t.Errorf("incomplete entry %+v", e)
		}
		if e.SKUID != 1001 && e.SKUID != 1002 {
			t.Errorf("sku %d not in list", e.SKUID)
		}
		switch {
		case e.Op == ledger.OpSubmit && e.OrderStatus != statusCreating,
			e.Stage == ledger.StageLanding && e.OrderStatus != statusPending,
			e.Stage == ledger.StageSettle && e.OrderStatus != statusPaid:
			t.Errorf("unexpected order status in %+v", e)
		}
	}
}

func TestSync201NeedsNoPolling(t *testing.T) {
	g := newFakeGateway(t)
	g.sync201 = true
	srv := g.start()
	cfg := testConfig(srv.URL)
	cfg.CancelRatio = 1

	rep, entries := runLoad(t.Context(), t, cfg)

	for _, k := range []string{"accepted", "accepted_sync", "landed", "cancelled"} {
		if rep.Orders[k] != 20 {
			t.Errorf("orders[%s] = %d, want 20 (orders: %v)", k, rep.Orders[k], rep.Orders)
		}
	}
	if rep.PollRequests != 0 {
		t.Errorf("poll requests = %d, want 0 for 201", rep.PollRequests)
	}
	if n := count(entries, ledger.OpPollFinal, ledger.StageLanding, ledger.OutcomeOK); n != 20 {
		t.Errorf("landing lines = %d, want 20", n)
	}
	for _, e := range entries {
		if e.Op == ledger.OpCancel && (e.Outcome != ledger.OutcomeOK || e.OrderStatus != "cancelled") {
			t.Errorf("cancel entry %+v", e)
		}
	}
}

func TestBusinessRejectIsCountedNotError(t *testing.T) {
	g := newFakeGateway(t)
	g.rejectCode = "INSUFFICIENT_STOCK"
	srv := g.start()
	cfg := testConfig(srv.URL)

	rep, entries := runLoad(t.Context(), t, cfg)

	if rep.Orders["rejected"] != 20 || rep.RejectCodes["INSUFFICIENT_STOCK"] != 20 {
		t.Errorf("orders %v reject codes %v, want 20 rejected", rep.Orders, rep.RejectCodes)
	}
	if rep.Requests["submit"]["rejected"] != 20 || len(rep.HTTPErrors) != 0 || rep.Orders["submit_gave_up"] != 0 {
		t.Errorf("requests %v http errors %v", rep.Requests, rep.HTTPErrors)
	}
	// 业务拒绝不重试。
	for k, n := range g.submitCounts() {
		if n != 1 {
			t.Errorf("key %s submitted %d times, want 1", k, n)
		}
	}
	for _, e := range entries {
		if e.Outcome != ledger.OutcomeRejected || e.HTTPStatus != http.StatusConflict ||
			e.ErrorCode != "INSUFFICIENT_STOCK" || e.OrderID != "" {
			t.Errorf("entry %+v", e)
		}
	}
}

func TestUnknownCodeIsHTTPError(t *testing.T) {
	g := newFakeGateway(t)
	g.rejectCode = "SKU_NOT_FOUND"
	srv := g.start()
	cfg := testConfig(srv.URL)
	cfg.Orders = 3

	rep, _ := runLoad(t.Context(), t, cfg)

	if rep.Orders["rejected"] != 0 || rep.Orders["submit_gave_up"] != 3 || rep.HTTPErrors["submit 409"] != 3 {
		t.Errorf("orders %v http errors %v", rep.Orders, rep.HTTPErrors)
	}
}

func TestDroppedConnectionIsUnknownAndRetriedWithSameKey(t *testing.T) {
	g := newFakeGateway(t)
	g.dropSubmits = 3
	srv := g.start()
	cfg := testConfig(srv.URL)
	cfg.Concurrency = 1
	cfg.Orders = 5
	cfg.SubmitAttempts = 5

	rep, entries := runLoad(t.Context(), t, cfg)

	if rep.Requests["submit"]["unknown"] != 3 || rep.Orders["accepted"] != 5 || rep.Orders["landed"] != 5 {
		t.Errorf("requests %v orders %v", rep.Requests, rep.Orders)
	}
	// 服务端执行了被断开的请求，重试用同一个幂等键，所以仍然只有 5 个订单。
	if n := g.orderCount(); n != 5 {
		t.Errorf("server has %d orders, want 5", n)
	}
	unknownKeys := map[string]bool{}
	for _, e := range entries {
		if e.Op == ledger.OpSubmit && e.Outcome == ledger.OutcomeUnknown {
			if e.HTTPStatus != 0 || e.Error == "" || e.OrderID != "" {
				t.Errorf("unknown entry %+v", e)
			}
			unknownKeys[e.IdempotencyKey] = true
		}
	}
	for _, e := range entries {
		if e.Op == ledger.OpSubmit && e.Outcome == ledger.OutcomeOK && unknownKeys[e.IdempotencyKey] && e.Attempt < 2 {
			t.Errorf("retried submit should have attempt >= 2: %+v", e)
		}
	}
	if len(unknownKeys) == 0 {
		t.Error("no unknown submit recorded")
	}
}

func TestRetryAfter5xxReusesKey(t *testing.T) {
	g := newFakeGateway(t)
	g.fail503First = true
	srv := g.start()
	cfg := testConfig(srv.URL)

	rep, entries := runLoad(t.Context(), t, cfg)

	if rep.Orders["accepted"] != 20 || rep.HTTPErrors["submit 503"] != 20 || rep.Orders["submit_retried"] != 20 {
		t.Errorf("orders %v http errors %v", rep.Orders, rep.HTTPErrors)
	}
	counts := g.submitCounts()
	if len(counts) != 20 {
		t.Errorf("server saw %d distinct keys, want 20", len(counts))
	}
	for k, n := range counts {
		if n != 2 {
			t.Errorf("key %s submitted %d times, want 2", k, n)
		}
	}
	attempts := map[string][]int{}
	for _, e := range entries {
		if e.Op == ledger.OpSubmit {
			attempts[e.IdempotencyKey] = append(attempts[e.IdempotencyKey], e.Attempt)
		}
	}
	for k, a := range attempts {
		if len(a) != 2 || a[0] != 1 || a[1] != 2 {
			t.Errorf("key %s attempts %v, want [1 2]", k, a)
		}
	}
}

func TestLostOrderIsResubmittedWithSameKey(t *testing.T) {
	g := newFakeGateway(t)
	g.loseFirstPoll = true
	srv := g.start()
	cfg := testConfig(srv.URL)

	rep, _ := runLoad(t.Context(), t, cfg)

	if rep.Orders["resubmitted"] != 20 || rep.Orders["landed"] != 20 || rep.Orders["accepted"] != 20 {
		t.Errorf("orders %v", rep.Orders)
	}
	for k, n := range g.submitCounts() {
		if n != 2 {
			t.Errorf("key %s submitted %d times, want 2", k, n)
		}
	}
}

func TestServerDownDoesNotStopRun(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // 之后的连接都会被拒绝
	cfg := testConfig(url)
	cfg.Orders = 6
	cfg.SubmitAttempts = 2

	rep, entries := runLoad(t.Context(), t, cfg)

	if rep.Orders["submit_gave_up"] != 6 || rep.Requests["submit"]["unknown"] != 12 {
		t.Errorf("orders %v requests %v", rep.Orders, rep.Requests)
	}
	if len(entries) != 12 {
		t.Errorf("ledger lines = %d, want 12", len(entries))
	}
}

func TestDurationModeStops(t *testing.T) {
	g := newFakeGateway(t)
	srv := g.start()
	cfg := testConfig(srv.URL)
	cfg.Orders = 0
	cfg.Duration = 100 * time.Millisecond
	cfg.PayRatio, cfg.CancelRatio = 0.5, 0.25

	start := time.Now()
	rep, _ := runLoad(t.Context(), t, cfg)
	if time.Since(start) > 5*time.Second {
		t.Errorf("run took %v", time.Since(start))
	}
	if rep.Orders["accepted"] == 0 || rep.Orders["accepted"] != rep.Orders["landed"] {
		t.Errorf("orders %v", rep.Orders)
	}
	if got := rep.Orders["paid"] + rep.Orders["cancelled"] + rep.Orders["abandoned"]; got != rep.Orders["landed"] {
		t.Errorf("paid+cancelled+abandoned = %d, landed = %d", got, rep.Orders["landed"])
	}
	if rep.Interrupted {
		t.Error("interrupted = true")
	}
}

func TestInterruptRecordsUnknownAndReturns(t *testing.T) {
	g := newFakeGateway(t)
	g.submitDelay = time.Hour
	srv := g.start()
	cfg := testConfig(srv.URL)
	cfg.Orders = 100

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	rep, entries := runLoad(ctx, t, cfg)

	if !rep.Interrupted {
		t.Error("interrupted = false")
	}
	if len(entries) != cfg.Concurrency {
		t.Errorf("ledger lines = %d, want one per virtual user", len(entries))
	}
	for _, e := range entries {
		if e.Outcome != ledger.OutcomeUnknown {
			t.Errorf("entry %+v, want unknown", e)
		}
	}
}

func TestSameSeedSameSequence(t *testing.T) {
	seq := func() []string {
		g := newFakeGateway(t)
		srv := g.start()
		cfg := testConfig(srv.URL)
		cfg.Concurrency = 1
		cfg.PayRatio, cfg.CancelRatio = 0.3, 0.3
		_, entries := runLoad(t.Context(), t, cfg)
		var out []string
		for _, e := range entries {
			out = append(out, fmt.Sprintf("%s/%s/%d/%d", e.Op, e.IdempotencyKey, e.UserID, e.SKUID))
		}
		return out
	}
	a, b := seq(), seq()
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Errorf("same seed produced different sequences:\n%v\n%v", a, b)
	}
}

func TestReportJSONAndText(t *testing.T) {
	g := newFakeGateway(t)
	srv := g.start()
	cfg := testConfig(srv.URL)
	rep, _ := runLoad(t.Context(), t, cfg)

	b, err := json.Marshal(rep, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back["seed"] != "42" {
		t.Errorf("seed = %v, want string 42", back["seed"])
	}
	if err := rep.WriteText(io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestValidate(t *testing.T) {
	base := testConfig("http://127.0.0.1:8080")
	if err := base.Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	for name, mut := range map[string]func(*Config){
		"bad url":      func(c *Config) { c.BaseURL = "127.0.0.1:8080" },
		"both modes":   func(c *Config) { c.Duration = time.Second },
		"neither mode": func(c *Config) { c.Orders = 0 },
		"no skus":      func(c *Config) { c.SKUs = nil },
		"ratios":       func(c *Config) { c.PayRatio, c.CancelRatio = 0.7, 0.4 },
		"users":        func(c *Config) { c.Users = 0 },
		"run id":       func(c *Config) { c.RunID = "abcdefghijklmnopqrstuvwxyz" },
	} {
		c := base
		mut(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}
