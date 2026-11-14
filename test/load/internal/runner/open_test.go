package runner

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
)

// recorder 收集 Send 收到的请求。
type recorder struct {
	mu   sync.Mutex
	reqs []openReq
}

func (r *recorder) add(q openReq) {
	r.mu.Lock()
	r.reqs = append(r.reqs, q)
	r.mu.Unlock()
}

func TestSchedulerFixedTimetable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder
		s := &scheduler{
			Rate: 100, Duration: 2 * time.Second, MaxInFlight: 1000, Clock: realClock{},
			Send: func(_ context.Context, q openReq) { rec.add(q) },
		}
		res := s.run(t.Context())
		if res.Planned != 200 || res.Sent != 200 || res.Dropped != 0 {
			t.Fatalf("planned %d sent %d dropped %d, want 200 200 0", res.Planned, res.Sent, res.Dropped)
		}
		slices.SortFunc(rec.reqs, func(a, b openReq) int { return int(a.Index - b.Index) })
		for i, q := range rec.reqs {
			if q.Index != int64(i) {
				t.Fatalf("request %d has index %d", i, q.Index)
			}
			if want := res.Start.Add(time.Duration(i) * 10 * time.Millisecond); !q.Planned.Equal(want) {
				t.Fatalf("request %d planned at %v, want %v", i, q.Planned.Sub(res.Start), want.Sub(res.Start))
			}
			if q.Sent.Before(q.Planned) {
				t.Fatalf("request %d sent before its planned time", i)
			}
		}
	})
}

func TestSchedulerInFlightLimitDropsInsteadOfBlocking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			mu               sync.Mutex
			fromSched, fromS []time.Duration
		)
		s := &scheduler{Rate: 100, Duration: 2 * time.Second, MaxInFlight: 10, Clock: realClock{}}
		s.Send = func(_ context.Context, q openReq) {
			time.Sleep(time.Second) // 每个请求占用名额 1 秒
			now := time.Now()
			mu.Lock()
			fromSched = append(fromSched, now.Sub(q.Planned))
			fromS = append(fromS, now.Sub(q.Sent))
			mu.Unlock()
		}
		res := s.run(t.Context())
		if res.Planned != 200 {
			t.Errorf("planned %d, want 200", res.Planned)
		}
		if res.Sent+res.Dropped != res.Planned {
			t.Errorf("sent %d + dropped %d != planned %d", res.Sent, res.Dropped, res.Planned)
		}
		// 1 秒一轮、每轮最多 10 个：2 秒的窗口里发出的数量远小于 200。
		if res.Sent < 10 || res.Sent > 30 || res.Dropped == 0 {
			t.Errorf("sent %d dropped %d, want sent in [10, 30] and dropped > 0", res.Sent, res.Dropped)
		}
		if len(fromS) != int(res.Sent) {
			t.Fatalf("recorded %d latencies, sent %d", len(fromS), res.Sent)
		}
		for i := range fromS {
			if fromSched[i] < fromS[i] {
				t.Errorf("latency from schedule %v < from send %v", fromSched[i], fromS[i])
			}
		}
	})
}

// jumpClock 是假时钟：每次 Sleep 至少跳过 jump，模拟调度循环被拖慢。
type jumpClock struct {
	mu   sync.Mutex
	now  time.Time
	jump time.Duration
}

func (c *jumpClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *jumpClock) Sleep(ctx context.Context, d time.Duration) bool {
	c.mu.Lock()
	c.now = c.now.Add(max(d, c.jump))
	c.mu.Unlock()
	return ctx.Err() == nil
}

func TestSchedulerCatchesUpAfterStall(t *testing.T) {
	clk := &jumpClock{now: time.Unix(1000, 0), jump: 500 * time.Millisecond}
	var rec recorder
	s := &scheduler{
		Rate: 100, Duration: 2 * time.Second, MaxInFlight: 1000, Clock: clk,
		Send: func(_ context.Context, q openReq) { rec.add(q) },
	}
	res := s.run(t.Context())
	if res.Planned != 200 || res.Sent != 200 || res.Dropped != 0 {
		t.Fatalf("planned %d sent %d dropped %d, want 200 200 0", res.Planned, res.Sent, res.Dropped)
	}
	// 时钟每次跳 500ms，每次补齐 50 个：计划时刻仍然是均匀的 10ms 间隔，被耽搁的请求实际发出得更晚。
	slices.SortFunc(rec.reqs, func(a, b openReq) int { return int(a.Index - b.Index) })
	var late int
	for i, q := range rec.reqs {
		if want := res.Start.Add(time.Duration(i) * 10 * time.Millisecond); !q.Planned.Equal(want) {
			t.Fatalf("request %d planned at %v, want %v", i, q.Planned, want)
		}
		if q.Sent.Sub(q.Planned) > 100*time.Millisecond {
			late++
		}
	}
	if late == 0 {
		t.Error("no request was sent late although the clock jumped")
	}
}

// openGateway 是开环端到端测试用的假网关：按幂等键里的序号轮流受理、背压拒绝、挂住不响应。
type openGateway struct {
	t           *testing.T
	stop        chan struct{}
	landAfter   int32 // 受理的订单被查询多少次之后离开 creating，0 表示永远不离开
	mu          sync.Mutex
	orders      map[string]*openOrder
	nextID      int64
	gets        atomic.Int64
	submitCalls atomic.Int64
}

type openOrder struct {
	user  string
	polls int32
}

func (g *openGateway) start() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		g.submitCalls.Add(1)
		key := r.Header.Get("Idempotency-Key")
		seq, err := strconv.Atoi(key[strings.LastIndexByte(key, '-')+1:])
		if err != nil {
			writeErr(w, http.StatusBadRequest, "INVALID_ARGUMENT")
			return
		}
		switch seq % 3 {
		case 0:
			writeErr(w, http.StatusTooManyRequests, "BACKLOGGED")
		case 1:
			g.mu.Lock()
			g.nextID++
			id := strconv.FormatInt(g.nextID, 10)
			g.orders[id] = &openOrder{user: r.Header.Get("X-User-Id")}
			g.mu.Unlock()
			writeJSON(w, http.StatusAccepted, map[string]any{"order_id": id, "user_id": r.Header.Get("X-User-Id"), "status": statusCreating})
		default:
			_, _ = io.Copy(io.Discard, r.Body)
			select { // 不响应，客户端超时
			case <-r.Context().Done():
			case <-g.stop:
			}
		}
	})
	mux.HandleFunc("GET /api/v1/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		g.gets.Add(1)
		g.mu.Lock()
		o := g.orders[r.PathValue("id")]
		if o == nil {
			g.mu.Unlock()
			writeErr(w, http.StatusNotFound, "ORDER_NOT_FOUND")
			return
		}
		o.polls++
		status := statusCreating
		if g.landAfter > 0 && o.polls >= g.landAfter {
			status = statusPending
		}
		g.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"order_id": r.PathValue("id"), "status": status})
	})
	srv := httptest.NewServer(mux)
	g.t.Cleanup(srv.Close)
	g.t.Cleanup(func() { close(g.stop) }) // 先于 srv.Close 运行，放走挂住的请求
	return srv
}

func openConfig(url string) Config {
	cfg := testConfig(url)
	cfg.Mode = ModeOpen
	cfg.Orders = 0
	cfg.Duration = 600 * time.Millisecond
	cfg.Rate = 100
	cfg.MaxInFlight = 100
	cfg.Drain = 5 * time.Second
	cfg.RequestTimeout = 150 * time.Millisecond
	return cfg
}

func TestOpenModeClassifiesAndRecordsLedger(t *testing.T) {
	g := &openGateway{t: t, stop: make(chan struct{}), landAfter: 2, orders: map[string]*openOrder{}}
	srv := g.start()
	cfg := openConfig(srv.URL)

	rep, entries := runLoad(t.Context(), t, cfg)
	o := rep.Open
	if o == nil {
		t.Fatal("report has no open section")
	}
	if o.Planned != 60 || o.Sent != 60 || o.Dropped != 0 {
		t.Errorf("planned %d sent %d dropped %d, want 60 60 0", o.Planned, o.Sent, o.Dropped)
	}
	for k, want := range map[string]int64{outAccepted: 20, outBacklogged: 20, outTimeout: 20} {
		if o.Outcomes[k] != want {
			t.Errorf("outcomes[%s] = %d, want %d (all: %v)", k, o.Outcomes[k], want, o.Outcomes)
		}
	}
	if o.AcceptedFromSchedule.Count != 20 || o.RejectedFromSchedule.Count != 20 {
		t.Errorf("latency samples: accepted %d rejected %d, want 20 and 20", o.AcceptedFromSchedule.Count, o.RejectedFromSchedule.Count)
	}
	if o.AcceptedFromSchedule.P50Ms < o.AcceptedServiceTime.P50Ms {
		t.Errorf("p50 from schedule %v < service time %v", o.AcceptedFromSchedule.P50Ms, o.AcceptedServiceTime.P50Ms)
	}
	if o.Drain.Accepted != 20 || o.Drain.LeftCreating != 20 || o.Drain.StillCreating != 0 || o.Drain.TimedOut {
		t.Errorf("drain = %+v", o.Drain)
	}
	if o.Drain.LandingLatencyMeasured {
		t.Error("landing latency must be reported as not measured")
	}
	var sum int64
	for _, n := range o.AcceptedPerSecond.Series {
		sum += n
	}
	if sum != 20 {
		t.Errorf("per-second series sums to %d, want 20", sum)
	}

	var submits, finals, unknown, httpErr int
	keys := map[string]bool{}
	for _, e := range entries {
		switch e.Op {
		case ledger.OpSubmit:
			submits++
			if keys[e.IdempotencyKey] {
				t.Errorf("duplicate idempotency key %s", e.IdempotencyKey)
			}
			keys[e.IdempotencyKey] = true
			switch e.Outcome {
			case ledger.OutcomeUnknown:
				unknown++
			case ledger.OutcomeHTTPError:
				httpErr++
				if e.HTTPStatus != http.StatusTooManyRequests || e.ErrorCode != "BACKLOGGED" {
					t.Errorf("http_error entry %+v", e)
				}
			case ledger.OutcomeOK, ledger.OutcomeRejected, ledger.OutcomeTimeout, ledger.OutcomeNotFound:
			}
		case ledger.OpPollFinal:
			finals++
			if e.Stage != ledger.StageLanding || e.Outcome != ledger.OutcomeOK || e.OrderStatus != statusPending || e.OrderID == "" {
				t.Errorf("final entry %+v", e)
			}
		default:
			t.Errorf("unexpected op %s in open mode", e.Op)
		}
	}
	if submits != 60 || finals != 20 || unknown != 20 || httpErr != 20 {
		t.Errorf("ledger: submits %d finals %d unknown %d http_error %d, want 60 20 20 20", submits, finals, unknown, httpErr)
	}
	// 压测期间不轮询：所有查询都发生在收尾阶段，每个订单至少 landAfter 次。
	if got := g.gets.Load(); got != 40 {
		t.Errorf("gets = %d, want 40", got)
	}
}

func TestOpenModeDrainTimeoutIsRecorded(t *testing.T) {
	g := &openGateway{t: t, stop: make(chan struct{}), orders: map[string]*openOrder{}} // 订单永远停在 creating
	srv := g.start()
	cfg := openConfig(srv.URL)
	cfg.Drain = 300 * time.Millisecond

	rep, entries := runLoad(t.Context(), t, cfg)
	d := rep.Open.Drain
	if !d.TimedOut || d.StillCreating != 20 || d.LeftCreating != 0 {
		t.Errorf("drain = %+v, want 20 still creating", d)
	}
	var timeouts int
	for _, e := range entries {
		if e.Op == ledger.OpPollFinal && e.Outcome == ledger.OutcomeTimeout && e.OrderStatus == statusCreating {
			timeouts++
		}
	}
	if timeouts != 20 {
		t.Errorf("timeout entries = %d, want 20", timeouts)
	}
}

func TestOpenModeReportTextAndDropWarning(t *testing.T) {
	g := &openGateway{t: t, stop: make(chan struct{}), landAfter: 1, orders: map[string]*openOrder{}}
	srv := g.start()
	cfg := openConfig(srv.URL)
	cfg.MaxInFlight = 1 // 挂住的请求占着名额，后面的请求发不出去

	rep, _ := runLoad(t.Context(), t, cfg)
	if rep.Open.Dropped == 0 || rep.Open.Sent+rep.Open.Dropped != rep.Open.Planned {
		t.Errorf("sent %d dropped %d planned %d", rep.Open.Sent, rep.Open.Dropped, rep.Open.Planned)
	}
	var sb strings.Builder
	if err := rep.WriteText(&sb); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"latency (from schedule)", "service time (from send)", "NOT SENT", "not measured"} {
		if !strings.Contains(sb.String(), want) {
			t.Errorf("text report lacks %q:\n%s", want, sb.String())
		}
	}
}

func TestValidateOpen(t *testing.T) {
	base := openConfig("http://127.0.0.1:8080")
	if err := base.Validate(); err != nil {
		t.Fatalf("valid open config: %v", err)
	}
	for name, mut := range map[string]func(*Config){
		"no rate":      func(c *Config) { c.Rate = 0 },
		"no duration":  func(c *Config) { c.Duration = 0 },
		"orders":       func(c *Config) { c.Orders = 5 },
		"no max":       func(c *Config) { c.MaxInFlight = 0 },
		"no drain":     func(c *Config) { c.Drain = 0 },
		"unknown mode": func(c *Config) { c.Mode = "half-open" },
	} {
		c := base
		mut(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}
