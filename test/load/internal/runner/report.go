package runner

import (
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
	"github.com/palemoky/oppenheimer/test/load/internal/stats"
)

// aggregator 汇总各虚拟用户的结果。一个请求只加一次锁，相比 HTTP 往返可以忽略。
type aggregator struct {
	start time.Time

	acceptLat stats.Recorder
	landLat   stats.Recorder

	mu           sync.Mutex
	requests     map[ledger.Op]map[ledger.Outcome]int64
	orders       map[string]int64
	rejectCodes  map[string]int64
	httpStatuses map[string]int64
	settledBy    map[string]int64
	pollOK       int64
	pollErr      int64
	lastAccept   time.Time
	lastLanded   time.Time
}

func newAggregator(start time.Time) *aggregator {
	return &aggregator{
		start:        start,
		requests:     map[ledger.Op]map[ledger.Outcome]int64{},
		orders:       map[string]int64{},
		rejectCodes:  map[string]int64{},
		httpStatuses: map[string]int64{},
		settledBy:    map[string]int64{},
	}
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func (a *aggregator) request(e ledger.Entry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e.Op == ledger.OpPollFinal {
		return // 轮询的结果按订单计数，不按请求
	}
	m := a.requests[e.Op]
	if m == nil {
		m = map[ledger.Outcome]int64{}
		a.requests[e.Op] = m
	}
	m[e.Outcome]++
	if e.Outcome == ledger.OutcomeHTTPError {
		a.httpStatuses[string(e.Op)+" "+strconv.Itoa(e.HTTPStatus)]++
	}
}

func (a *aggregator) incOrder(name string) {
	a.mu.Lock()
	a.orders[name]++
	a.mu.Unlock()
}

func (a *aggregator) accepted(at time.Time, latency time.Duration, sync bool) {
	a.acceptLat.Add(latency)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.orders["accepted"]++
	if sync {
		a.orders["accepted_sync"]++
	}
	a.lastAccept = later(a.lastAccept, at)
}

func (a *aggregator) rejectedOrder(code string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.orders["rejected"]++
	a.rejectCodes[code]++
}

func (a *aggregator) landed(at time.Time, latency time.Duration) {
	a.landLat.Add(latency)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.orders["landed"]++
	a.lastLanded = later(a.lastLanded, at)
}

func (a *aggregator) poll(ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if ok {
		a.pollOK++
	} else {
		a.pollErr++
	}
}

func (a *aggregator) settled(outcome ledger.Outcome, status string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if outcome != ledger.OutcomeOK {
		a.orders["settle_"+string(outcome)]++
	}
	if status == statusPaid {
		a.orders["paid"]++
	}
	a.settledBy[status]++
}

type progress struct {
	started, accepted, rejected, landed, unknown, httpErrors int64
}

func (a *aggregator) progress() progress {
	a.mu.Lock()
	defer a.mu.Unlock()
	var p progress
	p.started, p.accepted, p.rejected, p.landed = a.orders["started"], a.orders["accepted"], a.orders["rejected"], a.orders["landed"]
	for _, m := range a.requests {
		p.unknown += m[ledger.OutcomeUnknown]
		p.httpErrors += m[ledger.OutcomeHTTPError]
	}
	return p
}

// Latency 是延迟分位数，单位毫秒。
type Latency struct {
	Count  int     `json:"count"`
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P90Ms  float64 `json:"p90_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
}

// Throughput 是一类事件的吞吐：Count 个事件发生在从压测开始起 WindowSeconds 秒内。
type Throughput struct {
	Count         int64   `json:"count"`
	WindowSeconds float64 `json:"window_seconds"`
	PerSecond     float64 `json:"per_second"`
	Latency       Latency `json:"latency"`
}

// Report 是一次压测的汇总。
type Report struct {
	RunID          string    `json:"run_id"`
	Seed           uint64    `json:"seed,string"`
	Concurrency    int       `json:"concurrency"`
	StartedAt      time.Time `json:"started_at"`
	ElapsedSeconds float64   `json:"elapsed_seconds"`
	Interrupted    bool      `json:"interrupted"`

	// Accepted：提交返回 201/202 的订单。延迟是成功的那一次提交请求的耗时。
	Accepted Throughput `json:"accepted"`
	// Landed：订单离开 creating（且不是 failed）。延迟从第一次提交开始算到轮询看到为止，
	// 精度受轮询间隔限制；201 的订单等同于受理。
	Landed Throughput `json:"landed"`
	// DrainSeconds 是最后一个订单落库比最后一个订单受理晚了多久，近似积压排空的时间。
	DrainSeconds float64 `json:"drain_seconds"`

	// Orders 按订单流程计数：started、accepted、accepted_sync、rejected、submit_retried、submit_gave_up、
	// resubmitted、landed、land_failed、land_timeout、land_not_found、land_unknown、abandoned、
	// cancelled、paid、settle_timeout、settle_unknown。
	Orders map[string]int64 `json:"orders"`
	// Requests 按操作和结果分类计数每一个请求（不含轮询）。
	Requests map[string]map[string]int64 `json:"requests"`
	// RejectCodes 是提交被业务拒绝的原因码分布（按订单）。
	RejectCodes map[string]int64 `json:"reject_codes"`
	// HTTPErrors 是 http_error 的分布，键为“操作 状态码”。
	HTTPErrors map[string]int64 `json:"http_errors"`
	// SettledStatus 是支付之后客户端最后看到的订单状态分布。
	SettledStatus map[string]int64 `json:"settled_status"`
	// PollRequests 是查询订单的请求数，PollErrors 是其中没有拿到 200 或 404 的。
	PollRequests int64 `json:"poll_requests"`
	PollErrors   int64 `json:"poll_errors"`
}

func toMs(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Microsecond)) / 1000
}

func latencyOf(s stats.Summary) Latency {
	return Latency{
		Count: s.Count, MeanMs: toMs(s.Mean), P50Ms: toMs(s.P50), P90Ms: toMs(s.P90), P99Ms: toMs(s.P99), MaxMs: toMs(s.Max),
	}
}

func throughputOf(start, last time.Time, s stats.Summary) Throughput {
	t := Throughput{Count: int64(s.Count), Latency: latencyOf(s)}
	if s.Count > 0 && last.After(start) {
		t.WindowSeconds = last.Sub(start).Seconds()
		t.PerSecond = math.Round(float64(s.Count)/t.WindowSeconds*100) / 100
	}
	return t
}

func (a *aggregator) report(end time.Time, cfg *Config) *Report {
	acc, land := a.acceptLat.Summary(), a.landLat.Summary()
	a.mu.Lock()
	defer a.mu.Unlock()
	rep := &Report{
		RunID:          cfg.RunID,
		Seed:           cfg.Seed,
		Concurrency:    cfg.Concurrency,
		StartedAt:      a.start.UTC(),
		ElapsedSeconds: end.Sub(a.start).Seconds(),
		Accepted:       throughputOf(a.start, a.lastAccept, acc),
		Landed:         throughputOf(a.start, a.lastLanded, land),
		Orders:         maps.Clone(a.orders),
		Requests:       map[string]map[string]int64{},
		RejectCodes:    maps.Clone(a.rejectCodes),
		HTTPErrors:     maps.Clone(a.httpStatuses),
		SettledStatus:  maps.Clone(a.settledBy),
		PollRequests:   a.pollOK + a.pollErr,
		PollErrors:     a.pollErr,
	}
	if !a.lastLanded.IsZero() && a.lastLanded.After(a.lastAccept) {
		rep.DrainSeconds = a.lastLanded.Sub(a.lastAccept).Seconds()
	}
	for op, m := range a.requests {
		mm := map[string]int64{}
		for o, n := range m {
			mm[string(o)] = n
		}
		rep.Requests[string(op)] = mm
	}
	return rep
}

// WriteText 把报告以人读的格式写出。
func (r *Report) WriteText(w io.Writer) error {
	p := &printer{w: w}
	p.printf("run %s  seed %d  concurrency %d  elapsed %.1fs", r.RunID, r.Seed, r.Concurrency, r.ElapsedSeconds)
	if r.Interrupted {
		p.printf("  (interrupted)")
	}
	p.printf("\n\n")
	for _, t := range []struct {
		name string
		t    Throughput
	}{{"accepted", r.Accepted}, {"landed", r.Landed}} {
		l := t.t.Latency
		p.printf("%-9s %8d  %9.2f/s  p50 %8.2fms  p90 %8.2fms  p99 %8.2fms  max %8.2fms\n",
			t.name, t.t.Count, t.t.PerSecond, l.P50Ms, l.P90Ms, l.P99Ms, l.MaxMs)
	}
	p.printf("drain     %.3fs (last landing after last acceptance)\n", r.DrainSeconds)
	p.printf("polls     %d (errors %d)\n", r.PollRequests, r.PollErrors)
	p.section("orders", r.Orders)
	for _, op := range slices.Sorted(maps.Keys(r.Requests)) {
		p.section("requests "+op, r.Requests[op])
	}
	p.section("reject codes", r.RejectCodes)
	p.section("http errors", r.HTTPErrors)
	p.section("settled status", r.SettledStatus)
	return p.err
}

type printer struct {
	w   io.Writer
	err error
}

func (p *printer) printf(format string, args ...any) {
	if p.err != nil {
		return
	}
	_, p.err = fmt.Fprintf(p.w, format, args...)
}

func (p *printer) section(title string, m map[string]int64) {
	if len(m) == 0 {
		return
	}
	p.printf("\n%s:\n", title)
	for _, k := range slices.Sorted(maps.Keys(m)) {
		p.printf("  %-20s %d\n", k, m[k])
	}
}
