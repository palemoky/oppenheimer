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

	// Open 只在开环模式下有，闭环的报告里不出现。
	Open *OpenReport `json:"open,omitempty"`
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
	if r.Open != nil {
		p.printf("run %s  seed %d  mode open  elapsed %.1fs", r.RunID, r.Seed, r.ElapsedSeconds)
		if r.Interrupted {
			p.printf("  (interrupted)")
		}
		p.printf("\n\n")
		r.Open.writeText(p)
		p.printf("polls     %d (errors %d)\n", r.PollRequests, r.PollErrors)
		for _, op := range slices.Sorted(maps.Keys(r.Requests)) {
			p.section("requests "+op, r.Requests[op])
		}
		return p.err
	}
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

// DrainReport 是开环收尾阶段的结果：对所有受理的订单轮询，直到都离开 creating 或 -drain 到时。
//
// 落库延迟没有量：订单的响应只有 create_time 和 expire_time，没有落库（状态离开 creating）的时刻，
// 压测期间又不轮询，收尾阶段第一次看到的时间会比真实落库晚得多。这里只报告收尾用了多久、
// 有多少订单在 -drain 内没有离开 creating。
type DrainReport struct {
	Accepted      int64            `json:"accepted"`
	LeftCreating  int64            `json:"left_creating"`  // 收尾阶段看到已离开 creating 的订单数（含同步落库的）
	StillCreating int64            `json:"still_creating"` // -drain 到时仍在 creating（或一直没拿到响应）
	NotFound      int64            `json:"not_found"`      // 最后一次查询是 404
	TimedOut      bool             `json:"timed_out"`
	Seconds       float64          `json:"seconds"`
	FinalStatus   map[string]int64 `json:"final_status"`
	// LandingLatencyMeasured 恒为 false：见类型说明。
	LandingLatencyMeasured bool `json:"landing_latency_measured"`
}

// PerSecond 是每秒受理数的时间序列和它的最小值、中位数、最大值。
type PerSecond struct {
	Min    int64   `json:"min"`
	Median float64 `json:"median"`
	Max    int64   `json:"max"`
	Series []int64 `json:"series"`
}

// OpenReport 是开环模式特有的汇总。
type OpenReport struct {
	Rate            float64 `json:"rate"`
	DurationSeconds float64 `json:"duration_seconds"`
	MaxInFlight     int     `json:"max_in_flight"`
	Planned         int64   `json:"planned"`
	Sent            int64   `json:"sent"`
	Dropped         int64   `json:"dropped"`
	// SendWindowSeconds 是调度循环从开始到发完（或放弃）最后一个请求的时间，SentPerSecond 是 Sent 除以它。
	SendWindowSeconds float64 `json:"send_window_seconds"`
	SentPerSecond     float64 `json:"sent_per_second"`
	// Outcomes 是实际发出的请求按结果的分类计数。
	Outcomes map[string]int64 `json:"outcomes"`
	// 从计划时刻算的延迟（含请求在压测机这边被耽搁的时间）和从实际发出算的延迟（service time）。
	// 只统计实际发出且拿到响应的请求；受理的和被拒绝（背压、限流、业务拒绝）的分开。
	AcceptedFromSchedule Latency     `json:"accepted_from_schedule"`
	AcceptedServiceTime  Latency     `json:"accepted_service_time"`
	RejectedFromSchedule Latency     `json:"rejected_from_schedule"`
	RejectedServiceTime  Latency     `json:"rejected_service_time"`
	AcceptedPerSecond    PerSecond   `json:"accepted_per_second"`
	Drain                DrainReport `json:"drain"`
}

func (o *openRunner) openReport(sr scheduleResult, drain DrainReport) *OpenReport {
	o.mu.Lock()
	outcomes := maps.Clone(o.outcomes)
	series := slices.Clone(o.perSec)
	o.mu.Unlock()
	rep := &OpenReport{
		Rate:                 o.cfg.Rate,
		DurationSeconds:      o.cfg.Duration.Seconds(),
		MaxInFlight:          o.cfg.MaxInFlight,
		Planned:              sr.Planned,
		Sent:                 sr.Sent,
		Dropped:              sr.Dropped,
		SendWindowSeconds:    sr.End.Sub(sr.Start).Seconds(),
		Outcomes:             outcomes,
		AcceptedFromSchedule: latencyOf(o.acceptSched.Summary()),
		AcceptedServiceTime:  latencyOf(o.acceptSvc.Summary()),
		RejectedFromSchedule: latencyOf(o.rejectSched.Summary()),
		RejectedServiceTime:  latencyOf(o.rejectSvc.Summary()),
		AcceptedPerSecond:    perSecondOf(series),
		Drain:                drain,
	}
	if rep.SendWindowSeconds > 0 {
		rep.SentPerSecond = math.Round(float64(sr.Sent)/rep.SendWindowSeconds*100) / 100
	}
	return rep
}

func perSecondOf(series []int64) PerSecond {
	p := PerSecond{Series: series}
	if len(series) == 0 {
		return p
	}
	sorted := slices.Sorted(slices.Values(series))
	p.Min, p.Max = sorted[0], sorted[len(sorted)-1]
	if n := len(sorted); n%2 == 1 {
		p.Median = float64(sorted[n/2])
	} else {
		p.Median = float64(sorted[n/2-1]+sorted[n/2]) / 2
	}
	return p
}

func (o *OpenReport) writeText(p *printer) {
	p.printf("open loop: planned rate %.0f/s for %.0fs, max in flight %d\n", o.Rate, o.DurationSeconds, o.MaxInFlight)
	p.printf("  planned %d  sent %d  dropped %d  actual send rate %.2f/s\n", o.Planned, o.Sent, o.Dropped, o.SentPerSecond)
	p.printf("\noutcomes (of %d sent):\n", o.Sent)
	for _, k := range openOutcomes {
		n := o.Outcomes[k]
		if n == 0 && k != outAccepted {
			continue
		}
		pct := 0.0
		if o.Sent > 0 {
			pct = float64(n) / float64(o.Sent) * 100
		}
		p.printf("  %-20s %8d  %6.2f%%\n", k, n, pct)
	}
	p.printf("\n")
	for _, l := range []struct {
		name string
		l    Latency
	}{
		{"accepted latency (from schedule)", o.AcceptedFromSchedule},
		{"accepted service time (from send)", o.AcceptedServiceTime},
		{"rejected latency (from schedule)", o.RejectedFromSchedule},
		{"rejected service time (from send)", o.RejectedServiceTime},
	} {
		p.printf("%-35s n %7d  p50 %8.2fms  p90 %8.2fms  p99 %8.2fms  max %8.2fms\n",
			l.name, l.l.Count, l.l.P50Ms, l.l.P90Ms, l.l.P99Ms, l.l.MaxMs)
	}
	ps := o.AcceptedPerSecond
	p.printf("\naccepted per second: min %d  median %.1f  max %d (over %d s)\n", ps.Min, ps.Median, ps.Max, len(ps.Series))
	d := o.Drain
	p.printf("\ndrain: %.2fs, %d accepted, %d left creating, %d still creating, %d not found", d.Seconds, d.Accepted, d.LeftCreating, d.StillCreating, d.NotFound)
	if d.TimedOut {
		p.printf(" (hit -drain)")
	}
	p.printf("\n  landing latency: not measured (the order response carries no landing time; polling at drain time would overstate it)\n")
	if len(d.FinalStatus) > 0 {
		p.section("final status", d.FinalStatus)
	}
	if o.Dropped > 0 {
		p.printf("\n!!! %d of %d planned requests were NOT SENT (in-flight limit %d reached): the load generator is the bottleneck or the system is stuck.\n",
			o.Dropped, o.Planned, o.MaxInFlight)
	}
}
