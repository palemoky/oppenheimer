package runner

// 开环压测：按固定的到达速率发下单请求，不管上一个有没有返回。
//
// 闭环（每个虚拟用户“提交 → 轮询到终态 → 下一单”）下，系统变慢时请求也跟着变少，
// 量不出过载下的行为。开环的到达速率由时间表决定，和系统的响应速度无关，
// 受理端的背压、排队、超时才会如实暴露出来。
//
// 三个要点：
//   - 时间表固定：第 i 个请求计划在 start + i/rate 发出。调度循环落后时按“现在应该已经发出多少个”一次补齐，
//     不用 time.Ticker 逐个 tick（Ticker 落后时会丢 tick，计划数就不准了）。
//   - 在途上限：计划时刻到了而在途请求已达上限，这个请求不发，记为未发出（dropped）。
//     不阻塞等待（那就变回闭环了），也不悄悄跳过。
//   - 延迟从计划发出的时刻算起（见 request 里的说明）。

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
	"github.com/palemoky/oppenheimer/test/load/internal/stats"
)

// clock 是调度循环用的时钟，测试里换成假的。
type clock interface {
	Now() time.Time
	// Sleep 等待 d，ctx 被取消时提前返回 false。
	Sleep(ctx context.Context, d time.Duration) bool
}

type realClock struct{}

func (realClock) Now() time.Time                                  { return time.Now() }
func (realClock) Sleep(ctx context.Context, d time.Duration) bool { return sleep(ctx, d) }

// openReq 是调度循环交给发请求函数的一个请求。
type openReq struct {
	Index   int64     // 从 0 开始
	Planned time.Time // 计划发出的时刻：start + Index/rate
	Sent    time.Time // 实际开始发出的时刻（在请求的 goroutine 里取）
}

// scheduler 是开环的调度循环，和 HTTP 无关：时钟和发请求的函数都可以注入。
type scheduler struct {
	Rate        float64
	Duration    time.Duration
	MaxInFlight int
	Clock       clock
	// Send 发一个请求，在自己的 goroutine 里运行，返回时这个请求不再占用在途名额。
	Send func(ctx context.Context, q openReq)

	planned, sent, dropped atomic.Int64
}

// scheduleResult 是调度循环的结果。
type scheduleResult struct {
	Planned, Sent, Dropped int64
	Start, End             time.Time // End 是最后一个请求发出（或放弃）的时刻，不含等待在途请求
}

// plannedAt 返回第 i 个请求的计划时刻。
func (s *scheduler) plannedAt(start time.Time, i int64) time.Time {
	return start.Add(time.Duration(float64(i) * float64(time.Second) / s.Rate))
}

// run 运行调度循环到时间表发完（或 ctx 被取消），再等所有在途请求返回。
func (s *scheduler) run(ctx context.Context) scheduleResult {
	total := int64(math.Round(s.Rate * s.Duration.Seconds()))
	start := s.Clock.Now()
	var (
		wg       sync.WaitGroup
		inflight atomic.Int64
		next     int64
	)
	for next < total {
		now := s.Clock.Now()
		// 落后时（now 已经越过好几个计划时刻）在这里一次补齐。
		for next < total && !s.plannedAt(start, next).After(now) {
			q := openReq{Index: next, Planned: s.plannedAt(start, next)}
			next++
			s.planned.Add(1)
			if inflight.Load() >= int64(s.MaxInFlight) {
				s.dropped.Add(1)
				continue
			}
			inflight.Add(1)
			s.sent.Add(1)
			wg.Go(func() {
				defer inflight.Add(-1)
				q.Sent = s.Clock.Now()
				s.Send(ctx, q)
			})
		}
		if next >= total {
			break
		}
		if !s.Clock.Sleep(ctx, s.plannedAt(start, next).Sub(now)) {
			break
		}
	}
	end := s.Clock.Now()
	wg.Wait()
	return scheduleResult{Planned: s.planned.Load(), Sent: s.sent.Load(), Dropped: s.dropped.Load(), Start: start, End: end}
}

// 开环下对每个请求的结果分类。
const (
	outAccepted   = "accepted"          // 201/202
	outBacklogged = "backlogged"        // 429 BACKLOGGED：受理端背压，请求没有任何效果
	outRateLimit  = "rate_limited"      // 其他 429：网关限流（RATE_LIMITED）
	outBusiness   = "rejected_business" // 409 库存不足、超出限购
	outTimeout    = "timeout"           // 请求超时，结果未知
	outHTTPError  = "http_error"        // 其他状态码（含 5xx）
	outError      = "error"             // 其他传输错误或响应读不出来，结果未知
)

var openOutcomes = []string{outAccepted, outBacklogged, outRateLimit, outBusiness, outTimeout, outHTTPError, outError}

// errCodeBacklogged 是网关在受理端积压过多时返回的原因码（HTTP 429）。
const errCodeBacklogged = "BACKLOGGED"

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// openRunner 是一次开环压测的状态。
type openRunner struct {
	*runner

	// 四组延迟只统计实际发出且拿到响应的请求；受理的和被拒绝（背压、限流、业务拒绝）的分开。
	acceptSched, acceptSvc stats.Recorder
	rejectSched, rejectSvc stats.Recorder

	mu       sync.Mutex
	outcomes map[string]int64
	perSec   []int64 // 每秒的受理数，按受理返回的时刻分桶
	accepted []*flow
	start    time.Time
}

// request 发一个下单请求，只做这一次 POST，不轮询、不重试。
//
// 延迟有两个口径，必须区分（协调遗漏，coordinated omission）：
//   - 从计划时刻算（latency from schedule）：res.end - Planned。系统卡住 1 秒时，这 1 秒里本该发出的请求
//     因为在途满了或者调度被拖慢，实际发出得晚，它们被耽搁的时间要算进延迟里。只看从发出算起的延迟，
//     就等于让卡顿期间“没来得及发的请求”不参与统计，慢的样本被悄悄丢掉，p99 会好看得多。
//   - 从实际发出算起（service time）：res.end - Sent。纯粹是服务处理一个请求要多久。
//
// 两者之差就是请求在压测机这边排队的时间；差得大说明压测机或系统已经撑不住这个速率。
func (o *openRunner) request(ctx context.Context, q openReq) {
	rng := rand.New(rand.NewPCG(o.cfg.Seed, uint64(q.Index))) //nolint:gosec // 压测用的伪随机数；Index 非负
	f := o.newFlow(rng, 0, int(q.Index)+1)
	f.start, f.attempts = q.Sent, 1

	res := o.do(ctx, http.MethodPost, pathOrders, f.userID, f.key, f.body)
	e := f.entry(ledger.OpSubmit)
	e.Time, e.Attempt, e.HTTPStatus, e.LatencyMicros = res.end, 1, res.status, res.latency.Microseconds()
	e.Error, e.ErrorCode = res.errString(), res.errorCode()

	var out string
	switch {
	case res.err != nil:
		e.Outcome = ledger.OutcomeUnknown
		out = outError
		if isTimeout(res.err) {
			out = outTimeout
		}
	case res.status == http.StatusCreated || res.status == http.StatusAccepted:
		od, err := res.decodeOrder()
		if err != nil {
			// 状态码说受理了但读不出订单：结果未知（和闭环一样）。
			e.Outcome, e.Error, out = ledger.OutcomeUnknown, err.Error(), outError
			break
		}
		f.orderID, f.status = od.OrderID, od.Status
		if f.status == "" {
			f.status = statusCreating
			if res.status == http.StatusCreated {
				f.status = statusPending
			}
		}
		e.OrderID, e.OrderStatus, e.Outcome, out = f.orderID, f.status, ledger.OutcomeOK, outAccepted
	case res.status == http.StatusConflict && submitBusinessCodes[e.ErrorCode]:
		e.Outcome, out = ledger.OutcomeRejected, outBusiness
	case res.status == http.StatusTooManyRequests:
		e.Outcome, out = ledger.OutcomeHTTPError, outRateLimit
		if e.ErrorCode == errCodeBacklogged {
			out = outBacklogged
		}
	default:
		e.Outcome, out = ledger.OutcomeHTTPError, outHTTPError
	}
	o.record(e)

	o.mu.Lock()
	o.outcomes[out]++
	if out == outAccepted {
		o.accepted = append(o.accepted, f)
		sec := min(max(int(res.end.Sub(o.start)/time.Second), 0), len(o.perSec)-1)
		o.perSec[sec]++
	}
	o.mu.Unlock()

	switch out {
	case outAccepted:
		o.acceptSched.Add(res.end.Sub(q.Planned))
		o.acceptSvc.Add(res.end.Sub(q.Sent))
	case outBacklogged, outRateLimit, outBusiness:
		o.rejectSched.Add(res.end.Sub(q.Planned))
		o.rejectSvc.Add(res.end.Sub(q.Sent))
	}
}

// pendingOrder 是收尾阶段还没离开 creating 的受理订单。
type pendingOrder struct {
	f        *flow
	lastHTTP int
	lastErr  string
}

// finalEntry 写订单在收尾阶段的最终账本行：landing 阶段的 poll_final，格式和闭环相同。
func (o *openRunner) finalEntry(f *flow, outcome ledger.Outcome, httpStatus int, at time.Time, errMsg string) {
	e := f.entry(ledger.OpPollFinal)
	e.Stage, e.Time, e.HTTPStatus, e.Outcome, e.Error = ledger.StageLanding, at, httpStatus, outcome, errMsg
	e.LatencyMicros = at.Sub(f.start).Microseconds()
	o.record(e)
}

// drain 对所有受理的订单轮询，直到都离开 creating 或 -drain 到时，并把最终状态写进账本。
// 轮询的结果只用来写账本和计数，不用来量落库延迟：压测期间没有轮询，
// 落库早的订单会被收尾的第一次轮询才看到，量出来的延迟会偏大。
func (o *openRunner) drain(ctx context.Context) DrainReport {
	o.mu.Lock()
	flows := slices.Clone(o.accepted)
	o.mu.Unlock()

	start := time.Now()
	rep := DrainReport{Accepted: int64(len(flows)), FinalStatus: map[string]int64{}}
	var pending []*pendingOrder
	for _, f := range flows {
		if f.status != statusCreating {
			// 201（同步落库）：不需要轮询。
			o.finalEntry(f, ledger.OutcomeOK, http.StatusOK, start, "")
			rep.FinalStatus[f.status]++
			continue
		}
		pending = append(pending, &pendingOrder{f: f})
	}

	dctx, cancel := context.WithTimeout(ctx, o.cfg.Drain)
	defer cancel()
	workers := min(o.cfg.MaxInFlight, 256)
	for len(pending) > 0 {
		pending = o.sweep(dctx, pending, workers, rep.FinalStatus)
		if len(pending) == 0 || !sleep(dctx, o.cfg.PollInterval) {
			break
		}
	}
	rep.Seconds = time.Since(start).Seconds()

	for _, p := range pending {
		outcome, msg := ledger.OutcomeTimeout, p.lastErr
		switch {
		case ctx.Err() != nil:
			outcome, msg = ledger.OutcomeUnknown, "interrupted"
		case p.lastHTTP == http.StatusNotFound:
			outcome = ledger.OutcomeNotFound
			rep.NotFound++
		}
		o.finalEntry(p.f, outcome, p.lastHTTP, time.Now(), msg)
	}
	rep.StillCreating = int64(len(pending)) - rep.NotFound
	rep.TimedOut = len(pending) > 0
	rep.LeftCreating = rep.Accepted - int64(len(pending))
	return rep
}

// sweep 把 pending 里的订单各查一次，返回仍然没离开 creating 的。
func (o *openRunner) sweep(ctx context.Context, pending []*pendingOrder, workers int, final map[string]int64) []*pendingOrder {
	done := make([]bool, len(pending))
	idx := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for range min(workers, len(pending)) {
		wg.Go(func() {
			for i := range idx {
				p := pending[i]
				res := o.getOrder(ctx, p.f)
				p.lastHTTP, p.lastErr = res.status, res.errString()
				if res.err == nil && res.status == http.StatusOK && p.f.status != statusCreating {
					done[i] = true
					o.finalEntry(p.f, ledger.OutcomeOK, res.status, res.end, "")
					mu.Lock()
					final[p.f.status]++
					mu.Unlock()
				}
			}
		})
	}
feed:
	for i := range pending {
		select {
		case idx <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(idx)
	wg.Wait()
	rest := pending[:0]
	for i, p := range pending {
		if !done[i] {
			rest = append(rest, p)
		}
	}
	return rest
}

func (o *openRunner) progressLoop(ctx context.Context, s *scheduler) {
	t := time.NewTicker(o.cfg.ProgressInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			o.mu.Lock()
			acc, back := o.outcomes[outAccepted], o.outcomes[outBacklogged]
			o.mu.Unlock()
			o.log.InfoContext(ctx, "progress", "planned", s.planned.Load(), "sent", s.sent.Load(), "dropped", s.dropped.Load(),
				"accepted", acc, "backlogged", back)
		}
	}
}

// runOpen 是 Run 的开环分支。
func runOpen(ctx context.Context, cfg Config, led *ledger.Writer) (*Report, error) {
	rootCtx, cancelRoot := context.WithCancel(ctx)
	defer cancelRoot()

	startAt := time.Now()
	o := &openRunner{
		runner: &runner{
			cfg:        cfg,
			base:       strings.TrimRight(cfg.BaseURL, "/"),
			client:     cfg.HTTPClient,
			led:        led,
			log:        cfg.Logger,
			agg:        newAggregator(startAt),
			cancelRoot: cancelRoot,
		},
		outcomes: map[string]int64{},
		perSec:   make([]int64, int(math.Ceil(cfg.Duration.Seconds()))),
		start:    startAt,
	}
	o.log.InfoContext(ctx, "load run starting", "mode", ModeOpen, "run_id", cfg.RunID, "seed", cfg.Seed, "rate", cfg.Rate,
		"duration", cfg.Duration, "max_in_flight", cfg.MaxInFlight, "skus", cfg.SKUs)

	s := &scheduler{
		Rate: cfg.Rate, Duration: cfg.Duration, MaxInFlight: cfg.MaxInFlight,
		Clock: realClock{}, Send: o.request,
	}

	var bg sync.WaitGroup
	bgCtx, stopBG := context.WithCancel(ctx)
	bg.Go(func() { o.flushLoop(bgCtx) })
	if cfg.ProgressInterval > 0 {
		bg.Go(func() { o.progressLoop(bgCtx, s) })
	}

	sr := s.run(rootCtx)
	drain := o.drain(rootCtx)
	stopBG()
	bg.Wait()

	report := o.agg.report(time.Now(), &cfg)
	report.Interrupted = ctx.Err() != nil
	report.Open = o.openReport(sr, drain)
	if err := led.Flush(); err != nil {
		return report, fmt.Errorf("flush ledger: %w", err)
	}
	if p := o.ledgerErr.Load(); p != nil {
		return report, *p
	}
	return report, nil
}
