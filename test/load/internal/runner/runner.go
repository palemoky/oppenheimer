package runner

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/palemoky/Oppenheimer/test/load/internal/ledger"
)

// action 是订单落库后虚拟用户要做的事，在提交之前就抽好，保证同一个种子的动作序列不受结果影响。
type action int

const (
	actionAbandon action = iota
	actionPay
	actionCancel
)

// flow 是一个订单流程的状态，只属于一个虚拟用户的 goroutine。
type flow struct {
	key      string
	userID   int64
	skuID    int64
	quantity int32
	action   action
	body     []byte

	start    time.Time // 第一次提交的开始时间
	attempts int       // 已经发出的提交请求数
	accepted bool      // 是否已经计入受理
	orderID  string
	status   string // 最后看到的订单状态
}

func (f *flow) entry(op ledger.Op) ledger.Entry {
	return ledger.Entry{
		Op:             op,
		OrderID:        f.orderID,
		UserID:         f.userID,
		SKUID:          f.skuID,
		Quantity:       f.quantity,
		IdempotencyKey: f.key,
		OrderStatus:    f.status,
	}
}

type runner struct {
	cfg    Config
	base   string
	client *http.Client
	led    *ledger.Writer
	log    *slog.Logger
	agg    *aggregator
	issued atomic.Int64 // 按订单数结束时已经发出的订单号码

	ledgerErr  atomic.Pointer[error]
	cancelRoot context.CancelFunc
}

// Run 执行一次压测，返回汇总的报告。ctx 被取消（例如 Ctrl-C）时尽快停止，在途请求记为 unknown，
// 报告的 Interrupted 为 true。账本由调用方创建和关闭。
//
// 返回的错误只来自参数非法或账本写入失败；服务端的任何故障都只体现在报告和账本里。
func Run(ctx context.Context, cfg Config, led *ledger.Writer) (*Report, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Seed == 0 {
		cfg.Seed = rand.Uint64() //nolint:gosec // 压测用的伪随机数，不涉及安全
	}
	if cfg.RunID == "" {
		cfg.RunID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = newHTTPClient(cfg.Concurrency)
	}

	rootCtx, cancelRoot := context.WithCancel(ctx)
	defer cancelRoot()
	// 提交窗口：到时后不再开始新订单，但在途的订单继续轮询，这样才能测出积压排空的时间。
	windowCtx := rootCtx
	if cfg.Duration > 0 {
		var cancelWindow context.CancelFunc
		windowCtx, cancelWindow = context.WithTimeout(rootCtx, cfg.Duration)
		defer cancelWindow()
	}

	r := &runner{
		cfg:        cfg,
		base:       strings.TrimRight(cfg.BaseURL, "/"),
		client:     cfg.HTTPClient,
		led:        led,
		log:        cfg.Logger,
		agg:        newAggregator(time.Now()),
		cancelRoot: cancelRoot,
	}
	r.log.Info("load run starting", "run_id", cfg.RunID, "seed", cfg.Seed, "concurrency", cfg.Concurrency,
		"duration", cfg.Duration, "orders", cfg.Orders, "skus", cfg.SKUs)

	var bg sync.WaitGroup
	bgCtx, stopBG := context.WithCancel(ctx)
	bg.Go(func() { r.flushLoop(bgCtx) })
	if cfg.ProgressInterval > 0 {
		bg.Go(func() { r.progressLoop(bgCtx) })
	}

	var vus sync.WaitGroup
	for vu := range cfg.Concurrency {
		vus.Go(func() { r.vuser(rootCtx, windowCtx, vu) })
	}
	vus.Wait()
	stopBG()
	bg.Wait()

	report := r.agg.report(time.Now(), &cfg)
	report.Interrupted = ctx.Err() != nil
	if err := led.Flush(); err != nil {
		return report, err
	}
	if p := r.ledgerErr.Load(); p != nil {
		return report, *p
	}
	return report, nil
}

func (r *runner) flushLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.led.Flush(); err != nil {
				r.failLedger(err)
				return
			}
		}
	}
}

func (r *runner) progressLoop(ctx context.Context) {
	t := time.NewTicker(r.cfg.ProgressInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p := r.agg.progress()
			r.log.Info("progress", "started", p.started, "accepted", p.accepted, "rejected", p.rejected,
				"landed", p.landed, "unknown", p.unknown, "http_errors", p.httpErrors)
		}
	}
}

// failLedger 记下第一个账本错误并停止压测：账本不完整时对账没有意义。
func (r *runner) failLedger(err error) {
	if r.ledgerErr.CompareAndSwap(nil, &err) {
		r.log.Error("ledger write failed, stopping", "error", err)
		r.cancelRoot()
	}
}

// nextOrder 报告是否还应当开始一个新订单。
func (r *runner) nextOrder(windowCtx context.Context) bool {
	if windowCtx.Err() != nil {
		return false
	}
	if r.cfg.Orders > 0 {
		return r.issued.Add(1) <= r.cfg.Orders
	}
	return true
}

func (r *runner) vuser(ctx, windowCtx context.Context, vu int) {
	rng := rand.New(rand.NewPCG(r.cfg.Seed, uint64(vu))) //nolint:gosec // 压测用的伪随机数；vu 非负
	for seq := 1; r.nextOrder(windowCtx); seq++ {
		f := r.newFlow(rng, vu, seq)
		r.runFlow(ctx, f)
		if ctx.Err() != nil {
			return
		}
	}
}

func (r *runner) newFlow(rng *rand.Rand, vu, seq int) *flow {
	f := &flow{
		key:      fmt.Sprintf("%s-%d-%d", r.cfg.RunID, vu, seq),
		userID:   1 + rng.Int64N(r.cfg.Users),
		skuID:    r.cfg.SKUs[rng.IntN(len(r.cfg.SKUs))],
		quantity: r.cfg.Quantity,
	}
	switch x := rng.Float64(); {
	case x < r.cfg.PayRatio:
		f.action = actionPay
	case x < r.cfg.PayRatio+r.cfg.CancelRatio:
		f.action = actionCancel
	default:
		f.action = actionAbandon
	}
	body, err := json.Marshal(createOrderRequest{Items: []createOrderItem{{SKUID: f.skuID, Quantity: f.quantity}}})
	if err != nil {
		panic(fmt.Sprintf("encode create order request: %v", err)) // 固定结构，不可能失败
	}
	f.body = body
	return f
}

func (r *runner) runFlow(ctx context.Context, f *flow) {
	r.agg.incOrder("started")
	f.start = time.Now()
	if !r.submit(ctx, f) {
		return
	}
	if !r.land(ctx, f) {
		return
	}
	switch f.action {
	case actionPay:
		r.pay(ctx, f)
	case actionCancel:
		r.cancel(ctx, f)
	default:
		r.agg.incOrder("abandoned")
	}
}

// record 写一行账本并计数。
func (r *runner) record(e ledger.Entry) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	r.agg.request(e)
	if err := r.led.Write(e); err != nil {
		r.failLedger(err)
	}
}

// submit 提交订单，遇到结果未知、5xx、429 时用同一个幂等键重试，直到受理、被拒绝或次数用完。
// 返回是否受理。
func (r *runner) submit(ctx context.Context, f *flow) bool {
	for {
		f.attempts++
		res := r.do(ctx, http.MethodPost, pathOrders, f.userID, f.key, f.body)
		e := f.entry(ledger.OpSubmit)
		e.Time, e.Attempt, e.HTTPStatus, e.LatencyMicros = res.end, f.attempts, res.status, res.latency.Microseconds()
		e.Error, e.ErrorCode = res.errString(), res.errorCode()

		retry := false
		switch {
		case res.err != nil:
			e.Outcome = ledger.OutcomeUnknown
			retry = true
		case res.status == http.StatusCreated || res.status == http.StatusAccepted:
			o, err := res.decodeOrder()
			if err != nil {
				// 状态码说受理了，但读不出订单：当作结果未知，用同一个幂等键再提交一次就能拿到订单。
				e.Outcome, e.Error = ledger.OutcomeUnknown, err.Error()
				retry = true
				break
			}
			f.orderID, f.status = o.OrderID, o.Status
			if f.status == "" {
				// 响应里缺状态时按状态码推断：201 已落库，202 只是受理。
				f.status = statusCreating
				if res.status == http.StatusCreated {
					f.status = statusPending
				}
			}
			e.OrderID, e.OrderStatus, e.Outcome = f.orderID, f.status, ledger.OutcomeOK
		case res.status == http.StatusConflict && submitBusinessCodes[e.ErrorCode]:
			e.Outcome = ledger.OutcomeRejected
		default:
			e.Outcome = ledger.OutcomeHTTPError
			retry = res.status >= 500 || res.status == http.StatusTooManyRequests
		}
		r.record(e)

		switch {
		case e.Outcome == ledger.OutcomeOK:
			if !f.accepted {
				f.accepted = true
				r.agg.accepted(res.end, res.latency, res.status == http.StatusCreated)
			}
			return true
		case e.Outcome == ledger.OutcomeRejected:
			if !f.accepted {
				r.agg.rejectedOrder(e.ErrorCode)
			}
			sleep(ctx, r.cfg.RejectBackoff)
			return false
		case !retry || f.attempts >= r.cfg.SubmitAttempts || ctx.Err() != nil:
			if !f.accepted {
				r.agg.incOrder("submit_gave_up")
			}
			return false
		}
		if !sleep(ctx, r.cfg.RetryBackoff) {
			if !f.accepted {
				r.agg.incOrder("submit_gave_up")
			}
			return false
		}
		r.agg.incOrder("submit_retried")
	}
}

// land 轮询到订单离开 creating，写一行 landing 阶段的 poll_final。返回订单是否进入 pending_payment。
//
// 查询返回 404 表示受理后订单丢了（见 OpenAPI 的说明），用同一个幂等键重新提交。
func (r *runner) land(ctx context.Context, f *flow) bool {
	if f.status != statusCreating {
		// 201（同步落库）或幂等重放返回了已经落库的订单，不需要轮询。
		return r.finishLanding(f, ledger.OutcomeOK, http.StatusOK, time.Now(), "")
	}
	deadline := time.Now().Add(r.cfg.PollTimeout)
	lastHTTP := 0
	var lastErr string
	for {
		if !sleep(ctx, r.cfg.PollInterval) {
			return r.finishLanding(f, ledger.OutcomeUnknown, lastHTTP, time.Now(), "interrupted")
		}
		res := r.getOrder(ctx, f)
		lastHTTP, lastErr = res.status, res.errString()
		switch {
		case res.err == nil && res.status == http.StatusOK:
			if f.status != statusCreating {
				return r.finishLanding(f, ledger.OutcomeOK, res.status, res.end, "")
			}
		case res.err == nil && res.status == http.StatusNotFound:
			if f.attempts >= r.cfg.SubmitAttempts {
				return r.finishLanding(f, ledger.OutcomeNotFound, res.status, res.end, "")
			}
			r.agg.incOrder("resubmitted")
			if !r.submit(ctx, f) {
				return r.finishLanding(f, ledger.OutcomeNotFound, res.status, time.Now(), "resubmit did not succeed")
			}
			if f.status != statusCreating {
				return r.finishLanding(f, ledger.OutcomeOK, http.StatusOK, time.Now(), "")
			}
		}
		if ctx.Err() != nil {
			return r.finishLanding(f, ledger.OutcomeUnknown, lastHTTP, time.Now(), "interrupted")
		}
		if time.Now().After(deadline) {
			return r.finishLanding(f, ledger.OutcomeTimeout, lastHTTP, time.Now(), lastErr)
		}
	}
}

func (r *runner) finishLanding(f *flow, outcome ledger.Outcome, httpStatus int, at time.Time, errMsg string) bool {
	e := f.entry(ledger.OpPollFinal)
	e.Stage, e.Time, e.HTTPStatus, e.Outcome, e.Error = ledger.StageLanding, at, httpStatus, outcome, errMsg
	e.LatencyMicros = at.Sub(f.start).Microseconds()
	r.record(e)

	switch {
	case outcome != ledger.OutcomeOK:
		r.agg.incOrder("land_" + string(outcome))
		return false
	case f.status == statusFailed:
		r.agg.incOrder("land_failed")
		return false
	default:
		r.agg.landed(at, at.Sub(f.start))
		return f.status == statusPending
	}
}

// getOrder 查询一次订单，并在拿到 200 时更新 f.status。
func (r *runner) getOrder(ctx context.Context, f *flow) result {
	res := r.do(ctx, http.MethodGet, pathOrders+"/"+f.orderID, f.userID, "", nil)
	ok := false
	if res.err == nil && res.status == http.StatusOK {
		o, err := res.decodeOrder()
		switch {
		case err != nil:
			res.err = err
		case o.Status == "":
			res.err = errors.New("polled order has no status")
		default:
			f.status = o.Status
			ok = true
		}
	}
	r.agg.poll(ok || (res.err == nil && res.status == http.StatusNotFound))
	return res
}

// pay 发起支付，然后轮询到订单进入终态。支付请求结果未知或 5xx 时同样去轮询：支付可能已经发生。
func (r *runner) pay(ctx context.Context, f *flow) {
	res := r.do(ctx, http.MethodPost, pathOrders+"/"+f.orderID+"/pay", f.userID, "", nil)
	e := f.entry(ledger.OpPay)
	e.Time, e.HTTPStatus, e.LatencyMicros = res.end, res.status, res.latency.Microseconds()
	e.Error, e.ErrorCode = res.errString(), res.errorCode()
	switch {
	case res.err != nil:
		e.Outcome = ledger.OutcomeUnknown
	case res.status == http.StatusAccepted:
		e.Outcome = ledger.OutcomeOK
	case res.status == http.StatusConflict:
		e.Outcome = ledger.OutcomeRejected
	default:
		e.Outcome = ledger.OutcomeHTTPError
	}
	r.record(e)
	if e.Outcome == ledger.OutcomeRejected || ctx.Err() != nil {
		return
	}
	r.settle(ctx, f)
}

// settle 轮询到订单进入终态，写一行 settle 阶段的 poll_final。
func (r *runner) settle(ctx context.Context, f *flow) {
	deadline := time.Now().Add(r.cfg.PollTimeout)
	lastHTTP := 0
	var lastErr string
	outcome := ledger.OutcomeTimeout
	for {
		if !sleep(ctx, r.cfg.PollInterval) {
			outcome, lastErr = ledger.OutcomeUnknown, "interrupted"
			break
		}
		res := r.getOrder(ctx, f)
		lastHTTP, lastErr = res.status, res.errString()
		if res.err == nil && res.status == http.StatusOK && terminal(f.status) {
			outcome, lastErr = ledger.OutcomeOK, ""
			break
		}
		if ctx.Err() != nil {
			outcome, lastErr = ledger.OutcomeUnknown, "interrupted"
			break
		}
		if time.Now().After(deadline) {
			break
		}
	}
	at := time.Now()
	e := f.entry(ledger.OpPollFinal)
	e.Stage, e.Time, e.HTTPStatus, e.Outcome, e.Error = ledger.StageSettle, at, lastHTTP, outcome, lastErr
	e.LatencyMicros = at.Sub(f.start).Microseconds()
	r.record(e)
	r.agg.settled(outcome, f.status)
}

// cancel 取消订单。
func (r *runner) cancel(ctx context.Context, f *flow) {
	res := r.do(ctx, http.MethodPost, pathOrders+"/"+f.orderID+"/cancel", f.userID, "", nil)
	e := f.entry(ledger.OpCancel)
	e.Time, e.HTTPStatus, e.LatencyMicros = res.end, res.status, res.latency.Microseconds()
	e.Error, e.ErrorCode = res.errString(), res.errorCode()
	switch {
	case res.err != nil:
		e.Outcome = ledger.OutcomeUnknown
	case res.status == http.StatusOK:
		e.Outcome = ledger.OutcomeOK
		if o, err := res.decodeOrder(); err == nil && o.Status != "" {
			f.status = o.Status
			e.OrderStatus = f.status
		}
	case res.status == http.StatusConflict:
		e.Outcome = ledger.OutcomeRejected
	default:
		e.Outcome = ledger.OutcomeHTTPError
	}
	r.record(e)
	if e.Outcome == ledger.OutcomeOK {
		r.agg.incOrder("cancelled")
	}
}

// sleep 等待 d，ctx 被取消时提前返回 false。
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
