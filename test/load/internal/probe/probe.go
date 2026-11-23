// Package probe 实现 load probe：常驻在集群里的低速探针，每秒约一次从入口走一遍
// “查库存 → 下单 → 等落库 → 支付（或取消）→ 等已支付”，穿插查订单列表，把每一步的结果和耗时报成指标。
//
// 它给 SLO 提供连续的数据（没有压测时也有序列，不会出现 0 除以 0），并让“下单这条路断了”在没人压测时也能被发现。
// 设计见 docs/design/3c-slo-alerting.md 的 3.6 节。
//
// 探针只走 HTTP 入口，不读写数据库、不调 gRPC；SKU 的建立和补库存不归它管。
// 除了轮询以外不重试：一次失败就记失败，下一个周期用新的幂等键，探针量的是“用户第一次尝试的结果”。
package probe

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/gwapi"
)

// LoadRun 是探针请求的 X-Load-Run 头。
const LoadRun = "probe"

// keyPrefix 是幂等键的前缀，库里按它能把探针的订单挑出来。
const keyPrefix = "probe-"

// 压测流量的用户号段是 [2^39, 2^40)：对账把 >= 2^40 的当作造数排除（test/k6/lib/config.js）。
// k6 默认用 2^39 起的一百万个，探针默认用号段顶上的 2^20 个，互不重叠。
const (
	UserFloor        int64 = 1 << 39
	UserLimit        int64 = 1 << 40
	DefaultUserCount int64 = 1 << 20
	DefaultUserBase        = UserLimit - DefaultUserCount
)

// payBody 指定模拟支付成功：探针要的是“支付这条路通不通”，结果不能随支付服务的默认值变。
var payBody = []byte(`{"outcome":"success"}`)

// Config 是探针的参数。
type Config struct {
	// BaseURL 是入口地址，Host 不为空时作为 Host 头。
	BaseURL string
	Host    string
	// Interval 是两个周期开始的间隔。节拍是开环的：上一个周期没做完不推迟下一个，
	// 否则系统变慢时探针也跟着变慢，正好在最需要数据的时候数据变少。
	Interval time.Duration
	// MaxInFlight 是同时在途的周期数上限，到了上限这一拍跳过（计入 skipped）。
	// 系统卡住时每个周期要等到时限才结束，不设上限的话探针自己会堆出几百个 goroutine 和连接。
	MaxInFlight int
	// RequestTimeout 是单个请求的超时。
	RequestTimeout time.Duration
	// LandTimeout、PayTimeout 是等落库、等已支付的时限。
	LandTimeout time.Duration
	PayTimeout  time.Duration
	// CancelEvery 个周期里有一个走取消而不是支付，0 表示从不取消。ListEvery 个周期里有一个查订单列表，0 表示从不查。
	CancelEvery int
	ListEvery   int
	// SKUs 轮流使用。
	SKUs []int64
	// 用户号是 [UserBase, UserBase+UserCount) 里轮流的，必须落在压测号段里。
	UserBase  int64
	UserCount int64
	// Drain 是停止后等在途周期结束的上限，到时中断剩下的。
	Drain time.Duration
	// SummaryInterval 是打汇总日志的间隔，0 表示不打。
	SummaryInterval time.Duration
	// PollInitial、PollMax 是轮询的首次间隔和最大间隔（间隔每次翻倍），零值取 OpenAPI 建议的 200 毫秒和 2 秒。
	PollInitial time.Duration
	PollMax     time.Duration

	// Logger 为 nil 时用 slog.Default()。HTTPClient 为 nil 时按 MaxInFlight 新建。
	Logger     *slog.Logger
	HTTPClient *http.Client
}

// Validate 检查参数。
func (c *Config) Validate() error {
	u, err := url.Parse(c.BaseURL)
	if err != nil {
		return fmt.Errorf("invalid gateway address %q: %w", c.BaseURL, err)
	}
	var errs []error
	if u.Scheme != "http" && u.Scheme != "https" || u.Host == "" {
		errs = append(errs, fmt.Errorf("invalid gateway address %q: want http(s)://host:port", c.BaseURL))
	}
	if c.Interval <= 0 || c.RequestTimeout <= 0 || c.LandTimeout <= 0 || c.PayTimeout <= 0 || c.Drain <= 0 {
		errs = append(errs, errors.New("interval, request timeout, land timeout, pay timeout and drain must be > 0"))
	}
	if c.MaxInFlight < 1 {
		errs = append(errs, errors.New("max in flight must be >= 1"))
	}
	if c.CancelEvery < 0 || c.ListEvery < 0 {
		errs = append(errs, errors.New("cancel-every and list-every must not be negative"))
	}
	if c.SummaryInterval < 0 || c.PollInitial < 0 || c.PollMax < 0 {
		errs = append(errs, errors.New("summary interval and poll intervals must not be negative"))
	}
	if len(c.SKUs) == 0 {
		errs = append(errs, errors.New("at least one sku is required"))
	}
	for _, s := range c.SKUs {
		if s <= 0 {
			errs = append(errs, fmt.Errorf("invalid sku id %d", s))
		}
	}
	if c.UserCount < 1 || c.UserBase < UserFloor || c.UserBase > UserLimit-c.UserCount {
		errs = append(errs, fmt.Errorf("users [%d, %d+%d) must lie in the load range [2^39, 2^40)", c.UserBase, c.UserBase, c.UserCount))
	}
	return errors.Join(errs...)
}

type prober struct {
	cfg    Config
	client gwapi.Client
	log    *slog.Logger
	runID  string

	inFlight atomic.Int64
	// interval 和 total 按周期结果计数，给汇总日志用；指标另外记。
	interval [numCycleOutcomes]atomic.Int64
	total    [numCycleOutcomes]atomic.Int64
}

// Run 运行探针直到 ctx 被取消，然后等在途的周期结束（最多 Drain），返回 nil。
// 只有参数非法时返回错误：入口的任何故障都只体现在指标和日志里。
func Run(ctx context.Context, cfg Config) error {
	p, err := newProber(cfg)
	if err != nil {
		return err
	}
	p.run(ctx)
	return nil
}

func newProber(cfg Config) (*prober, error) {
	if cfg.PollInitial == 0 {
		cfg.PollInitial = 200 * time.Millisecond
	}
	if cfg.PollMax == 0 {
		cfg.PollMax = 2 * time.Second
	}
	cfg.PollMax = max(cfg.PollMax, cfg.PollInitial)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HTTPClient == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		// 每个周期同一时刻只有一个请求在途。
		t.MaxIdleConns = cfg.MaxInFlight
		t.MaxIdleConnsPerHost = cfg.MaxInFlight
		cfg.HTTPClient = &http.Client{Transport: t}
	}
	p := &prober{
		cfg: cfg,
		client: gwapi.Client{
			HTTP:    cfg.HTTPClient,
			Base:    strings.TrimRight(cfg.BaseURL, "/"),
			Host:    cfg.Host,
			LoadRun: LoadRun,
			Timeout: cfg.RequestTimeout,
		},
		log: cfg.Logger,
		// 进幂等键：重启后的序号从 1 重来，靠启动时刻区分，不会撞上上一次的键。
		runID: strconv.FormatInt(time.Now().UnixNano(), 36),
	}
	return p, nil
}

func (p *prober) run(ctx context.Context) {
	cfg := p.cfg
	p.log.InfoContext(ctx, "probe starting", "run_id", p.runID, "addr", cfg.BaseURL, "host", cfg.Host,
		"interval", cfg.Interval.String(), "max_in_flight", cfg.MaxInFlight, "skus", cfg.SKUs,
		"user_base", cfg.UserBase, "user_count", cfg.UserCount)

	// 周期用的 ctx 不跟着停止信号取消：停止时让在途的周期做完，到 Drain 才中断。
	cycleCtx, abort := context.WithCancel(context.WithoutCancel(ctx))
	defer abort()

	var bg sync.WaitGroup
	if cfg.SummaryInterval > 0 {
		bg.Go(func() { p.summaryLoop(ctx) })
	}

	var cycles sync.WaitGroup
	t := time.NewTicker(cfg.Interval)
	var seq int64
	for running := true; running; {
		if p.inFlight.Load() >= int64(cfg.MaxInFlight) {
			p.countCycle(ctx, cycleSkipped)
		} else {
			seq++
			n := seq
			p.inFlight.Add(1)
			cycles.Go(func() {
				defer p.inFlight.Add(-1)
				p.cycle(cycleCtx, n)
			})
		}
		select {
		case <-ctx.Done():
			running = false
		case <-t.C:
		}
	}
	t.Stop()

	done := make(chan struct{})
	go func() {
		cycles.Wait()
		close(done)
	}()
	drain := time.NewTimer(cfg.Drain)
	defer drain.Stop()
	aborted := false
	select {
	case <-done:
	case <-drain.C:
		aborted = true
		abort()
		<-done
	}
	bg.Wait()
	p.log.InfoContext(ctx, "probe stopped", "aborted_in_flight", aborted, "cycles_ok", p.total[cycleOK].Load(),
		"cycles_incomplete", p.total[cycleIncomplete].Load(), "cycles_failed", p.total[cycleFailed].Load(),
		"cycles_skipped", p.total[cycleSkipped].Load())
}

func (p *prober) countCycle(ctx context.Context, c cycleOutcome) {
	cyclesTotal.Add(ctx, 1, cycleOpts[c])
	p.interval[c].Add(1)
	p.total[c].Add(1)
}

func (p *prober) summaryLoop(ctx context.Context) {
	t := time.NewTicker(p.cfg.SummaryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.log.InfoContext(ctx, "probe summary", "interval", p.cfg.SummaryInterval.String(),
				"ok", p.interval[cycleOK].Swap(0), "incomplete", p.interval[cycleIncomplete].Swap(0),
				"failed", p.interval[cycleFailed].Swap(0), "skipped", p.interval[cycleSkipped].Swap(0),
				"in_flight", p.inFlight.Load())
		}
	}
}

// cycle 是一个周期的状态，只属于一个 goroutine。
type cycle struct {
	seq     int64
	userID  int64
	skuID   int64
	key     string
	orderID string
	status  string // 最后看到的订单状态
	// since 是上一步成功返回的时刻，等待的两步从这里算耗时。
	since time.Time
	// worst 是这个周期到目前为止最坏的结果：有 failed 的是 failed，否则有非 ok 的是 incomplete。
	worst cycleOutcome
}

func (p *prober) cycle(ctx context.Context, seq int64) {
	i := seq - 1
	c := &cycle{
		seq:    seq,
		userID: p.cfg.UserBase + i%p.cfg.UserCount,
		skuID:  p.cfg.SKUs[i%int64(len(p.cfg.SKUs))],
		key:    keyPrefix + p.runID + "-" + strconv.FormatInt(seq, 10),
	}
	// 查库存和下单互不依赖：库存查询坏了照样下单，两条路分别报。
	p.stock(ctx, c)
	if p.create(ctx, c) && p.land(ctx, c) {
		if p.cfg.CancelEvery > 0 && seq%int64(p.cfg.CancelEvery) == 0 {
			p.cancel(ctx, c)
		} else if p.pay(ctx, c) {
			p.paid(ctx, c)
		}
	}
	if p.cfg.ListEvery > 0 && seq%int64(p.cfg.ListEvery) == 0 {
		p.list(ctx, c)
	}
	if ctx.Err() != nil {
		// 停止时超过 Drain 被中断的周期不计：它的失败是探针自己造成的。
		return
	}
	p.countCycle(ctx, c.worst)
}

// stepResult 是一步的结果，给记指标和日志用。
type stepResult struct {
	outcome  outcome
	duration time.Duration
	status   int
	code     string
	err      string
}

// record 记一步的指标；结果不是 ok 时打一条日志（429 用 Debug，限流时不刷屏）。
func (p *prober) record(ctx context.Context, c *cycle, s step, r stepResult) {
	if ctx.Err() != nil {
		return
	}
	stepsTotal.Add(ctx, 1, stepOutcomeOpts[s][r.outcome])
	switch {
	case r.outcome.failed():
		c.worst = cycleFailed
	case r.outcome != outcomeOK && c.worst == cycleOK:
		c.worst = cycleIncomplete
	}
	if r.outcome == outcomeOK {
		if s.waiting() {
			stepDelay.Record(ctx, r.duration.Seconds(), stepOpts[s])
		} else {
			stepDuration.Record(ctx, r.duration.Seconds(), stepOpts[s])
		}
		return
	}
	level := slog.LevelWarn
	if r.outcome == outcomeRejected {
		level = slog.LevelDebug
	}
	p.log.Log(ctx, level, "probe step not ok", "step", s.String(), "outcome", r.outcome.String(),
		"http_status", r.status, "reason", r.code, "order_id", c.orderID, "user_id", c.userID, "sku_id", c.skuID,
		"cycle", c.seq, "duration_ms", r.duration.Milliseconds(), "error", r.err)
}

// classify 按通用的规则给一个请求的结果分类：okStatus 之外的 2xx 也算错误（接口变了）。
func classify(res gwapi.Result, okStatus ...int) stepResult {
	r := stepResult{duration: res.Latency, status: res.Status, code: res.ErrorCode(), err: res.ErrString()}
	switch {
	case res.TimedOut():
		r.outcome = outcomeTimeout
	case res.Err != nil:
		r.outcome = outcomeError
	case res.Status == http.StatusTooManyRequests:
		r.outcome = outcomeRejected
	case res.Status == http.StatusServiceUnavailable && r.code != "" && r.code != gwapi.CodeUnavailable:
		r.outcome = outcomeDegraded
	case slices.Contains(okStatus, res.Status):
		r.outcome = outcomeOK
	default:
		r.outcome = outcomeError
	}
	return r
}

// fail 把一个状态码对、内容不对的结果改成 error。
func (r *stepResult) fail(err error) {
	r.outcome, r.err = outcomeError, err.Error()
}

func (p *prober) stock(ctx context.Context, c *cycle) {
	res := p.client.Do(ctx, http.MethodGet, gwapi.StockPath(c.skuID), c.userID, "", nil)
	r := classify(res, http.StatusOK)
	if r.outcome == outcomeOK {
		var s struct {
			Available *int64 `json:"available"`
		}
		if err := json.Unmarshal(res.Body, &s); err != nil {
			r.fail(fmt.Errorf("decode stock: %w", err))
		} else if s.Available == nil {
			r.fail(errors.New("stock has no available field"))
		}
	}
	p.record(ctx, c, stepStock, r)
}

// create 下单，返回是否受理。订单号和状态记在 c 里给后面的步骤用。
func (p *prober) create(ctx context.Context, c *cycle) bool {
	body, err := json.Marshal(gwapi.CreateOrderRequest{Items: []gwapi.CreateOrderItem{{SKUID: c.skuID, Quantity: 1}}})
	if err != nil {
		// 固定的结构，不会失败；真失败了按 error 记，不让探针退出。
		p.record(ctx, c, stepCreate, stepResult{outcome: outcomeError, err: fmt.Sprintf("encode create order: %v", err)})
		return false
	}
	res := p.client.Do(ctx, http.MethodPost, gwapi.PathOrders, c.userID, c.key, body)
	r := classify(res, http.StatusAccepted, http.StatusCreated)
	if r.outcome == outcomeError && res.Err == nil && res.Status == http.StatusConflict &&
		(r.code == gwapi.CodeInsufficientStock || r.code == gwapi.CodePurchaseLimit) {
		// 超出限购也归到这里：和卖完一样是 SKU 的状态不让下单，要处理的是 SKU 的配置，不是系统。
		r.outcome = outcomeSoldOut
	}
	if r.outcome == outcomeOK {
		o, err := res.DecodeOrder()
		switch {
		case err != nil:
			r.fail(err)
		case o.OrderID == "":
			r.fail(errors.New("created order has no order_id"))
		default:
			c.orderID, c.status = o.OrderID, o.Status
			if c.status == "" {
				// 响应里缺状态时按状态码推断（和 load run 一样）：201 已落库，202 只是受理。
				c.status = gwapi.StatusCreating
				if res.Status == http.StatusCreated {
					c.status = gwapi.StatusPending
				}
			}
		}
	}
	p.record(ctx, c, stepCreate, r)
	c.since = res.End
	return r.outcome == outcomeOK
}

// landAccept 判断等落库时查到的状态。
func landAccept(status string) (outcome, bool) {
	switch status {
	case gwapi.StatusCreating:
		return outcomeOK, false
	case gwapi.StatusPending:
		return outcomeOK, true
	default:
		// failed 是下单失败；别的状态（已支付、已取消）不该出现在一个刚下的单上。
		return outcomeError, true
	}
}

// land 等订单离开 creating，返回是否到了待支付。耗时从下单返回算起。
func (p *prober) land(ctx context.Context, c *cycle) bool {
	var r stepResult
	if out, done := landAccept(c.status); done {
		// 201 或幂等重放直接给出了已落库的订单，不用轮询。
		r = stepResult{outcome: out}
		if out != outcomeOK {
			r.err = "unexpected order status " + c.status
		}
	} else {
		r = p.await(ctx, c, p.cfg.LandTimeout, landAccept)
	}
	p.record(ctx, c, stepLand, r)
	return r.outcome == outcomeOK
}

func (p *prober) pay(ctx context.Context, c *cycle) bool {
	res := p.client.Do(ctx, http.MethodPost, gwapi.OrderPath(c.orderID)+"/pay", c.userID, "", payBody)
	r := classify(res, http.StatusAccepted)
	p.record(ctx, c, stepPay, r)
	c.since = res.End
	return r.outcome == outcomeOK
}

// paid 等订单进入已支付，耗时从支付请求返回算起。
func (p *prober) paid(ctx context.Context, c *cycle) {
	r := p.await(ctx, c, p.cfg.PayTimeout, func(status string) (outcome, bool) {
		switch status {
		case gwapi.StatusPaid:
			return outcomeOK, true
		case gwapi.StatusPending:
			return outcomeOK, false
		default:
			// 指定了支付成功，订单却关了、取消了或者失败了。
			return outcomeError, true
		}
	})
	p.record(ctx, c, stepPaid, r)
}

func (p *prober) cancel(ctx context.Context, c *cycle) {
	res := p.client.Do(ctx, http.MethodPost, gwapi.OrderPath(c.orderID)+"/cancel", c.userID, "", nil)
	r := classify(res, http.StatusOK)
	if r.outcome == outcomeOK {
		o, err := res.DecodeOrder()
		switch {
		case err != nil:
			r.fail(err)
		case o.Status != gwapi.StatusCancelled:
			r.fail(fmt.Errorf("cancelled order has status %q", o.Status))
		}
	}
	p.record(ctx, c, stepCancel, r)
}

func (p *prober) list(ctx context.Context, c *cycle) {
	res := p.client.Do(ctx, http.MethodGet, gwapi.PathOrders, c.userID, "", nil)
	r := classify(res, http.StatusOK)
	if r.outcome == outcomeOK {
		var l struct {
			Orders *[]gwapi.Order `json:"orders"`
		}
		if err := json.Unmarshal(res.Body, &l); err != nil {
			r.fail(fmt.Errorf("decode order list: %w", err))
		} else if l.Orders == nil {
			r.fail(errors.New("order list has no orders field"))
		}
	}
	p.record(ctx, c, stepList, r)
}

// await 轮询订单直到 accept 说等到了，或者到 timeout。accept 收到每次查到的状态，返回结果和是否结束。
//
// 轮询照 OpenAPI 的建议退避：首次 PollInitial，之后每次翻倍到 PollMax，加最多 20% 的抖动（只往后加，
// 不早于建议的间隔），让同一时刻开始的周期错开。收到 429 时至少等 Retry-After；
// 要等的时间超过了剩下的时限就不再问，直接按超时算，不为了赶时限违反 Retry-After。
//
// 查询失败（5xx、超时、连接失败、429、降级）都接着问：轮询本身就是重试，结果看最后有没有等到。
// 404 表示订单丢了（OpenAPI 里说的受理后系统故障），探针不重新提交，记 error。
func (p *prober) await(ctx context.Context, c *cycle, timeout time.Duration, accept func(string) (outcome, bool)) stepResult {
	since := c.since
	deadline := time.Now().Add(timeout)
	backoff := p.cfg.PollInitial
	wait := jitter(backoff)
	var last gwapi.Result
	for {
		remaining := time.Until(deadline)
		if wait > remaining {
			if last.RetryAfter > remaining {
				sleep(ctx, remaining)
				return timedOut(last, since)
			}
			wait = remaining
		}
		if !sleep(ctx, wait) {
			return timedOut(last, since)
		}
		last = p.client.Do(ctx, http.MethodGet, gwapi.OrderPath(c.orderID), c.userID, "", nil)
		switch {
		case last.Err == nil && last.Status == http.StatusOK:
			o, err := last.DecodeOrder()
			if err != nil || o.Status == "" {
				// 解析不了当作这一次没问到，接着问；到时限还这样就是 timeout，日志里有最后的状态码。
				break
			}
			c.status = o.Status
			if out, done := accept(o.Status); done {
				r := stepResult{outcome: out, duration: last.End.Sub(since), status: last.Status}
				if out != outcomeOK {
					r.err = "unexpected order status " + o.Status
				}
				return r
			}
		case last.Err == nil && last.Status == http.StatusNotFound:
			return stepResult{
				outcome: outcomeError, duration: last.End.Sub(since), status: last.Status,
				code: last.ErrorCode(), err: "order not found while polling",
			}
		}
		if !time.Now().Before(deadline) {
			return timedOut(last, since)
		}
		backoff = min(backoff*2, p.cfg.PollMax)
		wait = max(jitter(backoff), last.RetryAfter)
	}
}

// timedOut 是轮询到时限还没等到结果，带上最后一次查询的状态码、原因码和错误，排查时知道卡在哪。
func timedOut(last gwapi.Result, since time.Time) stepResult {
	return stepResult{
		outcome: outcomeTimeout, duration: time.Since(since), status: last.Status,
		code: last.ErrorCode(), err: last.ErrString(),
	}
}

// jitter 在 d 上加 [0, d/5) 的随机量。
func jitter(d time.Duration) time.Duration {
	if d < 5 {
		return d
	}
	return d + rand.N(d/5) //nolint:gosec // 错开轮询用的伪随机数，不涉及安全
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
