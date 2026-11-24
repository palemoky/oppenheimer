package totals

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 指标。标签只有固定的几个取值；哪一分钟、哪一笔不进标签，在日志里。
//
//	recon.totals.runs{outcome=ok|error}        核对跑了多少轮。告警用它判断“核对自己还活着”：没有 ok 的增长时，
//	                                           下面几个瞬时值是旧的，不能当成“没有差异”。
//	recon.totals.mismatch{kind=count|amount, group=succeeded|refunded}
//	                                           违反规则的段上差的笔数、金额（分），按组。平时是 0。
//	recon.totals.count{side=payment|order}     封口的段上两边各自的笔数，
//	recon.totals.amount{side=payment|order}    和金额（分）。只报“成功”那一组；大盘上画出来就是“两条线重合”。
//	recon.totals.in_flight                     订单已接受、答复还没到支付服务的笔数（“成功”那一组）。
//	recon.totals.drilldowns{outcome=found|clean|error}
//	                                           下钻的次数。found 是找到了具体的支付单；clean 是逐笔没有差异
//	                                           （合计报了两轮、逐笔却没有，差异在这之间消失了，值得看）。
//	recon.totals.open_buckets                  没封口的段数。
//	recon.totals.oldest_open_age               最早的没封口的段有多老（秒），没有时是 0。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/recon/internal/totals")

	runsCounter = otelx.Counter(meter, "recon.totals.runs", "Payment totals checks by outcome")
	runOK       = metric.WithAttributes(attribute.String("outcome", "ok"))
	runError    = metric.WithAttributes(attribute.String("outcome", "error"))

	drillCounter = otelx.Counter(meter, "recon.totals.drilldowns", "Per-payment drill-downs into a mismatching bucket by outcome")
	drillFound   = metric.WithAttributes(attribute.String("outcome", "found"))
	drillClean   = metric.WithAttributes(attribute.String("outcome", "clean"))
	drillError   = metric.WithAttributes(attribute.String("outcome", "error"))
)

const (
	// drillAfterRounds：一段连续这么多轮对不上才下钻。规则本身不会因为在途而误报，等第二轮是多一层保险，
	// 也让只出现一轮的差异不触发逐笔的读取。
	drillAfterRounds = 2
	// maxDrillsPerRound 限制一轮里下钻几段。很多段同时对不上（例如一边的库整段丢了数据）时，
	// 不让下钻把这一轮拖长；剩下的段留给后面几轮，从最早的开始。
	maxDrillsPerRound = 3
	// maxFindingsLogged 是一次下钻最多打几笔，总数另外打。
	maxFindingsLogged = 20
)

// Config 是 Watcher 的配置。零值字段取默认值。
type Config struct {
	Sources Sources
	Logger  *slog.Logger
	// Interval 是两轮之间的间隔，默认 30 秒。
	Interval time.Duration
	// Window 是每一轮往回看多久，默认 30 分钟。它要比“一笔支付从创建到订单答复”最坏的时间长：
	// 支付单在窗口里的时候才被核对，滑出窗口之后由每小时的逐行对账负责。
	Window time.Duration
	// Skip 是最新的多久不看，默认 1 分钟，见 Window 函数。
	Skip time.Duration
	// QueryTimeout 是一轮的时限，默认 10 秒。
	QueryTimeout time.Duration
	// DrillTimeout 是一次下钻的时限，默认 10 秒。
	DrillTimeout time.Duration
	// Now 是可注入的时钟，默认 time.Now。
	Now func() time.Time
}

// Watcher 定时核对并把结果报成指标。
type Watcher struct {
	cfg  Config
	last atomic.Pointer[Result]

	// 下面两个只在跑核对的那一个协程里读写。键是段的起始时刻（Unix 毫秒）。
	// streak 是一段已经连续几轮对不上；drilled 是已经下钻过的段，滑出窗口之后删掉。
	streak  map[int64]int
	drilled map[int64]struct{}
}

// NewWatcher 校验配置。
func NewWatcher(cfg Config) (*Watcher, error) {
	if cfg.Sources.Payment == nil || len(cfg.Sources.Orders) == 0 || cfg.Sources.Router == nil {
		return nil, errors.New("totals: payment database, order databases and router are all required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Window <= 0 {
		cfg.Window = 30 * time.Minute
	}
	if cfg.Skip <= 0 {
		cfg.Skip = time.Minute
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = 10 * time.Second
	}
	if cfg.DrillTimeout <= 0 {
		cfg.DrillTimeout = 10 * time.Second
	}
	if cfg.Window < BucketWidth {
		return nil, fmt.Errorf("totals: window %s is shorter than one bucket (%s)", cfg.Window, BucketWidth)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Watcher{cfg: cfg, streak: map[int64]int{}, drilled: map[int64]struct{}{}}, nil
}

// Run 立刻跑一轮，之后每 Interval 一轮，直到 ctx 取消。一轮出错只记日志和计数，不退出：
// 库暂时连不上时它应当等着恢复，由“没有成功的轮次”这条告警报出来。
func (w *Watcher) Run(ctx context.Context) error {
	unregister, err := w.registerGauges()
	if err != nil {
		return err
	}
	defer unregister()

	t := time.NewTicker(w.cfg.Interval)
	defer t.Stop()
	for {
		w.round(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// round 跑一轮并更新指标读的那份结果。
func (w *Watcher) round(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	r, err := w.Once(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return // 正在停止，不算一次失败
		}
		runsCounter.Add(ctx, 1, runError)
		w.cfg.Logger.ErrorContext(ctx, "payment totals check failed", "error", err)
		return
	}
	runsCounter.Add(ctx, 1, runOK)
	w.last.Store(&r)
	for _, v := range r.Violations {
		// 每一轮都打：差异还在就一直在日志里。时间范围可以直接拿去给逐行对账定位是哪一笔。
		w.cfg.Logger.ErrorContext(ctx, "payment totals mismatch",
			"bucket_start", v.Start.UTC().Format(time.RFC3339), "bucket_end", v.Start.Add(BucketWidth).UTC().Format(time.RFC3339),
			"group", v.Group.String(), "closed", v.Closed,
			"payment_count", v.Payment.Count, "payment_amount", v.Payment.Amount,
			"order_count", v.Order.Count, "order_amount", v.Order.Amount)
	}
	w.drillDown(ctx, r)
}

// drillDown 对连续 drillAfterRounds 轮对不上的段逐笔比一次，把是哪几笔打进日志。每段只下钻一次：
// 逐笔的结果不会因为多看几遍而不同，差异还在的话合计那条日志每一轮都在。下钻自己出错的不算，下一轮再试。
func (w *Watcher) drillDown(ctx context.Context, r Result) {
	// 这一轮对不上的段，升序、去重（一段可能两组都对不上）。
	var starts []time.Time
	for _, v := range r.Violations {
		if len(starts) == 0 || !starts[len(starts)-1].Equal(v.Start) {
			starts = append(starts, v.Start)
		}
	}
	// “连续”：这一轮没有对不上的段清零。
	now := make(map[int64]struct{}, len(starts))
	for _, s := range starts {
		now[s.UnixMilli()] = struct{}{}
		w.streak[s.UnixMilli()]++
	}
	for k := range w.streak {
		if _, ok := now[k]; !ok {
			delete(w.streak, k)
		}
	}
	// 滑出窗口的段不会再被看到，记录可以删了。
	for k := range w.drilled {
		if k < r.From.UnixMilli() {
			delete(w.drilled, k)
		}
	}

	drills := 0
	for _, s := range starts {
		k := s.UnixMilli()
		if _, done := w.drilled[k]; done || w.streak[k] < drillAfterRounds {
			continue
		}
		if drills == maxDrillsPerRound || ctx.Err() != nil {
			return
		}
		drills++
		if w.drillOne(ctx, s) {
			w.drilled[k] = struct{}{}
		}
	}
}

// drillOne 下钻一段，报告做没做成。
func (w *Watcher) drillOne(ctx context.Context, start time.Time) bool {
	log := w.cfg.Logger.With("bucket_start", start.UTC().Format(time.RFC3339), "bucket_end", start.Add(BucketWidth).UTC().Format(time.RFC3339))
	dctx, cancel := context.WithTimeout(ctx, w.cfg.DrillTimeout)
	defer cancel()
	findings, err := Drill(dctx, w.cfg.Sources, start)
	if err != nil {
		if ctx.Err() != nil {
			return false // 正在停止，不算一次失败
		}
		drillCounter.Add(ctx, 1, drillError)
		log.ErrorContext(ctx, "payment totals drill-down failed", "error", err)
		return false
	}
	if len(findings) == 0 {
		drillCounter.Add(ctx, 1, drillClean)
		log.WarnContext(ctx, "payment totals drill-down found no differing payment: the totals disagreed for two checks and the rows agree now")
		return true
	}
	drillCounter.Add(ctx, 1, drillFound)
	for _, f := range findings[:min(len(findings), maxFindingsLogged)] {
		attrs := []any{
			"payment_id", f.PaymentID, "order_id", f.OrderID(), "kind", string(f.Kind), "group", f.Group.String(),
			"in_payment", f.Payment != nil, "in_order", f.Order != nil,
		}
		if p := f.Payment; p != nil {
			attrs = append(attrs, "payment_status", p.Status, "payment_notified", p.Notified, "payment_amount", p.Amount)
		}
		if o := f.Order; o != nil {
			attrs = append(attrs, "order_succeeded", o.Succeeded, "order_disposition", o.Disposition, "order_amount", o.Amount)
		}
		log.ErrorContext(ctx, "payment totals drill-down: differing payment", attrs...)
	}
	log.ErrorContext(ctx, "payment totals drill-down done", "differing_payments", len(findings), "logged", min(len(findings), maxFindingsLogged))
	return true
}

// Once 核对一次当前的窗口。
func (w *Watcher) Once(ctx context.Context) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, w.cfg.QueryTimeout)
	defer cancel()
	lo, hi := Window(w.cfg.Now(), w.cfg.Window, w.cfg.Skip)
	return Collect(ctx, w.cfg.Sources, lo, hi)
}

// registerGauges 登记那几个瞬时值。它们读的是最近一次成功的结果；还没有成功过时不报（没有序列），
// 而不是报 0：0 的意思是“核对过，没有差异”。
func (w *Watcher) registerGauges() (unregister func(), err error) {
	type gauge struct {
		name, desc string
		attrs      []attribute.KeyValue
		read       func(Result) int64
	}
	side := func(s string) []attribute.KeyValue { return []attribute.KeyValue{attribute.String("side", s)} }
	ok := func(r Result) GroupResult { return r.Groups[GroupSucceeded] }
	var gauges []gauge
	for g := range NumGroups {
		attrs := func(kind string) []attribute.KeyValue {
			return []attribute.KeyValue{attribute.String("kind", kind), attribute.String("group", g.String())}
		}
		gauges = append(gauges,
			gauge{"recon.totals.mismatch", "Count and amount by which payment and order totals disagree", attrs("count"), func(r Result) int64 { return r.Groups[g].Mismatch.Count }},
			gauge{"recon.totals.mismatch", "", attrs("amount"), func(r Result) int64 { return r.Groups[g].Mismatch.Amount }})
	}
	gauges = append(gauges, []gauge{
		{"recon.totals.count", "Accepted payments in closed buckets, by side", side("payment"), func(r Result) int64 { return ok(r).ClosedPayment.Count }},
		{"recon.totals.count", "", side("order"), func(r Result) int64 { return ok(r).ClosedOrder.Count }},
		{"recon.totals.amount", "Amount of accepted payments in closed buckets, by side", side("payment"), func(r Result) int64 { return ok(r).ClosedPayment.Amount }},
		{"recon.totals.amount", "", side("order"), func(r Result) int64 { return ok(r).ClosedOrder.Amount }},
		{"recon.totals.in_flight", "Payments accepted by the order service whose reply has not reached the payment service", nil, func(r Result) int64 { return ok(r).InFlight }},
		{"recon.totals.open_buckets", "Buckets that still hold pending or unacknowledged payments", nil, func(r Result) int64 { return int64(r.OpenBuckets) }},
		{"recon.totals.oldest_open_age", "Age in seconds of the oldest bucket that is still open", nil, func(r Result) int64 {
			if r.OldestOpen.IsZero() {
				return 0
			}
			return int64(max(w.cfg.Now().Sub(r.OldestOpen), 0) / time.Second)
		}},
	}...)

	// 同名的指标只建一个仪表，各个标签组合在一个回调里报。
	instruments := map[string]metric.Int64ObservableGauge{}
	var all []metric.Observable
	for _, g := range gauges {
		if _, ok := instruments[g.name]; ok {
			continue
		}
		inst, err := meter.Int64ObservableGauge(g.name, metric.WithDescription(g.desc))
		if err != nil {
			return nil, fmt.Errorf("totals: gauge %s: %w", g.name, err)
		}
		instruments[g.name] = inst
		all = append(all, inst)
	}
	opts := make([]metric.ObserveOption, len(gauges))
	for i, g := range gauges {
		opts[i] = metric.WithAttributes(g.attrs...)
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		r := w.last.Load()
		if r == nil {
			return nil
		}
		for i, g := range gauges {
			o.ObserveInt64(instruments[g.name], g.read(*r), opts[i])
		}
		return nil
	}, all...)
	if err != nil {
		return nil, fmt.Errorf("totals: register gauges: %w", err)
	}
	return func() { _ = reg.Unregister() }, nil
}
