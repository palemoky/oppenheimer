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
//	recon.totals.mismatch{kind=count|amount}   违反规则的段上差的笔数、金额（分）。平时是 0。
//	recon.totals.count{side=payment|order}     封口的段上两边各自的笔数，
//	recon.totals.amount{side=payment|order}    和金额（分）。大盘上画出来就是“两条线重合”。
//	recon.totals.in_flight                     订单已接受、答复还没到支付服务的笔数。
//	recon.totals.open_buckets                  没封口的段数。
//	recon.totals.oldest_open_age               最早的没封口的段有多老（秒），没有时是 0。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/recon/internal/totals")

	runsCounter = otelx.Counter(meter, "recon.totals.runs", "Payment totals checks by outcome")
	runOK       = metric.WithAttributes(attribute.String("outcome", "ok"))
	runError    = metric.WithAttributes(attribute.String("outcome", "error"))
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
	// Now 是可注入的时钟，默认 time.Now。
	Now func() time.Time
}

// Watcher 定时核对并把结果报成指标。
type Watcher struct {
	cfg  Config
	last atomic.Pointer[Result]
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
	if cfg.Window < BucketWidth {
		return nil, fmt.Errorf("totals: window %s is shorter than one bucket (%s)", cfg.Window, BucketWidth)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Watcher{cfg: cfg}, nil
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
			"closed", v.Closed,
			"payment_count", v.Payment.Count, "payment_amount", v.Payment.Amount,
			"order_count", v.Order.Count, "order_amount", v.Order.Amount)
	}
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
	kind := func(k string) []attribute.KeyValue { return []attribute.KeyValue{attribute.String("kind", k)} }
	gauges := []gauge{
		{"recon.totals.mismatch", "Count and amount by which payment and order totals disagree", kind("count"), func(r Result) int64 { return r.MismatchCount }},
		{"recon.totals.mismatch", "", kind("amount"), func(r Result) int64 { return r.MismatchAmount }},
		{"recon.totals.count", "Accepted payments in closed buckets, by side", side("payment"), func(r Result) int64 { return r.ClosedPayment.Count }},
		{"recon.totals.count", "", side("order"), func(r Result) int64 { return r.ClosedOrder.Count }},
		{"recon.totals.amount", "Amount of accepted payments in closed buckets, by side", side("payment"), func(r Result) int64 { return r.ClosedPayment.Amount }},
		{"recon.totals.amount", "", side("order"), func(r Result) int64 { return r.ClosedOrder.Amount }},
		{"recon.totals.in_flight", "Payments accepted by the order service whose reply has not reached the payment service", nil, func(r Result) int64 { return r.InFlight }},
		{"recon.totals.open_buckets", "Buckets that still hold pending or unacknowledged payments", nil, func(r Result) int64 { return int64(r.OpenBuckets) }},
		{"recon.totals.oldest_open_age", "Age in seconds of the oldest bucket that is still open", nil, func(r Result) int64 {
			if r.OldestOpen.IsZero() {
				return 0
			}
			return int64(max(w.cfg.Now().Sub(r.OldestOpen), 0) / time.Second)
		}},
	}

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
