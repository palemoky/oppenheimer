package stock

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 指标。标签只有固定的几个取值；哪个 SKU、差多少在日志里，不进标签。
//
//	recon.stock.runs{kind=quick|full, outcome=ok|error}  核对跑了多少轮。告警用它判断“核对自己还活着”：
//	                                                     没有 ok 的增长时，下面几个瞬时值是旧的。
//	recon.stock.mismatch_skus{check=reserved|sold}       这条等式对不上的 SKU 个数。平时是 0。
//	recon.stock.mismatch_quantity{check=reserved|sold}   差的件数的绝对值之和。
//	recon.stock.stuck_reservations                       卡住的预占的个数（完整的一轮才更新）。
//	recon.stock.skus                                     核对了多少个 SKU。
//
// reserved 那一组每一轮都更新；sold 和 stuck 只在完整的一轮更新，两轮之间保持上一次完整一轮的值。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/recon/internal/stock")

	runsCounter = otelx.Counter(meter, "recon.stock.runs", "Stock checks by kind (quick, full) and outcome")
	runOpts     = map[bool]map[bool]metric.MeasurementOption{
		false: {true: runAttrs("quick", "ok"), false: runAttrs("quick", "error")},
		true:  {true: runAttrs("full", "ok"), false: runAttrs("full", "error")},
	}
)

func runAttrs(kind, outcome string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("kind", kind), attribute.String("outcome", outcome))
}

// maxLogged 是一轮里最多把几个 SKU 的差异打进日志；其余的只有个数。
const maxLogged = 20

// Config 是 Watcher 的配置。零值字段取默认值。
type Config struct {
	// DB 是库存库（主库：它没有从库）。
	DB     *sql.DB
	Logger *slog.Logger
	// Interval 是两轮之间的间隔，默认 1 分钟。
	Interval time.Duration
	// FullEvery 是多久做一次完整的一轮，默认 1 小时。启动后的第一轮总是完整的。
	FullEvery time.Duration
	// ReservedWindow 见 Options，默认 2 小时。
	ReservedWindow time.Duration
	// StuckAfter 见 Options，默认 1 小时。
	StuckAfter time.Duration
	// QuickTimeout、FullTimeout 是两种轮次各自的时限，默认 5 秒和 2 分钟。时限同时限制了快照事务开多久。
	QuickTimeout time.Duration
	FullTimeout  time.Duration
	// Now 是可注入的时钟，默认 time.Now。
	Now func() time.Time
}

// state 是指标读的那一份：最近一次成功的快轮，和最近一次成功的完整一轮。
type state struct {
	quick *Result
	full  *Result
}

// Watcher 定时核对并把结果报成指标。
type Watcher struct {
	cfg      Config
	last     atomic.Pointer[state]
	lastFull time.Time
}

// NewWatcher 校验配置。
func NewWatcher(cfg Config) (*Watcher, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("%w: inventory database is required", ErrConfig)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&cfg.Interval, time.Minute)
	def(&cfg.FullEvery, time.Hour)
	def(&cfg.ReservedWindow, 2*time.Hour)
	def(&cfg.StuckAfter, time.Hour)
	def(&cfg.QuickTimeout, 5*time.Second)
	def(&cfg.FullTimeout, 2*time.Minute)
	if cfg.StuckAfter > cfg.ReservedWindow {
		// 否则有一段年龄的 RESERVED 既不在快轮的窗口里、又不算卡住：快轮报差值，完整的一轮却说不出原因。
		return nil, fmt.Errorf("%w: stuck-after (%s) must not be longer than the reserved window (%s)", ErrConfig, cfg.StuckAfter, cfg.ReservedWindow)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Watcher{cfg: cfg}, nil
}

// Run 立刻跑一轮（完整的），之后每 Interval 一轮，直到 ctx 取消。一轮出错只记日志和计数，不退出。
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

func (w *Watcher) round(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	now := w.cfg.Now()
	full := w.lastFull.IsZero() || now.Sub(w.lastFull) >= w.cfg.FullEvery
	r, err := w.Once(ctx, full)
	if err != nil {
		if ctx.Err() != nil {
			return // 正在停止，不算一次失败
		}
		runsCounter.Add(ctx, 1, runOpts[full][false])
		w.cfg.Logger.ErrorContext(ctx, "stock check failed", "full", full, "error", err)
		return
	}
	runsCounter.Add(ctx, 1, runOpts[full][true])
	next := state{quick: &r}
	if prev := w.last.Load(); prev != nil {
		next.full = prev.full
	}
	if full {
		// 失败的完整一轮不更新 lastFull：下一轮接着试，而不是再等一个 FullEvery。
		w.lastFull = now
		next.full = &r
	}
	w.last.Store(&next)
	w.log(ctx, &r)
}

// Once 核对一轮。
func (w *Watcher) Once(ctx context.Context, full bool) (Result, error) {
	timeout := w.cfg.QuickTimeout
	if full {
		timeout = w.cfg.FullTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return Collect(ctx, w.cfg.DB, Options{
		Now: w.cfg.Now(), ReservedWindow: w.cfg.ReservedWindow, Full: full, StuckAfter: w.cfg.StuckAfter,
	})
}

// log 把差异打进日志：每一轮都打，差异还在就一直在日志里。
func (w *Watcher) log(ctx context.Context, r *Result) {
	logDiffs := func(check string, diffs []Diff) {
		for i, d := range diffs {
			if i == maxLogged {
				w.cfg.Logger.ErrorContext(ctx, "stock mismatch: more skus not logged", "check", check, "skus", len(diffs)-maxLogged)
				break
			}
			w.cfg.Logger.ErrorContext(ctx, "stock mismatch", "check", check, "sku_id", d.SKU,
				"counter", d.Counter, "ledger", d.Ledger, "no_stock_row", d.NoStockRow)
		}
	}
	logDiffs(CheckReserved, r.Reserved)
	if r.Full {
		logDiffs(CheckSold, r.Sold)
		if r.Stuck > 0 {
			w.cfg.Logger.WarnContext(ctx, "stuck reservations: still RESERVED long after the payment deadline",
				"count", r.Stuck, "oldest_order_id", r.OldestStuck, "older_than", w.cfg.StuckAfter.String())
		}
	}
}

// registerGauges 登记瞬时值。还没有成功核对过时不报（没有序列），而不是报 0：0 的意思是“核对过，没有差异”。
func (w *Watcher) registerGauges() (unregister func(), err error) {
	gauge := func(name, desc string) (metric.Int64ObservableGauge, error) {
		g, err := meter.Int64ObservableGauge(name, metric.WithDescription(desc))
		if err != nil {
			return nil, fmt.Errorf("stock: gauge %s: %w", name, err)
		}
		return g, nil
	}
	skus, err := gauge("recon.stock.mismatch_skus", "SKUs whose counters disagree with the reservation ledger, by check")
	if err != nil {
		return nil, err
	}
	qty, err := gauge("recon.stock.mismatch_quantity", "Units by which counters and the reservation ledger disagree, by check")
	if err != nil {
		return nil, err
	}
	stuck, err := gauge("recon.stock.stuck_reservations", "Reservations still RESERVED long after the payment deadline")
	if err != nil {
		return nil, err
	}
	checked, err := gauge("recon.stock.skus", "SKUs covered by the last check")
	if err != nil {
		return nil, err
	}
	reserved := metric.WithAttributes(attribute.String("check", CheckReserved))
	sold := metric.WithAttributes(attribute.String("check", CheckSold))

	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := w.last.Load()
		if st == nil {
			return nil
		}
		o.ObserveInt64(skus, int64(len(st.quick.Reserved)), reserved)
		o.ObserveInt64(qty, Quantity(st.quick.Reserved), reserved)
		o.ObserveInt64(checked, int64(st.quick.SKUs))
		if st.full != nil {
			o.ObserveInt64(skus, int64(len(st.full.Sold)), sold)
			o.ObserveInt64(qty, Quantity(st.full.Sold), sold)
			o.ObserveInt64(stuck, st.full.Stuck)
		}
		return nil
	}, skus, qty, stuck, checked)
	if err != nil {
		return nil, fmt.Errorf("stock: register gauges: %w", err)
	}
	return func() { _ = reg.Unregister() }, nil
}
