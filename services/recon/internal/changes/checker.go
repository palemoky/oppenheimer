package changes

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// Target 是一个派生端。实现要并发安全：不同分区的批会同时调用它，复查也在另一个协程里。
type Target interface {
	// Name 是指标标签里的名字：index、replica、archive。
	Name() string
	// Wants 报告这个目标要不要核对这条变更（例如冷库只看 DELETE，商家索引不看没有商家的订单）。
	Wants(c Change) bool
	// Check 按主键读派生端的行，对 cs 里的每一条给出判定，out[i] 对应 cs[i]。cs 里的订单号互不相同，
	// 都是 Wants 返回 true 的。读不到（连不上、超时）时返回错误，不能把读不到说成 Behind 或 Mismatch。
	Check(ctx context.Context, cs []Change) (out []Result, err error)
}

// Result 是一个目标对一条变更的判定。
type Result struct {
	Verdict Verdict
	// Detail 是给人看的说明，Mismatch 时写哪一列不同、两边各是什么。
	Detail string
}

// 指标。标签只有固定的几个取值；订单号在日志里。
//
//	recon.changes.checked{target, result=ok|lagging|mismatch|skipped}
//	    核对过的变更数。lagging 是超过 LagAfter 仍然没追上的（每条变更只计一次）；等着复查的不在里面。
//	    skipped 是没有核对的：消息解不开（target="none"），或者核对被开关关掉了。
//	recon.changes.verified_through
//	    核对到了哪个提交时刻（Unix 秒）：第一次核对过的批里最晚的那个提交时刻。源库有写入而它不往前走，
//	    说明核对停了或者被关了。“差异是 0”要和它一起看，否则核对自己挂了也显示 0。
//	recon.changes.pending
//	    等着复查的变更数。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/recon/internal/changes")

	checkedCounter = otelx.Counter(meter, "recon.changes.checked", "Row changes checked against derived data, by target and result")
)

const (
	resultOK       = "ok"
	resultLagging  = "lagging"
	resultMismatch = "mismatch"
	resultSkipped  = "skipped"
	targetNone     = "none"
)

// Config 是 Checker 的配置。零值字段取默认值。
type Config struct {
	Targets []Target
	Logger  *slog.Logger
	// Delay 是变更提交之后等多久才去核对，默认 10 秒。只影响复查的次数，不影响判定。
	Delay time.Duration
	// RecheckEvery 是多久复查一次还没追上的，默认 5 秒。
	RecheckEvery time.Duration
	// LagAfter 是一条变更第一次核对之后多久仍然没追上才报“落后”，默认 2 分钟。
	LagAfter time.Duration
	// CheckTimeout 是一个目标核对一批的时限，默认 5 秒。
	CheckTimeout time.Duration
	// MaxPending 是最多记多少条等着复查的，默认 200000。派生端整个停住时每条变更都没追上，
	// 到了上限之后新的直接报成“落后”，不再记。
	MaxPending int
	// Off 报告核对是不是被人关掉了（pkg/degrade 的开关）。关掉时只消费、不查库。nil 表示没有开关。
	Off func() bool
}

// Checker 核对变更。Handler 交给 kafkax 的按批消费者，Run 在另一个协程里跑复查。
type Checker struct {
	cfg     Config
	results map[string]map[string]metric.MeasurementOption // target -> result -> 预先建好的标签

	mu      sync.Mutex
	pending map[pendingKey]*pendingChange

	// verifiedThrough 是核对到的最晚的提交时刻（Unix 微秒），0 表示还没有。
	verifiedThrough atomic.Int64
}

type pendingKey struct {
	target  int // cfg.Targets 的下标
	orderID int64
}

type pendingChange struct {
	change Change
	// since 是第一次核对发现它没追上的时刻。
	since time.Time
}

// New 校验配置。
func New(cfg Config) (*Checker, error) {
	if len(cfg.Targets) == 0 {
		return nil, errors.New("changes: at least one target is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Delay <= 0 {
		cfg.Delay = 10 * time.Second
	}
	if cfg.RecheckEvery <= 0 {
		cfg.RecheckEvery = 5 * time.Second
	}
	if cfg.LagAfter <= 0 {
		cfg.LagAfter = 2 * time.Minute
	}
	if cfg.CheckTimeout <= 0 {
		cfg.CheckTimeout = 5 * time.Second
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = 200_000
	}
	c := &Checker{cfg: cfg, results: map[string]map[string]metric.MeasurementOption{}, pending: map[pendingKey]*pendingChange{}}
	names := []string{targetNone}
	for _, t := range cfg.Targets {
		if _, dup := c.results[t.Name()]; dup || t.Name() == targetNone {
			return nil, fmt.Errorf("changes: target name %q is duplicated or reserved", t.Name())
		}
		c.results[t.Name()] = nil
		names = append(names, t.Name())
	}
	for _, name := range names {
		c.results[name] = map[string]metric.MeasurementOption{}
		for _, r := range []string{resultOK, resultLagging, resultMismatch, resultSkipped} {
			c.results[name][r] = metric.WithAttributes(attribute.String("target", name), attribute.String("result", r))
		}
	}
	return c, nil
}

func (c *Checker) count(ctx context.Context, target, result string, n int) {
	if n > 0 {
		checkedCounter.Add(ctx, int64(n), c.results[target][result])
	}
}

// Handler 返回交给 kafkax.NewBatchConsumer 的处理函数。消费者的 MaxAttempts 要设成最大值：
// 派生端读不到时这里返回普通错误，分区应当停在这一批上等它恢复，而不是把消息送进死信。
func (c *Checker) Handler() kafkax.BatchHandler {
	return func(ctx context.Context, msgs []kafkax.Message) error {
		changes := make([]Change, 0, len(msgs))
		for _, msg := range msgs {
			ch := &cdcv1.RowChange{}
			err := proto.Unmarshal(msg.Value, ch)
			var (
				change Change
				ok     bool
			)
			if err == nil {
				change, ok, err = Decode(ch)
			}
			if err != nil {
				// 解不开的消息跳过，不挡住分区（见包注释）。
				c.count(ctx, targetNone, resultSkipped, 1)
				c.cfg.Logger.ErrorContext(ctx, "row change skipped: cannot be decoded",
					"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key), "error", err)
				continue
			}
			if ok {
				changes = append(changes, change)
			}
		}
		return c.Check(ctx, changes)
	}
}

// Check 核对一批变更：等到其中最晚的一条提交满 Delay，再逐个目标核对。返回错误表示有目标读不到，
// 这一批没有核对完（已经核对的部分重做没有后果）。
func (c *Checker) Check(ctx context.Context, changes []Change) error {
	if len(changes) == 0 {
		return nil
	}
	var latest time.Time
	for _, ch := range changes {
		if ch.Commit.After(latest) {
			latest = ch.Commit
		}
	}
	if c.cfg.Off != nil && c.cfg.Off() {
		// 关掉时只消费：位点照常往前走，verified_through 不动，看得出它没在核对。
		for _, t := range c.cfg.Targets {
			c.count(ctx, t.Name(), resultSkipped, countWanted(t, changes))
		}
		return nil
	}
	if !latest.IsZero() {
		if err := sleepUntil(ctx, latest.Add(c.cfg.Delay)); err != nil {
			return err
		}
	}
	for i, t := range c.cfg.Targets {
		batch := latestPerOrder(t, changes)
		if len(batch) == 0 {
			continue
		}
		results, err := c.checkTarget(ctx, t, batch)
		if err != nil {
			return err
		}
		c.settle(ctx, i, batch, results, false)
	}
	if !latest.IsZero() {
		// 只往前走：各分区的批是并发来的。
		for {
			cur := c.verifiedThrough.Load()
			if latest.UnixMicro() <= cur || c.verifiedThrough.CompareAndSwap(cur, latest.UnixMicro()) {
				break
			}
		}
	}
	return nil
}

func (c *Checker) checkTarget(ctx context.Context, t Target, batch []Change) ([]Result, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.CheckTimeout)
	defer cancel()
	results, err := t.Check(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("changes: check %s: %w", t.Name(), err)
	}
	if len(results) != len(batch) {
		return nil, fmt.Errorf("changes: target %s returned %d results for %d changes", t.Name(), len(results), len(batch))
	}
	return results, nil
}

// settle 按判定记账。recheck 为 true 表示这是复查：batch 里的都已经在等着复查的表里。
func (c *Checker) settle(ctx context.Context, target int, batch []Change, results []Result, recheck bool) {
	name := c.cfg.Targets[target].Name()
	now := time.Now()
	var ok, lagging, mismatch int
	c.mu.Lock()
	for i, ch := range batch {
		key := pendingKey{target, ch.OrderID}
		p := c.pending[key]
		if recheck && (p == nil || p.change.Version != ch.Version) {
			continue // 复查期间被更新的变更顶替了，这次的结果作废，由新的那条负责
		}
		switch results[i].Verdict {
		case OK:
			ok++
			if p != nil && p.change.Version <= ch.Version {
				delete(c.pending, key) // 派生端已经到了这个版本或更晚，之前等着的那条也就追上了
			}
		case Mismatch:
			mismatch++
			if p != nil && p.change.Version <= ch.Version {
				delete(c.pending, key)
			}
			c.cfg.Logger.ErrorContext(ctx, "derived data does not match the row change",
				"target", name, "order_id", ch.OrderID, "version", ch.Version, "deleted", ch.Deleted, "detail", results[i].Detail)
		case Behind:
			switch {
			case p != nil && p.change.Version > ch.Version:
				// 已经在等一个更晚的版本，这条旧的不用管。
			case recheck && now.Sub(p.since) < c.cfg.LagAfter:
				// 接着等。
			case recheck, p == nil && len(c.pending) >= c.cfg.MaxPending:
				lagging++
				delete(c.pending, key)
				c.cfg.Logger.WarnContext(ctx, "derived data is lagging behind the row change",
					"target", name, "order_id", ch.OrderID, "version", ch.Version, "deleted", ch.Deleted,
					"commit", ch.Commit.UTC().Format(time.RFC3339Nano), "detail", results[i].Detail)
			default:
				// 新的变更顶替同一个订单上旧的那条：派生端追上新的，旧的也就追上了。时刻重新算。
				c.pending[key] = &pendingChange{change: ch, since: now}
			}
		default:
			// 目标返回了不认识的判定，是实现的缺陷。按没追上处理会一直复查，按通过处理会漏报；记成不一致让人看到。
			mismatch++
			c.cfg.Logger.ErrorContext(ctx, "target returned an unknown verdict", "target", name, "order_id", ch.OrderID, "verdict", int(results[i].Verdict))
		}
	}
	c.mu.Unlock()
	c.count(ctx, name, resultOK, ok)
	c.count(ctx, name, resultLagging, lagging)
	c.count(ctx, name, resultMismatch, mismatch)
}

// Run 定时复查还没追上的变更，并登记瞬时值，直到 ctx 取消。
func (c *Checker) Run(ctx context.Context) error {
	unregister, err := c.registerGauges()
	if err != nil {
		return err
	}
	defer unregister()
	t := time.NewTicker(c.cfg.RecheckEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			c.Recheck(ctx)
		}
	}
}

// Recheck 复查一遍等着的变更。目标读不到时那个目标的留到下一次，不计数。
func (c *Checker) Recheck(ctx context.Context) {
	if c.cfg.Off != nil && c.cfg.Off() {
		return // 关掉时不查库；等着的留着，打开之后接着查
	}
	batches := make([][]Change, len(c.cfg.Targets))
	c.mu.Lock()
	for key, p := range c.pending {
		batches[key.target] = append(batches[key.target], p.change)
	}
	c.mu.Unlock()
	for i, batch := range batches {
		// 派生端整个落后时等着的会很多，按消费时的批大小切开，一批一个时限。
		for len(batch) > 0 {
			if ctx.Err() != nil {
				return
			}
			n := min(len(batch), kafkax.DefaultMaxBatch)
			results, err := c.checkTarget(ctx, c.cfg.Targets[i], batch[:n])
			if err != nil {
				c.cfg.Logger.WarnContext(ctx, "recheck failed, will retry", "target", c.cfg.Targets[i].Name(), "error", err)
				break
			}
			c.settle(ctx, i, batch[:n], results, true)
			batch = batch[n:]
		}
	}
}

// Pending 返回等着复查的变更数。
func (c *Checker) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// VerifiedThrough 返回核对到的最晚的提交时刻；还没有核对过带提交时刻的变更时是零值。
func (c *Checker) VerifiedThrough() time.Time {
	if us := c.verifiedThrough.Load(); us > 0 {
		return time.UnixMicro(us)
	}
	return time.Time{}
}

func (c *Checker) registerGauges() (unregister func(), err error) {
	through, err := meter.Int64ObservableGauge("recon.changes.verified_through",
		metric.WithDescription("Commit time (Unix seconds) of the latest row change checked"))
	if err != nil {
		return nil, fmt.Errorf("changes: gauge verified_through: %w", err)
	}
	pending, err := meter.Int64ObservableGauge("recon.changes.pending",
		metric.WithDescription("Row changes waiting to be rechecked because derived data had not caught up"))
	if err != nil {
		return nil, fmt.Errorf("changes: gauge pending: %w", err)
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		// 还没核对过时不报（没有序列），而不是报 0。
		if t := c.VerifiedThrough(); !t.IsZero() {
			o.ObserveInt64(through, t.Unix())
		}
		o.ObserveInt64(pending, int64(c.Pending()))
		return nil
	}, through, pending)
	if err != nil {
		return nil, fmt.Errorf("changes: register gauges: %w", err)
	}
	return func() { _ = reg.Unregister() }, nil
}

// latestPerOrder 挑出目标要核对的变更，同一个订单只留版本最高的那条：派生端到了最高的版本，
// 之前的版本按规则都是通过。顺序按第一次出现的先后。
func latestPerOrder(t Target, changes []Change) []Change {
	var out []Change
	at := map[int64]int{}
	for _, ch := range changes {
		if !t.Wants(ch) {
			continue
		}
		if i, seen := at[ch.OrderID]; seen {
			if ch.Version >= out[i].Version {
				out[i] = ch
			}
			continue
		}
		at[ch.OrderID] = len(out)
		out = append(out, ch)
	}
	return out
}

func countWanted(t Target, changes []Change) int {
	n := 0
	for _, ch := range changes {
		if t.Wants(ch) {
			n++
		}
	}
	return n
}

// sleepUntil 等到 at，或者 ctx 取消。
func sleepUntil(ctx context.Context, at time.Time) error {
	d := time.Until(at)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
