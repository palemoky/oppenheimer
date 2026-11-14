// Package breaker 是进程里的熔断器，用在 Envoy 边车看不到的依赖上（Redis、MySQL）。
//
// 服务之间的 gRPC 调用由边车熔断（设计文档 1d 第 5 节），不用这个包。
//
// 三个状态（设计文档 1d 的 6.3）：
//
//	        连续失败 N 次                 冷却时间到
//	关闭 ───────────────→ 打开 ───────────────→ 半开
//	 ↑                      ↑                     │
//	 │                      └──── 探测失败 ───────┤
//	 └──────────────────── 探测成功 ──────────────┘
//
// 关闭时放行并数连续失败的次数；打开时不放行，调用方立刻走降级的那条路，不再为一个坏掉的依赖
// 每次都等满超时；半开时只放一个请求去探测，其余仍然不放行。
//
// 按连续失败而不是错误比例判断：这里保护的依赖坏起来是整体坏（连不上、挂起），连续失败很快就能数到。
// 按比例要维护滑动窗口，针对的是“一部分请求失败”，现在没有遇到。
package breaker

import (
	"cmp"
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ErrOpen 表示熔断器没有放行这次调用。
var ErrOpen = errors.New("breaker: open")

// 默认值，和 Envoy 边车的摘除参数一致（连续 5 次、5 秒）。
const (
	DefaultFailures = 5
	DefaultCooldown = 5 * time.Second
)

// State 是熔断器的状态。
type State int

// 三个状态。
const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Outcome 是一次被放行的调用的结果。
type Outcome int

const (
	// Success 表示依赖正常地回答了（包括业务上的拒绝，比如“库存不足”“订单不存在”）。
	Success Outcome = iota
	// Failure 表示依赖出错或者这一跳超时了。
	Failure
	// Ignored 表示这次调用说明不了依赖的状态，比如调用方自己取消了、上游的截止时间先到了。
	// 不计入失败；如果它是半开时的那个探测，把探测的名额让出来。
	Ignored
)

// Config 是熔断器的配置。
type Config struct {
	// Name 用于日志和状态展示。
	Name string
	// Failures 是连续失败多少次后打开，默认 DefaultFailures。
	Failures int
	// Cooldown 是打开多久之后进入半开，默认 DefaultCooldown。
	// 半开时放出去的探测超过这么久没有结果，也会再放一个，防止一个丢失的探测把熔断器永远卡在半开。
	Cooldown time.Duration
	// Now 是时钟，默认 time.Now。
	Now func() time.Time
	// OnChange 在状态变化后调用（不持有锁），用来记日志。并发的变化之间不保证调用的先后。
	OnChange func(name string, from, to State)
}

// Breaker 是一个熔断器。并发安全。零值不可用，用 New 创建。
type Breaker struct {
	name     string
	failures int
	cooldown time.Duration
	now      func() time.Time
	onChange func(name string, from, to State)

	mu    sync.Mutex
	state State
	// fails 是关闭状态下的连续失败次数。
	fails int
	// openedAt 是进入打开状态（或者打开状态下最近一次被报告失败）的时刻。
	openedAt time.Time
	// gen 在每次状态变化时加一。Allow 放行时记下当时的 gen，结果回来时 gen 变了就丢弃：
	// 那是上一个状态里发出去的调用，它的结果不应该影响现在的状态。
	gen uint64
	// probing 表示半开状态下已经放出去一个探测，probeAt 是放出去的时刻。
	probing bool
	probeAt time.Time
	// opens 是进入打开状态的累计次数，rejected 是没有放行的累计次数。
	opens, rejected int64
}

// New 返回一个处于关闭状态的熔断器。
//
//nolint:contextcheck // 见 metricCtx：记指标不需要调用方的 ctx
func New(cfg Config) *Breaker {
	b := &Breaker{
		name:     cfg.Name,
		failures: cfg.Failures,
		cooldown: cmp.Or(cfg.Cooldown, DefaultCooldown),
		now:      cfg.Now,
		onChange: cfg.OnChange,
	}
	if b.failures <= 0 {
		b.failures = DefaultFailures
	}
	if b.now == nil {
		b.now = time.Now
	}
	// 一建出来就报一次“关闭”：从没变过状态的熔断器在后端里也要有一条线，否则分不清“一直是好的”和“没接上”。
	stateGauge.Record(metricCtx, int64(Closed), metric.WithAttributes(attribute.String("breaker", b.name)))
	return b
}

// Name 返回创建时给的名字。
func (b *Breaker) Name() string { return b.name }

// Allow 申请一次调用。
//
// 放行时返回 done，调用结束后要调用 done 报告结果（多次调用只有第一次有效）。
// 不放行时返回 ErrOpen，done 为 nil。
func (b *Breaker) Allow() (done func(Outcome), err error) {
	done, _, err = b.AllowProbe()
	return done, err
}

// AllowProbe 和 Allow 相同，另外用 probe 告诉调用方这一次是不是半开状态下的那个探测。
// 给“探测的结果要过一会儿才知道”的调用方用，见 Report。
func (b *Breaker) AllowProbe() (done func(Outcome), probe bool, err error) {
	b.mu.Lock()
	now := b.now()
	from := b.state
	if b.state == Open && now.Sub(b.openedAt) >= b.cooldown {
		b.setState(HalfOpen)
	}
	switch b.state {
	case Open:
		b.rejected++
		b.mu.Unlock()
		return nil, false, ErrOpen
	case HalfOpen:
		if b.probing && now.Sub(b.probeAt) < b.cooldown {
			b.rejected++
			to := b.state
			b.mu.Unlock()
			b.notify(from, to)
			return nil, false, ErrOpen
		}
		b.probing, b.probeAt = true, now
	case Closed:
	}
	gen, to := b.gen, b.state
	b.mu.Unlock()
	b.notify(from, to)

	var once sync.Once
	return func(o Outcome) { once.Do(func() { b.record(gen, o) }) }, to == HalfOpen, nil
}

// record 处理一次被放行的调用的结果。
func (b *Breaker) record(gen uint64, o Outcome) {
	b.mu.Lock()
	if gen != b.gen {
		b.mu.Unlock()
		return
	}
	from := b.state
	switch o {
	case Success:
		b.succeeded()
	case Failure:
		b.failed()
	case Ignored:
		if b.state == HalfOpen {
			b.probing = false
		}
	}
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
}

// Report 被动地告诉熔断器依赖的一次结果，用在“调用不是经过 Allow 发出去的”场合。
//
// 例子是 order 的落库健康（设计文档 1d 的 6.5）：受理时用 Allow 判断要不要拒绝，而依赖是否正常
// 是从落库消费者的处理结果里看出来的，两者不是同一次调用。
//
// 和 Allow 返回的 done 有两点不同：
//   - 成功在任何状态下都直接回到关闭。报告的是刚刚发生的事实，不存在“上一个状态里发出去的调用”。
//     打开期间消费者的重试成功了，就是最好的探测。
//   - 打开状态下报告失败会把冷却重新算起。只要失败还在不断发生，就不进入半开、不放探测。
func (b *Breaker) Report(ok bool) {
	b.mu.Lock()
	from := b.state
	switch {
	case ok:
		b.succeeded()
	case b.state == Open:
		b.openedAt = b.now()
	default:
		b.failed()
	}
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
}

// succeeded 和 failed 在持有锁时调用。
func (b *Breaker) succeeded() {
	b.fails = 0
	if b.state != Closed {
		b.setState(Closed)
	}
}

func (b *Breaker) failed() {
	switch b.state {
	case Closed:
		b.fails++
		if b.fails >= b.failures {
			b.setState(Open)
		}
	case HalfOpen:
		b.setState(Open)
	case Open:
	}
}

// setState 在持有锁时调用。
//
//nolint:contextcheck // 见 metricCtx：记指标不需要调用方的 ctx
func (b *Breaker) setState(s State) {
	b.state = s
	// 在锁里报状态：放到解锁之后的话，两次紧挨着的变化可能后发生的先报，指标停在旧状态上。Record 不阻塞。
	stateGauge.Record(metricCtx, int64(s), metric.WithAttributes(attribute.String("breaker", b.name)))
	b.gen++
	b.fails = 0
	b.probing = false
	if s == Open {
		b.openedAt = b.now()
		b.opens++
	}
}

//nolint:contextcheck // 见 metricCtx：记指标不需要调用方的 ctx
func (b *Breaker) notify(from, to State) {
	if from == to {
		return
	}
	transitions.Add(metricCtx, 1, metric.WithAttributes(attribute.String("breaker", b.name), attribute.String("to", to.String())))
	if b.onChange != nil {
		b.onChange(b.name, from, to)
	}
}

// 指标（设计文档 1d 的 8.8）：每个熔断器现在的状态，和状态变化的次数。标签只有名字和状态，都是有限的。
// 全局的 meter 是代理，otelx.Setup 之前建的仪表在那之后自动生效；没有装配时是空操作。
var (
	meter = otel.Meter("github.com/palemoky/oppenheimer/pkg/breaker")
	// 0 关闭、1 打开、2 半开，和 State 的取值一致。
	stateGauge, _ = meter.Int64Gauge("breaker.state",
		metric.WithDescription("熔断器的状态：0 关闭，1 打开，2 半开"))
	transitions, _ = meter.Int64Counter("breaker.transitions",
		metric.WithDescription("熔断器状态变化的次数，按变化后的状态分"))
)

// metricCtx 是记指标时用的 ctx。状态变化发生在 Allow、Report 这些不带 ctx 的方法里；
// 指标的 ctx 只用来带 baggage 和采样 exemplar，这里都用不到。
var metricCtx = context.Background()

// State 返回当前状态。打开状态在冷却时间过后要等下一次 Allow 才会变成半开，
// 所以长时间没有调用时这里看到的仍然是 Open。
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Stats 是累计的计数，给状态展示和测试用。
type Stats struct {
	State State
	// Opens 是进入打开状态的次数，Rejected 是没有放行的调用数。
	Opens, Rejected int64
}

// Stats 返回当前状态和累计的计数。
func (b *Breaker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{State: b.state, Opens: b.opens, Rejected: b.rejected}
}

// Do 在熔断器放行时执行 fn，并用 classify 把它返回的错误归类后报告。不放行时返回 ErrOpen，不执行 fn。
//
// classify 只在 fn 返回非 nil 的错误时调用。哪些错误算依赖的失败只有调用方知道：
// 业务上的“不存在”是 Success，调用方自己的 ctx 结束是 Ignored，其余通常是 Failure。
func (b *Breaker) Do(fn func() error, classify func(error) Outcome) error {
	done, err := b.Allow()
	if err != nil {
		return err
	}
	err = fn()
	if err == nil {
		done(Success)
		return nil
	}
	done(classify(err))
	return err
}
