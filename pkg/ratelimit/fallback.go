package ratelimit

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// 默认值。
const (
	// DefaultPrimaryTimeout 是问一次 Primary 最多等多久。限流在每个请求的路径上，它慢了所有请求都慢。
	DefaultPrimaryTimeout = 20 * time.Millisecond
	// DefaultCooldown 是 Primary 被判定为不可用之后多久不再问它。
	DefaultCooldown = time.Second
	// DefaultFailures 是 Primary 连续失败多少次才判定为不可用，理由见 Fallback。
	DefaultFailures = 5
	// DefaultFailureSpan 是这一串连续的失败至少要持续多久才判定为不可用，理由见 Fallback。
	DefaultFailureSpan = 200 * time.Millisecond
)

// FallbackConfig 是 Fallback 的配置。
type FallbackConfig struct {
	// Primary 是平时用的限流器（Redis），Secondary 是它出错时用的（Local）。都必填。
	Primary, Secondary Limiter
	// Timeout 是问一次 Primary 的时限，默认 DefaultPrimaryTimeout。
	Timeout time.Duration
	// Failures 是 Primary 连续失败多少次才打开（之后的 Cooldown 内不再问它），默认 DefaultFailures。
	// 取 1 就是“一次失败即打开”。
	Failures int
	// FailureSpan 是这一串连续的失败（从第一次失败算起，中间没有一次成功）至少要持续多久才打开，
	// 默认 DefaultFailureSpan。负数表示不看持续时间，只数次数。
	FailureSpan time.Duration
	// Cooldown 是打开之后多久不再问 Primary，默认 DefaultCooldown。
	Cooldown time.Duration
	// Logger 为 nil 时用 slog.Default()。
	Logger *slog.Logger
	// Now 是时钟，默认 time.Now。
	Now func() time.Time
}

// Fallback 先问 Primary，它出错或超时就退回 Secondary。并发安全。
//
// 用在“Redis 里的限流器不可用时”：放行等于限流在最需要的时候消失（Redis 往往就是被流量打垮的），
// 拒绝等于给调用方加了一个单点。退回进程内的限流器，限额变宽（每个实例各算各的）但没有消失。
//
// 一次失败只让这一个请求退回。Primary 连续失败 Failures 次才“打开”：之后的 Cooldown 内直接用 Secondary，
// 不再去问，否则 Primary 挂着的时候每个请求都要先等满 Timeout 才退回，所有请求平白多出这么多延迟。
// Cooldown 过了放请求去试（半开）：成功就合上，失败就马上重新打开，不用再数满 Failures 次。
//
// 为什么要数连续失败，不是一次失败就打开（2026-11-23 以前是后者）：打开的代价是这个实例之后一整个
// Cooldown 的请求全部不走 Primary。请求很多的时候，零星的超时一定会有（节点 CPU 到九成时每秒几百个请求里
// 总有一个超过 20 毫秒），于是每秒 3 次超时换来每秒 800 次退回，一半的请求在系统最忙的时候用的是
// 各实例各算各的限额（3c 的压测，设计文档 3c 第 8 节“第 14 项的追查”第 1 条）。Primary 真的不可用时
// 失败是成片的，连续 5 次只要几个请求的工夫；零星的超时之间隔着几百次成功，数不到 5。
// 代价：Primary 刚挂掉的那一刻，头几个请求各自等满一次 Timeout（并发的请求同时在等，不是排着队等）。
//
// 为什么还要看这一串失败持续了多久（FailureSpan，2026-11-23 加）：只数次数的判据隐含“请求是一个接一个来的”。
// 一个实例每秒八百多个请求时，同一刻有十几到几十个在问 Primary；节点卡一下（几十毫秒拿不到 CPU），
// 这些在途的请求一起超时，“连续 5 次”一瞬间就数满了，而卡顿过去之后 Primary 是好的。只数次数的那一版
// 在过载压测里把退回从占请求的 47% 降到 21%，没有降到零（设计文档 3c 第 8 节“重跑打过拐点的压测”）。
// 成片的并发失败和真的不可用，差别在持续的时间：卡顿是几十毫秒，其间一有成功这一串就断了；
// 不可用是一直失败。所以两个条件都要：次数够（不是偶然的一两次），并且从第一次失败起已经过了 FailureSpan。
// 被动数失败的判据要和调用方的并发度比，pkg/breaker 的落库健康检查是同一个道理（那里叫 FailureSpan）。
// 代价：真的不可用时晚 FailureSpan 才打开，这段时间的请求各自等满一次 Timeout。
// 不用 pkg/breaker：它的冷却是 5 秒、带探测名额，是给“依赖坏了要保护调用方”用的；这里只要一个计数，
// 而且在每个请求的路径上，保持无锁。
//
// Primary 和 Secondary 各有各的桶，互不相通：退回期间 Secondary 里的桶从满的开始算，
// 回到 Primary 时它那边的桶也已经补了一段时间。两头都是偏宽松，不会比配置的更严。
type Fallback struct {
	primary, secondary Limiter
	timeout, cooldown  time.Duration
	failures           int32
	failureSpan        time.Duration
	log                *slog.Logger
	now                func() time.Time

	// openUntil 是在这个时刻（UnixNano）之前不问 Primary。0 表示 Primary 正常。
	openUntil atomic.Int64
	// fails 是 Primary 连续失败的次数，成功一次清零。并发的请求各自加减，“连续”指的是
	// 两次成功之间完成的失败数，不是严格的先后顺序。
	fails atomic.Int32
	// failSince 是这一串连续失败里第一次失败的时刻（UnixNano），成功一次清零。0 表示现在没有失败。
	failSince atomic.Int64
	// fallbacks 是退回的累计次数，给测试和监控用。
	fallbacks atomic.Int64
}

// NewFallback 返回 Fallback。
func NewFallback(cfg FallbackConfig) *Fallback {
	f := &Fallback{
		primary: cfg.Primary, secondary: cfg.Secondary,
		timeout: cfg.Timeout, cooldown: cfg.Cooldown,
		failures: int32(min(max(cfg.Failures, 0), 1<<20)), //nolint:gosec // 已经夹在 int32 的范围内
		log:      cfg.Logger, now: cfg.Now,
	}
	if f.failures == 0 {
		f.failures = DefaultFailures
	}
	f.failureSpan = cfg.FailureSpan
	if f.failureSpan == 0 {
		f.failureSpan = DefaultFailureSpan
	}
	if f.timeout <= 0 {
		f.timeout = DefaultPrimaryTimeout
	}
	if f.cooldown <= 0 {
		f.cooldown = DefaultCooldown
	}
	if f.log == nil {
		f.log = slog.Default()
	}
	if f.now == nil {
		f.now = time.Now
	}
	return f
}

// Allow 实现 Limiter。
func (f *Fallback) Allow(ctx context.Context, reqs ...Request) (Decision, error) {
	now := f.now()
	until := f.openUntil.Load()
	if until != 0 && now.UnixNano() < until {
		f.fallbacks.Add(1)
		return f.secondary.Allow(ctx, reqs...)
	}

	pctx, cancel := context.WithTimeout(ctx, f.timeout)
	d, err := f.primary.Allow(pctx, reqs...)
	cancel()
	if err == nil {
		f.fails.Store(0)
		f.failSince.Store(0)
		if until != 0 && f.openUntil.CompareAndSwap(until, 0) {
			f.log.InfoContext(ctx, "rate limiter primary is back")
		}
		return d, nil
	}
	if ctx.Err() != nil {
		// 是调用方自己取消或到期，不是 Primary 的问题，不为此打开。
		return Decision{}, ctx.Err()
	}
	f.fallbacks.Add(1)
	// until != 0 是半开时的试探失败了：马上重新打开。否则要连续失败满 failures 次、并且这一串失败
	// 已经持续了 failureSpan 才打开；不满足的只是这一个请求退回，下一个请求照常去问 Primary。
	// since 用 CAS 写：并发失败的请求里只有第一个把起点记下来。和成功的清零有竞争（清零之后又被一个更早开始、
	// 刚失败的请求写上），后果只是起点偏晚或偏早几毫秒，不影响“一有成功就重新数”的大意。
	n := f.fails.Add(1)
	since := f.failSince.Load()
	if since == 0 {
		// 0 是“没有失败”的记号，时钟正好是 0（测试里的假时钟）时记成 1。
		f.failSince.CompareAndSwap(0, max(now.UnixNano(), 1))
		since = f.failSince.Load()
	}
	lasted := f.failureSpan < 0 || (since != 0 && now.UnixNano()-since >= int64(f.failureSpan))
	if until == 0 && (n < f.failures || !lasted) {
		return f.secondary.Allow(ctx, reqs...)
	}
	// 只有把状态从“正常”或“已到期”改成“打开”的那一个请求记日志，其余并发失败的不重复记。
	if f.openUntil.CompareAndSwap(until, now.Add(f.cooldown).UnixNano()) {
		f.log.WarnContext(ctx, "rate limiter primary failed, falling back to the local limiter",
			"cooldown", f.cooldown.String(), "consecutive_failures", n, "error", err)
	}
	return f.secondary.Allow(ctx, reqs...)
}

// Fallbacks 返回退回 Secondary 的累计次数。
func (f *Fallback) Fallbacks() int64 { return f.fallbacks.Load() }
