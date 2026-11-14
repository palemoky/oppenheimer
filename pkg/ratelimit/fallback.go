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
	// DefaultCooldown 是 Primary 出错之后多久不再问它。
	DefaultCooldown = time.Second
)

// FallbackConfig 是 Fallback 的配置。
type FallbackConfig struct {
	// Primary 是平时用的限流器（Redis），Secondary 是它出错时用的（Local）。都必填。
	Primary, Secondary Limiter
	// Timeout 是问一次 Primary 的时限，默认 DefaultPrimaryTimeout。
	Timeout time.Duration
	// Cooldown 是 Primary 出错之后多久不再问它，默认 DefaultCooldown。
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
// Primary 出错之后的 Cooldown 内直接用 Secondary，不再去问：否则 Primary 挂着的时候每个请求都要先等满
// Timeout 才退回，所有请求平白多出这么多延迟。Cooldown 过了放下一个请求去试一次。
// 这是一个最小的熔断器：出错即打开，过一段时间半开试探。它不数失败率，一次失败就打开，
// 因为退回的代价很小（只是限额变宽），不值得为了少退回几次去多等几个超时。
//
// Primary 和 Secondary 各有各的桶，互不相通：退回期间 Secondary 里的桶从满的开始算，
// 回到 Primary 时它那边的桶也已经补了一段时间。两头都是偏宽松，不会比配置的更严。
type Fallback struct {
	primary, secondary Limiter
	timeout, cooldown  time.Duration
	log                *slog.Logger
	now                func() time.Time

	// openUntil 是在这个时刻（UnixNano）之前不问 Primary。0 表示 Primary 正常。
	openUntil atomic.Int64
	// fallbacks 是退回的累计次数，给测试和监控用。
	fallbacks atomic.Int64
}

// NewFallback 返回 Fallback。
func NewFallback(cfg FallbackConfig) *Fallback {
	f := &Fallback{
		primary: cfg.Primary, secondary: cfg.Secondary,
		timeout: cfg.Timeout, cooldown: cfg.Cooldown,
		log: cfg.Logger, now: cfg.Now,
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
		if until != 0 && f.openUntil.CompareAndSwap(until, 0) {
			f.log.InfoContext(ctx, "rate limiter primary is back")
		}
		return d, nil
	}
	if ctx.Err() != nil {
		// 是调用方自己取消或到期，不是 Primary 的问题，不为此打开。
		return Decision{}, ctx.Err()
	}
	// 只有把状态从“正常”或“已到期”改成“打开”的那一个请求记日志，其余并发失败的不重复记。
	if f.openUntil.CompareAndSwap(until, now.Add(f.cooldown).UnixNano()) {
		f.log.WarnContext(ctx, "rate limiter primary failed, falling back to the local limiter",
			"cooldown", f.cooldown.String(), "error", err)
	}
	f.fallbacks.Add(1)
	return f.secondary.Allow(ctx, reqs...)
}

// Fallbacks 返回退回 Secondary 的累计次数。
func (f *Fallback) Fallbacks() int64 { return f.fallbacks.Load() }
