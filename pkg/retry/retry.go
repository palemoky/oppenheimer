// Package retry 提供带指数退避和抖动的重试。
//
// 抖动的目的是避免大量调用方在同一时刻失败后又在同一时刻重试（惊群），
// 把重试流量在时间上打散。
package retry

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

const (
	defaultMaxAttempts = 3
	defaultInitial     = 50 * time.Millisecond
	defaultMax         = 5 * time.Second
	defaultMultiplier  = 2.0
	defaultJitter      = 0.2
)

// randFloat 返回 [0,1) 的随机数。做成变量是为了让测试能注入固定值验证抖动边界。
var randFloat = rand.Float64

// Policy 描述重试策略。零值字段取默认值。
type Policy struct {
	// MaxAttempts 是总尝试次数（含第一次），<=0 取默认值 3。
	MaxAttempts int
	// Initial 是第一次重试前的间隔，<=0 取默认值 50ms。
	Initial time.Duration
	// Max 是间隔上限，<=0 取默认值 5s。抖动之后的间隔也不会超过它。
	Max time.Duration
	// Multiplier 是每次间隔的倍数，<=0 取默认值 2。
	Multiplier float64
	// Jitter 是抖动比例 0~1，实际间隔在 [d*(1-Jitter), d*(1+Jitter)] 内均匀取值。
	// 0 取默认值 0.2（所以零值 Policy 带抖动）；负数表示关闭抖动；大于 1 按 1 处理。
	Jitter float64
	// Retryable 判断错误是否值得重试；nil 表示所有非 nil 错误都重试。
	Retryable func(error) bool
}

// withDefaults 返回把零值字段替换成默认值后的副本。
func (p Policy) withDefaults() Policy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = defaultMaxAttempts
	}
	if p.Initial <= 0 {
		p.Initial = defaultInitial
	}
	if p.Max <= 0 {
		p.Max = defaultMax
	}
	if p.Multiplier <= 0 {
		p.Multiplier = defaultMultiplier
	}
	switch {
	case p.Jitter == 0:
		p.Jitter = defaultJitter
	case p.Jitter < 0:
		p.Jitter = 0
	default:
		p.Jitter = min(p.Jitter, 1)
	}
	return p
}

// Backoff 返回第 attempt 次失败之后应当等待的间隔（attempt 从 1 开始），含抖动。
// 给不能阻塞等待的调用方用，例如把“下次重试时间”写进数据库。
// attempt 很大时不会溢出：先用 float64 计算再封顶在 Max，结果不会为负。
func Backoff(p Policy, attempt int) time.Duration {
	return backoff(p.withDefaults(), attempt)
}

// backoff 要求 p 已经过 withDefaults。withDefaults 不是幂等的（Jitter<0 会变成 0，
// 再次处理又会变成默认值），所以内部路径不能重复调用它。
func backoff(p Policy, attempt int) time.Duration {
	attempt = max(attempt, 1)

	// 用 float64 计算：Pow 溢出得到 +Inf，被 min 截到 Max，不会发生整数溢出。
	d := min(float64(p.Initial)*math.Pow(p.Multiplier, float64(attempt-1)), float64(p.Max))
	if p.Jitter > 0 {
		// randFloat()∈[0,1) → 系数∈[1-J, 1+J)
		d *= 1 - p.Jitter + 2*p.Jitter*randFloat()
	}
	d = min(max(d, 0), float64(p.Max))
	return time.Duration(d)
}

// Do 调用 fn，失败且可重试时按策略等待后再试。fn 收到的 attempt 从 1 开始。
//
//   - fn 返回 nil 立即返回 nil。
//   - 错误不可重试时立即原样返回。
//   - 用尽次数后原样返回最后一次的错误。
//   - 等待期间 ctx 取消立即返回，错误同时匹配 ctx.Err() 和最后一次 fn 的错误。
//   - 调用 fn 之前 ctx 已取消则不调用 fn，返回 ctx.Err()。
func Do(ctx context.Context, p Policy, fn func(ctx context.Context, attempt int) error) error {
	p = p.withDefaults()
	var last error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return joinCtx(err, last)
		}
		last = fn(ctx, attempt)
		if last == nil {
			return nil
		}
		if p.Retryable != nil && !p.Retryable(last) {
			return last
		}
		if attempt == p.MaxAttempts {
			break
		}
		if err := sleep(ctx, backoff(p, attempt)); err != nil {
			return errors.Join(err, last)
		}
	}
	return last
}

// joinCtx 在有上一次错误时把它和 ctx 错误合并，保证两者都能被 errors.Is 匹配。
func joinCtx(ctxErr, last error) error {
	if last == nil {
		return ctxErr
	}
	return errors.Join(ctxErr, last)
}

// sleep 等待 d，期间 ctx 取消则提前返回 ctx.Err()。
// 不用 time.Sleep 是因为它无法被取消。
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
