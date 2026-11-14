// Package ratelimit 是令牌桶限流：一个算法，两个存放状态的地方。
//
//   - Local 把桶放在进程里：没有网络往返，不依赖任何东西；多个实例各算各的。
//   - Redis 把桶放在 Redis 里：所有实例共用一份，限额精确；每次判断一次往返。
//   - Fallback 把两者接起来：先问前者，它出错时退回后者。
//
// 令牌桶：每个键一个桶，容量 Burst，以每秒 Rate 个的速度补充，一个请求取一个令牌，取不到就拒绝。
// 状态只有两个数（令牌数、上次计算的时间），补充是在取的时候按流逝的时间算出来的，没有后台任务。
// Rate 表达长期的平均速率，Burst 表达允许的突发，两件事分开配。
//
// 设计见 docs/design/1d-protection.md 第 4 节，决定见 ADR-0019。
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

// Limit 是一个桶的参数。零值表示不限。
type Limit struct {
	// Rate 是每秒补充的令牌数。
	Rate float64
	// Burst 是桶的容量，也就是一口气最多能通过多少个请求。
	Burst int
}

// Enabled 报告这个限制是否生效。Rate 或 Burst 不为正时不限。
func (l Limit) Enabled() bool { return l.Rate > 0 && l.Burst > 0 }

// String 返回 "速率/容量"，与 Parse 互逆。
func (l Limit) String() string {
	if !l.Enabled() {
		return "off"
	}
	return fmt.Sprintf("%g/%d", l.Rate, l.Burst)
}

// maxRate 和 maxBurst 是参数的上限，防止配置里的笔误（多打几个零）让浮点运算和过期时间溢出。
const (
	maxRate  = 1e9
	maxBurst = 1 << 30
)

// Validate 检查参数。不限（零值）是合法的。
func (l Limit) Validate() error {
	switch {
	case l == Limit{}:
		return nil
	case math.IsNaN(l.Rate) || l.Rate <= 0 || l.Rate > maxRate:
		return fmt.Errorf("ratelimit: rate %v out of range (0, %g]", l.Rate, float64(maxRate))
	case l.Burst <= 0 || l.Burst > maxBurst:
		return fmt.Errorf("ratelimit: burst %d out of range [1, %d]", l.Burst, maxBurst)
	}
	return nil
}

// Request 是对一个桶的一次取令牌：Key 标识桶，Limit 是它的参数。
type Request struct {
	Key   string
	Limit Limit
}

// Decision 是一次判断的结果。
type Decision struct {
	// Allowed 为 true 时，每个桶都已经扣掉一个令牌。
	Allowed bool
	// Denied 是第一个没有令牌的桶在请求里的下标；Allowed 为 true 时是 -1。
	Denied int
	// RetryAfter 是多久之后这次请求才可能通过：所有没有令牌的桶里等得最久的那个。Allowed 为 true 时是 0。
	// 它是下界：这段时间里别的请求也在取令牌。
	RetryAfter time.Duration
}

// Limiter 判断一组桶是否都有令牌。
//
// 全有或全无：所有桶都有令牌才各扣一个，任何一个没有就都不扣。调用方用它同时检查
// “这个用户”和“这个接口”：请求被接口的总量拒绝时，不应当白白消耗用户的配额。
// 不限（Limit 零值）的请求被忽略。一个请求都没有时直接通过。同一次调用里的 Key 不能重复。
type Limiter interface {
	Allow(ctx context.Context, reqs ...Request) (Decision, error)
}

var allowed = Decision{Allowed: true, Denied: -1}

// errDuplicateKey 表示同一次调用里有两个请求指向同一个桶。那样“先都看一遍、都够再都扣”
// 会让它被扣两次而只检查了一次，所以直接拒绝这种用法。
var errDuplicateKey = errors.New("ratelimit: duplicate key in one call")

// active 返回生效的请求和它们在原请求里的下标，并检查参数。
func active(reqs []Request) (out []Request, index []int, err error) {
	for i, r := range reqs {
		if err := r.Limit.Validate(); err != nil {
			return nil, nil, err
		}
		if !r.Limit.Enabled() {
			continue
		}
		if r.Key == "" {
			return nil, nil, errors.New("ratelimit: empty key")
		}
		for _, seen := range out {
			if seen.Key == r.Key {
				return nil, nil, errDuplicateKey
			}
		}
		out = append(out, r)
		index = append(index, i)
	}
	return out, index, nil
}

// refill 返回桶在 elapsed 之后的令牌数：加上这段时间补充的，不超过容量。
// 两个实现共用同一个公式（Redis 版在 Lua 里是同样的算术），所以它们过的是同一组测试。
func refill(tokens float64, elapsed time.Duration, l Limit) float64 {
	if elapsed < 0 {
		// 时钟回拨：当作没有时间流逝，不倒扣。
		elapsed = 0
	}
	return math.Min(float64(l.Burst), tokens+elapsed.Seconds()*l.Rate)
}

// wait 返回令牌数为 tokens（不足 1）的桶还要多久才有一个令牌。
func wait(tokens float64, l Limit) time.Duration {
	return time.Duration(math.Ceil((1 - tokens) / l.Rate * float64(time.Second)))
}
