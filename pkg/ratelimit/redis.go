package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// allowScript 对 KEYS 里的每个桶做一次判断：都有令牌才各扣一个，任何一个没有就都不扣。
// ARGV 是每个桶的 (rate, burst)，顺序与 KEYS 对应。返回 {是否通过, 第一个没有令牌的桶的序号（从 1 起）, 要等多少微秒}。
//
// 整个脚本在 Redis 里原子执行：两个请求不会同时读到同一个令牌，“先都看一遍、都够再都扣”之间也插不进别的请求。
//
// 时间用 Redis 自己的 TIME，不由调用方传入：各个网关实例的时钟有偏差，用各自的时间去算“流逝了多久”，
// 时钟快的实例会多补令牌，时钟慢的会把 ts 往回写。Redis 7 起脚本按效果复制，脚本里调用 TIME 是允许的。
//
// 每个桶一个 hash：tokens（令牌数）和 ts（上次计算的时刻，微秒）。ts 显式用 %.0f 写成整数。
// Lua 自己的 tostring 只保留 14 位有效数字，而微秒时间戳有 16 位；实测 Redis 把数字参数写入时
// 对整数值会按整数格式化，不丢精度，所以这里的格式化不是必需的。保留它是为了不依赖这个没有写进
// 文档的行为：ts 差 100 微秒，在高速率的桶上就是可见的误差。
//
// 过期时间是“从空到满要多久”再加一秒：过期的桶等于满桶，和它自然补满的结果一样，
// 所以过期不改变行为，只是回收不活跃的键。被内存淘汰掉也是同样的效果。
var allowScript = redis.NewScript(`
local t = redis.call('TIME')
local now = t[1] * 1000000 + t[2]
local n = #KEYS
local tokens = {}
local denied = 0
local retry = 0
for i = 1, n do
  local rate = tonumber(ARGV[2 * i - 1])
  local burst = tonumber(ARGV[2 * i])
  local v = redis.call('HMGET', KEYS[i], 'tokens', 'ts')
  local tk = tonumber(v[1])
  local ts = tonumber(v[2])
  if tk == nil or ts == nil then
    tk = burst
  else
    local elapsed = now - ts
    if elapsed < 0 then elapsed = 0 end
    tk = tk + elapsed * rate / 1000000
    if tk > burst then tk = burst end
  end
  tokens[i] = tk
  if tk < 1 then
    if denied == 0 then denied = i end
    local w = math.ceil((1 - tk) * 1000000 / rate)
    if w > retry then retry = w end
  end
end
if denied ~= 0 then
  return {0, denied, retry}
end
for i = 1, n do
  local rate = tonumber(ARGV[2 * i - 1])
  local burst = tonumber(ARGV[2 * i])
  redis.call('HSET', KEYS[i], 'tokens', string.format('%.6f', tokens[i] - 1), 'ts', string.format('%.0f', now))
  redis.call('PEXPIRE', KEYS[i], math.ceil(burst / rate * 1000) + 1000)
end
return {1, 0, 0}
`)

// Redis 是放在 Redis 里的令牌桶：所有实例共用一份状态。并发安全。
//
// 对客户端的两个要求（go-redis 的默认值都不满足）：
//   - 开 ContextTimeoutEnabled。不开的话命令不看 ctx 的截止时间，Fallback 给的时限（默认 20 毫秒）无效，
//     Redis 卡住时每个请求要等满客户端的 ReadTimeout。
//   - 不要自动重试（MaxRetries 设为 -1）。扣令牌的脚本不是幂等的：第一次执行成功而回复超时，
//     客户端重试会再扣一个。多扣一个令牌本身无伤大雅，但重试会把一次判断的耗时拉长到时限的好几倍。
//
// 单节点和主从下，一次调用里的几个桶在同一个脚本里处理：要么每个桶各扣一个令牌，要么一个都不扣。
//
// Redis Cluster 下做不到这一点。一个脚本只能访问同一个槽里的键，而按接口、按用户、按 IP 的桶键名不同、
// 落在不同的槽；它们也不能用 hash tag 绑到一起，因为同一个用户的桶要被他所有的请求共用，不能跟着某一次
// 请求的其他键走。所以 Cluster 下每个桶单独调用一次脚本，按调用方给的顺序，遇到第一个拒绝就停。
// 代价：排在前面的桶已经扣了令牌，而请求被后面的桶拒绝了，前面的令牌白扣。它让限流在被拒绝时略微偏严，
// 不会偏松；调用方把最容易拒绝的桶放在前面可以减少这种情况。往返次数也从一次变成最多“桶的个数”次。
// 见设计文档 2b 的 4.4、ADR-0019 的补充。
type Redis struct {
	rdb    redis.Scripter
	prefix string
	// perKey 为真时每个桶单独调用脚本（Cluster）。
	perKey bool
}

// NewRedis 返回 Redis。prefix 加在每个键前面，用来隔离环境。
// rdb 是 *redis.ClusterClient 时按桶逐个调用，见 Redis 的说明。
func NewRedis(rdb redis.Scripter, prefix string) *Redis {
	_, cluster := rdb.(*redis.ClusterClient)
	return &Redis{rdb: rdb, prefix: prefix, perKey: cluster}
}

// Allow 实现 Limiter。Redis 不可用或 ctx 到期时返回错误，这时没有令牌被扣（或者扣了而调用方不知道，
// 两种情况调用方都应当自己决定放行、拒绝还是换一个限流器，见 Fallback）。
func (r *Redis) Allow(ctx context.Context, reqs ...Request) (Decision, error) {
	reqs, index, err := active(reqs)
	if err != nil {
		return Decision{}, err
	}
	if len(reqs) == 0 {
		return allowed, nil
	}
	if r.perKey && len(reqs) > 1 {
		for i, q := range reqs {
			res, err := r.run(ctx, []string{r.prefix + "rl:" + q.Key}, q.Limit.Rate, q.Limit.Burst)
			if err != nil {
				return Decision{}, err
			}
			if res[0] != 1 {
				return Decision{Denied: index[i], RetryAfter: time.Duration(res[2]) * time.Microsecond}, nil
			}
		}
		return allowed, nil
	}
	keys := make([]string, len(reqs))
	args := make([]any, 0, 2*len(reqs))
	for i, q := range reqs {
		keys[i] = r.prefix + "rl:" + q.Key
		args = append(args, q.Limit.Rate, q.Limit.Burst)
	}
	res, err := r.run(ctx, keys, args...)
	if err != nil {
		return Decision{}, err
	}
	if res[0] == 1 {
		return allowed, nil
	}
	if res[1] < 1 || int(res[1]) > len(index) {
		return Decision{}, errors.New("ratelimit: redis: denied bucket out of range")
	}
	return Decision{Denied: index[res[1]-1], RetryAfter: time.Duration(res[2]) * time.Microsecond}, nil
}

// run 执行一次脚本，返回它的三个数：是否放行、被哪个桶拒绝（从 1 起）、要等多少微秒。
func (r *Redis) run(ctx context.Context, keys []string, args ...any) ([]int64, error) {
	res, err := allowScript.Run(ctx, r.rdb, keys, args...).Int64Slice()
	if err != nil {
		return nil, fmt.Errorf("ratelimit: redis: %w", err)
	}
	if len(res) != 3 {
		return nil, errors.New("ratelimit: redis: unexpected script reply")
	}
	return res, nil
}
