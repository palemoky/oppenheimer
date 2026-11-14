// Package cache 是读穿透式的查询缓存：未命中时回源，并按回源方给出的 TTL 写入。
//
// 它针对三个经典问题各做了一件事：
//   - 穿透（查不存在的数据）：把“不存在”也缓存下来（空值）。
//   - 击穿（热点 key 过期的瞬间大量请求同时回源）：进程内用 singleflight 合并，
//     进程间用一把短期的 Redis 锁让一个实例回源、其余实例等结果。
//   - 雪崩（大量 key 同时过期，或 Redis 整体不可用）：TTL 加随机抖动；
//     Redis 出错时直接回源，错误只记日志不向上返回。
//
// 没有 Delete：这个包只用来缓存“不会再变的”和“旧了也无所谓的”数据（ADR-0010）。
// 要缓存会变而且旧值有后果的数据，得先回答失效窗口有多大，再来加删除。
//
// 缓存应当使用允许淘汰的 Redis 实例（allkeys-lru），不要和库存闸门、分布式锁共用：
// 那些 key 被淘汰就是数据丢失。
package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"golang.org/x/sync/singleflight"

	"github.com/palemoky/oppenheimer/pkg/breaker"
	"github.com/palemoky/oppenheimer/pkg/degrade"
)

// 存进 Redis 的值的第一个字节是标记，用来区分“缓存了不存在”和“缓存了一个值”。
// 不用空字符串表示不存在：有的编码（例如所有字段都是零值的 proto 消息）本身就是空的。
const (
	tagNull  byte = 0
	tagValue byte = 1
)

// readTimeout 是 Get 进来时读一次缓存的时限。正常的往返在 1 毫秒以内，200 毫秒容得下一次 TCP 重传。
const readTimeout = 200 * time.Millisecond

// redisWarnEvery 是 Redis 持续出错期间重复告警的最小间隔。
const redisWarnEvery = 10 * time.Second

// randFloat 返回 [0,1) 的随机数。做成变量是为了让测试能注入固定值验证抖动边界。
var randFloat = mrand.Float64

// unlockScript 只在锁仍是自己的时才删除，理由同 pkg/lock 的 releaseScript。
var unlockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`)

// Codec 负责值的序列化。
type Codec[T any] interface {
	Marshal(v T) ([]byte, error)
	Unmarshal(data []byte) (T, error)
}

// Config 是 Cache 的配置。零值的可选项取默认值。
type Config struct {
	// Client 必填。只用到 GET、SET 和脚本。
	Client redis.Cmdable
	// KeyPrefix 遵守 redisx 的前缀约定。
	KeyPrefix string
	// Jitter 是 TTL 的抖动比例，实际 TTL 落在 [TTL×(1-Jitter), TTL×(1+Jitter))。
	// 默认 0.2；负数表示不抖动。
	Jitter float64
	// DisableLock 为 true 时不做进程间的重建互斥，只保留进程内的 singleflight。
	//
	// 重建互斥是为“少量热点 key、所有请求都读它”准备的（秒杀 SKU 的库存）。
	// 按订单号这种一人一个 key 的缓存没有热点可言，而互斥对每次未命中都要多两次
	// Redis 往返；回源结果大多不写缓存时（非终态订单），这笔开销换不来任何东西。
	DisableLock bool
	// LockTTL 是重建锁的过期时间，默认 3 秒。持锁的实例崩溃后，别人最多这么久就能接手。
	LockTTL time.Duration
	// WaitStep 是没拿到重建锁时重读缓存的间隔，默认 20 毫秒。
	WaitStep time.Duration
	// WaitMax 是等别人重建的上限，默认 200 毫秒。超过后自己回源，但不写缓存。
	// 有上限是为了不让“持锁的实例卡住”变成“所有实例一起卡住”。
	WaitMax time.Duration
	// Logger 为空时用 slog.Default()。
	Logger *slog.Logger
	// Breaker 是保护这个 Redis 的熔断器（设计文档 1d 的 6.6）。为 nil 时用默认参数建一个。
	// 多个 Cache 连同一个 Redis 时可以共用一个，让它们一起打开、一起恢复。
	Breaker *breaker.Breaker
	// Switch 是人工关掉这个缓存的开关，可以为 nil。关掉时和熔断器打开时一样：不碰 Redis，每次都回源。
	Switch *degrade.Switch
}

// Loaded 是回源的结果。TTL 由回源方决定，因为它取决于读到的内容（例如订单的状态）。
type Loaded[T any] struct {
	Value T
	// Found 为 false 表示数据不存在，此时 Value 被忽略。
	Found bool
	// TTL 为 0 表示这次的结果不缓存。
	TTL time.Duration
}

// Cache 是某一种值的缓存。可并发使用。
//
// 同一时刻对同一个 key 的多个 Get 会拿到同一个回源结果。T 是指针类型时它们共享同一个对象，
// 调用方不能修改返回值。
type Cache[T any] struct {
	client redis.Cmdable
	codec  Codec[T]
	prefix string
	jitter float64
	logger *slog.Logger

	disableLock bool
	lockTTL     time.Duration
	waitStep    time.Duration
	waitMax     time.Duration

	breaker *breaker.Breaker
	off     *degrade.Switch

	flight singleflight.Group
	// lastWarn 是上一次记录 Redis 错误的时刻（UnixNano），用来限频。
	lastWarn atomic.Int64
}

// New 创建 Cache。
func New[T any](cfg Config, codec Codec[T]) (*Cache[T], error) {
	if cfg.Client == nil {
		return nil, errors.New("cache: nil client")
	}
	if codec == nil {
		return nil, errors.New("cache: nil codec")
	}
	if cfg.Jitter >= 1 {
		return nil, fmt.Errorf("cache: jitter must be below 1, got %v", cfg.Jitter)
	}
	c := &Cache[T]{
		client:      cfg.Client,
		codec:       codec,
		prefix:      cfg.KeyPrefix,
		jitter:      cfg.Jitter,
		logger:      cfg.Logger,
		disableLock: cfg.DisableLock,
		lockTTL:     orDefault(cfg.LockTTL, 3*time.Second),
		waitStep:    orDefault(cfg.WaitStep, 20*time.Millisecond),
		waitMax:     orDefault(cfg.WaitMax, 200*time.Millisecond),
		breaker:     cfg.Breaker,
		off:         cfg.Switch,
	}
	if c.breaker == nil {
		c.breaker = breaker.New(breaker.Config{Name: "cache"})
	}
	switch {
	case c.jitter == 0:
		c.jitter = 0.2
	case c.jitter < 0:
		c.jitter = 0
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	return c, nil
}

func orDefault(v, def time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return def
}

// entry 是一次读取或回源的结果。
type entry[T any] struct {
	value T
	found bool
}

// Breaker 返回保护这个缓存的 Redis 的熔断器，给状态展示用。
func (c *Cache[T]) Breaker() *breaker.Breaker { return c.breaker }

// Get 返回 key 的值。未命中时调用 load，并按 Loaded.TTL 写入（Found 为 false 时写空值）。
//
// load 返回错误时不写缓存，错误原样返回：数据库故障不能被缓存成“不存在”。
// Redis 出错时退化为直接调用 load，不返回 Redis 的错误。
//
// Redis 连续出错之后熔断器打开，这期间的 Get 完全不碰 Redis、直接回源，不再每次都先在 Redis 上失败一回；
// 冷却之后放一次读去探测，成功就恢复。开关被关掉时同样直接回源。只有进入 Get 时的这一次读喂熔断器：
// 它失败了，这次调用后面的写入和加锁也都不做，所以熔断器打开期间没有任何 Redis 调用。
//
// load 使用的是“发起这次回源的那个调用”的 ctx。它被取消时，搭便车的调用不会跟着失败，
// 而是各自再回源一次。
func (c *Cache[T]) Get(ctx context.Context, key string, load func(context.Context) (Loaded[T], error)) (value T, found bool, err error) {
	redisOK := false
	if done, ok := c.allow(); ok {
		// 这一次读有自己的短时限：缓存是为了快，Redis 挂住时不能让每个请求先等满客户端的读超时（1 秒）
		// 才回源。熔断器要数到连续几次失败才打开，没有这个时限的话打开之前的每个请求都多等 1 秒
		// （故障注入实测，设计文档 1d 的 6.18）。
		rctx, cancel := context.WithTimeout(ctx, readTimeout)
		e, hit, rerr := c.read(rctx, key)
		cancel()
		switch {
		case rerr == nil:
			done(breaker.Success)
		case ctx.Err() != nil:
			// 调用方自己取消或到期，说明不了 Redis 的状态。
			done(breaker.Ignored)
			var zero T
			return zero, false, ctx.Err()
		default:
			done(breaker.Failure)
			c.warn("cache read failed, loading from source", key, rerr)
		}
		if hit {
			return e.value, e.found, nil
		}
		redisOK = rerr == nil
	}

	ch := c.flight.DoChan(key, func() (any, error) {
		return c.fill(ctx, key, load, redisOK)
	})
	select {
	case <-ctx.Done():
		var zero T
		return zero, false, ctx.Err()
	case res := <-ch:
		if res.Err == nil {
			e := res.Val.(entry[T])
			return e.value, e.found, nil
		}
		// 发起回源的那个调用被取消了，而自己还没有：不替别人的取消买单，自己回源。
		// 这条路不写缓存，也不再合并，因为它很少发生。
		if res.Shared && isContextErr(res.Err) && ctx.Err() == nil {
			e, err := c.loadOnly(ctx, load)
			return e.value, e.found, err
		}
		var zero T
		return zero, false, res.Err
	}
}

// allow 报告这次 Get 要不要读 Redis。要读时返回的 done 用来把读的结果告诉熔断器。
func (c *Cache[T]) allow() (done func(breaker.Outcome), ok bool) {
	if c.off.Off() {
		return nil, false
	}
	done, err := c.breaker.Allow()
	return done, err == nil
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// fill 回源并写缓存。同一个进程里同一个 key 同时只有一个 fill 在运行。
func (c *Cache[T]) fill(ctx context.Context, key string, load func(context.Context) (Loaded[T], error), redisOK bool) (entry[T], error) {
	// Redis 刚才就不可用：不再碰它，直接回源。这时保护数据源的只剩 singleflight。
	if !redisOK {
		return c.loadOnly(ctx, load)
	}
	if c.disableLock {
		return c.loadAndStore(ctx, key, load)
	}

	token, err := lockToken()
	if err != nil {
		return entry[T]{}, err
	}
	lockKey := c.prefix + "cache:lock:" + key
	deadline := time.Now().Add(c.waitMax)
	for {
		acquired, err := c.client.SetNX(ctx, lockKey, token, c.lockTTL).Result()
		if err != nil {
			c.warn("cache rebuild lock failed, loading from source", key, err)
			return c.loadOnly(ctx, load)
		}
		if acquired {
			break
		}

		// 别的实例正在重建。等一会儿再看缓存；每一轮也重新抢一次锁，因为对方的回源结果
		// 可能不写缓存（TTL 为 0），那样只等缓存是等不到的。
		if !time.Now().Before(deadline) {
			return c.loadOnly(ctx, load)
		}
		select {
		case <-ctx.Done():
			return entry[T]{}, ctx.Err()
		case <-time.After(c.waitStep):
		}
		e, hit, err := c.read(ctx, key)
		if err != nil {
			c.warn("cache read failed, loading from source", key, err)
			return c.loadOnly(ctx, load)
		}
		if hit {
			return e, nil
		}
	}

	// 解锁不能用调用方的 ctx：它可能在回源途中被取消，锁就要白白占满 LockTTL。
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if err := unlockScript.Run(unlockCtx, c.client, []string{lockKey}, token).Err(); err != nil {
			c.warn("cache rebuild unlock failed", key, err)
		}
	}()

	// 拿到锁之后再看一眼缓存：第一次读缓存和拿到锁之间，上一个持锁的实例可能刚写完。
	// 少了这一步，排在后面的实例会一个接一个地各自回源一次。
	e, hit, err := c.read(ctx, key)
	if err != nil {
		c.warn("cache read failed, loading from source", key, err)
		return c.loadOnly(ctx, load)
	}
	if hit {
		return e, nil
	}
	return c.loadAndStore(ctx, key, load)
}

func (c *Cache[T]) loadOnly(ctx context.Context, load func(context.Context) (Loaded[T], error)) (entry[T], error) {
	l, err := load(ctx)
	if err != nil {
		return entry[T]{}, err
	}
	return toEntry(l), nil
}

// toEntry 丢掉 Found 为 false 时的 Value：回源方可能顺手填了东西，但“不存在”不该带值，
// 否则直接回源和读缓存得到的结果会不一样。
func toEntry[T any](l Loaded[T]) entry[T] {
	if !l.Found {
		return entry[T]{}
	}
	return entry[T]{value: l.Value, found: true}
}

func (c *Cache[T]) loadAndStore(ctx context.Context, key string, load func(context.Context) (Loaded[T], error)) (entry[T], error) {
	l, err := load(ctx)
	if err != nil {
		return entry[T]{}, err
	}
	e := toEntry(l)
	if l.TTL > 0 {
		c.store(ctx, key, e, l.TTL)
	}
	return e, nil
}

// read 读缓存。hit 为 false 且 err 为 nil 表示未命中。
// 读到无法解码的内容按未命中处理：下一次回源会把它覆盖掉。
func (c *Cache[T]) read(ctx context.Context, key string) (e entry[T], hit bool, err error) {
	data, err := c.client.Get(ctx, c.prefix+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return e, false, nil
	}
	if err != nil {
		return e, false, err
	}
	if len(data) == 1 && data[0] == tagNull {
		return entry[T]{}, true, nil
	}
	if len(data) >= 1 && data[0] == tagValue {
		v, err := c.codec.Unmarshal(data[1:])
		if err == nil {
			return entry[T]{value: v, found: true}, true, nil
		}
		c.logger.WarnContext(ctx, "cache entry is not decodable, treating as a miss", "key", key, "error", err)
		return e, false, nil
	}
	c.logger.WarnContext(ctx, "cache entry has an unknown tag, treating as a miss", "key", key)
	return e, false, nil
}

// store 写缓存。失败只记日志：值已经拿到了，写不进缓存不影响这次请求。
func (c *Cache[T]) store(ctx context.Context, key string, e entry[T], ttl time.Duration) {
	data := []byte{tagNull}
	if e.found {
		b, err := c.codec.Marshal(e.value)
		if err != nil {
			c.logger.WarnContext(ctx, "cache value is not encodable, not caching", "key", key, "error", err)
			return
		}
		data = append([]byte{tagValue}, b...)
	}
	if err := c.client.Set(ctx, c.prefix+key, data, c.jittered(ttl)).Err(); err != nil {
		c.warn("cache write failed", key, err)
	}
}

// jittered 给 TTL 加抖动，让同一批写入的 key 不会同时过期。
func (c *Cache[T]) jittered(ttl time.Duration) time.Duration {
	if c.jitter > 0 {
		// randFloat()∈[0,1) → 系数∈[1-J, 1+J)
		ttl = time.Duration(float64(ttl) * (1 - c.jitter + 2*c.jitter*randFloat()))
	}
	// Redis 的 PX 以毫秒为单位，不足 1 毫秒会被当成不过期。
	return max(ttl, time.Millisecond)
}

// warn 限频记录 Redis 的错误。Redis 整体不可用时每个请求都会走到这里，不限频会刷屏。
func (c *Cache[T]) warn(msg, key string, err error) {
	now := time.Now().UnixNano()
	last := c.lastWarn.Load()
	if last != 0 && now-last < int64(redisWarnEvery) {
		return
	}
	if !c.lastWarn.CompareAndSwap(last, now) {
		return
	}
	//nolint:sloglint // msg 来自本文件里的几个固定字符串
	c.logger.Warn(msg, "key", key, "error", err)
}

func lockToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("cache: random lock token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
