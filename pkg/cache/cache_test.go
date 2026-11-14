package cache

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/palemoky/oppenheimer/pkg/breaker"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/testredis"
)

type stringCodec struct{}

func (stringCodec) Marshal(v string) ([]byte, error)      { return []byte(v), nil }
func (stringCodec) Unmarshal(data []byte) (string, error) { return string(data), nil }

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newCache(t *testing.T, client redis.Cmdable, prefix string, mutate ...func(*Config)) *Cache[string] {
	t.Helper()
	cfg := Config{Client: client, KeyPrefix: prefix, Logger: slog.New(slog.DiscardHandler)}
	for _, m := range mutate {
		m(&cfg)
	}
	c, err := New(cfg, stringCodec{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// loader 返回固定结果并统计调用次数。
type loader struct {
	calls atomic.Int64
	res   Loaded[string]
	err   error
	// delay 模拟回源耗时，让并发的调用有机会撞在一起。
	delay time.Duration
}

func (l *loader) load(ctx context.Context) (Loaded[string], error) {
	l.calls.Add(1)
	if l.delay > 0 {
		select {
		case <-ctx.Done():
			return Loaded[string]{}, ctx.Err()
		case <-time.After(l.delay):
		}
	}
	return l.res, l.err
}

func mustGet(ctx context.Context, t *testing.T, c *Cache[string], key string, l *loader) (string, bool) {
	t.Helper()
	v, found, err := c.Get(ctx, key, l.load)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return v, found
}

func TestNewValidates(t *testing.T) {
	client, prefix := testredis.New(t)
	if _, err := New(Config{KeyPrefix: prefix}, stringCodec{}); err == nil {
		t.Error("nil client: want error")
	}
	if _, err := New[string](Config{Client: client}, nil); err == nil {
		t.Error("nil codec: want error")
	}
	if _, err := New(Config{Client: client, Jitter: 1}, stringCodec{}); err == nil {
		t.Error("jitter 1: want error")
	}
}

func TestHitDoesNotLoad(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix)
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	for range 3 {
		if v, found := mustGet(ctx, t, c, "k", l); !found || v != "v" {
			t.Fatalf("got (%q, %v), want (v, true)", v, found)
		}
	}
	if n := l.calls.Load(); n != 1 {
		t.Fatalf("loaded %d times, want 1", n)
	}
}

func TestNullIsCached(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix)
	// Value 在 Found 为 false 时必须被忽略。
	l := &loader{res: Loaded[string]{Value: "ignored", Found: false, TTL: time.Minute}}

	for range 3 {
		if v, found := mustGet(ctx, t, c, "k", l); found || v != "" {
			t.Fatalf("got (%q, %v), want (\"\", false)", v, found)
		}
	}
	if n := l.calls.Load(); n != 1 {
		t.Fatalf("loaded %d times, want 1", n)
	}
}

// 空字符串是合法的值，不能和“不存在”混在一起。
func TestEmptyValueIsNotNull(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix)
	l := &loader{res: Loaded[string]{Value: "", Found: true, TTL: time.Minute}}

	mustGet(ctx, t, c, "k", l)
	if v, found := mustGet(ctx, t, c, "k", l); !found || v != "" {
		t.Fatalf("got (%q, %v), want (\"\", true)", v, found)
	}
	if n := l.calls.Load(); n != 1 {
		t.Fatalf("loaded %d times, want 1", n)
	}
}

func TestLoadErrorIsNotCached(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix)
	boom := errors.New("db down")
	l := &loader{err: boom, res: Loaded[string]{Found: false, TTL: time.Minute}}

	if _, _, err := c.Get(ctx, "k", l.load); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if n, _ := client.Exists(ctx, prefix+"k").Result(); n != 0 {
		t.Fatal("a failed load was written to the cache")
	}

	// 数据源恢复后能读到真实的值，而不是一个被缓存下来的“不存在”。
	l.err = nil
	l.res = Loaded[string]{Value: "v", Found: true, TTL: time.Minute}
	if v, found := mustGet(ctx, t, c, "k", l); !found || v != "v" {
		t.Fatalf("got (%q, %v), want (v, true)", v, found)
	}
}

func TestZeroTTLIsNotCached(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix)
	l := &loader{res: Loaded[string]{Value: "pending", Found: true}}

	mustGet(ctx, t, c, "k", l)
	mustGet(ctx, t, c, "k", l)
	if n := l.calls.Load(); n != 2 {
		t.Fatalf("loaded %d times, want 2", n)
	}
	if n, _ := client.Exists(ctx, prefix+"k").Result(); n != 0 {
		t.Fatal("a result with zero TTL was written to the cache")
	}

	// 值变了之后立刻读到新值：这是“非终态订单不缓存”要保证的性质。
	l.res = Loaded[string]{Value: "paid", Found: true, TTL: time.Minute}
	if v, _ := mustGet(ctx, t, c, "k", l); v != "paid" {
		t.Fatalf("got %q, want paid", v)
	}
}

// 进程内：100 个请求同时读同一个冷 key，只回源一次。
func TestConcurrentColdKeyLoadsOnceInProcess(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix)
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}, delay: 100 * time.Millisecond}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 100 {
		wg.Go(func() {
			<-start
			if v, found, err := c.Get(ctx, "hot", l.load); err != nil || !found || v != "v" {
				t.Errorf("got (%q, %v, %v)", v, found, err)
			}
		})
	}
	close(start)
	wg.Wait()
	if n := l.calls.Load(); n != 1 {
		t.Fatalf("loaded %d times, want 1", n)
	}
}

// 进程间：多个 Cache 实例模拟多个进程，只有一个回源并写入，其余等到结果。
func TestConcurrentColdKeyLoadsOnceAcrossProcesses(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}, delay: 60 * time.Millisecond}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		c := newCache(t, client, prefix)
		wg.Go(func() {
			<-start
			if v, found, err := c.Get(ctx, "hot", l.load); err != nil || !found || v != "v" {
				t.Errorf("got (%q, %v, %v)", v, found, err)
			}
		})
	}
	close(start)
	wg.Wait()
	if n := l.calls.Load(); n != 1 {
		t.Fatalf("loaded %d times, want 1", n)
	}
	if n, _ := client.Exists(ctx, prefix+"cache:lock:hot").Result(); n != 0 {
		t.Fatal("rebuild lock was not released")
	}
}

// 持锁的实例卡住不释放：等待方在 WaitMax 之后自己回源，但不写缓存。
func TestWaiterLoadsItselfAfterWaitMax(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix, func(cfg *Config) {
		cfg.WaitStep = 10 * time.Millisecond
		cfg.WaitMax = 80 * time.Millisecond
	})
	if err := client.Set(ctx, prefix+"cache:lock:k", "someone-else", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	began := time.Now()
	if v, found := mustGet(ctx, t, c, "k", l); !found || v != "v" {
		t.Fatalf("got (%q, %v), want (v, true)", v, found)
	}
	if waited := time.Since(began); waited < 80*time.Millisecond || waited > 2*time.Second {
		t.Fatalf("waited %v, want about WaitMax (80ms)", waited)
	}
	if n, _ := client.Exists(ctx, prefix+"k").Result(); n != 0 {
		t.Fatal("a waiter that gave up wrote to the cache")
	}
	// 别人的锁不能被等待方删掉。
	if got, _ := client.Get(ctx, prefix+"cache:lock:k").Result(); got != "someone-else" {
		t.Fatalf("lock value = %q, want it untouched", got)
	}
}

// 持锁方的回源结果不写缓存（TTL 为 0）时，等待方不能一直等到 WaitMax：
// 锁一释放它就应该接手。否则两个实例同时查同一个非终态订单，其中一个每次都要多等 200 毫秒。
func TestWaiterTakesOverWhenHolderDoesNotCache(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix, func(cfg *Config) {
		cfg.WaitStep = 10 * time.Millisecond
		cfg.WaitMax = 5 * time.Second
	})
	// 模拟另一个实例持锁 50 毫秒后释放，期间没有写缓存。
	if err := client.Set(ctx, prefix+"cache:lock:k", "someone-else", 50*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	began := time.Now()
	mustGet(ctx, t, c, "k", l)
	if waited := time.Since(began); waited > 2*time.Second {
		t.Fatalf("waited %v, want well under WaitMax", waited)
	}
	// 这一次是拿到锁之后回源的，所以会写缓存。
	if n, _ := client.Exists(ctx, prefix+"k").Result(); n != 1 {
		t.Fatal("the waiter took the lock but did not write the cache")
	}
}

func TestDisableLockSkipsRebuildLock(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix, func(cfg *Config) { cfg.DisableLock = true })
	// 即使有人占着锁也不等。
	if err := client.Set(ctx, prefix+"cache:lock:k", "someone-else", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	began := time.Now()
	mustGet(ctx, t, c, "k", l)
	mustGet(ctx, t, c, "k", l)
	if waited := time.Since(began); waited > 150*time.Millisecond {
		t.Fatalf("took %v, the rebuild lock seems to be in use", waited)
	}
	if n := l.calls.Load(); n != 1 {
		t.Fatalf("loaded %d times, want 1", n)
	}
}

// Redis 不可用：每次都回源，不向上报错。
func TestRedisDownFallsBackToLoad(t *testing.T) {
	dead := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1", // 没有服务监听
		DialTimeout: 100 * time.Millisecond,
		MaxRetries:  -1,
		// 默认会重拨 5 次、每次间隔 100 毫秒，测试里不需要。
		DialerRetries: 1,
	})
	t.Cleanup(func() { _ = dead.Close() })
	ctx := testCtx(t)
	c := newCache(t, dead, "t:dead:")
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	for range 3 {
		if v, found := mustGet(ctx, t, c, "k", l); !found || v != "v" {
			t.Fatalf("got (%q, %v), want (v, true)", v, found)
		}
	}
	if n := l.calls.Load(); n != 3 {
		t.Fatalf("loaded %d times, want 3", n)
	}
}

// countingHook 数实际发往 Redis 的命令（包括连不上的）。
type countingHook struct{ cmds atomic.Int64 }

func (h *countingHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *countingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.cmds.Add(1)
		return next(ctx, cmd)
	}
}

func (h *countingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// Redis 不可用：连续失败几次之后不再碰 Redis，直接回源；冷却之后只放一次去探测。
func TestBreakerStopsCallingDeadRedis(t *testing.T) {
	dead := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1, DialerRetries: 1,
	})
	t.Cleanup(func() { _ = dead.Close() })
	hook := &countingHook{}
	dead.AddHook(hook)

	now := time.Now()
	clock := func() time.Time { return now }
	br := breaker.New(breaker.Config{Failures: 3, Cooldown: 5 * time.Second, Now: clock})
	ctx := testCtx(t)
	c := newCache(t, dead, "t:dead:", func(cfg *Config) { cfg.Breaker = br })
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	for range 20 {
		if v, found := mustGet(ctx, t, c, "k", l); !found || v != "v" {
			t.Fatalf("got (%q, %v), want (v, true)", v, found)
		}
	}
	if n := l.calls.Load(); n != 20 {
		t.Fatalf("loaded %d times, want 20", n)
	}
	// 前 3 次各读一次 Redis（读失败后这次调用不再写、不再加锁），之后熔断器打开。
	if n := hook.cmds.Load(); n != 3 {
		t.Fatalf("%d redis commands for 20 gets against a dead redis, want 3", n)
	}
	if got := c.Breaker().State(); got != breaker.Open {
		t.Fatalf("breaker state = %s, want open", got)
	}

	// 冷却之后：一次探测，失败，继续打开。
	now = now.Add(5 * time.Second)
	for range 10 {
		mustGet(ctx, t, c, "k", l)
	}
	if n := hook.cmds.Load(); n != 4 {
		t.Fatalf("%d redis commands after the cooldown, want 4 (one probe)", n)
	}
}

// Redis 恢复之后熔断器关闭，缓存重新生效。
func TestBreakerRecovers(t *testing.T) {
	client, prefix := testredis.New(t)
	now := time.Now()
	br := breaker.New(breaker.Config{Failures: 1, Cooldown: time.Second, Now: func() time.Time { return now }})
	c := newCache(t, client, prefix, func(cfg *Config) { cfg.Breaker = br })
	ctx := testCtx(t)
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	// 模拟 Redis 出过一次错：熔断器打开，Get 不读缓存也不写缓存。
	br.Report(false)
	mustGet(ctx, t, c, "k", l)
	mustGet(ctx, t, c, "k", l)
	if n := l.calls.Load(); n != 2 {
		t.Fatalf("loaded %d times while the breaker is open, want 2", n)
	}
	// 冷却之后的第一次 Get 是探测：读成功（未命中），回源并写入；之后命中。
	now = now.Add(time.Second)
	mustGet(ctx, t, c, "k", l)
	mustGet(ctx, t, c, "k", l)
	if n := l.calls.Load(); n != 3 {
		t.Fatalf("loaded %d times in total, want 3 (the probe filled the cache)", n)
	}
	if got := br.State(); got != breaker.Closed {
		t.Fatalf("breaker state = %s, want closed", got)
	}
}

// 开关关掉：不读也不写缓存；打开之后恢复。
func TestSwitchOffBypassesCache(t *testing.T) {
	client, prefix := testredis.New(t)
	set := degrade.New("test", slog.New(slog.DiscardHandler))
	sw := set.Switch("cache", "")
	c := newCache(t, client, prefix, func(cfg *Config) { cfg.Switch = sw })
	ctx := testCtx(t)
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	if err := set.SetMode("cache", degrade.ModeOff); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		mustGet(ctx, t, c, "k", l)
	}
	if n := l.calls.Load(); n != 3 {
		t.Fatalf("loaded %d times with the cache switched off, want 3", n)
	}
	if err := set.SetMode("cache", degrade.ModeAuto); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		mustGet(ctx, t, c, "k", l)
	}
	if n := l.calls.Load(); n != 4 {
		t.Fatalf("loaded %d times in total, want 4 (one fill after switching back on)", n)
	}
}

// 发起回源的调用被取消时，搭便车的调用不跟着失败。
func TestFollowerSurvivesLeaderCancellation(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix)
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}, delay: 300 * time.Millisecond}

	leaderCtx, cancelLeader := context.WithCancel(ctx)
	leaderDone := make(chan error, 1)
	go func() {
		_, _, err := c.Get(leaderCtx, "k", l.load)
		leaderDone <- err
	}()
	// 等发起方进入回源，再让跟随方搭上去。
	for l.calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	followerDone := make(chan error, 1)
	go func() {
		v, found, err := c.Get(ctx, "k", l.load)
		if err == nil && (!found || v != "v") {
			err = errors.New("follower got a wrong value")
		}
		followerDone <- err
	}()
	time.Sleep(30 * time.Millisecond)
	cancelLeader()

	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader err = %v, want context.Canceled", err)
	}
	if err := <-followerDone; err != nil {
		t.Fatalf("follower: %v", err)
	}
}

func TestJitterBounds(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	old := randFloat
	t.Cleanup(func() { randFloat = old })

	c := newCache(t, client, prefix) // 默认抖动 0.2
	for _, tc := range []struct {
		r    float64
		want time.Duration
	}{
		{0, 80 * time.Second},
		{0.5, 100 * time.Second},
		{0.999999, 120 * time.Second},
	} {
		randFloat = func() float64 { return tc.r }
		if got := c.jittered(100 * time.Second); (got - tc.want).Abs() > time.Millisecond {
			t.Errorf("r=%v: got %v, want %v", tc.r, got, tc.want)
		}
	}

	// 实际写进 Redis 的过期时间落在范围内。
	randFloat = old
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: 100 * time.Second}}
	for i := range 20 {
		key := "k" + string(rune('a'+i))
		mustGet(ctx, t, c, key, l)
		ttl, err := client.PTTL(ctx, prefix+key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if ttl < 79*time.Second || ttl > 120*time.Second {
			t.Errorf("ttl %v is outside [80s, 120s)", ttl)
		}
	}

	// 负数关闭抖动；极短的 TTL 不会变成 0（Redis 会把它当成不过期或报错）。
	exact := newCache(t, client, prefix, func(cfg *Config) { cfg.Jitter = -1 })
	if got := exact.jittered(time.Second); got != time.Second {
		t.Errorf("no jitter: got %v, want 1s", got)
	}
	if got := exact.jittered(time.Microsecond); got != time.Millisecond {
		t.Errorf("tiny ttl: got %v, want 1ms", got)
	}
}

func TestUndecodableEntryIsAMiss(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix)
	if err := client.Set(ctx, prefix+"k", []byte{9, 'x'}, time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}
	if v, found := mustGet(ctx, t, c, "k", l); !found || v != "v" {
		t.Fatalf("got (%q, %v), want (v, true)", v, found)
	}
	// 回源的结果覆盖了坏数据。
	mustGet(ctx, t, c, "k", l)
	if n := l.calls.Load(); n != 1 {
		t.Fatalf("loaded %d times, want 1", n)
	}
}

func TestProtoCodec(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c, err := New(Config{Client: client, KeyPrefix: prefix}, ProtoCodec[*wrapperspb.Int64Value]{})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	get := func(key string, res Loaded[*wrapperspb.Int64Value]) (*wrapperspb.Int64Value, bool) {
		t.Helper()
		v, found, err := c.Get(ctx, key, func(context.Context) (Loaded[*wrapperspb.Int64Value], error) {
			calls++
			return res, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return v, found
	}

	// 第二次读的是从 Redis 解码出来的消息。
	get("n", Loaded[*wrapperspb.Int64Value]{Value: wrapperspb.Int64(42), Found: true, TTL: time.Minute})
	if v, found := get("n", Loaded[*wrapperspb.Int64Value]{}); !found || v.GetValue() != 42 {
		t.Fatalf("got (%v, %v), want (42, true)", v, found)
	}
	// 所有字段都是零值的消息编码后长度为 0，仍然要和“不存在”区分开。
	get("zero", Loaded[*wrapperspb.Int64Value]{Value: wrapperspb.Int64(0), Found: true, TTL: time.Minute})
	if v, found := get("zero", Loaded[*wrapperspb.Int64Value]{}); !found || v == nil || v.GetValue() != 0 {
		t.Fatalf("got (%v, %v), want (0, true)", v, found)
	}
	if calls != 2 {
		t.Fatalf("loaded %d times, want 2", calls)
	}
}
