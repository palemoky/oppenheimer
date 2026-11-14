package ratelimit_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/palemoky/oppenheimer/pkg/ratelimit"
	"github.com/palemoky/oppenheimer/pkg/testredis"
)

// harness 是一个被测的限流器加上“让时间过去 d”的办法。进程内和 Redis 两个实现过同一组用例：
// 它们必须是同一个算法，Redis 不可用时退回进程内，行为不应当变。
type harness struct {
	name    string
	limiter ratelimit.Limiter
	advance func(t *testing.T, d time.Duration)
}

func harnesses(t *testing.T) []harness {
	t.Helper()
	// 进程内：可拨动的时钟。
	var now atomic.Int64
	now.Store(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC).UnixNano())
	local := ratelimit.NewLocal(ratelimit.LocalConfig{Now: func() time.Time { return time.Unix(0, now.Load()) }})
	hs := []harness{{
		name:    "local",
		limiter: local,
		advance: func(_ *testing.T, d time.Duration) { now.Add(int64(d)) },
	}}

	// Redis：脚本用的是 Redis 自己的时钟，拨不动。让时间过去 d 等价于把每个桶的 ts 往回挪 d。
	rdb, prefix := testredis.New(t)
	hs = append(hs, harness{
		name:    "redis",
		limiter: ratelimit.NewRedis(rdb, prefix),
		advance: func(t *testing.T, d time.Duration) {
			t.Helper()
			keys, err := rdb.Keys(t.Context(), prefix+"rl:*").Result()
			if err != nil {
				t.Fatal(err)
			}
			for _, k := range keys {
				if err := rdb.HIncrBy(t.Context(), k, "ts", -d.Microseconds()).Err(); err != nil {
					t.Fatal(err)
				}
			}
		},
	})
	return hs
}

func forEach(t *testing.T, fn func(t *testing.T, h harness)) {
	t.Helper()
	for _, h := range harnesses(t) {
		t.Run(h.name, func(t *testing.T) { fn(t, h) })
	}
}

func allow(t *testing.T, h harness, reqs ...ratelimit.Request) ratelimit.Decision {
	t.Helper()
	d, err := h.limiter.Allow(t.Context(), reqs...)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return d
}

// take 连续取 n 次，返回通过的次数。
func take(t *testing.T, h harness, n int, req ratelimit.Request) int {
	t.Helper()
	ok := 0
	for range n {
		if allow(t, h, req).Allowed {
			ok++
		}
	}
	return ok
}

func req(key string, rate float64, burst int) ratelimit.Request {
	return ratelimit.Request{Key: key, Limit: ratelimit.Limit{Rate: rate, Burst: burst}}
}

// near 判断 got 在 want 的 ±tol 之内。Redis 的时钟在走，两次调用之间会多补一点点。
func near(got, want, tol time.Duration) bool { return got >= want-tol && got <= want+tol }

// 新桶是满的：能连过 burst 个，下一个被拒绝，并告诉调用方大约 1/rate 之后再来。
func TestBurstThenDeny(t *testing.T) {
	forEach(t, func(t *testing.T, h harness) {
		r := req("u1", 2, 5)
		if got := take(t, h, 5, r); got != 5 {
			t.Fatalf("first %d requests: %d allowed, want all (a new bucket is full)", 5, got)
		}
		d := allow(t, h, r)
		if d.Allowed || d.Denied != 0 {
			t.Fatalf("6th request = %+v, want denied by bucket 0", d)
		}
		if !near(d.RetryAfter, 500*time.Millisecond, 50*time.Millisecond) {
			t.Fatalf("RetryAfter = %s, want about 500ms (one token at 2/s)", d.RetryAfter)
		}
	})
}

// 被拒绝之后等 RetryAfter 就有一个令牌；按速率补充；补到容量为止。
func TestRefillAtRateUpToBurst(t *testing.T) {
	forEach(t, func(t *testing.T, h harness) {
		r := req("u1", 10, 5)
		take(t, h, 5, r)
		d := allow(t, h, r)
		if d.Allowed {
			t.Fatal("empty bucket allowed a request")
		}
		h.advance(t, d.RetryAfter)
		if !allow(t, h, r).Allowed {
			t.Fatalf("still denied after waiting RetryAfter (%s)", d.RetryAfter)
		}
		if allow(t, h, r).Allowed {
			t.Fatal("got two tokens after waiting for one")
		}

		// 300 毫秒补 3 个。
		h.advance(t, 300*time.Millisecond)
		if got := take(t, h, 10, r); got != 3 {
			t.Fatalf("after 300ms at 10/s: %d allowed, want 3", got)
		}
		// 很久不用也只补到容量。
		h.advance(t, time.Hour)
		if got := take(t, h, 20, r); got != 5 {
			t.Fatalf("after an hour idle: %d allowed, want the burst of 5", got)
		}
	})
}

// 两个桶一起判断：一个有令牌、一个没有时拒绝，而且有令牌的那个不被扣。
func TestAllBucketsOrNone(t *testing.T) {
	forEach(t, func(t *testing.T, h harness) {
		user, route := req("user:1", 1, 3), req("route:create", 1, 2)
		// 另一个用户把接口的桶用光。
		other := req("user:2", 1, 10)
		for i := range 2 {
			if !allow(t, h, other, route).Allowed {
				t.Fatalf("request %d of the other user denied", i)
			}
		}
		d := allow(t, h, user, route)
		if d.Allowed || d.Denied != 1 {
			t.Fatalf("decision = %+v, want denied by bucket 1 (the route)", d)
		}
		if !near(d.RetryAfter, time.Second, 50*time.Millisecond) {
			t.Fatalf("RetryAfter = %s, want about 1s", d.RetryAfter)
		}
		// 被接口拒绝的那几次没有消耗这个用户的配额：单独看，他的桶还是满的。
		for range 5 {
			allow(t, h, user, route)
		}
		if got := take(t, h, 5, user); got != 3 {
			t.Fatalf("user bucket had %d tokens after being denied by the route, want its full burst of 3", got)
		}
	})
}

// 两个桶都没有令牌：Denied 是第一个，RetryAfter 是等得最久的那个。
func TestDeniedReportsFirstBucketAndLongestWait(t *testing.T) {
	forEach(t, func(t *testing.T, h harness) {
		fast, slow := req("fast", 10, 1), req("slow", 0.5, 1)
		if !allow(t, h, fast, slow).Allowed {
			t.Fatal("first request denied")
		}
		d := allow(t, h, fast, slow)
		if d.Allowed || d.Denied != 0 {
			t.Fatalf("decision = %+v, want denied by bucket 0", d)
		}
		if !near(d.RetryAfter, 2*time.Second, 100*time.Millisecond) {
			t.Fatalf("RetryAfter = %s, want about 2s (the slow bucket)", d.RetryAfter)
		}
	})
}

// 不限的请求被忽略；下标仍然按调用方给的顺序算。
func TestDisabledLimitsAreIgnored(t *testing.T) {
	forEach(t, func(t *testing.T, h harness) {
		off := ratelimit.Request{Key: "anything"}
		for range 100 {
			if !allow(t, h, off).Allowed {
				t.Fatal("a disabled limit denied a request")
			}
		}
		if d := allow(t, h); !d.Allowed || d.Denied != -1 {
			t.Fatalf("no requests = %+v, want allowed", d)
		}
		r := req("k", 1, 1)
		allow(t, h, off, r)
		if d := allow(t, h, off, r); d.Allowed || d.Denied != 1 {
			t.Fatalf("decision = %+v, want denied by index 1 (index 0 is disabled)", d)
		}
	})
}

func TestInvalidRequests(t *testing.T) {
	forEach(t, func(t *testing.T, h harness) {
		cases := map[string][]ratelimit.Request{
			"duplicate key":      {req("k", 1, 1), req("k", 1, 1)},
			"empty key":          {req("", 1, 1)},
			"negative rate":      {{Key: "k", Limit: ratelimit.Limit{Rate: -1, Burst: 1}}},
			"rate without burst": {{Key: "k", Limit: ratelimit.Limit{Rate: 1}}},
			"huge burst":         {{Key: "k", Limit: ratelimit.Limit{Rate: 1, Burst: 1 << 31}}},
		}
		for name, reqs := range cases {
			if _, err := h.limiter.Allow(t.Context(), reqs...); err == nil {
				t.Errorf("%s: accepted", name)
			}
		}
	})
}

// 配置把容量调小之后，桶里多出来的令牌不算数。
func TestLoweredBurstCapsExistingTokens(t *testing.T) {
	forEach(t, func(t *testing.T, h harness) {
		allow(t, h, req("k", 1, 100)) // 桶里还有 99 个
		if got := take(t, h, 10, req("k", 1, 3)); got != 3 {
			t.Fatalf("%d allowed after lowering the burst to 3, want 3", got)
		}
	})
}

// 并发地取同一个桶：通过的总数不超过容量加上这段时间补充的，也不少于容量。
func TestConcurrentTakersNeverExceedTheBucket(t *testing.T) {
	forEach(t, func(t *testing.T, h harness) {
		const (
			burst   = 50
			rate    = 10.0
			workers = 16
			each    = 20
		)
		r := req("hot", rate, burst)
		start := time.Now()
		var (
			ok atomic.Int64
			wg sync.WaitGroup
		)
		for range workers {
			wg.Go(func() {
				for range each {
					d, err := h.limiter.Allow(context.WithoutCancel(t.Context()), r)
					if err != nil {
						t.Errorf("Allow: %v", err)
						return
					}
					if d.Allowed {
						ok.Add(1)
					}
				}
			})
		}
		wg.Wait()
		// 进程内的时钟没有动，补充为零；Redis 的时钟在走，最多补 rate × 实际用时。
		upper := int64(burst) + int64(time.Since(start).Seconds()*rate) + 1
		if h.name == "local" {
			upper = burst
		}
		if got := ok.Load(); got < burst || got > upper {
			t.Fatalf("%d of %d concurrent requests allowed, want between %d and %d", got, workers*each, burst, upper)
		}
	})
}

// ---------- 只针对进程内实现 ----------

// 键的数量有上限：超过之后丢掉一些桶，表不再增长；被丢掉的桶再出现时是满的。
func TestLocalBoundsItsKeys(t *testing.T) {
	t.Parallel()
	var now atomic.Int64
	l := ratelimit.NewLocal(ratelimit.LocalConfig{MaxKeys: 640, Now: func() time.Time { return time.Unix(0, now.Load()) }})
	for i := range 50_000 {
		d, err := l.Allow(t.Context(), req("ip:"+strconv.Itoa(i), 1, 1))
		if err != nil || !d.Allowed {
			t.Fatalf("first request of key %d = %+v, %v", i, d, err)
		}
	}
	// 每个分片的容量是 MaxKeys/64，哈希不完全均匀，所以总数在 MaxKeys 附近而不是恰好等于它。
	if n := l.Len(); n > 640 {
		t.Fatalf("limiter holds %d buckets, want at most MaxKeys (640)", n)
	}
	// 时间过去之后桶都补满了，新键进来时它们被优先丢掉：丢一个补满的桶不改变任何行为。
	now.Add(int64(10 * time.Second))
	for i := range 1000 {
		if _, err := l.Allow(t.Context(), req("new:"+strconv.Itoa(i), 1, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if n := l.Len(); n > 640 {
		t.Fatalf("limiter holds %d buckets after more keys, want at most 640", n)
	}
}

// 两次调用以相反的顺序列出同样的两个桶：不死锁，也不竞争（-race）。
func TestLocalMultiBucketCallsDoNotDeadlock(t *testing.T) {
	t.Parallel()
	l := ratelimit.NewLocal(ratelimit.LocalConfig{})
	var wg sync.WaitGroup
	done := make(chan struct{})
	for i := range 8 {
		wg.Go(func() {
			for n := range 2000 {
				a, b := req("a"+strconv.Itoa(n%7), 1e6, 1000), req("b"+strconv.Itoa(n%5), 1e6, 1000)
				if i%2 == 0 {
					a, b = b, a
				}
				if _, err := l.Allow(context.Background(), a, b); err != nil {
					t.Errorf("Allow: %v", err)
					return
				}
			}
		})
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("multi-bucket calls deadlocked")
	}
}

// ---------- 只针对 Redis 实现 ----------

// 桶带过期时间（从空到满再加一秒），ts 是精确的整数微秒。
func TestRedisBucketLayout(t *testing.T) {
	t.Parallel()
	rdb, prefix := testredis.New(t)
	l := ratelimit.NewRedis(rdb, prefix)
	if _, err := l.Allow(t.Context(), req("u", 2, 10)); err != nil {
		t.Fatal(err)
	}
	key := prefix + "rl:u"
	ttl, err := rdb.PTTL(t.Context(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 5*time.Second || ttl > 6*time.Second {
		t.Fatalf("ttl = %s, want just under 6s (10 tokens at 2/s plus one second)", ttl)
	}
	v, err := rdb.HGetAll(t.Context(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	ts, err := strconv.ParseInt(v["ts"], 10, 64)
	if err != nil {
		t.Fatalf("ts %q is not an integer: %v", v["ts"], err)
	}
	if d := time.Since(time.UnixMicro(ts)); d < -time.Minute || d > time.Minute {
		t.Fatalf("ts %d is %s away from now, want redis time in microseconds", ts, d)
	}
	if v["tokens"] != "9.000000" {
		t.Fatalf("tokens = %q, want 9.000000", v["tokens"])
	}
}

// Redis 不可用时返回错误，不悄悄放行。
func TestRedisReturnsErrorWhenUnavailable(t *testing.T) {
	t.Parallel()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	l := ratelimit.NewRedis(rdb, "")
	if d, err := l.Allow(t.Context(), req("u", 1, 1)); err == nil {
		t.Fatalf("Allow = %+v with redis down, want an error", d)
	}
}

// ---------- Fallback ----------

type scripted struct {
	calls atomic.Int64
	fn    func(ctx context.Context) (ratelimit.Decision, error)
}

func (s *scripted) Allow(ctx context.Context, _ ...ratelimit.Request) (ratelimit.Decision, error) {
	s.calls.Add(1)
	return s.fn(ctx)
}

var errDown = errors.New("redis is down")

// 主限流器出错：这次请求由备用的判断；冷却期内不再去问主的；冷却期过了再试，好了就回去。
func TestFallbackUsesSecondaryDuringCooldown(t *testing.T) {
	t.Parallel()
	var (
		now  atomic.Int64
		down atomic.Bool
	)
	primary := &scripted{fn: func(context.Context) (ratelimit.Decision, error) {
		if down.Load() {
			return ratelimit.Decision{}, errDown
		}
		return ratelimit.Decision{Allowed: true, Denied: -1}, nil
	}}
	secondary := &scripted{fn: func(context.Context) (ratelimit.Decision, error) {
		return ratelimit.Decision{Denied: 0, RetryAfter: time.Second}, nil
	}}
	f := ratelimit.NewFallback(ratelimit.FallbackConfig{
		Primary: primary, Secondary: secondary, Cooldown: time.Second,
		Now: func() time.Time { return time.Unix(0, now.Load()) },
	})
	call := func() ratelimit.Decision {
		t.Helper()
		d, err := f.Allow(t.Context(), req("k", 1, 1))
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		return d
	}

	if !call().Allowed || secondary.calls.Load() != 0 {
		t.Fatal("healthy primary was not used")
	}
	down.Store(true)
	if call().Allowed {
		t.Fatal("request was not decided by the secondary after the primary failed")
	}
	// 冷却期内：不碰主的。
	before := primary.calls.Load()
	for range 10 {
		call()
	}
	if got := primary.calls.Load() - before; got != 0 {
		t.Fatalf("primary was called %d times during the cooldown, want 0", got)
	}
	// 冷却期过了，主的还没好：试一次，继续用备用的。
	now.Add(int64(time.Second + time.Millisecond))
	call()
	call()
	if got := primary.calls.Load() - before; got != 1 {
		t.Fatalf("primary was probed %d times after the cooldown, want exactly once", got)
	}
	// 主的好了：下一个冷却期过后回去。
	down.Store(false)
	now.Add(int64(time.Second + time.Millisecond))
	for range 2 {
		if !call().Allowed {
			t.Fatal("did not return to the recovered primary")
		}
	}
	if f.Fallbacks() != 13 {
		t.Fatalf("fallbacks = %d, want 13", f.Fallbacks())
	}
}

// 主限流器挂住不返回：请求最多等 Timeout，然后由备用的判断。
func TestFallbackBoundsThePrimaryWait(t *testing.T) {
	t.Parallel()
	primary := &scripted{fn: func(ctx context.Context) (ratelimit.Decision, error) {
		<-ctx.Done()
		return ratelimit.Decision{}, ctx.Err()
	}}
	secondary := ratelimit.NewLocal(ratelimit.LocalConfig{})
	f := ratelimit.NewFallback(ratelimit.FallbackConfig{Primary: primary, Secondary: secondary, Timeout: 30 * time.Millisecond})

	start := time.Now()
	d, err := f.Allow(t.Context(), req("k", 1, 1))
	if err != nil || !d.Allowed {
		t.Fatalf("Allow = %+v, %v; want allowed by the local limiter", d, err)
	}
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("request waited %s for a hung primary, want about the 30ms timeout", el)
	}
	// 备用的限额仍然生效：这个桶只有 1 个令牌。
	if d, _ := f.Allow(t.Context(), req("k", 1, 1)); d.Allowed {
		t.Fatal("second request allowed: the fallback did not enforce the limit")
	}
}

// 调用方自己取消了：不算主限流器的故障，不为此退回。
func TestFallbackDoesNotTripOnCallerCancel(t *testing.T) {
	t.Parallel()
	primary := &scripted{fn: func(ctx context.Context) (ratelimit.Decision, error) {
		if err := ctx.Err(); err != nil {
			return ratelimit.Decision{}, err
		}
		return ratelimit.Decision{Allowed: true, Denied: -1}, nil
	}}
	secondary := &scripted{fn: func(context.Context) (ratelimit.Decision, error) {
		return ratelimit.Decision{}, errors.New("must not be called")
	}}
	f := ratelimit.NewFallback(ratelimit.FallbackConfig{Primary: primary, Secondary: secondary})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.Allow(ctx, req("k", 1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if d, err := f.Allow(t.Context(), req("k", 1, 1)); err != nil || !d.Allowed {
		t.Fatalf("after a cancelled call: %+v, %v; want the primary to be used", d, err)
	}
	if secondary.calls.Load() != 0 || f.Fallbacks() != 0 {
		t.Fatal("a caller cancellation tripped the fallback")
	}
}

// 真的 Redis 加真的进程内限流器接在一起：Redis 连不上时限额仍然生效。
func TestFallbackWithRealLimiters(t *testing.T) {
	t.Parallel()
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = dead.Close() })
	f := ratelimit.NewFallback(ratelimit.FallbackConfig{
		Primary:   ratelimit.NewRedis(dead, ""),
		Secondary: ratelimit.NewLocal(ratelimit.LocalConfig{}),
	})
	ok := 0
	for range 20 {
		d, err := f.Allow(t.Context(), req("u", 0.001, 5))
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			ok++
		}
	}
	if ok != 5 {
		t.Fatalf("%d of 20 allowed with redis down, want the burst of 5 from the local limiter", ok)
	}
}

func TestParse(t *testing.T) {
	t.Parallel()
	good := []struct {
		in   string
		want ratelimit.Limit
	}{
		{"5/10", ratelimit.Limit{Rate: 5, Burst: 10}},
		{" 0.5 / 3 ", ratelimit.Limit{Rate: 0.5, Burst: 3}}, // 两边和中间的空白都容忍
		{"off", ratelimit.Limit{}},
	}
	for _, c := range good {
		got, err := ratelimit.Parse(c.in)
		if err != nil || got != c.want {
			t.Errorf("Parse(%q) = %+v, %v; want %+v", c.in, got, err, c.want)
		}
	}
	for _, in := range []string{"", "5", "5/", "/10", "a/b", "0/10", "5/0", "-1/10", "5/1.5", "1e12/10"} {
		if got, err := ratelimit.Parse(in); err == nil {
			t.Errorf("Parse(%q) = %+v, want an error", in, got)
		}
	}
	for _, l := range []ratelimit.Limit{{Rate: 5, Burst: 10}, {Rate: 0.25, Burst: 1}, {}} {
		back, err := ratelimit.Parse(l.String())
		if err != nil || back != l {
			t.Errorf("Parse(%q) = %+v, %v; want %+v", l.String(), back, err, l)
		}
	}
}
