package lock

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/palemoky/Oppenheimer/pkg/testredis"
)

// testTTL 尽量短，让依赖过期的测试不用等太久；又不能太短，否则 -race 下调度延迟会让
// 看门狗来不及续期。
const testTTL = 300 * time.Millisecond

// testCtx 给单个测试加超时保护，避免某个等待永远不返回。
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newLocker 创建 Locker，并在测试结束时确认所有看门狗都已退出。
func newLocker(t *testing.T, client redis.Scripter, prefix string, mutate ...func(*Options)) *Locker {
	t.Helper()
	opts := Options{TTL: testTTL, KeyPrefix: prefix, Logger: slog.New(slog.DiscardHandler)}
	for _, m := range mutate {
		m(&opts)
	}
	l, err := New(client, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		// 锁丢失时看门狗自己退出，可能比测试结束稍晚一点，所以等一小会儿。
		deadline := time.Now().Add(time.Second)
		for l.watchdogs.Load() != 0 {
			if time.Now().After(deadline) {
				t.Errorf("%d watchdog goroutines still running", l.watchdogs.Load())
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	return l
}

func mustRelease(ctx context.Context, t *testing.T, k *Lock) {
	t.Helper()
	if err := k.Release(ctx); err != nil {
		t.Errorf("Release: %v", err)
	}
}

// waitDone 等 done 关闭，超时则测试失败。
func waitDone(t *testing.T, done <-chan struct{}, within time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(within):
		t.Fatalf("context not canceled within %v", within)
	}
}

func TestNewValidates(t *testing.T) {
	client, prefix := testredis.New(t)
	if _, err := New(client, Options{KeyPrefix: prefix}); err == nil {
		t.Error("New with zero TTL: want error")
	}
	if _, err := New(nil, Options{TTL: time.Second}); err == nil {
		t.Error("New with nil client: want error")
	}
}

func TestTryAcquireMutualExclusion(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	l := newLocker(t, client, prefix)

	const n = 20
	var (
		start sync.WaitGroup
		wg    sync.WaitGroup
		mu    sync.Mutex
		won   []*Lock
	)
	start.Add(1)
	for range n {
		wg.Go(func() {
			start.Wait()
			k, err := l.TryAcquire(ctx, "res")
			switch {
			case err == nil:
				mu.Lock()
				won = append(won, k)
				mu.Unlock()
			case !errors.Is(err, ErrNotAcquired):
				t.Errorf("TryAcquire: %v", err)
			}
		})
	}
	start.Done()
	wg.Wait()

	if len(won) != 1 {
		t.Fatalf("winners = %d, want exactly 1", len(won))
	}
	mustRelease(ctx, t, won[0])
}

// TestAcquireMutualExclusion 用 Acquire 保护一个“读-改-写”分成两步的计数器。
// 没有互斥时，两个 goroutine 读到同一个旧值，计数会少。
//
// 计数器的读和写各自用原子操作，只有组合起来不是原子的。这样做是因为 race detector 看不到
// 经由 Redis 建立的 happens-before 关系（它不跟踪网络往返），用普通 int 的话即使锁是对的
// 也会报竞态。互斥性由最终计数和 inside 标志来验证。
func TestAcquireMutualExclusion(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	l := newLocker(t, client, prefix)

	const workers, rounds = 8, 5
	var (
		counter atomic.Int64
		inside  atomic.Bool
		wg      sync.WaitGroup
		mu      sync.Mutex
		tokens  []int64 // 按进入临界区的顺序记录 token
	)
	for range workers {
		wg.Go(func() {
			for range rounds {
				k, err := l.Acquire(ctx, "counter", 5*time.Millisecond)
				if err != nil {
					t.Errorf("Acquire: %v", err)
					return
				}
				if !inside.CompareAndSwap(false, true) {
					t.Error("two holders inside the critical section")
				}
				v := counter.Load()
				time.Sleep(time.Millisecond) // 放大竞争窗口
				counter.Store(v + 1)
				mu.Lock()
				tokens = append(tokens, k.Token())
				mu.Unlock()
				inside.Store(false)
				mustRelease(ctx, t, k)
			}
		})
	}
	wg.Wait()

	if got := counter.Load(); got != workers*rounds {
		t.Errorf("counter = %d, want %d", got, workers*rounds)
	}
	// 并发竞争时，拿到锁的顺序与 token 顺序一致：按进入临界区的顺序记录的 token 严格递增。
	if !slices.IsSorted(tokens) || len(slices.Compact(slices.Clone(tokens))) != len(tokens) {
		t.Errorf("tokens not strictly increasing in acquisition order: %v", tokens)
	}
}

func TestDifferentNamesIndependent(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	l := newLocker(t, client, prefix)

	a, err := l.TryAcquire(ctx, "a")
	if err != nil {
		t.Fatalf("TryAcquire a: %v", err)
	}
	defer mustRelease(ctx, t, a)
	b, err := l.TryAcquire(ctx, "b")
	if err != nil {
		t.Fatalf("TryAcquire b while a is held: %v", err)
	}
	defer mustRelease(ctx, t, b)
	if _, err := l.TryAcquire(ctx, "a"); !errors.Is(err, ErrNotAcquired) {
		t.Errorf("TryAcquire a again: err = %v, want ErrNotAcquired", err)
	}
}

func TestReleaseLetsOthersAcquire(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	l := newLocker(t, client, prefix)

	a, err := l.TryAcquire(ctx, "res")
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if err := a.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := a.Context().Err(); err == nil {
		t.Error("Context not canceled after Release")
	}
	// 释放后不用等 TTL，别人立刻能拿到。
	b, err := l.TryAcquire(ctx, "res")
	if err != nil {
		t.Fatalf("TryAcquire after Release: %v", err)
	}
	// 重复释放无害，且不会删掉 b 的锁。
	if err := a.Release(ctx); err != nil {
		t.Errorf("second Release: %v", err)
	}
	if got, _ := client.Get(ctx, b.key).Result(); got != b.value {
		t.Errorf("b's lock value = %q, want %q", got, b.value)
	}
	mustRelease(ctx, t, b)
}

func TestWatchdogKeepsLock(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	l := newLocker(t, client, prefix)

	k, err := l.TryAcquire(ctx, "res")
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	defer mustRelease(ctx, t, k)

	time.Sleep(4 * testTTL)

	if got, err := client.Get(ctx, k.key).Result(); err != nil || got != k.value {
		t.Fatalf("after 4*TTL: value = %q, err = %v; want own value", got, err)
	}
	if err := k.Context().Err(); err != nil {
		t.Errorf("Context canceled while held: %v", context.Cause(k.Context()))
	}
	if _, err := l.TryAcquire(ctx, "res"); !errors.Is(err, ErrNotAcquired) {
		t.Errorf("TryAcquire by other: err = %v, want ErrNotAcquired", err)
	}
}

// TestCrashedHolderExpires 模拟持有者崩溃：加锁后没有看门狗续期，TTL 后别人能拿到。
func TestCrashedHolderExpires(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	crashed := newLocker(t, client, prefix, func(o *Options) { o.noWatchdog = true })
	other := newLocker(t, client, prefix)

	a, err := crashed.TryAcquire(ctx, "res")
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	start := time.Now()
	b, err := other.Acquire(ctx, "res", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("Acquire after crash: %v", err)
	}
	defer mustRelease(ctx, t, b)
	if waited := time.Since(start); waited < testTTL/2 {
		t.Errorf("acquired after %v; lock expired suspiciously early", waited)
	}
	if err := a.Release(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("crashed holder Release: err = %v, want ErrLost", err)
	}
}

func TestAcquireRespectsContext(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	l := newLocker(t, client, prefix)

	k, err := l.TryAcquire(ctx, "res")
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	defer mustRelease(ctx, t, k)

	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = l.Acquire(short, "res", 10*time.Millisecond)
	if !errors.Is(err, ErrNotAcquired) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Acquire: err = %v, want ErrNotAcquired and DeadlineExceeded", err)
	}
}

// TestLockOutlivesAcquireContext 确认调用方加锁用的 ctx 结束后，锁和看门狗仍然有效。
func TestLockOutlivesAcquireContext(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	l := newLocker(t, client, prefix)

	acquireCtx, cancel := context.WithCancel(ctx)
	k, err := l.TryAcquire(acquireCtx, "res")
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	cancel()
	defer mustRelease(ctx, t, k)

	time.Sleep(2 * testTTL)
	if err := k.Context().Err(); err != nil {
		t.Fatalf("lock context canceled with acquire ctx: %v", err)
	}
	if got, _ := client.Get(ctx, k.key).Result(); got != k.value {
		t.Errorf("lock gone after acquire ctx canceled")
	}
}

// TestContextCanceledOnLoss 把 Redis 里的值改成别人的，看门狗下一次续期时发现锁丢了。
func TestContextCanceledOnLoss(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	l := newLocker(t, client, prefix)

	k, err := l.TryAcquire(ctx, "res")
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if err := client.Set(ctx, k.key, "someone-else", redis.KeepTTL).Err(); err != nil {
		t.Fatalf("overwrite lock: %v", err)
	}

	waitDone(t, k.Context().Done(), 2*testTTL)
	if cause := context.Cause(k.Context()); !errors.Is(cause, ErrLost) {
		t.Errorf("cause = %v, want ErrLost", cause)
	}
	if err := k.Release(ctx); !errors.Is(err, ErrLost) {
		t.Errorf("Release: err = %v, want ErrLost", err)
	}
	if got, _ := client.Get(ctx, k.key).Result(); got != "someone-else" {
		t.Errorf("Release deleted someone else's lock")
	}
}

// flakyScripter 在 fail 为 true 时让所有脚本调用返回错误，模拟网络故障。
type flakyScripter struct {
	redis.Scripter
	fail atomic.Bool
}

var errInjected = errors.New("injected network error")

func (f *flakyScripter) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	if f.fail.Load() {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(errInjected)
		return cmd
	}
	return f.Scripter.EvalSha(ctx, sha1, keys, args...)
}

func (f *flakyScripter) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	if f.fail.Load() {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(errInjected)
		return cmd
	}
	return f.Scripter.Eval(ctx, script, keys, args...)
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// TestRenewErrorsPastTTLMeansLost：续期出错不立刻判定丢锁，但距上次成功超过 TTL 后必须判定。
// 用注入的时钟决定“过了多久”，测试不依赖真实等待 TTL 或真的断开 Redis。
func TestRenewErrorsPastTTLMeansLost(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	flaky := &flakyScripter{Scripter: client}
	clock := &fakeClock{t: time.Now()}
	l := newLocker(t, flaky, prefix, func(o *Options) { o.now = clock.Now })

	k, err := l.TryAcquire(ctx, "res")
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	flaky.fail.Store(true)

	// 真实时间过去了几个续期周期，续期全部失败；但注入的时钟没走，不应判定丢锁。
	// 注意此时 Redis 里的锁其实已经快过期了：本地判断永远只是对真实状态的估计。
	time.Sleep(testTTL * 2 / 3)
	if err := k.Context().Err(); err != nil {
		t.Fatalf("lost after transient errors within TTL: %v", context.Cause(k.Context()))
	}

	clock.Advance(testTTL)
	waitDone(t, k.Context().Done(), testTTL)
	if cause := context.Cause(k.Context()); !errors.Is(cause, ErrLost) {
		t.Errorf("cause = %v, want ErrLost", cause)
	}

	flaky.fail.Store(false)
	// Redis 恢复后释放：锁或者已经过期（ErrLost），或者还在、属于自己（nil）。
	if err := k.Release(ctx); err != nil && !errors.Is(err, ErrLost) {
		t.Errorf("Release: %v", err)
	}
}

func TestFencingTokenIncreasing(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	l := newLocker(t, client, prefix)

	var last int64
	for i := range 5 {
		k, err := l.TryAcquire(ctx, "res")
		if err != nil {
			t.Fatalf("round %d: TryAcquire: %v", i, err)
		}
		if k.Token() <= last {
			t.Errorf("round %d: token %d not greater than previous %d", i, k.Token(), last)
		}
		last = k.Token()
		mustRelease(ctx, t, k)
	}
}

func TestLeaderAlternation(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	a := newLocker(t, client, prefix).Leader("job")
	b := newLocker(t, client, prefix).Leader("job")
	defer func() {
		for _, ld := range []*Leader{a, b} {
			if err := ld.Close(context.Background()); err != nil {
				t.Errorf("Close: %v", err)
			}
		}
	}()

	for i := range 10 {
		okA, err := a.Allow(ctx)
		if err != nil {
			t.Fatalf("a.Allow: %v", err)
		}
		okB, err := b.Allow(ctx)
		if err != nil {
			t.Fatalf("b.Allow: %v", err)
		}
		if okA == okB {
			t.Fatalf("round %d: a=%v b=%v, want exactly one leader", i, okA, okB)
		}
		if !okA {
			t.Fatalf("round %d: a was leader first and should stay leader", i)
		}
	}
	if a.Token() == 0 || b.Token() != 0 {
		t.Errorf("tokens a=%d b=%d, want a>0 b=0", a.Token(), b.Token())
	}
	firstToken := a.Token()

	// 持有者 Close 后另一个能接上。
	if err := a.Close(ctx); err != nil {
		t.Fatalf("a.Close: %v", err)
	}
	if ok, err := b.Allow(ctx); err != nil || !ok {
		t.Fatalf("b.Allow after a.Close = %v, %v; want true", ok, err)
	}
	if ok, err := a.Allow(ctx); err != nil || ok {
		t.Fatalf("a.Allow after b took over = %v, %v; want false", ok, err)
	}
	if b.Token() <= firstToken {
		t.Errorf("new leader token %d not greater than old %d", b.Token(), firstToken)
	}
}

func TestLeaderConcurrentAllow(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	leaders := make([]*Leader, 5)
	for i := range leaders {
		leaders[i] = newLocker(t, client, prefix).Leader("job")
	}
	var (
		wg      sync.WaitGroup
		winners atomic.Int32
	)
	for _, ld := range leaders {
		wg.Go(func() {
			ok, err := ld.Allow(ctx)
			if err != nil {
				t.Errorf("Allow: %v", err)
			}
			if ok {
				winners.Add(1)
			}
		})
	}
	wg.Wait()
	if got := winners.Load(); got != 1 {
		t.Errorf("winners = %d, want 1", got)
	}
	for _, ld := range leaders {
		if err := ld.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}
}

// TestLeaderRecompetesAfterLoss：持有者的锁被外部删除，看门狗发现后，下一次 Allow 重新竞争。
func TestLeaderRecompetesAfterLoss(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	a := newLocker(t, client, prefix).Leader("job")
	b := newLocker(t, client, prefix).Leader("job")
	defer func() {
		for _, ld := range []*Leader{a, b} {
			if err := ld.Close(context.Background()); err != nil {
				t.Errorf("Close: %v", err)
			}
		}
	}()

	if ok, err := a.Allow(ctx); err != nil || !ok {
		t.Fatalf("a.Allow = %v, %v; want true", ok, err)
	}
	a.mu.Lock()
	held := a.lock
	a.mu.Unlock()

	if err := client.Del(ctx, held.key).Err(); err != nil {
		t.Fatalf("delete lock: %v", err)
	}
	waitDone(t, held.Context().Done(), 2*testTTL)
	if a.Token() != 0 {
		t.Errorf("a.Token after loss = %d, want 0", a.Token())
	}

	// b 先来，抢到了。
	if ok, err := b.Allow(ctx); err != nil || !ok {
		t.Fatalf("b.Allow after loss = %v, %v; want true", ok, err)
	}
	if ok, err := a.Allow(ctx); err != nil || ok {
		t.Fatalf("a.Allow after b took over = %v, %v; want false", ok, err)
	}

	// b 释放后，a 的下一次 Allow 又能拿到。
	if err := b.Close(ctx); err != nil {
		t.Fatalf("b.Close: %v", err)
	}
	if ok, err := a.Allow(ctx); err != nil || !ok {
		t.Fatalf("a.Allow after b.Close = %v, %v; want true", ok, err)
	}
}
