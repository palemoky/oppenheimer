package lock

// 本文件是两个刻意制造锁失效的实验。它们共用同一个剧本：
//
//  1. A 加锁。
//  2. A 卡顿（GC、虚拟机迁移……）。测试里用 stopWatchdog 停掉 A 的看门狗来模拟：
//     A 的进程“冻住”了，没有续期，也不知道锁快过期了。
//  3. 锁过期，B 加锁成功。
//  4. A 醒来，仍以为自己持有锁（A 的 Context 没被取消，因为发现丢锁的看门狗也冻住了）。
//
// 实验一看 A 醒来后解锁会怎样，实验二看 A 醒来后继续写资源会怎样。
// 每个实验都配一个对照测试，展示没有相应防护时会出什么事。

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/palemoky/oppenheimer/pkg/testredis"
)

// stalledHolder 走完剧本的 1 到 3 步：返回卡顿后醒来的 A 和新持有者 B。
func stalledHolder(ctx context.Context, t *testing.T, client redis.Scripter, prefix string) (a, b *Lock) {
	t.Helper()
	la := newLocker(t, client, prefix)
	lb := newLocker(t, client, prefix)

	a, err := la.TryAcquire(ctx, "res")
	if err != nil {
		t.Fatalf("A TryAcquire: %v", err)
	}
	a.stopWatchdog() // A 卡顿

	// B 一直重试，直到 A 的锁过期。
	b, err = lb.Acquire(ctx, "res", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("B Acquire: %v", err)
	}
	if err := a.Context().Err(); err != nil {
		t.Fatalf("A should not know it lost the lock, but context is %v", err)
	}
	return a, b
}

// ---------------------------------------------------------------------------
// 实验一：过期后误删。演示为什么解锁必须校验 token（锁的随机值）。

// TestExperimentStaleReleaseIsRejected：A 醒来后调用 Release，Lua 脚本发现值不是自己的，
// 什么都不删，返回 ErrLost；B 的锁完好无损。
func TestExperimentStaleReleaseIsRejected(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	a, b := stalledHolder(ctx, t, client, prefix)
	defer mustRelease(ctx, t, b)

	if err := a.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("A Release: err = %v, want ErrLost", err)
	}
	if got, err := client.Get(ctx, b.key).Result(); err != nil || got != b.value {
		t.Fatalf("B's lock after A's Release: value = %q, err = %v; want B's value", got, err)
	}
}

// TestExperimentNaiveDelDeletesOthersLock 是对照：A 醒来后用直接 DEL 解锁，删掉的是 B 的锁。
// 接着 C 也能加锁成功，而 B 还在临界区里，互斥被打破。
func TestExperimentNaiveDelDeletesOthersLock(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	a, b := stalledHolder(ctx, t, client, prefix)

	// A 的“解锁”：不看值是谁的，直接删。
	if err := client.Del(ctx, a.key).Err(); err != nil {
		t.Fatalf("naive DEL: %v", err)
	}
	if n, _ := client.Exists(ctx, b.key).Result(); n != 0 {
		t.Fatal("expected naive DEL to delete B's lock, but it is still there")
	}

	c, err := newLocker(t, client, prefix).TryAcquire(ctx, "res")
	if err != nil {
		t.Fatalf("C TryAcquire after naive DEL: %v", err)
	}
	if b.Context().Err() != nil {
		// B 的看门狗可能已经发现了，但在发现之前，B 和 C 同时“持有”锁。
		t.Log("B's watchdog already noticed the loss")
	}
	t.Log("B and C both believe they hold the lock: mutual exclusion is broken")

	// 收尾。A、B 的锁都已经不在了。
	_ = a.Release(ctx)
	_ = b.Release(ctx)
	mustRelease(ctx, t, c)
}

// ---------------------------------------------------------------------------
// 实验二：僵尸持有者与 fencing token。
//
// 实验一里的 A 只是解锁，危害有限；更常见的是 A 醒来后继续做“持锁期间”的工作，
// 比如写一个文件、更新一行数据。锁本身对此无能为力：锁服务（Redis）根本不参与 A 对资源的写入，
// A 也没法在每次写之前确认锁还在（确认和写入之间仍可能卡顿）。
// 唯一可靠的办法是让资源自己判断：每次写入带上 fencing token，资源记住见过的最大 token，
// 拒绝更小的。B 的 token 一定比 A 大（加锁和 INCR 是原子的），所以 B 写过之后，A 的写入必被拒绝。

var errStaleToken = errors.New("fenced store: stale fencing token")

// fencedStore 是演示用的受保护资源：一个只存一个值的内存存储。
type fencedStore struct {
	checkToken bool // false 时用作对照：不检查 token 的普通存储

	mu       sync.Mutex
	maxToken int64
	value    string
}

// Write 带 token 写入。checkToken 时拒绝比已见过的最大值更小的 token。
// 相等的 token 允许：那是同一个持有者的多次写入。
func (s *fencedStore) Write(token int64, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkToken && token < s.maxToken {
		return errStaleToken
	}
	s.maxToken = max(s.maxToken, token)
	s.value = value
	return nil
}

func (s *fencedStore) Value() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value
}

// TestExperimentFencingRejectsZombie：A 带着旧 token 写入，被资源拒绝。
func TestExperimentFencingRejectsZombie(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	a, b := stalledHolder(ctx, t, client, prefix)
	defer mustRelease(ctx, t, b)
	defer func() { _ = a.Release(ctx) }()

	t1, t2 := a.Token(), b.Token()
	if t2 <= t1 {
		t.Fatalf("B's token %d not greater than A's %d", t2, t1)
	}

	store := &fencedStore{checkToken: true}
	if err := store.Write(t2, "written by B"); err != nil {
		t.Fatalf("B write: %v", err)
	}
	// A 醒来，Context 没被取消，它以为自己还持有锁，继续写。
	if err := store.Write(t1, "written by A"); !errors.Is(err, errStaleToken) {
		t.Fatalf("zombie A write: err = %v, want errStaleToken", err)
	}
	if got := store.Value(); got != "written by B" {
		t.Errorf("store value = %q, want B's write", got)
	}
}

// TestExperimentNoFencingZombieOverwrites 是对照：资源不检查 token，A 的过期写入覆盖了 B 的。
func TestExperimentNoFencingZombieOverwrites(t *testing.T) {
	ctx := testCtx(t)
	client, prefix := testredis.New(t)
	a, b := stalledHolder(ctx, t, client, prefix)
	defer mustRelease(ctx, t, b)
	defer func() { _ = a.Release(ctx) }()

	store := &fencedStore{checkToken: false}
	if err := store.Write(b.Token(), "written by B"); err != nil {
		t.Fatalf("B write: %v", err)
	}
	if err := store.Write(a.Token(), "written by A"); err != nil {
		t.Fatalf("zombie A write: %v", err)
	}
	if got := store.Value(); got != "written by A" {
		t.Fatalf("store value = %q; expected the zombie's write to overwrite B's", got)
	}
}
