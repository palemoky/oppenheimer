package idgen_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

func newSQLStore(t *testing.T) *idgen.SQLLeaseStore {
	t.Helper()
	db, _ := testdb.Empty(t)
	store, err := idgen.NewSQLLeaseStore(t.Context(), db)
	if err != nil {
		t.Fatalf("NewSQLLeaseStore: %v", err)
	}
	// 建表和补行可以重复执行（每个副本启动时都会做一遍）。
	if _, err := idgen.NewSQLLeaseStore(t.Context(), db); err != nil {
		t.Fatalf("second NewSQLLeaseStore: %v", err)
	}
	return store
}

// 多个进程同时启动：各自租到不同的号。
func TestSQLLeaseConcurrentAcquireDistinct(t *testing.T) {
	store := newSQLStore(t)
	const n = 24
	ids := make([]int64, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			ids[i], _, errs[i] = store.Acquire(t.Context(), fmt.Sprintf("owner-%d", i), time.Minute)
		})
	}
	wg.Wait()
	seen := map[int64]int{}
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("acquirer %d: %v", i, errs[i])
		}
		if j, dup := seen[ids[i]]; dup {
			t.Fatalf("acquirers %d and %d both got worker id %d", j, i, ids[i])
		}
		seen[ids[i]] = i
	}
}

// 64 个号都被占着时租不到；让出一个之后能租到它，读到的上界是让出时登记的。
func TestSQLLeaseExhaustionAndRelease(t *testing.T) {
	store := newSQLStore(t)
	ctx := t.Context()
	for i := range idgen.MaxWorkerID + 1 {
		if _, _, err := store.Acquire(ctx, fmt.Sprintf("owner-%d", i), time.Minute); err != nil {
			t.Fatalf("acquire #%d: %v", i, err)
		}
	}
	if _, _, err := store.Acquire(ctx, "late", time.Minute); !errors.Is(err, idgen.ErrNoWorkerID) {
		t.Fatalf("acquire with every id taken: err = %v, want ErrNoWorkerID", err)
	}

	// owner-5 先把上界抬到 9000，让出时改成实际用到的 7000。
	const id = 5 // 新表按节点号从小到大给，第 i 个租到的就是 i
	if held, err := store.Extend(ctx, id, "owner-5", time.Minute, 9000); err != nil || !held {
		t.Fatalf("Extend = %v, %v", held, err)
	}
	if err := store.Release(ctx, id, "owner-5", 7000); err != nil {
		t.Fatalf("Release: %v", err)
	}
	time.Sleep(5 * time.Millisecond) // 让出把过期时间设成“现在”，过一毫秒才算过期
	got, prior, err := store.Acquire(ctx, "late", time.Minute)
	if err != nil {
		t.Fatalf("acquire after a release: %v", err)
	}
	if got != id || prior != 7000 {
		t.Fatalf("acquired worker id %d with prior bound %d, want %d with 7000", got, prior, id)
	}
}

func TestSQLLeaseExtend(t *testing.T) {
	store := newSQLStore(t)
	ctx := t.Context()
	id, prior, err := store.Acquire(ctx, "a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if prior != 0 {
		t.Fatalf("prior bound of a fresh id = %d, want 0", prior)
	}
	// 连续两次一样的续租（可能落在同一毫秒里，更新不改变任何值）都要算成功，不能当成丢了租约。
	for range 3 {
		if held, err := store.Extend(ctx, id, "a", time.Minute, 5000); err != nil || !held {
			t.Fatalf("Extend by the holder = %v, %v", held, err)
		}
	}
	// 上界只升不降。
	if held, err := store.Extend(ctx, id, "a", time.Minute, 4000); err != nil || !held {
		t.Fatalf("Extend with a lower bound = %v, %v", held, err)
	}
	if held, err := store.Extend(ctx, id, "b", time.Minute, 99999); err != nil || held {
		t.Fatalf("Extend by a non-holder = %v, %v, want false", held, err)
	}
	// 别人的让出不生效。
	if err := store.Release(ctx, id, "b", 1); err != nil {
		t.Fatal(err)
	}
	if held, err := store.Extend(ctx, id, "a", time.Minute, 5000); err != nil || !held {
		t.Fatalf("Extend after a foreign Release = %v, %v", held, err)
	}
}

// 过期后被接手：新持有者读到旧持有者登记的上界，旧持有者的续租从此失败。
func TestSQLLeaseTakeoverAfterExpiry(t *testing.T) {
	store := newSQLStore(t)
	ctx := t.Context()
	const ttl = 300 * time.Millisecond
	owners := map[int64]string{}
	for i := range idgen.MaxWorkerID + 1 {
		owner := fmt.Sprintf("old-%d", i)
		id, _, err := store.Acquire(ctx, owner, ttl)
		if err != nil {
			t.Fatalf("acquire #%d: %v", i, err)
		}
		owners[id] = owner
		if held, err := store.Extend(ctx, id, owner, ttl, 1000+id); err != nil || !held {
			t.Fatalf("Extend = %v, %v", held, err)
		}
	}
	if _, _, err := store.Acquire(ctx, "new", time.Minute); !errors.Is(err, idgen.ErrNoWorkerID) {
		t.Fatalf("acquire before expiry: err = %v, want ErrNoWorkerID", err)
	}
	time.Sleep(ttl + 100*time.Millisecond)

	id, prior, err := store.Acquire(ctx, "new", time.Minute)
	if err != nil {
		t.Fatalf("acquire after expiry: %v", err)
	}
	if prior != 1000+id {
		t.Fatalf("prior bound = %d, want %d (what the old holder registered)", prior, 1000+id)
	}
	if held, err := store.Extend(ctx, id, owners[id], time.Minute, 5000); err != nil || held {
		t.Fatalf("old holder's Extend after takeover = %v, %v, want false", held, err)
	}
	// 过期了但还没被接手的，原持有者续上就是。
	other := (id + 1) % (idgen.MaxWorkerID + 1)
	if held, err := store.Extend(ctx, other, owners[other], time.Minute, 5000); err != nil || !held {
		t.Fatalf("Extend of an expired but unclaimed lease = %v, %v, want true", held, err)
	}
}

// 整条路径：两个进程各租一个号、发号、让出；第三个进程接着用。发出的号不重复。
func TestSQLLeaseEndToEnd(t *testing.T) {
	store := newSQLStore(t)
	cfg := idgen.LeaseConfig{Store: store, TTL: 3 * time.Second, RenewInterval: 200 * time.Millisecond, Guard: time.Millisecond}
	seen := map[int64]bool{}
	var mu sync.Mutex
	issue := func(l *idgen.Lease, n int) {
		for range n {
			id, err := l.Generator().Next(42)
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			mu.Lock()
			if seen[id] {
				t.Errorf("duplicate id %d", id)
			}
			seen[id] = true
			mu.Unlock()
		}
	}
	run := func() (*idgen.Lease, func()) {
		l, err := idgen.AcquireLease(t.Context(), cfg)
		if err != nil {
			t.Fatalf("AcquireLease: %v", err)
		}
		done := make(chan error, 1)
		go func() { done <- l.Start(context.WithoutCancel(t.Context())) }()
		return l, func() {
			if err := l.Stop(t.Context()); err != nil {
				t.Errorf("Stop: %v", err)
			}
			if err := <-done; err != nil {
				t.Errorf("Start returned %v", err)
			}
		}
	}

	a, stopA := run()
	b, stopB := run()
	if a.WorkerID() == b.WorkerID() {
		t.Fatalf("two live leases share worker id %d", a.WorkerID())
	}
	var wg sync.WaitGroup
	wg.Go(func() { issue(a, 5000) })
	wg.Go(func() { issue(b, 5000) })
	wg.Wait()
	time.Sleep(500 * time.Millisecond) // 跨过两次续租
	issue(a, 100)
	stopA()
	stopB()

	c, stopC := run()
	issue(c, 5000)
	stopC()
}
