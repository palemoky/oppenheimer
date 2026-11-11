package idgen

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeClock 是手动推进的时钟。sleep 会把时间向前拨，模拟等待。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) sleep(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func newWithFakeClock(t *testing.T, start time.Time) (*Generator, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: start}
	g, err := New(1, WithClock(clock.now, clock.sleep))
	if err != nil {
		t.Fatal(err)
	}
	return g, clock
}

func TestNewRejectsOutOfRangeWorker(t *testing.T) {
	for _, id := range []int64{-1, MaxWorkerID + 1} {
		if _, err := New(id); err == nil {
			t.Errorf("New(%d) succeeded, want error", id)
		}
	}
}

func TestIDEmbedsGeneAndTime(t *testing.T) {
	at := Epoch.Add(1234567 * time.Millisecond)
	g, _ := newWithFakeClock(t, at)

	for _, userID := range []int64{0, 1, 63, 64, 1000003, 1<<40 + 17} {
		id, err := g.Next(userID)
		if err != nil {
			t.Fatal(err)
		}
		if id <= 0 {
			t.Errorf("id %d is not positive", id)
		}
		if Gene(id) != GeneOf(userID) {
			t.Errorf("Gene(id) = %d, want %d", Gene(id), GeneOf(userID))
		}
		// 订单号和用户号对 2 的幂取模结果相同，这是按订单号路由分片的依据。
		for _, shards := range []int64{2, 4, 8, 64} {
			if id%shards != userID%shards {
				t.Errorf("id %% %d = %d, user %% %d = %d", shards, id%shards, shards, userID%shards)
			}
		}
		if got := Time(id); !got.Equal(at) {
			t.Errorf("Time(id) = %v, want %v", got, at)
		}
	}
}

func TestIDsStrictlyIncreaseAcrossSequenceRollover(t *testing.T) {
	g, _ := newWithFakeClock(t, Epoch.Add(time.Hour))

	// 超过单毫秒容量（1024），迫使生成器等待下一毫秒。
	var prev int64
	for i := range 3000 {
		id, err := g.Next(int64(i))
		if err != nil {
			t.Fatal(err)
		}
		// 比较时去掉基因位：基因来自调用方，不参与排序保证。
		if id>>geneBits <= prev>>geneBits && i > 0 {
			t.Fatalf("id #%d = %d is not greater than previous %d", i, id, prev)
		}
		prev = id
	}
}

func TestSmallClockRollbackWaits(t *testing.T) {
	start := Epoch.Add(time.Hour)
	g, clock := newWithFakeClock(t, start)

	first, err := g.Next(0)
	if err != nil {
		t.Fatal(err)
	}

	clock.set(start.Add(-3 * time.Millisecond))
	second, err := g.Next(0)
	if err != nil {
		t.Fatalf("Next after 3ms rollback: %v", err)
	}
	if second <= first {
		t.Errorf("id after rollback %d is not greater than %d", second, first)
	}
}

func TestLargeClockRollbackFails(t *testing.T) {
	start := Epoch.Add(time.Hour)
	g, clock := newWithFakeClock(t, start)

	if _, err := g.Next(0); err != nil {
		t.Fatal(err)
	}

	clock.set(start.Add(-time.Second))
	if _, err := g.Next(0); !errors.Is(err, ErrClockMovedBackwards) {
		t.Fatalf("Next after 1s rollback = %v, want ErrClockMovedBackwards", err)
	}

	// 时钟追上之后恢复发号。
	clock.set(start.Add(time.Millisecond))
	if _, err := g.Next(0); err != nil {
		t.Errorf("Next after clock recovered: %v", err)
	}
}

func TestConcurrentNextIsUnique(t *testing.T) {
	g, err := New(3)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines, perGoroutine = 16, 5000
	results := make([][]int64, goroutines)

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Go(func() {
			ids := make([]int64, 0, perGoroutine)
			for j := range perGoroutine {
				id, err := g.Next(int64(i*perGoroutine + j))
				if err != nil {
					t.Error(err)
					return
				}
				ids = append(ids, id)
			}
			results[i] = ids
		})
	}
	wg.Wait()

	seen := make(map[int64]struct{}, goroutines*perGoroutine)
	for _, ids := range results {
		for _, id := range ids {
			// 唯一性必须在去掉基因位之后仍然成立，否则两个不同用户可能拿到只差基因的 ID。
			key := id >> geneBits
			if _, dup := seen[key]; dup {
				t.Fatalf("duplicate id %d", id)
			}
			seen[key] = struct{}{}
		}
	}
	if len(seen) != goroutines*perGoroutine {
		t.Errorf("got %d unique ids, want %d", len(seen), goroutines*perGoroutine)
	}
}

func BenchmarkNext(b *testing.B) {
	g, err := New(1)
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, err := g.Next(42); err != nil {
			b.Fatal(err)
		}
	}
}
