package bulkhead_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/bulkhead"
)

func TestAcquireUpToLimitThenReject(t *testing.T) {
	t.Parallel()
	b, err := bulkhead.New(3)
	if err != nil {
		t.Fatal(err)
	}
	var releases []func()
	for i := range 3 {
		r, err := b.Acquire()
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		releases = append(releases, r)
	}
	if _, err := b.Acquire(); !errors.Is(err, bulkhead.ErrFull) {
		t.Fatalf("4th acquire = %v, want ErrFull", err)
	}
	if b.InFlight() != 3 || b.Rejected() != 1 {
		t.Fatalf("in flight %d rejected %d, want 3 and 1", b.InFlight(), b.Rejected())
	}
	// 释放一个就能再进一个；同一个 release 调两次只算一次。
	releases[0]()
	releases[0]()
	if b.InFlight() != 2 {
		t.Fatalf("in flight = %d after releasing one slot twice, want 2", b.InFlight())
	}
	if _, err := b.Acquire(); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestConcurrentAcquireNeverExceedsLimit(t *testing.T) {
	t.Parallel()
	const limit, workers, rounds = 8, 64, 2000
	b, err := bulkhead.New(limit)
	if err != nil {
		t.Fatal(err)
	}
	var (
		inside atomic.Int64
		peak   atomic.Int64
		ok     atomic.Int64
		wg     sync.WaitGroup
	)
	for range workers {
		wg.Go(func() {
			for range rounds {
				release, err := b.Acquire()
				if err != nil {
					continue
				}
				n := inside.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				ok.Add(1)
				inside.Add(-1)
				release()
			}
		})
	}
	wg.Wait()
	if p := peak.Load(); p > limit {
		t.Fatalf("%d callers held a slot at once, limit is %d", p, limit)
	}
	if b.InFlight() != 0 {
		t.Fatalf("in flight = %d after everyone released, want 0", b.InFlight())
	}
	if ok.Load()+b.Rejected() != workers*rounds {
		t.Fatalf("acquired %d + rejected %d != %d attempts", ok.Load(), b.Rejected(), workers*rounds)
	}
}

func TestNilAndInvalid(t *testing.T) {
	t.Parallel()
	var none *bulkhead.Bulkhead
	for range 100 {
		release, err := none.Acquire()
		if err != nil {
			t.Fatalf("nil bulkhead rejected: %v", err)
		}
		release()
	}
	if none.InFlight() != 0 || none.Rejected() != 0 {
		t.Fatal("nil bulkhead reports usage")
	}
	for _, n := range []int{0, -1} {
		if _, err := bulkhead.New(n); err == nil {
			t.Errorf("New(%d) accepted", n)
		}
	}
}
