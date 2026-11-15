package idgen

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// memStore 是内存里的 LeaseStore，语义和 SQLLeaseStore 一样：过期用它自己的时钟（time.Now，在 synctest 里是假时钟）判断，
// 过期最久的优先。down 为真时所有调用都失败，模拟存储不可用。
type memStore struct {
	mu   sync.Mutex
	rows []memRow
	down bool
}

type memRow struct {
	owner   string
	expires time.Time
	lastMs  int64
}

var errStoreDown = errors.New("store down")

func newMemStore(n int) *memStore { return &memStore{rows: make([]memRow, n)} }

func (s *memStore) setDown(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = v
}

func (s *memStore) row(id int64) memRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rows[id]
}

func (s *memStore) Acquire(_ context.Context, owner string, ttl time.Duration) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return 0, 0, errStoreDown
	}
	now, best := time.Now(), -1
	for i, r := range s.rows {
		if r.expires.Before(now) && (best < 0 || r.expires.Before(s.rows[best].expires)) {
			best = i
		}
	}
	if best < 0 {
		return 0, 0, ErrNoWorkerID
	}
	s.rows[best].owner, s.rows[best].expires = owner, now.Add(ttl)
	return int64(best), s.rows[best].lastMs, nil
}

func (s *memStore) Extend(_ context.Context, id int64, owner string, ttl time.Duration, boundMs int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return false, errStoreDown
	}
	r := &s.rows[id]
	if r.owner != owner {
		return false, nil
	}
	r.expires, r.lastMs = time.Now().Add(ttl), max(r.lastMs, boundMs)
	return true, nil
}

func (s *memStore) Release(_ context.Context, id int64, owner string, boundMs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return errStoreDown
	}
	if r := &s.rows[id]; r.owner == owner {
		r.owner, r.expires, r.lastMs = "", time.Now(), boundMs
	}
	return nil
}

// skewed 返回一个比真实时间快（或慢）d 的时钟。
func skewed(d time.Duration) func() time.Time {
	return func() time.Time { return time.Now().Add(d) }
}

func mustAcquire(t *testing.T, cfg LeaseConfig) *Lease {
	t.Helper()
	l, err := AcquireLease(t.Context(), cfg)
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	return l
}

func msOf(id int64) int64 { return id >> timeShift }

// synctest 的假时钟从 2000-01-01 开始，早于 Epoch。把时间推到 Epoch 之后再开始，发号才有合法的时间戳。
func pastEpoch() { time.Sleep(time.Until(Epoch.Add(24 * time.Hour))) }

// 存储不可用时：已经确认的上界用完之前照常发号，用完之后拒绝，存储恢复后的续租让它恢复。
func TestLeaseStopsAtBoundAndRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pastEpoch()
		store := newMemStore(4)
		l := mustAcquire(t, LeaseConfig{Store: store, TTL: 30 * time.Second})
		runErr := make(chan error, 1)
		go func() { runErr <- l.Start(t.Context()) }()

		time.Sleep(25 * time.Second) // 续过两次，上界在第 50 秒
		store.setDown(true)
		time.Sleep(20 * time.Second) // 第 45 秒，还在上界以内
		if _, err := l.Generator().Next(1); err != nil {
			t.Fatalf("Next within the confirmed bound: %v", err)
		}
		time.Sleep(10 * time.Second) // 第 55 秒，过了上界
		if _, err := l.Generator().Next(1); !errors.Is(err, ErrLeaseExpired) {
			t.Fatalf("Next past the bound: err = %v, want ErrLeaseExpired", err)
		}
		store.setDown(false)
		time.Sleep(2 * time.Second) // 失败后每秒重试一次
		if _, err := l.Generator().Next(1); err != nil {
			t.Fatalf("Next after the store recovered: %v", err)
		}

		if err := l.Stop(t.Context()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if err := <-runErr; err != nil {
			t.Fatalf("Start returned %v after Stop, want nil", err)
		}
		if _, err := l.Generator().Next(1); !errors.Is(err, ErrLeaseExpired) {
			t.Fatalf("Next after Stop: err = %v, want ErrLeaseExpired", err)
		}
	})
}

// 核心性质：同一个节点号先后两任持有者发的号不重叠，即使两台机器的时钟差得比租期的余量还多，
// 即使上一任（卡住、被网络隔开）在租约过期、被接手之后还在继续发号。
func TestTakeoverNeverOverlaps(t *testing.T) {
	for _, tc := range []struct {
		name         string
		skewA, skewB time.Duration
	}{
		{"same clocks", 0, 0},
		// 相差 20 秒：接手的要等到自己的时钟超过上一任的上界。再大就会超过一个租期，
		// 这个号会被跳过（TestAcquireSkipsFarFutureBound），而这里只有一个号。
		{"old holder fast, new holder slow", 10 * time.Second, -10 * time.Second},
		{"old holder slow, new holder fast", -10 * time.Second, 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pastEpoch()
				store := newMemStore(1) // 只有一个号，接手的一定是同一个
				a := mustAcquire(t, LeaseConfig{Store: store, Owner: "a", TTL: 30 * time.Second, Now: skewed(tc.skewA)})

				// a 不续租（没有 Start），每 100 毫秒发一个号，一直发到被拒绝为止。b 在 a 的租约过期后接手。
				var aIDs, bIDs []int64
				var wg sync.WaitGroup
				wg.Go(func() {
					for {
						id, err := a.Generator().Next(7)
						if err != nil {
							if !errors.Is(err, ErrLeaseExpired) {
								t.Errorf("a.Next: %v", err)
							}
							return
						}
						aIDs = append(aIDs, id)
						time.Sleep(100 * time.Millisecond)
					}
				})
				time.Sleep(30*time.Second + time.Millisecond)
				b := mustAcquire(t, LeaseConfig{Store: store, Owner: "b", TTL: 30 * time.Second, Now: skewed(tc.skewB)})
				if b.WorkerID() != a.WorkerID() {
					t.Fatalf("b got worker id %d, want a's %d", b.WorkerID(), a.WorkerID())
				}
				for range 50 {
					id, err := b.Generator().Next(7)
					if err != nil {
						t.Fatalf("b.Next: %v", err)
					}
					bIDs = append(bIDs, id)
					time.Sleep(100 * time.Millisecond)
				}
				wg.Wait()

				if len(aIDs) == 0 {
					t.Fatal("a issued nothing")
				}
				maxA, minB := msOf(aIDs[len(aIDs)-1]), msOf(bIDs[0])
				if maxA >= minB {
					t.Fatalf("intervals overlap: a issued up to %d ms, b started at %d ms", maxA, minB)
				}
				seen := make(map[int64]bool, len(aIDs)+len(bIDs))
				for _, id := range append(aIDs, bIDs...) {
					if seen[id] {
						t.Fatalf("duplicate id %d", id)
					}
					seen[id] = true
				}
			})
		})
	}
}

// 节点号被别人租走：Start 返回 ErrLeaseLost，之后不再发号。
func TestLeaseLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pastEpoch()
		store := newMemStore(1)
		l := mustAcquire(t, LeaseConfig{Store: store, Owner: "a", TTL: 30 * time.Second})
		runErr := make(chan error, 1)
		go func() { runErr <- l.Start(t.Context()) }()

		store.mu.Lock()
		store.rows[0].owner = "someone-else"
		store.mu.Unlock()

		if err := <-runErr; !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("Start returned %v, want ErrLeaseLost", err)
		}
		if _, err := l.Generator().Next(1); !errors.Is(err, ErrLeaseExpired) {
			t.Fatalf("Next after losing the lease: err = %v, want ErrLeaseExpired", err)
		}
		// 让出不能动别人的行。
		if err := l.Stop(t.Context()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if got := store.row(0).owner; got != "someone-else" {
			t.Fatalf("owner after Stop = %q, the new holder's row must be untouched", got)
		}
	})
}

// 让出时登记的上界是实际发过的最大毫秒，一个号也没发过时保持上一任留下的，不能更小。
func TestStopRegistersLastIssued(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pastEpoch()
		store := newMemStore(1)
		a := mustAcquire(t, LeaseConfig{Store: store, Owner: "a", TTL: 30 * time.Second})
		id, err := a.Generator().Next(1)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if err := a.Stop(t.Context()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if got := store.row(0); got.owner != "" || got.lastMs != msOf(id) {
			t.Fatalf("row after Stop = %+v, want released with last_ms %d", got, msOf(id))
		}
		if err := a.Stop(t.Context()); err != nil {
			t.Fatalf("second Stop: %v", err)
		}

		// b 接手后一个号也不发就让出：上界还是 a 留下的。接手不用等一个租期（Guard 之后就能用）。
		time.Sleep(time.Millisecond)
		start := time.Now()
		b := mustAcquire(t, LeaseConfig{Store: store, Owner: "b", TTL: 30 * time.Second})
		if waited := time.Since(start); waited > 10*time.Second {
			t.Fatalf("taking over a released id waited %v, want at most the guard (10s)", waited)
		}
		if err := b.Stop(t.Context()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if got := store.row(0).lastMs; got != msOf(id) {
			t.Fatalf("last_ms after an idle holder released = %d, want %d (the previous holder's)", got, msOf(id))
		}
	})
}

// 上界远在将来的号（上一任的时钟快了很多）不在它上面耗着，换一个；它的上界原样留着。
func TestAcquireSkipsFarFutureBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pastEpoch()
		store := newMemStore(2)
		far := nowMs(time.Now) + time.Hour.Milliseconds()
		store.rows[0].lastMs = far
		store.rows[1].expires = time.Now().Add(-time.Second) // 比 0 号晚过期，排在后面

		start := time.Now()
		l := mustAcquire(t, LeaseConfig{Store: store, TTL: 30 * time.Second})
		if l.WorkerID() != 1 {
			t.Fatalf("worker id = %d, want 1 (0 is bounded an hour ahead)", l.WorkerID())
		}
		if waited := time.Since(start); waited > time.Second {
			t.Fatalf("AcquireLease waited %v", waited)
		}
		if got := store.row(0); got.owner != "" || got.lastMs != far {
			t.Fatalf("skipped row = %+v, want released with last_ms %d intact", got, far)
		}
	})
}

func TestAcquireNoFreeWorkerID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pastEpoch()
		store := newMemStore(1)
		mustAcquire(t, LeaseConfig{Store: store, Owner: "a"})
		if _, err := AcquireLease(t.Context(), LeaseConfig{Store: store, Owner: "b"}); !errors.Is(err, ErrNoWorkerID) {
			t.Fatalf("err = %v, want ErrNoWorkerID", err)
		}
	})
}

// 被拒绝的调用不能改动“这一毫秒的序号用到哪了”：否则时钟回到同一毫秒时会重号。
func TestRejectedNextKeepsSequence(t *testing.T) {
	now := Epoch.Add(time.Hour)
	g, err := New(3, WithClock(func() time.Time { return now }, func(time.Duration) {}))
	if err != nil {
		t.Fatal(err)
	}
	g.leased = true
	g.boundMs.Store(nowMs(func() time.Time { return now }) + 1000)

	seen := map[int64]bool{}
	for range 3 {
		id, err := g.Next(1)
		if err != nil {
			t.Fatal(err)
		}
		seen[id] = true
	}
	at := now
	now = at.Add(time.Minute) // 时钟向前跳过了上界
	if _, err := g.Next(1); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("err = %v, want ErrLeaseExpired", err)
	}
	now = at // 又跳回来
	for range 3 {
		id, err := g.Next(1)
		if err != nil {
			t.Fatal(err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %d after a rejected call", id)
		}
		seen[id] = true
	}
}

func TestLeasedGeneratorRefusesAtOrBelowFloor(t *testing.T) {
	now := Epoch.Add(time.Hour)
	g, err := New(3, WithClock(func() time.Time { return now }, func(time.Duration) {}))
	if err != nil {
		t.Fatal(err)
	}
	g.leased = true
	g.floorMs = nowMs(func() time.Time { return now })
	g.boundMs.Store(g.floorMs + 1000)
	if _, err := g.Next(1); !errors.Is(err, ErrClockMovedBackwards) {
		t.Fatalf("at the floor: err = %v, want ErrClockMovedBackwards", err)
	}
	now = now.Add(time.Millisecond)
	if _, err := g.Next(1); err != nil {
		t.Fatalf("one millisecond past the floor: %v", err)
	}
}
