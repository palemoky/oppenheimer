package worker

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// recHandler 把日志记录收集起来供断言。
type recHandler struct {
	mu   *sync.Mutex
	recs *[]slog.Record
}

func newRecLogger() (*slog.Logger, func() []slog.Record) {
	h := recHandler{mu: &sync.Mutex{}, recs: new([]slog.Record)}
	return slog.New(h), func() []slog.Record {
		h.mu.Lock()
		defer h.mu.Unlock()
		return slices.Clone(*h.recs)
	}
}

func (recHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h recHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, r)
	return nil
}
func (h recHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recHandler) WithGroup(string) slog.Handler      { return h }

func attrs(r slog.Record) map[string]slog.Value {
	m := map[string]slog.Value{}
	r.Attrs(func(a slog.Attr) bool { m[a.Key] = a.Value; return true })
	return m
}

// counter 记录 fn 的调用时刻（相对 bubble 起点）。
type counter struct {
	mu    sync.Mutex
	start time.Time
	at    []time.Duration
}

func newCounter() *counter { return &counter{start: time.Now()} }

func (c *counter) hit() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = append(c.at, time.Since(c.start))
}

func (c *counter) times() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.at)
}

func (c *counter) n() int { return len(c.times()) }

// run 在后台 Start，返回 Start 的结果通道。
func run(ctx context.Context, l *Loop) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- l.Start(ctx) }()
	return ch
}

func mustNew(t *testing.T, cfg Config, fn func(context.Context) error) *Loop {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = "test"
	}
	l, err := New(cfg, fn)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func mustStop(t *testing.T, l *Loop, res <-chan error) {
	t.Helper()
	if err := l.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := <-res; err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestNewValidation(t *testing.T) {
	ok := func(context.Context) error { return nil }
	for name, tc := range map[string]struct {
		cfg Config
		fn  func(context.Context) error
	}{
		"empty name":    {Config{Interval: time.Second}, ok},
		"zero interval": {Config{Name: "x"}, ok},
		"neg interval":  {Config{Name: "x", Interval: -1}, ok},
		"nil fn":        {Config{Name: "x", Interval: time.Second}, nil},
	} {
		if _, err := New(tc.cfg, tc.fn); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	l, err := New(Config{Name: "x", Interval: time.Second}, ok)
	if err != nil || l.Name() != "x" {
		t.Fatalf("err=%v", err)
	}
}

func TestRunsOnInterval(t *testing.T) {
	for _, runOnStart := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			c := newCounter()
			l := mustNew(t, Config{Interval: 10 * time.Second, RunOnStart: runOnStart}, func(context.Context) error {
				c.hit()
				return nil
			})
			res := run(t.Context(), l)
			time.Sleep(35 * time.Second)
			var want []time.Duration
			if runOnStart {
				want = []time.Duration{0, 10 * time.Second, 20 * time.Second, 30 * time.Second}
			} else {
				want = []time.Duration{10 * time.Second, 20 * time.Second, 30 * time.Second}
			}
			if got := c.times(); !slices.Equal(got, want) {
				t.Fatalf("runOnStart=%v: got %v, want %v", runOnStart, got, want)
			}
			mustStop(t, l, res)
		})
	}
}

func TestNoConcurrentRounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cur, peak atomic.Int32
		c := newCounter()
		l := mustNew(t, Config{Interval: 10 * time.Second}, func(context.Context) error {
			n := cur.Add(1)
			peak.Store(max(peak.Load(), n))
			c.hit()
			time.Sleep(25 * time.Second) // 比 Interval 长
			cur.Add(-1)
			return nil
		})
		res := run(t.Context(), l)
		time.Sleep(100 * time.Second)
		l.Kick() // 运行中的 Kick 也不能造成并发
		time.Sleep(time.Second)
		mustStop(t, l, res)
		if peak.Load() != 1 {
			t.Fatalf("peak concurrency = %d, want 1", peak.Load())
		}
		// 间隔从上一轮结束算起：10-35, 45-70, 80-105
		if got := c.times(); len(got) < 3 || got[1] != 45*time.Second || got[2] != 80*time.Second {
			t.Fatalf("start times = %v", got)
		}
	})
}

func TestErrorAndPanicContinue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logs := newRecLogger()
		var calls atomic.Int32
		boom := errors.New("boom")
		l := mustNew(t, Config{Name: "scan", Interval: time.Second, Logger: logger}, func(context.Context) error {
			switch calls.Add(1) {
			case 1:
				return boom
			case 2:
				panic("kaboom")
			}
			return nil
		})
		res := run(t.Context(), l)
		time.Sleep(3500 * time.Millisecond)
		if calls.Load() != 3 {
			t.Fatalf("calls = %d, want 3", calls.Load())
		}
		mustStop(t, l, res)

		var sawErr, sawPanic bool
		for _, r := range logs() {
			a := attrs(r)
			if r.Level != slog.LevelError || a["component"].String() != "scan" {
				t.Errorf("unexpected record %v %v", r.Level, r.Message)
				continue
			}
			if e, ok := a["error"].Any().(error); ok && errors.Is(e, boom) {
				sawErr = true
			}
			if a["panic"].String() == "kaboom" && a["stack"].String() != "" {
				sawPanic = true
			}
		}
		if !sawErr || !sawPanic {
			t.Fatalf("sawErr=%v sawPanic=%v", sawErr, sawPanic)
		}
	})
}

func TestKickTriggersImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCounter()
		l := mustNew(t, Config{Interval: time.Hour}, func(context.Context) error { c.hit(); return nil })
		res := run(t.Context(), l)
		synctest.Wait()
		l.Kick()
		synctest.Wait()
		if got := c.times(); !slices.Equal(got, []time.Duration{0}) {
			t.Fatalf("times = %v, want [0]", got)
		}
		mustStop(t, l, res)
	})
}

func TestKickCoalesces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCounter()
		release := make(chan struct{})
		l := mustNew(t, Config{Interval: time.Hour, RunOnStart: true}, func(context.Context) error {
			c.hit()
			if c.n() == 1 {
				<-release
			}
			return nil
		})
		res := run(t.Context(), l)
		synctest.Wait()
		// 第一轮阻塞期间连续 Kick 多次，只会合并成一次额外的运行。
		for range 10 {
			l.Kick()
		}
		close(release)
		synctest.Wait()
		if c.n() != 2 {
			t.Fatalf("calls = %d, want 2", c.n())
		}
		mustStop(t, l, res)
	})
}

func TestKickWhileRunningRerunsAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCounter()
		release := make(chan struct{})
		l := mustNew(t, Config{Interval: time.Hour, RunOnStart: true}, func(context.Context) error {
			c.hit()
			if c.n() == 1 {
				<-release
			}
			return nil
		})
		res := run(t.Context(), l)
		synctest.Wait() // 第一轮阻塞在 release
		l.Kick()
		l.Kick()
		synctest.Wait()
		if c.n() != 1 {
			t.Fatalf("calls = %d, want 1 while running", c.n())
		}
		close(release)
		synctest.Wait()
		if c.n() != 2 {
			t.Fatalf("calls = %d, want 2 after first round ends", c.n())
		}
		if got := c.times(); got[1] != 0 {
			t.Fatalf("second round at %v, want immediate", got[1])
		}
		mustStop(t, l, res)
	})
}

func TestKickBeforeStartDiscarded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCounter()
		l := mustNew(t, Config{Interval: time.Hour}, func(context.Context) error { c.hit(); return nil })
		l.Kick()
		res := run(t.Context(), l)
		synctest.Wait()
		if c.n() != 0 {
			t.Fatalf("calls = %d, want 0", c.n())
		}
		mustStop(t, l, res)
		l.Kick() // Stop 之后调用无害
	})
}

func TestStopWaitsForInflightRound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCounter()
		l := mustNew(t, Config{Interval: time.Second, RunOnStart: true}, func(context.Context) error {
			c.hit()
			time.Sleep(5 * time.Second)
			return nil
		})
		res := run(t.Context(), l)
		time.Sleep(time.Second)
		start := time.Now()
		if err := l.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		if el := time.Since(start); el != 4*time.Second {
			t.Fatalf("Stop took %v, want 4s", el)
		}
		if err := <-res; err != nil {
			t.Fatal(err)
		}
		if c.n() != 1 {
			t.Fatalf("calls = %d, want 1 (no new round after Stop)", c.n())
		}
	})
}

func TestStopTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, Config{Interval: time.Second, RunOnStart: true}, func(context.Context) error {
			time.Sleep(time.Hour) // 故意不理会 ctx
			return nil
		})
		res := run(t.Context(), l)
		synctest.Wait()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := l.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop err = %v, want DeadlineExceeded", err)
		}
		select {
		case err := <-res:
			t.Fatalf("Start returned (err=%v) before the round finished", err)
		default:
		}
		time.Sleep(time.Hour)
		if err := <-res; err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := l.Stop(t.Context()); err != nil { // 此时再 Stop 立即成功
			t.Fatal(err)
		}
	})
}

func TestStopIdempotentAndBeforeStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCounter()
		l := mustNew(t, Config{Interval: time.Second, RunOnStart: true}, func(context.Context) error { c.hit(); return nil })
		for range 3 {
			if err := l.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		if err := l.Start(t.Context()); err != nil {
			t.Fatalf("Start after Stop: %v", err)
		}
		if c.n() != 0 {
			t.Fatalf("calls = %d, want 0", c.n())
		}
	})
}

func TestStartTwiceFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustNew(t, Config{Interval: time.Second}, func(context.Context) error { return nil })
		res := run(t.Context(), l)
		synctest.Wait()
		if err := l.Start(t.Context()); err == nil {
			t.Fatal("second Start should fail")
		}
		mustStop(t, l, res)
	})
}

func TestStartCtxCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		var sawCancel atomic.Bool
		c := newCounter()
		l := mustNew(t, Config{Interval: time.Second, RunOnStart: true}, func(ctx context.Context) error {
			c.hit()
			<-ctx.Done()
			sawCancel.Store(true)
			return ctx.Err()
		})
		res := run(ctx, l)
		synctest.Wait()
		cancel()
		if err := <-res; err != nil {
			t.Fatalf("Start = %v, want nil", err)
		}
		if !sawCancel.Load() || c.n() != 1 {
			t.Fatalf("sawCancel=%v calls=%d", sawCancel.Load(), c.n())
		}
	})
}

type guardFunc func(context.Context) (bool, error)

func (g guardFunc) Allow(ctx context.Context) (bool, error) { return g(ctx) }

func TestGuard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, logs := newRecLogger()
		var step atomic.Int32
		c := newCounter()
		g := guardFunc(func(context.Context) (bool, error) {
			switch step.Add(1) {
			case 1:
				return false, nil
			case 2:
				return true, errors.New("lock backend down") // 有 error 时以 error 为准
			}
			return true, nil
		})
		l := mustNew(t, Config{Name: "g", Interval: time.Second, Logger: logger, Guard: g},
			func(context.Context) error { c.hit(); return nil })
		res := run(t.Context(), l)
		time.Sleep(3500 * time.Millisecond)
		// 前两轮被跳过，第三轮（t=3s）运行
		if got := c.times(); !slices.Equal(got, []time.Duration{3 * time.Second}) {
			t.Fatalf("times = %v", got)
		}
		mustStop(t, l, res)

		warns := 0
		for _, r := range logs() {
			if r.Level == slog.LevelWarn && attrs(r)["component"].String() == "g" {
				warns++
			}
		}
		if warns != 1 {
			t.Fatalf("warn logs = %d, want 1", warns)
		}
	})
}
