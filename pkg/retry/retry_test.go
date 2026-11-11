package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

var errBoom = errors.New("boom")

// noJitter 关闭抖动，让等待时间可精确断言。
func noJitter() Policy {
	return Policy{MaxAttempts: 5, Initial: 100 * time.Millisecond, Max: time.Second, Multiplier: 2, Jitter: -1}
}

// withRand 注入固定的随机值，测试结束后恢复。
func withRand(t *testing.T, v float64) {
	t.Helper()
	old := randFloat
	randFloat = func() float64 { return v }
	t.Cleanup(func() { randFloat = old })
}

func TestDoFirstSuccessNoWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		calls := 0
		err := Do(t.Context(), noJitter(), func(_ context.Context, attempt int) error {
			calls++
			if attempt != 1 {
				t.Errorf("attempt = %d, want 1", attempt)
			}
			return nil
		})
		if err != nil || calls != 1 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
		if el := time.Since(start); el != 0 {
			t.Fatalf("elapsed %v, want 0", el)
		}
	})
}

func TestDoSucceedsOnNthAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		calls := 0
		err := Do(t.Context(), noJitter(), func(_ context.Context, attempt int) error {
			calls++
			if attempt != calls {
				t.Errorf("attempt = %d, want %d", attempt, calls)
			}
			if attempt < 4 {
				return errBoom
			}
			return nil
		})
		if err != nil || calls != 4 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
		// 100 + 200 + 400
		if el := time.Since(start); el != 700*time.Millisecond {
			t.Fatalf("elapsed %v, want 700ms", el)
		}
	})
}

func TestDoNonRetryable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := noJitter()
		p.Retryable = func(err error) bool { return !errors.Is(err, errBoom) }
		start := time.Now()
		calls := 0
		err := Do(t.Context(), p, func(context.Context, int) error {
			calls++
			return errBoom
		})
		if err != errBoom { //nolint:errorlint // 要求原样返回，断言是同一个值
			t.Fatalf("err = %v, want unwrapped errBoom", err)
		}
		if calls != 1 || time.Since(start) != 0 {
			t.Fatalf("calls = %d, elapsed = %v", calls, time.Since(start))
		}
	})
}

func TestDoExhausted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := noJitter()
		p.MaxAttempts = 3
		calls := 0
		start := time.Now()
		err := Do(t.Context(), p, func(_ context.Context, attempt int) error {
			calls++
			return fmt.Errorf("fail %d", attempt)
		})
		if calls != 3 {
			t.Fatalf("calls = %d, want 3", calls)
		}
		if err == nil || err.Error() != "fail 3" {
			t.Fatalf("err = %v, want last error", err)
		}
		// 最后一次失败后不再等待：100 + 200
		if el := time.Since(start); el != 300*time.Millisecond {
			t.Fatalf("elapsed %v, want 300ms", el)
		}
	})
}

func TestDoCancelDuringWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		start := time.Now()
		time.AfterFunc(150*time.Millisecond, cancel)
		calls := 0
		err := Do(ctx, noJitter(), func(context.Context, int) error {
			calls++
			return errBoom
		})
		if !errors.Is(err, context.Canceled) || !errors.Is(err, errBoom) {
			t.Fatalf("err = %v, want Canceled and errBoom", err)
		}
		// 第 1 次失败后等 100ms，第 2 次失败后在等待 200ms 期间的 150ms 处被取消。
		if calls != 2 {
			t.Fatalf("calls = %d, want 2", calls)
		}
		if el := time.Since(start); el != 150*time.Millisecond {
			t.Fatalf("elapsed %v, want 150ms", el)
		}
	})
}

func TestDoCtxCanceledBeforeCall(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	called := false
	err := Do(ctx, noJitter(), func(context.Context, int) error {
		called = true
		return nil
	})
	if called || !errors.Is(err, context.Canceled) {
		t.Fatalf("called=%v err=%v", called, err)
	}
}

func TestBackoffExponentialAndCap(t *testing.T) {
	p := noJitter()
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second}
	for i, w := range want {
		if got := Backoff(p, i+1); got != w {
			t.Errorf("Backoff(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestBackoffNoOverflow(t *testing.T) {
	for _, attempt := range []int{63, 64, 100, 1000, 1 << 30} {
		for _, p := range []Policy{noJitter(), {}, {Multiplier: 10, Max: time.Hour}} {
			got := Backoff(p, attempt)
			if got < 0 || got > p.withDefaults().Max {
				t.Fatalf("Backoff(%d) = %v out of range", attempt, got)
			}
		}
	}
	if got := Backoff(noJitter(), 1000); got != time.Second {
		t.Fatalf("Backoff(1000) = %v, want Max", got)
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	p := Policy{Initial: time.Second, Max: time.Minute, Multiplier: 2, Jitter: 0.5}
	withRand(t, 0)
	if got := Backoff(p, 1); got != 500*time.Millisecond {
		t.Errorf("lower = %v, want 500ms", got)
	}
	withRand(t, 0.5)
	if got := Backoff(p, 1); got != time.Second {
		t.Errorf("mid = %v, want 1s", got)
	}
	withRand(t, 0.999999)
	if got := Backoff(p, 1); got < 1499*time.Millisecond || got > 1500*time.Millisecond {
		t.Errorf("upper = %v, want ~1.5s", got)
	}
	// 抖动不能把结果推过 Max。
	if got := Backoff(p, 100); got > time.Minute {
		t.Errorf("capped = %v, want <= Max", got)
	}
}

func TestBackoffRealRandomWithinRange(t *testing.T) {
	p := Policy{Initial: time.Second, Max: time.Minute, Jitter: 0.2}
	for range 1000 {
		if got := Backoff(p, 1); got < 800*time.Millisecond || got > 1200*time.Millisecond {
			t.Fatalf("got %v out of [0.8s,1.2s]", got)
		}
	}
}

func TestZeroPolicyDefaults(t *testing.T) {
	withRand(t, 0.5) // 系数恰为 1，抖动不改变值
	if got := Backoff(Policy{}, 1); got != 50*time.Millisecond {
		t.Errorf("attempt1 = %v, want 50ms", got)
	}
	if got := Backoff(Policy{}, 2); got != 100*time.Millisecond {
		t.Errorf("attempt2 = %v, want 100ms", got)
	}
	if got := Backoff(Policy{}, 50); got != 5*time.Second {
		t.Errorf("attempt50 = %v, want 5s", got)
	}
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		err := Do(t.Context(), Policy{}, func(context.Context, int) error {
			calls++
			return errBoom
		})
		if !errors.Is(err, errBoom) || calls != 3 {
			t.Fatalf("err=%v calls=%d, want 3 calls", err, calls)
		}
	})
}
