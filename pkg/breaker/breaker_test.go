package breaker_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/palemoky/oppenheimer/pkg/breaker"
)

// call 申请一次调用并立刻报告结果。返回是否被放行。
func call(b *breaker.Breaker, o breaker.Outcome) bool {
	done, err := b.Allow()
	if err != nil {
		return false
	}
	done(o)
	return true
}

func TestOpensAfterConsecutiveFailures(t *testing.T) {
	t.Parallel()
	b := breaker.New(breaker.Config{Failures: 3})

	// 中间的成功把计数清零：两次失败、一次成功、再两次失败，都不该打开。
	for _, o := range []breaker.Outcome{breaker.Failure, breaker.Failure, breaker.Success, breaker.Failure, breaker.Failure} {
		if !call(b, o) {
			t.Fatal("closed breaker rejected a call")
		}
	}
	if got := b.State(); got != breaker.Closed {
		t.Fatalf("state = %s after non-consecutive failures, want closed", got)
	}
	call(b, breaker.Failure)
	if got := b.State(); got != breaker.Open {
		t.Fatalf("state = %s after 3 consecutive failures, want open", got)
	}
	if _, err := b.Allow(); !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("Allow on open breaker = %v, want ErrOpen", err)
	}
	if s := b.Stats(); s.Opens != 1 || s.Rejected != 1 {
		t.Fatalf("stats = %+v, want 1 open and 1 rejected", s)
	}
}

func TestIgnoredDoesNotCount(t *testing.T) {
	t.Parallel()
	b := breaker.New(breaker.Config{Failures: 2})
	for range 10 {
		call(b, breaker.Ignored)
	}
	if got := b.State(); got != breaker.Closed {
		t.Fatalf("state = %s after ignored outcomes, want closed", got)
	}
	// Ignored 也不清零：失败、忽略、失败仍然是连续两次失败。
	call(b, breaker.Failure)
	call(b, breaker.Ignored)
	call(b, breaker.Failure)
	if got := b.State(); got != breaker.Open {
		t.Fatalf("state = %s, want open", got)
	}
}

func TestHalfOpenProbe(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := breaker.New(breaker.Config{Failures: 1, Cooldown: 5 * time.Second})
		call(b, breaker.Failure)

		time.Sleep(5*time.Second - time.Nanosecond)
		if _, err := b.Allow(); !errors.Is(err, breaker.ErrOpen) {
			t.Fatalf("Allow before cooldown = %v, want ErrOpen", err)
		}
		time.Sleep(time.Nanosecond)

		// 冷却到了：放一个探测，探测没回来之前其余的不放行。
		probe, err := b.Allow()
		if err != nil {
			t.Fatalf("probe not allowed after cooldown: %v", err)
		}
		if got := b.State(); got != breaker.HalfOpen {
			t.Fatalf("state = %s, want half-open", got)
		}
		if _, err := b.Allow(); !errors.Is(err, breaker.ErrOpen) {
			t.Fatalf("second Allow in half-open = %v, want ErrOpen", err)
		}

		// 探测失败：回到打开，冷却重新算起。
		probe(breaker.Failure)
		if got := b.State(); got != breaker.Open {
			t.Fatalf("state = %s after failed probe, want open", got)
		}
		time.Sleep(4 * time.Second)
		if _, err := b.Allow(); !errors.Is(err, breaker.ErrOpen) {
			t.Fatalf("Allow 4s after failed probe = %v, want ErrOpen", err)
		}
		time.Sleep(time.Second)

		// 探测成功：关闭，之后都放行。
		probe, err = b.Allow()
		if err != nil {
			t.Fatalf("second probe not allowed: %v", err)
		}
		probe(breaker.Success)
		if got := b.State(); got != breaker.Closed {
			t.Fatalf("state = %s after successful probe, want closed", got)
		}
		if !call(b, breaker.Success) {
			t.Fatal("closed breaker rejected a call")
		}
	})
}

func TestHalfOpenProbeIgnoredOrLost(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := breaker.New(breaker.Config{Failures: 1, Cooldown: time.Second})
		call(b, breaker.Failure)
		time.Sleep(time.Second)

		// 探测的调用方自己取消了：名额让出来，下一个请求立刻可以探测。
		probe, err := b.Allow()
		if err != nil {
			t.Fatal(err)
		}
		probe(breaker.Ignored)
		if got := b.State(); got != breaker.HalfOpen {
			t.Fatalf("state = %s after ignored probe, want half-open", got)
		}
		lost, err := b.Allow()
		if err != nil {
			t.Fatalf("Allow after ignored probe = %v, want allowed", err)
		}

		// 这个探测一直不报告结果：过了冷却时间再放一个，不会永远卡在半开。
		if _, err := b.Allow(); !errors.Is(err, breaker.ErrOpen) {
			t.Fatalf("Allow while a probe is outstanding = %v, want ErrOpen", err)
		}
		time.Sleep(time.Second)
		probe, err = b.Allow()
		if err != nil {
			t.Fatalf("Allow after the probe was lost for a cooldown = %v, want allowed", err)
		}
		probe(breaker.Success)
		if got := b.State(); got != breaker.Closed {
			t.Fatalf("state = %s, want closed", got)
		}
		// 丢失的那个探测很久以后才报告失败：它属于上一个状态，不能把已经关闭的熔断器再打开。
		lost(breaker.Failure)
		if got := b.State(); got != breaker.Closed {
			t.Fatalf("state = %s after a stale probe reported failure, want closed", got)
		}
	})
}

func TestStaleResultIgnored(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := breaker.New(breaker.Config{Failures: 2, Cooldown: time.Second})
		// 关闭时发出去的一个慢调用。
		slow, err := b.Allow()
		if err != nil {
			t.Fatal(err)
		}
		call(b, breaker.Failure)
		call(b, breaker.Failure)
		// 它在打开之后才成功返回：不能因此关闭。
		slow(breaker.Success)
		if got := b.State(); got != breaker.Open {
			t.Fatalf("state = %s after a stale success, want open", got)
		}
	})
}

func TestDoneIsIdempotent(t *testing.T) {
	t.Parallel()
	b := breaker.New(breaker.Config{Failures: 3})
	done, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		done(breaker.Failure)
	}
	if got := b.State(); got != breaker.Closed {
		t.Fatalf("state = %s after one call reported failure 5 times, want closed", got)
	}
}

func TestReport(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := breaker.New(breaker.Config{Failures: 2, Cooldown: 5 * time.Second})
		b.Report(false)
		b.Report(false)
		if got := b.State(); got != breaker.Open {
			t.Fatalf("state = %s, want open", got)
		}

		// 打开期间不断有失败报上来：冷却一直被重新算起，不进入半开。
		for range 5 {
			time.Sleep(4 * time.Second)
			b.Report(false)
			if _, err := b.Allow(); !errors.Is(err, breaker.ErrOpen) {
				t.Fatalf("Allow while failures keep being reported = %v, want ErrOpen", err)
			}
		}

		// 打开期间报告成功：直接关闭，不用等冷却。
		b.Report(true)
		if got := b.State(); got != breaker.Closed {
			t.Fatalf("state = %s after a reported success, want closed", got)
		}

		// 没有新的报告时，冷却过后放一个探测；探测的结果由 Report 给出。
		b.Report(false)
		b.Report(false)
		time.Sleep(5 * time.Second)
		if _, err := b.Allow(); err != nil {
			t.Fatalf("probe not allowed: %v", err)
		}
		if _, err := b.Allow(); !errors.Is(err, breaker.ErrOpen) {
			t.Fatalf("second Allow in half-open = %v, want ErrOpen", err)
		}
		b.Report(false)
		if got := b.State(); got != breaker.Open {
			t.Fatalf("state = %s after a failure reported in half-open, want open", got)
		}
	})
}

func TestOnChange(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var got []string
		b := breaker.New(breaker.Config{
			Name: "dep", Failures: 1, Cooldown: time.Second,
			OnChange: func(name string, from, to breaker.State) {
				got = append(got, name+":"+from.String()+"->"+to.String())
			},
		})
		call(b, breaker.Failure)
		time.Sleep(time.Second)
		call(b, breaker.Success)
		want := []string{"dep:closed->open", "dep:open->half-open", "dep:half-open->closed"}
		if len(got) != len(want) {
			t.Fatalf("transitions = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("transitions = %v, want %v", got, want)
			}
		}
	})
}

func TestDo(t *testing.T) {
	t.Parallel()
	b := breaker.New(breaker.Config{Failures: 1})
	errBusiness, errDown := errors.New("not found"), errors.New("connection refused")
	classify := func(err error) breaker.Outcome {
		if errors.Is(err, errBusiness) {
			return breaker.Success
		}
		return breaker.Failure
	}
	if err := b.Do(func() error { return errBusiness }, classify); !errors.Is(err, errBusiness) {
		t.Fatalf("Do = %v, want the business error", err)
	}
	if got := b.State(); got != breaker.Closed {
		t.Fatalf("state = %s after a business error, want closed", got)
	}
	if err := b.Do(func() error { return errDown }, classify); !errors.Is(err, errDown) {
		t.Fatalf("Do = %v, want the dependency error", err)
	}
	ran := false
	if err := b.Do(func() error { ran = true; return nil }, classify); !errors.Is(err, breaker.ErrOpen) || ran {
		t.Fatalf("Do on open breaker = %v, ran = %v; want ErrOpen and fn not run", err, ran)
	}
}

// 半开时并发的请求里只有一个被放行。带 -race 运行。
func TestHalfOpenAllowsOneProbeConcurrently(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := breaker.New(breaker.Config{Failures: 1, Cooldown: time.Second})
		call(b, breaker.Failure)
		time.Sleep(time.Second)

		var (
			allowed atomic.Int64
			wg      sync.WaitGroup
			start   = make(chan struct{})
		)
		for range 100 {
			wg.Go(func() {
				<-start
				if _, err := b.Allow(); err == nil {
					allowed.Add(1)
				}
			})
		}
		close(start)
		wg.Wait()
		if n := allowed.Load(); n != 1 {
			t.Fatalf("%d concurrent calls were allowed in half-open, want exactly 1", n)
		}
	})
}

// 关闭状态下并发地报告结果，计数不丢不乱。带 -race 运行。
func TestConcurrentUse(t *testing.T) {
	t.Parallel()
	b := breaker.New(breaker.Config{Failures: 1 << 30})
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			for j := range 1000 {
				o := breaker.Success
				if (i+j)%3 == 0 {
					o = breaker.Failure
				}
				call(b, o)
				b.Report(j%2 == 0)
				_ = b.Stats()
			}
		})
	}
	wg.Wait()
	if got := b.State(); got != breaker.Closed {
		t.Fatalf("state = %s, want closed", got)
	}
}

// 设了 MinFailureSpan 时，同一刻的一批失败不打开熔断器；失败持续超过这段时间才打开；中间有一次成功就重新算。
func TestMinFailureSpan(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	b := breaker.New(breaker.Config{Failures: 3, MinFailureSpan: time.Second, Now: func() time.Time { return now }})
	fail := func() {
		t.Helper()
		done, err := b.Allow()
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		done(breaker.Failure)
	}
	// 一次停顿：20 个在途的调用同时失败。
	for range 20 {
		fail()
	}
	if got := b.State(); got != breaker.Closed {
		t.Fatalf("state after a burst of simultaneous failures = %v, want closed", got)
	}
	// 还没到 1 秒，仍然关着。
	now = now.Add(900 * time.Millisecond)
	fail()
	if got := b.State(); got != breaker.Closed {
		t.Fatalf("state 900ms into the failure streak = %v, want closed", got)
	}
	// 一次成功说明依赖是好的，这一串作废。
	done, err := b.Allow()
	if err != nil {
		t.Fatal(err)
	}
	done(breaker.Success)
	now = now.Add(200 * time.Millisecond)
	for range 20 {
		fail()
	}
	if got := b.State(); got != breaker.Closed {
		t.Fatalf("state after a new burst following a success = %v, want closed", got)
	}
	// 失败持续了 1 秒：打开。
	now = now.Add(time.Second)
	fail()
	if got := b.State(); got != breaker.Open {
		t.Fatalf("state after failures lasting over the span = %v, want open", got)
	}
}
