package app

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var discard = slog.New(slog.DiscardHandler)

// recorder 记录各组件的停止顺序。
type recorder struct {
	mu      sync.Mutex
	stopped []string
}

func (r *recorder) add(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = append(r.stopped, name)
}

// fake 是可控的 Component：Start 阻塞到 Stop 被调用，或在 failAfter 之后返回 failErr。
type fake struct {
	name       string
	rec        *recorder
	failAfter  time.Duration
	failErr    error
	ignoreStop bool

	once sync.Once
	done chan struct{}
}

func newFake(name string, rec *recorder) *fake {
	return &fake{name: name, rec: rec, done: make(chan struct{})}
}

func (f *fake) Name() string { return f.name }

func (f *fake) Start(context.Context) error {
	if f.failErr != nil {
		select {
		case <-time.After(f.failAfter):
			return f.failErr
		case <-f.done:
			return nil
		}
	}
	<-f.done
	return nil
}

func (f *fake) Stop(context.Context) error {
	f.rec.add(f.name)
	if !f.ignoreStop {
		f.once.Do(func() { close(f.done) })
	}
	return nil
}

func TestRunStopsInReverseOrderOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		components := []Component{newFake("db", rec), newFake("consumer", rec), newFake("http", rec)}

		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { result <- run(ctx, discard, Options{StopTimeout: time.Second}, nil, components) }()

		synctest.Wait() // 所有组件都已阻塞在 Start 里
		cancel()

		if err := <-result; err != nil {
			t.Fatalf("run() = %v, want nil on graceful shutdown", err)
		}
		if want := []string{"http", "consumer", "db"}; !slices.Equal(rec.stopped, want) {
			t.Errorf("stop order = %v, want %v", rec.stopped, want)
		}
	})
}

func TestRunReturnsComponentFailureAndStopsOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		boom := errors.New("boom")
		failing := newFake("consumer", rec)
		failing.failAfter = 3 * time.Second
		failing.failErr = boom
		components := []Component{newFake("db", rec), failing, newFake("http", rec)}

		err := run(t.Context(), discard, Options{StopTimeout: time.Second}, nil, components)

		if !errors.Is(err, boom) {
			t.Fatalf("run() = %v, want it to wrap %v", err, boom)
		}
		cerr, ok := errors.AsType[*ComponentError](err)
		if !ok || cerr.Component != "consumer" {
			t.Errorf("run() error = %v, want ComponentError for consumer", err)
		}
		if want := []string{"http", "consumer", "db"}; !slices.Equal(rec.stopped, want) {
			t.Errorf("stop order = %v, want %v", rec.stopped, want)
		}
	})
}

func TestRunTimesOutWhenComponentIgnoresStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		stuck := newFake("stuck", rec)
		stuck.ignoreStop = true
		// 让卡住的 goroutine 在测试结束前退出，否则 synctest 会报告泄漏。
		defer close(stuck.done)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		start := time.Now()
		err := run(ctx, discard, Options{StopTimeout: 5 * time.Second}, nil, []Component{stuck})

		if !errors.Is(err, ErrStopTimeout) {
			t.Fatalf("run() = %v, want ErrStopTimeout", err)
		}
		// synctest 里是虚拟时钟，这里精确等于时限。
		if elapsed := time.Since(start); elapsed != 5*time.Second {
			t.Errorf("run() returned after %v, want 5s", elapsed)
		}
	})
}

// 真实网络上的端到端验证：能服务请求，取消后优雅退出并释放端口。
func TestHTTPServerServesAndShutsDown(t *testing.T) {
	srv, err := ListenHTTP(t.Context(), "http", "127.0.0.1:0", HealthHandler("order"))
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + srv.Addr().String() + "/healthz"

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- run(ctx, discard, Options{StopTimeout: 5 * time.Second}, nil, []Component{srv}) }()

	body := get(t, url)
	var got healthResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("response is not JSON: %v: %s", err, body)
	}
	if want := (healthResponse{Service: "order", Status: "ok"}); got != want {
		t.Errorf("GET /healthz = %+v, want %+v", got, want)
	}

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("run() = %v, want nil", err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		t.Error("request succeeded after shutdown, want connection refused")
	}
}

func TestHTTPServerStopWithoutStartReleasesPort(t *testing.T) {
	srv, err := ListenHTTP(t.Context(), "http", "127.0.0.1:0", HealthHandler("order"))
	if err != nil {
		t.Fatal(err)
	}
	addr := srv.Addr().String()

	if err := srv.Stop(t.Context()); err != nil {
		t.Fatalf("Stop() = %v", err)
	}

	again, err := ListenHTTP(t.Context(), "http", addr, HealthHandler("order"))
	if err != nil {
		t.Fatalf("port not released after Stop: %v", err)
	}
	if err := again.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, url string) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// drainFake 在 fake 的基础上记录 Drain 被调用时的就绪状态和时刻。
type drainFake struct {
	*fake
	ready *Readiness

	mu        sync.Mutex
	drainedAt time.Time
	// readyAtDrain 是 Drain 被调用时 Readiness 的值：那时应当已经是未就绪。
	readyAtDrain bool
	stoppedAt    time.Time
}

func (d *drainFake) Drain() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.drainedAt = time.Now()
	d.readyAtDrain = d.ready.Ready()
}

func (d *drainFake) Stop(ctx context.Context) error {
	d.mu.Lock()
	d.stoppedAt = time.Now()
	d.mu.Unlock()
	return d.fake.Stop(ctx)
}

func (d *drainFake) times() (drainedAt time.Time, readyAtDrain bool, stoppedAt time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.drainedAt, d.readyAtDrain, d.stoppedAt
}

func TestReadinessAndDrainDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		ready := &Readiness{}
		c := &drainFake{fake: newFake("grpc", rec), ready: ready}
		if ready.Ready() {
			t.Fatal("ready before run")
		}

		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		opts := Options{Readiness: ready, DrainDelay: 8 * time.Second, StopTimeout: time.Second}
		go func() { result <- run(ctx, discard, opts, nil, []Component{c}) }()

		synctest.Wait()
		if !ready.Ready() {
			t.Fatal("not ready after components started")
		}

		start := time.Now()
		cancel()
		synctest.Wait()
		// 收到退出请求：立刻未就绪、Drain 已调用，但组件还没有被停止（仍在服务）。
		if ready.Ready() {
			t.Fatal("still ready after shutdown was requested")
		}
		drainedAt, readyAtDrain, stoppedAt := c.times()
		if drainedAt != start || readyAtDrain {
			t.Fatalf("Drain at %v (shutdown at %v), ready at that time = %v; want immediately and not ready",
				drainedAt, start, readyAtDrain)
		}
		if !stoppedAt.IsZero() {
			t.Fatal("component stopped before the drain delay elapsed")
		}

		if err := <-result; err != nil {
			t.Fatalf("run() = %v, want nil", err)
		}
		_, _, stoppedAt = c.times()
		if got := stoppedAt.Sub(start); got != 8*time.Second {
			t.Fatalf("component stopped %v after shutdown was requested, want the 8s drain delay", got)
		}
		if ready.Ready() {
			t.Fatal("ready after run returned")
		}
	})
}

func TestComponentFailureSkipsDrainDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		bad := newFake("consumer", rec)
		bad.failErr, bad.failAfter = errors.New("boom"), time.Second
		ok := newFake("http", rec)

		start := time.Now()
		err := run(t.Context(), discard, Options{DrainDelay: time.Minute, StopTimeout: time.Second}, nil, []Component{ok, bad})
		if !errors.Is(err, bad.failErr) {
			t.Fatalf("run() = %v, want it to wrap the component error", err)
		}
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Fatalf("run() returned after %v, want 1s (no drain delay after a component failure)", elapsed)
		}
	})
}

func TestSecondSignalDuringDrainDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		c := newFake("http", rec)
		ctx, cancel := context.WithCancel(t.Context())
		force := make(chan struct{})
		result := make(chan error, 1)
		start := time.Now()
		go func() {
			result <- run(ctx, discard, Options{DrainDelay: time.Minute, StopTimeout: 20 * time.Second}, force, []Component{c})
		}()
		synctest.Wait()
		cancel()
		time.Sleep(3 * time.Second)
		close(force)

		err := <-result
		if !errors.Is(err, ErrForced) {
			t.Fatalf("run() = %v, want ErrForced", err)
		}
		if elapsed := time.Since(start); elapsed != 3*time.Second {
			t.Fatalf("run() returned after %v, want 3s (right after the second signal)", elapsed)
		}
	})
}

func TestSecondSignalDuringStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{}
		stuck := newFake("stuck", rec)
		stuck.ignoreStop = true
		defer close(stuck.done)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		force := make(chan struct{})
		go func() {
			time.Sleep(2 * time.Second)
			close(force)
		}()

		start := time.Now()
		err := run(ctx, discard, Options{StopTimeout: 20 * time.Second}, force, []Component{stuck})
		if !errors.Is(err, ErrForced) || errors.Is(err, ErrStopTimeout) {
			t.Fatalf("run() = %v, want ErrForced only", err)
		}
		if elapsed := time.Since(start); elapsed != 2*time.Second {
			t.Fatalf("run() returned after %v, want 2s", elapsed)
		}
	})
}

func TestAdminMuxProbes(t *testing.T) {
	t.Parallel()
	ready := &Readiness{}
	srv, err := ListenHTTP(t.Context(), "http", "127.0.0.1:0", AdminMux("order", ready))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Start(t.Context()) }()
	defer func() { _ = srv.Stop(context.Background()) }()
	base := "http://" + srv.Addr().String()

	status := func(path string) int {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}

	// 启动中：活着，但未就绪。
	if got := status("/healthz"); got != http.StatusOK {
		t.Fatalf("/healthz while starting = %d, want 200", got)
	}
	if got := status("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("/readyz while starting = %d, want 503", got)
	}
	ready.set(stateReady)
	if got := status("/readyz"); got != http.StatusOK {
		t.Fatalf("/readyz when ready = %d, want 200", got)
	}
	// 停止中：仍然活着，不再就绪。
	ready.set(stateDraining)
	if got := status("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("/readyz while draining = %d, want 503", got)
	}
	if got := status("/healthz"); got != http.StatusOK {
		t.Fatalf("/healthz while draining = %d, want 200", got)
	}

	// 没有接就绪状态的服务按一直就绪处理。
	var none *Readiness
	if !none.Ready() {
		t.Fatal("nil Readiness is not ready")
	}
}

func TestDrainDelayFromEnv(t *testing.T) {
	for _, tc := range []struct {
		val  string
		want time.Duration
		bad  bool
	}{
		{"", 0, false}, {"0s", 0, false}, {"7s", 7 * time.Second, false}, {"-1s", 0, true}, {"soon", 0, true},
	} {
		t.Setenv(DrainDelayEnv, tc.val)
		got, err := DrainDelayFromEnv()
		if (err != nil) != tc.bad || got != tc.want {
			t.Errorf("DRAIN_DELAY=%q: got %v, %v; want %v, error=%v", tc.val, got, err, tc.want, tc.bad)
		}
	}
}
