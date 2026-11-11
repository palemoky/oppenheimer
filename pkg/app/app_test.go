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
		go func() { result <- run(ctx, discard, time.Second, components) }()

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

		err := run(t.Context(), discard, time.Second, components)

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
		err := run(ctx, discard, 5*time.Second, []Component{stuck})

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
	go func() { result <- run(ctx, discard, 5*time.Second, []Component{srv}) }()

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
