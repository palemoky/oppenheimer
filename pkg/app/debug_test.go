package app

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDebugMuxRoutes(t *testing.T) {
	mux := DebugMux()
	for _, tt := range []struct {
		method, path string
		want         int
		body         string // 响应里应包含的内容，空表示不检查
	}{
		{http.MethodGet, "/debug/pprof/", http.StatusOK, "goroutine"},
		{http.MethodGet, "/debug/pprof/goroutine?debug=1", http.StatusOK, "goroutine profile:"},
		{http.MethodGet, "/debug/pprof/heap?debug=1", http.StatusOK, "heap profile:"},
		{http.MethodGet, "/debug/pprof/allocs", http.StatusOK, ""},
		{http.MethodGet, "/debug/pprof/block", http.StatusOK, ""},
		{http.MethodGet, "/debug/pprof/mutex", http.StatusOK, ""},
		{http.MethodGet, "/debug/pprof/cmdline", http.StatusOK, ""},
		{http.MethodGet, "/debug/pprof/symbol", http.StatusOK, "num_symbols"},
		{http.MethodGet, "/debug/pprof/nosuchprofile", http.StatusNotFound, ""},
		// 和标准库在 DefaultServeMux 上的注册一致，只接受 GET。
		{http.MethodPost, "/debug/pprof/", http.StatusMethodNotAllowed, ""},
		// 调试端口只有 pprof。
		{http.MethodGet, "/healthz", http.StatusNotFound, ""},
	} {
		req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != tt.want {
			t.Errorf("%s %s = %d, want %d", tt.method, tt.path, rec.Code, tt.want)
			continue
		}
		if tt.body != "" && !strings.Contains(rec.Body.String(), tt.body) {
			t.Errorf("%s %s body does not contain %q", tt.method, tt.path, tt.body)
		}
	}
}

func TestListenDebugDisabledWithoutEnv(t *testing.T) {
	t.Setenv(DebugAddrEnv, "")
	s, err := ListenDebug(t.Context(), discard)
	if err != nil || s != nil {
		t.Fatalf("ListenDebug without %s = %v, %v; want nil, nil", DebugAddrEnv, s, err)
	}
}

func TestListenDebugBadAddr(t *testing.T) {
	t.Setenv(DebugAddrEnv, "not-an-address")
	if _, err := ListenDebug(t.Context(), discard); err == nil {
		t.Fatal("ListenDebug with a bad address succeeded, want error")
	}
}

func TestListenHTTPRejectsNilHandler(t *testing.T) {
	if _, err := ListenHTTP(t.Context(), "x", "127.0.0.1:0", nil); !errors.Is(err, errNilHandler) {
		t.Fatalf("ListenHTTP(nil handler) error = %v, want errNilHandler", err)
	}
}

// TestDebugServerLifecycle 走真实的监听和 Run：端点可达；停止时不等在途的长剖析，停止时限内就退出。
func TestDebugServerLifecycle(t *testing.T) {
	t.Setenv(DebugAddrEnv, "127.0.0.1:0")
	s, err := ListenDebug(t.Context(), discard)
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + s.Addr().String()
	// 连接进入 StateActive 说明服务端已读到请求、handler 将要运行：这时 Shutdown 会等它。
	// 只在客户端写完请求时就停止的话，服务端可能还没读到，测不出 Shutdown 和 Close 的区别。
	active := make(chan struct{}, 16)
	s.srv.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateActive {
			select {
			case active <- struct{}{}:
			default:
			}
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	const stopTimeout = 10 * time.Second
	runErr := make(chan error, 1)
	go func() { runErr <- RunWith(ctx, discard, Options{StopTimeout: stopTimeout}, WithDebug(s)...) }()

	get := func(ctx context.Context, path string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if err != nil {
			return nil, err
		}
		return http.DefaultClient.Do(req)
	}
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/goroutine?debug=1"} {
		resp, err := get(t.Context(), path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, resp.StatusCode)
		}
	}

	// 一个 30 秒的 CPU 剖析在途时停止：Shutdown 会等它，超过停止时限；Close 立刻断开。
	// 新开一条连接，免得复用上面的空闲连接时等到的是前面请求留下的 StateActive。
	http.DefaultClient.CloseIdleConnections()
	for len(active) > 0 {
		<-active
	}
	profileDone := make(chan error, 1)
	go func() {
		resp, err := get(t.Context(), "/debug/pprof/profile?seconds=30")
		if err == nil {
			_, err = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		profileDone <- err
	}()
	<-active

	start := time.Now()
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("RunWith = %v, want nil", err)
		}
	case <-time.After(stopTimeout + 5*time.Second):
		t.Fatal("RunWith did not return")
	}
	if d := time.Since(start); d > stopTimeout/2 {
		t.Errorf("stop took %v, want it not to wait for the in-flight profile", d)
	}
	// 在途的剖析被断开，不能等到 30 秒。剖析要到时长结束才写响应，所以客户端拿到的一定是连接错误。
	select {
	case err := <-profileDone:
		if err == nil {
			t.Error("in-flight profile completed, want the connection cut off")
		}
	case <-time.After(5 * time.Second):
		t.Error("in-flight profile request was not cut off")
	}
}

func TestWithDebug(t *testing.T) {
	a, b := &fake{name: "a"}, &fake{name: "b"}
	if got := WithDebug(nil, a, b); len(got) != 2 || got[0] != a {
		t.Errorf("WithDebug(nil, a, b) = %v, want [a b]", got)
	}
	d := &HTTPServer{name: "debug"}
	if got := WithDebug(d, a, b); len(got) != 3 || got[0] != d || got[1] != a || got[2] != b {
		t.Errorf("WithDebug(d, a, b) = %v, want [d a b]", got)
	}
}
