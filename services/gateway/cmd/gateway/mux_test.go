package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi"
)

// muxes 按 main 的装配方式构造两个端口的路由。下游客户端为空：这里的请求都不会走到 handler。
func muxes(t *testing.T) (public, admin http.Handler, set *degrade.Set) {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	set = degrade.New(serviceName, logger)
	api := httpapi.New(httpapi.Deps{}, httpapi.Config{Logger: logger, Degrade: set})
	readiness := &app.Readiness{}
	return publicMux(api, readiness), adminMux(readiness, set), set
}

func statusOf(t *testing.T, h http.Handler, method, path string) int {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

// TestPublicMuxHidesDegrade 钉住：降级开关的接口没有鉴权，在对外的端口上任何路径都是 404，改不了开关。
func TestPublicMuxHidesDegrade(t *testing.T) {
	public, _, set := muxes(t)
	for _, tt := range []struct{ method, path string }{
		{http.MethodGet, "/degrade"},
		{http.MethodGet, "/degrade/"},
		{http.MethodPut, "/degrade/" + httpapi.SwitchStockQuery + "?mode=off"},
		{http.MethodPut, "/degrade/" + httpapi.SwitchUserOrders + "?mode=off"},
		{http.MethodPut, "/degrade/" + httpapi.SwitchMerchantOrders + "?mode=off"},
		{http.MethodPost, "/degrade/" + httpapi.SwitchStockQuery + "?mode=off"},
	} {
		if got := statusOf(t, public, tt.method, tt.path); got != http.StatusNotFound {
			t.Errorf("public %s %s = %d, want 404", tt.method, tt.path, got)
		}
	}
	for _, v := range set.Snapshot() {
		if v.Mode != degrade.ModeAuto {
			t.Errorf("switch %s is %s after requests to the public port, want auto", v.Name, v.Mode)
		}
	}
}

// TestMuxesHidePprof 钉住：pprof 只在单独的调试端口（DEBUG_ADDR）上。对外的 8080 是 API 端口，
// 管理端口也不挂；包里导入了 net/http/pprof，它往 DefaultServeMux 上注册的那一份不能经这两个端口漏出来。
func TestMuxesHidePprof(t *testing.T) {
	public, admin, _ := muxes(t)
	for _, path := range []string{
		"/debug/pprof/",
		"/debug/pprof/goroutine?debug=1",
		"/debug/pprof/heap",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile?seconds=1",
		"/debug/pprof/trace?seconds=1",
	} {
		if got := statusOf(t, public, http.MethodGet, path); got != http.StatusNotFound {
			t.Errorf("public GET %s = %d, want 404", path, got)
		}
		if got := statusOf(t, admin, http.MethodGet, path); got != http.StatusNotFound {
			t.Errorf("admin GET %s = %d, want 404", path, got)
		}
	}
	// 对照：同样的路径在调试端口的 mux 上是可达的，上面的 404 不是路径写错了。
	if got := statusOf(t, app.DebugMux(), http.MethodGet, "/debug/pprof/goroutine?debug=1"); got != http.StatusOK {
		t.Errorf("debug GET /debug/pprof/goroutine = %d, want 200", got)
	}
}

func TestPublicMuxProbes(t *testing.T) {
	public, _, _ := muxes(t)
	if got := statusOf(t, public, http.MethodGet, "/healthz"); got != http.StatusOK {
		t.Errorf("public /healthz = %d, want 200", got)
	}
	// 还没有经过 RunWith，就绪状态是零值（未就绪）：503，说明 /readyz 挂上了并且跟着就绪状态走。
	if got := statusOf(t, public, http.MethodGet, "/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("public /readyz = %d, want 503 before ready", got)
	}
}

func TestAdminMux(t *testing.T) {
	_, admin, set := muxes(t)
	for _, tt := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodGet, "/readyz", http.StatusServiceUnavailable},
		{http.MethodGet, "/degrade", http.StatusOK},
		{http.MethodPut, "/degrade/" + httpapi.SwitchStockQuery + "?mode=off", http.StatusOK},
		// 内部端口不提供 API。
		{http.MethodGet, "/api/v1/orders", http.StatusNotFound},
		{http.MethodGet, "/api/v1/skus/1/stock", http.StatusNotFound},
	} {
		if got := statusOf(t, admin, tt.method, tt.path); got != tt.want {
			t.Errorf("admin %s %s = %d, want %d", tt.method, tt.path, got, tt.want)
		}
	}
	for _, v := range set.Snapshot() {
		if wantOff := v.Name == httpapi.SwitchStockQuery; (v.Mode == degrade.ModeOff) != wantOff {
			t.Errorf("switch %s mode = %s after PUT stock_query off", v.Name, v.Mode)
		}
	}
	// 三个开关都登记在 gateway 的集合上。
	if n := len(set.Snapshot()); n != 3 {
		t.Errorf("gateway registers %d switches, want 3", n)
	}
}
