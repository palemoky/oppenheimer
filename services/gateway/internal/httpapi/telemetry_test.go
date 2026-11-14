package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/ratelimit"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi"
)

// 全局的 TracerProvider 和 MeterProvider 是进程级的状态，在这里设一次。所有用例的 span 都进同一个 recorder，
// 用例之间靠各自响应里的 X-Trace-Id 区分；指标是累计值，用例比较前后的差。
var (
	spans   = tracetest.NewSpanRecorder()
	metrics = sdkmetric.NewManualReader()
)

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metrics)))
	os.Exit(m.Run())
}

// serverSpan 返回 trace id 为 traceID 的服务端 span（traceEntry 建的那个）。找不到时失败。
func serverSpan(t *testing.T, traceID string) sdktrace.ReadOnlySpan {
	t.Helper()
	if len(traceID) != 32 {
		t.Fatalf("X-Trace-Id = %q, want 32 hex characters", traceID)
	}
	for _, s := range spans.Ended() {
		if s.SpanContext().TraceID().String() == traceID && s.SpanKind() == trace.SpanKindServer {
			return s
		}
	}
	t.Fatalf("no server span with trace id %s", traceID)
	return nil
}

func spanAttr(s sdktrace.ReadOnlySpan, key attribute.Key) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// collect 读一次当前所有指标。
func collect(t *testing.T) []metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := metrics.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	var out []metricdata.Metrics
	for _, sm := range rm.ScopeMetrics {
		out = append(out, sm.Metrics...)
	}
	return out
}

// counter 返回名为 name 的计数器在标签含有 want 全部的那些序列上的合计。
func counter(t *testing.T, name string, want ...attribute.KeyValue) int64 {
	t.Helper()
	var total int64
	for _, m := range collect(t) {
		if m.Name != name {
			continue
		}
		sum, ok := m.Data.(metricdata.Sum[int64])
		if !ok {
			t.Fatalf("%s is %T, want an int64 sum", name, m.Data)
		}
		for _, dp := range sum.DataPoints {
			if hasAll(dp.Attributes, want) {
				total += dp.Value
			}
		}
	}
	return total
}

func hasAll(set attribute.Set, want []attribute.KeyValue) bool {
	for _, kv := range want {
		if v, ok := set.Value(kv.Key); !ok || v != kv.Value {
			return false
		}
	}
	return true
}

// durationSeries 返回 http.server.request.duration 的所有标签组合。
func durationSeries(t *testing.T) []attribute.Set {
	t.Helper()
	var out []attribute.Set
	for _, m := range collect(t) {
		if m.Name != "http.server.request.duration" {
			continue
		}
		h, ok := m.Data.(metricdata.Histogram[float64])
		if !ok {
			t.Fatalf("http.server.request.duration is %T, want a float64 histogram", m.Data)
		}
		for _, dp := range h.DataPoints {
			out = append(out, dp.Attributes)
		}
	}
	return out
}

// 外面带进来的 traceparent 不当父 span，只记成 link；响应的 X-Trace-Id 是服务端 span 自己的 trace id；
// span 名是路由模板，订单号和用户号在 span 上。
func TestTraceIgnoresInboundParent(t *testing.T) {
	e, _ := newRateLimitedEnv(t, nil)
	const (
		remoteTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
		remoteSpan  = "00f067aa0ba902b7"
	)
	r := send(t, e.h, http.MethodGet, "/api/v1/orders/123", "7", "",
		"traceparent", "00-"+remoteTrace+"-"+remoteSpan+"-01",
		"baggage", "tenant=evil")
	wantStatus(t, r, http.StatusOK)

	traceID := r.header.Get(httpapi.TraceIDHeader)
	if traceID == remoteTrace {
		t.Fatal("the server span joined the inbound trace; want a new root")
	}
	s := serverSpan(t, traceID)
	if s.Parent().IsValid() {
		t.Errorf("server span has parent %v, want none", s.Parent())
	}
	if got, want := s.Name(), "GET /api/v1/orders/:order_id"; got != want {
		t.Errorf("span name = %q, want %q", got, want)
	}
	links := s.Links()
	if len(links) != 1 {
		t.Fatalf("links = %d, want 1", len(links))
	}
	if got := links[0].SpanContext; got.TraceID().String() != remoteTrace || got.SpanID().String() != remoteSpan {
		t.Errorf("link = %s/%s, want %s/%s", got.TraceID(), got.SpanID(), remoteTrace, remoteSpan)
	}
	if v, ok := spanAttr(s, "app.order_id"); !ok || v.AsInt64() != 123 {
		t.Errorf("app.order_id = %v (set %v), want 123", v.AsInt64(), ok)
	}
	if v, ok := spanAttr(s, "app.user_id"); !ok || v.AsInt64() != 7 {
		t.Errorf("app.user_id = %v (set %v), want 7", v.AsInt64(), ok)
	}
	if s.Status().Code == codes.Error {
		t.Errorf("span status = %v, want not Error", s.Status())
	}
}

// 没带 traceparent 的请求也有 X-Trace-Id，没有 link；带了坏的 traceparent 也不出 link。
func TestTraceWithoutInboundParent(t *testing.T) {
	e, _ := newRateLimitedEnv(t, nil)
	for _, hdr := range [][]string{nil, {"traceparent", "garbage"}} {
		r := send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "", hdr...)
		wantStatus(t, r, http.StatusOK)
		if s := serverSpan(t, r.header.Get(httpapi.TraceIDHeader)); len(s.Links()) != 0 {
			t.Errorf("headers %v: links = %v, want none", hdr, s.Links())
		}
	}
}

// 被限流拒绝的请求也有 span 和 X-Trace-Id（链路的中间件在限流之外），拒绝计进指标，原因记在 span 上，span 不标 Error。
func TestTraceRateLimited(t *testing.T) {
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		IP:      newLocal(newFakeClock()),
		IPLimit: ratelimit.Limit{Rate: slow, Burst: 1},
	})
	const addr = "198.51.100.9:1000"
	ipRejected := []attribute.KeyValue{attribute.String("rule", "ip"), attribute.String("outcome", "rejected")}
	ipAllowed := []attribute.KeyValue{attribute.String("rule", "ip"), attribute.String("outcome", "allowed")}
	rejectedBefore := counter(t, "gateway.ratelimit.decisions", ipRejected...)
	allowedBefore := counter(t, "gateway.ratelimit.decisions", ipAllowed...)

	wantStatus(t, send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", addr), http.StatusOK)
	r := send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", addr)
	wantRateLimited(t, r, "ip", 0)

	s := serverSpan(t, r.header.Get(httpapi.TraceIDHeader))
	if got, want := s.Name(), "GET /api/v1/orders/:order_id"; got != want {
		t.Errorf("span name = %q, want %q", got, want)
	}
	if v, _ := spanAttr(s, "app.reason"); v.AsString() != "RATE_LIMITED" {
		t.Errorf("app.reason = %q, want RATE_LIMITED", v.AsString())
	}
	if v, _ := spanAttr(s, "app.ratelimit.rule"); v.AsString() != "ip" {
		t.Errorf("app.ratelimit.rule = %q, want ip", v.AsString())
	}
	if s.Status().Code == codes.Error {
		t.Errorf("span status = %v, want not Error for a 429", s.Status())
	}
	if got := counter(t, "gateway.ratelimit.decisions", ipRejected...) - rejectedBefore; got != 1 {
		t.Errorf("rejected ip decisions = %d, want 1", got)
	}
	if got := counter(t, "gateway.ratelimit.decisions", ipAllowed...) - allowedBefore; got != 1 {
		t.Errorf("allowed ip decisions = %d, want 1", got)
	}
}

// 按用户和接口放行时两维各记一次，被接口的总量拒绝时只记 route。
func TestRateLimitDecisionsUserAndRoute(t *testing.T) {
	e, _ := newRateLimitedEnv(t, &httpapi.RateLimit{
		Shared: newLocal(newFakeClock()),
		User:   map[string]ratelimit.Limit{"getOrder": {Rate: slow, Burst: 10}},
		Route:  map[string]ratelimit.Limit{"getOrder": {Rate: slow, Burst: 1}},
	})
	key := func(rule, outcome string) []attribute.KeyValue {
		return []attribute.KeyValue{attribute.String("rule", rule), attribute.String("outcome", outcome)}
	}
	before := map[string]int64{}
	for _, rule := range []string{"user", "route"} {
		for _, o := range []string{"allowed", "rejected"} {
			before[rule+o] = counter(t, "gateway.ratelimit.decisions", key(rule, o)...)
		}
	}

	wantStatus(t, getOrder(t, e.h, "7"), http.StatusOK)
	wantRateLimited(t, getOrder(t, e.h, "8"), "route", 0)

	for k, want := range map[[2]string]int64{
		{"user", "allowed"}: 1, {"route", "allowed"}: 1, {"user", "rejected"}: 0, {"route", "rejected"}: 1,
	} {
		if got := counter(t, "gateway.ratelimit.decisions", key(k[0], k[1])...) - before[k[0]+k[1]]; got != want {
			t.Errorf("%s/%s = %d, want %d", k[0], k[1], got, want)
		}
	}
}

// 降级的 503 有 X-Trace-Id，按原因码计数，原因码记在 span 上。
func TestTraceDegraded(t *testing.T) {
	e := newSwitchEnv(t)
	e.setMode(t, httpapi.SwitchStockQuery, degrade.ModeOff)
	reason := attribute.String("reason", errx.ReasonFeatureDisabled)
	before := counter(t, "gateway.degraded.responses", reason)

	r := do(t, e.h, http.MethodGet, "/api/v1/skus/1/stock", "7", "")
	wantStatus(t, r, http.StatusServiceUnavailable)
	s := serverSpan(t, r.header.Get(httpapi.TraceIDHeader))
	if v, _ := spanAttr(s, "app.reason"); v.AsString() != errx.ReasonFeatureDisabled {
		t.Errorf("app.reason = %q, want %s", v.AsString(), errx.ReasonFeatureDisabled)
	}
	// 降级是“自己是好的、依赖坏了或功能被关掉”，不是 gateway 的故障（设计文档 1d 8.4）。
	if s.Status().Code == codes.Error {
		t.Errorf("span status = %v, want not Error for a degraded 503", s.Status())
	}
	if got := counter(t, "gateway.degraded.responses", reason) - before; got != 1 {
		t.Errorf("degraded responses = %d, want 1", got)
	}
}

// 没匹配到路由的请求：有 X-Trace-Id，但 span 名和指标的标签里都不出现原始路径（否则扫描器每打一个路径就是一条新序列）。
func TestTraceUnmatchedRoute(t *testing.T) {
	e, _ := newRateLimitedEnv(t, nil)
	const path = "/wp-admin/install-9f3c2.php"
	r := send(t, e.h, http.MethodGet, path, "7", "")
	wantStatus(t, r, http.StatusNotFound)
	// 方法也是客户端写的：不认识的方法不能变成新的标签值。
	other := send(t, e.h, "PROPFIND", path, "7", "")
	wantStatus(t, other, http.StatusNotFound)
	if got, want := serverSpan(t, other.header.Get(httpapi.TraceIDHeader)).Name(), "_OTHER unmatched"; got != want {
		t.Errorf("span name = %q, want %q", got, want)
	}

	s := serverSpan(t, r.header.Get(httpapi.TraceIDHeader))
	if got, want := s.Name(), "GET unmatched"; got != want {
		t.Errorf("span name = %q, want %q", got, want)
	}
	if v, _ := spanAttr(s, "url.path"); v.AsString() != path {
		t.Errorf("url.path = %q, want %q (the raw path belongs on the span)", v.AsString(), path)
	}
	var unmatched bool
	for _, set := range durationSeries(t) {
		for _, kv := range set.ToSlice() {
			if strings.Contains(kv.Value.String(), "wp-admin") {
				t.Errorf("metric series %v contains the raw path", set.ToSlice())
			}
			if kv.Key == "http.request.method" && kv.Value.AsString() == "PROPFIND" {
				t.Errorf("metric series %v has a non-standard method", set.ToSlice())
			}
		}
		if v, _ := set.Value("http.route"); v.AsString() == "unmatched" {
			unmatched = true
		}
	}
	if !unmatched {
		t.Error("no http.server.request.duration series with http.route=unmatched")
	}
}

// 匹配到的路由在指标里是模板，不是带 ID 的路径。
func TestRequestDurationUsesRouteTemplate(t *testing.T) {
	e, _ := newRateLimitedEnv(t, nil)
	wantStatus(t, getOrder(t, e.h, "7"), http.StatusOK)
	want := []attribute.KeyValue{
		attribute.String("http.request.method", "GET"),
		attribute.String("http.route", "/api/v1/orders/:order_id"),
		attribute.Int("http.response.status_code", http.StatusOK),
	}
	for _, set := range durationSeries(t) {
		if hasAll(set, want) {
			if set.Len() != len(want) {
				t.Errorf("series %v has labels beyond method, route and status", set.ToSlice())
			}
			return
		}
	}
	t.Errorf("no http.server.request.duration series with %v", want)
}

// 外面的 traceparent 带着 sampled 标志，也不能让我们采样：span 是新的根，ParentBased 只看父 span，不看 link。
// 用 ParentBased(NeverSample()) 的 provider 建一个 handler：外面说“已采样”，我们仍然不采。
func TestTraceIgnoresInboundSampledFlag(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	e, _ := newRateLimitedEnv(t, nil) // New 在这时取 tracer，取到的是 tp 的
	otel.SetTracerProvider(prev)

	r := send(t, e.h, http.MethodGet, "/api/v1/orders/1", "7", "",
		"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	wantStatus(t, r, http.StatusOK)
	if got := len(r.header.Get(httpapi.TraceIDHeader)); got != 32 {
		t.Errorf("X-Trace-Id has %d characters, want 32 even when the span is not sampled", got)
	}
	if ended := rec.Ended(); len(ended) != 0 {
		t.Errorf("the inbound sampled flag forced %d spans to be sampled", len(ended))
	}
}

// 下游返回 Internal：gateway 回 500，span 标 Error。
func TestTraceServerFaultIsError(t *testing.T) {
	e, _ := newRateLimitedEnv(t, nil)
	e.order.get = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
		return nil, status.Error(grpccodes.Internal, "boom")
	}
	r := getOrder(t, e.h, "7")
	wantStatus(t, r, http.StatusInternalServerError)
	s := serverSpan(t, r.header.Get(httpapi.TraceIDHeader))
	if s.Status().Code != codes.Error {
		t.Errorf("span status = %v, want Error for a 500", s.Status())
	}
	if v, _ := spanAttr(s, "http.response.status_code"); v.AsInt64() != http.StatusInternalServerError {
		t.Errorf("http.response.status_code = %d, want 500", v.AsInt64())
	}
}

// 客户端断开（499）不是服务端的故障，span 不标 Error。
func TestTraceClientClosedIsNotError(t *testing.T) {
	e, _ := newRateLimitedEnv(t, nil)
	e.inventory.getStock = func(ctx context.Context, _ *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // 客户端在请求处理途中走了：请求的 ctx 已经取消
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/skus/1/stock", nil)
	req.Header.Set("X-User-Id", "7")
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, req)
	if w.Code != 499 {
		t.Fatalf("status = %d, want 499; body %s", w.Code, w.Body)
	}
	s := serverSpan(t, w.Header().Get(httpapi.TraceIDHeader))
	if s.Status().Code == codes.Error {
		t.Errorf("span status = %v, want not Error for 499", s.Status())
	}
}
