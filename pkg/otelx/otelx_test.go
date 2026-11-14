package otelx

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// 本文件的用例会改全局状态（环境变量、全局 provider），所以都不并行；改动在用例结束时恢复。

func quietLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// restoreGlobals 记下现在的全局 provider 和传播器，用例结束时放回去。
func restoreGlobals(t *testing.T) {
	t.Helper()
	tp, mp, prop := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(tp)
		otel.SetMeterProvider(mp)
		otel.SetTextMapPropagator(prop)
	})
}

// clearOTelEnv 把会影响 Setup 的变量清空；t.Setenv 在用例结束时恢复原值。
func clearOTelEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{EndpointEnv, SamplerEnv, SamplerArgEnv, "OTEL_RESOURCE_ATTRIBUTES"} {
		t.Setenv(k, "")
	}
}

func TestSetupWithoutEndpointStillProducesSpanIDs(t *testing.T) {
	restoreGlobals(t)
	clearOTelEnv(t)

	shutdown, err := Setup(t.Context(), "test-svc", quietLogger())
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	_, span := otel.Tracer("test").Start(t.Context(), "op")
	if !span.SpanContext().IsValid() {
		t.Errorf("span context %v is not valid, want a real trace id even without an exporter", span.SpanContext())
	}
	span.End()

	start := time.Now()
	if err := shutdown(t.Context()); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("shutdown took %v, want it to return immediately without an exporter", d)
	}
}

func TestSetupWithUnreachableBackendDoesNotBlock(t *testing.T) {
	restoreGlobals(t)
	clearOTelEnv(t)
	t.Setenv(EndpointEnv, "http://127.0.0.1:1")

	start := time.Now()
	shutdown, err := Setup(t.Context(), "test-svc", quietLogger())
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("Setup took %v, want it not to wait for the backend", d)
	}

	start = time.Now()
	for range 100 {
		_, span := otel.Tracer("test").Start(t.Context(), "op")
		if !span.SpanContext().IsValid() {
			t.Fatal("span context is not valid")
		}
		span.End()
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("starting spans took %v, want it not to block on the exporter", d)
	}

	start = time.Now()
	// 后端不在时 shutdown 可以返回错误（导出失败），这里只要求它按时返回。
	_ = shutdown(t.Context())
	if d := time.Since(start); d > ShutdownTimeout+time.Second {
		t.Errorf("shutdown took %v, want it within %v plus a margin", d, ShutdownTimeout)
	}
}

func TestSetupRejectsBadSampler(t *testing.T) {
	restoreGlobals(t)
	clearOTelEnv(t)
	t.Setenv(SamplerEnv, "nope")
	if _, err := Setup(t.Context(), "test-svc", quietLogger()); err == nil {
		t.Error("Setup with an unknown sampler returned nil error")
	}
}

func sampleParams(kind trace.SpanKind, parent trace.SpanContext) sdktrace.SamplingParameters {
	ctx := context.Background()
	if parent.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	}
	return sdktrace.SamplingParameters{
		ParentContext: ctx,
		TraceID:       trace.TraceID{1, 2, 3},
		Name:          "op",
		Kind:          kind,
	}
}

func remoteParent(sampled bool) trace.SpanContext {
	var flags trace.TraceFlags
	if sampled {
		flags = trace.FlagsSampled
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{9, 9, 9},
		SpanID:     trace.SpanID{8, 8},
		TraceFlags: flags,
		Remote:     true,
	})
}

func TestSamplerFromEnv(t *testing.T) {
	type check struct {
		kind   trace.SpanKind
		parent trace.SpanContext // 无效表示没有父 span
		want   sdktrace.SamplingDecision
	}
	const (
		sample = sdktrace.RecordAndSample
		drop   = sdktrace.Drop
	)
	noParent := trace.SpanContext{}
	tests := []struct {
		name, arg string
		checks    []check
	}{
		{"", "", []check{
			{trace.SpanKindServer, noParent, sample},
			{trace.SpanKindServer, remoteParent(true), sample},
			{trace.SpanKindServer, remoteParent(false), drop},
		}},
		{"parentbased_always_on", "", []check{
			{trace.SpanKindServer, noParent, sample},
			{trace.SpanKindServer, remoteParent(false), drop},
		}},
		{"parentbased_always_off", "", []check{
			{trace.SpanKindServer, noParent, drop},
			{trace.SpanKindServer, remoteParent(true), sample},
		}},
		{"always_on", "", []check{
			{trace.SpanKindServer, noParent, sample},
			{trace.SpanKindServer, remoteParent(false), sample},
		}},
		{"always_off", "", []check{
			{trace.SpanKindServer, noParent, drop},
			{trace.SpanKindServer, remoteParent(true), drop},
		}},
		// 比例缺省是 1。
		{"traceidratio", "", []check{{trace.SpanKindServer, noParent, sample}}},
		{"parentbased_traceidratio", "", []check{
			{trace.SpanKindServer, noParent, sample},
			{trace.SpanKindServer, remoteParent(false), drop},
		}},
		{"traceidratio", "0", []check{
			{trace.SpanKindServer, noParent, drop},
			{trace.SpanKindServer, remoteParent(true), drop},
		}},
		{"traceidratio", "1", []check{{trace.SpanKindServer, noParent, sample}}},
		{"parentbased_traceidratio", "0", []check{
			{trace.SpanKindServer, noParent, drop},
			// 入口按比例决定，后面的跟着父 span。
			{trace.SpanKindServer, remoteParent(true), sample},
		}},
		{" always_on ", "", []check{{trace.SpanKindServer, noParent, sample}}},
	}
	for _, tt := range tests {
		t.Run(tt.name+"/"+tt.arg, func(t *testing.T) {
			clearOTelEnv(t)
			t.Setenv(SamplerEnv, tt.name)
			t.Setenv(SamplerArgEnv, tt.arg)
			s, err := samplerFromEnv()
			if err != nil {
				t.Fatalf("samplerFromEnv: %v", err)
			}
			if _, ok := s.(dropRootClient); !ok {
				t.Errorf("sampler is %T, want it wrapped in dropRootClient", s)
			}
			for _, c := range tt.checks {
				got := s.ShouldSample(sampleParams(c.kind, c.parent)).Decision
				if got != c.want {
					t.Errorf("kind %v parent valid=%v sampled=%v: decision %v, want %v",
						c.kind, c.parent.IsValid(), c.parent.IsSampled(), got, c.want)
				}
			}
		})
	}
}

func TestSamplerFromEnvRejectsBadConfig(t *testing.T) {
	tests := []struct{ name, arg string }{
		{"nope", ""},
		{"ALWAYS_ON", ""}, // 名字区分大小写，和现有的其他配置一致：写错就拒绝
		{"traceidratio", "abc"},
		{"traceidratio", "1.5"},
		{"traceidratio", "-0.1"},
		{"parentbased_traceidratio", "abc"},
		{"parentbased_traceidratio", "2"},
	}
	for _, tt := range tests {
		t.Run(tt.name+"/"+tt.arg, func(t *testing.T) {
			clearOTelEnv(t)
			t.Setenv(SamplerEnv, tt.name)
			t.Setenv(SamplerArgEnv, tt.arg)
			if s, err := samplerFromEnv(); err == nil {
				t.Errorf("samplerFromEnv() = %v, want an error", s.Description())
			}
		})
	}
}

// 非法的比例参数只在用到比例的采样器上才有关系：always_on 不看它。
func TestSamplerFromEnvIgnoresArgForNonRatio(t *testing.T) {
	clearOTelEnv(t)
	t.Setenv(SamplerEnv, "always_on")
	t.Setenv(SamplerArgEnv, "garbage")
	if _, err := samplerFromEnv(); err != nil {
		t.Errorf("samplerFromEnv: %v, want the arg to be ignored", err)
	}
}

func TestDropRootClient(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(dropRootClient{next: sdktrace.AlwaysSample()}),
		sdktrace.WithSpanProcessor(sr),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tracer := tp.Tracer("test")

	// 没有父 span 的 CLIENT：丢弃。
	_, span := tracer.Start(t.Context(), "root-client", trace.WithSpanKind(trace.SpanKindClient))
	if span.IsRecording() || span.SpanContext().IsSampled() {
		t.Errorf("root CLIENT span: recording=%v sampled=%v, want both false", span.IsRecording(), span.SpanContext().IsSampled())
	}
	span.End()

	// 有父 span 的 CLIENT：照常采样。
	pctx, parent := tracer.Start(t.Context(), "parent", trace.WithSpanKind(trace.SpanKindServer))
	_, child := tracer.Start(pctx, "child-client", trace.WithSpanKind(trace.SpanKindClient))
	if !child.IsRecording() || !child.SpanContext().IsSampled() {
		t.Errorf("CLIENT span with a parent: recording=%v sampled=%v, want both true", child.IsRecording(), child.SpanContext().IsSampled())
	}
	child.End()
	parent.End()

	// 没有父 span 的其他类型：照常采样。
	for _, kind := range []trace.SpanKind{trace.SpanKindServer, trace.SpanKindConsumer, trace.SpanKindInternal, trace.SpanKindProducer} {
		_, s := tracer.Start(t.Context(), "root-"+kind.String(), trace.WithSpanKind(kind))
		if !s.IsRecording() || !s.SpanContext().IsSampled() {
			t.Errorf("root %v span: recording=%v sampled=%v, want both true", kind, s.IsRecording(), s.SpanContext().IsSampled())
		}
		s.End()
	}

	// 父 span 是远端的（刚从 traceparent 解析出来）也算有父。
	rctx := trace.ContextWithRemoteSpanContext(t.Context(), remoteParent(true))
	_, rc := tracer.Start(rctx, "client-with-remote-parent", trace.WithSpanKind(trace.SpanKindClient))
	if !rc.IsRecording() {
		t.Error("CLIENT span under a remote parent was dropped")
	}
	rc.End()

	for _, s := range sr.Ended() {
		if s.Name() == "root-client" {
			t.Errorf("the dropped root CLIENT span was exported")
		}
	}
	if got := len(sr.Ended()); got != 7 {
		t.Errorf("recorded %d spans, want 7 (everything except the root CLIENT)", got)
	}
}

func TestDropRootClientDelegatesDecision(t *testing.T) {
	// 有父 span 的 CLIENT 交给 next 决定：next 是“全不采样”时也丢。
	s := dropRootClient{next: sdktrace.NeverSample()}
	if got := s.ShouldSample(sampleParams(trace.SpanKindClient, remoteParent(true))).Decision; got != sdktrace.Drop {
		t.Errorf("decision = %v, want Drop from next", got)
	}
	if d := s.Description(); d != "DropRootClient{AlwaysOffSampler}" {
		t.Errorf("Description() = %q", d)
	}
}

func TestTraceparent(t *testing.T) {
	if got := Traceparent(t.Context()); got != "" {
		t.Errorf("Traceparent without a span = %q, want empty", got)
	}

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(t.Context(), "op")
	defer span.End()

	// Traceparent 内部用 propagation.TraceContext{}，不依赖全局传播器，所以这里不用设置它。
	tpHeader := Traceparent(ctx)
	if len(tpHeader) != 55 {
		t.Fatalf("Traceparent = %q (%d chars), want 55", tpHeader, len(tpHeader))
	}
	back := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": tpHeader})
	sc := trace.SpanContextFromContext(back)
	if sc.TraceID() != span.SpanContext().TraceID() || sc.SpanID() != span.SpanContext().SpanID() {
		t.Errorf("parsed back %v/%v, want %v/%v", sc.TraceID(), sc.SpanID(), span.SpanContext().TraceID(), span.SpanContext().SpanID())
	}
	if !sc.IsSampled() || !sc.IsRemote() {
		t.Errorf("parsed back sampled=%v remote=%v, want true, true", sc.IsSampled(), sc.IsRemote())
	}
}

func TestTraceparentUnsampledKeepsTheDecision(t *testing.T) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(t.Context(), "op")
	defer span.End()

	// 没被采样的 span 也有有效的上下文，要原样传下去：采样的决定就在标志位里（outbox 依赖这一点）。
	tpHeader := Traceparent(ctx)
	if len(tpHeader) != 55 || tpHeader[len(tpHeader)-2:] != "00" {
		t.Errorf("Traceparent = %q, want a 55 char value ending in flags 00", tpHeader)
	}
}

func TestHasParent(t *testing.T) {
	if HasParent(t.Context()) {
		t.Error("HasParent without a span = true")
	}
	ctx := trace.ContextWithRemoteSpanContext(t.Context(), remoteParent(true))
	if !HasParent(ctx) {
		t.Error("HasParent with a span = false")
	}
}

func spanAttrs(s sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	m := make(map[attribute.Key]attribute.Value)
	for _, kv := range s.Attributes() {
		m[kv.Key] = kv.Value
	}
	return m
}

func TestSetOrderAndSetReason(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tracer := tp.Tracer("test")

	ctx, span := tracer.Start(t.Context(), "with-user")
	SetOrder(ctx, 1001, 42)
	SetReason(ctx, "out_of_stock")
	span.End()

	ctx, span = tracer.Start(t.Context(), "no-user")
	SetOrder(ctx, 1002, 0)
	span.End()

	// 没有 span 时什么也不做，不 panic。
	SetOrder(t.Context(), 1, 2)
	SetReason(t.Context(), "x")

	byName := make(map[string]sdktrace.ReadOnlySpan)
	for _, s := range sr.Ended() {
		byName[s.Name()] = s
	}
	a := spanAttrs(byName["with-user"])
	if got := a["app.order_id"].AsInt64(); got != 1001 {
		t.Errorf("app.order_id = %v, want 1001", got)
	}
	if got := a["app.user_id"].AsInt64(); got != 42 {
		t.Errorf("app.user_id = %v, want 42", got)
	}
	if got := a["app.reason"].AsString(); got != "out_of_stock" {
		t.Errorf("app.reason = %q, want out_of_stock", got)
	}
	if byName["with-user"].Status().Code != 0 {
		t.Errorf("SetReason changed the span status to %v, want Unset", byName["with-user"].Status().Code)
	}
	b := spanAttrs(byName["no-user"])
	if _, ok := b["app.user_id"]; ok {
		t.Error("app.user_id recorded for user 0, want it omitted")
	}
	if got := b["app.order_id"].AsInt64(); got != 1002 {
		t.Errorf("app.order_id = %v, want 1002", got)
	}
}

func TestInstrumentHelpers(t *testing.T) {
	// 没有装配 MeterProvider 时（API 默认的空实现）这些建仪表的函数也要能用。
	m := Meter("github.com/palemoky/oppenheimer/pkg/otelx/test")
	Counter(m, "otelx_test.counter", "d").Add(t.Context(), 1)
	Histogram(m, "otelx_test.hist", "d").Record(t.Context(), 0.1)
	SizeHistogram(m, "otelx_test.size", "d", 1, 10).Record(t.Context(), 3)
	Gauge(m, "otelx_test.gauge", "d").Record(t.Context(), 1)
	unregister, err := ObserveGauge(m, "otelx_test.observed", "d", func() int64 { return 7 })
	if err != nil {
		t.Fatalf("ObserveGauge: %v", err)
	}
	unregister()
}
