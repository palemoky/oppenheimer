package grpcx

import (
	"os"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/stats"
)

// 全局的 provider 是进程级的，在 TestMain 里设一次。用例之间按 trace id 区分自己的 span，
// 其他用例（有的并行）产生的 span 也在 recorder 里。
var spans = tracetest.NewSpanRecorder()

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(spans),
	))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	os.Exit(m.Run())
}

// spansOf 返回 recorder 里属于 traceID 的已结束的 span。
func spansOf(traceID trace.TraceID) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range spans.Ended() {
		if s.SpanContext().TraceID() == traceID {
			out = append(out, s)
		}
	}
	return out
}

// waitSpans 轮询到 traceID 下有 n 个已结束的 span：服务端的 span 在客户端的调用返回之后才结束。
func waitSpans(t *testing.T, traceID trace.TraceID, n int) []sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := spansOf(traceID); len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d spans in trace %s, have %d", n, traceID, len(spansOf(traceID)))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServerSpanIsChildOfClientSpan(t *testing.T) {
	t.Parallel()
	conn, _, _ := startServer(t)

	ctx, parent := otel.Tracer("test").Start(t.Context(), "caller")
	if err := check(ctx, conn, "ok"); err != nil {
		t.Fatal(err)
	}
	parent.End()

	got := waitSpans(t, parent.SpanContext().TraceID(), 3)
	find := func(kind trace.SpanKind) sdktrace.ReadOnlySpan {
		t.Helper()
		i := slices.IndexFunc(got, func(s sdktrace.ReadOnlySpan) bool { return s.SpanKind() == kind })
		if i < 0 {
			t.Fatalf("no %v span among %d spans", kind, len(got))
		}
		return got[i]
	}
	client, server := find(trace.SpanKindClient), find(trace.SpanKindServer)

	if client.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Errorf("client span parent = %v, want the caller span %v", client.Parent().SpanID(), parent.SpanContext().SpanID())
	}
	if server.Parent().SpanID() != client.SpanContext().SpanID() {
		t.Errorf("server span parent = %v, want the client span %v", server.Parent().SpanID(), client.SpanContext().SpanID())
	}
	if !server.Parent().IsRemote() {
		t.Error("server span parent is not remote, want it taken from the request metadata")
	}
	for _, s := range []sdktrace.ReadOnlySpan{client, server} {
		if s.Name() != "test.Probe/Check" {
			t.Errorf("%v span name = %q, want test.Probe/Check", s.SpanKind(), s.Name())
		}
	}
}

func TestHealthChecksProduceNoSpans(t *testing.T) {
	t.Parallel()
	conn, _, _ := startServer(t)

	ctx, parent := otel.Tracer("test").Start(t.Context(), "caller")
	client := healthpb.NewHealthClient(conn)
	if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	// 再调一次会产生 span 的方法：它的 span 出现时，前面的健康检查（同步的）早已结束，它们要是有 span 也该在了。
	if err := check(ctx, conn, "ok"); err != nil {
		t.Fatal(err)
	}
	parent.End()

	got := waitSpans(t, parent.SpanContext().TraceID(), 3) // caller + 探针调用的客户端和服务端
	for _, s := range got {
		if s.Name() != "caller" && s.Name() != "test.Probe/Check" {
			t.Errorf("unexpected span %q (%v): health checks must not be traced", s.Name(), s.SpanKind())
		}
	}
	// 所有 trace 里都不该有健康检查的 span（别的用例也调过内置的健康检查）。
	for _, s := range spans.Ended() {
		if n := s.Name(); n == "grpc.health.v1.Health/Check" || n == "grpc.health.v1.Health/Watch" {
			t.Errorf("found a span named %q", n)
		}
	}
}

func TestTraceFilter(t *testing.T) {
	t.Parallel()
	for method, want := range map[string]bool{
		healthpb.Health_Check_FullMethodName: false,
		healthpb.Health_Watch_FullMethodName: false,
		healthpb.Health_List_FullMethodName:  true,
		"/order.v1.OrderService/CreateOrder": true,
	} {
		if got := traceFilter(&stats.RPCTagInfo{FullMethodName: method}); got != want {
			t.Errorf("traceFilter(%s) = %v, want %v", method, got, want)
		}
	}
}
