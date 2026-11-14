package redisx_test

import (
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/palemoky/oppenheimer/pkg/redisx"
	"github.com/palemoky/oppenheimer/pkg/testredis"
)

// dropRootClient 是 otelx 里同名采样器的等价物（那个是未导出的，这里测不了它）：
// 没有父 span 的 CLIENT span 丢掉，其余全采样。redisx 的“没有父 span 不出 span”靠它实现。
type dropRootClient struct{}

func (dropRootClient) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if p.Kind == trace.SpanKindClient && !trace.SpanContextFromContext(p.ParentContext).IsValid() {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	}
	return sdktrace.AlwaysSample().ShouldSample(p)
}
func (dropRootClient) Description() string { return "test DropRootClient" }

// 全局的 TracerProvider 是进程级的，在 TestMain 里设一次：redisotel 在 InstrumentTracing 时取全局的 provider。
var spans = tracetest.NewSpanRecorder()

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithSampler(dropRootClient{}),
		sdktrace.WithSpanProcessor(spans),
	))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	os.Exit(m.Run())
}

func spansOf(traceID trace.TraceID) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range spans.Ended() {
		if s.SpanContext().TraceID() == traceID {
			out = append(out, s)
		}
	}
	return out
}

func TestCommandSpanIsChildAndHasNoCommandText(t *testing.T) {
	client, prefix := testredis.New(t)

	ctx, parent := otel.Tracer("test").Start(t.Context(), "request")
	const secret = "s3cret-value-123"
	key := prefix + "user:42:idem"
	if err := client.Set(ctx, key, secret, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	// 管道和脚本（key 和参数更长）也一样。
	pipe := client.Pipeline()
	pipe.Incr(ctx, key+":n")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if err := redis.NewScript("return redis.call('GET', KEYS[1])").Run(ctx, client, []string{key}).Err(); err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	parent.End()

	got := spansOf(parent.SpanContext().TraceID())
	var clients []sdktrace.ReadOnlySpan
	for _, s := range got {
		if s.SpanKind() == trace.SpanKindClient {
			clients = append(clients, s)
		}
	}
	if len(clients) < 3 {
		t.Fatalf("%d CLIENT spans under the request, want at least 3 (set, get, pipeline/script)", len(clients))
	}
	var names []string
	parentOf := make(map[trace.SpanID]trace.SpanID)
	for _, s := range got {
		parentOf[s.SpanContext().SpanID()] = s.Parent().SpanID()
	}
	// 管道和新建连接（hello）会多出嵌套的 span，所以不要求都直接挂在请求下面，只要求都在请求的子树里。
	descendsFromRequest := func(id trace.SpanID) bool {
		for range len(got) + 1 {
			id = parentOf[id]
			if id == parent.SpanContext().SpanID() {
				return true
			}
		}
		return false
	}
	for _, s := range clients {
		names = append(names, s.Name())
		if !descendsFromRequest(s.SpanContext().SpanID()) {
			t.Errorf("span %q is not under the request span", s.Name())
		}
	}
	for _, name := range []string{"set", "get"} {
		for _, s := range clients {
			if s.Name() == name && s.Parent().SpanID() != parent.SpanContext().SpanID() {
				t.Errorf("span %q parent = %s, want the request span", name, s.Parent().SpanID())
			}
		}
	}
	for _, want := range []string{"set", "get"} {
		found := false
		for _, n := range names {
			found = found || n == want
		}
		if !found {
			t.Errorf("no CLIENT span named %q; have %v", want, names)
		}
	}

	// key 里有用户号和幂等键，值可能是任何东西：命令的内容不能出现在 span 的任何地方。
	for _, s := range got {
		if strings.Contains(s.Name(), prefix) || strings.Contains(s.Name(), secret) {
			t.Errorf("span name %q contains command text", s.Name())
		}
		for _, kv := range s.Attributes() {
			if k := string(kv.Key); k == "db.statement" || k == "db.query.text" {
				t.Errorf("span %q has attribute %s = %q, want the command text omitted", s.Name(), k, kv.Value.String())
			}
			if v := kv.Value.String(); strings.Contains(v, secret) || strings.Contains(v, key) {
				t.Errorf("span %q attribute %s = %q leaks the key or value", s.Name(), kv.Key, v)
			}
		}
		for _, ev := range s.Events() {
			for _, kv := range ev.Attributes {
				if v := kv.Value.String(); strings.Contains(v, secret) || strings.Contains(v, key) {
					t.Errorf("span %q event %s leaks the key or value: %q", s.Name(), ev.Name, v)
				}
			}
		}
	}
	// 建连接的 span 被过滤了（WithDialFilter）。
	for _, s := range got {
		if strings.Contains(s.Name(), "dial") || strings.Contains(s.Name(), "connect") {
			t.Errorf("unexpected dial span %q", s.Name())
		}
	}
}

// 没有父 span 的命令（后台循环发的）不出 span：靠采样器丢弃，所以这个用例带着 otelx 采样器的等价物。
// 这个用例不并行，数 span 的总数。
func TestCommandWithoutParentProducesNoSpan(t *testing.T) {
	client, prefix := testredis.New(t)
	before := len(spans.Ended())
	for range 5 {
		if err := client.Set(t.Context(), prefix+"bg", "1", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if after := len(spans.Ended()); after != before {
		t.Errorf("%d spans were recorded for commands without a parent span, want 0", after-before)
	}
}

func TestOpenLazyClientIsInstrumentedToo(t *testing.T) {
	// OpenLazy 和 Open 走同一个 newClient，埋点一样。testredis 用的是 Open，这里直接确认 OpenLazy 的客户端。
	_, _ = testredis.New(t) // 没有 Redis 时跳过
	addr := os.Getenv(testredis.AddrEnv)
	client, err := openLazy(t, addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, parent := otel.Tracer("test").Start(t.Context(), "request")
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	parent.End()
	found := false
	for _, s := range spansOf(parent.SpanContext().TraceID()) {
		found = found || (s.SpanKind() == trace.SpanKindClient && s.Parent().SpanID() == parent.SpanContext().SpanID())
	}
	if !found {
		t.Error("no CLIENT span under the request for a client from OpenLazy")
	}
}

func openLazy(t *testing.T, addr string) (*redis.Client, error) {
	t.Helper()
	client, err := redisx.OpenLazy(t.Context(), redisx.Config{Addr: addr}, slog.New(slog.DiscardHandler))
	if err == nil {
		t.Cleanup(func() { _ = client.Close() })
	}
	return client, err
}
