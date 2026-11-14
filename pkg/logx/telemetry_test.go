package logx_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"log/slog"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/palemoky/oppenheimer/pkg/logx"
)

// 本文件测日志里的 trace_id、span_id。用自己建的 TracerProvider，不碰全局的。

func newSpan(t *testing.T, sampler sdktrace.Sampler) (context.Context, trace.Span) {
	t.Helper()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(t.Context(), "op")
	t.Cleanup(func() { span.End() })
	return ctx, span
}

// lastLine 把 buf 里的最后一行日志解析成 map。
func lastLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &m); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, buf.String())
	}
	return m
}

func TestLogsCarryTraceAndSpanID(t *testing.T) {
	ctx, span := newSpan(t, sdktrace.AlwaysSample())
	sc := span.SpanContext()

	tests := []struct {
		name string
		log  func(l *slog.Logger)
		// 嵌套在组里时 trace_id 在哪一层：WithGroup 之后 handler 自己加的属性也在组里。
		path []string
	}{
		{"plain", func(l *slog.Logger) { l.InfoContext(ctx, "m") }, nil},
		{"with", func(l *slog.Logger) { l.With("k", "v").InfoContext(ctx, "m") }, nil},
		{"with twice", func(l *slog.Logger) { l.With("a", 1).With("b", 2).InfoContext(ctx, "m") }, nil},
		{"with group", func(l *slog.Logger) { l.WithGroup("g").InfoContext(ctx, "m", "x", 1) }, []string{"g"}},
		{"with then group", func(l *slog.Logger) { l.With("k", "v").WithGroup("g").With("z", 1).InfoContext(ctx, "m") }, []string{"g"}},
		{"log method", func(l *slog.Logger) { l.Log(ctx, slog.LevelWarn, "m") }, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			tt.log(logx.New("svc", &buf, slog.LevelInfo))
			entry := lastLine(t, &buf)
			scope := entry
			for _, g := range tt.path {
				var ok bool
				if scope, ok = scope[g].(map[string]any); !ok {
					t.Fatalf("no group %q in %v", g, entry)
				}
			}
			if got := scope["trace_id"]; got != sc.TraceID().String() {
				t.Errorf("trace_id = %v, want %s (entry %v)", got, sc.TraceID(), entry)
			}
			if got := scope["span_id"]; got != sc.SpanID().String() {
				t.Errorf("span_id = %v, want %s (entry %v)", got, sc.SpanID(), entry)
			}
			if entry["service"] != "svc" {
				t.Errorf("service = %v, want svc", entry["service"])
			}
		})
	}
}

func TestLogsWithoutSpanHaveNoTraceFields(t *testing.T) {
	var buf bytes.Buffer
	logger := logx.New("svc", &buf, slog.LevelInfo)

	logger.InfoContext(t.Context(), "no span")
	logger.Info("no ctx")
	logger.With("k", "v").InfoContext(t.Context(), "with")
	logger.WithGroup("g").InfoContext(t.Context(), "group", "x", 1)

	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if strings.Contains(line, "trace_id") || strings.Contains(line, "span_id") {
			t.Errorf("line without a span has trace fields: %s", line)
		}
	}
}

// 不带 ctx 的方法（Info）拿不到 span，即使调用方手里有。这是文档里写明的限制，这里固定下来。
func TestLogWithoutContextMethodHasNoTraceFields(t *testing.T) {
	_, _ = newSpan(t, sdktrace.AlwaysSample())
	var buf bytes.Buffer
	logx.New("svc", &buf, slog.LevelInfo).Info("no ctx")
	if strings.Contains(buf.String(), "trace_id") {
		t.Errorf("Info without ctx has trace_id: %s", buf.String())
	}
}

// 没被采样的 span 也有 trace id，日志照样带：日志不采样。
func TestLogsCarryTraceIDOfUnsampledSpan(t *testing.T) {
	ctx, span := newSpan(t, sdktrace.NeverSample())
	if span.SpanContext().IsSampled() {
		t.Fatal("span is sampled, want unsampled")
	}
	var buf bytes.Buffer
	logx.New("svc", &buf, slog.LevelInfo).InfoContext(ctx, "m")
	if got := lastLine(t, &buf)["trace_id"]; got != span.SpanContext().TraceID().String() {
		t.Errorf("trace_id = %v, want %s", got, span.SpanContext().TraceID())
	}
}

// 级别过滤仍然生效，也不会因为多包了一层而重复输出。
func TestTraceHandlerKeepsLevelFilter(t *testing.T) {
	ctx, _ := newSpan(t, sdktrace.AlwaysSample())
	var buf bytes.Buffer
	logger := logx.New("svc", &buf, slog.LevelWarn)
	logger.InfoContext(ctx, "dropped")
	logger.WarnContext(ctx, "kept")
	if n := strings.Count(strings.TrimSpace(buf.String()), "\n") + 1; n != 1 || !strings.Contains(buf.String(), "kept") {
		t.Errorf("output = %q, want exactly the warn line", buf.String())
	}
}
