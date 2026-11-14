// Package logx 提供各服务统一的结构化日志配置。
package logx

import (
	"context"
	"io"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// LevelEnv 是控制日志级别的环境变量名，取值为 debug、info、warn、error。
const LevelEnv = "LOG_LEVEL"

// New 返回输出 JSON 的 logger，每条日志都带 service 字段。
//
// 调用时传进来的 ctx 里有 span 的话（InfoContext 这一类带 ctx 的方法），日志还会带上 trace_id 和 span_id，
// 可以从一条日志找到它所在的链路，也可以按 trace id 把一次请求在各个服务里的日志捞出来。
// 不带 ctx 的方法（Info、Warn）拿不到 span，所以手边有 ctx 时一律用带 ctx 的方法。
func New(service string, w io.Writer, level slog.Leveler) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(traceHandler{handler}).With("service", service)
}

// traceHandler 给日志加上 ctx 里 span 的 trace_id 和 span_id。
//
// 只读 OTel 的 API，不依赖 SDK：没有装配链路时 ctx 里没有有效的 span，什么也不加。
// 没被采样的 span 也有 trace id，照样加：日志不采样，靠它仍然能把同一次请求的日志串起来。
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs 和 WithGroup 要重新包一层，否则 logger.With(...) 之后就退回了里面的 handler，trace id 丢了。
func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{h.Handler.WithGroup(name)}
}

// LevelFromEnv 读取 LOG_LEVEL，未设置或无法解析时返回 info。
func LevelFromEnv() slog.Level {
	return ParseLevel(os.Getenv(LevelEnv))
}

// ParseLevel 把文本解析为日志级别，无法解析时返回 info。
func ParseLevel(s string) slog.Level {
	var level slog.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return level
}
