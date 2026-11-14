package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// HTTP 入口的链路和指标（设计文档 1d 的 8.4、8.8）。
//
// 中间件的顺序（从外到内）：traceEntry → requestLog → recoverPanic → API 分组上的开关和限流 → handler。
// 链路在最外层，被限流、被开关拒绝、404 的请求也有 span 和 X-Trace-Id；panic 被 recoverPanic 变成 500 之后才回到这里。
//
// 不用 otelgin：它没有“公共入口”的选项（外面的 traceparent 会成为父 span），指标的 server.port 标签取自客户端写的
// Host 头，span 状态按“5xx 就是 Error”定（降级的 503 也算，和 8.4 矛盾），客户端地址信任任何 X-Forwarded-For。
// 这几处都要绕开之后，它只剩“建一个 span”，不如自己写。
//
// 健康检查不用过滤：/healthz、/readyz 挂在 cmd/gateway 的外层 mux 上，根本不进这个 gin 引擎（见 publicMux）。

// TraceIDHeader 是响应里的链路 ID（当前 span 的 trace id，32 个十六进制字符）。压测工具和排查时靠它找链路。
const TraceIDHeader = "X-Trace-Id"

// instrumentationScope 是本包的 tracer 和 meter 的 scope（包的导入路径）。
const instrumentationScope = "github.com/palemoky/oppenheimer/services/gateway/internal/httpapi"

// unmatchedRoute 是没有匹配到路由（404、405）的请求在 span 名和指标里的路由名。不能用原始路径：
// 扫描器随便打的路径每个都会变成一个新的 span 名、一条新的时间序列。
const unmatchedRoute = "unmatched"

// degradedKey 是 gin context 里的键：writeDegraded 写出降级响应时设上，traceEntry 据此不把 503 标成 Error。
const degradedKey = "otel.degraded"

var (
	meter = otelx.Meter(instrumentationScope)

	// requestDuration 是 HTTP 请求的耗时，标签只有方法、路由模板和状态码。
	requestDuration = otelx.Histogram(meter, "http.server.request.duration", "HTTP 请求的耗时，从进入 gateway 到 handler 返回")
	// rateLimitDecisions 是限流的判定次数，标签 rule（ip、user、route、hot）和 outcome（allowed、rejected）。
	// 限流器自己出错（rateLimiterFailed）时不计：那时没有判定，有 WARN 日志。
	rateLimitDecisions = otelx.Counter(meter, "gateway.ratelimit.decisions", "限流的判定次数")
	// degradedResponses 是降级的 503 的次数，标签 reason 是降级原因码（errx 里的有限几个，见 writeDegraded）。
	degradedResponses = otelx.Counter(meter, "gateway.degraded.responses", "降级响应（503 加降级原因码）的次数")
)

// 限流判定的标签组合是有限的，预先建好，判定时不分配内存。
const (
	outcomeAllowed = iota
	outcomeRejected
	numOutcomes
)

var decisionAttrs = func() (out [numDimensions][numOutcomes]metric.AddOption) {
	outcomes := [numOutcomes]string{"allowed", "rejected"}
	for d := range numDimensions {
		for o := range numOutcomes {
			out[d][o] = metric.WithAttributeSet(attribute.NewSet(
				attribute.String("rule", dimensionNames[d]),
				attribute.String("outcome", outcomes[o]),
			))
		}
	}
	return out
}()

// recordDecision 记一次限流判定。
func recordDecision(ctx context.Context, dim dimension, allowed bool) {
	o := outcomeRejected
	if allowed {
		o = outcomeAllowed
	}
	rateLimitDecisions.Add(ctx, 1, decisionAttrs[dim][o])
}

// inboundTraceContext 只用来读请求里的 traceparent，生成 link。不读 baggage：外面带进来的 baggage 同样不可信，
// 而且 baggage 会随 ctx 传到所有下游。
var inboundTraceContext = propagation.TraceContext{}

// newTracer 在 New 里取一次全局的 tracer。
func newTracer() trace.Tracer { return otel.GetTracerProvider().Tracer(instrumentationScope) }

// traceEntry 是 HTTP 入口的链路中间件：建服务端 span，在响应头里写 X-Trace-Id，结束时定 span 的状态、记耗时的指标。
//
// gateway 是对外的入口，外面带进来的 traceparent 不当父 span（任何人都能把自己的 span 接到我们的链路上，
// 或者用采样位让我们全量采样），span 永远是新的根（WithNewRoot），外面的上下文只记成 link。
// 采样器能看到这个 link，但我们的采样器（otelx：ParentBased 外包 dropRootClient）不看 link，
// 新的根按我们自己的采样率决定，外面的 sampled 标志影响不了（TestTraceIgnoresInboundSampledFlag 钉住）。
//
// span 名在开始时就是路由模板：gin 先匹配路由、设好 c.FullPath()，再执行中间件链（Engine.handleHTTPRequest），
// 所以这里已经知道是哪个路由。
func (s *server) traceEntry(c *gin.Context) {
	start := time.Now()
	req := c.Request
	method := standardMethod(req.Method)
	route := c.FullPath()
	if route == "" {
		route = unmatchedRoute
	}

	attrs := []attribute.KeyValue{
		semconv.HTTPRequestMethodKey.String(method),
		semconv.HTTPRoute(route),
		semconv.URLPath(req.URL.Path),
	}
	if ua := req.UserAgent(); ua != "" {
		attrs = append(attrs, semconv.UserAgentOriginal(ua))
	}
	// 客户端地址和按 IP 限流用同一套规则（clientAddr）：X-Forwarded-For 只在来自受信任的代理时才信。
	if addr, ok := clientAddr(req.RemoteAddr, req.Header.Values("X-Forwarded-For"), s.trusted); ok {
		attrs = append(attrs, semconv.ClientAddress(addr.String()))
	}
	opts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attrs...),
	}
	remote := trace.SpanContextFromContext(inboundTraceContext.Extract(context.Background(), propagation.HeaderCarrier(req.Header)))
	if remote.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: remote}))
	}

	ctx, span := s.tracer.Start(req.Context(), method+" "+route, opts...)
	defer span.End()
	// 下游的 gRPC 调用（传播 traceparent）和日志（trace_id）都从请求的 ctx 里取 span。
	c.Request = req.WithContext(ctx)
	if sc := span.SpanContext(); sc.HasTraceID() {
		// 在 c.Next 之前写：响应头只能在写响应体之前设置。
		c.Header(TraceIDHeader, sc.TraceID().String())
	}

	c.Next()

	status := c.Writer.Status()
	span.SetAttributes(semconv.HTTPResponseStatusCode(status))
	if serverFault(c, status) {
		span.SetStatus(codes.Error, http.StatusText(status))
	}
	requestDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
		semconv.HTTPRequestMethodKey.String(method),
		semconv.HTTPRoute(route),
		semconv.HTTPResponseStatusCode(status),
	))
}

// serverFault 判断这个响应是不是服务端的故障（设计文档 1d 8.4“什么算错误”，和 grpcx.isServerFault 同一条规则）。
// 只有 5xx 而且不是降级响应才算。降级的 503 是“自己是好的、依赖坏了”，原因码在 app.reason；
// 4xx、429、499（客户端断开）都不算：否则错误率会被业务拒绝和客户端的行为淹没。
func serverFault(c *gin.Context, status int) bool {
	return status >= http.StatusInternalServerError && !c.GetBool(degradedKey)
}

// standardMethod 把方法名限制在标准的几个里，其余的记成 _OTHER（OTel 语义约定的写法）：方法名也是客户端随便写的。
func standardMethod(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return m
	default:
		return "_OTHER"
	}
}

// setRateLimited 在 span 上记被限流：原因码和是哪条规则拒绝的。429 不是服务端的错，span 不标 Error。
func setRateLimited(ctx context.Context, dim dimension) {
	otelx.SetReason(ctx, codeRateLimited)
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.String("app.ratelimit.rule", dimensionNames[dim]))
	}
}

// RegisterFallbackMetric 登记 gateway.ratelimit.fallbacks：按用户和接口的限流退回进程内的累计次数。
// fallbacks 是 ratelimit.Fallback.Fallbacks（只增），在每次导出时现读。返回的 unregister 在限流器关闭时调用。
func RegisterFallbackMetric(fallbacks func() int64) (unregister func(), err error) {
	return otelx.ObserveCounter(meter, "gateway.ratelimit.fallbacks",
		"按用户和接口的限流因为 Redis 不可用而退回进程内的累计次数", fallbacks)
}
