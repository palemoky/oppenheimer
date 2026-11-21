// Package otelx 装配 OpenTelemetry：建链路和指标的 provider，设成全局的，并提供几个埋点时共用的小工具。
//
// 业务代码和 pkg 里的其他包只依赖 OTel 的 API（otel.Tracer、otel.Meter），不依赖这个包里的 SDK 装配；
// 每个服务在 main 里调用一次 Setup。设计见设计文档 1d 第 8 节，决定见 ADR-0004、ADR-0023。
package otelx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// 用到的环境变量都是 OTel 规范里的标准名字，不自己发明：到了集群上改部署清单就能指向 Collector。
const (
	// EndpointEnv 是 OTLP（gRPC）的地址，例如 http://127.0.0.1:4317。没设时不导出任何东西。
	EndpointEnv = "OTEL_EXPORTER_OTLP_ENDPOINT"
	// SamplerEnv 和 SamplerArgEnv 选采样器，见 samplerFromEnv。
	SamplerEnv    = "OTEL_TRACES_SAMPLER"
	SamplerArgEnv = "OTEL_TRACES_SAMPLER_ARG"
)

// ShutdownTimeout 是停止时把缓冲里的 span 和最后一次指标发出去的时限。
// 后端不在时导出会一直失败，不能让它拖住进程退出。
const ShutdownTimeout = 5 * time.Second

// DurationBuckets 是耗时类直方图的桶边界，单位秒。
//
// SDK 默认的边界是给秒级的 HTTP 请求定的（最小 5 毫秒），而一次 Redis 调用不到 1 毫秒、一次提交几毫秒，
// 全落在第一个桶里就什么也看不出来。这里从 0.5 毫秒到 10 秒，Setup 用 View 把它套在所有单位是秒的直方图上。
var DurationBuckets = []float64{
	0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10,
}

// WaitBuckets 是“排队等了多久”这类直方图的桶边界，单位秒，给名字以 .delay 结尾的直方图用
// （order.persist.delay、payment.result.delay）。它们量的不是一次调用的耗时，而是一件事在队列里等的时间，
// 系统跟不上时是几十秒到十几分钟：3b 的阶梯压测里落库延迟全部落在“超过 10 秒”那一个桶里，
// 支付单排队超过了 15 分钟的支付超时。上限取 30 分钟，是支付超时的两倍。
var WaitBuckets = []float64{
	0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600, 900, 1800,
}

// waitSuffix 是用 WaitBuckets 的直方图的名字后缀。
const waitSuffix = ".delay"

// Setup 建全局的 TracerProvider、MeterProvider 和传播器，返回的 shutdown 在所有组件停止之后调用。
//
// 没设 OTEL_EXPORTER_OTLP_ENDPOINT 时：仍然建 TracerProvider，只是不挂导出器。span 照常产生、有 trace id，
// 日志就能用 trace id 串起来；不往任何地方发。指标保持 API 默认的空实现。测试、e2e、chaos 默认是这个状态。
//
// 设了地址而后端不在时不影响服务：导出器的连接是惰性建立的，Setup 不等它；批处理的队列满了就丢 span，
// 不阻塞业务 goroutine；导出失败只记日志（限频）。可观测性是可选依赖。
//
// 只有配置本身不对（采样器的名字或参数写错）才返回错误，和别处“名字写错拒绝启动”一致。
func Setup(ctx context.Context, service string, logger *slog.Logger) (shutdown func(context.Context) error, err error) {
	if logger == nil {
		logger = slog.Default()
	}
	sampler, err := samplerFromEnv()
	if err != nil {
		return nil, err
	}
	res, err := newResource(ctx, service)
	if err != nil {
		return nil, fmt.Errorf("otelx: resource: %w", err)
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	otel.SetErrorHandler(newErrorHandler(logger))

	tpOpts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res), sdktrace.WithSampler(sampler)}
	exporting := os.Getenv(EndpointEnv) != ""
	var mp *sdkmetric.MeterProvider
	if exporting {
		traceExp, err := otlptracegrpc.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("otelx: trace exporter: %w", err)
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(traceExp))

		metricExp, err := otlpmetricgrpc.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("otelx: metric exporter: %w", err)
		}
		mp = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			// 导出间隔读 OTEL_METRIC_EXPORT_INTERVAL（SDK 自己读），默认 60 秒。
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
			sdkmetric.WithView(DurationView()),
		)
		otel.SetMeterProvider(mp)
		// Go 运行时的指标：goroutine 数、堆、GC。
		if err := runtime.Start(runtime.WithMeterProvider(mp)); err != nil {
			return nil, fmt.Errorf("otelx: runtime metrics: %w", err)
		}
	}
	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)
	logger.InfoContext(ctx, "telemetry configured", "exporting", exporting, "sampler", sampler.Description())

	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, ShutdownTimeout)
		defer cancel()
		errs := []error{tp.Shutdown(ctx)}
		if mp != nil {
			errs = append(errs, mp.Shutdown(ctx))
		}
		return errors.Join(errs...)
	}, nil
}

// DurationView 给所有单位是秒的直方图定桶边界：名字以 .delay 结尾的用 WaitBuckets，其余用 DurationBuckets。
// 导出给测试里自己建 MeterProvider 时用。
//
// 写成一个函数而不是两个 NewView：一个仪表匹配上的每个 View 各产生一条流，
// “单位是秒”和“名字以 .delay 结尾”两个条件同时成立时会导出两份、边界还不一样。
func DurationView() sdkmetric.View {
	return func(i sdkmetric.Instrument) (sdkmetric.Stream, bool) {
		if i.Kind != sdkmetric.InstrumentKindHistogram || i.Unit != "s" {
			return sdkmetric.Stream{}, false
		}
		b := DurationBuckets
		if strings.HasSuffix(i.Name, waitSuffix) {
			b = WaitBuckets
		}
		return sdkmetric.Stream{
			Name:        i.Name,
			Description: i.Description,
			Unit:        i.Unit,
			Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: b},
		}, true
	}
}

func newResource(ctx context.Context, service string) (*resource.Resource, error) {
	host, _ := os.Hostname()
	attrs := []attribute.KeyValue{
		semconv.ServiceName(service),
		// 同一个服务的多个实例要能分开：主机名加进程号。在 K8s 里主机名就是 Pod 名。
		semconv.ServiceInstanceID(host + ":" + strconv.Itoa(os.Getpid())),
	}
	if v := buildRevision(); v != "" {
		attrs = append(attrs, semconv.ServiceVersion(v))
	}
	// WithFromEnv 放在后面：OTEL_RESOURCE_ATTRIBUTES 里写的可以覆盖上面的值（部署时加环境、集群名）。
	return resource.New(ctx, resource.WithTelemetrySDK(), resource.WithAttributes(attrs...), resource.WithFromEnv())
}

// buildRevision 返回构建时的提交号（前 12 位），拿不到时返回空串。
func buildRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			return s.Value[:min(len(s.Value), 12)]
		}
	}
	return ""
}

// samplerFromEnv 按 OTEL_TRACES_SAMPLER 和 OTEL_TRACES_SAMPLER_ARG 选采样器，没设时是 parentbased_always_on。
//
// SDK 自己也会读这两个变量，但只在没有显式传采样器时才读；这里要在外面再包一层 dropRootClient，
// 所以自己解析。支持规范里的六个名字，写错了返回错误。
//
// 常用的是 parentbased_traceidratio：入口按比例决定，决定写在 traceparent 的标志位里，后面每一跳
// （包括经过 Kafka 和 outbox 的）都跟着这个决定，一条链路要么完整、要么没有。
func samplerFromEnv() (sdktrace.Sampler, error) {
	name := strings.TrimSpace(os.Getenv(SamplerEnv))
	arg := strings.TrimSpace(os.Getenv(SamplerArgEnv))
	ratio := func() (sdktrace.Sampler, error) {
		if arg == "" {
			return sdktrace.TraceIDRatioBased(1), nil
		}
		f, err := strconv.ParseFloat(arg, 64)
		if err != nil || f < 0 || f > 1 {
			return nil, fmt.Errorf("otelx: %s=%q: want a number between 0 and 1", SamplerArgEnv, arg)
		}
		return sdktrace.TraceIDRatioBased(f), nil
	}
	var root sdktrace.Sampler
	parentBased := true
	switch name {
	case "", "parentbased_always_on":
		root = sdktrace.AlwaysSample()
	case "parentbased_always_off":
		root = sdktrace.NeverSample()
	case "parentbased_traceidratio":
		s, err := ratio()
		if err != nil {
			return nil, err
		}
		root = s
	case "always_on":
		root, parentBased = sdktrace.AlwaysSample(), false
	case "always_off":
		root, parentBased = sdktrace.NeverSample(), false
	case "traceidratio":
		s, err := ratio()
		if err != nil {
			return nil, err
		}
		root, parentBased = s, false
	default:
		return nil, fmt.Errorf("otelx: %s=%q: unknown sampler", SamplerEnv, name)
	}
	if parentBased {
		root = sdktrace.ParentBased(root)
	}
	return dropRootClient{next: root}, nil
}

// dropRootClient 丢掉没有父 span 的 CLIENT span，其余交给 next。
//
// 中继、sweeper、库存的应用者、锁的续期这些后台循环每秒都在访问 MySQL 和 Redis，它们的 ctx 里没有 span，
// 客户端的埋点会给每一次访问开一条只有一个 span 的链路。这些链路没有信息量，数量却比真实请求的还多。
// 后台循环的情况看指标。有父 span 的访问（请求里的、消息处理里的）不受影响。
type dropRootClient struct{ next sdktrace.Sampler }

func (s dropRootClient) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	psc := trace.SpanContextFromContext(p.ParentContext)
	if p.Kind == trace.SpanKindClient && !psc.IsValid() {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop, Tracestate: psc.TraceState()}
	}
	return s.next.ShouldSample(p)
}

func (s dropRootClient) Description() string { return "DropRootClient{" + s.next.Description() + "}" }

// errorHandler 把 SDK 内部的错误（主要是导出失败）记成 WARN，每 10 秒最多一条。
// 默认的处理是用标准库的 log 打到标准错误，不是 JSON；后端不在时每次导出都会失败，不限频会刷屏。
type errorHandler struct {
	logger *slog.Logger
	last   atomic.Int64
	missed atomic.Int64
}

const errorLogInterval = 10 * time.Second

func newErrorHandler(logger *slog.Logger) *errorHandler { return &errorHandler{logger: logger} }

// Handle 实现 otel.ErrorHandler。
func (h *errorHandler) Handle(err error) {
	now := time.Now().UnixNano()
	last := h.last.Load()
	if now-last < int64(errorLogInterval) || !h.last.CompareAndSwap(last, now) {
		h.missed.Add(1)
		return
	}
	h.logger.Warn("telemetry error", "error", err, "suppressed_since_last", h.missed.Swap(0))
}

// HasParent 报告 ctx 里有没有有效的 span。给“没有父 span 就不建 span”的过滤用。
func HasParent(ctx context.Context) bool {
	return trace.SpanContextFromContext(ctx).IsValid()
}

// Traceparent 返回 ctx 里链路上下文的 W3C traceparent 文本，没有 span 时返回空串。
// 给要把上下文存进数据库的地方用（outbox 表的 trace_context 列）。
func Traceparent(ctx context.Context) string {
	if !HasParent(ctx) {
		return ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Shutdown 调用 Setup 返回的 shutdown，失败只记日志。给 main 里 defer 用：
//
//	shutdown, err := otelx.Setup(ctx, serviceName, logger)
//	...
//	defer otelx.Shutdown(ctx, shutdown, logger)
//
// 这个 defer 要最先注册，于是最后执行：别的组件停止时产生的 span 也能发出去。
// ctx 这时多半已经取消了（进程在退出），所以只沿用它带的值，不沿用它的取消；时限由 shutdown 自己设。
func Shutdown(ctx context.Context, shutdown func(context.Context) error, logger *slog.Logger) {
	if err := shutdown(context.WithoutCancel(ctx)); err != nil {
		logger.WarnContext(ctx, "telemetry shutdown did not finish cleanly, some spans or metrics may be lost", "error", err)
	}
}
