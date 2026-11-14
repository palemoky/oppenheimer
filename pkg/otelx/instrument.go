package otelx

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// 各服务建指标和给 span 加属性时共用的小工具，让名字和写法在各处一致。
//
// 约定（设计文档 1d 的 8.8，ADR-0023）：
//   - 仪表在包级变量里建一次，用全局的 meter。全局的 meter 是代理，在 Setup 之前建的仪表之后自动生效，
//     没有装配时是空操作，所以测试里不用管它。
//   - 指标的标签只能是有限的枚举：结果、原因码、规则名、topic。订单号、用户号、SKU、IP 一律不进标签：
//     每种标签组合是一条独立的时间序列，在进程里和后端里各占一份内存。
//   - 订单号、用户号放在 span 上（SetOrder），链路里高基数没有代价。

// Meter 返回名为 scope 的 meter。scope 用包的导入路径。
func Meter(scope string) metric.Meter { return otel.Meter(scope) }

// Counter 建一个只增的计数器。名字不合法时 panic：那是写代码时的错误，不是运行时会出现的情况。
func Counter(m metric.Meter, name, desc string) metric.Int64Counter {
	c, err := m.Int64Counter(name, metric.WithDescription(desc))
	if err != nil {
		panic(err)
	}
	return c
}

// Histogram 建一个耗时的直方图，单位是秒（记录时用 d.Seconds()）。桶边界由 Setup 统一设成 DurationBuckets。
func Histogram(m metric.Meter, name, desc string) metric.Float64Histogram {
	h, err := m.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(desc))
	if err != nil {
		panic(err)
	}
	return h
}

// SizeHistogram 建一个记“个数”的直方图（每批的行数这一类），boundaries 是桶边界。
func SizeHistogram(m metric.Meter, name, desc string, boundaries ...float64) metric.Int64Histogram {
	h, err := m.Int64Histogram(name, metric.WithDescription(desc), metric.WithExplicitBucketBoundaries(boundaries...))
	if err != nil {
		panic(err)
	}
	return h
}

// Gauge 建一个同步的瞬时值（状态、开关）：值变化时记录一次，导出的是最后一次记录的值。
func Gauge(m metric.Meter, name, desc string) metric.Int64Gauge {
	g, err := m.Int64Gauge(name, metric.WithDescription(desc))
	if err != nil {
		panic(err)
	}
	return g
}

// ObserveGauge 登记一个在每次导出时现读的瞬时值（积压的行数、在用的名额这一类）。
//
// read 在导出的 goroutine 里调用，要快、不能阻塞；它要查数据库时，在别处定期查好存进一个原子变量，这里只读变量。
// 返回的 unregister 在组件停止时调用。
func ObserveGauge(m metric.Meter, name, desc string, read func() int64, attrs ...attribute.KeyValue) (unregister func(), err error) {
	g, err := m.Int64ObservableGauge(name, metric.WithDescription(desc))
	if err != nil {
		return nil, err
	}
	opt := metric.WithAttributes(attrs...)
	reg, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(g, read(), opt)
		return nil
	}, g)
	if err != nil {
		return nil, err
	}
	return func() { _ = reg.Unregister() }, nil
}

// ObserveCounter 登记一个在每次导出时现读的累计值：别处已经在数（一个只增的原子变量），这里只把它报出去。
// 和 ObserveGauge 的区别是后端把它当计数器，可以直接算速率。read 的要求和 unregister 的用法相同。
func ObserveCounter(m metric.Meter, name, desc string, read func() int64, attrs ...attribute.KeyValue) (unregister func(), err error) {
	c, err := m.Int64ObservableCounter(name, metric.WithDescription(desc))
	if err != nil {
		return nil, err
	}
	opt := metric.WithAttributes(attrs...)
	reg, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(c, read(), opt)
		return nil
	}, c)
	if err != nil {
		return nil, err
	}
	return func() { _ = reg.Unregister() }, nil
}

// SetUser 把用户号记在 ctx 当前的 span 上，给只知道用户、还不知道订单的地方用（入口的鉴权、按用户列出订单）。
func SetUser(ctx context.Context, userID int64) {
	if span := trace.SpanFromContext(ctx); span.IsRecording() && userID != 0 {
		span.SetAttributes(attribute.Int64("app.user_id", userID))
	}
}

// SetOrder 把订单号和用户号记在 ctx 当前的 span 上。按订单号找链路是排查时最常用的入口。
// userID 为 0 时不记。ctx 里没有 span 或 span 没被采样时什么也不做。
func SetOrder(ctx context.Context, orderID, userID int64) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(attribute.Int64("app.order_id", orderID))
	if userID != 0 {
		span.SetAttributes(attribute.Int64("app.user_id", userID))
	}
}

// SetReason 把业务上的拒绝原因记在当前的 span 上（库存不足、限流、降级的原因码）。
// 这些是正常结果，不把 span 标成 Error：否则“错误率”会被业务拒绝淹没。
func SetReason(ctx context.Context, reason string) {
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attribute.String("app.reason", reason))
	}
}
