package processor

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 支付处理器的指标（设计文档 1d 的 8.8）。支付单号、订单号不进标签。
var (
	meter  = otelx.Meter("github.com/palemoky/oppenheimer/services/payment/internal/processor")
	tracer = otel.Tracer("github.com/palemoky/oppenheimer/services/payment/internal/processor")

	results = otelx.Counter(meter, "payment.results",
		"支付单得出结果的次数（PENDING 转为终态，只记完成这次转换的那一次），按结果分")

	// 名字以 .delay 结尾：otelx 给这类“排队等了多久”的直方图用到 30 分钟的桶（otelx.WaitBuckets）。
	// 3b 的阶梯压测里支付单排队超过 15 分钟，当时只有上面的计数，看不出在排队。
	resultDelay = otelx.Histogram(meter, "payment.result.delay",
		"支付单从创建到得出结果等了多久，按结果分。正常值是模拟延迟加一轮的间隔；比它大说明处理器跟不上")

	resultSucceeded = metric.WithAttributes(attribute.String("result", "succeeded"))
	resultFailed    = metric.WithAttributes(attribute.String("result", "failed"))
)
