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

	resultSucceeded = metric.WithAttributes(attribute.String("result", "succeeded"))
	resultFailed    = metric.WithAttributes(attribute.String("result", "failed"))
)
