package consumer

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 订单答复的指标（设计文档 1d 的 8.8）。支付单号、订单号不进标签。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/payment/internal/consumer")

	refunds = otelx.Counter(meter, "payment.refunds",
		"实际退款的次数（SUCCEEDED 转为 REFUNDED，只记完成这次转换的那一次），按订单服务给的原因分")
	orderReplies = otelx.Counter(meter, "payment.order_replies",
		"订单服务的答复被处理完的次数（含重复投递的答复），按答复的种类分")

	replyAccepted       = metric.WithAttributes(attribute.String("disposition", "ACCEPTED"))
	replyRefundRequired = metric.WithAttributes(attribute.String("disposition", "REFUND_REQUIRED"))
	replyUnknown        = metric.WithAttributes(attribute.String("disposition", "unknown"))
)

// refundReasons 是订单服务要求退款时会给的原因码。不在表里的记成 other，不让任意字符串进标签。
var refundReasons = func() map[string]metric.AddOption {
	m := map[string]metric.AddOption{}
	for _, r := range []string{
		errx.ReasonOrderNotFound,
		errx.ReasonOrderNotPayable,
		errx.ReasonAmountMismatch,
		"other",
	} {
		m[r] = metric.WithAttributes(attribute.String("reason", r))
	}
	return m
}()

func refundReasonOpt(reason string) metric.AddOption {
	if opt, ok := refundReasons[reason]; ok {
		return opt
	}
	return refundReasons["other"]
}
