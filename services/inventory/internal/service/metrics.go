package service

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 库存业务的指标（设计文档 1d 的 8.8）。不按 SKU 打标签：SKU 放在日志和 span 上。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/inventory/internal/service")

	stockReserved = otelx.Counter(meter, "inventory.stock.reserved",
		"预占生效的件数（新的预占；幂等重放不计）")
	stockReleased = otelx.Counter(meter, "inventory.stock.released",
		"释放生效的件数（RESERVED 转为 RELEASED；幂等重放和空回滚不计）")
	stockDeducted = otelx.Counter(meter, "inventory.stock.deducted",
		"扣减生效的件数（RESERVED 转为 CONFIRMED，即已售；幂等重放不计）")
	stockRejections = otelx.Counter(meter, "inventory.stock.rejections",
		"预占被业务拒绝的次数，按原因码分")
)

// rejectReasons 是 Reserve 会返回的业务拒绝原因码。标签预先建好，热路径上不分配；
// 不在表里的原因码（以后新增而这里没跟上）记成 other，不让任意字符串进标签。
var rejectReasons = func() map[string]metric.AddOption {
	m := map[string]metric.AddOption{}
	for _, r := range []string{
		errx.ReasonSKUNotFound,
		errx.ReasonMixedMerchants,
		errx.ReasonReservationReleased,
		errx.ReasonReservationMismatch,
		errx.ReasonInsufficientStock,
		"other",
	} {
		m[r] = metric.WithAttributes(attribute.String("reason", r))
	}
	return m
}()

func rejectionOpt(reason string) metric.AddOption {
	if opt, ok := rejectReasons[reason]; ok {
		return opt
	}
	return rejectReasons["other"]
}

// countPend 包一层 pend，把记下的件数累加到 n。只用于指标：事务提交成功之后才把 n 记进计数器。
func countPend(n *int64, pend func(ctx context.Context, orderID, skuID int64, qty int32) error) func(ctx context.Context, orderID, skuID int64, qty int32) error {
	return func(ctx context.Context, orderID, skuID int64, qty int32) error {
		if err := pend(ctx, orderID, skuID, qty); err != nil {
			return err
		}
		*n += int64(qty)
		return nil
	}
}
