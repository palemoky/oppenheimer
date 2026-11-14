package reconcile

import "github.com/palemoky/oppenheimer/pkg/otelx"

// 闸门对账的指标（设计文档 1d 的 8.8）。不按 SKU 打标签。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/inventory/internal/reconcile")

	drifts = otelx.Counter(meter, "inventory.reconcile.drifts",
		"认定为真实偏差的次数：同一个 SKU 连续若干轮得到同一个非零偏差、要去修正时记一次（单轮的瞬时偏差不记）")
	repairs = otelx.Counter(meter, "inventory.reconcile.repairs",
		"实际改动了闸门 avail 的修正次数（被上下限截住、什么都没改的不记）")
)
