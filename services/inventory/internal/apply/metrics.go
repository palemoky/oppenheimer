package apply

import "github.com/palemoky/oppenheimer/pkg/otelx"

// 应用者的指标（设计文档 1d 的 8.8）。不按 SKU 打标签。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/inventory/internal/apply")

	// batchSize 的桶边界覆盖到 store.MaxApplyBatch。
	batchSize = otelx.SizeHistogram(meter, "inventory.applier.batch_size",
		"每批应用进 sku_stock 的增量行数（只记取到了行的批）",
		1, 10, 50, 100, 200, 500, 1000, 2000, 5000)
	batchDuration = otelx.Histogram(meter, "inventory.applier.duration",
		"每批应用的耗时（一个事务，只记取到了行的批）")
	oversellDetected = otelx.Counter(meter, "inventory.oversell.detected",
		"新发现卡住的 SKU 数：未应用的增量加上去后有数为负（超卖或恒等式被破坏），或 sku_stock 里没有这一行")
)
