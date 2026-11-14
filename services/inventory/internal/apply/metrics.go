package apply

import (
	"context"

	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/otelx"
)

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

// RegisterMetrics 登记 inventory.stock_pending.backlog：stock_pending 里还没应用进 sku_stock 的行数，没有标签。
// 一直涨说明应用者跟不上或者卡住了。
//
// 值由应用者自己定期数（refreshBacklog），这里只读它存下的结果；没数到过或者不新鲜时不上报（而不是报 0）。
// 每个实例都跑应用者，各自上报的是同一张表的行数，看的时候取最大值，不要求和。
// 返回的 unregister 在应用者停止后调用。
func (a *Applier) RegisterMetrics() (unregister func(), err error) {
	g, err := meter.Int64ObservableGauge("inventory.stock_pending.backlog",
		metric.WithDescription("stock_pending 里还没应用进 sku_stock 的增量行数（应用者每 5 秒左右数一次）"))
	if err != nil {
		return nil, err
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		if rows, ok := a.Backlog(); ok {
			o.ObserveInt64(g, rows)
		}
		return nil
	}, g)
	if err != nil {
		return nil, err
	}
	return func() { _ = reg.Unregister() }, nil
}
