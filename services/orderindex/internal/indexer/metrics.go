package indexer

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 写索引的指标（设计文档 1d 的 8.8）。handler 的总耗时 kafkax 已经记了，这里记其中写库的那一段。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/orderindex/internal/indexer")

	batchSize = otelx.SizeHistogram(meter, "orderindex.batch_size",
		"每批合并后写进索引库的行数（只记有行要写的批）",
		1, 10, 50, 100, 200, 500, 1000)
	applyDuration = otelx.Histogram(meter, "orderindex.apply.duration",
		"每批写索引库的耗时（store.UpsertRows，两个库并行）")
	rows = otelx.Counter(meter, "orderindex.rows",
		"有商家的变更按结果分：applied 改变了索引，stale 没改变（批内被合并掉的旧映像或版本不高于索引里的）。"+
			"多行语句分不开插入和更新，applied 可能偏大、stale 可能偏小，见 Counts.Changed")

	outcomeApplied = metric.WithAttributes(attribute.String("outcome", "applied"))
	outcomeStale   = metric.WithAttributes(attribute.String("outcome", "stale"))
)
