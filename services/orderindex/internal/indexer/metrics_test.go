package indexer

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
)

// metricReader 收集本测试包里的指标。全局的 MeterProvider 是进程级状态，只在 TestMain 里设一次；
// 包级变量里建的仪表经全局 meter 的代理在这之后生效。用例可能并行，断言差值。
var metricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader)))
	os.Exit(m.Run())
}

// metricValue 返回名为 name、标签包含 attrs 的数据点的合计：计数器和瞬时值是值的和，
// 直方图的 sum 是记录值的和、count 是记录次数。没有数据点时都是 0。
func metricValue(t *testing.T, name string, attrs ...attribute.KeyValue) (sum int64, count uint64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	match := func(set attribute.Set) bool {
		for _, kv := range attrs {
			if v, ok := set.Value(kv.Key); !ok || v.String() != kv.Value.String() {
				return false
			}
		}
		return true
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						sum += dp.Value
						count++
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						sum += dp.Value
						count++
					}
				}
			case metricdata.Histogram[int64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						sum += dp.Sum
						count += dp.Count
					}
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						count += dp.Count
					}
				}
			}
		}
	}
	return sum, count
}

// metricSum 返回 metricValue 的 sum。
func metricSum(t *testing.T, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	sum, _ := metricValue(t, name, attrs...)
	return sum
}

// 每批写库记一次批大小和写库耗时；按版本被丢弃的和实际写入的分别计数。
// 别的用例并行地写同一组指标，只断言“至少增加了”。
func TestApplyMetrics(t *testing.T) {
	ix, _ := newIndexer(t)
	sum0, batches0 := metricValue(t, "orderindex.batch_size")
	_, durations0 := metricValue(t, "orderindex.apply.duration")
	applied := attribute.String("outcome", "applied")
	stale := attribute.String("outcome", "stale")
	applied0, stale0 := metricSum(t, "orderindex.rows", applied), metricSum(t, "orderindex.rows", stale)

	// 第一批：两个订单，100 有两个版本（v1 被批内合并掉），写 2 行。
	apply(t, ix,
		change(cdcv1.Op_OP_INSERT, image(1, 100, 1)),
		change(cdcv1.Op_OP_UPDATE, image(1, 100, 2)),
		change(cdcv1.Op_OP_INSERT, image(1, 101, 1)),
	)
	// 第二批：旧版本，写 1 行但不改变索引。
	apply(t, ix, change(cdcv1.Op_OP_UPDATE, image(1, 100, 1)))

	sum, batches := metricValue(t, "orderindex.batch_size")
	if batches-batches0 < 2 || sum-sum0 < 3 {
		t.Errorf("batch_size: %d more batches with %d more rows, want at least 2 and 3", batches-batches0, sum-sum0)
	}
	if _, durations := metricValue(t, "orderindex.apply.duration"); durations-durations0 < 2 {
		t.Errorf("apply.duration recorded %d more times, want at least 2", durations-durations0)
	}
	if got := metricSum(t, "orderindex.rows", applied) - applied0; got < 2 {
		t.Errorf("rows{applied} increased by %d, want at least 2", got)
	}
	if got := metricSum(t, "orderindex.rows", stale) - stale0; got < 2 {
		t.Errorf("rows{stale} increased by %d, want at least 2", got)
	}
}
