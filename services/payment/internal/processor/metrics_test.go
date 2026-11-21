package processor_test

import (
	"context"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/palemoky/oppenheimer/services/payment/internal/processor"
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

// payment.results 只在完成转换的那一次记，按结果分；已出结果的支付单再跑一轮不再记。
func TestResultMetrics(t *testing.T) {
	e := newEnv(t, processor.Config{})
	ok := attribute.String("result", "succeeded")
	bad := attribute.String("result", "failed")
	ok0, bad0 := metricSum(t, "payment.results", ok), metricSum(t, "payment.results", bad)

	e.create(t, success)
	e.create(t, success)
	e.create(t, failure)
	e.clock.Advance(delay)
	e.round(t)
	e.clock.Advance(time.Hour)
	e.round(t)

	if got := metricSum(t, "payment.results", ok) - ok0; got < 2 {
		t.Errorf("results{succeeded} increased by %d, want at least 2", got)
	}
	if got := metricSum(t, "payment.results", bad) - bad0; got < 1 {
		t.Errorf("results{failed} increased by %d, want at least 1", got)
	}
}

// payment.result.delay 记的是创建到出结果的时间，每完成一次转换记一次。
func TestResultDelayMetric(t *testing.T) {
	e := newEnv(t, processor.Config{})
	ok := attribute.String("result", "succeeded")
	_, n0 := metricValue(t, "payment.result.delay", ok)

	e.create(t, success)
	e.create(t, success)
	e.clock.Advance(7 * time.Minute)
	e.round(t)
	e.round(t)

	if _, n := metricValue(t, "payment.result.delay", ok); n-n0 < 2 {
		t.Errorf("result.delay{succeeded} recorded %d times, want at least 2", n-n0)
	}
}
