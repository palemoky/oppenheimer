package service

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"google.golang.org/grpc/codes"

	"github.com/palemoky/oppenheimer/pkg/errx"
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

// 预占、释放、扣减的件数只在操作确实生效时记：幂等重放、空回滚不记。拒绝按原因码记。
func TestStockMetrics(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	e.seed(t, 2, 100, 10)
	ctx := t.Context()

	delta := func(name string, attrs ...attribute.KeyValue) func() int64 {
		before := metricSum(t, name, attrs...)
		return func() int64 { return metricSum(t, name, attrs...) - before }
	}
	reserved := delta("inventory.stock.reserved")
	released := delta("inventory.stock.released")
	deducted := delta("inventory.stock.deducted")
	notFound := delta("inventory.stock.rejections", attribute.String("reason", errx.ReasonSKUNotFound))

	// 订单 100 预占 2+3 件，重放一次。
	for range 2 {
		_, err := e.reserve(ctx, 100, 1, 2, 2, 3)
		mustOK(t, err)
	}
	if got := reserved(); got != 5 {
		t.Errorf("reserved = %d, want 5 (the replay must not count)", got)
	}

	// 订单 101 预占 1 件后确认两次；订单 100 释放两次；订单 102 没有预占，释放是空回滚。
	_, err := e.reserve(ctx, 101, 1, 1)
	mustOK(t, err)
	for range 2 {
		mustOK(t, e.confirm(ctx, 101))
		mustOK(t, e.release(ctx, 100))
	}
	mustOK(t, e.release(ctx, 102))
	if got := deducted(); got != 1 {
		t.Errorf("deducted = %d, want 1", got)
	}
	if got := released(); got != 5 {
		t.Errorf("released = %d, want 5 (replays and empty rollbacks must not count)", got)
	}

	// SKU 不存在：业务拒绝，不记预占件数。
	before := reserved()
	_, err = e.reserve(ctx, 103, 999, 1)
	assertErr(t, err, codes.NotFound, errx.ReasonSKUNotFound)
	if got := notFound(); got != 1 {
		t.Errorf("rejections{reason=SKU_NOT_FOUND} = %d, want 1", got)
	}
	if got := reserved(); got != before {
		t.Errorf("a rejected reserve changed reserved by %d", got-before)
	}
}
