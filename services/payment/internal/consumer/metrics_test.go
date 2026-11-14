package consumer_test

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

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

// payment.refunds 只在真正退款的那一次记；payment.order_replies 按答复的种类记每次处理完的答复（含重复投递）。
func TestRefundAndReplyMetrics(t *testing.T) {
	e := newEnv(t)
	reason := attribute.String("reason", errx.ReasonOrderNotPayable)
	refundReply := attribute.String("disposition", "REFUND_REQUIRED")
	acceptReply := attribute.String("disposition", "ACCEPTED")
	refunds0 := metricSum(t, "payment.refunds", reason)
	refundReplies0 := metricSum(t, "payment.order_replies", refundReply)
	acceptReplies0 := metricSum(t, "payment.order_replies", acceptReply)

	p := e.insert(t, 1, succeeded)
	e.handleTwice(t, reply(p, refundRequired, errx.ReasonOrderNotPayable))
	q := e.insert(t, 2, succeeded)
	e.handle(t, reply(q, accepted, ""))

	if got := metricSum(t, "payment.refunds", reason) - refunds0; got != 1 {
		t.Errorf("refunds{reason=ORDER_NOT_PAYABLE} increased by %d, want 1 (a duplicate reply must not count)", got)
	}
	if got := metricSum(t, "payment.order_replies", refundReply) - refundReplies0; got < 2 {
		t.Errorf("order_replies{REFUND_REQUIRED} increased by %d, want at least 2", got)
	}
	if got := metricSum(t, "payment.order_replies", acceptReply) - acceptReplies0; got < 1 {
		t.Errorf("order_replies{ACCEPTED} increased by %d, want at least 1", got)
	}
}
