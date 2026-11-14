package capture

import (
	"context"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/testdb"
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

// cdc.events 按流水线和 op 计数（全量是 read）；读完带提交时间的事务后 cdc.lag_ms 有读数。
// 别的用例并行运行、流水线名相同，只断言“至少增加了”。
func TestPipelineMetrics(t *testing.T) {
	t.Parallel()
	e := newEnv(t, "orders_2")
	insertOrder(t, e.db, "orders_2", 501) // 全量读到

	pipe := attribute.String("pipeline", "order-test")
	ops := []string{"read", "insert", "update", "delete"}
	before := map[string]int64{}
	for _, op := range ops {
		before[op] = metricSum(t, "cdc.events", pipe, attribute.String("op", op))
	}

	p := e.pipeline(serverIDs.Add(1))
	r := e.start(p)
	e.waitStreaming(r)
	c := e.collector()
	insertOrder(t, e.db, "orders_2", 502)
	testdb.MustExec(t, e.db, "UPDATE orders_2 SET status = 3, version = version + 1 WHERE order_id = 502")
	testdb.MustExec(t, e.db, "DELETE FROM orders_2 WHERE order_id = 501")
	c.waitFor("all changes", func(ms []*cdcv1.RowChange) bool {
		return find(ms, 501, cdcv1.Op_OP_DELETE) != nil
	})

	for _, op := range ops {
		if got := metricSum(t, "cdc.events", pipe, attribute.String("op", op)) - before[op]; got < 1 {
			t.Errorf("events{op=%s} increased by %d, want at least 1", op, got)
		}
	}

	// 提交时间在事务的终点（XID）记下，比消息到达 collector 稍晚一点。
	deadline := time.Now().Add(10 * time.Second)
	for p.lastCommitMicros.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.lastCommitMicros.Load() == 0 {
		t.Fatal("no commit time recorded after streaming transactions")
	}
	if lag := p.lagMillis(); lag < 0 || lag > time.Minute.Milliseconds() {
		t.Errorf("lag = %d ms, want a small non-negative number", lag)
	}
	if _, n := metricValue(t, "cdc.lag_ms", pipe); n == 0 {
		t.Error("cdc.lag_ms has no data point while the pipeline is running")
	}
	if err := r.stop(t); err != nil {
		t.Errorf("Start returned %v after stop", err)
	}
}
