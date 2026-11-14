package breaker_test

import (
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/palemoky/oppenheimer/pkg/breaker"
)

// 全局的 MeterProvider 是进程级的，在 TestMain 里设一次。各用例用自己独有的标签值（熔断器、开关、缓存的名字）
// 区分自己的数据点，不假设别的用例没有记过指标。
var reader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	os.Exit(m.Run())
}

// metricInt 返回名为 name、标签恰好是 attrs 的 int64 数据点（计数器的累计值或仪表的最新值）。
func metricInt(t testing.TB, name string, attrs ...attribute.KeyValue) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	want := attribute.NewSet(attrs...)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			var points []metricdata.DataPoint[int64]
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				points = d.DataPoints
			case metricdata.Gauge[int64]:
				points = d.DataPoints
			default:
				t.Fatalf("metric %s has unexpected data type %T", name, m.Data)
			}
			for _, dp := range points {
				if dp.Attributes.Equals(&want) {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

func uniqueName(prefix string) string { return prefix + "-" + rand.Text() }

func TestStateAndTransitionMetrics(t *testing.T) {
	t.Parallel()
	name := uniqueName("m")
	nameAttr := attribute.String("breaker", name)
	state := func() (int64, bool) { return metricInt(t, "breaker.state", nameAttr) }
	transitions := func(to string) int64 {
		v, _ := metricInt(t, "breaker.transitions", nameAttr, attribute.String("to", to))
		return v
	}

	now := time.Now()
	b := breaker.New(breaker.Config{Name: name, Failures: 2, Cooldown: 5 * time.Second, Now: func() time.Time { return now }})

	// 一建出来就有一条“关闭”的线，转换次数还没有。
	if v, ok := state(); !ok || v != int64(breaker.Closed) {
		t.Fatalf("state after New = %d, %v; want 0, true", v, ok)
	}
	for _, to := range []string{"open", "half-open", "closed"} {
		if got := transitions(to); got != 0 {
			t.Fatalf("transitions to %s after New = %d, want 0", to, got)
		}
	}

	// 一次失败还不够。
	call(b, breaker.Failure)
	if v, _ := state(); v != int64(breaker.Closed) {
		t.Fatalf("state after 1 failure = %d, want closed (0)", v)
	}

	call(b, breaker.Failure)
	if v, _ := state(); v != int64(breaker.Open) || transitions("open") != 1 {
		t.Fatalf("after 2 failures: state = %d, to=open = %d; want 1, 1", v, transitions("open"))
	}

	// 冷却之后第一次 Allow 进入半开。
	now = now.Add(5 * time.Second)
	probe, err := b.Allow()
	if err != nil {
		t.Fatalf("probe not allowed: %v", err)
	}
	if v, _ := state(); v != int64(breaker.HalfOpen) || transitions("half-open") != 1 {
		t.Fatalf("in half-open: state = %d, to=half-open = %d; want 2, 1", v, transitions("half-open"))
	}

	// 探测失败：回到打开，到“打开”的转换是第二次。
	probe(breaker.Failure)
	if v, _ := state(); v != int64(breaker.Open) || transitions("open") != 2 {
		t.Fatalf("after failed probe: state = %d, to=open = %d; want 1, 2", v, transitions("open"))
	}

	now = now.Add(5 * time.Second)
	probe, err = b.Allow()
	if err != nil {
		t.Fatalf("second probe not allowed: %v", err)
	}
	probe(breaker.Success)
	if v, _ := state(); v != int64(breaker.Closed) || transitions("closed") != 1 {
		t.Fatalf("after successful probe: state = %d, to=closed = %d; want 0, 1", v, transitions("closed"))
	}
	if got := transitions("half-open"); got != 2 {
		t.Errorf("to=half-open = %d, want 2", got)
	}
}

// 没有状态变化时不增加转换计数：被拒绝的调用、成功的调用都不算。
func TestNoTransitionWhenStateUnchanged(t *testing.T) {
	t.Parallel()
	name := uniqueName("steady")
	b := breaker.New(breaker.Config{Name: name, Failures: 1})
	for range 5 {
		call(b, breaker.Success)
	}
	call(b, breaker.Failure) // 打开
	for range 5 {
		if _, err := b.Allow(); err == nil {
			t.Fatal("open breaker allowed a call")
		}
	}
	if v, _ := metricInt(t, "breaker.transitions", attribute.String("breaker", name), attribute.String("to", "open")); v != 1 {
		t.Errorf("to=open = %d, want 1", v)
	}
}

// 两个熔断器的数据互不干扰。
func TestMetricsAreSeparatePerBreaker(t *testing.T) {
	t.Parallel()
	a, bName := uniqueName("a"), uniqueName("b")
	ba := breaker.New(breaker.Config{Name: a, Failures: 1})
	_ = breaker.New(breaker.Config{Name: bName, Failures: 1})
	call(ba, breaker.Failure)
	if v, _ := metricInt(t, "breaker.state", attribute.String("breaker", a)); v != int64(breaker.Open) {
		t.Errorf("state of %s = %d, want open", a, v)
	}
	if v, _ := metricInt(t, "breaker.state", attribute.String("breaker", bName)); v != int64(breaker.Closed) {
		t.Errorf("state of %s = %d, want closed", bName, v)
	}
}
