package degrade_test

import (
	"context"
	"crypto/rand"
	"os"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/palemoky/oppenheimer/pkg/degrade"
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

func switchOff(t *testing.T, name string) (int64, bool) {
	t.Helper()
	return metricInt(t, "degrade.switch.off", attribute.String("switch", name))
}

func TestSwitchOffMetric(t *testing.T) {
	t.Parallel()
	name := "m-" + rand.Text()
	s := newSet()
	s.Switch(name, "")

	// 登记时就报一次 0：没动过的开关在后端里也有一条线。
	if v, ok := switchOff(t, name); !ok || v != 0 {
		t.Fatalf("after registration = %d, %v; want 0, true", v, ok)
	}
	if err := s.SetMode(name, degrade.ModeOff); err != nil {
		t.Fatal(err)
	}
	if v, _ := switchOff(t, name); v != 1 {
		t.Errorf("after off = %d, want 1", v)
	}
	// 重复设成同一个值不改变读数。
	if err := s.SetMode(name, degrade.ModeOff); err != nil {
		t.Fatal(err)
	}
	if v, _ := switchOff(t, name); v != 1 {
		t.Errorf("after off twice = %d, want 1", v)
	}
	if err := s.SetMode(name, degrade.ModeAuto); err != nil {
		t.Fatal(err)
	}
	if v, _ := switchOff(t, name); v != 0 {
		t.Errorf("after auto = %d, want 0", v)
	}
}

func TestApplyReportsMetric(t *testing.T) {
	t.Parallel()
	a, b := "a-"+rand.Text(), "b-"+rand.Text()
	s := newSet()
	s.Switch(a, "")
	s.Switch(b, "")
	if err := s.Apply(a + "=off"); err != nil {
		t.Fatal(err)
	}
	if v, _ := switchOff(t, a); v != 1 {
		t.Errorf("%s = %d, want 1", a, v)
	}
	if v, _ := switchOff(t, b); v != 0 {
		t.Errorf("%s = %d, want 0", b, v)
	}
}

// 设置失败（名字不认识、模式不对）不改变读数，也不会凭空多出一条线。
func TestFailedSetModeDoesNotReport(t *testing.T) {
	t.Parallel()
	name, ghost := "f-"+rand.Text(), "ghost-"+rand.Text()
	s := newSet()
	s.Switch(name, "")
	if err := s.SetMode(ghost, degrade.ModeOff); err == nil {
		t.Fatal("unknown switch: want error")
	}
	if err := s.SetMode(name, "bogus"); err == nil {
		t.Fatal("bad mode: want error")
	}
	if _, ok := switchOff(t, ghost); ok {
		t.Error("a data point exists for an unknown switch")
	}
	if v, _ := switchOff(t, name); v != 0 {
		t.Errorf("after rejected SetMode = %d, want 0", v)
	}
}

// 并发改同一个开关之后，读数要和开关的实际状态一致。
//
// SetMode 在同一把锁里改值和报指标；分开做的话，两个并发的 SetMode 可能让读数和实际状态相反。
func TestSwitchOffMetricMatchesStateAfterConcurrentSets(t *testing.T) {
	t.Parallel()
	name := "c-" + rand.Text()
	s := newSet()
	sw := s.Switch(name, "")
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Go(func() {
			mode := degrade.ModeAuto
			if i%2 == 0 {
				mode = degrade.ModeOff
			}
			if err := s.SetMode(name, mode); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	want := int64(0)
	if sw.Off() {
		want = 1
	}
	if v, _ := switchOff(t, name); v != want {
		t.Errorf("metric = %d but the switch is off=%v", v, sw.Off())
	}
}
