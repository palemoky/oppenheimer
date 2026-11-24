package totals

import (
	"context"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// 全局的 MeterProvider 是进程级的，只在这里设一次。指标是包级的、各用例共用，所以用到它的用例不并行，
// 计数只看差值。
var metricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	otel.SetMeterProvider(mp)
	code := m.Run()
	_ = mp.Shutdown(context.Background())
	os.Exit(code)
}

// metric 返回名为 name、标签包含 want 的 Int64 数据点的值；found 表示有这样的数据点。
func metricValue(t *testing.T, name string, want map[string]string) (v int64, found bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	match := func(set attribute.Set) bool {
		for k, val := range want {
			if got, ok := set.Value(attribute.Key(k)); !ok || got.String() != val {
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
						v, found = v+dp.Value, true
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						v, found = v+dp.Value, true
					}
				}
			}
		}
	}
	return v, found
}

// 一轮成功之前不报瞬时值（没有序列不等于 0）；成功之后报的是那一轮的结果；之后一轮失败时计一次 error，
// 瞬时值停在上一次成功的结果上。
func TestWatcherReportsMetrics(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	e := newEnv(t)
	e.add(0, 100, payAcceptedRow, orderAccepted)
	e.add(0, 250, payAcceptedRow, orderNone) // 差一笔，250 分
	e.add(1, 600, payUnacked, orderAccepted) // 在途一笔，这一段从第 1 分钟起没封口

	now := e.base.Add(4 * time.Minute)
	w, err := NewWatcher(Config{Sources: e.src, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	unregister, err := w.registerGauges()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unregister)

	if _, found := metricValue(t, "recon.totals.mismatch", nil); found {
		t.Fatal("recon.totals.mismatch reported before any successful check")
	}

	okBefore, _ := metricValue(t, "recon.totals.runs", map[string]string{"outcome": "ok"})
	w.round(t.Context())
	okAfter, _ := metricValue(t, "recon.totals.runs", map[string]string{"outcome": "ok"})
	if okAfter-okBefore != 1 {
		t.Errorf("runs{outcome=ok} grew by %d, want 1", okAfter-okBefore)
	}
	for _, c := range []struct {
		name  string
		attrs map[string]string
		want  int64
	}{
		{"recon.totals.mismatch", map[string]string{"kind": "count", "group": "succeeded"}, 1},
		{"recon.totals.mismatch", map[string]string{"kind": "amount", "group": "succeeded"}, 250},
		// 没有差异的那一组也报，值是 0：有序列才分得清“没有差异”和“没在核对”。
		{"recon.totals.mismatch", map[string]string{"kind": "count", "group": "refunded"}, 0},
		{"recon.totals.mismatch", map[string]string{"kind": "amount", "group": "refunded"}, 0},
		{"recon.totals.count", map[string]string{"side": "payment"}, 2},
		{"recon.totals.count", map[string]string{"side": "order"}, 1},
		{"recon.totals.amount", map[string]string{"side": "payment"}, 350},
		{"recon.totals.amount", map[string]string{"side": "order"}, 100},
		{"recon.totals.in_flight", nil, 1},
		{"recon.totals.open_buckets", nil, 1},
		// 没封口的那一段从第 1 分钟开始，现在是第 4 分钟。
		{"recon.totals.oldest_open_age", nil, 180},
	} {
		if got, found := metricValue(t, c.name, c.attrs); !found || got != c.want {
			t.Errorf("%s%v = %d (found %v), want %d", c.name, c.attrs, got, found, c.want)
		}
	}

	// 库不可用：关掉支付库的连接，下一轮失败。
	if err := e.src.Payment.Close(); err != nil {
		t.Fatal(err)
	}
	errBefore, _ := metricValue(t, "recon.totals.runs", map[string]string{"outcome": "error"})
	w.round(t.Context())
	errAfter, _ := metricValue(t, "recon.totals.runs", map[string]string{"outcome": "error"})
	if errAfter-errBefore != 1 {
		t.Errorf("runs{outcome=error} grew by %d, want 1", errAfter-errBefore)
	}
	if got, _ := metricValue(t, "recon.totals.mismatch", map[string]string{"kind": "amount"}); got != 250 {
		t.Errorf("mismatch amount after a failed check = %d, want the last successful value 250", got)
	}
}

func TestNewWatcherValidates(t *testing.T) {
	t.Parallel()
	if _, err := NewWatcher(Config{}); err == nil {
		t.Error("NewWatcher accepted empty sources")
	}
	e := newEnv(t)
	if _, err := NewWatcher(Config{Sources: e.src, Window: time.Second}); err == nil {
		t.Error("NewWatcher accepted a window shorter than one bucket")
	}
}
