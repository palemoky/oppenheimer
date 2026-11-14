package apply_test

import (
	"context"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/palemoky/oppenheimer/services/inventory/internal/store"
)

// metricReader 读本包测试进程里的全部指标。全局的 MeterProvider 是进程级状态，只在 TestMain 里设一次。
var metricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	otel.SetMeterProvider(mp)
	code := m.Run()
	_ = mp.Shutdown(context.Background())
	os.Exit(code)
}

// gaugeValue 返回名为 name 的 Int64 瞬时值的数据点之和；found 表示有数据点。
func gaugeValue(t *testing.T, name string) (v int64, found bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("metric %s has unexpected data type %T", name, m.Data)
			}
			for _, dp := range g.DataPoints {
				v, found = v+dp.Value, true
			}
		}
	}
	return v, found
}

// 应用者定期数 stock_pending 的行数：数到的和表里的行数一致，不是每一轮都数，不新鲜了就不上报。
// 只有这一个用例登记这个指标（多个用例同时登记的话数据点会被加在一起），所以可以和别的用例并行。
func TestBacklogMetric(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.addSKU(t, 1, 1000)
	a := f.applier(t, 1000)
	unregister, err := a.RegisterMetrics()
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	pend := func(n int, firstOrder int64) {
		t.Helper()
		for i := range n {
			f.pend(t.Context(), t, func(tx *store.Tx) error {
				return tx.PendReserve(t.Context(), firstOrder+int64(i), 1, 1)
			})
		}
	}
	want := func(rows int64) {
		t.Helper()
		if got, ok := a.Backlog(); !ok || got != rows {
			t.Fatalf("Backlog = %d (ok %v), want %d", got, ok, rows)
		}
		if got, found := gaugeValue(t, "inventory.stock_pending.backlog"); !found || got != rows {
			t.Fatalf("inventory.stock_pending.backlog = %d (found %v), want %d", got, found, rows)
		}
	}

	if _, found := gaugeValue(t, "inventory.stock_pending.backlog"); found {
		t.Fatal("backlog reported before the applier counted it")
	}
	// 一轮里先数（4 行）再应用：数到的是这一轮开始时的积压。
	pend(4, 1)
	if err := a.Round(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := f.pendingRows(t); n != 0 {
		t.Fatalf("%d pending rows left after a round", n)
	}
	want(4)

	// 5 秒内的下一轮不数，值不变。
	pend(2, 100)
	f.advance(4 * time.Second)
	if err := a.Round(t.Context()); err != nil {
		t.Fatal(err)
	}
	want(4)

	// 过了 5 秒再数。
	pend(3, 200)
	f.advance(time.Second)
	if err := a.Round(t.Context()); err != nil {
		t.Fatal(err)
	}
	want(3)
	if n := f.pendingRows(t); n != 0 {
		t.Fatalf("%d pending rows left after a round", n)
	}

	// 应用者不再跑（卡住或停了）：过一会儿不上报，而不是一直报那个旧值。
	f.advance(21 * time.Second)
	if got, ok := a.Backlog(); ok {
		t.Fatalf("Backlog = %d still reported after the applier stopped counting", got)
	}
	if got, found := gaugeValue(t, "inventory.stock_pending.backlog"); found {
		t.Fatalf("inventory.stock_pending.backlog = %d still reported after the applier stopped counting", got)
	}
}
