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

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
)

// metricReader 读本包测试进程里的全部指标。全局的 MeterProvider 是进程级状态，只在 TestMain 里设一次；
// 用例并行跑、共用这些计数，所以断言只看差值（“至少增加了多少”），不看绝对值。
var metricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	otel.SetMeterProvider(mp)
	code := m.Run()
	_ = mp.Shutdown(context.Background())
	os.Exit(code)
}

// collect 读一次全部指标。
func collect(t *testing.T) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	return rm
}

// matches 判断数据点的标签里是否包含 want 的全部键值。
func matches(set attribute.Set, want map[string]string) bool {
	for k, v := range want {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.String() != v {
			return false
		}
	}
	return true
}

// metricValue 返回名为 name 的 Int64 计数（Sum）或瞬时值（Gauge）里标签匹配 want 的数据点之和，
// 直方图（Float64）返回匹配的数据点的记录次数之和。found 表示有匹配的数据点。
func metricValue(t *testing.T, name string, want map[string]string) (v int64, found bool) {
	t.Helper()
	for _, sm := range collect(t).ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					if matches(dp.Attributes, want) {
						v, found = v+dp.Value, true
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					if matches(dp.Attributes, want) {
						v, found = v+dp.Value, true
					}
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					if matches(dp.Attributes, want) {
						v, found = v+int64(dp.Count), true //nolint:gosec // 测试里的计数很小
					}
				}
			default:
				t.Fatalf("metric %s has unexpected data type %T", name, m.Data)
			}
		}
	}
	return v, found
}

// counterDelta 执行 fn，返回 name 里标签匹配 want 的计数在 fn 前后的差。
func counterDelta(t *testing.T, name string, want map[string]string, fn func()) int64 {
	t.Helper()
	before, _ := metricValue(t, name, want)
	fn()
	after, _ := metricValue(t, name, want)
	return after - before
}

func TestMetricsAdmission(t *testing.T) {
	t.Parallel()
	e := newEnv(t)

	if d := counterDelta(t, "order.admissions", map[string]string{"outcome": admissionAdmitted}, func() {
		e.submit(t, 1, "metrics-ok", items(skuA, 1))
	}); d < 1 {
		t.Errorf("admitted delta = %d, want >= 1", d)
	}
	if d := counterDelta(t, "order.admissions", map[string]string{"outcome": admissionInsufficientStock}, func() {
		_, err := e.svc.SubmitOrder(t.Context(), submitReq(2, "metrics-scarce", items(skuScarce, 2)))
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonInsufficientStock)
	}); d < 1 {
		t.Errorf("insufficient_stock delta = %d, want >= 1", d)
	}
}

func TestMetricsPersistAndTransition(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 3, "metrics-land", items(skuA, 1))

	persisted := map[string]string{"outcome": persistPendingPayment}
	transitioned := map[string]string{"from": "creating", "to": "pending_payment"}
	p0, _ := metricValue(t, "order.persists", persisted)
	d0, _ := metricValue(t, "order.persist.delay", persisted)
	t0, _ := metricValue(t, "order.transitions", transitioned)
	e.mustLand(t, o.GetOrderId())
	p1, _ := metricValue(t, "order.persists", persisted)
	d1, _ := metricValue(t, "order.persist.delay", persisted)
	t1, _ := metricValue(t, "order.transitions", transitioned)
	if p1-p0 < 1 || d1-d0 < 1 || t1-t0 < 1 {
		t.Errorf("persists/delay/transitions delta = %d/%d/%d, want >= 1 each", p1-p0, d1-d0, t1-t0)
	}
}

func TestMetricsRead(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 4, "metrics-read")

	want := map[string]string{"source": "primary", "mode": ReadPrimary.String(), "partial": "false"}
	if d := counterDelta(t, "order.reads", want, func() {
		mustGetStatus(t, e.svc, o.GetOrderId(), 4, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	}); d < 1 {
		t.Errorf("reads delta = %d, want >= 1", d)
	}
}

// 名额的在用数和拒绝数按名字登记，unregister 之后不再导出。
func TestMetricsReadLimits(t *testing.T) {
	t.Parallel()
	svc, err := New(Deps{}, Config{ArchiveMaxConcurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	unregister, err := svc.RegisterLimitMetrics()
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		release, err := acquire(svc.limits.cold)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(release)
	}
	if _, err := acquire(svc.limits.cold); err == nil {
		t.Fatal("third acquire succeeded, want read busy")
	}
	name := map[string]string{"name": "cold"}
	if v, ok := metricValue(t, "order.bulkhead.in_flight", name); !ok || v != 2 {
		t.Errorf("in_flight = %d (found %v), want 2", v, ok)
	}
	if v, ok := metricValue(t, "order.bulkhead.rejected", name); !ok || v != 1 {
		t.Errorf("rejected = %d (found %v), want 1", v, ok)
	}
	unregister()
	if _, ok := metricValue(t, "order.bulkhead.in_flight", name); ok {
		t.Error("in_flight still exported after unregister")
	}
}

// 受理路径上返回的每个原因码都映射到一个登记过的结局，标签取值不会是空的选项。
func TestAdmissionOutcomesAreRegistered(t *testing.T) {
	t.Parallel()
	for reason, outcome := range admissionByReason {
		if admissionOpts[outcome] == nil {
			t.Errorf("reason %s maps to unregistered outcome %q", reason, outcome)
		}
	}
	direct := []string{
		admissionAdmitted, admissionDuplicate, admissionDuplicateAbandoned,
		admissionCanceled, admissionDeadlineExceeded, admissionUnavailable, admissionInternal,
	}
	for _, outcome := range direct {
		if admissionOpts[outcome] == nil {
			t.Errorf("outcome %q not registered", outcome)
		}
	}
}
