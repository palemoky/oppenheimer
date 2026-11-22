package outbox

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// 指标建在包级变量里，用的是全局的 meter，所以这里只能设全局的 MeterProvider（进程级状态，只在 TestMain 里设一次）。
// 用例可能并行、共用同一个直方图，各用例用自己独有的 topic 找自己的数据点。
var metricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	otel.SetMeterProvider(mp)
	code := m.Run()
	_ = mp.Shutdown(context.Background())
	os.Exit(code)
}

// delayPoint 返回 outbox.relay.delay 在 topic 标签下的数据点，没有时 ok 为 false。
func delayPoint(t *testing.T, topic string) (dp metricdata.HistogramDataPoint[float64], ok bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	want := attribute.NewSet(attribute.String("messaging.destination.name", topic))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "outbox.relay.delay" {
				continue
			}
			h, isHist := m.Data.(metricdata.Histogram[float64])
			if !isHist {
				t.Fatalf("outbox.relay.delay is %T, want Histogram[float64]", m.Data)
			}
			for _, p := range h.DataPoints {
				if p.Attributes.Equals(&want) {
					return p, true
				}
			}
		}
	}
	return dp, false
}

// 年龄用数据库的时钟算：把 created_at 改成数据库此刻的 5 秒之前，记下来的就是 5 秒多一点。
func TestFlushRecordsRelayDelayFromDatabaseClock(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)

	const topic = "outbox-delay-ok"
	commitAdd(t, db, Message{Topic: topic, Key: []byte("k"), Payload: []byte("p")})
	if _, err := db.ExecContext(t.Context(),
		"UPDATE outbox SET created_at = CURRENT_TIMESTAMP(6) - INTERVAL 5 SECOND"); err != nil {
		t.Fatal(err)
	}

	if n := mustFlush(t, r); n != 1 {
		t.Fatalf("published %d, want 1", n)
	}
	dp, ok := delayPoint(t, topic)
	if !ok {
		t.Fatal("no outbox.relay.delay data point for the topic")
	}
	if dp.Count != 1 {
		t.Fatalf("count = %d, want 1", dp.Count)
	}
	// 上限给得宽：CI 上数据库和中继之间可能慢，但绝不会到 5 秒那么多。
	if dp.Sum < 5 || dp.Sum >= 7 {
		t.Errorf("delay = %.3fs, want in [5, 7)", dp.Sum)
	}
}

// 发送失败的行还在表里，不记；下一轮发出去了才记，而且只记一次。
func TestFlushDoesNotRecordRelayDelayForFailedRows(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)

	const okTopic, badTopic = "outbox-delay-mixed-ok", "outbox-delay-mixed-bad"
	commitAdd(t, db,
		Message{Topic: okTopic, Key: []byte("a"), Payload: []byte("good")},
		Message{Topic: badTopic, Key: []byte("b"), Payload: []byte("bad")},
	)
	s.set(func(f *fakeSender) { f.failIf = failPayload("bad") })

	if _, err := r.Flush(t.Context()); err == nil {
		t.Fatal("Flush succeeded, want an error for the failed row")
	}
	if dp, ok := delayPoint(t, okTopic); !ok || dp.Count != 1 {
		t.Errorf("ok topic: data point %v (found %v), want count 1", dp.Count, ok)
	}
	if dp, ok := delayPoint(t, badTopic); ok && dp.Count != 0 {
		t.Errorf("failed topic recorded %d times, want 0", dp.Count)
	}

	s.set(func(f *fakeSender) { f.failIf = nil })
	if n := mustFlush(t, r); n != 1 {
		t.Fatalf("second flush published %d, want 1", n)
	}
	if dp, ok := delayPoint(t, badTopic); !ok || dp.Count != 1 {
		t.Errorf("retried topic: count %d (found %v), want 1", dp.Count, ok)
	}
}
