package kafkax_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"maps"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// 全局的 provider 是进程级的，在 TestMain 里设一次。很多用例并行，recorder 里有所有用例的 span：
// 各用例用自己的 trace id、topic 找自己的 span，不假设里面只有自己的。
var (
	spans  = tracetest.NewSpanRecorder()
	reader = sdkmetric.NewManualReader()
)

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(spans),
	))
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	os.Exit(m.Run())
}

// ---- 找 span ----

// waitSpans 轮询到 recorder 里有 n 个满足 match 的已结束 span，返回它们（按结束顺序）。
func waitSpans(t *testing.T, what string, n int, match func(sdktrace.ReadOnlySpan) bool) []sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for {
		var out []sdktrace.ReadOnlySpan
		for _, s := range spans.Ended() {
			if match(s) {
				out = append(out, s)
			}
		}
		if len(out) >= n {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d span(s): %s (have %d)", n, what, len(out))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// topicSpans 返回 kind 类型、发往/来自 topic 的 span。topic 是每个用例独有的。
func topicSpans(t *testing.T, kind trace.SpanKind, topic string, n int) []sdktrace.ReadOnlySpan {
	t.Helper()
	return waitSpans(t, kind.String()+" spans of "+topic, n, func(s sdktrace.ReadOnlySpan) bool {
		return s.SpanKind() == kind && hasAttr(s, "messaging.destination.name", topic)
	})
}

func hasAttr(s sdktrace.ReadOnlySpan, key, want string) bool {
	return slices.ContainsFunc(s.Attributes(), func(kv attribute.KeyValue) bool {
		return string(kv.Key) == key && kv.Value.String() == want
	})
}

func attrInt(t *testing.T, s sdktrace.ReadOnlySpan, key string) int64 {
	t.Helper()
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsInt64()
		}
	}
	t.Fatalf("span %q has no attribute %s", s.Name(), key)
	return 0
}

func startSpan(t *testing.T, name string) (context.Context, trace.Span) {
	t.Helper()
	ctx, span := otel.Tracer("test").Start(t.Context(), name)
	t.Cleanup(func() { span.End() })
	return ctx, span
}

// spanRef 是 handler 里看到的 span 身份，通过通道交出来。
type spanRef struct {
	sc      trace.SpanContext
	headers map[string]string
}

// ---- 指标 ----

func collect(t *testing.T) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

// counterValue 返回计数器 name 在 attrs 标签下的累计值，没有数据点时为 0。
func counterValue(t *testing.T, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	m, ok := findMetric(collect(t), name)
	if !ok {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %s is %T, want Sum[int64]", name, m.Data)
	}
	want := attribute.NewSet(attrs...)
	for _, dp := range sum.DataPoints {
		if dp.Attributes.Equals(&want) {
			return dp.Value
		}
	}
	return 0
}

// histogramCount 返回直方图 name 在 attrs 标签下的记录次数。
func histogramCount(t *testing.T, name string, attrs ...attribute.KeyValue) uint64 {
	t.Helper()
	m, ok := findMetric(collect(t), name)
	if !ok {
		return 0
	}
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("metric %s is %T, want Histogram[float64]", name, m.Data)
	}
	want := attribute.NewSet(attrs...)
	for _, dp := range h.DataPoints {
		if dp.Attributes.Equals(&want) {
			return dp.Count
		}
	}
	return 0
}

// consumerAttrs 是消费者指标的标签：topic 和消费组。
func consumerAttrs(e env, extra ...attribute.KeyValue) []attribute.KeyValue {
	return append([]attribute.KeyValue{
		attribute.String("messaging.destination.name", e.topic),
		attribute.String("messaging.consumer.group.name", e.group),
	}, extra...)
}

// ---- 用例 ----

// 在一个 span 里 Send：PRODUCER span 是它的子 span，handler 的 span 是 PRODUCER 的子 span，三个在同一条 trace 里。
func TestSendAndConsumeShareOneTrace(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)

	got := make(chan spanRef, 4)
	startConsumer(t, e.consumerConfig(t), func(ctx context.Context, m kafkax.Message) error {
		got <- spanRef{sc: trace.SpanContextFromContext(ctx), headers: m.Headers}
		return nil
	})

	ctx, sender := startSpan(t, "sender")
	if _, err := p.Send(ctx, kafkax.Record{Topic: e.topic, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	sender.End()

	var ref spanRef
	select {
	case ref = <-got:
	case <-time.After(waitTimeout):
		t.Fatal("handler was not called")
	}

	prod := topicSpans(t, trace.SpanKindProducer, e.topic, 1)[0]
	cons := topicSpans(t, trace.SpanKindConsumer, e.topic, 1)[0]

	traceID := sender.SpanContext().TraceID()
	if prod.SpanContext().TraceID() != traceID || cons.SpanContext().TraceID() != traceID {
		t.Errorf("trace ids: sender %s, producer %s, consumer %s; want all equal", traceID, prod.SpanContext().TraceID(), cons.SpanContext().TraceID())
	}
	if prod.Parent().SpanID() != sender.SpanContext().SpanID() {
		t.Errorf("PRODUCER parent = %s, want the sender span %s", prod.Parent().SpanID(), sender.SpanContext().SpanID())
	}
	if cons.Parent().SpanID() != prod.SpanContext().SpanID() {
		t.Errorf("CONSUMER parent = %s, want the PRODUCER span %s", cons.Parent().SpanID(), prod.SpanContext().SpanID())
	}
	if len(cons.Links()) != 0 {
		t.Errorf("CONSUMER span has %d links, want 0 (parent/child, not link)", len(cons.Links()))
	}
	// handler 的 ctx 里的 span 就是 CONSUMER span。
	if ref.sc.SpanID() != cons.SpanContext().SpanID() || ref.sc.TraceID() != traceID {
		t.Errorf("handler ctx span = %s/%s, want the CONSUMER span %s/%s", ref.sc.TraceID(), ref.sc.SpanID(), traceID, cons.SpanContext().SpanID())
	}
	// header 里的 traceparent 是 PRODUCER span 的，不是发送方的。
	back := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier(ref.headers)))
	if back.SpanID() != prod.SpanContext().SpanID() {
		t.Errorf("traceparent header span = %s, want the PRODUCER span %s", back.SpanID(), prod.SpanContext().SpanID())
	}

	if cons.Status().Code == otelcodes.Error {
		t.Errorf("CONSUMER span of a successful message has status %v", cons.Status())
	}
	if !hasAttr(prod, "messaging.system", "kafka") || !hasAttr(cons, "messaging.system", "kafka") {
		t.Error("spans lack messaging.system=kafka")
	}
	if !hasAttr(cons, "messaging.consumer.group.name", e.group) {
		t.Errorf("CONSUMER span lacks group %s", e.group)
	}
	if !hasAttr(cons, "kafkax.attempt", "1") {
		t.Error("CONSUMER span lacks kafkax.attempt=1")
	}
}

// 一次 Send 多条记录：每条一个 PRODUCER span，各带自己的 traceparent。
func TestSendCreatesOneProducerSpanPerRecord(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	rec := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	})

	ctx, sender := startSpan(t, "sender")
	const n = 3
	if _, err := p.Send(ctx, withTopic(e.topic, values("m", n))...); err != nil {
		t.Fatal(err)
	}
	sender.End()
	waitFor(t, "messages", func() bool { return rec.len() >= n })

	prods := topicSpans(t, trace.SpanKindProducer, e.topic, n)
	ids := make(map[trace.SpanID]bool)
	for _, s := range prods {
		ids[s.SpanContext().SpanID()] = true
	}
	if len(ids) != n {
		t.Errorf("%d distinct PRODUCER spans, want %d", len(ids), n)
	}
	for _, m := range rec.all() {
		sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier(m.Headers)))
		if !ids[sc.SpanID()] {
			t.Errorf("message %s has traceparent span %s which is not one of the PRODUCER spans", m.Value, sc.SpanID())
		}
	}
}

// Send 不改调用方传进来的 Headers map（它可能被重发时复用）。
func TestSendDoesNotMutateCallerHeaders(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	ctx, sender := startSpan(t, "sender")
	defer sender.End()
	headers := map[string]string{"x": "1"}
	mustSend(t, p, kafkax.Record{Topic: e.topic, Value: []byte("v"), Headers: headers})
	if _, err := p.Send(ctx, kafkax.Record{Topic: e.topic, Value: []byte("v"), Headers: headers}); err != nil {
		t.Fatal(err)
	}
	if len(headers) != 1 || headers["x"] != "1" {
		t.Errorf("caller headers = %v, want them untouched", headers)
	}
}

// 第一次失败、第二次成功：每次尝试一个 CONSUMER span。
func TestEachAttemptGetsItsOwnSpan(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)

	var calls atomic.Int64
	done := make(chan struct{})
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, _ kafkax.Message) error {
		if calls.Add(1) == 1 {
			return errors.New("transient")
		}
		close(done)
		return nil
	})
	ctx, sender := startSpan(t, "sender")
	if _, err := p.Send(ctx, kafkax.Record{Topic: e.topic, Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	sender.End()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("handler never succeeded")
	}

	cons := topicSpans(t, trace.SpanKindConsumer, e.topic, 2)
	if len(cons) != 2 {
		t.Fatalf("%d CONSUMER spans, want 2", len(cons))
	}
	slices.SortFunc(cons, func(a, b sdktrace.ReadOnlySpan) int {
		return int(attrInt(t, a, "kafkax.attempt") - attrInt(t, b, "kafkax.attempt"))
	})
	for i, s := range cons {
		if got := attrInt(t, s, "kafkax.attempt"); got != int64(i+1) {
			t.Errorf("span %d: kafkax.attempt = %d, want %d", i, got, i+1)
		}
		// 每次尝试都接在 PRODUCER 下面，同一条链路。
		if s.SpanContext().TraceID() != sender.SpanContext().TraceID() {
			t.Errorf("attempt %d is in another trace", i+1)
		}
	}
	if cons[0].Status().Code != otelcodes.Error {
		t.Errorf("first attempt status = %v, want Error", cons[0].Status())
	}
	if cons[1].Status().Code == otelcodes.Error {
		t.Errorf("second attempt status = %v, want not Error", cons[1].Status())
	}
	if cons[0].Parent().SpanID() != cons[1].Parent().SpanID() {
		t.Error("the two attempts do not share a parent (the PRODUCER span)")
	}
	if len(cons[0].Events()) == 0 {
		t.Error("the failed attempt has no recorded error event")
	}

	// 指标：一次失败（error）、一次成功（ok），一次重试。
	if got := histogramCount(t, "kafkax.consumer.process.duration", consumerAttrs(e, attribute.String("outcome", "error"))...); got != 1 {
		t.Errorf("process.duration outcome=error count = %d, want 1", got)
	}
	if got := histogramCount(t, "kafkax.consumer.process.duration", consumerAttrs(e, attribute.String("outcome", "ok"))...); got != 1 {
		t.Errorf("process.duration outcome=ok count = %d, want 1", got)
	}
	if got := counterValue(t, "kafkax.consumer.retries", consumerAttrs(e)...); got != 1 {
		t.Errorf("retries = %d, want 1", got)
	}
	if got := counterValue(t, "kafkax.consumer.dead_lettered", consumerAttrs(e)...); got != 0 {
		t.Errorf("dead_lettered = %d, want 0", got)
	}
}

// 处理成功一条：process.duration 有 outcome=ok 的数据点，生产者的耗时也有。
func TestSuccessfulMessageRecordsDurations(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	rec := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	})
	mustSend(t, p, kafkax.Record{Topic: e.topic, Value: []byte("v")})
	waitFor(t, "message", func() bool { return rec.len() >= 1 })

	// handler 返回之后才记 process.duration，轮询到它出现。
	waitFor(t, "process.duration", func() bool {
		return histogramCount(t, "kafkax.consumer.process.duration", consumerAttrs(e, attribute.String("outcome", "ok"))...) == 1
	})
	if got := histogramCount(t, "kafkax.producer.send.duration",
		attribute.String("messaging.destination.name", e.topic), attribute.String("outcome", "ok")); got != 1 {
		t.Errorf("producer.send.duration outcome=ok count = %d, want 1", got)
	}
	for _, name := range []string{"kafkax.consumer.queue.duration", "kafkax.consumer.delivery.duration"} {
		waitFor(t, name, func() bool { return histogramCount(t, name, consumerAttrs(e)...) == 1 })
	}
	if got := counterValue(t, "kafkax.consumer.retries", consumerAttrs(e)...); got != 0 {
		t.Errorf("retries = %d, want 0", got)
	}
}

// 进死信：header 里仍然带着原来的 traceparent；dead_lettered 增加；outcome=permanent_error。
func TestDeadLetterKeepsTraceparent(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	startConsumer(t, e.consumerConfig(t), func(context.Context, kafkax.Message) error {
		return kafkax.Permanent(errors.New("bad message"))
	})

	ctx, sender := startSpan(t, "sender")
	if _, err := p.Send(ctx, kafkax.Record{Topic: e.topic, Value: []byte("poison")}); err != nil {
		t.Fatal(err)
	}
	sender.End()

	dead := readAll(t, e.brokers, e.dlq, 1)[0]
	var tp string
	for _, h := range dead.Headers {
		if h.Key == kafkax.TraceparentHeader {
			tp = string(h.Value)
		}
	}
	if tp == "" {
		t.Fatalf("dead letter has no %s header: %v", kafkax.TraceparentHeader, dead.Headers)
	}
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": tp}))
	if sc.TraceID() != sender.SpanContext().TraceID() {
		t.Errorf("dead letter traceparent trace id = %s, want %s", sc.TraceID(), sender.SpanContext().TraceID())
	}
	prod := topicSpans(t, trace.SpanKindProducer, e.topic, 1)[0]
	if sc.SpanID() != prod.SpanContext().SpanID() {
		t.Errorf("dead letter traceparent span id = %s, want the original PRODUCER span %s", sc.SpanID(), prod.SpanContext().SpanID())
	}

	if got := counterValue(t, "kafkax.consumer.dead_lettered", consumerAttrs(e)...); got != 1 {
		t.Errorf("dead_lettered = %d, want 1", got)
	}
	if got := counterValue(t, "kafkax.consumer.retries", consumerAttrs(e)...); got != 0 {
		t.Errorf("retries = %d for a permanent error, want 0", got)
	}
	if got := histogramCount(t, "kafkax.consumer.process.duration", consumerAttrs(e, attribute.String("outcome", "permanent_error"))...); got != 1 {
		t.Errorf("process.duration outcome=permanent_error count = %d, want 1", got)
	}
}

// 重试用尽进死信：重试次数是 MaxAttempts-1，每次尝试一个 span。
func TestRetriesExhaustedCountsRetriesAndDeadLetter(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	cfg := e.consumerConfig(t)
	cfg.MaxAttempts = 3
	startConsumer(t, cfg, func(context.Context, kafkax.Message) error { return errors.New("always") })

	mustSend(t, p, kafkax.Record{Topic: e.topic, Value: []byte("v")})
	readAll(t, e.brokers, e.dlq, 1)

	if got := counterValue(t, "kafkax.consumer.retries", consumerAttrs(e)...); got != 2 {
		t.Errorf("retries = %d, want 2 (3 attempts)", got)
	}
	if got := counterValue(t, "kafkax.consumer.dead_lettered", consumerAttrs(e)...); got != 1 {
		t.Errorf("dead_lettered = %d, want 1", got)
	}
	cons := topicSpans(t, trace.SpanKindConsumer, e.topic, 3)
	if len(cons) != 3 {
		t.Errorf("%d CONSUMER spans, want 3", len(cons))
	}
	for _, s := range cons {
		if s.Status().Code != otelcodes.Error {
			t.Errorf("attempt %d status = %v, want Error", attrInt(t, s, "kafkax.attempt"), s.Status())
		}
	}
}

func newTraceparent(t *testing.T) (header string, sc trace.SpanContext) {
	t.Helper()
	var tid [16]byte
	var sid [8]byte
	if _, err := rand.Read(tid[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(sid[:]); err != nil {
		t.Fatal(err)
	}
	tid[0] |= 1 // 保证不是全零
	sid[0] |= 1
	header = "00-" + hex.EncodeToString(tid[:]) + "-" + hex.EncodeToString(sid[:]) + "-01"
	return header, trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, Remote: true})
}

// outbox 中继的路径：ctx 里没有 span，记录的 Headers 里已经有 traceparent。
// PRODUCER span 接在它下面，消费者再接在 PRODUCER 下面。
func TestHeaderTraceparentIsUsedWhenContextHasNoSpan(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	got := make(chan spanRef, 1)
	startConsumer(t, e.consumerConfig(t), func(ctx context.Context, m kafkax.Message) error {
		got <- spanRef{sc: trace.SpanContextFromContext(ctx), headers: m.Headers}
		return nil
	})

	header, origin := newTraceparent(t)
	mustSend(t, p, kafkax.Record{Topic: e.topic, Value: []byte("v"), Headers: map[string]string{kafkax.TraceparentHeader: header, "other": "x"}})

	var ref spanRef
	select {
	case ref = <-got:
	case <-time.After(waitTimeout):
		t.Fatal("handler was not called")
	}
	prod := topicSpans(t, trace.SpanKindProducer, e.topic, 1)[0]
	cons := topicSpans(t, trace.SpanKindConsumer, e.topic, 1)[0]

	if prod.SpanContext().TraceID() != origin.TraceID() || prod.Parent().SpanID() != origin.SpanID() {
		t.Errorf("PRODUCER trace/parent = %s/%s, want %s/%s from the header", prod.SpanContext().TraceID(), prod.Parent().SpanID(), origin.TraceID(), origin.SpanID())
	}
	if cons.SpanContext().TraceID() != origin.TraceID() || cons.Parent().SpanID() != prod.SpanContext().SpanID() {
		t.Errorf("CONSUMER trace/parent = %s/%s, want %s/%s", cons.SpanContext().TraceID(), cons.Parent().SpanID(), origin.TraceID(), prod.SpanContext().SpanID())
	}
	if ref.sc.TraceID() != origin.TraceID() {
		t.Errorf("handler span trace id = %s, want %s", ref.sc.TraceID(), origin.TraceID())
	}
	// header 被换成了 PRODUCER span 的；其他 header 原样保留。
	if ref.headers[kafkax.TraceparentHeader] == header {
		t.Error("the message still carries the original traceparent, want it replaced by the PRODUCER span's")
	}
	if ref.headers["other"] != "x" {
		t.Errorf("other header = %q, want it kept", ref.headers["other"])
	}
}

// ctx 里有 span 时，记录 Headers 里已有的 traceparent 不起作用：以 ctx 为准。
func TestContextSpanWinsOverHeaderTraceparent(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	header, origin := newTraceparent(t)
	ctx, sender := startSpan(t, "sender")
	if _, err := p.Send(ctx, kafkax.Record{Topic: e.topic, Value: []byte("v"), Headers: map[string]string{kafkax.TraceparentHeader: header}}); err != nil {
		t.Fatal(err)
	}
	sender.End()
	prod := topicSpans(t, trace.SpanKindProducer, e.topic, 1)[0]
	if prod.SpanContext().TraceID() == origin.TraceID() || prod.Parent().SpanID() != sender.SpanContext().SpanID() {
		t.Errorf("PRODUCER trace/parent = %s/%s, want the sender's %s/%s", prod.SpanContext().TraceID(), prod.Parent().SpanID(), sender.SpanContext().TraceID(), sender.SpanContext().SpanID())
	}
}

// 没有任何上下文：PRODUCER span 是新链路的根，消息仍然带 traceparent，消费者接在它下面。
func TestSendWithoutAnyContextStartsATrace(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	rec := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	})
	mustSend(t, p, kafkax.Record{Topic: e.topic, Value: []byte("v")})
	waitFor(t, "message", func() bool { return rec.len() >= 1 })
	prod := topicSpans(t, trace.SpanKindProducer, e.topic, 1)[0]
	if prod.Parent().IsValid() {
		t.Errorf("PRODUCER parent = %v, want none", prod.Parent())
	}
	cons := topicSpans(t, trace.SpanKindConsumer, e.topic, 1)[0]
	if cons.Parent().SpanID() != prod.SpanContext().SpanID() {
		t.Errorf("CONSUMER parent = %s, want PRODUCER %s", cons.Parent().SpanID(), prod.SpanContext().SpanID())
	}
}

// 一个消息的 traceparent 坏了（不是合法的 W3C 格式）：消费者照常处理，开一条新链路，不 panic、不卡住。
func TestMalformedTraceparentIsIgnored(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	rec := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	})
	sendToPartitionsWithHeaders(t, e.brokers, e.topic, []string{"v"}, func(int) map[string]string {
		return map[string]string{kafkax.TraceparentHeader: "garbage"}
	})
	waitFor(t, "message", func() bool { return rec.len() >= 1 })
	cons := topicSpans(t, trace.SpanKindConsumer, e.topic, 1)[0]
	if cons.Parent().IsValid() {
		t.Errorf("CONSUMER parent = %v, want none for a malformed traceparent", cons.Parent())
	}
	if !cons.SpanContext().IsValid() {
		t.Error("CONSUMER span is not valid")
	}
}

// 延迟消费者：开新链路，用 link 指向 PRODUCER span。
func TestDelayConsumerLinksInsteadOfParenting(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	got := make(chan spanRef, 1)
	startDelayConsumer(t, e.consumerConfig(t), nil, func(ctx context.Context, m kafkax.Message) error {
		got <- spanRef{sc: trace.SpanContextFromContext(ctx), headers: m.Headers}
		return nil
	})

	ctx, sender := startSpan(t, "sender")
	// 已经到期的延迟消息：立刻处理。
	rec := kafkax.NewDelayRecord(e.topic, []byte("o1"), []byte("v"), time.Now().Add(-time.Second))
	if _, err := p.Send(ctx, rec); err != nil {
		t.Fatal(err)
	}
	sender.End()

	var ref spanRef
	select {
	case ref = <-got:
	case <-time.After(waitTimeout):
		t.Fatal("handler was not called")
	}
	prod := topicSpans(t, trace.SpanKindProducer, e.topic, 1)[0]
	cons := topicSpans(t, trace.SpanKindConsumer, e.topic, 1)[0]

	if cons.SpanContext().TraceID() == sender.SpanContext().TraceID() {
		t.Error("delay CONSUMER span is in the sender's trace, want a new trace")
	}
	if cons.Parent().IsValid() {
		t.Errorf("delay CONSUMER parent = %v, want none (new root)", cons.Parent())
	}
	if ref.sc.SpanID() != cons.SpanContext().SpanID() {
		t.Errorf("handler span = %s, want the CONSUMER span %s", ref.sc.SpanID(), cons.SpanContext().SpanID())
	}
	links := cons.Links()
	if len(links) != 1 {
		t.Fatalf("%d links, want 1", len(links))
	}
	if links[0].SpanContext.SpanID() != prod.SpanContext().SpanID() || links[0].SpanContext.TraceID() != sender.SpanContext().TraceID() {
		t.Errorf("link = %s/%s, want the PRODUCER span %s/%s", links[0].SpanContext.TraceID(), links[0].SpanContext.SpanID(), sender.SpanContext().TraceID(), prod.SpanContext().SpanID())
	}
	// 延迟消费者的 delivery.duration 从到期时刻算起，是个非负数；数据点要有。
	waitFor(t, "delivery.duration", func() bool {
		return histogramCount(t, "kafkax.consumer.delivery.duration", consumerAttrs(e)...) == 1
	})
}

// 按批处理：整批一个 span，count 等于批大小，link 指向各条消息的 PRODUCER span。
func TestBatchConsumerOneSpanPerBatchWithLinks(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)

	var mu sync.Mutex
	var batches []struct {
		span trace.SpanContext
		size int
	}
	var total atomic.Int64
	const n = 5
	// 先发后启动消费者：消息已经都在 broker 上，第一次拉取多半就是一整批；不依赖这一点，按实际收到的批断言。
	ctx, sender := startSpan(t, "sender")
	if _, err := p.Send(ctx, withTopic(e.topic, values("b", n))...); err != nil {
		t.Fatal(err)
	}
	sender.End()
	startBatchConsumer(t, batchConfig(t, e, 100), func(ctx context.Context, msgs []kafkax.Message) error {
		mu.Lock()
		batches = append(batches, struct {
			span trace.SpanContext
			size int
		}{trace.SpanContextFromContext(ctx), len(msgs)})
		mu.Unlock()
		total.Add(int64(len(msgs)))
		return nil
	})
	waitFor(t, "all messages", func() bool { return total.Load() >= n })

	prods := topicSpans(t, trace.SpanKindProducer, e.topic, n)
	prodIDs := make(map[trace.SpanID]bool)
	for _, s := range prods {
		prodIDs[s.SpanContext().SpanID()] = true
	}

	mu.Lock()
	got := slices.Clone(batches)
	mu.Unlock()
	cons := topicSpans(t, trace.SpanKindConsumer, e.topic, len(got))
	byID := make(map[trace.SpanID]sdktrace.ReadOnlySpan)
	for _, s := range cons {
		byID[s.SpanContext().SpanID()] = s
	}
	linked := make(map[trace.SpanID]bool)
	for _, b := range got {
		s, ok := byID[b.span.SpanID()]
		if !ok {
			t.Fatalf("no CONSUMER span for the batch span %s", b.span.SpanID())
		}
		if c := attrInt(t, s, "messaging.batch.message_count"); c != int64(b.size) {
			t.Errorf("messaging.batch.message_count = %d, want the batch size %d", c, b.size)
		}
		if s.Parent().IsValid() {
			t.Errorf("batch span has parent %v, want a new root", s.Parent())
		}
		if len(s.Links()) > 32 {
			t.Errorf("batch span has %d links, want at most 32", len(s.Links()))
		}
		if len(s.Links()) != b.size {
			t.Errorf("batch of %d messages has %d links, want one per message", b.size, len(s.Links()))
		}
		for _, l := range s.Links() {
			if !prodIDs[l.SpanContext.SpanID()] {
				t.Errorf("link to %s, which is not one of the PRODUCER spans", l.SpanContext.SpanID())
			}
			linked[l.SpanContext.SpanID()] = true
		}
	}
	if len(linked) != n {
		t.Errorf("batches link to %d distinct PRODUCER spans, want %d", len(linked), n)
	}
}

// 一批超过 32 条消息时 link 最多 32 个。
func TestBatchConsumerCapsLinks(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	const n = 100
	ctx, sender := startSpan(t, "sender")
	if _, err := p.Send(ctx, withTopic(e.topic, values("b", n))...); err != nil {
		t.Fatal(err)
	}
	sender.End()

	var mu sync.Mutex
	sizes := map[trace.SpanID]int{}
	var total atomic.Int64
	startBatchConsumer(t, batchConfig(t, e, n), func(ctx context.Context, msgs []kafkax.Message) error {
		mu.Lock()
		sizes[trace.SpanContextFromContext(ctx).SpanID()] = len(msgs)
		mu.Unlock()
		total.Add(int64(len(msgs)))
		return nil
	})
	waitFor(t, "all messages", func() bool { return total.Load() >= n })

	mu.Lock()
	want := maps.Clone(sizes)
	mu.Unlock()
	cons := topicSpans(t, trace.SpanKindConsumer, e.topic, len(want))
	sawBig := false
	for _, s := range cons {
		size, ok := want[s.SpanContext().SpanID()]
		if !ok {
			continue
		}
		wantLinks := min(size, 32)
		if len(s.Links()) != wantLinks {
			t.Errorf("batch of %d has %d links, want %d", size, len(s.Links()), wantLinks)
		}
		sawBig = sawBig || size > 32
	}
	if !sawBig {
		t.Skipf("no batch larger than 32 formed (sizes %v); the cap was not exercised", want)
	}
}

// 按批的 handler 失败后退回逐条处理：每条一个普通的 CONSUMER span，是各自 PRODUCER 的子 span。
func TestBatchFallbackToSingleMessagesUsesParentSpans(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	ctx, sender := startSpan(t, "sender")
	if _, err := p.Send(ctx, withTopic(e.topic, values("f", 2))...); err != nil {
		t.Fatal(err)
	}
	sender.End()

	var seenSingle atomic.Int64
	startBatchConsumer(t, batchConfig(t, e, 10), func(_ context.Context, msgs []kafkax.Message) error {
		if len(msgs) > 1 {
			return errors.New("batch fails")
		}
		seenSingle.Add(1)
		return nil
	})
	waitFor(t, "both messages handled one by one", func() bool { return seenSingle.Load() >= 2 })

	prods := topicSpans(t, trace.SpanKindProducer, e.topic, 2)
	prodIDs := map[trace.SpanID]bool{}
	for _, s := range prods {
		prodIDs[s.SpanContext().SpanID()] = true
	}
	// 在 trace 里有父是 PRODUCER 的 CONSUMER span（逐条的）。
	singles := waitSpans(t, "single-message CONSUMER spans", 2, func(s sdktrace.ReadOnlySpan) bool {
		return s.SpanKind() == trace.SpanKindConsumer && hasAttr(s, "messaging.destination.name", e.topic) && prodIDs[s.Parent().SpanID()]
	})
	if len(singles) < 2 {
		t.Errorf("%d single-message spans, want 2", len(singles))
	}
	// 之前失败的那整批：也有 span，状态是 Error。
	failed := waitSpans(t, "failed batch span", 1, func(s sdktrace.ReadOnlySpan) bool {
		return s.SpanKind() == trace.SpanKindConsumer && hasAttr(s, "messaging.destination.name", e.topic) &&
			s.Status().Code == otelcodes.Error && !s.Parent().IsValid()
	})
	if len(failed) == 0 {
		t.Error("no Error span for the failed batch")
	}
}
