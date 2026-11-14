package kafkax

import (
	"context"
	"maps"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// 埋点（设计文档 1d 的 8.4、8.5、8.8，ADR-0023）。
//
// 传播和 span 写在这个包里，没有用 franz-go 的 kotel 插件：它的钩子对应“消息进出客户端的缓冲”，
// 而处理的过程（按 key 分 lane、排队、重试、按批、延迟消息等到期）都在这个包自己的代码里。

// TraceparentHeader 是消息里携带链路上下文的 header（W3C Trace Context）。
const TraceparentHeader = "traceparent"

const scopeName = "github.com/palemoky/oppenheimer/pkg/kafkax"

// maxBatchLinks 是按批处理的 span 最多带多少个 link。一批可以有几百条消息，全带上的话一个 span 比它描述的工作还大。
const maxBatchLinks = 32

// 全局的 tracer 和 meter 是代理：在 otelx.Setup 之前拿到的，Setup 之后自动指向真正的实现。
var (
	tracer = otel.Tracer(scopeName)
	meter  = otel.Meter(scopeName)

	sendDuration    = mustHistogram("kafkax.producer.send.duration", "发送一条消息到 broker 确认的耗时")
	processDuration = mustHistogram("kafkax.consumer.process.duration", "handler 处理一次（一次尝试）的耗时")
	queueDuration   = mustHistogram("kafkax.consumer.queue.duration", "消息从拉到进程里到 handler 开始处理的排队时长")
	lagDuration     = mustHistogram("kafkax.consumer.delivery.duration", "消息从写入 Kafka 到 handler 开始处理的时长（延迟消息从到期时刻算起）")
	retryCount      = mustCounter("kafkax.consumer.retries", "handler 失败后重试的次数")
	dlqCount        = mustCounter("kafkax.consumer.dead_lettered", "进死信的消息数")
)

func mustHistogram(name, desc string) metric.Float64Histogram {
	h, err := meter.Float64Histogram(name, metric.WithUnit("s"), metric.WithDescription(desc))
	if err != nil {
		panic(err) // 只有名字不合法才会出错，是写代码时的错误。
	}
	return h
}

func mustCounter(name, desc string) metric.Int64Counter {
	c, err := meter.Int64Counter(name, metric.WithDescription(desc))
	if err != nil {
		panic(err)
	}
	return c
}

func topicAttr(topic string) attribute.KeyValue {
	return attribute.String("messaging.destination.name", topic)
}

var (
	systemAttr     = attribute.String("messaging.system", "kafka")
	outcomeOK      = attribute.String("outcome", "ok")
	outcomeError   = attribute.String("outcome", "error")
	outcomePermErr = attribute.String("outcome", "permanent_error")
)

// startSend 给一条要发送的消息建 PRODUCER span，并把链路上下文写进它的 header。返回要实际发出去的 header。
//
// ctx 里没有 span、而消息已经带着 traceparent 时（outbox 的中继：它是后台循环，上下文是从表里读出来的），
// 把 span 接在 header 里的那个上下文下面。
func startSend(ctx context.Context, r Record) (map[string]string, trace.Span) {
	prop := otel.GetTextMapPropagator()
	if !trace.SpanContextFromContext(ctx).IsValid() && r.Headers[TraceparentHeader] != "" {
		ctx = prop.Extract(ctx, propagation.MapCarrier(r.Headers))
	}
	ctx, span := tracer.Start(ctx, "send "+r.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(systemAttr, topicAttr(r.Topic), attribute.String("messaging.operation.type", "send")))
	if !span.SpanContext().IsValid() {
		return r.Headers, span
	}
	// 不改调用方的 map：它可能被重发时复用。
	headers := maps.Clone(r.Headers)
	if headers == nil {
		headers = make(map[string]string, 2)
	}
	prop.Inject(ctx, propagation.MapCarrier(headers))
	return headers, span
}

func endSend(ctx context.Context, span trace.Span, topic string, start time.Time, err error) {
	outcome := outcomeOK
	if err != nil {
		outcome = outcomeError
		span.RecordError(err)
		span.SetStatus(codes.Error, "send failed")
	}
	span.End()
	sendDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(topicAttr(topic), outcome))
}

// polledKey 是记录的 Context 里存“拉到进程里的时刻”用的 key。
type polledKey struct{}

// stampPolled 在一批刚拉到的记录上记下此刻，后面用它算进程内的排队时长。
// 一批共用一个 context，不是每条记录分配一个。kgo.Record.Context 就是留给消费方挂东西用的。
func stampPolled(recs []*kgo.Record, now time.Time) {
	ctx := context.WithValue(context.Background(), polledKey{}, now)
	for _, rec := range recs {
		rec.Context = ctx //nolint:fatcontext // 同一个 ctx 赋给一批记录，没有在循环里层层嵌套
	}
}

func polledAt(rec *kgo.Record) (time.Time, bool) {
	if rec.Context == nil {
		return time.Time{}, false
	}
	t, ok := rec.Context.Value(polledKey{}).(time.Time)
	return t, ok
}

// metricAttrs 返回这个消费者在 topic 上的指标标签。标签只有 topic 和消费组，都是有限的。
func (c *Consumer) metricAttrs(topic string, extra ...attribute.KeyValue) metric.MeasurementOption {
	attrs := append([]attribute.KeyValue{topicAttr(topic), attribute.String("messaging.consumer.group.name", c.cfg.Group)}, extra...)
	return metric.WithAttributes(attrs...)
}

// startProcess 给一次处理（一次尝试）建 CONSUMER span，返回带着这个 span 的 ctx。
//
// 和生产者的关系（ADR-0023）：
//   - 普通消费者：父子。消费者的 span 接在生产者的 span 下面，受理和落库在同一条链路里，
//     消息在 Kafka 里排了多久就是两个 span 之间的空隙。
//   - 延迟消费者：link。消息在到期之后才处理（延迟关单是支付时限之后），给一条早已结束的链路再添 span，
//     后端按“链路已结束”做的处理都会出错。所以开一条新的链路，只带一个指向生产者的引用。
//
// 每次尝试一个 span，不是整条消息一个：消费者对普通错误不设重试上限，依赖坏着时一条消息可以重试几分钟，
// 而 span 结束了才会被导出。整条消息一个 span 的话，分区停住的那段时间里什么也看不到。
func (c *Consumer) startProcess(ctx context.Context, rec *kgo.Record, msg Message, attempt int) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			systemAttr, topicAttr(rec.Topic),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.consumer.group.name", c.cfg.Group),
			attribute.String("messaging.destination.partition.id", strconv.FormatInt(int64(rec.Partition), 10)),
			attribute.Int64("messaging.kafka.offset", rec.Offset),
			attribute.Int("kafkax.attempt", attempt),
		),
	}
	remote := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(msg.Headers))
	if c.clock != nil {
		if link := trace.LinkFromContext(remote); link.SpanContext.IsValid() {
			opts = append(opts, trace.WithLinks(link))
		}
		opts = append(opts, trace.WithNewRoot())
	} else {
		ctx = remote
	}
	return tracer.Start(ctx, "process "+rec.Topic, opts...) //nolint:spancheck // 由调用方用 endProcess 结束
}

// observeStart 在第一次尝试开始时记录两段等待：进程内排队，和从写入 Kafka 算起的总等待。
// 两个数都记在 span 上（看单个订单），也记进直方图（看整体）。
func (c *Consumer) observeStart(ctx context.Context, span trace.Span, rec *kgo.Record, now time.Time) {
	attrs := c.metricAttrs(rec.Topic)
	if polled, ok := polledAt(rec); ok {
		wait := now.Sub(polled)
		span.SetAttributes(attribute.Float64("kafkax.queue_ms", float64(wait.Microseconds())/1000))
		queueDuration.Record(ctx, wait.Seconds(), attrs)
	}
	// 延迟消息的“应该被处理的时刻”是到期时刻，不是写入时刻。
	from := rec.Timestamp
	if c.clock != nil {
		if due, ok := dueAt(rec); ok {
			from = due
		}
	}
	if !from.IsZero() {
		wait := max(now.Sub(from), 0)
		span.SetAttributes(attribute.Float64("kafkax.delivery_ms", float64(wait.Microseconds())/1000))
		lagDuration.Record(ctx, wait.Seconds(), attrs)
	}
}

// endProcess 结束一次尝试的 span 并记录耗时。
func (c *Consumer) endProcess(ctx context.Context, span trace.Span, topic string, start time.Time, err error) {
	outcome := outcomeOK
	switch {
	case err == nil:
	case IsPermanent(err):
		outcome = outcomePermErr
	default:
		outcome = outcomeError
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "handler failed")
	}
	span.End()
	processDuration.Record(ctx, time.Since(start).Seconds(), c.metricAttrs(topic, outcome))
}

// startBatch 给按批处理的一次调用建 span。一个 span 只能有一个父，所以它是一条新链路，
// 用 link 指向批里各条消息的生产者（最多 maxBatchLinks 个，重复的上下文只算一次）。
func (c *Consumer) startBatch(ctx context.Context, recs []*kgo.Record, msgs []Message) (context.Context, trace.Span) {
	prop := otel.GetTextMapPropagator()
	var links []trace.Link
	seen := make(map[trace.SpanID]struct{})
	for _, msg := range msgs {
		if len(links) == maxBatchLinks {
			break
		}
		if msg.Headers[TraceparentHeader] == "" {
			continue
		}
		link := trace.LinkFromContext(prop.Extract(context.Background(), propagation.MapCarrier(msg.Headers)))
		if !link.SpanContext.IsValid() {
			continue
		}
		if _, dup := seen[link.SpanContext.SpanID()]; dup {
			continue
		}
		seen[link.SpanContext.SpanID()] = struct{}{}
		links = append(links, link)
	}
	first := recs[0]
	return tracer.Start(ctx, "process "+first.Topic, //nolint:spancheck // 由调用方用 endProcess 结束
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithNewRoot(),
		trace.WithLinks(links...),
		trace.WithAttributes(
			systemAttr, topicAttr(first.Topic),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.consumer.group.name", c.cfg.Group),
			attribute.String("messaging.destination.partition.id", strconv.FormatInt(int64(first.Partition), 10)),
			attribute.Int64("messaging.kafka.offset", first.Offset),
			attribute.Int("messaging.batch.message_count", len(recs)),
		))
}
