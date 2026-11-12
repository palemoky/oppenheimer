package kafkax

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Record 是要发送的一条消息。
type Record struct {
	Topic string
	// Key 决定分区：相同 key 的记录总是进同一个分区（murmur2 哈希，与 Java 客户端一致），
	// 从而保持顺序。Key 为 nil 时由客户端挑选分区，不保证与其他记录的相对顺序。
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// Sender 是 Producer 的发送能力，方便别的包在测试里替换成假实现。
type Sender interface {
	Send(ctx context.Context, recs ...Record) ([]error, error)
}

// Producer 是同步确认的生产者。并发安全。
type Producer struct {
	client *kgo.Client
	// owned 表示 client 由本 Producer 创建、需要由它关闭。
	// 消费者发死信时复用自己的 client，不能被 Producer 关掉。
	owned bool
}

var _ Sender = (*Producer)(nil)

// NewProducer 基于 Config 创建。acks=all，开启幂等生产者。调用方负责 Close。
//
// 为什么是这两个选项：
//   - acks=all：leader 等所有 ISR 副本写入后才回复成功。只等 leader（acks=1）时，
//     leader 回复后、副本同步前宕机，已确认的消息会丢。
//   - 幂等生产者：网络超时后客户端重发同一批消息，broker 按 producer id 和序号去重，
//     既不会产生重复，也不会因为重发而打乱同一分区内的顺序（最多 5 个在途请求时仍保序）。
//     franz-go 默认开启幂等，这里不调用 DisableIdempotentWrite 即可；它要求 acks=all。
func NewProducer(ctx context.Context, cfg Config) (*Producer, error) {
	client, err := NewClient(ctx, cfg, producerOpts()...)
	if err != nil {
		return nil, err
	}
	return &Producer{client: client, owned: true}, nil
}

// producerOpts 是生产者固定使用的选项，消费者的 client 也会带上（用于发死信）。
func producerOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
}

// Send 同步发送并等待 broker 确认。
//
// 返回的切片与 recs 一一对应：errs[i] == nil 表示 recs[i] 已持久化。
// 全部成功时第二个返回值为 nil；任何一条失败时第二个返回值是汇总错误（errors.Join）。
// 相同 key 的记录进同一分区，且分区内顺序与 recs 中的顺序一致。
//
// 注意：失败不代表消息一定没写入（例如 ctx 在 broker 写入后、回复前到期），
// 调用方重发时可能产生重复，下游要按至少一次投递来设计。
func (p *Producer) Send(ctx context.Context, recs ...Record) ([]error, error) {
	if len(recs) == 0 {
		return nil, nil
	}
	krs := make([]*kgo.Record, len(recs))
	for i, r := range recs {
		krs[i] = &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value, Headers: toKgoHeaders(r.Headers)}
	}
	return p.sendRaw(ctx, krs)
}

// sendRaw 发送已经构造好的 kgo 记录，结果按输入顺序返回。
func (p *Producer) sendRaw(ctx context.Context, krs []*kgo.Record) ([]error, error) {
	// ProduceSync 按完成顺序而不是输入顺序返回结果，用记录指针找回下标。
	index := make(map[*kgo.Record]int, len(krs))
	for i, r := range krs {
		index[r] = i
	}
	errs := make([]error, len(krs))
	var failed []error
	for _, res := range p.client.ProduceSync(ctx, krs...) {
		if res.Err == nil {
			continue
		}
		i := index[res.Record]
		errs[i] = fmt.Errorf("kafkax: produce record %d to %s: %w", i, res.Record.Topic, res.Err)
		failed = append(failed, errs[i])
	}
	if len(failed) == 0 {
		return errs, nil
	}
	return errs, errors.Join(failed...)
}

// Close 等缓冲中的记录发送完毕后关闭连接。
func (p *Producer) Close() {
	if p.owned {
		p.client.Close()
	}
}

// toKgoHeaders 把 map 转成 header 列表。按 key 排序，使相同输入得到相同的字节，便于排查。
func toKgoHeaders(h map[string]string) []kgo.RecordHeader {
	if len(h) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, 0, len(h))
	for _, k := range slices.Sorted(maps.Keys(h)) {
		out = append(out, kgo.RecordHeader{Key: k, Value: []byte(h[k])})
	}
	return out
}
