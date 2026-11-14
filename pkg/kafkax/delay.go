package kafkax

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DueHeader 是携带到期时间的 header 名，值是 RFC 3339（带纳秒）的 UTC 时间。
const DueHeader = "due-at"

// NewDelayRecord 构造一条到期时间为 due 的延迟消息。
func NewDelayRecord(topic string, key, value []byte, due time.Time) Record {
	return Record{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: map[string]string{DueHeader: due.UTC().Format(time.RFC3339Nano)},
	}
}

// Clock 是延迟消费者使用的时钟，测试里可以注入假时钟。
type Clock interface {
	Now() time.Time
	// After 在 d 之后向返回的通道发送一次当前时间。
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// DelayConsumerConfig 是延迟消费者的配置。
type DelayConsumerConfig struct {
	ConsumerConfig
	// Clock 为空时用系统时钟。
	Clock Clock
}

// NewDelayConsumer 创建延迟消费者：消息到期（DueHeader）后才交给 handler。
//
// 实现：分区 worker 取到队首消息时，如果还没到期，就暂停该分区的拉取并等到到期，
// 再恢复拉取、交给 handler。其余语义（处理成功才提交、重试、死信、再平衡、停止）与 NewConsumer 相同，
// 用的是同一套代码，只是处理每条消息前多了等待到期这一步。
//
// 适用于“固定延迟”：同一 topic 的消息延迟相同，所以分区内的到期时间随位点单调递增，
// 只看队首就够了。若同一分区里混入延迟更长的消息，它会挡住后面先到期的消息（队头阻塞）。
//
// 为什么长时间不拉取是安全的：
//   - Java 客户端的 max.poll.interval.ms 是客户端自己的检查（两次 poll 间隔太久就主动离组），
//     broker 并不检查 poll 间隔。franz-go 没有这个机制：组成员的心跳由后台 goroutine 按
//     HeartbeatInterval 发送，与是否 poll 无关，所以只要进程活着，会话就不会因为等待而过期。
//   - 再平衡超时（RebalanceTimeout）只约束再平衡期间回调的耗时。等待到期的 worker 在收回分区时
//     会被立即打断（该消息未提交，之后重新读到），回调只需要等正在执行的 handler；
//     BlockRebalanceOnPoll 也只在把拉到的记录分发到队列的那一小段时间里阻塞再平衡。
//   - 拉取循环一直在 poll（其他分区照常拉取），暂停只作用于在等待的分区。
func NewDelayConsumer(ctx context.Context, cfg DelayConsumerConfig, handler Handler) (*Consumer, error) {
	clock := cfg.Clock
	if clock == nil {
		clock = realClock{}
	}
	return newConsumer(ctx, cfg.ConsumerConfig, handler, clock)
}

// waitDue 等到消息到期。等待期间暂停该分区的拉取，被 halt 或强制取消打断时返回 false。
func (c *Consumer) waitDue(ctx context.Context, w *worker, rec *kgo.Record, log *slog.Logger) bool {
	due, ok := dueAt(rec)
	if !ok {
		log.WarnContext(ctx, "kafka delay message has no valid due-at header, processing now")
		return true
	}
	waited := false
	for {
		now := c.clock.Now()
		if !now.Before(due) {
			break
		}
		if !waited {
			waited = true
			w.setWaiting(true)
		}
		select {
		case <-c.clock.After(due.Sub(now)):
			// 醒来后重新读时钟：假时钟或系统时钟调整都可能让 After 早于到期时间返回。
		case <-w.stop:
			return false
		case <-ctx.Done():
			return false
		}
	}
	if waited {
		w.setWaiting(false)
	}
	return true
}

func dueAt(rec *kgo.Record) (time.Time, bool) {
	for _, h := range slices.Backward(rec.Headers) {
		if h.Key != DueHeader {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, string(h.Value))
		if err != nil {
			return time.Time{}, false
		}
		return t, true
	}
	return time.Time{}, false
}
