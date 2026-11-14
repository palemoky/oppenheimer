package kafkax_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// 本文件测试按批处理的消费者（NewBatchConsumer，设计文档 1d 的 2.11）。

func startBatchConsumer(t *testing.T, cfg kafkax.BatchConsumerConfig, h kafkax.BatchHandler) *runner {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	c, err := kafkax.NewBatchConsumer(ctx, cfg, h)
	if err != nil {
		t.Fatalf("new batch consumer: %v", err)
	}
	return start(t, c)
}

func batchConfig(t *testing.T, e env, maxBatch int) kafkax.BatchConsumerConfig {
	return kafkax.BatchConsumerConfig{ConsumerConfig: e.consumerConfig(t), MaxBatch: maxBatch}
}

// batchLog 记录 handler 收到的每一批。
type batchLog struct {
	mu      sync.Mutex
	batches [][]kafkax.Message
}

func (l *batchLog) add(msgs []kafkax.Message) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batches = append(l.batches, slices.Clone(msgs))
}

func (l *batchLog) snapshot() [][]kafkax.Message {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.batches)
}

func (l *batchLog) total() int {
	n := 0
	for _, b := range l.snapshot() {
		n += len(b)
	}
	return n
}

func sumCommitted(t *testing.T, e env) int64 {
	var sum int64
	for _, off := range committed(t, e.brokers, e.group, e.topic) {
		sum += off
	}
	return sum
}

// 有积压时消息成批到达：每批来自一个分区、位点连续递增、不超过上限；所有消息恰好出现一次，位点提交到末尾。
func TestBatchConsumerDeliversContiguousBatches(t *testing.T) {
	t.Parallel()
	const (
		total    = 3000
		maxBatch = 200
	)
	e := newEnv(t, 4)
	mustSend(t, newProducer(t, e.brokers), keyed(e.topic, total, 300)...)

	var log batchLog
	startBatchConsumer(t, batchConfig(t, e, maxBatch), func(_ context.Context, msgs []kafkax.Message) error {
		log.add(msgs)
		return nil
	})
	waitFor(t, "all messages", func() bool { return log.total() >= total })
	waitFor(t, "commit to the end", func() bool { return sumCommitted(t, e) == total })

	next := map[int32]int64{}
	largest := 0
	for _, b := range log.snapshot() {
		if len(b) == 0 || len(b) > maxBatch {
			t.Fatalf("batch of %d messages, want 1..%d", len(b), maxBatch)
		}
		largest = max(largest, len(b))
		p := b[0].Partition
		for _, m := range b {
			if m.Partition != p {
				t.Fatalf("batch mixes partitions %d and %d", p, m.Partition)
			}
			if m.Offset != next[p] {
				t.Fatalf("partition %d: got offset %d, want %d (batches must be contiguous and in order)", p, m.Offset, next[p])
			}
			next[p]++
		}
	}
	if log.total() != total {
		t.Fatalf("handler saw %d messages, want exactly %d", log.total(), total)
	}
	// 消费者启动时消息都已经在 topic 里，一定有攒起来的批。
	if largest < 2 {
		t.Fatalf("largest batch has %d message, want real batches under a backlog", largest)
	}
	t.Logf("%d batches, largest %d", len(log.snapshot()), largest)
}

// 没有积压时一批就是一条：不等凑满。
func TestBatchConsumerDoesNotWaitToFillABatch(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	var log batchLog
	startBatchConsumer(t, batchConfig(t, e, 100), func(_ context.Context, msgs []kafkax.Message) error {
		log.add(msgs)
		return nil
	})
	for i := range 5 {
		mustSend(t, p, distinctKeys(e.topic, 1)...)
		waitFor(t, "message "+strconv.Itoa(i), func() bool { return log.total() >= i+1 })
	}
	for _, b := range log.snapshot() {
		if len(b) != 1 {
			t.Fatalf("got a batch of %d although messages arrived one at a time", len(b))
		}
	}
}

// 一批里有一条坏消息：整批失败后逐条处理，坏的那条进死信，其余的都完成，位点越过整批。
func TestBatchConsumerIsolatesPoisonMessage(t *testing.T) {
	t.Parallel()
	const total = 50
	e := newEnv(t, 1)
	mustSend(t, newProducer(t, e.brokers), distinctKeys(e.topic, total)...)

	var (
		mu      sync.Mutex
		applied = map[string]int{}
		failed  atomic.Int32
	)
	startBatchConsumer(t, batchConfig(t, e, total), func(_ context.Context, msgs []kafkax.Message) error {
		// 像一个事务：有坏消息时整批不生效。
		if slices.ContainsFunc(msgs, func(m kafkax.Message) bool { return string(m.Value) == "m17" }) {
			failed.Add(1)
			return kafkax.Permanent(errors.New("poison"))
		}
		mu.Lock()
		defer mu.Unlock()
		for _, m := range msgs {
			applied[string(m.Value)]++
		}
		return nil
	})
	waitFor(t, "commit to the end", func() bool { return sumCommitted(t, e) == total })

	dead := readAll(t, e.brokers, e.dlq, 1)
	if string(dead[0].Value) != "m17" {
		t.Fatalf("dead letter = %s, want m17", dead[0].Value)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(applied) != total-1 {
		t.Fatalf("applied %d distinct messages, want %d", len(applied), total-1)
	}
	for v, n := range applied {
		if n != 1 {
			t.Errorf("%s applied %d times, want once", v, n)
		}
	}
	if applied["m17"] != 0 {
		t.Error("the poison message was applied")
	}
}

// 整批因为暂时的原因失败（比如下游抖了一下）：整批重试，成功后仍然是一批，不退回逐条。
func TestBatchConsumerRetriesTheWholeBatchOnTransientFailure(t *testing.T) {
	t.Parallel()
	const total = 40
	e := newEnv(t, 1)
	mustSend(t, newProducer(t, e.brokers), distinctKeys(e.topic, total)...)

	var (
		log   batchLog
		calls atomic.Int32
	)
	startBatchConsumer(t, batchConfig(t, e, total), func(_ context.Context, msgs []kafkax.Message) error {
		if calls.Add(1) <= 2 {
			return errors.New("downstream unavailable")
		}
		log.add(msgs)
		return nil
	})
	waitFor(t, "commit to the end", func() bool { return sumCommitted(t, e) == total })
	if log.total() != total {
		t.Fatalf("applied %d messages, want %d", log.total(), total)
	}
	for _, b := range log.snapshot() {
		if len(b) == 1 {
			t.Fatalf("a transient failure made the consumer fall back to single messages: batches %d", len(log.snapshot()))
		}
	}
}

// 整批一直失败（重试几次也不行），但逐条能成功：退回逐条，全部完成，没有消息进死信。
// 模拟“只在批量时才出现的问题”，逐条是兜底。
func TestBatchConsumerFallsBackWhenTheBatchKeepsFailing(t *testing.T) {
	t.Parallel()
	const total = 40
	e := newEnv(t, 1)
	mustSend(t, newProducer(t, e.brokers), distinctKeys(e.topic, total)...)

	var (
		mu       sync.Mutex
		applied  = map[string]int{}
		batchTry atomic.Int32
	)
	startBatchConsumer(t, batchConfig(t, e, total), func(_ context.Context, msgs []kafkax.Message) error {
		if len(msgs) > 1 {
			batchTry.Add(1)
			return errors.New("too large")
		}
		mu.Lock()
		defer mu.Unlock()
		applied[string(msgs[0].Value)]++
		return nil
	})
	waitFor(t, "commit to the end", func() bool { return sumCommitted(t, e) == total })
	mu.Lock()
	defer mu.Unlock()
	if len(applied) != total {
		t.Fatalf("applied %d distinct messages, want %d", len(applied), total)
	}
	for v, n := range applied {
		if n != 1 {
			t.Errorf("%s applied %d times, want once", v, n)
		}
	}
	// 消息可能分成不止一批到达，每一批各试 3 次。
	if n := batchTry.Load(); n < 3 || n%3 != 0 {
		t.Errorf("whole-batch attempts = %d, want a multiple of 3 (three tries per batch before falling back)", n)
	}
}

// 一批处理到一半被强制停止：这一批不提交，重启后整批重新投递。
func TestBatchConsumerDoesNotCommitAnUnfinishedBatch(t *testing.T) {
	t.Parallel()
	const total = 30
	e := newEnv(t, 1)
	mustSend(t, newProducer(t, e.brokers), distinctKeys(e.topic, total)...)

	entered := make(chan struct{})
	var once sync.Once
	r := startBatchConsumer(t, batchConfig(t, e, total), func(ctx context.Context, _ []kafkax.Message) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	})
	select {
	case <-entered:
	case <-time.After(waitTimeout):
		t.Fatal("handler never called")
	}
	if err := r.stop(t, 100*time.Millisecond); err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if got := sumCommitted(t, e); got != 0 {
		t.Fatalf("committed %d offsets although no batch finished", got)
	}

	var log batchLog
	startBatchConsumer(t, batchConfig(t, e, total), func(_ context.Context, msgs []kafkax.Message) error {
		log.add(msgs)
		return nil
	})
	waitFor(t, "redelivery of everything", func() bool { return log.total() >= total })
	if first := log.snapshot()[0][0]; first.Offset != 0 {
		t.Fatalf("redelivery started at offset %d, want 0", first.Offset)
	}
}

func TestBatchConsumerConfigIsValidated(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	noop := func(context.Context, []kafkax.Message) error { return nil }

	if _, err := kafkax.NewBatchConsumer(ctx, batchConfig(t, e, 10), nil); err == nil {
		t.Error("nil handler accepted")
	}
	if _, err := kafkax.NewBatchConsumer(ctx, batchConfig(t, e, -1), noop); err == nil {
		t.Error("negative max batch accepted")
	}
	both := batchConfig(t, e, 10)
	both.Concurrency = 4
	if _, err := kafkax.NewBatchConsumer(ctx, both, noop); err == nil {
		t.Error("batch consumer with concurrency 4 accepted")
	}
}
