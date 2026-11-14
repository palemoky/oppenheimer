package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/twmb/franz-go/pkg/kgo"
)

// BatchHandler 处理一批消息。msgs 来自同一个分区、按位点升序、中间没有跳过，至少一条。
// 返回 nil 表示整批处理完；返回错误表示这一批没有处理完（即使其中一部分已经生效）。
//
// 一批失败之后，这些消息会一条一条地重新交给同一个 handler（每次只有一条的批），所以 handler 必须幂等，
// 而且对只有一条消息的批，返回值的含义与 Handler 相同：Permanent 包装的错误直接进死信，其他错误原地重试。
type BatchHandler func(ctx context.Context, msgs []Message) error

// DefaultMaxBatch 是 BatchConsumerConfig.MaxBatch 的默认值。
const DefaultMaxBatch = 500

// BatchConsumerConfig 是按批处理的消费者的配置。
type BatchConsumerConfig struct {
	ConsumerConfig
	// MaxBatch 是一批最多多少条消息，默认 DefaultMaxBatch。
	MaxBatch int
}

// NewBatchConsumer 创建按批处理的消费者（设计文档 1d 的 2.11、ADR-0017 的补充）。
//
// 适合“每条消息的处理就是写一行、成本主要在提交上”的消费者：把一段消息合成一次写入、一次提交。
// 每条消息各自要等下游往返的（比如落库），按批没有用，用 ConsumerConfig.Concurrency。
//
// 一批是分区本地队列里现成的一段，最多 MaxBatch 条，不等凑满：没有积压时一批就是一条，
// 延迟与逐条处理相同；有积压时批自然变大。所以没有“攒多久”这个参数。
//
// 位点：一批成功后提交到它的最后一条。一批失败时先整批重试几次，仍然失败或错误是 Permanent 的
// 就退回逐条处理（见 worker.tryBatch），逐条走与普通消费者相同的重试和死信。一条坏消息因此被隔离出来，
// 不会让整批反复失败，也不需要 handler 报告是哪一条坏了。
//
// 和 Concurrency 互斥：按批已经把一个分区的写入合成一次提交，再分 lane 只会让事务之间互相等。
func NewBatchConsumer(ctx context.Context, cfg BatchConsumerConfig, handler BatchHandler) (*Consumer, error) {
	if handler == nil {
		return nil, errors.New("kafkax: batch consumer handler is required")
	}
	if cfg.Concurrency > 1 {
		return nil, errors.New("kafkax: a batch consumer cannot also process a partition concurrently")
	}
	if cfg.MaxBatch < 0 {
		return nil, fmt.Errorf("kafkax: max batch %d is negative", cfg.MaxBatch)
	}
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = DefaultMaxBatch
	}
	// 逐条回退时复用普通消费者的 process：它调用 c.handler，这里把它接到“只有一条的批”上。
	single := func(ctx context.Context, msg Message) error { return handler(ctx, []Message{msg}) }
	c, err := newConsumer(ctx, cfg.ConsumerConfig, single, nil)
	if err != nil {
		return nil, err
	}
	// worker 在 Start 之后才创建，这里赋值不会和它们并发。
	c.batch = handler
	c.maxBatch = cfg.MaxBatch
	return c, nil
}

// runBatches 是按批处理时 worker 的主循环。
func (w *worker) runBatches(ctx context.Context) {
	for {
		recs, ok := w.nextBatch(ctx, w.c.maxBatch)
		if !ok {
			return
		}
		if len(recs) > 1 {
			done, interrupted := w.tryBatch(ctx, recs)
			if interrupted {
				// 被要求停止：这一批没有处理完，不标记、不提交，下次重新投递。
				return
			}
			if done {
				w.markDone(recs[len(recs)-1])
				continue
			}
		}
		for _, rec := range recs {
			if w.stopping(ctx) {
				return
			}
			if !w.c.process(ctx, w, rec) {
				return
			}
			w.markDone(rec)
		}
	}
}

// batchAttempts 是一批作为整体最多尝试的次数，之后退回逐条处理。
const batchAttempts = 3

// tryBatch 把 recs 作为一批交给 handler。done 为 true 表示整批处理完；
// 两个返回值都为 false 表示这一批应当退回逐条处理。
//
// 暂时性的错误（下游抖了一下、死锁）先把整批重试几次：这时逐条处理是对的但很慢，第一条重试到下游恢复之后，
// 其余的每条各自提交一次，按批的意义就没有了。重试几次仍然失败，或者错误是 Permanent 的
// （重试也不会成功，说明批里有坏消息），才逐条处理：好的消息各自完成，坏的那条按普通消费者的规则
// 重试或进死信。逐条是兜底，它保证一条坏消息或一个只在批量时才出现的问题不会让分区永远卡在整批上。
func (w *worker) tryBatch(ctx context.Context, recs []*kgo.Record) (done, interrupted bool) {
	for attempt := 1; ; attempt++ {
		err := w.c.invokeBatch(ctx, recs, w.log)
		if err == nil {
			return true, false
		}
		if w.stopping(ctx) {
			return false, true
		}
		if IsPermanent(err) || attempt >= batchAttempts {
			w.log.Warn("kafka batch failed, processing it one message at a time",
				"first_offset", recs[0].Offset, "last_offset", recs[len(recs)-1].Offset, "size", len(recs),
				"attempt", attempt, "permanent", IsPermanent(err), "error", err)
			return false, false
		}
		backoff := w.c.backoff(attempt)
		w.log.Warn("kafka batch failed, retrying the batch", "first_offset", recs[0].Offset, "size", len(recs),
			"attempt", attempt, "backoff", backoff, "error", err)
		if !w.sleep(ctx, backoff) {
			return false, true
		}
	}
}

// nextBatch 取本地队列开头最多 limit 条记录，队列为空时等待；被 halt 时返回 false。
func (w *worker) nextBatch(ctx context.Context, limit int) ([]*kgo.Record, bool) {
	for {
		if w.stopping(ctx) {
			return nil, false
		}
		w.mu.Lock()
		if n := min(len(w.queue), limit); n > 0 {
			recs := make([]*kgo.Record, n)
			copy(recs, w.queue)
			clear(w.queue[:n])
			w.queue = w.queue[n:]
			w.maybeResumeLocked()
			w.mu.Unlock()
			return recs, true
		}
		w.mu.Unlock()
		select {
		case <-w.wake:
		case <-w.stop:
		case <-ctx.Done():
		}
	}
}

// invokeBatch 调用按批的 handler，把 panic 转成错误。
func (c *Consumer) invokeBatch(ctx context.Context, recs []*kgo.Record, log *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("kafka batch handler panicked", "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = fmt.Errorf("kafkax: batch handler panic: %v", r)
		}
	}()
	msgs := make([]Message, len(recs))
	for i, rec := range recs {
		msgs[i] = toMessage(rec)
	}
	return c.batch(ctx, msgs)
}
