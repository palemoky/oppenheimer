package kafkax

import (
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"
)

// prefixTracker 记录一个分区里已经分发给 handler、位点还不能提交的记录，
// 并算出“可以提交到哪里”。只在并发消费（ConsumerConfig.Concurrency > 1）时使用。不是并发安全的。
//
// Kafka 一个分区的已提交位点是一个数，含义是“这之前的都处理完了”。并发处理时记录不按偏移量的顺序完成：
// 偏移量 7 做完了而 5 还在重试，这时只能提交到 5，提交到 8 的话进程重启后 5 就再也不会被投递。
// 所以能提交的只有“从头开始、连续完成”的那一段前缀，前缀之后已经完成的记录要等它前面的都完成。
//
// 代价是重复：前缀之后已完成的记录，在重启或再平衡之后会被重新投递一次。handler 必须幂等。
type prefixTracker struct {
	// pending 按偏移量升序：分发的顺序就是偏移量的顺序。
	pending []pendingRecord
}

type pendingRecord struct {
	rec  *kgo.Record
	done bool
}

// add 登记一条即将分发的记录。必须按偏移量递增的顺序调用。
func (t *prefixTracker) add(rec *kgo.Record) {
	t.pending = append(t.pending, pendingRecord{rec: rec})
}

// complete 标记偏移量为 offset 的记录处理完了，并弹出开头连续完成的那一段。
// 这一段非空时返回其中最后一条记录：到它为止（含）都可以提交。否则返回 nil。
// offset 不在登记的记录里时什么都不做。
//
// 按偏移量二分查找而不是用“offset - 第一个 offset”当下标：一个分区里的偏移量不保证连续
// （压缩过的 topic、事务的控制记录都会留下空洞）。
func (t *prefixTracker) complete(offset int64) *kgo.Record {
	i, ok := slices.BinarySearchFunc(t.pending, offset, func(p pendingRecord, off int64) int {
		switch {
		case p.rec.Offset < off:
			return -1
		case p.rec.Offset > off:
			return 1
		}
		return 0
	})
	if !ok {
		return nil
	}
	t.pending[i].done = true

	n := 0
	for n < len(t.pending) && t.pending[n].done {
		n++
	}
	if n == 0 {
		return nil
	}
	last := t.pending[n-1].rec
	// 清掉弹出的元素再切片，底层数组不再引用这些记录；全部弹空时丢掉数组，避免它只增不减。
	clear(t.pending[:n])
	if t.pending = t.pending[n:]; len(t.pending) == 0 {
		t.pending = nil
	}
	return last
}

// len 返回已分发、还不能提交的记录数：在处理的、排队的，以及做完了但前面还有没做完的。
func (t *prefixTracker) len() int { return len(t.pending) }
