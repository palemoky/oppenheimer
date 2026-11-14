// Package idgen 生成带分片基因的全局唯一 ID（雪花算法变体）。
//
// 63 位有效位的布局，从高到低：
//
//	41 位  毫秒时间戳，相对 Epoch，可用约 69 年（到 2089 年）
//	 6 位  节点号，最多 64 个节点
//	10 位  同一毫秒内的序号，每节点每毫秒 1024 个
//	 6 位  分片基因，取自分片键（如 user_id）的低 6 位
//
// 基因放在最低位，因此 id % N == 分片键 % N 对任何不超过 64 的 2 的幂 N 成立：
// 只凭订单号就能算出它和它的用户落在哪个分片，不需要额外的映射。
// 时间戳在最高位，所以同一节点生成的 ID 随时间严格递增，对 B+ 树主键友好。
package idgen

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	geneBits   = 6
	seqBits    = 10
	workerBits = 6

	geneMask = 1<<geneBits - 1
	seqMask  = 1<<seqBits - 1

	// MaxWorkerID 是允许的最大节点号。
	MaxWorkerID = 1<<workerBits - 1

	seqShift    = geneBits
	workerShift = geneBits + seqBits
	timeShift   = geneBits + seqBits + workerBits

	// 时钟回拨不超过这个幅度时原地等待，超过则报错。
	maxBackwardWait = 5 * time.Millisecond
)

// Epoch 是时间戳的起点。一旦有 ID 生成就不能再改：改了以后旧 ID 解出的时间全部错位，
// 新旧 ID 之间“越晚生成越大”的顺序也不再成立（不会重复，但按 ID 排序不再等于按时间排序）。
//
// 2026-11-14 从 2026-01-01 提前到 2020-01-01（设计文档 1c 的 5.3），当时清空了所有开发库。
// 原因是造数工具要生成过去几年的订单，而 ID 里的时间不能早于 Epoch。
var Epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// ErrClockMovedBackwards 表示系统时钟回拨幅度过大，为避免生成重复 ID 而拒绝服务。
var ErrClockMovedBackwards = errors.New("idgen: clock moved backwards")

// Generator 并发安全地生成 ID。同一个节点号在同一时刻只能有一个 Generator 在用，
// 否则会生成重复的 ID。
type Generator struct {
	workerID int64
	now      func() time.Time
	sleep    func(time.Duration)

	mu     sync.Mutex
	lastMs int64
	seq    int64
}

// Option 配置 Generator。
type Option func(*Generator)

// WithClock 替换时间来源和等待方式，用于测试。
func WithClock(now func() time.Time, sleep func(time.Duration)) Option {
	return func(g *Generator) {
		g.now = now
		g.sleep = sleep
	}
}

// New 返回使用给定节点号的 Generator。
func New(workerID int64, opts ...Option) (*Generator, error) {
	if workerID < 0 || workerID > MaxWorkerID {
		return nil, fmt.Errorf("idgen: worker id %d out of range [0, %d]", workerID, MaxWorkerID)
	}
	g := &Generator{workerID: workerID, now: time.Now, sleep: time.Sleep}
	for _, opt := range opts {
		opt(g)
	}
	return g, nil
}

// Next 生成一个 ID，其基因取自 shardKey 的低 6 位。
func (g *Generator) Next(shardKey int64) (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	ms := g.now().Sub(Epoch).Milliseconds()

	if ms < g.lastMs {
		// 时钟回拨（NTP 校时、虚拟机迁移等）。此时若继续发号，可能与回拨前发出的 ID 重复。
		behind := time.Duration(g.lastMs-ms) * time.Millisecond
		if behind > maxBackwardWait {
			return 0, fmt.Errorf("%w by %v", ErrClockMovedBackwards, behind)
		}
		g.sleep(behind)
		ms = g.now().Sub(Epoch).Milliseconds()
		if ms < g.lastMs {
			return 0, fmt.Errorf("%w: still behind after waiting %v", ErrClockMovedBackwards, behind)
		}
	}

	if ms == g.lastMs {
		g.seq = (g.seq + 1) & seqMask
		if g.seq == 0 {
			// 这一毫秒的序号用完了，等到下一毫秒。
			for ms <= g.lastMs {
				g.sleep(100 * time.Microsecond)
				ms = g.now().Sub(Epoch).Milliseconds()
			}
		}
	} else {
		g.seq = 0
	}
	g.lastMs = ms

	if ms < 0 || ms >= 1<<41 {
		return 0, fmt.Errorf("idgen: timestamp %d ms outside the 41-bit range", ms)
	}

	return ms<<timeShift | g.workerID<<workerShift | g.seq<<seqShift | GeneOf(shardKey), nil
}

// GeneOf 返回分片键对应的基因。
func GeneOf(shardKey int64) int64 { return shardKey & geneMask }

// Gene 返回 ID 中嵌入的基因。
func Gene(id int64) int64 { return id & geneMask }

// LowerBound 返回生成时间不早于 t 的 ID 的下界：时间早于 t 的 ID 都小于它，其余的都不小于它。
// t 早于 Epoch 时返回 0。用来把“早于某个时间”换成主键上的范围条件（归档的截止点、按时间分区的边界）。
func LowerBound(t time.Time) int64 {
	return max(t.Sub(Epoch).Milliseconds(), 0) << timeShift
}

// Time 返回 ID 的生成时间，精确到毫秒。
func Time(id int64) time.Time {
	return Epoch.Add(time.Duration(id>>timeShift) * time.Millisecond)
}
