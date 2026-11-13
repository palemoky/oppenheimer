// Package indexer 把 CDC 发出的订单行变更（cdc.v1.RowChange）写进按商家分片的索引表。
//
// 规则（设计文档 1c 4.6、4.8）：
//   - 只处理逻辑表 orders；INSERT、UPDATE、READ 一视同仁，只看 after 映像。
//   - merchant_id 为 NULL 的订单不进索引（还没预占成功）。
//   - DELETE 忽略并计数：现在没有任何路径删除订单，归档（步骤五）时再定。
//   - 幂等和定序靠源行的 version 列：只接受更高版本的映像，不依赖到达顺序（见 store.Upsert）。
//   - 需要的列缺失或类型不对是源表结构被改坏了，返回 ErrSchema，消费停住，不跳过。
//   - 不认识的列、表、op 忽略。
package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

// SourceTable 是要同步的逻辑表名。
const SourceTable = "orders"

// DefaultWriteTimeout 是处理一条消息（一次写库）的时限。
const DefaultWriteTimeout = 5 * time.Second

// Result 是应用一条变更的结果。
type Result int

const (
	// Changed 表示索引行被插入或被更高版本覆盖。
	Changed Result = iota + 1
	// Stale 表示索引里已有不低于这个版本的映像，行没变（重复或乱序的旧消息）。
	Stale
	// SkippedNoMerchant 表示 merchant_id 为 NULL，不进索引。
	SkippedNoMerchant
	// IgnoredDelete 表示这是一条 DELETE，按约定忽略。
	IgnoredDelete
	// Ignored 表示表名或 op 不认识，忽略。
	Ignored
)

// Indexer 应用行变更并统计同步延迟。并发安全。
type Indexer struct {
	store *store.Store
	log   *slog.Logger
	now   func() time.Time
	stats *Stats
}

// New 返回写入 st 的 Indexer。logger 为 nil 时用 slog.Default()。
func New(st *store.Store, logger *slog.Logger) (*Indexer, error) {
	if st == nil {
		return nil, errors.New("indexer: nil store")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Indexer{store: st, log: logger, now: time.Now, stats: &Stats{}}, nil
}

// Stats 返回 Indexer 的统计，供 Reporter 定期输出。
func (ix *Indexer) Stats() *Stats { return ix.stats }

// Apply 把一条变更应用到索引。返回的错误包着 ErrSchema 时说明消息与表结构不符，重试不会成功。
func (ix *Indexer) Apply(ctx context.Context, ch *cdcv1.RowChange) (Result, error) {
	if ch.GetTable() != SourceTable {
		ix.stats.add(Ignored, 0, false)
		return Ignored, nil
	}
	switch ch.GetOp() {
	case cdcv1.Op_OP_INSERT, cdcv1.Op_OP_UPDATE, cdcv1.Op_OP_READ:
	case cdcv1.Op_OP_DELETE:
		ix.stats.add(IgnoredDelete, 0, false)
		return IgnoredDelete, nil
	default:
		ix.stats.add(Ignored, 0, false)
		return Ignored, nil
	}

	row, ok, err := decodeRow(ch.GetAfter())
	if err != nil {
		return 0, fmt.Errorf("indexer: %s op=%s slot=%d gtid=%q: %w",
			ch.GetSource(), ch.GetOp(), ch.GetSlot(), ch.GetGtid(), err)
	}
	if !ok {
		ix.stats.add(SkippedNoMerchant, 0, false)
		return SkippedNoMerchant, nil
	}

	now := ix.now().UTC()
	outcome, err := ix.store.Upsert(ctx, row, now)
	if err != nil {
		return 0, fmt.Errorf("indexer: %w", err)
	}
	// 端到端延迟：写库时刻减去事务在主库上的提交时刻。全量阶段读到的行没有提交时间，不计入。
	var lag time.Duration
	c := ch.GetCommitMicros()
	if c > 0 {
		lag = max(now.Sub(time.UnixMicro(c)), 0)
	}
	res := Stale
	if outcome.Changed() {
		res = Changed
	}
	ix.stats.add(res, lag, c > 0)
	return res, nil
}

// Handler 返回消费 cdc.order.orders 的 kafkax.Handler，每条消息的写库时限为 timeout。
//
// 错误的处理（配合 main 里 MaxAttempts 取最大值）：
//   - 消息不是 RowChange（反序列化失败）：kafkax.Permanent，直接进死信，分区继续。它不对应任何一张表的结构问题，
//     而 topic 是压缩保留的，同一个订单的下一个版本到来时索引会被修正。
//   - 表结构不符（ErrSchema）和写库失败：普通错误，原地重试、分区停在这条消息上，日志持续报错，不跳过。
func (ix *Indexer) Handler(timeout time.Duration) kafkax.Handler {
	return func(ctx context.Context, msg kafkax.Message) error {
		ch := &cdcv1.RowChange{}
		if err := proto.Unmarshal(msg.Value, ch); err != nil {
			return kafkax.Permanent(fmt.Errorf("indexer: unmarshal row change: %w", err))
		}
		wctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		_, err := ix.Apply(wctx, ch)
		if errors.Is(err, ErrSchema) {
			ix.log.Error("cdc row change does not match the index schema, consumption stalled until fixed",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key), "error", err)
		}
		return err
	}
}

// Stats 累计一段时间内的应用结果和端到端延迟。并发安全。
type Stats struct {
	mu sync.Mutex
	window
}

// window 是一个统计周期的数据。
type window struct {
	Changed           int64
	Stale             int64
	SkippedNoMerchant int64
	Deletes           int64
	Ignored           int64
	// LagCount 是计入延迟的条数（commit_micros 非 0 的写库），LagSum、LagMax 是它们的合计和最大值。
	LagCount int64
	LagSum   time.Duration
	LagMax   time.Duration
}

func (s *Stats) add(r Result, lag time.Duration, hasLag bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r {
	case Changed:
		s.Changed++
	case Stale:
		s.Stale++
	case SkippedNoMerchant:
		s.SkippedNoMerchant++
	case IgnoredDelete:
		s.Deletes++
	case Ignored:
		s.Ignored++
	}
	if hasLag {
		s.LagCount++
		s.LagSum += lag
		s.LagMax = max(s.LagMax, lag)
	}
}

// take 返回当前周期的数据并清零。
func (s *Stats) take() window {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.window
	s.window = window{}
	return w
}

func (w window) empty() bool {
	return w.Changed+w.Stale+w.SkippedNoMerchant+w.Deletes+w.Ignored == 0
}
