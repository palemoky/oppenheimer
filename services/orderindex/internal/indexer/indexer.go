// Package indexer 把 CDC 发出的订单行变更（cdc.v1.RowChange）写进按商家分片的索引表。
//
// 规则（设计文档 1c 4.6、4.8）：
//   - 只处理逻辑表 orders；INSERT、UPDATE、READ 一视同仁，只看 after 映像。
//   - DELETE 用 before 映像，和上面走完全相同的带版本号的 upsert，绝不删除索引行（设计文档 1c 6.7）。
//     约定：orders 的 DELETE 只表示“这一行离开了热库”（归档），before 是它的最终映像；订单在业务上从不删除，
//     商家索引保留归档订单的行。平时这次 upsert 是空操作（索引里已是这个版本）；但 topic 是压缩保留的，
//     归档订单在 topic 里只剩这条 DELETE，而 CDC 的全量只读热库，索引从头重建时它是这个订单唯一的来源，
//     所以不能忽略。
//   - merchant_id 为 NULL 的订单不进索引（还没预占成功）。
//   - 幂等和定序靠源行的 version 列：只接受更高版本的映像，不依赖到达顺序（见 store.UpsertRows）。
//   - 需要的列缺失或类型不对是源表结构被改坏了，返回 ErrSchema，消费停住，不跳过。
//   - 不认识的列、表、op 忽略。
//
// 按批处理（设计文档 1d 2.11）：一批消息合并成每个索引库一个事务、每张索引表一条多行语句，
// 把提交（刷盘）的次数从“每条消息一次”降到“每批每库一次”。
package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

// SourceTable 是要同步的逻辑表名。
const SourceTable = "orders"

// DefaultWriteTimeout 是处理一批消息（一次写库，两个库并行）的时限。
// 一批最多几百行，正常几十毫秒内写完；超时说明库有问题，返回错误让消费者重试。
const DefaultWriteTimeout = 5 * time.Second

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

// changeKind 是一条变更解码后的去向。
type changeKind int

const (
	// kindIgnored：表名或 op 不认识，忽略。
	kindIgnored changeKind = iota + 1
	// kindNoMerchant：merchant_id 为 NULL，不进索引。
	kindNoMerchant
	// kindRow：要写进索引的一行。
	kindRow
)

// decodeChange 解码一条变更。archived 表示这是一条归档产生的 DELETE（只对 kindNoMerchant、kindRow 有意义）。
// 返回的错误包着 ErrSchema 时说明消息与表结构不符，重试不会成功。
func decodeChange(ch *cdcv1.RowChange) (row store.Row, kind changeKind, archived bool, err error) {
	return decodeChangeWith(ch, decodeRow)
}

// decodeChangeWith 是 decodeChange，行映像交给 rowOf 解码（它和 decodeRow 的约定相同）。
func decodeChangeWith(ch *cdcv1.RowChange, rowOf func(map[string]*cdcv1.Value) (store.Row, bool, error)) (
	row store.Row, kind changeKind, archived bool, err error,
) {
	if ch.GetTable() != SourceTable {
		return store.Row{}, kindIgnored, false, nil
	}
	var (
		img     map[string]*cdcv1.Value
		imgName string
	)
	switch ch.GetOp() {
	case cdcv1.Op_OP_INSERT, cdcv1.Op_OP_UPDATE, cdcv1.Op_OP_READ:
		img, imgName = ch.GetAfter(), "after"
	case cdcv1.Op_OP_DELETE:
		// before 缺失时 decodeRow 报 ErrSchema，和 INSERT 缺 after 一样停住消费：缺映像说明 CDC 或
		// binlog_row_image 的配置不对，跳过的话重建时这个归档订单会悄悄丢掉。
		img, imgName, archived = ch.GetBefore(), "before", true
	default:
		return store.Row{}, kindIgnored, false, nil
	}
	row, ok, err := rowOf(img)
	if err != nil {
		return store.Row{}, 0, false, fmt.Errorf("indexer: %s op=%s slot=%d gtid=%q %s image: %w",
			ch.GetSource(), ch.GetOp(), ch.GetSlot(), ch.GetGtid(), imgName, err)
	}
	if !ok {
		return store.Row{}, kindNoMerchant, archived, nil
	}
	return row, kindRow, archived, nil
}

// decodedChange 是一条变更解码后的结果，含义见 decodeChange。
type decodedChange struct {
	row      store.Row
	kind     changeKind
	archived bool
}

// decodeInOrder 按顺序逐条解码一批变更，返回和 changes 一一对应的结果；遇到第一条出错的就返回它的错误。
func decodeInOrder(changes []*cdcv1.RowChange) ([]decodedChange, error) {
	out := make([]decodedChange, len(changes))
	for i, ch := range changes {
		row, kind, archived, err := decodeChange(ch)
		if err != nil {
			return nil, err
		}
		out[i] = decodedChange{row: row, kind: kind, archived: archived}
	}
	return out, nil
}

// rowKey 是索引行的主键。
type rowKey struct{ merchant, order int64 }

// Apply 把一批变更应用到索引，返回这一批的统计（同时累加进 Stats）。
//
// 步骤：
//  1. 逐条解码。任何一条出错时整批返回那个错误（包着 ErrSchema 时说明消息与表结构不符），什么都不写、不计数。
//  2. 批内合并：同一个订单（主键）出现多次时只留版本最高的映像，版本相同的内容相同，留哪个都行。
//     被合并掉的映像本来写进去也是空操作（版本不高于留下的那个），统计里算 Stale。
//  3. 交给 store.UpsertRows：每个库一个事务、每张表一条多行语句。两个库不是原子的，
//     一个失败时整批返回错误、不计数，重做时成功的那一半是空操作（会计成 Stale）。
//
// 只有一条消息的批走同一条路径，没有另一套写入实现。
func (ix *Indexer) Apply(ctx context.Context, changes []*cdcv1.RowChange) (Counts, error) {
	var (
		c       Counts
		latest  = make(map[rowKey]store.Row, len(changes))
		commits []int64 // 要写进索引的消息的 commit_micros，写完后算延迟
		indexed int64   // 有商家、要写进索引的消息数（合并前）
	)
	decoded, err := decodeChanges(changes)
	if err != nil {
		return Counts{}, err
	}
	for i, d := range decoded {
		ch, row := changes[i], d.row
		if d.archived {
			c.Archived++
		}
		switch d.kind {
		case kindIgnored:
			c.Ignored++
			continue
		case kindNoMerchant:
			c.SkippedNoMerchant++
			continue
		case kindRow:
		}
		indexed++
		// 全量阶段读到的行没有提交时间，不计入延迟。DELETE 也计入：归档期间它们排在在线变更前面，延迟上升正是要量的东西。
		if cm := ch.GetCommitMicros(); cm > 0 {
			commits = append(commits, cm)
		}
		k := rowKey{row.MerchantID, row.OrderID}
		if old, ok := latest[k]; !ok || row.Version > old.Version {
			latest[k] = row
		}
	}

	if len(latest) > 0 {
		// synced_at 是写入时刻；同一批共用一个，表示“这一批写进去的时刻”。
		start := time.Now()
		res, err := ix.store.UpsertRows(ctx, slices.Collect(maps.Values(latest)), ix.now().UTC())
		if err != nil {
			return Counts{}, fmt.Errorf("indexer: %w", err)
		}
		applyDuration.Record(ctx, time.Since(start).Seconds())
		batchSize.Record(ctx, res.Rows)
		c.Written = res.Rows
		c.Merged = indexed - res.Rows
		c.Changed = res.Changed
		c.Stale = indexed - res.Changed
		rows.Add(ctx, c.Changed, outcomeApplied)
		rows.Add(ctx, c.Stale, outcomeStale)
	}
	// 端到端延迟：写完的时刻减去事务在主库上的提交时刻。取写完而不是写之前，批写的耗时也是延迟的一部分。
	done := ix.now()
	for _, cm := range commits {
		lag := max(done.Sub(time.UnixMicro(cm)), 0)
		c.LagCount++
		c.LagSum += lag
		c.LagMax = max(c.LagMax, lag)
	}
	ix.stats.add(c)
	return c, nil
}

// Handler 返回消费 cdc.order.orders 的 kafkax.BatchHandler，每一批的写库时限为 timeout。
//
// 错误的处理（配合 main 里 MaxAttempts 取最大值）。一批（多于一条）返回任何错误，kafkax 都会把这一批
// 一条一条地重新交给它，下面说的是只有一条消息时的含义，和按批之前逐条的 handler 完全一致：
//   - 消息不是 RowChange（反序列化失败）：kafkax.Permanent，直接进死信，分区继续。它不对应任何一张表的结构问题，
//     而 topic 是压缩保留的，同一个订单的下一个版本到来时索引会被修正。
//   - 表结构不符（ErrSchema）和写库失败：普通错误，原地重试，日志持续报错，不跳过，分区停在这条消息上。
//     死锁、锁等待超时也走这条路：事务已整个回滚，原样重试即可。
//
// 多条的批里哪一条坏了不用 handler 判断：返回第一个遇到的错误即可，逐条回退会把坏的那条隔离出来，
// 其余的各自成功。
//
// 并发安全：每个分区一个 worker，不同分区的批会同时调用它。每一批只用自己的局部变量；
// Store 的语句模板在构造时渲染好、只读，连接由 database/sql 的池分配；Stats 有锁。
func (ix *Indexer) Handler(timeout time.Duration) kafkax.BatchHandler {
	return func(ctx context.Context, msgs []kafkax.Message) error {
		changes, err := unmarshalChanges(msgs)
		if err != nil {
			return err
		}
		wctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		_, err = ix.Apply(wctx, changes)
		// 多条的批出错时 kafkax 会记一条告警再逐条回退，这里只在逐条（会原地重试、停住分区）时报错，
		// 避免同一个问题在日志里报两遍，也只有这时才知道是哪一条。
		if errors.Is(err, ErrSchema) && len(msgs) == 1 {
			msg := msgs[0]
			ix.log.ErrorContext(ctx, "cdc row change does not match the index schema, consumption stalled until fixed",
				"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key), "error", err)
		}
		return err
	}
}

// unmarshalInOrder 按顺序逐条反序列化一批消息，返回和 msgs 一一对应的变更；遇到第一条反序列化不了的就返回它的错误。
func unmarshalInOrder(msgs []kafkax.Message) ([]*cdcv1.RowChange, error) {
	changes := make([]*cdcv1.RowChange, len(msgs))
	for i, msg := range msgs {
		ch, err := unmarshalOne(msg)
		if err != nil {
			return nil, err
		}
		changes[i] = ch
	}
	return changes, nil
}

// unmarshalOne 反序列化一条消息。消息不是 RowChange 时返回 kafkax.Permanent，理由见 Handler。
func unmarshalOne(msg kafkax.Message) (*cdcv1.RowChange, error) {
	ch := &cdcv1.RowChange{}
	if err := proto.Unmarshal(msg.Value, ch); err != nil {
		return nil, kafkax.Permanent(fmt.Errorf("indexer: unmarshal row change (partition %d offset %d): %w",
			msg.Partition, msg.Offset, err))
	}
	return ch, nil
}

// Stats 累计一段时间内的应用结果和端到端延迟。并发安全。
type Stats struct {
	mu sync.Mutex
	Counts
}

// Counts 是一批或一个统计周期的数据。除 Changed、Stale 外都是精确的。
type Counts struct {
	// Changed 是改变了索引的行数（新插入的 + 被更高版本覆盖的）的估计，可能偏大，不会偏小。
	// 多行语句只给受影响行数之和，分不开插入和更新，估法和误差见 store.WriteResult.Changed：
	// 正常消费（没有重放）和从头重建时是精确的，同一条语句里既有覆盖又有旧版本时偏大。
	Changed int64
	// Stale 是有商家、但没有改变索引的消息数：批内被合并掉的旧映像（精确）加上写进去行没变的（估计，可能偏小）。
	// Changed + Stale 恰好等于有商家的消息数，和逐条处理时一样。
	Stale int64
	// Written 是合并后实际写进索引库的行数。
	Written int64
	// Merged 是批内因同一个订单有更高版本而被合并掉、没有写库的映像数（已算在 Stale 里）。
	// Written + Merged = Changed + Stale。
	Merged            int64
	SkippedNoMerchant int64
	Ignored           int64
	// Archived 是应用了的 DELETE（归档）条数。它们同时计在 Changed/Stale/SkippedNoMerchant 里，
	// 不是另一类结果，所以不参与 empty 的求和。
	Archived int64
	// LagCount 是计入延迟的条数（commit_micros 非 0 的写库），LagSum、LagMax 是它们的合计和最大值。
	LagCount int64
	LagSum   time.Duration
	LagMax   time.Duration
}

func (s *Stats) add(c Counts) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Changed += c.Changed
	s.Stale += c.Stale
	s.Written += c.Written
	s.Merged += c.Merged
	s.SkippedNoMerchant += c.SkippedNoMerchant
	s.Ignored += c.Ignored
	s.Archived += c.Archived
	s.LagCount += c.LagCount
	s.LagSum += c.LagSum
	s.LagMax = max(s.LagMax, c.LagMax)
}

// take 返回当前周期的数据并清零。
func (s *Stats) take() Counts {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.Counts
	s.Counts = Counts{}
	return c
}

func (c Counts) empty() bool {
	return c.Changed+c.Stale+c.SkippedNoMerchant+c.Ignored == 0
}
