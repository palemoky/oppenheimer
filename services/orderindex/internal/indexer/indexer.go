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
		ix.stats.add(Ignored, false, 0, false)
		return Ignored, nil
	}
	// archived 表示这是一条归档产生的 DELETE：结果照常归入 Changed/Stale/SkippedNoMerchant，另外单独计数，
	// 用来观察归档对索引消费的冲击（设计文档 1c 6.7）。
	var (
		img      map[string]*cdcv1.Value
		imgName  string
		archived bool
	)
	switch ch.GetOp() {
	case cdcv1.Op_OP_INSERT, cdcv1.Op_OP_UPDATE, cdcv1.Op_OP_READ:
		img, imgName = ch.GetAfter(), "after"
	case cdcv1.Op_OP_DELETE:
		// before 缺失时 decodeRow 报 ErrSchema，和 INSERT 缺 after 一样停住消费：缺映像说明 CDC 或
		// binlog_row_image 的配置不对，跳过的话重建时这个归档订单会悄悄丢掉。
		img, imgName, archived = ch.GetBefore(), "before", true
	default:
		ix.stats.add(Ignored, false, 0, false)
		return Ignored, nil
	}

	row, ok, err := decodeRow(img)
	if err != nil {
		return 0, fmt.Errorf("indexer: %s op=%s slot=%d gtid=%q %s image: %w",
			ch.GetSource(), ch.GetOp(), ch.GetSlot(), ch.GetGtid(), imgName, err)
	}
	if !ok {
		ix.stats.add(SkippedNoMerchant, archived, 0, false)
		return SkippedNoMerchant, nil
	}

	now := ix.now().UTC()
	outcome, err := ix.store.Upsert(ctx, row, now)
	if err != nil {
		return 0, fmt.Errorf("indexer: %w", err)
	}
	// 端到端延迟：写库时刻减去事务在主库上的提交时刻。全量阶段读到的行没有提交时间，不计入。
	// DELETE 也计入：归档期间它们排在在线变更前面，延迟上升正是要量的东西。
	var lag time.Duration
	c := ch.GetCommitMicros()
	if c > 0 {
		lag = max(now.Sub(time.UnixMicro(c)), 0)
	}
	res := Stale
	if outcome.Changed() {
		res = Changed
	}
	ix.stats.add(res, archived, lag, c > 0)
	return res, nil
}

// Handler 返回消费 cdc.order.orders 的 kafkax.Handler，每条消息的写库时限为 timeout。
//
// 错误的处理（配合 main 里 MaxAttempts 取最大值）：
//   - 消息不是 RowChange（反序列化失败）：kafkax.Permanent，直接进死信，分区继续。它不对应任何一张表的结构问题，
//     而 topic 是压缩保留的，同一个订单的下一个版本到来时索引会被修正。
//   - 表结构不符（ErrSchema）和写库失败：普通错误，原地重试，日志持续报错，不跳过。分区内并发时停住的是
//     这条消息所在的 lane，位点停在它前面；其他 lane 继续，直到未提交的消息数到 MaxUncommitted 整个分区才停。
//     死锁、锁等待超时也走这条路：InnoDB 已回滚这条单语句的事务，原样重试即可。
//
// 并发安全：分区内并发（kafkax ConsumerConfig.Concurrency > 1）时它会被多个 goroutine 同时调用。
// 每条消息只用自己的局部变量，没有跨消息的状态；Store 的语句在构造时渲染好、只读，连接由 database/sql
// 的池分配；Stats 有锁。同一个订单的消息由 kafkax 保证串行，不同订单之间的先后由版本号兜住。
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
	Ignored           int64
	// Archived 是应用了的 DELETE（归档）条数。它们同时计在 Changed/Stale/SkippedNoMerchant 里，
	// 不是另一类结果，所以不参与 empty 的求和。
	Archived int64
	// LagCount 是计入延迟的条数（commit_micros 非 0 的写库），LagSum、LagMax 是它们的合计和最大值。
	LagCount int64
	LagSum   time.Duration
	LagMax   time.Duration
}

func (s *Stats) add(r Result, archived bool, lag time.Duration, hasLag bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if archived {
		s.Archived++
	}
	switch r {
	case Changed:
		s.Changed++
	case Stale:
		s.Stale++
	case SkippedNoMerchant:
		s.SkippedNoMerchant++
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
	return w.Changed+w.Stale+w.SkippedNoMerchant+w.Ignored == 0
}
