// Package apply 是把库存增量合并进 sku_stock 的后台任务，见设计文档 1d 第 3 节、ADR-0018。
//
// 预占、确认、释放只往 stock_pending 里记增量；这里周期性地取一批、按 SKU 合并、更新 sku_stock。
// 合并和“恰好一次”都在 store.ApplyPending 的一个事务里，本包只管节奏、卡住的 SKU 和日志。
package apply

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/palemoky/oppenheimer/pkg/worker"
	"github.com/palemoky/oppenheimer/services/inventory/internal/store"
)

// 默认值，对应配置 STOCK_APPLY_INTERVAL 和 STOCK_APPLY_BATCH。
const (
	// DefaultInterval 是没有积压时两轮之间的间隔，也就是 sku_stock 平时落后的量级。
	// 有积压时一轮里连着做，不等这个间隔。
	DefaultInterval = 20 * time.Millisecond
	// DefaultBatch 是一个事务最多应用的增量行数。一批的行在事务里是锁住的，太大会让事务变长；
	// 太小则合并的倍数低。1000 行的一批在本机约几毫秒。
	DefaultBatch = 1000
	// DefaultStuckRetry 是多久重试一次卡住的 SKU。卡住要靠人修数据，不值得每轮都试。
	DefaultStuckRetry = time.Second
)

// roundTimeout 是一轮的时限。一轮最多做 maxBatchesPerRound 个事务。
const (
	batchTimeout       = 5 * time.Second
	maxBatchesPerRound = 100
	// summaryInterval 是汇总日志的最小间隔。
	summaryInterval = 10 * time.Second
)

// Config 是 Applier 的配置，零值取默认。
type Config struct {
	Interval   time.Duration
	Batch      int
	StuckRetry time.Duration
	Logger     *slog.Logger
	// Now 是时钟，默认 time.Now。
	Now func() time.Time
}

// Applier 执行应用。Round 和 Drain 不能并发调用（worker.Loop 保证同一时刻只有一轮）。
//
// 多个实例各跑一个 Applier 是安全的，不需要 leader 锁：store.ApplyPending 靠增量行上的行锁互斥，
// 后到的等先到的提交，之后读到的是剩下的行。所以它也不依赖 Redis：闸门不可用时计数照常更新。
type Applier struct {
	store      *store.Store
	logger     *slog.Logger
	interval   time.Duration
	batch      int
	stuckRetry time.Duration
	now        func() time.Time

	// stuck 是已知卡住的 SKU 和原因。平时的轮次绕开它们，否则它们留在表头的增量会把每一批占满，
	// 别的 SKU 就再也轮不到了。每隔 stuckRetry 带上它们试一次，数据被修好后自动恢复。
	stuck     map[int64]string
	lastRetry time.Time

	// 汇总日志的累计值。
	sinceSummary summary
	lastSummary  time.Time
}

type summary struct {
	rows, skuUpdates, batches int
	maxAge                    time.Duration
}

// New 返回 Applier。
func New(st *store.Store, cfg Config) (*Applier, error) {
	a := &Applier{
		store:      st,
		logger:     cfg.Logger,
		interval:   cfg.Interval,
		batch:      cfg.Batch,
		stuckRetry: cfg.StuckRetry,
		now:        cfg.Now,
		stuck:      map[int64]string{},
	}
	if a.logger == nil {
		a.logger = slog.Default()
	}
	if a.interval <= 0 {
		a.interval = DefaultInterval
	}
	if a.batch == 0 {
		a.batch = DefaultBatch
	}
	if a.batch < 0 || a.batch > store.MaxApplyBatch {
		return nil, fmt.Errorf("apply: batch %d, want 1..%d", a.batch, store.MaxApplyBatch)
	}
	if a.stuckRetry <= 0 {
		a.stuckRetry = DefaultStuckRetry
	}
	if a.now == nil {
		a.now = time.Now
	}
	return a, nil
}

// NewLoop 把 Applier 包装成周期任务组件（实现 app.Component）。
func NewLoop(a *Applier) (*worker.Loop, error) {
	return worker.New(worker.Config{
		Name:       "stock-apply",
		Interval:   a.interval,
		Logger:     a.logger,
		RunOnStart: true,
	}, a.Round)
}

// Round 应用到没有积压为止（最多 maxBatchesPerRound 批，留出响应停止的机会）。
func (a *Applier) Round(ctx context.Context) error {
	for range maxBatchesPerRound {
		more, err := a.once(ctx)
		if err != nil {
			return err
		}
		if !more {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	a.maybeLogSummary(ctx)
	return nil
}

// Drain 一直应用到 stock_pending 里除了卡住的 SKU 之外没有别的行。给测试和停止前的收尾用。
func (a *Applier) Drain(ctx context.Context) error {
	for {
		more, err := a.once(ctx)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// Stuck 返回当前已知卡住的 SKU，升序。
func (a *Applier) Stuck() []int64 { return slices.Sorted(maps.Keys(a.stuck)) }

// once 应用一批。more 为 true 表示这一批是满的，后面可能还有。
func (a *Applier) once(ctx context.Context) (more bool, err error) {
	now := a.now()
	// 到了重试卡住的 SKU 的时候，这一批不绕开它们。
	retry := len(a.stuck) > 0 && now.Sub(a.lastRetry) >= a.stuckRetry
	var exclude []int64
	if !retry {
		exclude = a.Stuck()
	} else {
		a.lastRetry = now
	}

	bctx, cancel := context.WithTimeout(ctx, batchTimeout)
	defer cancel()
	res, err := a.store.ApplyPending(bctx, a.batch, exclude, now)
	if err != nil {
		return false, err
	}
	full := res.Fetched == a.batch

	a.sinceSummary.rows += res.Applied
	a.sinceSummary.skuUpdates += res.SKUs
	if res.Fetched > 0 {
		a.sinceSummary.batches++
		a.sinceSummary.maxAge = max(a.sinceSummary.maxAge, res.OldestAge)
	}

	nowStuck := make(map[int64]string, len(res.Stuck))
	for _, s := range res.Stuck {
		nowStuck[s.SKUID] = s.Reason
		if _, known := a.stuck[s.SKUID]; !known {
			// 这是 MySQL 这一侧发现超卖或恒等式被破坏的地方（设计文档 1d 的 3.4）：闸门放行的数量
			// 超过了 MySQL 里有的。订单已经成功，这里只能报出来等人处理。
			a.logger.ErrorContext(ctx, "stock deltas cannot be applied, sku is stuck until the stock is fixed",
				"sku_id", s.SKUID, "pending_rows", s.Rows, "reason", s.Reason)
		}
	}
	switch {
	case !retry:
		// 这一批绕开了已知卡住的 SKU，只可能新增。
		maps.Copy(a.stuck, nowStuck)
	case full:
		// 重试的这一批没有看完整张表：没出现在结果里的旧 SKU 可能只是没轮到，保守地留着。
		maps.Copy(a.stuck, nowStuck)
	default:
		// 看完了整张表：结果里没有的就是恢复了。
		for sku := range a.stuck {
			if _, still := nowStuck[sku]; !still {
				a.logger.InfoContext(ctx, "stuck sku recovered, its stock deltas are applied", "sku_id", sku)
			}
		}
		a.stuck = nowStuck
	}

	// 满批里如果一行都没应用（全是卡住的 SKU），再来一批也是同样的行，不算“还有”。
	return full && res.Applied > 0, nil
}

func (a *Applier) maybeLogSummary(ctx context.Context) {
	now := a.now()
	if a.sinceSummary.batches == 0 || now.Sub(a.lastSummary) < summaryInterval {
		return
	}
	s := a.sinceSummary
	a.sinceSummary, a.lastSummary = summary{}, now
	// rows / sku_updates 是合并的倍数：每次更新 sku_stock 的一行顶了多少笔逐单的更新。
	a.logger.InfoContext(ctx, "stock deltas applied",
		"rows", s.rows, "sku_updates", s.skuUpdates, "batches", s.batches,
		"max_age_ms", s.maxAge.Milliseconds(), "stuck_skus", len(a.stuck))
}
