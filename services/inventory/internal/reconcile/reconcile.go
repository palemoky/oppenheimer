// Package reconcile 是库存闸门（Redis）与 MySQL 的对账任务，见设计文档 6.6。
//
// 稳定状态下每个 SKU 满足：
//
//	Redis: avail + held  ==  MySQL: available + reserved
//
// 两边都是“还没卖掉的数量”。受理（Hold）、预占、释放、归还都只在一侧内部挪动数量，
// 两边各自不变；只有支付确认让 MySQL 一侧先减少，随后的结清（Settle）让 Redis 一侧减少同样的数量。
//
// 偏差 d = (avail + held) - (available + reserved)。读 Redis 和读 MySQL 不是同一时刻，
// 夹在两次读之间的确认或结清会让这一轮看到一个瞬时的 d，所以单轮的 d 不能作为修正的依据：
// 同一个 SKU 连续若干轮（默认 3）得到相同的非零 d 才认定为真实偏差，用增量修正 avail。
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/palemoky/Oppenheimer/pkg/stockgate"
	"github.com/palemoky/Oppenheimer/pkg/worker"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/store"
)

// 默认值，对应配置 GATE_RECONCILE_INTERVAL 和 GATE_RECONCILE_ROUNDS。
const (
	DefaultInterval = 5 * time.Second
	DefaultRounds   = 3
)

// skuTimeout 是对账一个 SKU（读 Redis、读 MySQL、必要时修正）的时限。
// 一个 SKU 卡住不应拖住整轮，更不应拖住停止流程。
const skuTimeout = 2 * time.Second

// Config 是对账任务的配置。
type Config struct {
	// Interval 是两轮之间的间隔，<= 0 时取 DefaultInterval。
	Interval time.Duration
	// Rounds 是认定偏差为真实偏差所需的连续轮数，<= 0 时取 DefaultRounds。
	Rounds int
	// Logger 为 nil 时用 slog.Default()。
	Logger *slog.Logger
}

// Reconciler 执行对账。Round 不能并发调用（worker.Loop 保证同一时刻只有一轮）。
type Reconciler struct {
	gate     *stockgate.Gate
	store    *store.Store
	logger   *slog.Logger
	interval time.Duration
	rounds   int
	now      func() time.Time

	// streaks 记录每个 SKU 最近连续几轮得到的同一个非零偏差。
	streaks map[int64]streak
	// lastEnd 是上一轮结束的时刻，用来发现“中间有轮次没跑”（见 Round）。
	lastEnd time.Time
}

type streak struct {
	d int64
	n int
}

// New 返回一个对账器。
func New(gate *stockgate.Gate, st *store.Store, cfg Config) *Reconciler {
	r := &Reconciler{
		gate:     gate,
		store:    st,
		logger:   cfg.Logger,
		interval: cfg.Interval,
		rounds:   cfg.Rounds,
		now:      time.Now,
		streaks:  map[int64]streak{},
	}
	if r.logger == nil {
		r.logger = slog.Default()
	}
	if r.interval <= 0 {
		r.interval = DefaultInterval
	}
	if r.rounds <= 0 {
		r.rounds = DefaultRounds
	}
	return r
}

// NewLoop 把对账器包装成周期任务组件（实现 app.Component）。
//
// 多个实例同时对账会对同一个偏差各修一次（修正是增量，两次就是两倍），
// 所以部署多副本时要配 Guard（分布式锁的 Leader）。锁在持有者卡顿超过 TTL 时可能短暂双主，
// 这时最坏是多修一次；修正有上下限，多修的部分会在之后几轮被反向修回。
func NewLoop(r *Reconciler, guard worker.Guard) (*worker.Loop, error) {
	return worker.New(worker.Config{
		Name:     "gate-reconcile",
		Interval: r.interval,
		Logger:   r.logger,
		Guard:    guard,
	}, r.Round)
}

// Round 执行一轮对账：遍历闸门里已装载的 SKU，逐个比较并在需要时修正。
// 单个 SKU 失败不影响其他 SKU，所有失败合并返回。
func (r *Reconciler) Round(ctx context.Context) error {
	// “连续”指相邻的轮次。如果上一轮结束到现在远超间隔（失去 leader 一段时间后又拿回来、进程长时间停顿），
	// 中间发生过什么不得而知，旧的计数不能和这一轮拼成“连续”。从上一轮结束算起而不是开始：
	// worker.Loop 的间隔就是从结束算的，一轮本身跑得慢（SKU 多）不应被当成跳过了轮次。
	if !r.lastEnd.IsZero() && r.now().Sub(r.lastEnd) > 2*r.interval {
		clear(r.streaks)
	}
	defer func() { r.lastEnd = r.now() }()

	skus, err := r.gate.SKUs(ctx)
	if err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}

	next := make(map[int64]streak, len(r.streaks))
	var errs []error
	for _, sku := range skus {
		if err := r.check(ctx, sku, next); err != nil {
			errs = append(errs, err)
		}
	}
	// 只保留这一轮仍有偏差的 SKU：偏差消失、SKU 不再装载、读取失败的，计数都从头开始。
	// 读取失败也清零，是因为这一轮没有观测，下一轮和上一轮已经不相邻。
	r.streaks = next
	return errors.Join(errs...)
}

// check 对账一个 SKU，把仍需跟踪的偏差写进 next。
func (r *Reconciler) check(ctx context.Context, sku int64, next map[int64]streak) error {
	ctx, cancel := context.WithTimeout(ctx, skuTimeout)
	defer cancel()

	// 先读 Redis 再读 MySQL，两次读挨在一起，尽量缩小中间能插进操作的窗口。
	gs, loaded, err := r.gate.Stock(ctx, sku)
	if err != nil {
		return fmt.Errorf("reconcile sku %d: %w", sku, err)
	}
	if !loaded {
		// 在 SKUs 和这里之间被删掉了（只有人工清理会这样），下一轮自然不会再看到。
		return nil
	}
	ms, found, err := r.store.GetStock(ctx, sku)
	if err != nil {
		return fmt.Errorf("reconcile sku %d: %w", sku, err)
	}
	if !found {
		// 闸门里有而 MySQL 里没有：MySQL 是事实来源，这个 SKU 要么被删了，要么闸门被写错了。
		// 不知道该修成什么，交给人。它的 avail 不会让 MySQL 超卖（Reserve 会报 SKU 不存在）。
		r.logger.ErrorContext(ctx, "sku in stock gate but not in mysql", "sku_id", sku,
			"gate_avail", gs.Avail, "gate_held", gs.Held)
		return nil
	}

	d := (gs.Avail + gs.Held) - (ms.Available + ms.Reserved)
	if d == 0 {
		return nil
	}
	s := r.streaks[sku]
	if s.d == d {
		s.n++
	} else {
		// 偏差的值变了，说明两边还在变化，之前的观测不能证明这是稳定的偏差，重新计数。
		s = streak{d: d, n: 1}
	}
	if s.n < r.rounds {
		next[sku] = s
		r.logger.DebugContext(ctx, "stock gate deviation observed", "sku_id", sku, "deviation", d, "rounds", s.n)
		return nil
	}

	// 连续 rounds 轮得到同一个 d，认定为真实偏差。修正后不再记入 next：
	// 修正如果没有完全消除偏差（被上限截住），下一轮重新从 1 开始数，而不是每轮都修。
	after, ok, err := r.gate.Adjust(ctx, sku, -d, ms.Available)
	if err != nil {
		next[sku] = s // 没修成，保留计数，下一轮再试
		return fmt.Errorf("reconcile sku %d: %w", sku, err)
	}
	if !ok {
		return nil
	}
	attrs := []any{
		"sku_id", sku, "deviation", d, "rounds", s.n,
		"avail_before", gs.Avail, "avail_after", after, "held", gs.Held,
		"mysql_available", ms.Available, "mysql_reserved", ms.Reserved,
	}
	if after == gs.Avail {
		// 被上限（MySQL 的 available）或下限 0 截住，什么都没改。典型场景是 Redis 丢数据后，
		// 在途订单的占用没了而 MySQL 还有预占：偏差真实存在，但不能把 avail 调得比 available 大，
		// 要等这些订单结束。只是持续存在，不是新发生的错误，所以记 warn。
		r.logger.WarnContext(ctx, "stock gate deviation persists, correction capped", attrs...)
		return nil
	}
	r.logger.ErrorContext(ctx, "stock gate deviation corrected", attrs...)
	return nil
}
