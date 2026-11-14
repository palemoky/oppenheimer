package service

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/pkg/worker"
)

// ReaperConfig 是回收任务的配置，零值字段使用默认值。
type ReaperConfig struct {
	// Interval 是两轮之间的间隔，默认 10 秒（HOLD_REAP_INTERVAL）。
	Interval time.Duration
	// Grace 是占用多久没被认领就回收，默认 1 分钟（HOLD_REAP_GRACE）。
	// 它只是让正常的“受理 → 消费”有时间完成，正确性不依赖它：回收与认领在 Redis 里互斥。
	Grace time.Duration
	// BatchSize 是每轮最多检查的占用数，默认 1000。
	BatchSize int
	// Guard 可选，决定这一轮是否运行。多副本时用分布式锁让只有一个副本在回收（设计文档 7.1）。
	Guard worker.Guard
}

// 回收任务的默认配置。
const (
	DefaultReapInterval  = 10 * time.Second
	DefaultReapGrace     = time.Minute
	DefaultReapBatchSize = 1000
)

// reapCallTimeout 是回收任务每次访问 Redis 的时限。
const reapCallTimeout = time.Second

// Reaper 回收孤儿占用（设计文档 6.4），实现 app.Component：
// 受理时 Hold 成功、消息却没发出去（进程在两步之间崩溃），占用就没人认领，数量永远少一块。
//
// 它只归还未被认领的占用（ReturnIfUnclaimed），与落库时的 Claim 互斥：
// 回收先得手，消费者 Claim 不到就丢弃消息；认领先得手，回收跳过它，占用由订单的终态归还。
// 所以即使消费积压超过了宽限期，也不会出现“占用还了、订单又在 MySQL 里预占成功”。
//
// 多个副本同时运行是安全的：每笔占用的归还在 Redis 里原子执行，只有第一次生效。
// 所以 Guard 只是为了省事，锁不可用时应当放行（lock.FailOpen）。
type Reaper struct {
	svc    *Service
	cfg    ReaperConfig
	logger *slog.Logger
	loop   *worker.Loop

	// 下面两个字段只在 RunOnce 里读写。RunOnce 由 loop 串行调用。
	// failing 表示上一轮列占用时闸门不可用；holdUntil 之前不回收未认领的占用，见 RunOnce。
	failing   bool
	holdUntil time.Time

	// queued 是最近一次读到的闸门队列长度（refreshQueued），给 order.gate.* 指标用；从没读到过时为 nil。
	// RunOnce 写，导出指标的 goroutine 读。
	queued atomic.Pointer[queuedHolds]
}

// queuedHolds 是某一时刻闸门里的占用个数，见 stockgate.Gate.Queued。
type queuedHolds struct {
	unclaimed, claimed int64
	// at 是读到它的时刻（Service 的时钟）。
	at time.Time
}

// queuedStaleRounds 是 queued 多少个回收间隔没有更新就不再上报：这个实例不是 leader 了（回收任务不跑），
// 或者闸门一直不可用。上报一个旧值会让人以为队列停在那里，不如不报。
const queuedStaleRounds = 3

// NewReaper 返回使用 svc 的闸门和时钟的回收任务。
func NewReaper(svc *Service, cfg ReaperConfig) (*Reaper, error) {
	cfg.Interval = cmp.Or(cfg.Interval, DefaultReapInterval)
	cfg.Grace = cmp.Or(cfg.Grace, DefaultReapGrace)
	cfg.BatchSize = cmp.Or(cfg.BatchSize, DefaultReapBatchSize)
	r := &Reaper{svc: svc, cfg: cfg, logger: svc.logger.With("component", "reaper")}
	loop, err := worker.New(worker.Config{
		Name:     "hold-reaper",
		Interval: cfg.Interval,
		Logger:   r.logger,
		Guard:    cfg.Guard,
	}, func(ctx context.Context) error {
		r.RunOnce(ctx)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("new reaper: %w", err)
	}
	r.loop = loop
	return r, nil
}

// Name 实现 app.Component。
func (r *Reaper) Name() string { return r.loop.Name() }

// Start 实现 app.Component。
func (r *Reaper) Start(ctx context.Context) error { return r.loop.Start(ctx) }

// Stop 实现 app.Component：等当前这一轮做完。中途被中断是安全的，没处理的占用下一轮还在。
func (r *Reaper) Stop(ctx context.Context) error { return r.loop.Stop(ctx) }

// ReapStats 是一轮回收的结果。
type ReapStats struct {
	// Returned 是归还的孤儿占用数；Claimed 是列出之后被消费者抢先认领的数量；
	// Stuck 是已认领却长期没有结束、需要人工检查的数量。
	Returned, Claimed, Stuck int
}

// RunOnce 执行一轮回收。单笔占用失败只记日志，不中断整轮。
//
// 闸门恢复之后的半个宽限期内不回收未认领的占用（设计文档 1d 的 6.4）。回收任务按“受理了多久还没被认领”
// 判断孤儿，前提是落库的消费者一直在认领。闸门不可用期间这个前提不成立：消费者认领不了，排队订单的
// 等待时长却一直在涨。Redis 一恢复就照常回收的话，回收任务会和刚恢复的消费者抢着处理这些订单，
// 抢赢了，用户拿到过 202 的订单就没了。等半个宽限期让消费者先把队列排空（队列长度有上限，见背压），
// 真正的孤儿只是晚一点被归还。
//
// “闸门不可用过”有两个来源：本进程里闸门的熔断器打开过（受理、查询、落库都会喂它），
// 或者回收任务自己上一轮列占用失败（这个实例可能只跑周期任务，没有别的调用去喂熔断器）。
// 两个都是本进程的见闻：消费者在别的实例上、而只有那个实例连不上 Redis 时，这里看不到，不会等。
//
// 每轮最后还顺带读一次闸门的队列长度给指标用（refreshQueued），它不影响这一轮的结果。
func (r *Reaper) RunOnce(ctx context.Context) ReapStats {
	defer r.refreshQueued(ctx)
	var stats ReapStats
	now := r.svc.now()
	ids, err := r.holdsBefore(ctx, now.Add(-r.cfg.Grace))
	if err != nil {
		if !r.failing || !isBreakerOpen(err) {
			r.logger.ErrorContext(ctx, "list holds failed", "error", err)
		}
		r.failing = true
		return stats
	}
	holdOff := r.cfg.Grace / 2
	if r.failing {
		r.failing = false
		r.holdUntil = now.Add(holdOff)
	}
	if now.Before(r.holdUntil) || r.svc.gateRecoveredWithin(holdOff) {
		if len(ids) > 0 {
			r.logger.InfoContext(ctx, "stock gate just recovered, not reaping unclaimed holds yet", "candidates", len(ids))
		}
		ids = nil
	}
	for _, id := range ids {
		res, err := r.returnIfUnclaimed(ctx, id)
		if err != nil {
			r.logger.ErrorContext(ctx, "reap hold failed", "order_id", id, "error", err)
			continue
		}
		switch res {
		case stockgate.ReapReturned:
			stats.Returned++
			reclaimed(ctx, reclaimOrphanHold)
			r.logger.WarnContext(ctx, "orphan hold returned", "order_id", id)
		case stockgate.ReapClaimed:
			// 列出之后刚被消费者认领：它赢了，占用由订单的终态归还。
			stats.Claimed++
		case stockgate.ReapMissing:
			// 列出之后被归还了（受理方发消息失败后撤销）。
		}
	}

	// 已认领却超过支付时限两倍还没结束的占用：认领之后流程卡住了（进程崩溃且消息进了死信），
	// 没有订单的终态会去归还它。不自动归还（不知道 MySQL 里有没有预占），记 error 等人工处理。
	stuck, err := r.claimedBefore(ctx, now.Add(-2*r.svc.paymentTimeout))
	if err != nil {
		r.logger.ErrorContext(ctx, "list stuck holds failed", "error", err)
		return stats
	}
	stats.Stuck = len(stuck)
	for _, id := range stuck {
		r.logger.ErrorContext(ctx, "claimed hold outlived twice the payment timeout, needs manual check", "order_id", id)
	}
	return stats
}

func (r *Reaper) claimedBefore(ctx context.Context, before time.Time) (ids []int64, err error) {
	err = r.svc.gateCallWithin(ctx, reapCallTimeout, func(ctx context.Context) error {
		var err error
		ids, err = r.svc.gate.ClaimedBefore(ctx, before, r.cfg.BatchSize)
		return err
	})
	return ids, err
}

func (r *Reaper) holdsBefore(ctx context.Context, before time.Time) (ids []int64, err error) {
	err = r.svc.gateCallWithin(ctx, reapCallTimeout, func(ctx context.Context) error {
		var err error
		ids, err = r.svc.gate.HoldsBefore(ctx, before, r.cfg.BatchSize)
		return err
	})
	return ids, err
}

func (r *Reaper) returnIfUnclaimed(ctx context.Context, orderID int64) (res stockgate.ReapResult, err error) {
	err = r.svc.gateCallWithin(ctx, reapCallTimeout, func(ctx context.Context) error {
		var err error
		res, err = r.svc.gate.ReturnIfUnclaimed(ctx, orderID)
		return err
	})
	return res, err
}

// refreshQueued 读闸门里的占用个数存进 r.queued，给 order.gate.unclaimed_holds 和 claimed_holds 用（metrics.go）。
// 只在回收任务这里读：它每轮本来就要访问闸门，受理、落库、查询的路径上不加任何查询。
//
// 和别的闸门调用一样经过熔断器。这一轮列占用已经失败（r.failing）时不读，闸门坏着不多等一次时限、
// 也不多给熔断器记一次失败。读失败只记 DEBUG，不影响回收；值不更新，过一会儿指标就不再上报（queuedStaleRounds）。
func (r *Reaper) refreshQueued(ctx context.Context) {
	if r.failing {
		return
	}
	var unclaimed, claimed int64
	err := r.svc.gateCallWithin(ctx, reapCallTimeout, func(ctx context.Context) error {
		var err error
		unclaimed, claimed, err = r.svc.gate.Queued(ctx)
		return err
	})
	if err != nil {
		r.logger.DebugContext(ctx, "count queued holds failed", "error", err)
		return
	}
	r.queued.Store(&queuedHolds{unclaimed: unclaimed, claimed: claimed, at: r.svc.clock()})
}

// queuedNow 返回最近一次读到的占用个数。ok 为 false 表示从没读到过，或者已经 queuedStaleRounds 个
// 回收间隔没有更新（不知道现在是多少）。只读原子变量，可以在导出指标的 goroutine 里调用。
func (r *Reaper) queuedNow() (q queuedHolds, ok bool) {
	p := r.queued.Load()
	if p == nil || r.svc.clock().Sub(p.at) > queuedStaleRounds*r.cfg.Interval {
		return queuedHolds{}, false
	}
	return *p, true
}
