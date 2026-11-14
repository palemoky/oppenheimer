package service

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/palemoky/oppenheimer/pkg/worker"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// SweeperConfig 是后台任务的配置，零值字段使用默认值。
type SweeperConfig struct {
	// Interval 是两轮扫描之间的间隔，默认 30 秒。
	Interval time.Duration
	// BatchSize 是每类扫描每轮在每个 slot 上最多处理的订单数，默认 100。
	BatchSize int
	// CreateTimeout 是订单落库后停留在 CREATING 的时限，超过后置为失败，默认 1 分钟。
	// 应明显大于一次 Reserve 调用的超时，否则会和正常进行中的落库抢。
	//
	// 从落库时刻（updated_at）算起，而不是从受理时刻（created_at）：消费积压时订单落库可能比受理晚
	// 很多，按受理时刻算的话，积压超过这个时限的订单一落库就会被 sweeper 和消费者抢着处理，
	// 恰好在削峰最需要的时候成批失败。CREATING 的行在落库之后不会再被更新，updated_at 就是落库时间。
	CreateTimeout time.Duration
	// SlotTimeout 是每个 slot 一轮的时限，涵盖列出和处理这一批，默认 10 秒。
	//
	// 驱动没有设读超时，一个库挂起（连上了但不响应，而不是拒绝连接）时查询会一直等下去；
	// 不设时限的话整轮卡在这个库上，另一个库上的 slot 也扫不到。超时只放弃这个 slot 这一轮，
	// 没做完的订单下一轮会被重新扫到。
	SlotTimeout time.Duration
	// Guard 可选，决定这一轮是否运行。多副本时用分布式锁让只有一个副本在扫（设计文档 7.1）。
	Guard worker.Guard
}

// 后台任务的默认配置。
const (
	// 超时关单的主路径是延迟消息（ADR-0009），扫表只兜底延迟消息丢失、进死信的情况，所以间隔可以大。
	DefaultSweepInterval  = 30 * time.Second
	DefaultSweepBatchSize = 100
	DefaultCreateTimeout  = time.Minute
	// 一批 100 个订单、每个一次条件更新加写事件，正常几百毫秒内做完，10 秒足够宽裕。
	DefaultSweepSlotTimeout = 10 * time.Second
)

// Sweeper 是订单服务的后台任务，实现 app.Component：
// 关闭超时未支付的订单、让卡在 CREATING 的订单失败。
//
// 它只做状态转换加写事件，不调用库存：库存的确认和释放由库存服务消费 order.events 完成。
// 1a 的“补结清”随之删除——事件和状态变更在同一个事务里提交，不存在“状态变了、通知没发”的窗口。
//
// 多个副本同时运行是安全的：所有转换都是条件更新，只有完成转换的一方写事件，重复执行只是多做无用功。
// 所以 Guard 只是为了省事，锁不可用时应当放行（lock.FailOpen）。
type Sweeper struct {
	svc    *Service
	cfg    SweeperConfig
	logger *slog.Logger
	loop   *worker.Loop
}

// NewSweeper 返回使用 svc 的时钟和依赖的 Sweeper。
func NewSweeper(svc *Service, cfg SweeperConfig) (*Sweeper, error) {
	cfg.Interval = cmp.Or(cfg.Interval, DefaultSweepInterval)
	cfg.BatchSize = cmp.Or(cfg.BatchSize, DefaultSweepBatchSize)
	cfg.CreateTimeout = cmp.Or(cfg.CreateTimeout, DefaultCreateTimeout)
	cfg.SlotTimeout = cmp.Or(cfg.SlotTimeout, DefaultSweepSlotTimeout)
	s := &Sweeper{
		svc:    svc,
		cfg:    cfg,
		logger: svc.logger.With("component", "sweeper"),
	}
	loop, err := worker.New(worker.Config{
		Name:     "sweeper",
		Interval: cfg.Interval,
		Logger:   s.logger,
		Guard:    cfg.Guard,
	}, func(ctx context.Context) error {
		s.RunOnce(ctx)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("new sweeper: %w", err)
	}
	s.loop = loop
	return s, nil
}

// Name 实现 app.Component。
func (s *Sweeper) Name() string { return s.loop.Name() }

// Start 实现 app.Component：按固定间隔循环扫描，直到 Stop 或 ctx 取消。
func (s *Sweeper) Start(ctx context.Context) error { return s.loop.Start(ctx) }

// Stop 实现 app.Component：等当前这一轮做完。
// 中途被中断也是安全的，没做完的订单下一轮会被重新扫到。
func (s *Sweeper) Stop(ctx context.Context) error { return s.loop.Stop(ctx) }

// RunOnce 执行一轮两类扫描。单个订单失败只记日志，不中断整轮。
//
// 订单分在多个 slot 上，每一轮依次扫所有 slot，每个 slot 各取一批（BatchSize 是每个 slot 的上限）。
// 某个库不可用时，它上面的 slot 列表失败、记日志后跳过，不影响其他库的 slot；
// 库挂起不响应时，每个 slot 最多耗 SlotTimeout，之后照常扫下一个 slot。
func (s *Sweeper) RunOnce(ctx context.Context) {
	now := s.svc.now()
	for slot := range s.svc.store.Router().Slots() {
		if ctx.Err() != nil {
			return
		}
		s.sweepSlot(ctx, slot, now)
	}
}

// sweepSlot 在 SlotTimeout 内对一个 slot 做两类扫描。
func (s *Sweeper) sweepSlot(ctx context.Context, slot int, now time.Time) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.SlotTimeout)
	defer cancel()
	s.closeExpired(ctx, slot, now)
	s.failStaleCreating(ctx, slot, now)
}

// list 先在从库上列出候选，从库出错时在同一个 slot 的时限内改用主库再列一次。
//
// 扫描可以走从库：列表只用来找候选，真正的状态转换是主库上带前置条件的更新。从库旧了只有两种后果：
// 还没复制到的订单这一轮看不到，下一轮（或延迟消息）再处理；已在主库上被支付、取消的订单还被列出来，
// 对它做的条件更新影响 0 行、不写事件，只是白做一次。两者都不会让订单走错状态。
// 从库读挂了就退回主库，不让兜底任务因为从库停摆。
//
// 从库那次查询只给 slot 剩余时限的一半：从库挂起（连得上但不响应）时，如果它和主库共用整个 slot 的时限，
// 会把时限耗光，退回主库时已经没有时间，这个 slot 这一轮就被跳过；从库一直挂着，兜底任务就一直不工作。
// 留下的另一半给主库那次查询和处理这一批订单。按剩余时间而不是固定的 SlotTimeout/2 算：一个 slot 依次做
// 两类扫描，第二次列表开始时 slot 的时限已经用掉一部分，固定的一半可能把剩下的全部占掉。
// 没有从库时直接查一次主库，不加这层时限。从库被标记为不健康时（设计文档 1d 的 6.7，见 ReplicaProber）
// 也直接查主库，不再每个 slot 都先在一个落后或挂住的从库上耗掉一半时限。
func (s *Sweeper) list(ctx context.Context, slot int, kind string,
	query func(ctx context.Context, src store.Source) ([]store.OrderRef, error),
) ([]store.OrderRef, error) {
	if !s.svc.store.HasReplicas() || !s.svc.replicaHealthy(s.svc.store.Router().DB(slot)) {
		return query(ctx, store.Primary)
	}
	budget := s.cfg.SlotTimeout / 2
	if dl, ok := ctx.Deadline(); ok {
		budget = time.Until(dl) / 2
	}
	rctx, cancel := context.WithTimeout(ctx, budget)
	refs, err := query(rctx, store.Replica)
	cancel()
	if err == nil {
		return refs, nil
	}
	s.logger.WarnContext(ctx, "list on replica failed, retrying on primary", "kind", kind, "slot", slot, "error", err)
	return query(ctx, store.Primary)
}

func (s *Sweeper) closeExpired(ctx context.Context, slot int, now time.Time) {
	refs, err := s.list(ctx, slot, "expired", func(ctx context.Context, src store.Source) ([]store.OrderRef, error) {
		return s.svc.store.ListExpired(ctx, slot, now, s.cfg.BatchSize, src)
	})
	if err != nil {
		s.logger.ErrorContext(ctx, "list expired orders failed", "slot", slot, "error", err)
		return
	}
	for _, r := range refs {
		s.closeOrder(ctx, r, now)
	}
}

// closeOrder 关闭一个超时未支付的订单并发 closed 事件。
func (s *Sweeper) closeOrder(ctx context.Context, r store.OrderRef, now time.Time) {
	ok, err := s.svc.closeExpired(ctx, r, now)
	if err != nil {
		s.logger.ErrorContext(ctx, "close expired order failed", "order_id", r.OrderID, "error", err)
		return
	}
	if ok {
		reclaimed(ctx, reclaimCloseExpired)
		s.logger.InfoContext(ctx, "order closed for payment timeout", "order_id", r.OrderID)
	}
	// !ok：扫描之后被支付或取消抢先了，事件由抢先的一方写。
}

func (s *Sweeper) failStaleCreating(ctx context.Context, slot int, now time.Time) {
	refs, err := s.list(ctx, slot, "stale creating", func(ctx context.Context, src store.Source) ([]store.OrderRef, error) {
		return s.svc.store.ListStaleCreating(ctx, slot, now.Add(-s.cfg.CreateTimeout), s.cfg.BatchSize, src)
	})
	if err != nil {
		s.logger.ErrorContext(ctx, "list stale creating orders failed", "slot", slot, "error", err)
		return
	}
	for _, r := range refs {
		s.failCreating(ctx, r, now)
	}
}

// failCreating 把一个创建超时的订单置为失败并发 failed 事件。
// 预占结果未知，库存服务收到事件后 Release：释放可能存在的预占，
// 没有预占时留下“已释放”标记，挡住之后迟到的 Reserve。
func (s *Sweeper) failCreating(ctx context.Context, r store.OrderRef, now time.Time) {
	ok, err := s.svc.failCreating(ctx, r, ReasonCreateTimeout, now)
	if err != nil {
		s.logger.ErrorContext(ctx, "fail stale creating order failed", "order_id", r.OrderID, "error", err)
		return
	}
	if ok {
		reclaimed(ctx, reclaimCreateTimeout)
		s.logger.InfoContext(ctx, "order failed for create timeout", "order_id", r.OrderID)
	}
}
