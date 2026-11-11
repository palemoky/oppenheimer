package service

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/palemoky/Oppenheimer/pkg/worker"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

// SweeperConfig 是后台任务的配置，零值字段使用默认值。
type SweeperConfig struct {
	// Interval 是两轮扫描之间的间隔，默认 30 秒。
	Interval time.Duration
	// BatchSize 是每类扫描每轮最多处理的订单数，默认 100。
	BatchSize int
	// CreateTimeout 是订单落库后停留在 CREATING 的时限，超过后置为失败，默认 1 分钟。
	// 应明显大于一次 Reserve 调用的超时，否则会和正常进行中的落库抢。
	//
	// 从落库时刻（updated_at）算起，而不是从受理时刻（created_at）：消费积压时订单落库可能比受理晚
	// 很多，按受理时刻算的话，积压超过这个时限的订单一落库就会被 sweeper 和消费者抢着处理，
	// 恰好在削峰最需要的时候成批失败。CREATING 的行在落库之后不会再被更新，updated_at 就是落库时间。
	CreateTimeout time.Duration
}

// 后台任务的默认配置。
const (
	// 超时关单的主路径是延迟消息（ADR-0009），扫表只兜底延迟消息丢失、进死信的情况，所以间隔可以大。
	DefaultSweepInterval  = 30 * time.Second
	DefaultSweepBatchSize = 100
	DefaultCreateTimeout  = time.Minute
)

// Sweeper 是订单服务的后台任务，实现 app.Component：
// 关闭超时未支付的订单、让卡在 CREATING 的订单失败。
//
// 它只做状态转换加写事件，不调用库存：库存的确认和释放由库存服务消费 order.events 完成。
// 1a 的“补结清”随之删除——事件和状态变更在同一个事务里提交，不存在“状态变了、通知没发”的窗口。
//
// 多个副本同时运行是安全的：所有转换都是条件更新，只有完成转换的一方写事件，重复执行只是多做无用功。
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
	s := &Sweeper{
		svc:    svc,
		cfg:    cfg,
		logger: svc.logger.With("component", "sweeper"),
	}
	loop, err := worker.New(worker.Config{
		Name:     "sweeper",
		Interval: cfg.Interval,
		Logger:   s.logger,
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
func (s *Sweeper) RunOnce(ctx context.Context) {
	now := s.svc.now()
	s.closeExpired(ctx, now)
	s.failStaleCreating(ctx, now)
}

func (s *Sweeper) closeExpired(ctx context.Context, now time.Time) {
	refs, err := s.svc.store.ListExpired(ctx, now, s.cfg.BatchSize)
	if err != nil {
		s.logger.ErrorContext(ctx, "list expired orders failed", "error", err)
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
		s.logger.InfoContext(ctx, "order closed for payment timeout", "order_id", r.OrderID)
	}
	// !ok：扫描之后被支付或取消抢先了，事件由抢先的一方写。
}

func (s *Sweeper) failStaleCreating(ctx context.Context, now time.Time) {
	refs, err := s.svc.store.ListStaleCreating(ctx, now.Add(-s.cfg.CreateTimeout), s.cfg.BatchSize)
	if err != nil {
		s.logger.ErrorContext(ctx, "list stale creating orders failed", "error", err)
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
		s.logger.InfoContext(ctx, "order failed for create timeout", "order_id", r.OrderID)
	}
}
