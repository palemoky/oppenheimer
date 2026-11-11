package service

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
)

// SweeperConfig 是后台任务的配置，零值字段使用默认值。
type SweeperConfig struct {
	// Interval 是两轮扫描之间的间隔，默认 1 秒。
	Interval time.Duration
	// BatchSize 是每类扫描每轮最多处理的订单数，默认 100。
	BatchSize int
	// CreateTimeout 是订单停留在 CREATING 的时限，超过后置为失败，默认 1 分钟。
	// 应明显大于一次 Reserve 调用的超时，否则会和正常进行中的下单抢。
	CreateTimeout time.Duration
	// SettleGrace 是补结清前等待的宽限，留给在线路径先完成结清，默认 5 秒。
	SettleGrace time.Duration
}

// 后台任务的默认配置。
const (
	DefaultSweepInterval  = time.Second
	DefaultSweepBatchSize = 100
	DefaultCreateTimeout  = time.Minute
	DefaultSettleGrace    = 5 * time.Second
)

// Sweeper 是订单服务的后台任务，实现 app.Component：
// 关闭超时未支付的订单、让卡在 CREATING 的订单失败、补做没完成的库存结清。
//
// 多个副本同时运行是安全的：所有转换都是条件更新，所有库存调用都幂等，重复执行只是多做无用功。
type Sweeper struct {
	svc    *Service
	cfg    SweeperConfig
	logger *slog.Logger

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	// cancel 取消正在进行的一轮，用于 Stop 的时限到期时。
	mu     sync.Mutex
	cancel context.CancelFunc
}

// NewSweeper 返回使用 svc 的时钟和依赖的 Sweeper。
func NewSweeper(svc *Service, cfg SweeperConfig) *Sweeper {
	cfg.Interval = cmp.Or(cfg.Interval, DefaultSweepInterval)
	cfg.BatchSize = cmp.Or(cfg.BatchSize, DefaultSweepBatchSize)
	cfg.CreateTimeout = cmp.Or(cfg.CreateTimeout, DefaultCreateTimeout)
	cfg.SettleGrace = cmp.Or(cfg.SettleGrace, DefaultSettleGrace)
	return &Sweeper{
		svc:    svc,
		cfg:    cfg,
		logger: svc.logger.With("component", "sweeper"),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Name 实现 app.Component。
func (s *Sweeper) Name() string { return "sweeper" }

// Start 实现 app.Component：按固定间隔循环扫描，直到 Stop 或 ctx 取消。
func (s *Sweeper) Start(ctx context.Context) error {
	defer close(s.done)
	// app.Run 在关停时会先取消 Start 的 ctx。一轮扫描与它脱钩，
	// 使正在进行的一轮能做完，只在 Stop 的时限到期时才被中断。
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.RunOnce(runCtx)
		}
	}
}

// Stop 实现 app.Component：等当前这一轮做完；ctx 到期则中断这一轮。
// 中断是安全的，没做完的订单下一次启动后会被重新扫到。
func (s *Sweeper) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stop) })
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		if s.cancel != nil {
			s.cancel()
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

// RunOnce 执行一轮三类扫描。单个订单失败只记日志，不中断整轮。
func (s *Sweeper) RunOnce(ctx context.Context) {
	now := s.svc.now()
	s.closeExpired(ctx, now)
	s.failStaleCreating(ctx, now)
	s.resettle(ctx, now)
}

func (s *Sweeper) closeExpired(ctx context.Context, now time.Time) {
	ids, err := s.svc.store.ListExpired(ctx, now, s.cfg.BatchSize)
	if err != nil {
		s.logger.ErrorContext(ctx, "list expired orders failed", "error", err)
		return
	}
	for _, id := range ids {
		s.closeOrder(ctx, id, now)
	}
}

// closeOrder 关闭一个超时未支付的订单，成功的随即结清。
func (s *Sweeper) closeOrder(ctx context.Context, id int64, now time.Time) {
	ok, err := s.svc.store.CloseExpired(ctx, id, now)
	if err != nil {
		s.logger.ErrorContext(ctx, "close expired order failed", "order_id", id, "error", err)
		return
	}
	if !ok {
		// 扫描之后被支付或取消抢先了。
		return
	}
	s.logger.InfoContext(ctx, "order closed for payment timeout", "order_id", id)
	s.settleLogged(ctx, id, orderv1.OrderStatus_ORDER_STATUS_CLOSED)
}

func (s *Sweeper) failStaleCreating(ctx context.Context, now time.Time) {
	ids, err := s.svc.store.ListStaleCreating(ctx, now.Add(-s.cfg.CreateTimeout), s.cfg.BatchSize)
	if err != nil {
		s.logger.ErrorContext(ctx, "list stale creating orders failed", "error", err)
		return
	}
	for _, id := range ids {
		s.failCreating(ctx, id, now)
	}
}

// failCreating 把一个创建超时的订单置为失败，成功的随即结清。
func (s *Sweeper) failCreating(ctx context.Context, id int64, now time.Time) {
	// 预占结果未知，所以标为未结清：Release 会释放可能存在的预占，
	// 没有预占时也会留下“已释放”标记，挡住之后迟到的 Reserve。
	ok, err := s.svc.store.MarkCreateFailed(ctx, id, ReasonCreateTimeout, false, now)
	if err != nil {
		s.logger.ErrorContext(ctx, "fail stale creating order failed", "order_id", id, "error", err)
		return
	}
	if !ok {
		return
	}
	s.logger.InfoContext(ctx, "order failed for create timeout", "order_id", id)
	s.settleLogged(ctx, id, orderv1.OrderStatus_ORDER_STATUS_FAILED)
}

func (s *Sweeper) resettle(ctx context.Context, now time.Time) {
	refs, err := s.svc.store.ListUnsettled(ctx, now.Add(-s.cfg.SettleGrace), s.cfg.BatchSize)
	if err != nil {
		s.logger.ErrorContext(ctx, "list unsettled orders failed", "error", err)
		return
	}
	for _, r := range refs {
		s.settleLogged(ctx, r.OrderID, r.Status)
	}
}

func (s *Sweeper) settleLogged(ctx context.Context, orderID int64, st orderv1.OrderStatus) {
	// 不变量破坏已在 settle 里按 error 记过。
	if err := s.svc.settle(ctx, orderID, st); err != nil && !errors.Is(err, ErrStockInvariant) {
		s.logger.WarnContext(ctx, "settle stock failed, will retry", "order_id", orderID, "error", err)
	}
}
