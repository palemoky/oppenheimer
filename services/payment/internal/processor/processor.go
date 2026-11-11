// Package processor 是 payment 的后台任务：为待支付的支付单模拟出结果，并把结果回调给订单服务。
//
// 回调是至少一次投递。订单服务以 payment_id 幂等，所以重复回调是安全的；
// 本地的每一步都是带前置状态的条件更新，多个实例同时运行也只有一个能完成某次转换。
package processor

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/Oppenheimer/pkg/app"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/services/payment/internal/store"
)

// 默认值，见设计文档第 5 节。
const (
	DefaultInterval    = 200 * time.Millisecond
	DefaultBatchSize   = 100
	DefaultResultDelay = 100 * time.Millisecond
	DefaultRetryBase   = 200 * time.Millisecond
	DefaultRetryMax    = 30 * time.Second
	DefaultCallTimeout = 3 * time.Second
)

// Config 配置 Processor。零值字段使用默认值。
type Config struct {
	// Interval 是两轮处理之间的间隔。
	Interval time.Duration
	// BatchSize 是每轮每类处理的最大条数。
	BatchSize int
	// ResultDelay 是支付单创建后多久得出模拟结果。
	ResultDelay time.Duration
	// RetryBase、RetryMax 决定回调失败后的退避：min(RetryBase × 2^attempts, RetryMax)。
	RetryBase time.Duration
	RetryMax  time.Duration
	// CallTimeout 是单次回调的超时。一次卡住的回调不应拖住整批。
	CallTimeout time.Duration
	// Now 是时钟，默认 time.Now。
	Now func() time.Time
}

func (c Config) withDefaults() Config {
	c.Interval = cmp.Or(c.Interval, DefaultInterval)
	c.BatchSize = cmp.Or(c.BatchSize, DefaultBatchSize)
	c.ResultDelay = cmp.Or(c.ResultDelay, DefaultResultDelay)
	c.RetryBase = cmp.Or(c.RetryBase, DefaultRetryBase)
	c.RetryMax = cmp.Or(c.RetryMax, DefaultRetryMax)
	c.CallTimeout = cmp.Or(c.CallTimeout, DefaultCallTimeout)
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Processor 周期性地处理支付单，实现 app.Component。
type Processor struct {
	store  *store.Store
	orders orderv1.OrderServiceClient
	logger *slog.Logger
	cfg    Config

	// round 是每轮执行的工作，默认 RunOnce；测试生命周期时替换它。
	round func(ctx context.Context)

	mu      sync.Mutex
	started bool
	stopped chan struct{}
	done    chan struct{}
	cancel  context.CancelFunc
}

var _ app.Component = (*Processor)(nil)

// New 返回 Processor。
func New(st *store.Store, orders orderv1.OrderServiceClient, logger *slog.Logger, cfg Config) *Processor {
	p := &Processor{
		store:   st,
		orders:  orders,
		logger:  logger,
		cfg:     cfg.withDefaults(),
		stopped: make(chan struct{}),
		done:    make(chan struct{}),
	}
	p.round = func(ctx context.Context) {
		if err := p.RunOnce(ctx); err != nil {
			p.logger.ErrorContext(ctx, "payment processor round failed", "error", err)
		}
	}
	return p
}

// Name 实现 app.Component。
func (p *Processor) Name() string { return "payment-processor" }

// Start 实现 app.Component：立即执行一轮，之后每隔 Interval 执行一轮，直到 Stop 或 ctx 取消。
func (p *Processor) Start(ctx context.Context) error {
	// 每轮的 context 不随 ctx 取消：app.Run 收到退出信号时先取消 ctx 再调 Stop，
	// 进行中的一轮应在 Stop 的时限内做完，而不是在两次数据库操作之间被打断。
	roundCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		cancel()
		return errors.New("payment processor already started")
	}
	p.started = true
	p.cancel = cancel
	p.mu.Unlock()
	defer close(p.done)
	defer cancel()

	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopped:
			return nil
		case <-ctx.Done():
			return nil
		default:
		}
		p.round(roundCtx)
		select {
		case <-p.stopped:
			return nil
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Stop 实现 app.Component：不再开始新的一轮，等待进行中的一轮结束；ctx 到期则中断它。
func (p *Processor) Stop(ctx context.Context) error {
	p.mu.Lock()
	started := p.started
	select {
	case <-p.stopped:
	default:
		close(p.stopped)
	}
	p.mu.Unlock()
	if !started {
		return nil
	}

	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		p.mu.Lock()
		p.cancel()
		p.mu.Unlock()
		<-p.done
		return ctx.Err()
	}
}

// RunOnce 执行一轮：先出结果，再回调。单条支付单的失败只记日志，不影响同批的其他支付单。
func (p *Processor) RunOnce(ctx context.Context) error {
	return errors.Join(p.resolvePending(ctx), p.notifyResults(ctx))
}

func (p *Processor) now() time.Time {
	return p.cfg.Now().UTC().Truncate(time.Microsecond)
}

// resolvePending 为创建超过模拟延迟的 PENDING 支付单按 outcome 得出结果。
func (p *Processor) resolvePending(ctx context.Context) error {
	now := p.now()
	pending, err := p.store.ListPendingCreatedBefore(ctx, now.Add(-p.cfg.ResultDelay), p.cfg.BatchSize)
	if err != nil {
		return err
	}
	for _, pay := range pending {
		to := paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
		if pay.Outcome == paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_FAILURE {
			to = paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED
		}
		// 返回 false 说明另一个实例已经处理过，不是错误。
		if _, err := p.store.CompletePending(ctx, pay.PaymentID, to, now); err != nil {
			p.logger.ErrorContext(ctx, "complete payment failed", "payment_id", pay.PaymentID, "error", err)
		}
	}
	return nil
}

// notifyResults 把已出结果、未确认的支付单回调给订单服务。
func (p *Processor) notifyResults(ctx context.Context) error {
	due, err := p.store.ListUnnotifiedDue(ctx, p.now(), p.cfg.RetryBase, p.cfg.RetryMax, p.cfg.BatchSize)
	if err != nil {
		return err
	}
	for _, pay := range due {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := p.notify(ctx, pay); err != nil {
			p.logger.ErrorContext(ctx, "record notify result failed", "payment_id", pay.PaymentID, "error", err)
		}
	}
	return nil
}

func (p *Processor) notify(ctx context.Context, pay store.Payment) error {
	succeeded := pay.Status == paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	callCtx, cancel := context.WithTimeout(ctx, p.cfg.CallTimeout)
	resp, callErr := p.orders.HandlePaymentResult(callCtx, &orderv1.HandlePaymentResultRequest{
		OrderId:   pay.OrderID,
		PaymentId: pay.PaymentID,
		Succeeded: succeeded,
		Amount:    &commonv1.Money{Amount: pay.Amount, Currency: pay.Currency},
	})
	cancel()

	// 记录结果用调用之后的时间：退避从这次失败算起，而不是从扫描时算起。
	now := p.now()
	log := p.logger.With("payment_id", pay.PaymentID, "order_id", pay.OrderID)

	switch {
	case callErr == nil:
		switch resp.GetDisposition() {
		case orderv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED:
			_, err := p.store.MarkNotified(ctx, pay.PaymentID, pay.Status, now)
			return err
		case orderv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED:
			if !succeeded {
				// 失败的支付没有钱可退，订单服务这样答复不合约定，但结果已送达。
				log.WarnContext(ctx, "refund required for failed payment")
				_, err := p.store.MarkNotified(ctx, pay.PaymentID, pay.Status, now)
				return err
			}
			return p.refund(ctx, log, pay, now, "refund required by order service")
		default:
			// 不认识的答复说明双方版本不一致，不能当作已确认，留待重试。
			log.WarnContext(ctx, "unknown payment disposition", "disposition", resp.GetDisposition().String())
			return p.retryLater(ctx, pay, now)
		}

	case errx.Is(callErr, errx.ReasonAmountMismatch), errx.Is(callErr, errx.ReasonOrderNotFound):
		// 订单服务不会接受这笔支付，重试没有意义：成功的钱退回去，结束回调。
		log.ErrorContext(ctx, "payment rejected by order service", "reason", errx.Reason(callErr), "error", callErr)
		if !succeeded {
			_, err := p.store.MarkNotified(ctx, pay.PaymentID, pay.Status, now)
			return err
		}
		return p.refund(ctx, log, pay, now, "refund after rejection")

	default:
		log.WarnContext(ctx, "notify order service failed, will retry",
			"attempts", pay.NotifyAttempts+1, "error", callErr)
		return p.retryLater(ctx, pay, now)
	}
}

func (p *Processor) refund(ctx context.Context, log *slog.Logger, pay store.Payment, now time.Time, msg string) error {
	ok, err := p.store.RefundAndMarkNotified(ctx, pay.PaymentID, now)
	if err != nil {
		return err
	}
	// 只有完成转换的那个调用者记录退款，重复处理不会产生第二条退款记录。
	if ok {
		log.InfoContext(ctx, "payment refunded", "cause", msg, "amount", pay.Amount, "currency", pay.Currency)
	}
	return nil
}

func (p *Processor) retryLater(ctx context.Context, pay store.Payment, now time.Time) error {
	_, err := p.store.IncrNotifyAttempts(ctx, pay.PaymentID, pay.Status, pay.NotifyAttempts, now)
	return err
}
