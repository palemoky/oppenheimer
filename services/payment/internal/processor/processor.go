// Package processor 是 payment 的后台任务：为待支付的支付单模拟出结果，并发布 PaymentEvent.completed。
//
// 1a 里这里还负责把结果同步回调给订单服务、失败按退避重试。1b 改成 outbox：
// 状态从 PENDING 变为终态和事件写在同一个本地事务里，由 outbox 中继发到 Kafka，
// 订单服务处理后在 order.events 上答复（见 internal/consumer）。处理器不再关心订单服务是否可达。
//
// 每次转换都是带前置状态的条件更新，多个实例同时运行也只有一个能完成某次转换，
// 也就只有它写出事件：一次转换对应恰好一条事件。
package processor

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/palemoky/Oppenheimer/api/eventx"
	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	eventv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/event/v1"
	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/Oppenheimer/pkg/app"
	"github.com/palemoky/Oppenheimer/pkg/outbox"
	"github.com/palemoky/Oppenheimer/pkg/worker"
	"github.com/palemoky/Oppenheimer/services/payment/internal/store"
)

// 默认值。
const (
	DefaultInterval    = 200 * time.Millisecond
	DefaultBatchSize   = 100
	DefaultResultDelay = 100 * time.Millisecond
)

// Kicker 在事务提交后被调用，让 outbox 中继立即发送，生产环境是 *outbox.Relay。
type Kicker interface {
	Kick()
}

type noKick struct{}

func (noKick) Kick() {}

// Config 配置 Processor。零值字段使用默认值。
type Config struct {
	// Interval 是两轮处理之间的间隔。
	Interval time.Duration
	// BatchSize 是每轮处理的最大条数。
	BatchSize int
	// ResultDelay 是支付单创建后多久得出模拟结果。
	ResultDelay time.Duration
	// TopicPrefix 是 topic 名的环境前缀（KAFKA_PREFIX）。
	TopicPrefix string
	// Now 是时钟，默认 time.Now。
	Now func() time.Time
	// Guard 可选。传入分布式锁可以让多个实例里只有一个在处理；为 nil 时每个实例都处理。
	// 互斥只是为了省去重复的工作：正确性由条件更新保证。
	Guard worker.Guard
}

func (c Config) withDefaults() Config {
	c.Interval = cmp.Or(c.Interval, DefaultInterval)
	c.BatchSize = cmp.Or(c.BatchSize, DefaultBatchSize)
	c.ResultDelay = cmp.Or(c.ResultDelay, DefaultResultDelay)
	if c.Now == nil {
		c.Now = time.Now
	}
	return c
}

// Processor 周期性地处理支付单，实现 app.Component。
type Processor struct {
	store  *store.Store
	kicker Kicker
	logger *slog.Logger
	cfg    Config
	loop   *worker.Loop
}

var _ app.Component = (*Processor)(nil)

// New 返回 Processor。kicker 为 nil 时不触发中继，消息等中继的兜底轮询发出。
func New(st *store.Store, kicker Kicker, logger *slog.Logger, cfg Config) (*Processor, error) {
	if kicker == nil {
		kicker = noKick{}
	}
	p := &Processor{store: st, kicker: kicker, logger: logger, cfg: cfg.withDefaults()}
	loop, err := worker.New(worker.Config{
		Name:       "payment-processor",
		Interval:   p.cfg.Interval,
		Logger:     logger,
		Guard:      p.cfg.Guard,
		RunOnStart: true,
	}, p.RunOnce)
	if err != nil {
		return nil, fmt.Errorf("payment processor: %w", err)
	}
	p.loop = loop
	return p, nil
}

// Name 实现 app.Component。
func (p *Processor) Name() string { return p.loop.Name() }

// Start 实现 app.Component。
func (p *Processor) Start(ctx context.Context) error { return p.loop.Start(ctx) }

// Stop 实现 app.Component：不再开始新的一轮，等进行中的一轮结束。
func (p *Processor) Stop(ctx context.Context) error { return p.loop.Stop(ctx) }

func (p *Processor) now() time.Time {
	return p.cfg.Now().UTC().Truncate(time.Microsecond)
}

// RunOnce 执行一轮：为创建超过模拟延迟的 PENDING 支付单按 outcome 得出结果。
// 单条支付单的失败只记日志，不影响同批的其他支付单；它仍是 PENDING，下一轮会再试。
func (p *Processor) RunOnce(ctx context.Context) error {
	now := p.now()
	pending, err := p.store.ListPendingCreatedBefore(ctx, now.Add(-p.cfg.ResultDelay), p.cfg.BatchSize)
	if err != nil {
		return err
	}
	completed := 0
	for _, pay := range pending {
		if ctx.Err() != nil {
			break
		}
		ok, err := p.complete(ctx, pay, now)
		if err != nil {
			p.logger.ErrorContext(ctx, "complete payment failed", "payment_id", pay.PaymentID, "error", err)
			continue
		}
		if ok {
			completed++
		}
	}
	// 一轮只 Kick 一次：Kick 本身会合并，但没有新事件时不必唤醒中继。
	// 必须在提交之后：中继在另一个连接上，看不到未提交的行。
	if completed > 0 {
		p.kicker.Kick()
	}
	return ctx.Err()
}

// complete 在一个事务里完成状态转换并写入 PaymentEvent.completed。
// 返回 false 说明另一个实例已经处理过（条件更新影响 0 行），这时不写事件，也不是错误。
func (p *Processor) complete(ctx context.Context, pay store.Payment, now time.Time) (bool, error) {
	to := paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	if pay.Outcome == paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_FAILURE {
		to = paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED
	}

	// 事件在事务外先序列化好：事务里只做数据库操作，持锁时间越短越好。
	// pay 来自事务外的读取，但事件里用到的字段（订单号、用户、金额）创建后不会变，
	// 结果 to 由 outcome 决定，而真正写进去的状态由条件更新保证，不会读到旧值而写错。
	msg, err := p.event(pay, to, now)
	if err != nil {
		return false, err
	}

	var done bool
	err = p.store.InTx(ctx, func(tx *sql.Tx) error {
		ok, err := p.store.CompletePending(ctx, tx, pay.PaymentID, to, now)
		if err != nil || !ok {
			return err
		}
		if err := outbox.Add(ctx, tx, msg); err != nil {
			return err
		}
		done = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("complete payment %d: %w", pay.PaymentID, err)
	}
	return done, nil
}

func (p *Processor) event(pay store.Payment, to paymentv1.PaymentStatus, now time.Time) (outbox.Message, error) {
	key, value, err := eventx.MarshalPaymentEvent(&eventv1.PaymentEvent{
		PaymentId: pay.PaymentID,
		OrderId:   pay.OrderID,
		UserId:    pay.UserID,
		OccurTime: timestamppb.New(now),
		Body: &eventv1.PaymentEvent_Completed{Completed: &eventv1.PaymentCompleted{
			Succeeded: to == paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED,
			Amount:    &commonv1.Money{Amount: pay.Amount, Currency: pay.Currency},
		}},
	})
	if err != nil {
		return outbox.Message{}, err
	}
	return outbox.Message{Topic: eventx.Topic(p.cfg.TopicPrefix, eventx.PaymentEvents), Key: key, Payload: value}, nil
}
