// Package processor 是 payment 的后台任务：为待支付的支付单模拟出结果，并发布 PaymentEvent.completed。
//
// 1a 里这里还负责把结果同步回调给订单服务、失败按退避重试。1b 改成 outbox：
// 状态从 PENDING 变为终态和事件写在同一个本地事务里，由 outbox 中继发到 Kafka，
// 订单服务处理后在 order.events 上答复（见 internal/consumer）。处理器不再关心订单服务是否可达。
//
// 每次转换都是带前置状态的条件更新，多个实例同时运行也只有一个能完成某次转换，
// 也就只有它写出事件：一次转换对应恰好一条事件。
//
// 一轮取出的支付单并发处理，每笔仍然是自己的一个事务（3b 第 1 轮，docs/notes/3b-rounds/）。
// 原来是一笔一笔串行：一次提交要等一次刷盘，在集群的盘上约 56 毫秒，所以不管来多少支付，
// 每秒只能出约 17 笔结果，排队超过支付超时的就在订单关掉之后才出结果，被退款。
// 并发之后吞吐是“并发度 ÷ 一次提交的耗时”，而且同时提交的事务会被 MySQL 的组提交合进同一次刷盘。
// 没有改成“几笔合进一个事务”：那样一笔失败要连累同批的其他笔，而这里要的只是不再排队等刷盘。
// 并发不改变上面那条保证：一轮里的各笔是不同的行，互不相干；同一笔被两个实例同时拿到时仍然靠条件更新。
package processor

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/palemoky/oppenheimer/api/eventx"
	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/pkg/outbox"
	"github.com/palemoky/oppenheimer/pkg/worker"
	"github.com/palemoky/oppenheimer/services/payment/internal/store"
)

// 默认值。
const (
	DefaultInterval    = 200 * time.Millisecond
	DefaultBatchSize   = 100
	DefaultResultDelay = 100 * time.Millisecond
	// DefaultConcurrency 是一轮里同时在处理的支付单数，每笔占一个数据库连接。
	// 连接池默认 50 个（pkg/mysqlx），同一个池还要给支付的受理和消费者用，所以不取更大的值。
	DefaultConcurrency = 16
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
	// Concurrency 是一轮里同时处理的支付单数，1 就是串行。
	Concurrency int
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
	c.Concurrency = cmp.Or(c.Concurrency, DefaultConcurrency)
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

// RunOnce 执行一轮：为创建超过模拟延迟的 PENDING 支付单按 outcome 得出结果，最多 BatchSize 笔，
// 其中同时在处理的不超过 Concurrency 笔。返回时这一轮开出去的 goroutine 都已经结束。
// 单条支付单的失败只记日志，不影响同批的其他支付单；它仍是 PENDING，下一轮会再试。
func (p *Processor) RunOnce(ctx context.Context) error {
	now := p.now()
	pending, err := p.store.ListPendingCreatedBefore(ctx, now.Add(-p.cfg.ResultDelay), p.cfg.BatchSize)
	if err != nil {
		return err
	}
	var (
		completed atomic.Int64
		wg        sync.WaitGroup
		slots     = make(chan struct{}, p.cfg.Concurrency)
	)
dispatch:
	for _, pay := range pending {
		// 拿名额时也看 ctx：被取消之后不再开始新的一笔，已经开始的那些各自在数据库调用上收到取消。
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			break dispatch
		}
		wg.Go(func() {
			defer func() { <-slots }()
			if p.completeOne(ctx, pay, now) {
				completed.Add(1)
			}
		})
	}
	wg.Wait()
	// 一轮只 Kick 一次：Kick 本身会合并，但没有新事件时不必唤醒中继。
	// 必须在提交之后：中继在另一个连接上，看不到未提交的行。
	if completed.Load() > 0 {
		p.kicker.Kick()
		// 取满了一批说明多半还有积压：不等 Interval，紧接着再跑一轮（下一轮照常先问 Guard）。
		// 只在这一轮确实出了结果时才这样做：整批都失败（比如数据库不可用）时退回到按 Interval 重试，
		// 不能变成一个不停打数据库的死循环。
		if len(pending) == p.cfg.BatchSize {
			p.loop.Kick()
		}
	}
	return ctx.Err()
}

// completeOne 处理一笔并报告这一次是否完成了转换。错误只记日志。
// panic 也在这里接住：worker.Loop 只接得住它自己那个 goroutine 里的 panic，
// 这里是另开的 goroutine，不接的话一笔坏数据会带崩整个进程。
func (p *Processor) completeOne(ctx context.Context, pay store.Payment, now time.Time) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
			p.logger.ErrorContext(ctx, "complete payment panicked", "payment_id", pay.PaymentID,
				"panic", r, "stack", string(debug.Stack()))
		}
	}()
	ok, err := p.complete(ctx, pay, now)
	if err != nil {
		p.logger.ErrorContext(ctx, "complete payment failed", "payment_id", pay.PaymentID, "error", err)
		return false
	}
	return ok
}

// complete 在一个事务里完成状态转换并写入 PaymentEvent.completed。
// 返回 false 说明另一个实例已经处理过（条件更新影响 0 行），这时不写事件，也不是错误。
func (p *Processor) complete(ctx context.Context, pay store.Payment, now time.Time) (_ bool, err error) {
	// 给“支付出结果”开一条链路（设计文档 1d 的 8.6）。这里是后台循环，ctx 里没有 span；而出结果是一个业务动作的
	// 起点：它写的 PaymentEvent 经 outbox 到 order，再到 inventory。不在这里开头的话，outbox 里存不下上下文，
	// 那条链路要从中继发消息的那一刻才开始，根 span 上没有订单号，这个事务也不在链路里。
	// 真实系统里这一步是支付渠道的回调，本来就是一个新请求，和用户发起支付的那次请求不是同一条链路。
	ctx, span := tracer.Start(ctx, "payment.complete")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "complete payment failed")
		}
		span.End()
	}()
	otelx.SetOrder(ctx, pay.OrderID, pay.UserID)

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
	if done {
		// 从创建到出结果等了多久。now 是这一轮开始的时刻，也是写进 updated_at 的那个值，
		// 所以这个数和库里 updated_at 减 created_at 是同一个数。
		wait := now.Sub(pay.CreatedAt).Seconds()
		if to == paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED {
			results.Add(ctx, 1, resultSucceeded)
			resultDelay.Record(ctx, wait, resultSucceeded)
		} else {
			results.Add(ctx, 1, resultFailed)
			resultDelay.Record(ctx, wait, resultFailed)
		}
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
