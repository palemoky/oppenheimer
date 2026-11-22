package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/palemoky/oppenheimer/api/eventx"
	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/pkg/retry"
	"github.com/palemoky/oppenheimer/services/order/internal/archive"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

const (
	accepted       = eventv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED
	refundRequired = eventv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED
)

// DefaultEventTimeout 是处理一条事件的时限。
//
// 必须明显小于消费者组的再平衡超时（kafkax 默认 60 秒）：分区被收回时要等在途的 handler 返回，
// 卡住太久本实例会被踢出组，新持有者会和它并发处理同一条消息。
const DefaultEventTimeout = 10 * time.Second

// ErrInvalidEvent 表示事件本身不合法（缺字段、与已记录的事实矛盾），重试也不会成功。
// Kafka 适配层把它转成 kafkax.Permanent，消息进死信等人工处理。
var ErrInvalidEvent = errors.New("invalid event")

// paymentTxRetry 在 txRetry 的基础上把 payment_results 的主键冲突也当作可重试：
// 订单不存在时没有订单行锁可以排队，同一 payment_id 的两次并发处理会在插入时撞上；
// 输的一方重做整个事务时就能读到赢的一方提交的记录，走“已处理过”的分支。
var paymentTxRetry = retry.Policy{
	MaxAttempts: txRetry.MaxAttempts,
	Initial:     txRetry.Initial,
	Max:         txRetry.Max,
	Retryable: func(err error) bool {
		return mysqlx.IsDeadlock(err) || errors.Is(err, store.ErrDuplicate)
	},
}

// HandlePaymentEvent 消费 payment.events 的一条消息。
//
// 投递是至少一次，且不保证与订单的其他变更有序，所以这里不假设任何先后关系，
// 一律以订单行上的状态前置条件判断：
//   - 重复：payment_results 以 payment_id 去重，已处理过的什么都不做（答复已在当时的事务里写过）。
//   - 延迟：支付结果在订单关闭很久之后才到，走“订单不可支付 → 退款”。
//   - 与取消、关单并发：几方都以 PENDING_PAYMENT 为前置条件，只有一个成功；支付输了就退款。
//
// 每个分支都在同一个事务里写 payment_results 和 payment_result_handled 答复；
// 订单被置为 PAID 时同一事务里还写 paid 事件。业务上的异常（金额不符、订单不存在）
// 给出明确的处置而不是返回错误，否则消费者会因为业务原因卡住整个分区。
func (s *Service) HandlePaymentEvent(ctx context.Context, ev *eventv1.PaymentEvent) error {
	completed := ev.GetCompleted()
	if completed == nil {
		// 不认识或不关心的事件直接跳过，新增事件类型不会卡住旧的消费者。
		return nil
	}
	if ev.GetOrderId() <= 0 || ev.GetPaymentId() <= 0 {
		return fmt.Errorf("%w: payment event without order_id or payment_id", ErrInvalidEvent)
	}
	otelx.SetOrder(ctx, ev.GetOrderId(), ev.GetUserId())
	amount := completed.GetAmount()
	if completed.GetSucceeded() && (amount == nil || amount.GetAmount() < 0 || amount.GetCurrency() == "") {
		return fmt.Errorf("%w: succeeded payment %d without amount", ErrInvalidEvent, ev.GetPaymentId())
	}

	var (
		handled bool // false 表示该 payment_id 已经处理过，本次什么都没写
		reply   *eventv1.PaymentResultHandled
		paid    bool // 只给指标用：本次事务把订单置为了 PAID
	)
	now := s.now()
	// 事务落在订单所在的分片上：订单行、payment_results 和 outbox 都在那个库里。
	err := s.inTx(ctx, paymentTxRetry, ev.GetOrderId(), func(tx *store.Tx) error {
		handled, reply, paid = false, nil, false

		// 先锁订单行：同一订单的重复投递在此排队，与取消、关单的条件更新也互斥。
		o, err := tx.LockOrder(ctx, ev.GetOrderId())
		notFound := errors.Is(err, store.ErrNotFound)
		if err != nil && !notFound {
			return err
		}

		prev, err := tx.GetPaymentResult(ctx, ev.GetPaymentId())
		switch {
		case err == nil:
			if prev.OrderID != ev.GetOrderId() {
				// payment_id 由支付服务全局唯一地分配，同一支付单不会属于两个订单。
				return fmt.Errorf("%w: payment %d was recorded for order %d, event says order %d",
					ErrInvalidEvent, ev.GetPaymentId(), prev.OrderID, ev.GetOrderId())
			}
			return nil
		case !errors.Is(err, store.ErrNotFound):
			return err
		}

		// 热库里既没有订单也没有处理记录，而订单号老到可能已经归档：以冷库为准（见 archivedReply）。
		// 顺序必须是先热后冷，上面两次热库查询已经做完。
		if notFound && s.cold != nil && archive.MaybeCold(ev.GetOrderId(), now) {
			r, aref, archived, err := s.archivedReply(ctx, ev)
			if err != nil {
				return err
			}
			if archived {
				if r == nil {
					return nil // 归档前已经处理过这笔支付
				}
				reply = r
				answer := newEvent(aref, now)
				answer.Body = &eventv1.OrderEvent_PaymentResultHandled{PaymentResultHandled: reply}
				if err := s.addEvents(ctx, tx, answer); err != nil {
					return err
				}
				handled = true
				return nil
			}
		}

		ref := store.OrderRef{OrderID: ev.GetOrderId(), UserID: ev.GetUserId()}
		if !notFound {
			// 以订单自己记录的 user_id 为准。
			ref.UserID = o.UserID
		}
		reply = &eventv1.PaymentResultHandled{PaymentId: ev.GetPaymentId(), Disposition: accepted}
		var events []*eventv1.OrderEvent
		switch {
		case !completed.GetSucceeded():
			// 支付失败只需知悉，没有收钱，订单保持原状（仍可用另一笔支付付清）。
			// 订单不存在时也一样：没有钱需要退。
		case notFound:
			reply.Disposition, reply.Reason = refundRequired, errx.ReasonOrderNotFound
		case o.Status != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT:
			// 已取消、已关闭、创建失败、创建中，或已被另一笔支付付清：钱收了但订单不能要。
			reply.Disposition, reply.Reason = refundRequired, errx.ReasonOrderNotPayable
		case amount.GetAmount() != o.TotalAmount || amount.GetCurrency() != o.Currency:
			// 1a 里以错误返回、不做记录。事件化之后同一 payment_id 不会再带着修正后的金额重来，
			// 不给出处置的话支付服务永远等不到答复，所以记录下来并要求退款。
			reply.Disposition, reply.Reason = refundRequired, errx.ReasonAmountMismatch
		default:
			ok, err := tx.MarkPaid(ctx, o.OrderID, ev.GetPaymentId(), now)
			if err != nil {
				return err
			}
			if !ok {
				// 行已被本事务锁住且状态刚读过，不应发生。
				return fmt.Errorf("mark paid affected no rows for order %d", o.OrderID)
			}
			paidEv := newEvent(ref, now)
			paidEv.Body = &eventv1.OrderEvent_Paid{Paid: &eventv1.OrderPaid{PaymentId: ev.GetPaymentId()}}
			events = append(events, paidEv)
			paid = true
		}

		if err := tx.InsertPaymentResult(ctx, &store.PaymentResult{
			PaymentID:   ev.GetPaymentId(),
			OrderID:     ev.GetOrderId(),
			Succeeded:   completed.GetSucceeded(),
			Amount:      amount.GetAmount(),
			Disposition: reply.GetDisposition(),
		}, now); err != nil {
			return err
		}
		// 答复排在 paid 之后：两条都属于这个订单，同一分区内按写入顺序发出。
		// 即使重发让它们乱了序，消费方也不依赖谁先到（见设计文档 5.1）。
		answer := newEvent(ref, now)
		answer.Body = &eventv1.OrderEvent_PaymentResultHandled{PaymentResultHandled: reply}
		events = append(events, answer)
		if err := s.addEvents(ctx, tx, events...); err != nil {
			return err
		}
		handled = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("handle payment %d of order %d: %w", ev.GetPaymentId(), ev.GetOrderId(), err)
	}
	if !handled {
		return nil
	}
	s.afterCommit(ctx, ev.GetOrderId())
	if paid {
		transitionDone(ctx, transPendingToPaid)
		s.paymentApplied(ctx, ev)
	}
	if reply.GetDisposition() == refundRequired {
		otelx.SetReason(ctx, reply.GetReason())
		s.logger.WarnContext(ctx, "payment requires refund",
			"order_id", ev.GetOrderId(), "payment_id", ev.GetPaymentId(), "reason", reply.GetReason())
	}
	return nil
}

// archivedReply 给出对已归档订单的一笔支付结果的答复。调用方已经确认热库里没有这个订单、
// 也没有这笔支付的处理记录。
//
//   - archived 为 false：冷库里也没有这个订单，调用方按“订单不存在”处理。
//   - archived 为 true、reply 为 nil：冷库里有这笔支付的处理记录，是重复投递，什么都不用做。
//   - archived 为 true、reply 非 nil：订单已归档，这笔支付没处理过。归档的订单都是终态、不可支付：
//     支付失败只需知悉，支付成功则要求退款。和热库里对终态订单的处理结果相同。
//
// 不写任何记录。冷库不接受在线写入，而热库里已经没有这个订单，往热库的 payment_results 写
// 会留下没有订单的孤儿行。不记录也是幂等的：答复只取决于冷库里不会再变的内容，
// 重复投递每次都得出同样的答复，支付服务以第一次答复为准。
//
// 没有这条路径的话，归档之后重放一个已支付订单的支付结果（消费组位移被重置、上游重发），
// 会因为热库里订单和处理记录都没有了，而对一个正常付清的订单要求退款。
//
// 冷库查询失败时返回错误，消费者原地重试：不能在不知道订单是否存在的情况下给出涉及退款的答复。
// 两次冷库查询经过冷库的熔断器（coldDecisionRead）：打开时立刻失败（错误不是 ErrNotFound，同样重试），
// 每次最多 coldTimeout，不在热库的事务里等满事件的时限。只有 ErrNotFound 才算“冷库里没有”。
func (s *Service) archivedReply(ctx context.Context, ev *eventv1.PaymentEvent) (
	reply *eventv1.PaymentResultHandled, ref store.OrderRef, archived bool, err error,
) {
	var o *store.Order
	err = s.coldDecisionRead(ctx, func(ctx context.Context) error {
		var err error
		o, err = s.cold.GetOrder(ctx, ev.GetOrderId())
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, ref, false, nil
	}
	if err != nil {
		return nil, ref, false, fmt.Errorf("load archived order %d: %w", ev.GetOrderId(), err)
	}
	ref = store.OrderRef{OrderID: o.OrderID, UserID: o.UserID}
	switch err := s.coldDecisionRead(ctx, func(ctx context.Context) error {
		_, err := s.cold.GetPaymentResult(ctx, ev.GetOrderId(), ev.GetPaymentId())
		return err
	}); {
	case err == nil:
		return nil, ref, true, nil
	case !errors.Is(err, store.ErrNotFound):
		return nil, ref, false, fmt.Errorf("load archived payment result %d: %w", ev.GetPaymentId(), err)
	}
	reply = &eventv1.PaymentResultHandled{PaymentId: ev.GetPaymentId(), Disposition: accepted}
	if ev.GetCompleted().GetSucceeded() {
		reply.Disposition, reply.Reason = refundRequired, errx.ReasonOrderNotPayable
	}
	return reply, ref, true, nil
}

// PaymentEventsHandler 把 HandlePaymentEvent 接到 Kafka 消费者上：解码消息、加上时限、
// 把“重试也没用”的错误标记为 Permanent 送进死信。timeout <= 0 时取 DefaultEventTimeout。
func (s *Service) PaymentEventsHandler(timeout time.Duration) kafkax.Handler {
	return adapt(s, "payment event", timeout, eventx.UnmarshalPaymentEvent, s.HandlePaymentEvent)
}
