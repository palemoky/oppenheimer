// Package consumer 消费 order.events，处理订单服务对支付结果的答复（payment_result_handled）。
//
// 投递是至少一次，而且不保证严格有序（outbox 重发可能让旧消息排到新消息后面）。
// 所以这里不依赖“消息到达的先后”，每一步都是带前置状态的条件更新：
// 同一条答复处理多少次，结果和副作用都与一次相同。
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/palemoky/oppenheimer/api/eventx"
	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/services/payment/internal/store"
)

// DefaultTimeout 是处理一条消息的超时。
//
// 远小于消费者组的再平衡超时（默认 60 秒）：分区被收回时要等在途的 handler 返回，
// 卡得太久本实例会被踢出组，新持有者会和这里并发处理同一条消息。
const DefaultTimeout = 5 * time.Second

// ErrPaymentPending 表示答复到达时支付单在本地还是 PENDING。
//
// 正常情况下不会发生：事件和状态变更在同一个事务里提交，订单服务收到事件时状态一定已经可见。
// 但消费者不能假设这一点（例如将来读从库有复制延迟），所以返回普通错误让消费者重试，
// 而不是在 PENDING 上做任何标记。
var ErrPaymentPending = errors.New("payment result reply arrived before payment completed")

// Handler 处理 order.events 上的事件。
type Handler struct {
	store  *store.Store
	logger *slog.Logger
	now    func() time.Time
}

// New 返回 Handler。now 为 nil 时用 time.Now。
func New(st *store.Store, logger *slog.Logger, now func() time.Time) *Handler {
	if now == nil {
		now = time.Now
	}
	return &Handler{store: st, logger: logger, now: now}
}

// Kafka 把 HandleOrderEvent 适配成 kafkax.Handler：反序列化、加超时。
//
// 消费者传入的 ctx 只在强制停止时取消，所以超时必须在这里加。
func (h *Handler) Kafka(timeout time.Duration) kafkax.Handler {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return func(ctx context.Context, msg kafkax.Message) error {
		ev, err := eventx.UnmarshalOrderEvent(msg.Value)
		if err != nil {
			// 消息本身坏了，重试没有意义。
			return kafkax.Permanent(err)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return h.HandleOrderEvent(ctx, ev)
	}
}

// HandleOrderEvent 处理一条 OrderEvent。只关心 payment_result_handled，其他事件直接跳过。
//
// 返回 kafkax.Permanent 包装的错误表示重试也不会成功（消息不合法、支付单不存在），应进死信；
// 返回普通错误表示暂时失败，消费者会原地重试。
func (h *Handler) HandleOrderEvent(ctx context.Context, ev *eventv1.OrderEvent) error {
	reply := ev.GetPaymentResultHandled()
	if reply == nil {
		return nil
	}
	otelx.SetOrder(ctx, ev.GetOrderId(), ev.GetUserId())
	if reply.GetPaymentId() <= 0 {
		return kafkax.Permanent(fmt.Errorf("payment_result_handled without payment_id (order %d)", ev.GetOrderId()))
	}
	log := h.logger.With("payment_id", reply.GetPaymentId(), "order_id", ev.GetOrderId(),
		"disposition", reply.GetDisposition().String(), "reason", reply.GetReason())

	pay, err := h.store.GetByID(ctx, reply.GetPaymentId())
	if errors.Is(err, store.ErrNotFound) {
		// 订单服务答复了一笔本服务没有的支付：要么消息错了，要么数据丢了，重试不会让它出现。
		return kafkax.Permanent(fmt.Errorf("payment %d: %w", reply.GetPaymentId(), err))
	}
	if err != nil {
		return err
	}
	if pay.OrderID != ev.GetOrderId() {
		return kafkax.Permanent(fmt.Errorf("payment %d belongs to order %d, reply is for order %d",
			pay.PaymentID, pay.OrderID, ev.GetOrderId()))
	}
	if pay.Status == paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING {
		return fmt.Errorf("payment %d: %w", pay.PaymentID, ErrPaymentPending)
	}

	now := h.now().UTC().Truncate(time.Microsecond)
	switch reply.GetDisposition() {
	case eventv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED:
		err := h.accept(ctx, log, pay, now)
		if err == nil {
			orderReplies.Add(ctx, 1, replyAccepted)
		}
		return err
	case eventv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED:
		err := h.refund(ctx, log, pay, reply.GetReason(), now)
		if err == nil {
			orderReplies.Add(ctx, 1, replyRefundRequired)
		}
		return err
	default:
		orderReplies.Add(ctx, 1, replyUnknown)
		// 不认识的答复说明双方版本不一致，重试也不会变，进死信等人处理。不能当作已答复。
		return kafkax.Permanent(fmt.Errorf("payment %d: unknown disposition %v", pay.PaymentID, reply.GetDisposition()))
	}
}

// accept 标记订单服务已接受。前置条件 notified = 0（在 store 里还带上读到的状态）：重复的答复不生效。
func (h *Handler) accept(ctx context.Context, log *slog.Logger, pay store.Payment, now time.Time) error {
	if pay.Notified {
		if pay.Status == paymentv1.PaymentStatus_PAYMENT_STATUS_REFUNDED {
			// 已经按“需要退款”退了，现在又说接受：订单服务对同一笔支付给了两个答复。以第一个为准。
			log.ErrorContext(ctx, "conflicting payment reply ignored: accepted after refunded")
		}
		return nil
	}
	ok, err := h.store.MarkNotified(ctx, pay.PaymentID, pay.Status, now)
	if err != nil {
		return err
	}
	if !ok {
		// 读和写之间状态变了：可能是同一条答复被并发处理，也可能是一条矛盾的答复抢先生效。
		// 重读一遍，把真正的矛盾记下来；重复处理则什么也不做。
		return h.checkAfterLost(ctx, log, pay.PaymentID, eventv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED)
	}
	return nil
}

// refund 处理“需要退款”：成功的支付退款并标记，失败的支付没有钱可退，只标记。
func (h *Handler) refund(ctx context.Context, log *slog.Logger, pay store.Payment, reason string, now time.Time) error {
	if pay.Notified {
		if pay.Status == paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED {
			// 已答复过“接受”，现在又要求退款：订单服务对同一笔支付给了两个答复。以第一个为准。
			log.ErrorContext(ctx, "conflicting payment reply ignored: refund required after accepted")
		}
		return nil
	}

	switch pay.Status {
	case paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED:
		ok, err := h.store.RefundAndMarkNotified(ctx, pay.PaymentID, now)
		if err != nil {
			return err
		}
		if !ok {
			return h.checkAfterLost(ctx, log, pay.PaymentID, eventv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED)
		}
		// 只有完成转换的那一次记录退款，重复处理不会产生第二条退款记录。
		refunds.Add(ctx, 1, refundReasonOpt(reason))
		logRejection(ctx, log, reason)
		log.InfoContext(ctx, "payment refunded", "amount", pay.Amount, "currency", pay.Currency)
		return nil

	case paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED:
		ok, err := h.store.MarkNotified(ctx, pay.PaymentID, pay.Status, now)
		if err != nil {
			return err
		}
		if ok {
			logRejection(ctx, log, reason)
			// 失败的支付没有钱可退，订单服务这样答复不合约定，但答复已送达。
			log.WarnContext(ctx, "refund required for failed payment")
		}
		return nil

	default:
		// REFUNDED 而 notified = 0 不应出现（退款和标记在同一条语句里），说明数据被手工改过。
		log.ErrorContext(ctx, "refund required for payment in unexpected state", "status", pay.Status.String())
		return nil
	}
}

// logRejection 对 AMOUNT_MISMATCH、ORDER_NOT_FOUND 记 error：
// 正常流程里这两种答复不应出现（支付单创建时校验过订单和金额），出现说明系统有缺陷。
func logRejection(ctx context.Context, log *slog.Logger, reason string) {
	if reason == errx.ReasonAmountMismatch || reason == errx.ReasonOrderNotFound {
		log.ErrorContext(ctx, "payment rejected by order service")
	}
}

// checkAfterLost 在条件更新没抢到时重读支付单。另一次处理已经得出同样的结果就静默返回；
// 若结果与这次的答复矛盾（一笔支付既被接受又要求退款），记 error，以先生效的为准。
func (h *Handler) checkAfterLost(ctx context.Context, log *slog.Logger, paymentID int64, d eventv1.PaymentDisposition) error {
	cur, err := h.store.GetByID(ctx, paymentID)
	if err != nil {
		return err
	}
	if !cur.Notified {
		// 状态变了却还没标记，不应发生；返回错误让消费者重试，重读后会走正常分支。
		return fmt.Errorf("payment %d changed to %v without notification", paymentID, cur.Status)
	}
	refunded := cur.Status == paymentv1.PaymentStatus_PAYMENT_STATUS_REFUNDED
	if (d == eventv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED) == refunded {
		log.ErrorContext(ctx, "conflicting payment reply ignored", "current_status", cur.Status.String())
	}
	return nil
}
