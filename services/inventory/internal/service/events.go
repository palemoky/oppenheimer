package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/palemoky/Oppenheimer/api/eventx"
	eventv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/event/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
)

// DefaultEventTimeout 是处理一条订单事件的时限。
//
// 消费者收回分区时要等在途的 handler 返回，卡住超过再平衡超时（60 秒）本实例会被踢出
// 消费者组，新的持有者会并发处理同一条消息。一次 Confirm/Release 是一个小事务加最多几次
// 死锁退避，正常远小于 1 秒；10 秒足够应付数据库抖动，又远小于再平衡超时。超时按普通错误
// 返回，由消费者原地重试。
const DefaultEventTimeout = 10 * time.Second

// HandleOrderEvent 处理一条 order.events 的事件，是库存服务对订单终态的响应。
//
// 消息按至少一次投递，且不假设严格有序：Confirm、Release 都以 order_id 幂等，
// 同一事件处理多次与一次结果相同；failed 先于迟到的 Reserve 到达时，Release 留下的
// 空回滚标记会挡住那次 Reserve。
//
// 返回 nil 表示事件已处理（或无需处理）；返回 kafkax.Permanent 表示订单与库存出现了
// 不可能的组合，重试没有意义，应进死信待人工处理；其他错误（数据库故障、超时等）由消费者重试。
func (s *Service) HandleOrderEvent(ctx context.Context, ev *eventv1.OrderEvent) error {
	var (
		op string
		fn func(context.Context, int64) error
	)
	switch ev.GetBody().(type) {
	case *eventv1.OrderEvent_Paid:
		op, fn = "confirm", s.confirm
	case *eventv1.OrderEvent_Cancelled, *eventv1.OrderEvent_Closed, *eventv1.OrderEvent_Failed:
		op, fn = "release", s.release
	default:
		// payment_result_handled 和以后新增的事件与库存无关，直接确认跳过。
		return nil
	}

	orderID := ev.GetOrderId()
	if orderID <= 0 {
		// 发布方保证有 order_id（eventx.MarshalOrderEvent 会拒绝），出现就是消息损坏。
		return kafkax.Permanent(fmt.Errorf("order event %s without order_id", op))
	}

	err := fn(ctx, orderID)
	if err == nil {
		return nil
	}
	if impossible(op, err) {
		// 订单已经进入终态，库存却处在相反的状态，两边至少有一边出了 bug。
		// 不自作主张修复（例如把已释放的库存再扣一次），送进死信由人判断。
		s.logger.ErrorContext(ctx, "order event conflicts with reservation state",
			"op", op, "order_id", orderID, "reason", errx.Reason(err), "error", err.Error())
		return kafkax.Permanent(fmt.Errorf("%s order %d: %w", op, orderID, err))
	}
	return fmt.Errorf("%s order %d: %w", op, orderID, err)
}

// impossible 判断 op 的错误是否说明订单与库存出现了不可能的组合。
func impossible(op string, err error) bool {
	switch op {
	case "confirm":
		// 订单已支付：它的预占必然在下单时成功过（下单同步 Reserve），
		// 不应已被释放，也不应不存在。
		return errx.Is(err, errx.ReasonReservationReleased) || errx.Is(err, errx.ReasonReservationNotFound)
	case "release":
		// 订单被取消、关闭或创建失败：都不是已支付，预占不应已转为已售。
		return errx.Is(err, errx.ReasonReservationConfirmed)
	default:
		return false
	}
}

// OrderEventHandler 把 HandleOrderEvent 适配成 Kafka 消费者的 handler：
// 解码消息、加上处理时限。timeout <= 0 时用 DefaultEventTimeout。
func (s *Service) OrderEventHandler(timeout time.Duration) kafkax.Handler {
	if timeout <= 0 {
		timeout = DefaultEventTimeout
	}
	return func(ctx context.Context, msg kafkax.Message) error {
		ev, err := eventx.UnmarshalOrderEvent(msg.Value)
		if err != nil {
			// 无法解码的消息重试多少次都一样，直接进死信。
			return kafkax.Permanent(err)
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		err = s.HandleOrderEvent(ctx, ev)
		if err != nil && !kafkax.IsPermanent(err) && errors.Is(err, context.DeadlineExceeded) {
			s.logger.WarnContext(ctx, "order event timed out, will retry",
				"order_id", ev.GetOrderId(), "timeout", timeout.String())
		}
		return err
	}
}
