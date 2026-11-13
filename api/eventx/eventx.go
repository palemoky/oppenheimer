// Package eventx 集中定义事件的 topic 名、消息 key 和编解码，供发布方和订阅方共用。
//
// 事件的消息类型在 api/proto/oppenheimer/event/v1/events.proto。
// topic 名、key 的格式和序列化方式同样是契约的一部分，写在一处，避免两边各写各的而对不上。
package eventx

import (
	"errors"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
)

// topic 的基础名。实际使用时要经过 Topic 加上环境前缀。
const (
	// OrderEvents 由订单服务发布，消息类型是 OrderEvent。
	OrderEvents = "order.events"
	// PaymentEvents 由支付服务发布，消息类型是 PaymentEvent。
	PaymentEvents = "payment.events"
	// OrderSubmit 是已受理、等待落库的下单请求，消息类型是 OrderSubmitted。
	OrderSubmit = "order.submit"
	// OrderCloseDelay 是延迟关单的提醒，消息类型是 OrderCloseDue。
	// 这是一个固定延迟的 topic：里面所有消息的延迟时长都等于支付时限（见 ADR-0009）。
	OrderCloseDelay = "order.close.delay"
)

// Partitions 是本机创建 topic 时使用的分区数。
const Partitions = 4

// DLQSuffix 是死信 topic 的后缀，与 kafkax 的默认值一致。
const DLQSuffix = ".dlq"

// Topic 返回带环境前缀的 topic 名。prefix 来自配置 KAFKA_PREFIX，
// 用来让端到端测试和本机手动运行的环境共用一个 Kafka 而互不干扰。
func Topic(prefix, name string) string { return prefix + name }

// Group 返回带环境前缀的消费者组名。约定基础名为 "<消费方服务>.<topic 去掉点后的简称>"，
// 例如 inventory.order-events。
func Group(prefix, name string) string { return prefix + name }

// 各消费者组的基础名。
const (
	GroupInventoryOrderEvents = "inventory.order-events"
	GroupPaymentOrderEvents   = "payment.order-events"
	GroupOrderPaymentEvents   = "order.payment-events"
	GroupOrderSubmit          = "order.order-submit"
	GroupOrderCloseDelay      = "order.order-close-delay"
)

// OrderKey 返回以订单号为准的消息 key。
// 两个 topic 的消息都用它：同一订单的事件落在同一分区，按发布顺序到达。
func OrderKey(orderID int64) []byte {
	return strconv.AppendInt(nil, orderID, 10)
}

// MarshalOrderEvent 序列化 OrderEvent，返回消息的 key 和 value。
func MarshalOrderEvent(ev *eventv1.OrderEvent) (key, value []byte, err error) {
	if ev.GetOrderId() <= 0 {
		return nil, nil, errors.New("eventx: order event without order_id")
	}
	value, err = proto.Marshal(ev)
	if err != nil {
		return nil, nil, fmt.Errorf("eventx: marshal order event: %w", err)
	}
	return OrderKey(ev.GetOrderId()), value, nil
}

// UnmarshalOrderEvent 反序列化 OrderEvent。
// 失败说明消息不是这个类型或已损坏，重试没有意义，消费者应当把它送进死信。
func UnmarshalOrderEvent(value []byte) (*eventv1.OrderEvent, error) {
	ev := &eventv1.OrderEvent{}
	if err := proto.Unmarshal(value, ev); err != nil {
		return nil, fmt.Errorf("eventx: unmarshal order event: %w", err)
	}
	return ev, nil
}

// MarshalPaymentEvent 序列化 PaymentEvent，返回消息的 key 和 value。
// key 用订单号而不是支付单号：订单服务按订单处理支付结果，同一订单的消息应当有序。
func MarshalPaymentEvent(ev *eventv1.PaymentEvent) (key, value []byte, err error) {
	if ev.GetOrderId() <= 0 || ev.GetPaymentId() <= 0 {
		return nil, nil, errors.New("eventx: payment event without order_id or payment_id")
	}
	value, err = proto.Marshal(ev)
	if err != nil {
		return nil, nil, fmt.Errorf("eventx: marshal payment event: %w", err)
	}
	return OrderKey(ev.GetOrderId()), value, nil
}

// UnmarshalPaymentEvent 反序列化 PaymentEvent。
func UnmarshalPaymentEvent(value []byte) (*eventv1.PaymentEvent, error) {
	ev := &eventv1.PaymentEvent{}
	if err := proto.Unmarshal(value, ev); err != nil {
		return nil, fmt.Errorf("eventx: unmarshal payment event: %w", err)
	}
	return ev, nil
}

// MarshalOrderSubmitted 序列化 OrderSubmitted，返回消息的 key 和 value。
func MarshalOrderSubmitted(m *eventv1.OrderSubmitted) (key, value []byte, err error) {
	if m.GetOrderId() <= 0 {
		return nil, nil, errors.New("eventx: order submitted without order_id")
	}
	value, err = proto.Marshal(m)
	if err != nil {
		return nil, nil, fmt.Errorf("eventx: marshal order submitted: %w", err)
	}
	return OrderKey(m.GetOrderId()), value, nil
}

// UnmarshalOrderSubmitted 反序列化 OrderSubmitted。
func UnmarshalOrderSubmitted(value []byte) (*eventv1.OrderSubmitted, error) {
	m := &eventv1.OrderSubmitted{}
	if err := proto.Unmarshal(value, m); err != nil {
		return nil, fmt.Errorf("eventx: unmarshal order submitted: %w", err)
	}
	return m, nil
}

// MarshalOrderCloseDue 序列化 OrderCloseDue，返回消息的 key 和 value。
func MarshalOrderCloseDue(m *eventv1.OrderCloseDue) (key, value []byte, err error) {
	if m.GetOrderId() <= 0 {
		return nil, nil, errors.New("eventx: order close due without order_id")
	}
	value, err = proto.Marshal(m)
	if err != nil {
		return nil, nil, fmt.Errorf("eventx: marshal order close due: %w", err)
	}
	return OrderKey(m.GetOrderId()), value, nil
}

// UnmarshalOrderCloseDue 反序列化 OrderCloseDue。
func UnmarshalOrderCloseDue(value []byte) (*eventv1.OrderCloseDue, error) {
	m := &eventv1.OrderCloseDue{}
	if err := proto.Unmarshal(value, m); err != nil {
		return nil, fmt.Errorf("eventx: unmarshal order close due: %w", err)
	}
	return m, nil
}
