// Package verify 实现杀进程测试的对账检查（设计文档 7.3.3）和静止等待（7.3.2 第 3 步）。
//
// 检查分两层：Collect 把三个库、闸门、死信 topic、库存服务日志读成一个 Snapshot；
// Check 是纯函数，输入 Snapshot 和客户端账本，输出违规列表和计数。这样检查逻辑可以不连任何
// 外部系统就测试，连库的部分只负责“读对”。
package verify

import "github.com/palemoky/oppenheimer/test/load/internal/ledger"

// 订单状态，与 proto OrderStatus 的数值一致（order 的 00001_init.sql）。
const (
	OrderCreating       = 1
	OrderPendingPayment = 2
	OrderPaid           = 3
	OrderCancelled      = 4
	OrderClosed         = 5
	OrderFailed         = 6
)

// 支付单状态，与 proto PaymentStatus 的数值一致（payment 的 00001_init.sql）。
const (
	PaymentPending   = 1
	PaymentSucceeded = 2
	PaymentFailed    = 3
	PaymentRefunded  = 4
)

// 预占状态（inventory 的 00001_init.sql）。
const (
	ReservationReserved  = 1
	ReservationConfirmed = 2
	ReservationReleased  = 3
)

// OrderStatusName 返回订单状态的名字，用于报告。
func OrderStatusName(s int) string {
	switch s {
	case OrderCreating:
		return "CREATING"
	case OrderPendingPayment:
		return "PENDING_PAYMENT"
	case OrderPaid:
		return "PAID"
	case OrderCancelled:
		return "CANCELLED"
	case OrderClosed:
		return "CLOSED"
	case OrderFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// PaymentStatusName 返回支付单状态的名字。
func PaymentStatusName(s int) string {
	switch s {
	case PaymentPending:
		return "PENDING"
	case PaymentSucceeded:
		return "SUCCEEDED"
	case PaymentFailed:
		return "FAILED"
	case PaymentRefunded:
		return "REFUNDED"
	default:
		return "UNKNOWN"
	}
}

// ReservationStatusName 返回预占状态的名字。
func ReservationStatusName(s int) string {
	switch s {
	case ReservationReserved:
		return "RESERVED"
	case ReservationConfirmed:
		return "CONFIRMED"
	case ReservationReleased:
		return "RELEASED"
	default:
		return "UNKNOWN"
	}
}

// SKUStock 是 sku_stock 的一行。
type SKUStock struct {
	SKUID     int64
	Available int64
	Reserved  int64
	Sold      int64
}

// Reservation 是 reservation 头记录及其明细。
type Reservation struct {
	OrderID int64
	Status  int
	Items   map[int64]int64 // sku → quantity
}

// Order 是 orders 的一行及其明细。
type Order struct {
	OrderID        int64
	UserID         int64
	Status         int
	TotalAmount    int64
	Currency       string
	IdempotencyKey string
	PaymentID      int64 // 0 表示 NULL
	FailReason     string
	Items          map[int64]int64 // sku → quantity
}

// Payment 是 payments 的一行。
type Payment struct {
	PaymentID int64
	OrderID   int64
	UserID    int64
	Status    int
	Amount    int64
	Currency  string
	Notified  bool
}

// GateStock 是闸门里一个 SKU 的数量。
type GateStock struct {
	Avail int64
	Held  int64
	Limit int64
}

// DLQMessage 是死信 topic 里的一条消息。
type DLQMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	// Headers 是 kafkax 附加的 dlq-* header（原 topic、消费者组、错误、尝试次数）。
	Headers map[string]string
	// Body 是解码后的消息内容（解不出时是十六进制）。
	Body string
}

// Snapshot 是对账需要的全部服务端数据。
type Snapshot struct {
	// Initial 是各 SKU 灌入时的库存（命令行参数）。
	Initial map[int64]int64

	Stock        map[int64]SKUStock
	Reservations map[int64]Reservation
	Orders       map[int64]Order
	// OrdersByIdem 以 (user_id, 幂等键) 索引 Orders。
	OrdersByIdem map[IdemRef]int64
	Payments     []Payment

	// Gate 是闸门里已装载的 SKU（gate:skus 里的成员）。
	Gate map[int64]GateStock
	// Bought 是 gate:bought:<sku> 的内容：sku → user → 数量。
	Bought map[int64]map[int64]int64
	// Holds 是仍然存在的 gate:hold:<order_id>。
	Holds map[int64]bool
	// Unclaimed、Claimed 是 gate:holds 和 gate:claimed 的成员。
	Unclaimed []int64
	Claimed   []int64

	// OutboxPending 是各服务 outbox 里未发布的行数，key 是服务名。
	OutboxPending map[string]int64

	DLQ []DLQMessage

	// Corrections 是库存服务日志里闸门对账做过修正的行；Capped 是偏差持续但被上限截住的行。
	Corrections []string
	Capped      []string
}

// IdemRef 是订单库唯一键 (user_id, idempotency_key)。
type IdemRef struct {
	UserID int64
	Key    string
}

// Input 是 Check 的全部输入。
type Input struct {
	Snapshot *Snapshot
	Ledger   []ledger.Entry
}
