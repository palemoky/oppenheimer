// Package verify 实现杀进程测试的对账检查（设计文档 7.3.3）和静止等待（7.3.2 第 3 步）。
//
// 检查分两层：Collect 把三个库、闸门、死信 topic、库存服务日志读成一个 Snapshot；
// Check 是纯函数，输入 Snapshot 和客户端账本，输出违规列表和计数。这样检查逻辑可以不连任何
// 外部系统就测试，连库的部分只负责“读对”。
package verify

import (
	"fmt"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
)

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

// SKUStock 是一个 SKU 的三个库存数（Snapshot.Stock 里是真实数，RawStock 里是 sku_stock 的原始值）。
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

	// Stock 是库存的真实数：sku_stock 的值加上 stock_pending 里没有应用的增量。检查用它。
	Stock map[int64]SKUStock
	// RawStock 是 sku_stock 表里的原始值（落后于真实数），只用来检查恒等式和报告第 22 条。
	// 为 nil 时（测试直接构造的 Snapshot）只检查 Stock。
	RawStock map[int64]SKUStock
	// Pending 是 stock_pending 里还没有应用的增量，按 SKU 汇总；空表为空 map。
	Pending      map[int64]PendingSKU
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

	// MerchantIndex 是商家索引对账要的数据；没有给索引库时为 nil，对应的检查跳过。
	MerchantIndex *MerchantIndexData

	// Archive 是冷热归档对账的结果；没有给冷库时为 nil，对应的检查跳过。
	Archive *ArchiveData

	// Corrections 是库存服务日志里闸门对账做过修正的行；Capped 是偏差持续但被上限截住的行。
	Corrections []string
	Capped      []string
}

// MerchantOrderRow 是订单库（merchant_id 非空的订单）或商家索引表里的一行，只取对账要比的列。
type MerchantOrderRow struct {
	OrderID     int64
	MerchantID  int64
	Status      int
	TotalAmount int64
	// Version 在订单侧是 orders.version，在索引侧是 src_version。
	Version int64
	// Slot 是行所在的物理表的 slot：订单侧是 orders_{slot}，索引侧是 merchant_orders_{slot}。
	Slot int
}

// MerchantIndexData 是商家索引与订单表的两侧数据。
type MerchantIndexData struct {
	Orders []MerchantOrderRow // 订单侧：全部 merchant_id IS NOT NULL 的订单
	// ColdOrders 是冷库里 merchant_id 非空的订单（没有给冷库时为空）。归档后订单从热库删掉，索引行保留，
	// 所以订单侧是 Orders 并上 ColdOrders；同一订单两边都有时取热库的。
	ColdOrders []MerchantOrderRow
	Index      []MerchantOrderRow // 索引侧：全部 merchant_orders_{slot} 的行
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
	// ArchiveInFlight 为 true 表示归档任务还在跑：第 21 条里“同一订单热库冷库都有”只记 note，不算失败。
	// 零值（false）是归档已结束，这是默认。
	ArchiveInFlight bool
}

// Issues 是一类问题的总数和前几个例子（按订单号升序）。例子只留几个：出问题时可能有成千上万行。
type Issues struct {
	N        int
	Examples []string
}

func (is *Issues) add(format string, args ...any) {
	is.N++
	if len(is.Examples) < archiveExamples {
		is.Examples = append(is.Examples, fmt.Sprintf(format, args...))
	}
}

// ArchiveData 是第 21 条（冷热归档一致）在读库时就算好的结果。冷库可能有几百万行，
// 不把订单号带进 Snapshot，只留各类问题的数量和例子。
type ArchiveData struct {
	// Cutoff 是“现在减 ArchiveMinAge”：冷库里不应该有订单号时间不早于它的订单，热库子表的孤儿也只查早于它的。
	Cutoff time.Time
	// ColdOrders 是冷库的订单数，HotOrders 是参与比较的热库订单数。
	ColdOrders, HotOrders int64

	NonFinal Issues // 冷库里有非终态订单
	TooNew   Issues // 冷库里有订单号时间不早于 Cutoff 的订单
	Both     Issues // 同一个订单号热库冷库都有
	NoItems  Issues // 冷库订单没有任何明细行
	// ItemOrphans、PaymentOrphans 是冷库里有明细、支付结果行但没有订单行的订单号。
	ItemOrphans, PaymentOrphans Issues
	// HotItemOrphans、HotPaymentOrphans 是热库里订单号早于 Cutoff、有明细或支付结果行但没有订单行的订单号。
	HotItemOrphans, HotPaymentOrphans Issues
}
