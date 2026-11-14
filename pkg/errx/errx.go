// Package errx 定义服务间传递业务错误的约定：gRPC 状态码表示错误类别，
// ErrorInfo.Reason 携带机器可读的原因码。调用方按原因码分支，不解析错误消息。
package errx

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Domain 是本系统所有原因码所属的域。
const Domain = "oppenheimer"

// 原因码。跨服务透传的放在这里，保证各服务用同一个字符串。
const (
	ReasonInvalidArgument = "INVALID_ARGUMENT"

	ReasonSKUNotFound          = "SKU_NOT_FOUND"
	ReasonInsufficientStock    = "INSUFFICIENT_STOCK"
	ReasonPurchaseLimit        = "PURCHASE_LIMIT_EXCEEDED"
	ReasonReservationNotFound  = "RESERVATION_NOT_FOUND"
	ReasonReservationReleased  = "RESERVATION_RELEASED"
	ReasonReservationConfirmed = "RESERVATION_CONFIRMED"
	ReasonReservationMismatch  = "RESERVATION_MISMATCH"
	ReasonMixedMerchants       = "MIXED_MERCHANTS"

	ReasonOrderNotFound        = "ORDER_NOT_FOUND"
	ReasonOrderNotCancellable  = "ORDER_NOT_CANCELLABLE"
	ReasonOrderNotPayable      = "ORDER_NOT_PAYABLE"
	ReasonIdempotencyKeyReused = "IDEMPOTENCY_KEY_REUSED"
	ReasonAmountMismatch       = "AMOUNT_MISMATCH"
	ReasonCreateTimeout        = "CREATE_TIMEOUT"
	// ReasonBacklogged：等待落库的订单积压过多，暂不受理（RESOURCE_EXHAUSTED，HTTP 429）。
	// 什么都没有发生，可以稍后用同一个幂等键重试。
	ReasonBacklogged = "BACKLOGGED"

	ReasonPaymentNotFound = "PAYMENT_NOT_FOUND"

	// 以下是“降级”的原因码，见 Degraded。
	//
	// ReasonStockGateUnavailable：库存闸门（Redis）不可用，暂不受理下单；或者查的订单还没落库，说不清它的状态。
	ReasonStockGateUnavailable = "STOCK_GATE_UNAVAILABLE"
	// ReasonOrderLandingUnavailable：订单落库在持续失败（库存服务或订单库不可用），暂不受理下单。
	ReasonOrderLandingUnavailable = "ORDER_LANDING_UNAVAILABLE"
	// ReasonInventoryUnavailable：受理时要请库存服务装载 SKU，而它不可用。
	ReasonInventoryUnavailable = "INVENTORY_UNAVAILABLE"
	// ReasonOrderQueueUnavailable：受理的消息发不出去（Kafka 不可用）。
	ReasonOrderQueueUnavailable = "ORDER_QUEUE_UNAVAILABLE"
	// ReasonOrderUnavailable：别的服务（payment）要先问订单服务，而它不可用。
	ReasonOrderUnavailable = "ORDER_UNAVAILABLE"
	// ReasonArchiveUnavailable：冷库不可用，已归档的订单暂时查不到。
	ReasonArchiveUnavailable = "ARCHIVE_UNAVAILABLE"
	// ReasonFeatureDisabled：这个功能被人工关掉了（降级开关）。
	ReasonFeatureDisabled = "FEATURE_DISABLED"
)

// degradedReasons 是全部降级原因码。
var degradedReasons = map[string]bool{
	ReasonStockGateUnavailable:    true,
	ReasonOrderLandingUnavailable: true,
	ReasonInventoryUnavailable:    true,
	ReasonOrderQueueUnavailable:   true,
	ReasonOrderUnavailable:        true,
	ReasonArchiveUnavailable:      true,
	ReasonFeatureDisabled:         true,
}

// Degraded 构造“这个服务是好的，但它的某个依赖不可用（或功能被关掉），所以这个请求现在做不了”的错误。
// 什么都没有发生，调用方可以稍后重试。网关把它映射成 HTTP 503 并带 Retry-After。
//
// gRPC 状态码用 FAILED_PRECONDITION 而不是 UNAVAILABLE，这是有意的（设计文档 1d 的 6.16）：
// 调用方的 Envoy 边车把 UNAVAILABLE 算作这个服务的一次失败，连续 5 次就把整个服务摘掉 5 秒。
// 闸门的 Redis 坏了，下单请求全部被拒绝；要是用 UNAVAILABLE，gateway 的边车会把 order 整个摘掉，
// 本来不受影响的查询、取消、支付也一起不可用，降级就成了全挂。FAILED_PRECONDITION 在边车那里
// 归为 4xx，不计入摘除。服务自己真的不行了（主库连不上、内部错误）才用 UNAVAILABLE 和 INTERNAL。
//
// reason 必须是上面列出的降级原因码之一。
// 消息会经网关原样给到最终用户，只写给调用方看的话，不要把内部的错误拼进去。
func Degraded(reason, format string, args ...any) error {
	return New(codes.FailedPrecondition, reason, format, args...)
}

// IsDegraded 判断 err 是不是 Degraded 构造的错误（按原因码）。
func IsDegraded(err error) bool {
	return degradedReasons[Reason(err)]
}

// New 构造带原因码的 gRPC 错误。
func New(code codes.Code, reason, format string, args ...any) error {
	st := status.New(code, fmt.Sprintf(format, args...))
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: Domain})
	if err != nil {
		// 只有 code 为 OK 或 detail 无法序列化时才会失败，两者都是编程错误。
		// 退回不带详情的状态，仍然是一个可用的错误。
		return st.Err()
	}
	return detailed.Err()
}

// Reason 返回 err 携带的原因码。err 不是本系统的业务错误时返回空字符串。
func Reason(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == Domain {
			return info.GetReason()
		}
	}
	return ""
}

// Is 判断 err 是否携带指定的原因码。
func Is(err error, reason string) bool {
	return reason != "" && Reason(err) == reason
}

// Code 返回 err 的 gRPC 状态码。nil 为 OK，非 gRPC 错误为 Unknown。
func Code(err error) codes.Code {
	return status.Code(err)
}
