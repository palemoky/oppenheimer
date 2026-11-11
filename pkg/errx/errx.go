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
	ReasonReservationNotFound  = "RESERVATION_NOT_FOUND"
	ReasonReservationReleased  = "RESERVATION_RELEASED"
	ReasonReservationConfirmed = "RESERVATION_CONFIRMED"
	ReasonReservationMismatch  = "RESERVATION_MISMATCH"

	ReasonOrderNotFound        = "ORDER_NOT_FOUND"
	ReasonOrderNotCancellable  = "ORDER_NOT_CANCELLABLE"
	ReasonOrderNotPayable      = "ORDER_NOT_PAYABLE"
	ReasonIdempotencyKeyReused = "IDEMPOTENCY_KEY_REUSED"
	ReasonAmountMismatch       = "AMOUNT_MISMATCH"
	ReasonCreateTimeout        = "CREATE_TIMEOUT"

	ReasonPaymentNotFound = "PAYMENT_NOT_FOUND"
)

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
