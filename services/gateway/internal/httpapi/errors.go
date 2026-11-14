package httpapi

import (
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi/oas"
)

// writeJSON 用 encoding/json/v2 输出响应。先序列化再写，失败时还能改成 500。
func writeJSON(c *gin.Context, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		// DTO 都是固定结构，走到这里是代码缺陷；不把细节给客户端。
		c.Error(err) //nolint:errcheck // 只记录到 gin 的错误列表
		status, b = http.StatusInternalServerError, []byte(`{"error":{"code":"INTERNAL","message":"internal error"}}`)
	}
	c.Header("Content-Type", "application/json; charset=utf-8")
	c.Status(status)
	_, _ = c.Writer.Write(b)
}

func writeError(c *gin.Context, status int, code, message string) {
	writeJSON(c, status, oas.Error{Error: oas.ErrorDetail{Code: code, Message: message}})
}

// statusClientClosedRequest 是客户端在响应返回之前断开了连接时记下的状态码。它不是标准状态码，
// 是 Nginx 的约定（client closed request），net/http 里没有对应的常量。客户端已经走了，这个响应没有人收，
// 它的意义在访问日志和统计里：把“客户端放弃了”和“服务端出错了”（500）、“服务端超时了”（504）分开，
// 否则客户端一断开，错误率和告警就跟着涨，而服务端其实没有问题。
const statusClientClosedRequest = 499

// codeClientClosedRequest 是 499 的原因码。
const codeClientClosedRequest = "CLIENT_CLOSED_REQUEST"

// clientClosed 报告客户端是否已经断开：请求自己的 ctx 被取消了。net/http 在连接断开时取消请求的 ctx。
//
// 判断依据只能是请求的 ctx，不能是下游返回的 Canceled：
//   - withDeadline 设的总时限到期时，ctx 的错误是 DeadlineExceeded，不是 Canceled。那是服务端超时，仍然是 504。
//   - 下游返回了 Canceled 而请求的 ctx 没有被取消，是下游自己的问题（比如它取消了它自己的调用），按原来的映射处理。
func clientClosed(c *gin.Context) bool {
	return errors.Is(c.Request.Context().Err(), context.Canceled)
}

// writeGRPCError 把下游错误转成 HTTP 响应。
// code 优先用 errx 原因码，没有时用 gRPC 状态码名。
//
// 客户端已经断开时一律是 499，不看下游返回了什么：这时下游通常返回 Canceled，也可能恰好返回了别的错误，
// 但不管是什么，都没有人在等这个响应了。
func (s *server) writeGRPCError(c *gin.Context, err error) {
	if clientClosed(c) {
		// 记 INFO 而不是 ERROR（设计文档 1d 5.9）：客户端主动断开是正常现象（用户关了页面、客户端自己超时），
		// 不是服务端的故障，不该触发告警。不完全不记：这一条带着断开时正在进行的下游调用和它的错误，
		// 访问日志里只有一个 499，看不出请求当时卡在哪里。
		s.logger.LogAttrs(c.Request.Context(), slog.LevelInfo, "client closed request",
			slog.String("route", c.FullPath()), slog.Any("error", err))
		writeError(c, statusClientClosedRequest, codeClientClosedRequest, "client closed request")
		return
	}
	st, _ := status.FromError(err) // 非 gRPC 错误得到 Unknown 状态
	code := errx.Reason(err)
	if code == "" {
		code = codeName(st.Code())
	}
	httpStatus := httpStatusOf(st.Code())

	message := st.Message()
	if httpStatus == http.StatusInternalServerError {
		// 500 的消息可能带下游内部细节，只记日志不透出。
		s.logger.ErrorContext(c.Request.Context(), "downstream call failed", "error", err, "grpc_code", st.Code().String())
		message = "internal error"
	}
	writeError(c, httpStatus, code, message)
}

func httpStatusOf(c codes.Code) int {
	switch c {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.FailedPrecondition:
		// INSUFFICIENT_STOCK 也属于此类，同为 409。
		return http.StatusConflict
	case codes.Unavailable:
		// 也包括 Envoy 边车的路由超时（DownstreamHopTimeout）到期：边车回给 gateway 的是 Unavailable，
		// 错误信息里有 upstream request timeout。这里不解析错误信息去把它改成 504：信息的格式是 Envoy 的实现细节，
		// 而且进程里的同一个时限（WithHopTimeout）和边车的一样长，通常先到期的是进程里的那个，走下面的 504。
		return http.StatusServiceUnavailable
	case codes.ResourceExhausted:
		// 目前只有下单的削峰（BACKLOGGED）。429 告诉客户端“稍后重试”，和下游故障的 503 分开。
		return http.StatusTooManyRequests
	case codes.DeadlineExceeded:
		// 两种来源：gateway 的总时限（withDeadline，DefaultCallTimeout）到期，或者这一跳的时限
		// （grpcx.WithHopTimeout，DownstreamHopTimeout）到期。对客户端都是“下游没有及时回答”。
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

// codeName 返回 gRPC 状态码的大写下划线名，如 FAILED_PRECONDITION。
func codeName(c codes.Code) string {
	switch c {
	case codes.OK:
		return "OK"
	case codes.Canceled:
		return "CANCELLED"
	case codes.Unknown:
		return "UNKNOWN"
	case codes.InvalidArgument:
		return "INVALID_ARGUMENT"
	case codes.DeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case codes.NotFound:
		return "NOT_FOUND"
	case codes.AlreadyExists:
		return "ALREADY_EXISTS"
	case codes.PermissionDenied:
		return "PERMISSION_DENIED"
	case codes.ResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case codes.FailedPrecondition:
		return "FAILED_PRECONDITION"
	case codes.Aborted:
		return "ABORTED"
	case codes.OutOfRange:
		return "OUT_OF_RANGE"
	case codes.Unimplemented:
		return "UNIMPLEMENTED"
	case codes.Internal:
		return "INTERNAL"
	case codes.Unavailable:
		return "UNAVAILABLE"
	case codes.DataLoss:
		return "DATA_LOSS"
	case codes.Unauthenticated:
		return "UNAUTHENTICATED"
	default:
		return "UNKNOWN"
	}
}

// isBodyTooLarge 判断读请求体时是否触发了大小限制。
func isBodyTooLarge(err error) bool {
	_, ok := errors.AsType[*http.MaxBytesError](err)
	return ok
}
