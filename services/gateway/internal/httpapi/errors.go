package httpapi

import (
	"encoding/json/v2"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/services/gateway/internal/httpapi/oas"
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

// writeGRPCError 把下游错误转成 HTTP 响应。
// code 优先用 errx 原因码，没有时用 gRPC 状态码名。
func (s *server) writeGRPCError(c *gin.Context, err error) {
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
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
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
