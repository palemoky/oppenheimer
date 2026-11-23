// Package gwapi 是压测工具对网关 HTTP 接口的最小客户端：路径、请求头、订单状态、原因码和响应的解析。
// 接口的定义见 api/openapi/gateway.yaml；这里只放 run 和 probe 共用、对接口形状敏感的部分。
package gwapi

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 路径和请求头。
const (
	PathOrders    = "/api/v1/orders"
	HeaderUserID  = "X-User-Id"
	HeaderIdemKey = "Idempotency-Key"
	// HeaderLoadRun 标记压测流量（和 k6 的写法相同），服务端的访问日志和链路里能按它把压测流量挑出来。
	HeaderLoadRun = "X-Load-Run"
	// MaxBody 是读响应体的上限，和网关对请求体的限制相同。
	MaxBody = 1 << 20
)

// 订单状态，见 OrderStatus。
const (
	StatusCreating  = "creating"
	StatusPending   = "pending_payment"
	StatusPaid      = "paid"
	StatusCancelled = "cancelled"
	StatusClosed    = "closed"
	StatusFailed    = "failed"
)

// 客户端要区别对待的原因码，见 TooManyRequests、ServiceUnavailable 和下单的 409。
const (
	CodeInsufficientStock = "INSUFFICIENT_STOCK"
	CodePurchaseLimit     = "PURCHASE_LIMIT_EXCEEDED"
	// CodeUnavailable 是 503 里“下游不可用”的那一类；503 的其他原因码都是降级。
	CodeUnavailable = "UNAVAILABLE"
)

// Terminal 报告订单状态是否不会再变。
func Terminal(status string) bool {
	switch status {
	case StatusPaid, StatusCancelled, StatusClosed, StatusFailed:
		return true
	default:
		return false
	}
}

// OrderPath 返回单个订单的路径。
func OrderPath(orderID string) string { return PathOrders + "/" + orderID }

// StockPath 返回查库存的路径。
func StockPath(skuID int64) string { return "/api/v1/skus/" + strconv.FormatInt(skuID, 10) + "/stock" }

// CreateOrderItem 是下单请求里的一行。64 位 ID 在 JSON 里是字符串。
type CreateOrderItem struct {
	SKUID    int64 `json:"sku_id,string"`
	Quantity int32 `json:"quantity"`
}

// CreateOrderRequest 是下单的请求体。
type CreateOrderRequest struct {
	Items []CreateOrderItem `json:"items"`
}

// Order 只解码压测需要的字段，其余字段忽略。
type Order struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

type apiError struct {
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
}

// Result 是一次 HTTP 请求的结果。Err 不为 nil 表示没有拿到完整的响应，结果未知；
// 这时 Status 可能仍有值（响应头到了、响应体没读完）。
type Result struct {
	Status  int
	Body    []byte
	Err     error
	Latency time.Duration
	End     time.Time
	// RetryAfter 是响应头 Retry-After 给的等待时间（网关只用秒数的写法），没有或写法不认识时为 0。
	RetryAfter time.Duration
}

// ErrorCode 返回错误响应里的原因码，不是错误响应或解析不了时返回空串。
func (r Result) ErrorCode() string {
	if r.Err != nil || len(r.Body) == 0 {
		return ""
	}
	var e apiError
	if json.Unmarshal(r.Body, &e) != nil {
		return ""
	}
	return e.Error.Code
}

// DecodeOrder 把响应体解码成订单。
func (r Result) DecodeOrder() (Order, error) {
	var o Order
	if err := json.Unmarshal(r.Body, &o); err != nil {
		return Order{}, fmt.Errorf("decode order (status %d): %w", r.Status, err)
	}
	return o, nil
}

// ErrString 返回 Err 的文本，没有错误时为空串。
func (r Result) ErrString() string {
	if r.Err == nil {
		return ""
	}
	return r.Err.Error()
}

// TimedOut 报告请求是否因为超时没有拿到完整的响应。
func (r Result) TimedOut() bool {
	if r.Err == nil {
		return false
	}
	if errors.Is(r.Err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(r.Err, &ne) && ne.Timeout()
}

// Client 发请求到网关。零值不可用：HTTP、Base、Timeout 必须设置。
type Client struct {
	HTTP *http.Client
	// Base 是网关地址，不带结尾的斜杠。
	Base string
	// Host 不为空时作为请求的 Host 头：从集群里按 Service 地址访问入口时，入口按 Host 选路由。
	Host string
	// LoadRun 不为空时放进 X-Load-Run 头。
	LoadRun string
	// Timeout 是单个请求的超时，包括读完响应体。
	Timeout time.Duration
}

// Do 发出一个请求。userID 为 0 时不带 X-User-Id，idemKey 为空时不带幂等键，body 为 nil 时没有请求体。
func (c *Client) Do(ctx context.Context, method, path string, userID int64, idemKey string, body []byte) Result {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, rd)
	if err != nil {
		return Result{Err: fmt.Errorf("build request: %w", err), End: time.Now()}
	}
	if userID != 0 {
		req.Header.Set(HeaderUserID, strconv.FormatInt(userID, 10))
	}
	if idemKey != "" {
		req.Header.Set(HeaderIdemKey, idemKey)
	}
	if c.LoadRun != "" {
		req.Header.Set(HeaderLoadRun, c.LoadRun)
	}
	if c.Host != "" {
		req.Host = c.Host
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		end := time.Now()
		return Result{Err: err, Latency: end.Sub(start), End: end}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, MaxBody))
	end := time.Now()
	res := Result{
		Status: resp.StatusCode, Body: b, Latency: end.Sub(start), End: end,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
	}
	if err != nil {
		res.Err = fmt.Errorf("read response body: %w", err)
	}
	return res
}

// parseRetryAfter 只认秒数的写法（网关只发这一种），HTTP 日期的写法返回 0。
func parseRetryAfter(v string) time.Duration {
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return time.Duration(min(n, 3600)) * time.Second
}
