package runner

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// 网关的路径和请求头，见 api/openapi/gateway.yaml。
const (
	pathOrders     = "/api/v1/orders"
	headerUserID   = "X-User-Id"
	headerIdemKey  = "Idempotency-Key"
	maxBody        = 1 << 20
	statusCreating = "creating"
	statusPending  = "pending_payment"
	statusPaid     = "paid"
	statusFailed   = "failed"
)

// 提交时属于业务拒绝的原因码。SKU_NOT_FOUND 不算：那是压测参数配错了，应当作为错误暴露出来。
var submitBusinessCodes = map[string]bool{
	"INSUFFICIENT_STOCK":      true,
	"PURCHASE_LIMIT_EXCEEDED": true,
}

// terminal 报告订单状态是否不会再变。
func terminal(status string) bool {
	switch status {
	case statusPaid, "cancelled", "closed", statusFailed:
		return true
	default:
		return false
	}
}

type createOrderItem struct {
	SKUID    int64 `json:"sku_id,string"`
	Quantity int32 `json:"quantity"`
}

type createOrderRequest struct {
	Items []createOrderItem `json:"items"`
}

// order 只解码压测需要的字段，其余字段忽略。
type order struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

type apiError struct {
	Error struct {
		Code string `json:"code"`
	} `json:"error"`
}

// result 是一次 HTTP 请求的结果。err 不为 nil 表示没有拿到完整的响应，结果未知；
// 这时 status 可能仍有值（响应头到了、响应体没读完）。
type result struct {
	status  int
	body    []byte
	err     error
	latency time.Duration
	end     time.Time
}

func (r result) errorCode() string {
	if r.err != nil || len(r.body) == 0 {
		return ""
	}
	var e apiError
	if json.Unmarshal(r.body, &e) != nil {
		return ""
	}
	return e.Error.Code
}

func (r result) decodeOrder() (order, error) {
	var o order
	if err := json.Unmarshal(r.body, &o); err != nil {
		return order{}, fmt.Errorf("decode order (status %d): %w", r.status, err)
	}
	return o, nil
}

func (r result) errString() string {
	if r.err == nil {
		return ""
	}
	return r.err.Error()
}

// newHTTPClient 按并发数配置连接池：每个虚拟用户同一时刻最多一个请求在途。
func newHTTPClient(concurrency int) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = concurrency * 2
	t.MaxIdleConnsPerHost = concurrency * 2
	return &http.Client{Transport: t}
}

// do 发出一个请求，超时由 RequestTimeout 控制。
func (r *runner) do(ctx context.Context, method, path string, userID int64, idemKey string, body []byte) result {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
	defer cancel()

	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, method, r.base+path, rd)
	if err != nil {
		return result{err: fmt.Errorf("build request: %w", err), end: time.Now()}
	}
	req.Header.Set(headerUserID, strconv.FormatInt(userID, 10))
	if idemKey != "" {
		req.Header.Set(headerIdemKey, idemKey)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.client.Do(req)
	if err != nil {
		end := time.Now()
		return result{err: err, latency: end.Sub(start), end: end}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	end := time.Now()
	res := result{status: resp.StatusCode, body: b, latency: end.Sub(start), end: end}
	if err != nil {
		res.err = fmt.Errorf("read response body: %w", err)
	}
	return res
}
