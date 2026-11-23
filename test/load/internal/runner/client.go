package runner

import (
	"context"
	"net/http"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/gwapi"
)

// 网关的路径和状态值，定义在 gwapi，见 api/openapi/gateway.yaml。
const (
	pathOrders     = gwapi.PathOrders
	statusCreating = gwapi.StatusCreating
	statusPending  = gwapi.StatusPending
	statusPaid     = gwapi.StatusPaid
	statusFailed   = gwapi.StatusFailed
)

// 提交时属于业务拒绝的原因码。SKU_NOT_FOUND 不算：那是压测参数配错了，应当作为错误暴露出来。
var submitBusinessCodes = map[string]bool{
	gwapi.CodeInsufficientStock: true,
	gwapi.CodePurchaseLimit:     true,
}

// terminal 报告订单状态是否不会再变。
func terminal(status string) bool { return gwapi.Terminal(status) }

type (
	createOrderItem    = gwapi.CreateOrderItem
	createOrderRequest = gwapi.CreateOrderRequest
	order              = gwapi.Order
)

// result 是一次 HTTP 请求的结果。err 不为 nil 表示没有拿到完整的响应，结果未知；
// 这时 status 可能仍有值（响应头到了、响应体没读完）。
type result struct {
	status  int
	body    []byte
	err     error
	latency time.Duration
	end     time.Time
}

// gw 把结果换成 gwapi 的形状，解析交给它，保证 run 和 probe 对响应的理解一致。
func (r result) gw() gwapi.Result {
	return gwapi.Result{Status: r.status, Body: r.body, Err: r.err, Latency: r.latency, End: r.end}
}

func (r result) errorCode() string { return r.gw().ErrorCode() }

func (r result) decodeOrder() (order, error) { return r.gw().DecodeOrder() }

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
	c := gwapi.Client{HTTP: r.client, Base: r.base, Timeout: r.cfg.RequestTimeout}
	res := c.Do(ctx, method, path, userID, idemKey, body)
	return result{status: res.Status, body: res.Body, err: res.Err, latency: res.Latency, end: res.End}
}
