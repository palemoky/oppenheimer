//go:build faults

package httpapi

import (
	"context"
	"sync"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/fault"
)

var settledReads = fault.Switch("f02")

// orderDetail 查一张订单的详情。
func (s *server) orderDetail(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if !settledReads.On() {
		return s.deps.Order.GetOrder(ctx, req)
	}
	return s.settledOrder(ctx, req)
}

// settledKey 带着用户号：订单不属于这个用户时 order 返回 NOT_FOUND，记下来的结果不能绕过这一条。
type settledKey struct {
	user, order int64
}

// settled 是已经到终态的订单的详情。终态（已支付、已取消、已关闭、创建失败）之后订单的任何字段都不会再变，
// 客户端在支付之后轮询结果、用户反复打开同一张订单的详情页，都不必每次再问一遍 order。
var settled = struct {
	mu sync.RWMutex
	m  map[settledKey]*orderv1.GetOrderResponse
}{m: make(map[settledKey]*orderv1.GetOrderResponse)}

// settledOrder 先看这张订单是不是已经查到过终态，是就直接用；不是就问 order，查到终态的记下来。
// 返回的响应会被并发的请求共用，调用方只读。
func (s *server) settledOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	key := settledKey{user: req.GetUserId(), order: req.GetOrderId()}
	settled.mu.RLock()
	resp, ok := settled.m[key]
	settled.mu.RUnlock()
	if ok {
		return resp, nil
	}

	resp, err := s.deps.Order.GetOrder(ctx, req)
	if err != nil {
		return nil, err
	}
	if isSettled(resp.GetOrder().GetStatus()) {
		settled.mu.Lock()
		settled.m[key] = resp
		settled.mu.Unlock()
	}
	return resp, nil
}

// isSettled 报告订单是不是到了终态（见 order.proto 里 OrderStatus 的注释）。
func isSettled(st orderv1.OrderStatus) bool {
	switch st {
	case orderv1.OrderStatus_ORDER_STATUS_PAID,
		orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
		orderv1.OrderStatus_ORDER_STATUS_CLOSED,
		orderv1.OrderStatus_ORDER_STATUS_FAILED:
		return true
	default:
		return false
	}
}
