//go:build !faults

package httpapi

import (
	"context"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
)

// orderDetail 查一张订单的详情。
func (s *server) orderDetail(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	return s.deps.Order.GetOrder(ctx, req)
}
