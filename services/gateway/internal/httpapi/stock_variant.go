//go:build faults

package httpapi

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/fault"
)

var bounded = fault.Switch("f01")

// stockBudget 是商品页等库存数的最长时间：库存只是页面上的一个角标，慢了宁可不显示，也不拖住整个页面。
const stockBudget = 10 * time.Millisecond

// stock 查一个 SKU 的库存。
func (s *server) stock(ctx context.Context, req *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
	if !bounded.On() {
		return s.deps.Inventory.GetStock(ctx, req)
	}
	return s.stockWithin(ctx, req, stockBudget)
}

type stockResult struct {
	resp *inventoryv1.GetStockResponse
	err  error
}

// stockWithin 最多等 budget，到时还没有结果就先返回超时。
func (s *server) stockWithin(ctx context.Context, req *inventoryv1.GetStockRequest, budget time.Duration) (*inventoryv1.GetStockResponse, error) {
	done := make(chan stockResult)
	go func() {
		resp, err := s.deps.Inventory.GetStock(ctx, req)
		done <- stockResult{resp: resp, err: err}
	}()

	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.resp, r.err
	case <-timer.C:
		return nil, status.Error(codes.DeadlineExceeded, "stock lookup exceeded its budget")
	}
}
