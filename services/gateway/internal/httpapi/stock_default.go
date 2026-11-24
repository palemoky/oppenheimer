//go:build !faults

package httpapi

import (
	"context"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
)

// stock 查一个 SKU 的库存。
func (s *server) stock(ctx context.Context, req *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
	return s.deps.Inventory.GetStock(ctx, req)
}
