//go:build !faults

package service

import (
	"context"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
)

// stock 查一个 SKU 的库存，给 GetStock 用。
func (s *Service) stock(ctx context.Context, skuID int64) (*inventoryv1.GetStockResponse, bool, error) {
	return s.lookupStock(ctx, skuID)
}
