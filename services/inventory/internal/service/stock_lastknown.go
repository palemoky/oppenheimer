//go:build faults

package service

import (
	"context"
	"sync"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/fault"
)

var keepLastKnown = fault.Switch("f03")

// lastKnown 记着每个 SKU 最近一次查到的库存。缓存的 Redis 和库一起查不到的那几秒（熔断刚打开、主库切换），
// 商品页的库存角标显示上一次的数，而不是报错让页面空一块：展示用的数旧几秒没关系，Reserve 不读这里。
var lastKnown = struct {
	mu    sync.Mutex
	bySKU map[int64]*inventoryv1.GetStockResponse
}{bySKU: map[int64]*inventoryv1.GetStockResponse{}}

// stock 查一个 SKU 的库存，给 GetStock 用。
func (s *Service) stock(ctx context.Context, skuID int64) (*inventoryv1.GetStockResponse, bool, error) {
	if !keepLastKnown.On() {
		return s.lookupStock(ctx, skuID)
	}
	return s.stockOrLastKnown(ctx, skuID)
}

// stockOrLastKnown 查库存，查到了记下来；查不到（不是调用方自己取消或到期）时退回上一次记下的数。
// SKU 不存在时删掉旧的记录，下架的商品不再显示库存。
func (s *Service) stockOrLastKnown(ctx context.Context, skuID int64) (*inventoryv1.GetStockResponse, bool, error) {
	lastKnown.mu.Lock()
	defer lastKnown.mu.Unlock()

	resp, found, err := s.lookupStock(ctx, skuID)
	switch {
	case err == nil && found:
		lastKnown.bySKU[skuID] = resp
	case err == nil:
		delete(lastKnown.bySKU, skuID)
	case ctx.Err() == nil:
		if last, ok := lastKnown.bySKU[skuID]; ok {
			return last, true, nil
		}
	}
	return resp, found, err
}
