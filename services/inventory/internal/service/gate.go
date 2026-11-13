package service

import (
	"context"
	"errors"
	"fmt"
	"slices"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
)

// maxWarmUpSKUs 是一次 WarmUp 最多装载的 SKU 数。订单服务只为一个订单里未装载的 SKU 调用，
// 正常只有几个；设上限是为了不让一个异常请求在 gRPC 时限内做成千上万次往返。
const maxWarmUpSKUs = 100

// errGate 标记闸门（Redis）操作失败。它总是可以重试的：
// 走到闸门这一步时 MySQL 的那一步已经提交，而两边都是幂等的，重来一遍只会补上闸门。
var errGate = errors.New("stock gate unavailable")

func gateErr(err error) error { return fmt.Errorf("%w: %w", errGate, err) }

// WarmUp 实现 InventoryServiceServer：把 SKU 装载进库存闸门，见 6.5。
//
// 读 MySQL 和 Load 之间不加锁也不开事务：Load 只在未装载时写入数量，
// 所以多个实例并发预热同一个 SKU，只有第一个写入生效，之后的只更新限购。
// 未装载的 SKU 不可能有占用（Hold 要求 SKU 已装载），此刻 MySQL 的 available 就是闸门该有的数量。
func (s *Service) WarmUp(ctx context.Context, req *inventoryv1.WarmUpRequest) (*inventoryv1.WarmUpResponse, error) {
	ids := slices.Clone(req.GetSkuIds())
	if len(ids) == 0 {
		return nil, invalidArgument("sku_ids must not be empty")
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	if len(ids) > maxWarmUpSKUs {
		return nil, invalidArgument("at most %d sku_ids per call", maxWarmUpSKUs)
	}
	if ids[0] <= 0 {
		return nil, invalidArgument("sku_id must be positive")
	}

	resp := &inventoryv1.WarmUpResponse{}
	for _, id := range ids {
		stock, found, err := s.store.GetStock(ctx, id)
		if err != nil {
			return nil, s.toStatus(ctx, "warm up", err, "sku_id", id)
		}
		if !found {
			resp.Missing = append(resp.Missing, id)
			continue
		}
		loaded, err := s.gate.Load(ctx, id, stock.Available, stock.PurchaseLimit)
		if err != nil {
			return nil, s.toStatus(ctx, "warm up", gateErr(err), "sku_id", id)
		}
		if loaded {
			s.logger.InfoContext(ctx, "sku loaded into stock gate",
				"sku_id", id, "avail", stock.Available, "limit", stock.PurchaseLimit)
		}
	}
	return resp, nil
}

// confirmAndSettle 先在 MySQL 确认预占，成功（含幂等重放）后再结清闸门里的占用。
//
// 顺序是先 MySQL 后 Redis，且不在事务里访问 Redis：事务提交之后才动闸门。
// 结清只减 held，不影响放行；放在后面是为了让“MySQL 已确认、闸门未结清”成为唯一的中间状态，
// 失败时整体重试即可（Confirm 重放返回成功，Settle 对已结清的占用什么都不做）。
//
// MySQL 返回的错误原样返回（调用方据原因码判断是否进死信）；闸门的错误带 errGate。
func (s *Service) confirmAndSettle(ctx context.Context, orderID int64) error {
	if err := s.confirm(ctx, orderID); err != nil {
		return err
	}
	if _, err := s.gate.Settle(ctx, orderID); err != nil {
		return gateErr(err)
	}
	return nil
}

// releaseAndReturn 先在 MySQL 释放预占，成功（含幂等重放、空回滚）后再归还闸门里的占用。
//
// 必须先 MySQL 后 Redis：归还会让闸门变大，先还 Redis 的话，闸门会放进来一个
// MySQL 还没有库存给它的订单。空回滚（预占不存在）同样归还：占用是受理时扣的，
// 和 MySQL 有没有预占无关，订单失败了就该还。错误约定同 confirmAndSettle。
func (s *Service) releaseAndReturn(ctx context.Context, orderID int64) error {
	if err := s.release(ctx, orderID); err != nil {
		return err
	}
	if _, err := s.gate.Return(ctx, orderID); err != nil {
		return gateErr(err)
	}
	return nil
}
