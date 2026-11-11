// Package service 实现 inventory 的 gRPC 接口与库存业务规则。
package service

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/pkg/retry"
	"github.com/palemoky/Oppenheimer/pkg/stockgate"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/store"
)

// 事务遇到死锁时的重试参数。
//
// 按 sku_id 排序消除了库存行之间的死锁，Release 在冲突时直接加排他锁消除了锁升级死锁，
// 但还剩一种无法在应用层排除的情况：同一订单的 Reserve 插入者回滚（例如库存不足）时，
// 排队等它的重复请求在记录消失后都继承了同一个间隙锁，又都要插入，彼此的插入意向锁
// 与对方的间隙锁冲突，InnoDB 会判定死锁并回滚其中一些。三个操作都幂等，整个事务重来
// 是安全的；退避加随机抖动，让重来的请求错开，不再同时撞到同一个间隙上。
//
// 抖动比例取 1（间隔在 [0, 2d) 内均匀取值），接近 1a 的全抖动：这里要的是把
// 同时撞上的几个请求尽量拉开，而不是让间隔稳定。只重试死锁，其他错误原样返回。
var defaultTxRetry = retry.Policy{
	MaxAttempts: 5,
	Initial:     2 * time.Millisecond,
	Max:         50 * time.Millisecond,
	Jitter:      1,
	Retryable:   mysqlx.IsDeadlock,
}

// Service 实现 inventoryv1.InventoryServiceServer。
type Service struct {
	inventoryv1.UnimplementedInventoryServiceServer

	store  *store.Store
	gate   *stockgate.Gate
	logger *slog.Logger
	// txRetry 的 MaxAttempts 只在测试中调为 1，用来证明某些场景根本不会死锁，而不是靠重试掩盖。
	txRetry retry.Policy
}

// New 返回使用 st 和库存闸门 gate 的 Service。
func New(st *store.Store, gate *stockgate.Gate, logger *slog.Logger) *Service {
	return &Service{store: st, gate: gate, logger: logger, txRetry: defaultTxRetry}
}

// Reserve 实现 InventoryServiceServer。
func (s *Service) Reserve(ctx context.Context, req *inventoryv1.ReserveRequest) (*inventoryv1.ReserveResponse, error) {
	orderID := req.GetOrderId()
	want, err := validateReserve(req)
	if err != nil {
		return nil, err
	}

	var result []store.Item
	err = s.inTx(ctx, func(tx *store.Tx) error {
		result = nil
		// 先插入再判断，让唯一键充当互斥；并发的重复请求会等这里提交或回滚。
		inserted, err := tx.InsertReservation(ctx, orderID, store.StatusReserved)
		if err != nil {
			return err
		}
		if !inserted {
			result, err = reserveReplay(ctx, tx, orderID, want)
			return err
		}

		// 所有事务按同一顺序锁 sku_stock 的行，避免交叉购买时死锁。
		for _, it := range want {
			ok, err := tx.ReserveStock(ctx, it.SKUID, it.Quantity)
			if err != nil {
				return err
			}
			if ok {
				continue
			}
			exists, err := tx.SKUExists(ctx, it.SKUID)
			if err != nil {
				return err
			}
			// 返回错误让整个事务回滚，连同上面插入的头记录和已扣的其他 SKU。
			if !exists {
				return errx.New(codes.NotFound, errx.ReasonSKUNotFound, "sku %d not found", it.SKUID)
			}
			return errx.New(codes.FailedPrecondition, errx.ReasonInsufficientStock, "insufficient stock for sku %d", it.SKUID)
		}

		skuIDs := make([]int64, len(want))
		for i, it := range want {
			skuIDs[i] = it.SKUID
		}
		prices, err := tx.SKUPrices(ctx, skuIDs)
		if err != nil {
			return err
		}
		items := slices.Clone(want)
		for i := range items {
			p, ok := prices[items[i].SKUID]
			if !ok {
				// 刚刚成功扣减过的行不可能消失，出现就是数据被破坏。
				return errors.New("sku price missing after reserve")
			}
			items[i].UnitPrice = p
		}
		if err := tx.InsertReservationItems(ctx, orderID, items); err != nil {
			return err
		}
		result = items
		return nil
	})
	if err != nil {
		return nil, s.toStatus(ctx, "reserve", err, "order_id", orderID)
	}
	return &inventoryv1.ReserveResponse{Items: toProtoItems(result)}, nil
}

// reserveReplay 处理订单已有预占记录的重复 Reserve：内容一致返回原结果，否则拒绝。
func reserveReplay(ctx context.Context, tx *store.Tx, orderID int64, want []store.Item) ([]store.Item, error) {
	st, found, err := tx.ReservationStatusForShare(ctx, orderID)
	if err != nil {
		return nil, err
	}
	if !found {
		// 刚撞上唯一键冲突，记录又不存在，只可能是被删除了，而本服务从不删除预占。
		return nil, errors.New("reservation vanished after duplicate key")
	}
	if st == store.StatusReleased {
		return nil, errx.New(codes.FailedPrecondition, errx.ReasonReservationReleased, "reservation of order %d has been released", orderID)
	}

	stored, err := tx.ReservationItems(ctx, orderID)
	if err != nil {
		return nil, err
	}
	same := slices.EqualFunc(stored, want, func(a, b store.Item) bool {
		return a.SKUID == b.SKUID && a.Quantity == b.Quantity
	})
	if !same {
		return nil, errx.New(codes.AlreadyExists, errx.ReasonReservationMismatch, "order %d already reserved with different items", orderID)
	}
	return stored, nil
}

// validateReserve 校验请求，返回按 sku_id 升序排列的预占项。
func validateReserve(req *inventoryv1.ReserveRequest) ([]store.Item, error) {
	if req.GetOrderId() <= 0 {
		return nil, invalidArgument("order_id must be positive")
	}
	if len(req.GetItems()) == 0 {
		return nil, invalidArgument("items must not be empty")
	}
	items := make([]store.Item, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		if it.GetQuantity() <= 0 {
			return nil, invalidArgument("quantity of sku %d must be positive", it.GetSkuId())
		}
		items = append(items, store.Item{SKUID: it.GetSkuId(), Quantity: it.GetQuantity()})
	}
	slices.SortFunc(items, func(a, b store.Item) int { return cmp.Compare(a.SKUID, b.SKUID) })
	for i := 1; i < len(items); i++ {
		if items[i].SKUID == items[i-1].SKUID {
			return nil, invalidArgument("duplicate sku %d", items[i].SKUID)
		}
	}
	return items, nil
}

// Confirm 实现 InventoryServiceServer。
//
// 和事件消费走同一条路径（确认后结清闸门里的占用）：排障时手工确认一个订单，
// 闸门也应该得到同样的结果，否则这笔占用会一直挂着，held 偏大而对账看不出来
// （Confirm 让 MySQL 一侧减少、占用不结清时 Redis 一侧不变，偏差会被对账当作
// avail 偏大扣掉，占用本身却永远不会消失）。结清以订单号幂等，之后订单的 paid 事件再来也无害。
func (s *Service) Confirm(ctx context.Context, req *inventoryv1.ConfirmRequest) (*inventoryv1.ConfirmResponse, error) {
	orderID := req.GetOrderId()
	if orderID <= 0 {
		return nil, invalidArgument("order_id must be positive")
	}
	if err := s.confirmAndSettle(ctx, orderID); err != nil {
		return nil, s.toStatus(ctx, "confirm", err, "order_id", orderID)
	}
	return &inventoryv1.ConfirmResponse{}, nil
}

// confirm 把订单的预占转为已售，以 order_id 幂等。gRPC 和事件消费共用。
// 返回业务错误（errx）或未经转换的内部错误，由调用方决定如何对外呈现。
func (s *Service) confirm(ctx context.Context, orderID int64) error {
	return s.inTx(ctx, func(tx *store.Tx) error {
		// 记录应当已经存在，加排他锁不会落到间隙上；不存在就报错，不会接着插入。
		st, found, err := tx.ReservationStatusForUpdate(ctx, orderID)
		if err != nil {
			return err
		}
		if !found {
			return errx.New(codes.NotFound, errx.ReasonReservationNotFound, "reservation of order %d not found", orderID)
		}
		switch st {
		case store.StatusConfirmed:
			return nil
		case store.StatusReleased:
			return errx.New(codes.FailedPrecondition, errx.ReasonReservationReleased, "reservation of order %d has been released", orderID)
		case store.StatusReserved:
			return settle(ctx, tx, orderID, store.StatusConfirmed, tx.ConfirmStock)
		default:
			return errors.New("unknown reservation status")
		}
	})
}

// Release 实现 InventoryServiceServer。
//
// 和 Confirm 一样走事件消费的同一条路径（释放后归还占用）。只释放 MySQL 而不归还的话，
// 恒等式两边都不变，对账发现不了，闸门会永久少放这部分数量。
func (s *Service) Release(ctx context.Context, req *inventoryv1.ReleaseRequest) (*inventoryv1.ReleaseResponse, error) {
	orderID := req.GetOrderId()
	if orderID <= 0 {
		return nil, invalidArgument("order_id must be positive")
	}
	if err := s.releaseAndReturn(ctx, orderID); err != nil {
		return nil, s.toStatus(ctx, "release", err, "order_id", orderID)
	}
	return &inventoryv1.ReleaseResponse{}, nil
}

// release 释放订单的预占，以 order_id 幂等；预占不存在时留下空回滚标记。
// gRPC 和事件消费共用，错误约定同 confirm。
func (s *Service) release(ctx context.Context, orderID int64) error {
	return s.inTx(ctx, func(tx *store.Tx) error {
		// 没有预占记录时插入成功，这一行就是“已释放”标记（空回滚），
		// 它让仍在路上的 Reserve 撞上唯一键而被拒绝，防止库存悬挂。
		// 已有记录时这一步已对它加了排他锁，见 InsertReservationOrLock。
		inserted, err := tx.InsertReservationOrLock(ctx, orderID, store.StatusReleased)
		if err != nil || inserted {
			return err
		}

		st, found, err := tx.ReservationStatusForUpdate(ctx, orderID)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("reservation vanished after duplicate key")
		}
		switch st {
		case store.StatusReleased:
			return nil
		case store.StatusConfirmed:
			return errx.New(codes.FailedPrecondition, errx.ReasonReservationConfirmed, "reservation of order %d has been confirmed", orderID)
		case store.StatusReserved:
			return settle(ctx, tx, orderID, store.StatusReleased, tx.ReleaseStock)
		default:
			return errors.New("unknown reservation status")
		}
	})
}

// settle 把 RESERVED 的预占转为 to，并对每一项按 sku_id 升序执行 move。
// 调用方必须已对 reservation 记录持有排他锁。
func settle(ctx context.Context, tx *store.Tx, orderID int64, to store.Status, move func(context.Context, int64, int32) (bool, error)) error {
	ok, err := tx.UpdateReservationStatus(ctx, orderID, store.StatusReserved, to)
	if err != nil {
		return err
	}
	if !ok {
		// 已持有排他锁且刚读到 RESERVED，条件更新不可能落空。
		return errors.New("reservation status changed under lock")
	}
	items, err := tx.ReservationItems(ctx, orderID)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return errors.New("reserved reservation has no items")
	}
	for _, it := range items {
		ok, err := move(ctx, it.SKUID, it.Quantity)
		if err != nil {
			return err
		}
		if !ok {
			// 预占量不够扣，说明 available + reserved + sold 的恒等式已被破坏，
			// 宁可整单失败报警，也不能继续写出错误的库存。
			return errors.New("stock invariant violated")
		}
	}
	return nil
}

// GetStock 实现 InventoryServiceServer。
func (s *Service) GetStock(ctx context.Context, req *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
	skuID := req.GetSkuId()
	stock, found, err := s.store.GetStock(ctx, skuID)
	if err != nil {
		return nil, s.toStatus(ctx, "get stock", err, "sku_id", skuID)
	}
	if !found {
		return nil, errx.New(codes.NotFound, errx.ReasonSKUNotFound, "sku %d not found", skuID)
	}
	return &inventoryv1.GetStockResponse{
		SkuId:     stock.SKUID,
		Available: stock.Available,
		Reserved:  stock.Reserved,
		Sold:      stock.Sold,
		UnitPrice: toProtoMoney(stock.UnitPrice),
	}, nil
}

// inTx 执行事务，遇到死锁时整体重试，原因见 defaultTxRetry。
func (s *Service) inTx(ctx context.Context, fn func(*store.Tx) error) error {
	return retry.Do(ctx, s.txRetry, func(ctx context.Context, attempt int) error {
		if attempt > 1 {
			s.logger.WarnContext(ctx, "transaction deadlocked, retrying", "attempt", attempt)
		}
		return s.store.InTx(ctx, fn)
	})
}

// toStatus 把 handler 的错误转换为返回给调用方的 gRPC 错误。
// 业务错误原样返回；其他错误在这里记录细节，对外只给出不含内部信息的 Internal。
func (s *Service) toStatus(ctx context.Context, op string, err error, attrs ...any) error {
	if errx.Reason(err) != "" {
		return err
	}
	switch {
	case errors.Is(err, errGate):
		// MySQL 的那一步已经提交（或根本没开始，如 WarmUp），调用方重试即可补上。
		s.logger.WarnContext(ctx, "stock gate operation failed", append([]any{"op", op, "error", err.Error()}, attrs...)...)
		return status.Error(codes.Unavailable, "stock gate unavailable, retry later")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}
	s.logger.ErrorContext(ctx, "inventory operation failed", append([]any{"op", op, "error", err.Error()}, attrs...)...)
	return status.Error(codes.Internal, "internal error")
}

func invalidArgument(format string, args ...any) error {
	return errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument, format, args...)
}

func toProtoItems(items []store.Item) []*inventoryv1.ReservedItem {
	out := make([]*inventoryv1.ReservedItem, len(items))
	for i, it := range items {
		out[i] = &inventoryv1.ReservedItem{
			SkuId:     it.SKUID,
			Quantity:  it.Quantity,
			UnitPrice: toProtoMoney(it.UnitPrice),
		}
	}
	return out
}

func toProtoMoney(p store.Price) *commonv1.Money {
	return &commonv1.Money{Amount: p.Amount, Currency: p.Currency}
}
