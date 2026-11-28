// Package service 实现 inventory 的 gRPC 接口与库存业务规则。
package service

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/breaker"
	"github.com/palemoky/oppenheimer/pkg/cache"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/pkg/retry"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/services/inventory/internal/store"
)

// 事务遇到死锁时的重试参数。
//
// 按 sku_id 排序消除了库存行之间的死锁，Release 在冲突时直接加排他锁消除了锁升级死锁，
// 但还剩一种无法在应用层排除的情况：同一订单的 Reserve 插入者回滚（例如 SKU 不存在）时，
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

	// stockCache 和 stockCacheTTL 只用于 gRPC GetStock 的读路径；stockCache 为 nil 表示不缓存。
	// 写路径和所有内部决策（Reserve、Confirm、Release、对账、闸门）一律读库，不经过它。
	stockCache    *cache.Cache[*inventoryv1.GetStockResponse]
	stockCacheTTL time.Duration
}

// Option 是 New 的可选配置。
type Option func(*Service)

// WithStockCache 让 GetStock 经过缓存 c，结果（包括 SKU 不存在）缓存 ttl。
// 写库存时不失效缓存，读到的数据最多落后 ttl（再加抖动）。c 为 nil 或 ttl <= 0 时不缓存。
func WithStockCache(c *cache.Cache[*inventoryv1.GetStockResponse], ttl time.Duration) Option {
	return func(s *Service) {
		if c != nil && ttl > 0 {
			s.stockCache, s.stockCacheTTL = c, ttl
		}
	}
}

// errCorruptReservation 标记“预占的数据本身不对”（状态值不认识、已预占却没有明细）。
// 重试多少次结果都一样，处理订单事件时遇到它直接进死信（见 HandleOrderEvent）：
// 消费者对普通错误不设重试上限（ADR-0021），不标出来的话这一条消息会永远挡住它的分区。
var errCorruptReservation = errors.New("corrupt reservation")

// SwitchCache 是库存查询缓存的降级开关（设计文档 1d 的 6.9）：关掉之后 GetStock 不经过缓存，每次都读库。
// 由 NewStockCache 登记；没开缓存（STOCK_CACHE_TTL=0）时不登记。
const SwitchCache = "cache"

// NewStockCache 创建 GetStock 的查询缓存，接上降级开关和一个状态变化时记日志的熔断器（设计文档 1d 的 6.6）：
// Redis 连续出错或开关被关掉时，查询不碰 Redis、直接读库。set 为 nil 时不登记开关（测试用）。
func NewStockCache(client redis.Cmdable, prefix string, logger *slog.Logger, set *degrade.Set) (*cache.Cache[*inventoryv1.GetStockResponse], error) {
	var sw *degrade.Switch
	if set != nil {
		sw = set.Switch(SwitchCache, "stock queries skip the cache and read the database")
	}
	c, err := cache.New(cache.Config{
		Client:    client,
		Name:      "stock",
		KeyPrefix: prefix,
		Logger:    logger,
		Switch:    sw,
		Breaker: breaker.New(breaker.Config{Name: "stock-cache", OnChange: func(name string, from, to breaker.State) {
			logger.Warn("cache redis breaker changed", "breaker", name, "from", from.String(), "to", to.String())
		}}),
	}, cache.ProtoCodec[*inventoryv1.GetStockResponse]{})
	if err != nil {
		return nil, fmt.Errorf("new stock cache: %w", err)
	}
	if sw != nil {
		sw.Watch(func() degrade.Status {
			st := c.Breaker().Stats()
			return degrade.Status{
				Degraded: st.State != breaker.Closed,
				Detail:   fmt.Sprintf("redis=%s(opens=%d,rejected=%d)", st.State, st.Opens, st.Rejected),
			}
		})
	}
	return c, nil
}

// New 返回使用 st 和库存闸门 gate 的 Service。
func New(st *store.Store, gate *stockgate.Gate, logger *slog.Logger, opts ...Option) *Service {
	s := &Service{store: st, gate: gate, logger: logger, txRetry: defaultTxRetry}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Reserve 实现 InventoryServiceServer。
func (s *Service) Reserve(ctx context.Context, req *inventoryv1.ReserveRequest) (*inventoryv1.ReserveResponse, error) {
	orderID := req.GetOrderId()
	otelx.SetOrder(ctx, orderID, 0)
	want, err := validateReserve(req)
	if err != nil {
		return nil, err
	}

	var (
		result     []store.Item
		merchantID int64
		// fresh 只给指标用：这次是新的预占（不是幂等重放）。
		fresh bool
	)
	err = s.inTx(ctx, func(tx *store.Tx) error {
		result, merchantID, fresh = nil, 0, false
		// 先插入再判断，让唯一键充当互斥；并发的重复请求会等这里提交或回滚。
		inserted, err := tx.InsertReservation(ctx, orderID, store.StatusReserved)
		if err != nil {
			return err
		}
		if !inserted {
			result, merchantID, err = reserveReplay(ctx, tx, orderID, want)
			return err
		}

		// 读单价和商家，同时确认 SKU 都存在。普通的快照读，不锁 sku_stock 的行。
		skuIDs := make([]int64, len(want))
		for i, it := range want {
			skuIDs[i] = it.SKUID
		}
		skus, err := tx.SKUs(ctx, skuIDs)
		if err != nil {
			return err
		}
		items := slices.Clone(want)
		for i := range items {
			sku, ok := skus[items[i].SKUID]
			if !ok {
				// 返回错误让整个事务回滚，连同上面插入的头记录。
				return errx.New(codes.NotFound, errx.ReasonSKUNotFound, "sku %d not found", items[i].SKUID)
			}
			items[i].UnitPrice = sku.UnitPrice
		}
		// 一个订单只属于一个商家（ADR-0014）。返回错误让整个事务回滚。
		merchant, err := singleMerchant(skus)
		if err != nil {
			return err
		}
		if err := tx.SetReservationMerchant(ctx, orderID, merchant); err != nil {
			return err
		}
		if err := tx.InsertReservationItems(ctx, orderID, items); err != nil {
			return err
		}
		// 库存的变化只记增量，不改 sku_stock（ADR-0018）：买同一个 SKU 的订单不再抢同一行的锁，
		// 后台的应用者按 SKU 合并后一次更新。这里不检查可售数够不够：能不能卖由库存闸门在受理时
		// 原子地判断过了。闸门和 MySQL 对不上时，以前是这里返回库存不足，现在是应用者发现并报错。
		for _, it := range items {
			if err := tx.PendReserve(ctx, orderID, it.SKUID, it.Quantity); err != nil {
				return err
			}
		}
		result, merchantID, fresh = items, merchant, true
		return nil
	})
	if err != nil {
		if reason := errx.Reason(err); reason != "" {
			stockRejections.Add(ctx, 1, rejectionOpt(reason))
		}
		return nil, s.toStatus(ctx, "reserve", err, "order_id", orderID)
	}
	if fresh {
		var n int64
		for _, it := range result {
			n += int64(it.Quantity)
		}
		stockReserved.Add(ctx, n)
	}
	return &inventoryv1.ReserveResponse{Items: toProtoItems(result), MerchantId: merchantID}, nil
}

// singleMerchant 返回这些 SKU 共同所属的商家；它们属于不同商家时返回 MIXED_MERCHANTS。
func singleMerchant(skus map[int64]store.SKU) (int64, error) {
	var merchant int64
	first := true
	for _, sku := range skus {
		if first {
			merchant, first = sku.MerchantID, false
			continue
		}
		if sku.MerchantID != merchant {
			return 0, errx.New(codes.FailedPrecondition, errx.ReasonMixedMerchants, "skus belong to different merchants")
		}
	}
	if first {
		return 0, errors.New("no sku to take the merchant from")
	}
	return merchant, nil
}

// reserveReplay 处理订单已有预占记录的重复 Reserve：内容一致返回原结果，否则拒绝。
func reserveReplay(ctx context.Context, tx *store.Tx, orderID int64, want []store.Item) ([]store.Item, int64, error) {
	st, found, err := tx.ReservationStatusForShare(ctx, orderID)
	if err != nil {
		return nil, 0, err
	}
	if !found {
		// 刚撞上唯一键冲突，记录又不存在，只可能是被删除了，而本服务从不删除预占。
		return nil, 0, errors.New("reservation vanished after duplicate key")
	}
	if st == store.StatusReleased {
		return nil, 0, errx.New(codes.FailedPrecondition, errx.ReasonReservationReleased, "reservation of order %d has been released", orderID)
	}

	stored, err := tx.ReservationItems(ctx, orderID)
	if err != nil {
		return nil, 0, err
	}
	same := slices.EqualFunc(stored, want, func(a, b store.Item) bool {
		return a.SKUID == b.SKUID && a.Quantity == b.Quantity
	})
	if !same {
		return nil, 0, errx.New(codes.AlreadyExists, errx.ReasonReservationMismatch, "order %d already reserved with different items", orderID)
	}

	merchant, found, err := tx.ReservationMerchant(ctx, orderID)
	if err != nil {
		return nil, 0, err
	}
	if !found {
		// 头记录是加 merchant_id 这一列之前写的。SKU 所属的商家不会变，现在读出来的和当时一样。
		// 那时没有“一个订单一个商家”的检查，这里不补做：预占已经成立，拒绝它只会让订单卡住。
		skuIDs := make([]int64, len(stored))
		for i, it := range stored {
			skuIDs[i] = it.SKUID
		}
		skus, err := tx.SKUs(ctx, skuIDs)
		if err != nil {
			return nil, 0, err
		}
		if sku, ok := skus[stored[0].SKUID]; ok {
			merchant = sku.MerchantID
		}
	}
	return stored, merchant, nil
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
	otelx.SetOrder(ctx, orderID, 0)
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
	// n 只给指标用：这次确认记下的件数，事务提交成功后才计入。
	var n int64
	err := s.inTx(ctx, func(tx *store.Tx) error {
		n = 0
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
			return settle(ctx, tx, orderID, store.StatusConfirmed, countPend(&n, tx.PendConfirm))
		default:
			return fmt.Errorf("%w: unknown reservation status %v", errCorruptReservation, st)
		}
	})
	if err == nil && n > 0 {
		stockDeducted.Add(ctx, n)
	}
	return err
}

// Release 实现 InventoryServiceServer。
//
// 和 Confirm 一样走事件消费的同一条路径（释放后归还占用）。只释放 MySQL 而不归还的话，
// 恒等式两边都不变，对账发现不了，闸门会永久少放这部分数量。
func (s *Service) Release(ctx context.Context, req *inventoryv1.ReleaseRequest) (*inventoryv1.ReleaseResponse, error) {
	orderID := req.GetOrderId()
	otelx.SetOrder(ctx, orderID, 0)
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
	// n 只给指标用：这次释放记下的件数，事务提交成功后才计入。
	var n int64
	err := s.inTx(ctx, func(tx *store.Tx) error {
		n = 0
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
			return settle(ctx, tx, orderID, store.StatusReleased, countPend(&n, tx.PendRelease))
		default:
			return fmt.Errorf("%w: unknown reservation status %v", errCorruptReservation, st)
		}
	})
	if err == nil && n > 0 {
		stockReleased.Add(ctx, n)
	}
	return err
}

// settle 把 RESERVED 的预占转为 to，并对每一项记一笔库存增量（pend）。
// 调用方必须已对 reservation 记录持有排他锁。
//
// 状态的条件更新保证一个订单只会被确认或释放一次，所以每笔增量只记一次。
// 增量应用到 sku_stock 时如果预占量不够扣，说明恒等式已被破坏，由应用者发现并报错（见 store.ApplyPending）。
func settle(ctx context.Context, tx *store.Tx, orderID int64, to store.Status, pend func(ctx context.Context, orderID, skuID int64, qty int32) error) error {
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
		return fmt.Errorf("%w: reserved reservation of order %d has no items", errCorruptReservation, orderID)
	}
	for _, it := range items {
		if err := pend(ctx, orderID, it.SKUID, it.Quantity); err != nil {
			return err
		}
	}
	return nil
}

// StockCacheKey 返回 sku 的库存查询缓存 key（不含 REDIS_PREFIX）。
func StockCacheKey(skuID int64) string {
	return "cache:stock:" + strconv.FormatInt(skuID, 10)
}

// GetStock 实现 InventoryServiceServer。启用缓存时，返回的消息可能被并发调用共享，不能修改。
func (s *Service) GetStock(ctx context.Context, req *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
	skuID := req.GetSkuId()
	resp, found, err := s.stock(ctx, skuID)
	return s.stockResult(ctx, skuID, resp, found, err)
}

// lookupStock 查一个 SKU 的库存：开了缓存时经过缓存，否则读库。
func (s *Service) lookupStock(ctx context.Context, skuID int64) (*inventoryv1.GetStockResponse, bool, error) {
	if s.stockCache == nil {
		return s.loadStock(ctx, skuID)
	}
	return s.stockCache.Get(ctx, StockCacheKey(skuID), func(ctx context.Context) (cache.Loaded[*inventoryv1.GetStockResponse], error) {
		resp, found, err := s.loadStock(ctx, skuID)
		if err != nil {
			return cache.Loaded[*inventoryv1.GetStockResponse]{}, err
		}
		return cache.Loaded[*inventoryv1.GetStockResponse]{Value: resp, Found: found, TTL: s.stockCacheTTL}, nil
	})
}

// loadStock 从库里读库存，转成响应。
func (s *Service) loadStock(ctx context.Context, skuID int64) (*inventoryv1.GetStockResponse, bool, error) {
	stock, found, err := s.store.GetStock(ctx, skuID)
	if err != nil || !found {
		return nil, false, err
	}
	return &inventoryv1.GetStockResponse{
		SkuId: stock.SKUID,
		// 照实返回，超卖时 available 是负数（ADR-0018）：这个接口也给对账和排查用，
		// 在这里截断会让 available + reserved + sold 对不上入库总量。面向买家的展示由 gateway 截到 0。
		Available: stock.Available,
		Reserved:  stock.Reserved,
		Sold:      stock.Sold,
		UnitPrice: toProtoMoney(stock.UnitPrice),
	}, true, nil
}

func (s *Service) stockResult(ctx context.Context, skuID int64, resp *inventoryv1.GetStockResponse, found bool, err error) (*inventoryv1.GetStockResponse, error) {
	if err != nil {
		return nil, s.toStatus(ctx, "get stock", err, "sku_id", skuID)
	}
	if !found {
		return nil, errx.New(codes.NotFound, errx.ReasonSKUNotFound, "sku %d not found", skuID)
	}
	return resp, nil
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
// 业务错误原样返回；闸门（Redis）的错误是降级错误；其他错误在这里记录细节，对外只给出不含内部信息的 Internal。
func (s *Service) toStatus(ctx context.Context, op string, err error, attrs ...any) error {
	if reason := errx.Reason(err); reason != "" {
		otelx.SetReason(ctx, reason)
		return err
	}
	switch {
	case errors.Is(err, errGate):
		// MySQL 的那一步已经提交（或根本没开始，如 WarmUp），调用方重试即可补上。
		// 用降级错误而不是 UNAVAILABLE（理由见 errx.Degraded）：坏的是闸门的 Redis，不是 inventory。
		// 返回 UNAVAILABLE 的话，order 的边车连续 5 次就把 inventory 整个摘掉，不碰 Redis 的 Reserve 也跟着不可用。
		// 排在取消、超时之前：闸门的调用因为这个请求的时限到了而失败，多半是 Redis 慢或连不上，照样算降级。
		s.logger.WarnContext(ctx, "stock gate operation failed", append([]any{"op", op, "error", err.Error()}, attrs...)...)
		return errx.Degraded(errx.ReasonStockGateUnavailable, "stock gate unavailable, retry later")
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
