// Package service 实现 OrderService 的业务逻辑与后台任务。
//
// 一致性的基本做法：先在本地事务里完成订单自己的状态转换，再调用库存服务；
// 库存接口都以 order_id 幂等，调用失败的由后台任务重试。
package service

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

// ReasonCreateTimeout 是订单因迟迟没能完成预占而被后台任务置为失败时的 fail_reason。
// 它只在本服务内使用，所以不在 errx 里。
const ReasonCreateTimeout = "CREATE_TIMEOUT"

// DefaultPaymentTimeout 是下单后等待支付的默认时限。
const DefaultPaymentTimeout = 15 * time.Minute

// 预占成功前写入的币种占位，与表的默认值一致，预占成功后改为库存服务给出的币种。
const placeholderCurrency = "CNY"

const maxIdempotencyKeyLen = 64

// IDGenerator 生成订单号。*idgen.Generator 满足此接口。
type IDGenerator interface {
	Next(shardKey int64) (int64, error)
}

// Config 是 Service 的可选配置，零值字段使用默认值。
type Config struct {
	// PaymentTimeout 是下单后等待支付的时限，默认 DefaultPaymentTimeout。
	PaymentTimeout time.Duration
	// Clock 返回当前时间，默认 time.Now。测试里注入以控制超时。
	Clock func() time.Time
}

// Service 实现 orderv1.OrderServiceServer。
type Service struct {
	orderv1.UnimplementedOrderServiceServer

	store          *store.Store
	inventory      inventoryv1.InventoryServiceClient
	ids            IDGenerator
	logger         *slog.Logger
	paymentTimeout time.Duration
	clock          func() time.Time
}

// New 返回 Service。
func New(st *store.Store, inventory inventoryv1.InventoryServiceClient, ids IDGenerator, logger *slog.Logger, cfg Config) *Service {
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Service{
		store:          st,
		inventory:      inventory,
		ids:            ids,
		logger:         logger,
		paymentTimeout: cmp.Or(cfg.PaymentTimeout, DefaultPaymentTimeout),
		clock:          clock,
	}
}

// now 截断到微秒：列是 DATETIME(6)，MySQL 写入时会四舍五入多余的精度，
// 不截断的话内存里的时间和库里的会差一点，时限比较可能差一个边界。
func (s *Service) now() time.Time {
	return s.clock().UTC().Truncate(time.Microsecond)
}

var errInternal = status.Error(codes.Internal, "internal error")

func invalidArgument(format string, args ...any) error {
	return errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument, format, args...)
}

func orderNotFound(orderID int64) error {
	return errx.New(codes.NotFound, errx.ReasonOrderNotFound, "order %d not found", orderID)
}

// internal 记录真实原因后返回不带细节的 Internal。
// op 说明是哪一步失败，作为日志字段而不是消息，保持消息为常量便于检索。
func (s *Service) internal(ctx context.Context, op string, err error, attrs ...any) error {
	s.logger.ErrorContext(ctx, "internal error", append(attrs, "op", op, "error", err)...)
	return errInternal
}

// ---------- CreateOrder ----------

// CreateOrder 实现 OrderServiceServer。
func (s *Service) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	items, err := validateCreate(req)
	if err != nil {
		return nil, err
	}
	hash := requestHash(items)

	orderID, err := s.ids.Next(req.GetUserId())
	if err != nil {
		return nil, s.internal(ctx, "generate order id", err)
	}
	now := s.now()
	o := &store.Order{
		OrderID:        orderID,
		UserID:         req.GetUserId(),
		Status:         orderv1.OrderStatus_ORDER_STATUS_CREATING,
		Currency:       placeholderCurrency,
		IdempotencyKey: req.GetIdempotencyKey(),
		RequestHash:    hash,
		ExpireAt:       now.Add(s.paymentTimeout),
		CreatedAt:      now,
		UpdatedAt:      now,
		Items:          items,
	}
	err = s.store.InsertOrder(ctx, o)
	switch {
	case errors.Is(err, store.ErrDuplicate):
		existing, err := s.store.GetOrderByIdempotencyKey(ctx, req.GetUserId(), req.GetIdempotencyKey())
		if err != nil {
			// 唯一键冲突却按幂等键找不到，只可能是 order_id 本身重复，属于 idgen 的问题。
			return nil, s.internal(ctx, "load order by idempotency key", err, "user_id", req.GetUserId())
		}
		if !bytes.Equal(existing.RequestHash, hash) {
			return nil, errx.New(codes.AlreadyExists, errx.ReasonIdempotencyKeyReused,
				"idempotency key was used for a different request")
		}
		o = existing
	case err != nil:
		return nil, s.internal(ctx, "insert order", err, "user_id", req.GetUserId())
	}
	return s.advance(ctx, o)
}

// validateCreate 校验请求，返回按 sku_id 排序的明细。
func validateCreate(req *orderv1.CreateOrderRequest) ([]store.Item, error) {
	if req.GetUserId() <= 0 {
		return nil, invalidArgument("user_id must be positive")
	}
	if n := len(req.GetIdempotencyKey()); n < 1 || n > maxIdempotencyKeyLen {
		return nil, invalidArgument("idempotency_key must be 1 to %d characters", maxIdempotencyKeyLen)
	}
	if len(req.GetItems()) == 0 {
		return nil, invalidArgument("items must not be empty")
	}
	items := make([]store.Item, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		if it.GetSkuId() <= 0 {
			return nil, invalidArgument("sku_id must be positive")
		}
		if it.GetQuantity() <= 0 {
			return nil, invalidArgument("quantity of sku %d must be positive", it.GetSkuId())
		}
		items = append(items, store.Item{SKUID: it.GetSkuId(), Quantity: it.GetQuantity()})
	}
	slices.SortFunc(items, func(a, b store.Item) int { return cmp.Compare(a.SKUID, b.SKUID) })
	for i := 1; i < len(items); i++ {
		if items[i].SKUID == items[i-1].SKUID {
			return nil, invalidArgument("duplicate sku_id %d", items[i].SKUID)
		}
	}
	return items, nil
}

// requestHash 对排好序的 (sku_id, quantity) 序列做 SHA-256。
// 定长大端编码，不同序列不会拼出相同的字节串。
func requestHash(sorted []store.Item) []byte {
	h := sha256.New()
	var buf [12]byte
	for _, it := range sorted {
		binary.BigEndian.PutUint64(buf[:8], uint64(it.SKUID))    //nolint:gosec // 已校验为正数
		binary.BigEndian.PutUint32(buf[8:], uint32(it.Quantity)) //nolint:gosec // 已校验为正数
		h.Write(buf[:])
	}
	return h.Sum(nil)
}

// advance 把一个已落库的订单往前推进一步，对任意状态的订单都可以安全地重复调用：
// Reserve 以 order_id 幂等，状态更新都带前置条件。
func (s *Service) advance(ctx context.Context, o *store.Order) (*orderv1.CreateOrderResponse, error) {
	if o.Status != orderv1.OrderStatus_ORDER_STATUS_CREATING {
		return answerCreate(o)
	}

	reserveItems := make([]*inventoryv1.ReserveItem, 0, len(o.Items))
	for _, it := range o.Items {
		reserveItems = append(reserveItems, &inventoryv1.ReserveItem{SkuId: it.SKUID, Quantity: it.Quantity})
	}
	resp, err := s.inventory.Reserve(ctx, &inventoryv1.ReserveRequest{OrderId: o.OrderID, Items: reserveItems})
	if err != nil {
		return s.handleReserveError(ctx, o, err)
	}

	priced, total, currency, err := priceItems(o.Items, resp.GetItems())
	if err != nil {
		// 预占已成功但结果不可用，且重试也会得到同样的结果。保持 CREATING，
		// 由后台任务按创建超时置为失败并释放库存。
		return nil, s.internal(ctx, "invalid reserve response", err, "order_id", o.OrderID)
	}
	now := s.now()
	ok, err := s.store.MarkReserved(ctx, o.OrderID, total, currency, priced, now)
	if err != nil {
		return nil, s.internal(ctx, "mark order reserved", err, "order_id", o.OrderID)
	}
	if ok {
		o.Status = orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT
		o.TotalAmount = total
		o.Currency = currency
		o.Items = priced
		o.UpdatedAt = now
		return answerCreate(o)
	}

	cur, err := s.store.GetOrder(ctx, o.OrderID)
	if err != nil {
		return nil, s.internal(ctx, "reload order", err, "order_id", o.OrderID)
	}
	if cur.Status == orderv1.OrderStatus_ORDER_STATUS_FAILED && cur.StockSettled {
		// 并发的另一次推进拿到了业务拒绝并把订单标成“失败且无需释放”，
		// 但本次的 Reserve 确实成功了，库存里有一份预占没人会释放。改回未结清交给后台任务释放。
		if _, err := s.store.MarkFailedUnsettled(ctx, cur.OrderID, s.now()); err != nil {
			s.logger.ErrorContext(ctx, "reopen settlement of failed order failed", "order_id", cur.OrderID, "error", err)
		} else {
			s.logger.WarnContext(ctx, "reserve succeeded on a failed order, scheduled release", "order_id", cur.OrderID)
		}
	}
	return answerCreate(cur)
}

func (s *Service) handleReserveError(ctx context.Context, o *store.Order, reserveErr error) (*orderv1.CreateOrderResponse, error) {
	switch reason := errx.Reason(reserveErr); reason {
	case errx.ReasonSKUNotFound, errx.ReasonInsufficientStock, errx.ReasonReservationReleased:
		// 这几种拒绝下库存没有被本订单占用，直接记为已结清。
		ok, err := s.store.MarkCreateFailed(ctx, o.OrderID, reason, true, s.now())
		if err != nil {
			return nil, s.internal(ctx, "mark order failed", err, "order_id", o.OrderID)
		}
		if ok {
			return nil, reserveErr
		}
		cur, err := s.store.GetOrder(ctx, o.OrderID)
		if err != nil {
			return nil, s.internal(ctx, "reload order", err, "order_id", o.OrderID)
		}
		return answerCreate(cur)
	case errx.ReasonReservationMismatch:
		// 每次推进发出的明细都来自同一份落库数据，不可能不一致。不自作主张修复。
		return nil, s.internal(ctx, "reservation mismatch, needs manual intervention", reserveErr, "order_id", o.OrderID)
	default:
		// 超时、不可达等：预占可能成功也可能没有。保持 CREATING，让客户端用同一幂等键重试。
		s.logger.WarnContext(ctx, "reserve outcome unknown", "order_id", o.OrderID, "error", reserveErr)
		return nil, status.Error(codes.Unavailable, "inventory unavailable, retry with the same idempotency key")
	}
}

// priceItems 按 Reserve 返回的单价计算各商品单价和总金额，检查明细一致、币种一致和溢出。
func priceItems(items []store.Item, reserved []*inventoryv1.ReservedItem) ([]store.Item, int64, string, error) {
	if len(reserved) != len(items) {
		return nil, 0, "", fmt.Errorf("reserve returned %d items, want %d", len(reserved), len(items))
	}
	bySKU := make(map[int64]*inventoryv1.ReservedItem, len(reserved))
	for _, r := range reserved {
		bySKU[r.GetSkuId()] = r
	}
	var (
		total    int64
		currency string
	)
	priced := make([]store.Item, 0, len(items))
	for _, it := range items {
		r, ok := bySKU[it.SKUID]
		if !ok || r.GetQuantity() != it.Quantity {
			return nil, 0, "", fmt.Errorf("reserve result does not match sku %d", it.SKUID)
		}
		price := r.GetUnitPrice()
		if price == nil || price.GetAmount() < 0 || len(price.GetCurrency()) != 3 {
			return nil, 0, "", fmt.Errorf("invalid unit price for sku %d", it.SKUID)
		}
		if currency == "" {
			currency = price.GetCurrency()
		} else if price.GetCurrency() != currency {
			return nil, 0, "", fmt.Errorf("mixed currencies %s and %s", currency, price.GetCurrency())
		}
		line, ok := mulAmount(price.GetAmount(), int64(it.Quantity))
		if !ok {
			return nil, 0, "", fmt.Errorf("amount overflow for sku %d", it.SKUID)
		}
		if total, ok = addAmount(total, line); !ok {
			return nil, 0, "", errors.New("total amount overflow")
		}
		priced = append(priced, store.Item{SKUID: it.SKUID, Quantity: it.Quantity, UnitPrice: price.GetAmount()})
	}
	return priced, total, currency, nil
}

// mulAmount 计算非负金额与正数量的积，溢出时返回 false。
func mulAmount(amount, quantity int64) (int64, bool) {
	if amount < 0 || quantity <= 0 {
		return 0, false
	}
	if amount > math.MaxInt64/quantity {
		return 0, false
	}
	return amount * quantity, true
}

// addAmount 计算两个非负金额的和，溢出时返回 false。
func addAmount(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}

// answerCreate 按订单当前状态给出 CreateOrder 的答复。
func answerCreate(o *store.Order) (*orderv1.CreateOrderResponse, error) {
	if o.Status == orderv1.OrderStatus_ORDER_STATUS_FAILED {
		return nil, failError(o.FailReason)
	}
	return &orderv1.CreateOrderResponse{Order: toProto(o)}, nil
}

// failError 把 fail_reason 还原成第一次失败时返回给调用方的错误类别，使重复请求得到相同的错误。
func failError(reason string) error {
	switch reason {
	case errx.ReasonSKUNotFound:
		return errx.New(codes.NotFound, reason, "sku not found")
	case errx.ReasonInsufficientStock:
		return errx.New(codes.FailedPrecondition, reason, "insufficient stock")
	case errx.ReasonReservationReleased:
		return errx.New(codes.FailedPrecondition, reason, "reservation already released")
	case ReasonCreateTimeout:
		// 同一幂等键重试也不会再成功，所以不用 UNAVAILABLE。
		return errx.New(codes.Aborted, reason, "order creation timed out, use a new idempotency key")
	default:
		return errInternal
	}
}

func toProto(o *store.Order) *orderv1.Order {
	items := make([]*orderv1.OrderItem, 0, len(o.Items))
	for _, it := range o.Items {
		items = append(items, &orderv1.OrderItem{
			SkuId:     it.SKUID,
			Quantity:  it.Quantity,
			UnitPrice: &commonv1.Money{Amount: it.UnitPrice, Currency: o.Currency},
		})
	}
	p := &orderv1.Order{
		OrderId:    o.OrderID,
		UserId:     o.UserID,
		Status:     o.Status,
		Total:      &commonv1.Money{Amount: o.TotalAmount, Currency: o.Currency},
		Items:      items,
		CreateTime: timestamppb.New(o.CreatedAt),
		ExpireTime: timestamppb.New(o.ExpireAt),
	}
	if !o.PaidAt.IsZero() {
		p.PayTime = timestamppb.New(o.PaidAt)
	}
	return p
}

// ---------- GetOrder ----------

// GetOrder 实现 OrderServiceServer。
func (s *Service) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if req.GetOrderId() <= 0 || req.GetUserId() <= 0 {
		return nil, invalidArgument("order_id and user_id must be positive")
	}
	o, err := s.loadOwned(ctx, req.GetOrderId(), req.GetUserId())
	if err != nil {
		return nil, err
	}
	return &orderv1.GetOrderResponse{Order: toProto(o)}, nil
}

// loadOwned 读属于 userID 的订单。不属于该用户时与不存在一样返回 NOT_FOUND，不泄露订单是否存在。
func (s *Service) loadOwned(ctx context.Context, orderID, userID int64) (*store.Order, error) {
	o, err := s.store.GetOrder(ctx, orderID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && o.UserID != userID) {
		return nil, orderNotFound(orderID)
	}
	if err != nil {
		return nil, s.internal(ctx, "load order", err, "order_id", orderID)
	}
	return o, nil
}

// ---------- CancelOrder ----------

// CancelOrder 实现 OrderServiceServer。
func (s *Service) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
	if req.GetOrderId() <= 0 || req.GetUserId() <= 0 {
		return nil, invalidArgument("order_id and user_id must be positive")
	}
	cancelled, err := s.store.Cancel(ctx, req.GetOrderId(), req.GetUserId(), s.now())
	if err != nil {
		return nil, s.internal(ctx, "cancel order", err, "order_id", req.GetOrderId())
	}
	if cancelled {
		s.settleBestEffort(ctx, req.GetOrderId(), orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	}
	o, err := s.loadOwned(ctx, req.GetOrderId(), req.GetUserId())
	if err != nil {
		return nil, err
	}
	if o.Status != orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
		return nil, errx.New(codes.FailedPrecondition, errx.ReasonOrderNotCancellable,
			"order %d is %s", o.OrderID, o.Status)
	}
	return &orderv1.CancelOrderResponse{Order: toProto(o)}, nil
}

// ---------- HandlePaymentResult ----------

// errRollback 让事务回滚，真正要返回的业务错误另行保存。
var errRollback = errors.New("rollback")

// HandlePaymentResult 实现 OrderServiceServer。
func (s *Service) HandlePaymentResult(ctx context.Context, req *orderv1.HandlePaymentResultRequest) (*orderv1.HandlePaymentResultResponse, error) {
	if req.GetOrderId() <= 0 || req.GetPaymentId() <= 0 {
		return nil, invalidArgument("order_id and payment_id must be positive")
	}
	if req.GetSucceeded() && (req.GetAmount() == nil || req.GetAmount().GetAmount() < 0 || req.GetAmount().GetCurrency() == "") {
		return nil, invalidArgument("amount is required for a succeeded payment")
	}

	var (
		disposition orderv1.PaymentDisposition
		paid        bool
		bizErr      error
	)
	now := s.now()
	err := s.store.InTx(ctx, func(tx *store.Tx) error {
		// 先锁订单行：同一订单的并发回调在此排队，与取消、关单的条件更新也互斥。
		o, err := tx.LockOrder(ctx, req.GetOrderId())
		if errors.Is(err, store.ErrNotFound) {
			bizErr = orderNotFound(req.GetOrderId())
			return errRollback
		}
		if err != nil {
			return err
		}

		prev, err := tx.GetPaymentResult(ctx, req.GetPaymentId())
		switch {
		case err == nil:
			if prev.OrderID != req.GetOrderId() {
				bizErr = invalidArgument("payment %d belongs to another order", req.GetPaymentId())
				return errRollback
			}
			disposition = prev.Disposition
			return nil
		case !errors.Is(err, store.ErrNotFound):
			return err
		}

		disposition = orderv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED
		if req.GetSucceeded() {
			if o.Status == orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
				if req.GetAmount().GetAmount() != o.TotalAmount || req.GetAmount().GetCurrency() != o.Currency {
					// 不记录答复：金额不符是请求方的错误，修正后的同一 payment_id 仍应能被处理。
					bizErr = errx.New(codes.FailedPrecondition, errx.ReasonAmountMismatch,
						"payment amount does not match order %d", o.OrderID)
					return errRollback
				}
				ok, err := tx.MarkPaid(ctx, o.OrderID, req.GetPaymentId(), now)
				if err != nil {
					return err
				}
				if !ok {
					// 行已被本事务锁住且状态刚读过，不应发生。
					return fmt.Errorf("mark paid affected no rows for order %d", o.OrderID)
				}
				paid = true
			} else {
				// 已取消、已关闭、创建失败、创建中，或已被另一笔支付付清：钱收了但订单不能要。
				disposition = orderv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED
			}
		}
		return tx.InsertPaymentResult(ctx, &store.PaymentResult{
			PaymentID:   req.GetPaymentId(),
			OrderID:     req.GetOrderId(),
			Succeeded:   req.GetSucceeded(),
			Amount:      req.GetAmount().GetAmount(),
			Disposition: disposition,
		}, now)
	})
	if bizErr != nil {
		return nil, bizErr
	}
	if err != nil {
		return nil, s.internal(ctx, "handle payment result", err,
			"order_id", req.GetOrderId(), "payment_id", req.GetPaymentId())
	}
	if paid {
		s.settleBestEffort(ctx, req.GetOrderId(), orderv1.OrderStatus_ORDER_STATUS_PAID)
	}
	return &orderv1.HandlePaymentResultResponse{Disposition: disposition}, nil
}

// ---------- 结清 ----------

// ErrStockInvariant 表示订单与库存的状态出现了不可能的组合，需要人工介入。
var ErrStockInvariant = errors.New("order and stock states are inconsistent")

// settle 让库存跟上订单的终态：PAID 确认预占，其他终态释放预占。成功后标记已结清。
// 重复执行无害：库存接口幂等，标记是条件更新。
func (s *Service) settle(ctx context.Context, orderID int64, st orderv1.OrderStatus) error {
	var err error
	switch st {
	case orderv1.OrderStatus_ORDER_STATUS_PAID:
		_, err = s.inventory.Confirm(ctx, &inventoryv1.ConfirmRequest{OrderId: orderID})
	case orderv1.OrderStatus_ORDER_STATUS_CANCELLED, orderv1.OrderStatus_ORDER_STATUS_CLOSED, orderv1.OrderStatus_ORDER_STATUS_FAILED:
		_, err = s.inventory.Release(ctx, &inventoryv1.ReleaseRequest{OrderId: orderID})
	default:
		return fmt.Errorf("order %d in status %s cannot be settled", orderID, st)
	}
	if err != nil {
		switch errx.Reason(err) {
		case errx.ReasonReservationReleased, errx.ReasonReservationNotFound, errx.ReasonReservationConfirmed:
			// 保持未结清，留给人工处理；后台任务会反复遇到它并持续报警。
			s.logger.ErrorContext(ctx, "stock invariant violated, needs manual intervention",
				"order_id", orderID, "status", st.String(), "error", err)
			return fmt.Errorf("settle order %d: %w: %w", orderID, ErrStockInvariant, err)
		}
		return fmt.Errorf("settle order %d: %w", orderID, err)
	}
	if _, err := s.store.MarkSettled(ctx, orderID, s.now()); err != nil {
		return fmt.Errorf("mark order %d settled: %w", orderID, err)
	}
	return nil
}

// settleBestEffort 在线路径上尽力立即结清，失败只记日志，留给后台任务。
func (s *Service) settleBestEffort(ctx context.Context, orderID int64, st orderv1.OrderStatus) {
	if err := s.settle(ctx, orderID, st); err != nil && !errors.Is(err, ErrStockInvariant) {
		s.logger.WarnContext(ctx, "settle stock failed, left to sweeper", "order_id", orderID, "error", err)
	}
}
