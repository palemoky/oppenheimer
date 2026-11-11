// Package service 实现 OrderService 的业务逻辑、事件消费与后台任务。
//
// 一致性的基本做法：订单的每个状态转换和描述它的 OrderEvent 写在同一个本地事务里（outbox），
// 由 outbox 中继发到 Kafka，库存和支付服务消费事件跟上订单状态。
// 下单时的 Reserve 仍是同步调用，它以 order_id 幂等。
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

	"github.com/palemoky/Oppenheimer/api/eventx"
	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	eventv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/event/v1"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/pkg/outbox"
	"github.com/palemoky/Oppenheimer/pkg/retry"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

// ReasonCreateTimeout 是订单因迟迟没能完成预占而被后台任务置为失败时的 fail_reason。
const ReasonCreateTimeout = errx.ReasonCreateTimeout

// DefaultPaymentTimeout 是下单后等待支付的默认时限。
const DefaultPaymentTimeout = 15 * time.Minute

// 预占成功前写入的币种占位，与表的默认值一致，预占成功后改为库存服务给出的币种。
const placeholderCurrency = "CNY"

const maxIdempotencyKeyLen = 64

// IDGenerator 生成订单号。*idgen.Generator 满足此接口。
type IDGenerator interface {
	Next(shardKey int64) (int64, error)
}

// Kicker 在事务提交后被调用，让 outbox 中继立即发送。*outbox.Relay 满足此接口。
type Kicker interface {
	Kick()
}

// Config 是 Service 的可选配置，零值字段使用默认值。
type Config struct {
	// PaymentTimeout 是下单后等待支付的时限，默认 DefaultPaymentTimeout。
	PaymentTimeout time.Duration
	// Clock 返回当前时间，默认 time.Now。测试里注入以控制超时。
	Clock func() time.Time
	// OrderEventsTopic 是 OrderEvent 写入 outbox 时的 topic（已带环境前缀），默认 eventx.OrderEvents。
	OrderEventsTopic string
	// Relay 可选。为空时事件只靠中继的兜底轮询发出，正确性不受影响，只是延迟变大。
	Relay Kicker
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
	eventsTopic    string
	relay          Kicker
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
		eventsTopic:    cmp.Or(cfg.OrderEventsTopic, eventx.OrderEvents),
		relay:          cfg.Relay,
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
	// 走到这里说明订单在 Reserve 期间被别人改了状态。若它已是 FAILED，本次成功的预占
	// 不需要在这里处理：把订单置为 FAILED 的一方在同一个事务里写了 failed 事件，
	// 库存服务消费它时会 Release 掉这份预占（或留下“已释放”标记挡住还在路上的 Reserve）。
	return answerCreate(cur)
}

func (s *Service) handleReserveError(ctx context.Context, o *store.Order, reserveErr error) (*orderv1.CreateOrderResponse, error) {
	switch reason := errx.Reason(reserveErr); reason {
	case errx.ReasonSKUNotFound, errx.ReasonInsufficientStock, errx.ReasonReservationReleased:
		// 拒绝只说明“这一次”Reserve 没有占到库存，不能推出“这个订单没有预占”：
		// 同一订单可能有另一次推进在并发进行（客户端重试），库存恰好在两次之间被释放出来，
		// 那一次的 Reserve 就会成功。所以不区分原因，一律置为 FAILED 并发 failed 事件，
		// 由库存服务 Release：它释放可能存在的预占，没有预占时留下标记挡住之后的 Reserve。
		// Release 幂等，多一次无害；这条路径上不再有对库存的同步写调用。
		ok, err := s.failCreating(ctx, store.OrderRef{OrderID: o.OrderID, UserID: o.UserID}, reason, s.now())
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
	// 不先查后改：直接做带前置条件的更新，并发的取消、支付、关单里只有一个能完成转换。
	// 订单不属于该用户时更新不到任何行，下面按不存在作答。
	ref := store.OrderRef{OrderID: req.GetOrderId(), UserID: req.GetUserId()}
	now := s.now()
	ev := newEvent(ref, now)
	ev.Body = &eventv1.OrderEvent_Cancelled{Cancelled: &eventv1.OrderCancelled{}}
	if _, err := s.transition(ctx, ev, func(ctx context.Context, tx *store.Tx) (bool, error) {
		return tx.Cancel(ctx, ref.OrderID, ref.UserID, now)
	}); err != nil {
		return nil, s.internal(ctx, "cancel order", err, "order_id", req.GetOrderId())
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

// ---------- 状态转换与事件 ----------

// txRetry 是事务遇到死锁时的重试策略。InnoDB 检测到死锁会回滚其中一个事务，整个事务重做即可。
var txRetry = retry.Policy{
	MaxAttempts: 3,
	Initial:     10 * time.Millisecond,
	Max:         200 * time.Millisecond,
	Retryable:   mysqlx.IsDeadlock,
}

// inTx 在事务里执行 fn，遇到死锁时整个重做。fn 可能执行多次，不能在外部变量里累积结果。
func (s *Service) inTx(ctx context.Context, p retry.Policy, fn func(tx *store.Tx) error) error {
	return retry.Do(ctx, p, func(ctx context.Context, _ int) error {
		return s.store.InTx(ctx, fn)
	})
}

// kick 在事务提交后通知中继立即发送。提交前调用没有意义：中继看不到未提交的行。
func (s *Service) kick() {
	if s.relay != nil {
		s.relay.Kick()
	}
}

// newEvent 构造一条 OrderEvent。occur_time 用状态变更时写入库里的同一个时间。
func newEvent(ref store.OrderRef, now time.Time) *eventv1.OrderEvent {
	return &eventv1.OrderEvent{OrderId: ref.OrderID, UserId: ref.UserID, OccurTime: timestamppb.New(now)}
}

// addEvents 把事件序列化后写入本事务的 outbox。
func (s *Service) addEvents(ctx context.Context, tx *store.Tx, evs ...*eventv1.OrderEvent) error {
	msgs := make([]outbox.Message, 0, len(evs))
	for _, ev := range evs {
		key, value, err := eventx.MarshalOrderEvent(ev)
		if err != nil {
			return err
		}
		msgs = append(msgs, outbox.Message{Topic: s.eventsTopic, Key: key, Payload: value})
	}
	if err := tx.AddOutbox(ctx, msgs...); err != nil {
		return fmt.Errorf("add order events: %w", err)
	}
	return nil
}

// transition 在一个事务里执行一次带前置条件的状态更新，并且只在它真的完成了转换（影响 1 行）时
// 写入对应的事件。这样“事件存在”与“转换由本事务完成”严格等价：
// 并发的几方里只有赢的那一方发事件，输的一方什么都不写。
func (s *Service) transition(ctx context.Context, ev *eventv1.OrderEvent,
	update func(ctx context.Context, tx *store.Tx) (bool, error),
) (bool, error) {
	var done bool
	err := s.inTx(ctx, txRetry, func(tx *store.Tx) error {
		done = false
		ok, err := update(ctx, tx)
		if err != nil || !ok {
			return err
		}
		if err := s.addEvents(ctx, tx, ev); err != nil {
			return err
		}
		done = true
		return nil
	})
	if err != nil {
		return false, err
	}
	if done {
		s.kick()
	}
	return done, nil
}

// failCreating 把 CREATING 的订单置为 FAILED 并发 failed 事件。返回是否由本次完成了转换。
func (s *Service) failCreating(ctx context.Context, ref store.OrderRef, reason string, now time.Time) (bool, error) {
	ev := newEvent(ref, now)
	ev.Body = &eventv1.OrderEvent_Failed{Failed: &eventv1.OrderFailed{Reason: reason}}
	return s.transition(ctx, ev, func(ctx context.Context, tx *store.Tx) (bool, error) {
		return tx.MarkCreateFailed(ctx, ref.OrderID, reason, now)
	})
}

// closeExpired 关闭超时未支付的订单并发 closed 事件。返回是否由本次完成了转换。
func (s *Service) closeExpired(ctx context.Context, ref store.OrderRef, now time.Time) (bool, error) {
	ev := newEvent(ref, now)
	ev.Body = &eventv1.OrderEvent_Closed{Closed: &eventv1.OrderClosed{}}
	return s.transition(ctx, ev, func(ctx context.Context, tx *store.Tx) (bool, error) {
		return tx.CloseExpired(ctx, ref.OrderID, now)
	})
}
