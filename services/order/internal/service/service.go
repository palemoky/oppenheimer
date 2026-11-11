// Package service 实现 OrderService 的业务逻辑、事件消费与后台任务。
//
// 一致性的基本做法：订单的每个状态转换和描述它的 OrderEvent 写在同一个本地事务里（outbox），
// 由 outbox 中继发到 Kafka，库存和支付服务消费事件跟上订单状态。
//
// 下单分两段（设计文档 6.3）：受理（SubmitOrder）只碰 Redis 里的库存闸门和 Kafka，立即返回；
// 落库（HandleOrderSubmitted）消费 order.submit，写订单行并同步 Reserve。Reserve 以 order_id 幂等。
package service

import (
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
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/pkg/outbox"
	"github.com/palemoky/Oppenheimer/pkg/retry"
	"github.com/palemoky/Oppenheimer/pkg/stockgate"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

// ReasonCreateTimeout 是订单因迟迟没能完成预占而被后台任务置为失败时的 fail_reason。
const ReasonCreateTimeout = errx.ReasonCreateTimeout

// DefaultPaymentTimeout 是下单后等待支付的默认时限。
const DefaultPaymentTimeout = 15 * time.Minute

// DefaultIdempotencyTTL 是幂等键在 Redis 里的默认保留时长。
const DefaultIdempotencyTTL = 24 * time.Hour

// DefaultSubmitTimeout 是受理一个下单请求的时限（闸门、按需预热、发消息合计）。
// 调用方给的 deadline 更短时以调用方为准。
const DefaultSubmitTimeout = 3 * time.Second

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
	// IdempotencyTTL 是幂等键在闸门里的保留时长，默认 DefaultIdempotencyTTL。
	IdempotencyTTL time.Duration
	// SubmitTimeout 是受理一个请求的时限，默认 DefaultSubmitTimeout。
	SubmitTimeout time.Duration
	// Clock 返回当前时间，默认 time.Now。测试里注入以控制超时。
	Clock func() time.Time
	// OrderEventsTopic 是 OrderEvent 写入 outbox 时的 topic（已带环境前缀），默认 eventx.OrderEvents。
	OrderEventsTopic string
	// SubmitTopic、CloseDelayTopic 是受理时发出的两条消息的 topic（已带环境前缀），
	// 默认 eventx.OrderSubmit、eventx.OrderCloseDelay。
	SubmitTopic     string
	CloseDelayTopic string
	// Relay 可选。为空时事件只靠中继的兜底轮询发出，正确性不受影响，只是延迟变大。
	Relay Kicker
}

// Deps 是 Service 依赖的外部组件。
type Deps struct {
	Store     *store.Store
	Gate      *stockgate.Gate
	Sender    kafkax.Sender
	Inventory inventoryv1.InventoryServiceClient
	IDs       IDGenerator
	// Logger 为空时用 slog.Default()。
	Logger *slog.Logger
}

// Service 实现 orderv1.OrderServiceServer。
type Service struct {
	orderv1.UnimplementedOrderServiceServer

	store          *store.Store
	gate           *stockgate.Gate
	sender         kafkax.Sender
	inventory      inventoryv1.InventoryServiceClient
	ids            IDGenerator
	logger         *slog.Logger
	paymentTimeout time.Duration
	idemTTL        time.Duration
	submitTimeout  time.Duration
	clock          func() time.Time
	eventsTopic    string
	submitTopic    string
	closeTopic     string
	relay          Kicker
}

// New 返回 Service。
func New(d Deps, cfg Config) *Service {
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		store:          d.Store,
		gate:           d.Gate,
		sender:         d.Sender,
		inventory:      d.Inventory,
		ids:            d.IDs,
		logger:         logger,
		paymentTimeout: cmp.Or(cfg.PaymentTimeout, DefaultPaymentTimeout),
		idemTTL:        cmp.Or(cfg.IdempotencyTTL, DefaultIdempotencyTTL),
		submitTimeout:  cmp.Or(cfg.SubmitTimeout, DefaultSubmitTimeout),
		clock:          clock,
		eventsTopic:    cmp.Or(cfg.OrderEventsTopic, eventx.OrderEvents),
		submitTopic:    cmp.Or(cfg.SubmitTopic, eventx.OrderSubmit),
		closeTopic:     cmp.Or(cfg.CloseDelayTopic, eventx.OrderCloseDelay),
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

// ---------- 校验与请求哈希 ----------

// validateSubmit 校验请求，返回按 sku_id 排序的明细。
func validateSubmit(req *orderv1.SubmitOrderRequest) ([]store.Item, error) {
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
	sortItems(items)
	for i := 1; i < len(items); i++ {
		if items[i].SKUID == items[i-1].SKUID {
			return nil, invalidArgument("duplicate sku_id %d", items[i].SKUID)
		}
	}
	return items, nil
}

func sortItems(items []store.Item) {
	slices.SortFunc(items, func(a, b store.Item) int { return cmp.Compare(a.SKUID, b.SKUID) })
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

// ---------- 推进：Reserve ----------

// ErrUnrecoverable 表示订单卡在了重试也解决不了的状态（预占结果不可用、预占明细不一致），
// 需要人工介入。订单保持 CREATING，由 sweeper 按创建超时置为失败并释放库存。
// Kafka 适配层把它转成 kafkax.Permanent。
var ErrUnrecoverable = errors.New("unrecoverable")

// errReserveUnknown 表示 Reserve 的结果未知（超时、不可达），预占可能成功也可能没有。
// 订单保持 CREATING，原地重试即可：Reserve 以 order_id 幂等。
var errReserveUnknown = errors.New("reserve outcome unknown")

// advance 把一个已落库的订单往前推进一步，返回推进后的订单。对任意状态的订单都可以安全地重复调用：
// Reserve 以 order_id 幂等，状态更新都带前置条件。
//
// 被库存拒绝不是错误：订单置为 FAILED（同事务写 failed 事件）后正常返回。
// 返回错误时订单保持 CREATING：errReserveUnknown 和数据库错误可以重试，ErrUnrecoverable 不行。
func (s *Service) advance(ctx context.Context, o *store.Order) (*store.Order, error) {
	if o.Status != orderv1.OrderStatus_ORDER_STATUS_CREATING {
		return o, nil
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
		return nil, fmt.Errorf("%w: invalid reserve response for order %d: %w", ErrUnrecoverable, o.OrderID, err)
	}
	now := s.now()
	ok, err := s.store.MarkReserved(ctx, o.OrderID, total, currency, priced, now)
	if err != nil {
		return nil, fmt.Errorf("mark order %d reserved: %w", o.OrderID, err)
	}
	if ok {
		o.Status = orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT
		o.TotalAmount = total
		o.Currency = currency
		o.Items = priced
		o.UpdatedAt = now
		return o, nil
	}

	// 走到这里说明订单在 Reserve 期间被别人改了状态。若它已是 FAILED，本次成功的预占
	// 不需要在这里处理：把订单置为 FAILED 的一方在同一个事务里写了 failed 事件，
	// 库存服务消费它时会 Release 掉这份预占（或留下“已释放”标记挡住还在路上的 Reserve）。
	return s.reload(ctx, o.OrderID)
}

func (s *Service) reload(ctx context.Context, orderID int64) (*store.Order, error) {
	cur, err := s.store.GetOrder(ctx, orderID)
	if err != nil {
		return nil, fmt.Errorf("reload order %d: %w", orderID, err)
	}
	return cur, nil
}

func (s *Service) handleReserveError(ctx context.Context, o *store.Order, reserveErr error) (*store.Order, error) {
	switch reason := errx.Reason(reserveErr); reason {
	case errx.ReasonSKUNotFound, errx.ReasonInsufficientStock, errx.ReasonReservationReleased:
		// 拒绝只说明“这一次”Reserve 没有占到库存，不能推出“这个订单没有预占”：
		// 同一订单可能有另一次推进在并发进行（同一条消息被重复投递），库存恰好在两次之间被释放出来，
		// 那一次的 Reserve 就会成功。所以不区分原因，一律置为 FAILED 并发 failed 事件，
		// 由库存服务 Release：它释放可能存在的预占，没有预占时留下标记挡住之后的 Reserve，
		// 之后再归还闸门里的占用。Release 幂等，多一次无害；这条路径上没有对库存的同步写调用。
		now := s.now()
		ok, err := s.failCreating(ctx, store.OrderRef{OrderID: o.OrderID, UserID: o.UserID}, reason, now)
		if err != nil {
			return nil, fmt.Errorf("mark order %d failed: %w", o.OrderID, err)
		}
		if !ok {
			return s.reload(ctx, o.OrderID)
		}
		o.Status = orderv1.OrderStatus_ORDER_STATUS_FAILED
		o.FailReason = reason
		o.UpdatedAt = now
		return o, nil
	case errx.ReasonReservationMismatch:
		// 每次推进发出的明细都来自同一份落库数据，不可能不一致。不自作主张修复。
		return nil, fmt.Errorf("%w: reservation mismatch for order %d: %w", ErrUnrecoverable, o.OrderID, reserveErr)
	default:
		// 超时、不可达等：预占可能成功也可能没有。保持 CREATING，由调用方重试。
		return nil, fmt.Errorf("order %d: %w: %w", o.OrderID, errReserveUnknown, reserveErr)
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
	o, err := s.lookup(ctx, req.GetOrderId(), req.GetUserId())
	if err != nil {
		return nil, err
	}
	return &orderv1.GetOrderResponse{Order: o}, nil
}

// lookup 返回属于 userID 的订单的当前状态。不属于该用户时与不存在一样返回 NOT_FOUND，不泄露订单是否存在。
//
// 受理之后、落库之前，订单库里还没有这一行，这时以闸门里的占用为准，合成一个 CREATING 的订单
// （只有商品和数量）。占用从受理一直活到订单进入终态，所以“行不存在且占用也不存在”只有两种可能：
// 订单从未受理过或已被回收；或者在两次读之间订单落库、走完了终态、占用被归还。
// 后一种用再读一次订单行排除。
func (s *Service) lookup(ctx context.Context, orderID, userID int64) (*orderv1.Order, error) {
	o, err := s.loadOwned(ctx, orderID, userID)
	if err == nil || errx.Reason(err) != errx.ReasonOrderNotFound {
		return o, err
	}
	hold, ok, err := s.gate.Get(ctx, orderID)
	if err != nil {
		s.logger.WarnContext(ctx, "read hold failed", "order_id", orderID, "error", err)
		return nil, status.Error(codes.Unavailable, "stock gate unavailable, retry later")
	}
	if ok {
		if hold.UserID != userID {
			return nil, orderNotFound(orderID)
		}
		return s.creatingProto(orderID, userID, holdItems(hold.Items), hold.At), nil
	}
	return s.loadOwned(ctx, orderID, userID)
}

// loadOwned 读属于 userID 的已落库订单，转成对外的形式。
func (s *Service) loadOwned(ctx context.Context, orderID, userID int64) (*orderv1.Order, error) {
	o, err := s.store.GetOrder(ctx, orderID)
	if errors.Is(err, store.ErrNotFound) || (err == nil && o.UserID != userID) {
		return nil, orderNotFound(orderID)
	}
	if err != nil {
		return nil, s.internal(ctx, "load order", err, "order_id", orderID)
	}
	return toProto(o), nil
}

// creatingProto 构造一个还没落库的订单的对外形式：只有商品和数量，没有价格。
// expire_time 由受理时间加支付时限算出。受理时间截断到毫秒（与占用里记的精度一致），
// 所以从占用合成的值与受理时返回、写进消息的值相同。
func (s *Service) creatingProto(orderID, userID int64, items []store.Item, acceptedAt time.Time) *orderv1.Order {
	out := make([]*orderv1.OrderItem, 0, len(items))
	for _, it := range items {
		out = append(out, &orderv1.OrderItem{SkuId: it.SKUID, Quantity: it.Quantity})
	}
	return &orderv1.Order{
		OrderId:    orderID,
		UserId:     userID,
		Status:     orderv1.OrderStatus_ORDER_STATUS_CREATING,
		Items:      out,
		CreateTime: timestamppb.New(acceptedAt),
		ExpireTime: timestamppb.New(acceptedAt.Add(s.paymentTimeout)),
	}
}

func holdItems(items []stockgate.Item) []store.Item {
	out := make([]store.Item, 0, len(items))
	for _, it := range items {
		out = append(out, store.Item{SKUID: it.SKUID, Quantity: it.Quantity})
	}
	sortItems(out)
	return out
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
	// 与 GetOrder 用同一套查询：还没落库的订单在 GetOrder 里是 CREATING，
	// 这里也按 CREATING 答“不可取消”而不是“不存在”，两个接口对同一个订单的说法一致。
	o, err := s.lookup(ctx, req.GetOrderId(), req.GetUserId())
	if err != nil {
		return nil, err
	}
	if o.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
		return nil, errx.New(codes.FailedPrecondition, errx.ReasonOrderNotCancellable,
			"order %d is %s", o.GetOrderId(), o.GetStatus())
	}
	return &orderv1.CancelOrderResponse{Order: o}, nil
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
