// Package testkit 提供 payment 测试共用的假订单服务和可控时钟，只在测试里使用。
package testkit

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
)

// Clock 是手动推进的时钟，并发安全。
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock 返回停在 start 的时钟。
func NewClock(start time.Time) *Clock {
	return &Clock{now: start.UTC()}
}

// Now 返回当前时间。
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance 把时钟向前推进 d。
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// HandleFunc 决定一次 HandlePaymentResult 的答复。call 从 1 开始，是该 payment_id 的第几次调用。
type HandleFunc func(ctx context.Context, call int, req *orderv1.HandlePaymentResultRequest) (*orderv1.HandlePaymentResultResponse, error)

// Orders 是实现 orderv1.OrderServiceClient 的假订单服务，并发安全。
type Orders struct {
	mu        sync.Mutex
	orders    map[int64]*orderv1.Order
	getErr    error
	getCalls  int
	handle    HandleFunc
	callsByID map[int64]int
	handled   []*orderv1.HandlePaymentResultRequest
}

var _ orderv1.OrderServiceClient = (*Orders)(nil)

// NewOrders 返回没有任何订单、对回调一律答复 ACCEPTED 的假订单服务。
func NewOrders() *Orders {
	return &Orders{orders: make(map[int64]*orderv1.Order), callsByID: make(map[int64]int)}
}

// PutOrder 添加或替换一个订单。
func (o *Orders) PutOrder(order *orderv1.Order) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.orders[order.GetOrderId()] = proto.CloneOf(order)
}

// PendingOrder 构造一个待支付、金额为 amount 分的人民币订单。
func PendingOrder(orderID, userID, amount int64) *orderv1.Order {
	return &orderv1.Order{
		OrderId: orderID,
		UserId:  userID,
		Status:  orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT,
		Total:   &commonv1.Money{Amount: amount, Currency: "CNY"},
	}
}

// SetGetOrderError 让之后的 GetOrder 都返回 err；nil 恢复正常。
func (o *Orders) SetGetOrderError(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.getErr = err
}

// GetOrderCalls 返回 GetOrder 被调用的次数。
func (o *Orders) GetOrderCalls() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.getCalls
}

// OnHandlePaymentResult 设置回调的答复方式；nil 恢复为一律 ACCEPTED。
func (o *Orders) OnHandlePaymentResult(fn HandleFunc) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.handle = fn
}

// HandleCalls 返回收到的全部回调请求（副本）。
func (o *Orders) HandleCalls() []*orderv1.HandlePaymentResultRequest {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]*orderv1.HandlePaymentResultRequest, len(o.handled))
	for i, r := range o.handled {
		out[i] = proto.CloneOf(r)
	}
	return out
}

// GetOrder 实现 OrderServiceClient。订单不属于该用户时与不存在一样返回 ORDER_NOT_FOUND。
func (o *Orders) GetOrder(_ context.Context, in *orderv1.GetOrderRequest, _ ...grpc.CallOption) (*orderv1.GetOrderResponse, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.getCalls++
	if o.getErr != nil {
		return nil, o.getErr
	}
	order, ok := o.orders[in.GetOrderId()]
	if !ok || order.GetUserId() != in.GetUserId() {
		return nil, errx.New(codes.NotFound, errx.ReasonOrderNotFound, "order %d not found", in.GetOrderId())
	}
	return &orderv1.GetOrderResponse{Order: proto.CloneOf(order)}, nil
}

// HandlePaymentResult 实现 OrderServiceClient。
func (o *Orders) HandlePaymentResult(ctx context.Context, in *orderv1.HandlePaymentResultRequest, _ ...grpc.CallOption) (*orderv1.HandlePaymentResultResponse, error) {
	o.mu.Lock()
	o.callsByID[in.GetPaymentId()]++
	call := o.callsByID[in.GetPaymentId()]
	o.handled = append(o.handled, proto.CloneOf(in))
	fn := o.handle
	o.mu.Unlock()

	// 答复函数可能阻塞（模拟超时），不能持锁调用。
	if fn == nil {
		return &orderv1.HandlePaymentResultResponse{Disposition: orderv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED}, nil
	}
	return fn(ctx, call, in)
}

// CreateOrder 实现 OrderServiceClient，payment 不会调用。
func (o *Orders) CreateOrder(context.Context, *orderv1.CreateOrderRequest, ...grpc.CallOption) (*orderv1.CreateOrderResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// CancelOrder 实现 OrderServiceClient，payment 不会调用。
func (o *Orders) CancelOrder(context.Context, *orderv1.CancelOrderRequest, ...grpc.CallOption) (*orderv1.CancelOrderResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// Reply 返回固定答复的 HandleFunc。
func Reply(d orderv1.PaymentDisposition) HandleFunc {
	return func(context.Context, int, *orderv1.HandlePaymentResultRequest) (*orderv1.HandlePaymentResultResponse, error) {
		return &orderv1.HandlePaymentResultResponse{Disposition: d}, nil
	}
}

// Fail 返回固定报错的 HandleFunc。
func Fail(err error) HandleFunc {
	return func(context.Context, int, *orderv1.HandlePaymentResultRequest) (*orderv1.HandlePaymentResultResponse, error) {
		return nil, err
	}
}

// FailFirst 前 n 次调用返回 err，之后按 then 答复。
func FailFirst(n int, err error, then HandleFunc) HandleFunc {
	return func(ctx context.Context, call int, req *orderv1.HandlePaymentResultRequest) (*orderv1.HandlePaymentResultResponse, error) {
		if call <= n {
			return nil, err
		}
		return then(ctx, call, req)
	}
}

// BlockUntilDone 一直阻塞到调用方的截止时间，模拟订单服务无响应。
func BlockUntilDone() HandleFunc {
	return func(ctx context.Context, _ int, _ *orderv1.HandlePaymentResultRequest) (*orderv1.HandlePaymentResultResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}
