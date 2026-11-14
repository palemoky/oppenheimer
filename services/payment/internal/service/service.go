// Package service 实现 PaymentService 的 gRPC 接口。
package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/services/payment/internal/store"
)

// IDGenerator 生成支付单号。生产环境是 *idgen.Generator。
type IDGenerator interface {
	Next(shardKey int64) (int64, error)
}

// Service 实现 paymentv1.PaymentServiceServer。
type Service struct {
	paymentv1.UnimplementedPaymentServiceServer

	store  *store.Store
	orders orderv1.OrderServiceClient
	ids    IDGenerator
	now    func() time.Time
	logger *slog.Logger
}

var _ paymentv1.PaymentServiceServer = (*Service)(nil)

// New 返回 Service。now 是时钟，返回值会被转为 UTC。
func New(st *store.Store, orders orderv1.OrderServiceClient, ids IDGenerator, now func() time.Time, logger *slog.Logger) *Service {
	return &Service{store: st, orders: orders, ids: ids, now: now, logger: logger}
}

// CreatePayment 实现 PaymentServiceServer。
func (s *Service) CreatePayment(ctx context.Context, req *paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
	otelx.SetOrder(ctx, req.GetOrderId(), req.GetUserId())
	if req.GetOrderId() <= 0 || req.GetUserId() <= 0 {
		return nil, errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument, "order_id and user_id must be positive")
	}
	outcome := req.GetOutcome()
	switch outcome {
	case paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_UNSPECIFIED:
		outcome = paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_SUCCESS
	case paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_SUCCESS, paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_FAILURE:
	default:
		return nil, errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument, "unknown outcome %d", outcome)
	}

	// 先查本地：重复请求不必再打订单服务，订单此后变成什么状态也不影响已受理的支付单。
	existing, err := s.store.GetByOrderID(ctx, req.GetOrderId())
	switch {
	case err == nil:
		return s.existingPayment(existing, req.GetUserId())
	case !errors.Is(err, store.ErrNotFound):
		return nil, s.internal(ctx, "get payment", err)
	}

	resp, err := s.orders.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: req.GetOrderId(), UserId: req.GetUserId()})
	if err != nil {
		if reason := errx.Reason(err); reason != "" {
			otelx.SetReason(ctx, reason)
			return nil, err
		}
		// 调用方自己取消或超时了：按调用方的原因作答，而不是说订单服务不可用（设计文档 1d 的 5.9）。
		if cerr := s.callerGone(ctx, "get order", err); cerr != nil {
			return nil, cerr
		}
		// 订单服务暂时不可用不是本服务的内部错误：如实告诉调用方可以稍后重试，
		// 网关才能返回 503 而不是 500。DEADLINE_EXCEEDED 走到这里说明调用方的 ctx 还有效，
		// 是这一跳自己的时限（orderHopTimeout）到了：order 慢，同样算不可用。
		// 用降级错误而不是 UNAVAILABLE（理由见 errx.Degraded）：payment 是好的，坏的是 order。
		// 返回 UNAVAILABLE 的话，gateway 的边车连续 5 次就把 payment 整个摘掉，不调 order 的查支付也跟着不可用。
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded:
			s.logger.WarnContext(ctx, "order service unavailable", "error", err)
			otelx.SetReason(ctx, errx.ReasonOrderUnavailable)
			return nil, errx.Degraded(errx.ReasonOrderUnavailable, "order service unavailable, retry later")
		default:
			return nil, s.internal(ctx, "get order", err)
		}
	}
	order := resp.GetOrder()
	// 订单服务已按 user_id 过滤；这里再核对一次，防止下游实现有误时把别人的订单当成自己的支付。
	if order.GetOrderId() != req.GetOrderId() || order.GetUserId() != req.GetUserId() {
		otelx.SetReason(ctx, errx.ReasonOrderNotFound)
		return nil, errx.New(codes.NotFound, errx.ReasonOrderNotFound, "order %d not found", req.GetOrderId())
	}
	if order.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
		otelx.SetReason(ctx, errx.ReasonOrderNotPayable)
		return nil, errx.New(codes.FailedPrecondition, errx.ReasonOrderNotPayable, "order %d is not pending payment", req.GetOrderId())
	}
	total := order.GetTotal()
	if total.GetAmount() < 0 || len(total.GetCurrency()) != 3 {
		return nil, s.internal(ctx, "validate order total", errors.New("invalid order total"))
	}

	paymentID, err := s.ids.Next(req.GetUserId())
	if err != nil {
		return nil, s.internal(ctx, "generate payment id", err)
	}
	p := store.Payment{
		PaymentID: paymentID,
		OrderID:   req.GetOrderId(),
		UserID:    req.GetUserId(),
		Status:    paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING,
		Amount:    total.GetAmount(),
		Currency:  total.GetCurrency(),
		Outcome:   outcome,
		CreatedAt: s.clock(),
	}
	err = s.store.Insert(ctx, p)
	switch {
	case err == nil:
		return &paymentv1.CreatePaymentResponse{Payment: toProto(p)}, nil
	case errors.Is(err, store.ErrDuplicate):
		// 并发请求抢先创建了支付单，以它为准。
		existing, err := s.store.GetByOrderID(ctx, req.GetOrderId())
		if err != nil {
			return nil, s.internal(ctx, "get payment after duplicate", err)
		}
		return s.existingPayment(existing, req.GetUserId())
	default:
		return nil, s.internal(ctx, "insert payment", err)
	}
}

func (s *Service) existingPayment(p store.Payment, userID int64) (*paymentv1.CreatePaymentResponse, error) {
	// 不属于该用户的支付单与订单不存在同等对待，不泄露他人订单的存在。
	if p.UserID != userID {
		return nil, errx.New(codes.NotFound, errx.ReasonOrderNotFound, "order %d not found", p.OrderID)
	}
	return &paymentv1.CreatePaymentResponse{Payment: toProto(p)}, nil
}

// GetPayment 实现 PaymentServiceServer。
func (s *Service) GetPayment(ctx context.Context, req *paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
	otelx.SetOrder(ctx, req.GetOrderId(), req.GetUserId())
	if req.GetOrderId() <= 0 || req.GetUserId() <= 0 {
		return nil, errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument, "order_id and user_id must be positive")
	}
	p, err := s.store.GetByOrderID(ctx, req.GetOrderId())
	if errors.Is(err, store.ErrNotFound) || (err == nil && p.UserID != req.GetUserId()) {
		return nil, errx.New(codes.NotFound, errx.ReasonPaymentNotFound, "payment for order %d not found", req.GetOrderId())
	}
	if err != nil {
		return nil, s.internal(ctx, "get payment", err)
	}
	return &paymentv1.GetPaymentResponse{Payment: toProto(p)}, nil
}

// clock 返回截断到微秒的 UTC 时间：DATETIME(6) 只存到微秒，截断后写入与读回的值一致。
func (s *Service) clock() time.Time {
	return s.now().UTC().Truncate(time.Microsecond)
}

// internal 记录内部错误的细节，只把不含细节的 Internal 返回给调用方。
//
// 调用方取消、调用方超时、这一跳超时都不是内部错误（设计文档 1d 的 5.9），先按 callerGone 和 hopTimeout 分出去。
// ctx 必须是请求自己的 ctx。
func (s *Service) internal(ctx context.Context, op string, err error) error {
	if cerr := s.callerGone(ctx, op, err); cerr != nil {
		return cerr
	}
	// 请求的 ctx 还有效而错误是超时：某一跳自己的时限到了（数据库驱动的读超时、下游的按跳时限），
	// 下游慢，稍后重试。
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		s.logger.WarnContext(ctx, "dependency timed out", "op", op, "error", err)
		return status.Error(codes.Unavailable, "a dependency timed out, retry later")
	}
	s.logger.ErrorContext(ctx, "internal error", "op", op, "error", err)
	return status.Error(codes.Internal, "internal error")
}

// callerGone 在请求自己的 ctx 已经结束时返回对外的错误，否则返回 nil。
//   - 被取消（调用方断开、上游放弃）：CANCELED，只记 DEBUG。调用方已经不在了，不是故障。
//   - 到了截止时间：DEADLINE_EXCEEDED，记 WARN。
//
// 先看 ctx 再看 err：ctx 已经结束时，不管下游返回的是什么错误，根因都是 ctx 结束。
func (s *Service) callerGone(ctx context.Context, op string, err error) error {
	switch ctxErr := ctx.Err(); {
	case errors.Is(ctxErr, context.Canceled):
		s.logger.DebugContext(ctx, "request canceled", "op", op, "error", err)
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(ctxErr, context.DeadlineExceeded):
		s.logger.WarnContext(ctx, "request deadline exceeded", "op", op, "error", err)
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}
	return nil
}

func toProto(p store.Payment) *paymentv1.Payment {
	return &paymentv1.Payment{
		PaymentId:  p.PaymentID,
		OrderId:    p.OrderID,
		UserId:     p.UserID,
		Status:     p.Status,
		Amount:     &commonv1.Money{Amount: p.Amount, Currency: p.Currency},
		CreateTime: timestamppb.New(p.CreatedAt),
		Notified:   p.Notified,
	}
}
