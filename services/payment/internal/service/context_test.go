package service_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/services/payment/internal/service"
	"github.com/palemoky/oppenheimer/services/payment/internal/testkit"
)

// 本文件测试调用方取消、超时时的错误映射（设计文档 1d 的 5.9）。

// levelCounter 数每个级别的日志条数。并发安全。
type levelCounter struct {
	mu     sync.Mutex
	counts map[slog.Level]int
}

func (h *levelCounter) Enabled(context.Context, slog.Level) bool { return true }

func (h *levelCounter) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.counts == nil {
		h.counts = map[slog.Level]int{}
	}
	h.counts[r.Level]++
	return nil
}

func (h *levelCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *levelCounter) WithGroup(string) slog.Handler      { return h }

func (h *levelCounter) want(t *testing.T, errorsWant, warnsWant int) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.counts[slog.LevelError] != errorsWant || h.counts[slog.LevelWarn] != warnsWant {
		t.Errorf("logs: %d ERROR, %d WARN; want %d ERROR, %d WARN",
			h.counts[slog.LevelError], h.counts[slog.LevelWarn], errorsWant, warnsWant)
	}
}

// ordersFunc 是 GetOrder 行为可定制的假订单服务，其余方法来自 testkit.Orders。
type ordersFunc struct {
	*testkit.Orders
	get func(ctx context.Context) error
}

func (o ordersFunc) GetOrder(ctx context.Context, _ *orderv1.GetOrderRequest, _ ...grpc.CallOption) (*orderv1.GetOrderResponse, error) {
	return nil, o.get(ctx)
}

func TestCreatePaymentContextErrors(t *testing.T) {
	e := newEnv(t)
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	req := &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID}
	newSvc := func(orders orderv1.OrderServiceClient) (*service.Service, *levelCounter) {
		h := &levelCounter{}
		return service.New(e.store, orders, ids, e.clock.Now, slog.New(h)), h
	}

	t.Run("caller canceled before reading the store", func(t *testing.T) {
		svc, logs := newSvc(e.orders)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := svc.CreatePayment(ctx, req)
		wantErr(t, err, codes.Canceled, "")
		_, err = svc.GetPayment(ctx, &paymentv1.GetPaymentRequest{OrderId: orderID, UserId: userID})
		wantErr(t, err, codes.Canceled, "")
		logs.want(t, 0, 0)
	})

	t.Run("caller canceled while asking order", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		svc, logs := newSvc(ordersFunc{Orders: e.orders, get: func(context.Context) error {
			cancel()
			return status.Error(codes.Canceled, "context canceled")
		}})
		_, err := svc.CreatePayment(ctx, req)
		wantErr(t, err, codes.Canceled, "")
		logs.want(t, 0, 0)
	})

	t.Run("caller deadline while asking order", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		svc, logs := newSvc(ordersFunc{Orders: e.orders, get: func(ctx context.Context) error {
			<-ctx.Done()
			return status.FromContextError(ctx.Err()).Err()
		}})
		_, err := svc.CreatePayment(ctx, req)
		wantErr(t, err, codes.DeadlineExceeded, "")
		logs.want(t, 0, 1)
	})

	t.Run("caller deadline before reading the store", func(t *testing.T) {
		svc, logs := newSvc(e.orders)
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		_, err := svc.CreatePayment(ctx, req)
		wantErr(t, err, codes.DeadlineExceeded, "")
		logs.want(t, 0, 1)
	})

	t.Run("hop timeout to order", func(t *testing.T) {
		// 调用方的 ctx 还有效，order 那一跳自己的时限到了：下游慢，答降级错误（ORDER_UNAVAILABLE）。
		svc, logs := newSvc(ordersFunc{Orders: e.orders, get: func(context.Context) error {
			return status.Error(codes.DeadlineExceeded, "context deadline exceeded")
		}})
		_, err := svc.CreatePayment(t.Context(), req)
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderUnavailable)
		logs.want(t, 0, 1)
	})

	t.Run("internal error", func(t *testing.T) {
		svc, logs := newSvc(ordersFunc{Orders: e.orders, get: func(context.Context) error {
			return status.Error(codes.Internal, "boom")
		}})
		_, err := svc.CreatePayment(t.Context(), req)
		wantErr(t, err, codes.Internal, "")
		logs.want(t, 1, 0)
	})

	if n := e.count(t); n != 0 {
		t.Errorf("%d payments, want 0", n)
	}
}
