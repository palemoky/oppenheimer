package service_test

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/services/payment"
	"github.com/palemoky/Oppenheimer/services/payment/internal/service"
	"github.com/palemoky/Oppenheimer/services/payment/internal/store"
	"github.com/palemoky/Oppenheimer/services/payment/internal/testkit"
)

const (
	orderID = int64(1001)
	userID  = int64(42)
	amount  = int64(12345)
)

type env struct {
	svc    *service.Service
	db     *sql.DB
	store  *store.Store
	orders *testkit.Orders
	clock  *testkit.Clock
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testdb.New(t, payment.Migrations, payment.MigrationsDir)
	ids, err := idgen.New(0)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{
		db:     db,
		store:  store.New(db),
		orders: testkit.NewOrders(),
		clock:  testkit.NewClock(time.Date(2026, 3, 1, 8, 0, 0, 123456789, time.UTC)),
	}
	e.svc = service.New(e.store, e.orders, ids, e.clock.Now, slog.New(slog.DiscardHandler))
	e.orders.PutOrder(testkit.PendingOrder(orderID, userID, amount))
	return e
}

func (e *env) count(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM payments").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func wantErr(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	if got := errx.Code(err); got != code {
		t.Fatalf("code = %v (err %v), want %v", got, err, code)
	}
	if got := errx.Reason(err); got != reason {
		t.Fatalf("reason = %q (err %v), want %q", got, err, reason)
	}
}

func TestCreatePaymentCreatesPending(t *testing.T) {
	e := newEnv(t)

	resp, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID})
	if err != nil {
		t.Fatal(err)
	}
	p := resp.GetPayment()
	if p.GetStatus() != paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING || p.GetNotified() {
		t.Errorf("status = %v notified = %v, want PENDING, not notified", p.GetStatus(), p.GetNotified())
	}
	if p.GetAmount().GetAmount() != amount || p.GetAmount().GetCurrency() != "CNY" {
		t.Errorf("amount = %v, want %d CNY from order", p.GetAmount(), amount)
	}
	if p.GetOrderId() != orderID || p.GetUserId() != userID {
		t.Errorf("payment = %v, want order %d user %d", p, orderID, userID)
	}
	if idgen.Gene(p.GetPaymentId()) != idgen.GeneOf(userID) {
		t.Errorf("payment id %d does not carry user gene", p.GetPaymentId())
	}
	wantTime := e.clock.Now().Truncate(time.Microsecond)
	if !p.GetCreateTime().AsTime().Equal(wantTime) {
		t.Errorf("create_time = %v, want %v", p.GetCreateTime().AsTime(), wantTime)
	}

	row, err := e.store.GetByOrderID(t.Context(), orderID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Outcome != paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_SUCCESS {
		t.Errorf("unspecified outcome stored as %v, want SUCCESS", row.Outcome)
	}
	if !row.CreatedAt.Equal(wantTime) {
		t.Errorf("stored created_at = %v, want %v", row.CreatedAt, wantTime)
	}
}

func TestCreatePaymentStoresFailureOutcome(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{
		OrderId: orderID, UserId: userID, Outcome: paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_FAILURE,
	})
	if err != nil {
		t.Fatal(err)
	}
	row, err := e.store.GetByOrderID(t.Context(), orderID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Outcome != paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_FAILURE {
		t.Errorf("outcome = %v, want FAILURE", row.Outcome)
	}
}

func TestCreatePaymentRepeatedReturnsSamePayment(t *testing.T) {
	e := newEnv(t)
	req := &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID}

	first, err := e.svc.CreatePayment(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	// 订单此后变化（金额、状态）不影响已受理的支付单。
	changed := testkit.PendingOrder(orderID, userID, amount+1)
	changed.Status = orderv1.OrderStatus_ORDER_STATUS_PAID
	e.orders.PutOrder(changed)
	e.clock.Advance(time.Minute)

	for range 3 {
		again, err := e.svc.CreatePayment(t.Context(), req)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(again.GetPayment(), first.GetPayment()) {
			t.Errorf("repeat = %v, want %v", again.GetPayment(), first.GetPayment())
		}
	}
	if n := e.count(t); n != 1 {
		t.Errorf("%d payments, want 1", n)
	}
	if n := e.orders.GetOrderCalls(); n != 1 {
		t.Errorf("GetOrder called %d times, want 1", n)
	}
}

func TestCreatePaymentConcurrentCreatesOne(t *testing.T) {
	e := newEnv(t)
	const n = 20

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		ids   = make([]int64, n)
	)
	for i := range n {
		wg.Go(func() {
			<-start
			resp, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID})
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = resp.GetPayment().GetPaymentId()
		})
	}
	close(start)
	wg.Wait()

	if c := e.count(t); c != 1 {
		t.Fatalf("%d payments, want 1", c)
	}
	row, err := e.store.GetByOrderID(t.Context(), orderID)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		if id != row.PaymentID {
			t.Errorf("caller %d got payment %d, want %d", i, id, row.PaymentID)
		}
	}
}

func TestCreatePaymentOrderNotFound(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID + 1, UserId: userID})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	if n := e.count(t); n != 0 {
		t.Errorf("%d payments, want 0", n)
	}
}

func TestCreatePaymentOrderOfAnotherUser(t *testing.T) {
	e := newEnv(t)
	_, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID + 1})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
}

func TestCreatePaymentExistingPaymentOfAnotherUser(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID}); err != nil {
		t.Fatal(err)
	}
	_, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID + 1})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
}

// wrongOwner 模拟有缺陷的订单服务：不按 user_id 过滤，返回别人的订单。
type wrongOwner struct{ *testkit.Orders }

func (wrongOwner) GetOrder(_ context.Context, in *orderv1.GetOrderRequest, _ ...grpc.CallOption) (*orderv1.GetOrderResponse, error) {
	return &orderv1.GetOrderResponse{Order: testkit.PendingOrder(in.GetOrderId(), in.GetUserId()+1, amount)}, nil
}

func TestCreatePaymentRejectsOrderOwnedByOthers(t *testing.T) {
	e := newEnv(t)
	ids, err := idgen.New(1)
	if err != nil {
		t.Fatal(err)
	}
	svc := service.New(e.store, wrongOwner{e.orders}, ids, e.clock.Now, slog.New(slog.DiscardHandler))

	_, err = svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	if n := e.count(t); n != 0 {
		t.Errorf("%d payments, want 0", n)
	}
}

func TestCreatePaymentOrderNotPayable(t *testing.T) {
	statuses := []orderv1.OrderStatus{
		orderv1.OrderStatus_ORDER_STATUS_CREATING,
		orderv1.OrderStatus_ORDER_STATUS_PAID,
		orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
		orderv1.OrderStatus_ORDER_STATUS_CLOSED,
		orderv1.OrderStatus_ORDER_STATUS_FAILED,
	}
	for _, s := range statuses {
		t.Run(s.String(), func(t *testing.T) {
			e := newEnv(t)
			o := testkit.PendingOrder(orderID, userID, amount)
			o.Status = s
			e.orders.PutOrder(o)

			_, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID})
			wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderNotPayable)
			if n := e.count(t); n != 0 {
				t.Errorf("%d payments, want 0", n)
			}
		})
	}
}

func TestCreatePaymentDownstreamErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantCode   codes.Code
		wantReason string
	}{
		{"business error passes through", errx.New(codes.NotFound, errx.ReasonOrderNotFound, "gone"), codes.NotFound, errx.ReasonOrderNotFound},
		{"unavailable", status.Error(codes.Unavailable, "connection refused"), codes.Internal, ""},
		{"timeout", status.Error(codes.DeadlineExceeded, "deadline exceeded"), codes.Internal, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.orders.SetGetOrderError(tt.err)
			_, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID})
			wantErr(t, err, tt.wantCode, tt.wantReason)
			if tt.wantCode == codes.Internal && status.Convert(err).Message() != "internal error" {
				t.Errorf("internal error leaks details: %v", err)
			}
			if n := e.count(t); n != 0 {
				t.Errorf("%d payments, want 0", n)
			}
		})
	}
}

func TestCreatePaymentInvalidArgument(t *testing.T) {
	e := newEnv(t)
	reqs := []*paymentv1.CreatePaymentRequest{
		{OrderId: 0, UserId: userID},
		{OrderId: orderID, UserId: 0},
		{OrderId: -1, UserId: userID},
		{OrderId: orderID, UserId: userID, Outcome: paymentv1.SimulatedOutcome(99)},
	}
	for _, req := range reqs {
		_, err := e.svc.CreatePayment(t.Context(), req)
		wantErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
	}
	if n := e.orders.GetOrderCalls(); n != 0 {
		t.Errorf("GetOrder called %d times for invalid requests", n)
	}
}

func TestGetPayment(t *testing.T) {
	e := newEnv(t)
	created, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID})
	if err != nil {
		t.Fatal(err)
	}

	got, err := e.svc.GetPayment(t.Context(), &paymentv1.GetPaymentRequest{OrderId: orderID, UserId: userID})
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.GetPayment(), created.GetPayment()) {
		t.Errorf("GetPayment = %v, want %v", got.GetPayment(), created.GetPayment())
	}

	_, err = e.svc.GetPayment(t.Context(), &paymentv1.GetPaymentRequest{OrderId: orderID, UserId: userID + 1})
	wantErr(t, err, codes.NotFound, errx.ReasonPaymentNotFound)
	_, err = e.svc.GetPayment(t.Context(), &paymentv1.GetPaymentRequest{OrderId: orderID + 1, UserId: userID})
	wantErr(t, err, codes.NotFound, errx.ReasonPaymentNotFound)
	_, err = e.svc.GetPayment(t.Context(), &paymentv1.GetPaymentRequest{OrderId: 0, UserId: userID})
	wantErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
}

func TestGetPaymentShowsRefunded(t *testing.T) {
	e := newEnv(t)
	created, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: orderID, UserId: userID})
	if err != nil {
		t.Fatal(err)
	}
	testdb.MustExec(t, e.db, "UPDATE payments SET status = ?, notified = 1 WHERE payment_id = ?",
		int32(paymentv1.PaymentStatus_PAYMENT_STATUS_REFUNDED), created.GetPayment().GetPaymentId())

	got, err := e.svc.GetPayment(t.Context(), &paymentv1.GetPaymentRequest{OrderId: orderID, UserId: userID})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetPayment().GetStatus() != paymentv1.PaymentStatus_PAYMENT_STATUS_REFUNDED || !got.GetPayment().GetNotified() {
		t.Errorf("payment = %v, want REFUNDED and notified", got.GetPayment())
	}
}
