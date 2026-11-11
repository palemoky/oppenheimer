package processor_test

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/services/payment"
	"github.com/palemoky/Oppenheimer/services/payment/internal/processor"
	"github.com/palemoky/Oppenheimer/services/payment/internal/service"
	"github.com/palemoky/Oppenheimer/services/payment/internal/store"
	"github.com/palemoky/Oppenheimer/services/payment/internal/testkit"
)

const (
	userID = int64(42)
	amount = int64(5000)
	delay  = 100 * time.Millisecond
)

const (
	pending   = paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING
	succeeded = paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	failed    = paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED
	refunded  = paymentv1.PaymentStatus_PAYMENT_STATUS_REFUNDED
)

// refundCounter 统计 “payment refunded” 日志的条数，用来断言每笔支付只退一次。
type refundCounter struct {
	slog.Handler
	n *atomic.Int32
}

func (refundCounter) Enabled(context.Context, slog.Level) bool { return true }

func (h refundCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "payment refunded" {
		h.n.Add(1)
	}
	return nil
}

func (h refundCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h refundCounter) WithGroup(string) slog.Handler      { return h }

type env struct {
	store   *store.Store
	svc     *service.Service
	orders  *testkit.Orders
	clock   *testkit.Clock
	logger  *slog.Logger
	refunds *atomic.Int32
	cfg     processor.Config
	proc    *processor.Processor
	nextID  int64
}

func newEnv(t *testing.T, cfg processor.Config) *env {
	t.Helper()
	db := testdb.New(t, payment.Migrations, payment.MigrationsDir)
	ids, err := idgen.New(0)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{
		store:   store.New(db),
		orders:  testkit.NewOrders(),
		clock:   testkit.NewClock(time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)),
		refunds: new(atomic.Int32),
		nextID:  1000,
	}
	e.logger = slog.New(refundCounter{Handler: slog.DiscardHandler, n: e.refunds})
	e.svc = service.New(e.store, e.orders, ids, e.clock.Now, e.logger)
	cfg.Now = e.clock.Now
	cfg.ResultDelay = delay
	e.cfg = cfg
	e.proc = processor.New(e.store, e.orders, e.logger, cfg)
	return e
}

// create 新建一个待支付订单并为它发起支付，返回订单号。
func (e *env) create(t *testing.T, outcome paymentv1.SimulatedOutcome) int64 {
	t.Helper()
	e.nextID++
	e.orders.PutOrder(testkit.PendingOrder(e.nextID, userID, amount))
	_, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: e.nextID, UserId: userID, Outcome: outcome})
	if err != nil {
		t.Fatal(err)
	}
	return e.nextID
}

func (e *env) round(t *testing.T) {
	t.Helper()
	if err := e.proc.RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func (e *env) get(t *testing.T, orderID int64) store.Payment {
	t.Helper()
	p, err := e.store.GetByOrderID(t.Context(), orderID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (e *env) want(t *testing.T, orderID int64, status paymentv1.PaymentStatus, notified bool, attempts int) {
	t.Helper()
	p := e.get(t, orderID)
	if p.Status != status || p.Notified != notified || p.NotifyAttempts != attempts {
		t.Fatalf("payment = {status %v, notified %v, attempts %d}, want {%v, %v, %d}",
			p.Status, p.Notified, p.NotifyAttempts, status, notified, attempts)
	}
}

func (e *env) wantCalls(t *testing.T, n int) {
	t.Helper()
	if got := len(e.orders.HandleCalls()); got != n {
		t.Fatalf("HandlePaymentResult called %d times, want %d", got, n)
	}
}

const success = paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_SUCCESS

func TestResultAfterDelayThenNotify(t *testing.T) {
	e := newEnv(t, processor.Config{})
	orderID := e.create(t, success)

	e.clock.Advance(delay - time.Microsecond)
	e.round(t)
	e.want(t, orderID, pending, false, 0)
	e.wantCalls(t, 0)

	e.clock.Advance(time.Microsecond)
	e.round(t)
	e.want(t, orderID, succeeded, true, 0)
	e.wantCalls(t, 1)

	p := e.get(t, orderID)
	call := e.orders.HandleCalls()[0]
	if call.GetOrderId() != orderID || call.GetPaymentId() != p.PaymentID || !call.GetSucceeded() ||
		call.GetAmount().GetAmount() != amount || call.GetAmount().GetCurrency() != "CNY" {
		t.Errorf("callback = %v, want order %d payment %d succeeded amount %d CNY", call, orderID, p.PaymentID, amount)
	}

	// 已确认的不再回调。
	e.clock.Advance(time.Hour)
	e.round(t)
	e.wantCalls(t, 1)
}

func TestFailureOutcomeNotifiesFailed(t *testing.T) {
	e := newEnv(t, processor.Config{})
	orderID := e.create(t, paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_FAILURE)

	e.clock.Advance(delay)
	e.round(t)
	e.want(t, orderID, failed, true, 0)
	if call := e.orders.HandleCalls()[0]; call.GetSucceeded() {
		t.Errorf("callback succeeded = true for failed payment")
	}
}

func TestNotifyRetriesWithExponentialBackoff(t *testing.T) {
	e := newEnv(t, processor.Config{})
	e.orders.OnHandlePaymentResult(testkit.FailFirst(3, status.Error(codes.Unavailable, "order down"),
		testkit.Reply(orderv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED)))
	orderID := e.create(t, success)

	e.clock.Advance(delay)
	e.round(t)
	e.want(t, orderID, succeeded, false, 1)
	e.wantCalls(t, 1)

	// 第 n 次失败后等待 min(200ms × 2^n, 30s)。
	calls := 1
	for _, wait := range []time.Duration{400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond} {
		e.clock.Advance(wait - time.Microsecond)
		e.round(t)
		e.wantCalls(t, calls)

		e.clock.Advance(time.Microsecond)
		e.round(t)
		calls++
		e.wantCalls(t, calls)
	}
	e.want(t, orderID, succeeded, true, 3)
}

func TestNotifyBackoffCappedAt30s(t *testing.T) {
	e := newEnv(t, processor.Config{})
	e.orders.OnHandlePaymentResult(testkit.Fail(status.Error(codes.Unavailable, "order down")))
	orderID := e.create(t, success)
	e.clock.Advance(delay)

	for attempts := 1; attempts <= 10; attempts++ {
		e.round(t)
		e.want(t, orderID, succeeded, false, attempts)
		e.clock.Advance(min(200*time.Millisecond<<attempts, 30*time.Second))
	}
	// 第 10 次失败后应等满 30 秒，而不是 200ms × 2^10。
	e.wantCalls(t, 10)
	e.round(t)
	e.wantCalls(t, 11)
}

func TestRefundRequiredRefundsOnce(t *testing.T) {
	e := newEnv(t, processor.Config{})
	e.orders.OnHandlePaymentResult(testkit.Reply(orderv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED))
	orderID := e.create(t, success)

	e.clock.Advance(delay)
	e.round(t)
	e.want(t, orderID, refunded, true, 0)

	for range 3 {
		e.clock.Advance(time.Minute)
		e.round(t)
	}
	e.wantCalls(t, 1)
	if n := e.refunds.Load(); n != 1 {
		t.Errorf("%d refunds, want 1", n)
	}
}

func TestRejectedPaymentIsRefunded(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"amount mismatch", errx.New(codes.FailedPrecondition, errx.ReasonAmountMismatch, "amount mismatch")},
		{"order not found", errx.New(codes.NotFound, errx.ReasonOrderNotFound, "order not found")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, processor.Config{})
			e.orders.OnHandlePaymentResult(testkit.Fail(tt.err))
			okOrder := e.create(t, success)
			failedOrder := e.create(t, paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_FAILURE)

			e.clock.Advance(delay)
			e.round(t)
			// 不可重试：不递增 attempts，直接结束回调。失败的支付无钱可退，保持 FAILED。
			e.want(t, okOrder, refunded, true, 0)
			e.want(t, failedOrder, failed, true, 0)

			e.clock.Advance(time.Minute)
			e.round(t)
			e.wantCalls(t, 2)
			if n := e.refunds.Load(); n != 1 {
				t.Errorf("%d refunds, want 1", n)
			}
		})
	}
}

func TestConcurrentProcessorsRefundEachPaymentOnce(t *testing.T) {
	e := newEnv(t, processor.Config{})
	e.orders.OnHandlePaymentResult(testkit.Reply(orderv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED))
	const n = 20
	orders := make([]int64, n)
	for i := range n {
		orders[i] = e.create(t, success)
	}
	e.clock.Advance(delay)

	// 模拟多个实例同时运行：同一笔支付可能被回调多次，但退款只能发生一次。
	procs := make([]*processor.Processor, 4)
	for i := range procs {
		procs[i] = processor.New(e.store, e.orders, e.logger, e.cfg)
	}
	var wg sync.WaitGroup
	for _, p := range procs {
		wg.Go(func() {
			for range 3 {
				if err := p.RunOnce(t.Context()); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()

	for _, id := range orders {
		e.want(t, id, refunded, true, 0)
	}
	if got := e.refunds.Load(); got != n {
		t.Errorf("%d refunds, want %d", got, n)
	}
}

func TestNotifyAppliedButTimedOutIsResent(t *testing.T) {
	e := newEnv(t, processor.Config{})
	// 订单服务处理成功了，但响应在路上丢了；之后的重发由它按 payment_id 幂等答复。
	var (
		mu      sync.Mutex
		applied = map[int64]int{}
	)
	e.orders.OnHandlePaymentResult(func(_ context.Context, call int, req *orderv1.HandlePaymentResultRequest) (*orderv1.HandlePaymentResultResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		if applied[req.GetPaymentId()] == 0 {
			applied[req.GetPaymentId()]++
		}
		if call == 1 {
			return nil, status.Error(codes.DeadlineExceeded, "context deadline exceeded")
		}
		return &orderv1.HandlePaymentResultResponse{Disposition: orderv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED}, nil
	})
	orderID := e.create(t, success)

	e.clock.Advance(delay)
	e.round(t)
	e.want(t, orderID, succeeded, false, 1)

	e.clock.Advance(400 * time.Millisecond)
	e.round(t)
	e.want(t, orderID, succeeded, true, 1)
	e.wantCalls(t, 2)

	p := e.get(t, orderID)
	mu.Lock()
	defer mu.Unlock()
	if applied[p.PaymentID] != 1 {
		t.Errorf("order applied payment %d times, want 1", applied[p.PaymentID])
	}
	for _, c := range e.orders.HandleCalls() {
		if c.GetPaymentId() != p.PaymentID || !c.GetSucceeded() || c.GetAmount().GetAmount() != amount {
			t.Errorf("resent callback differs: %v", c)
		}
	}
}

func TestNotifyHangingCallTimesOut(t *testing.T) {
	// 订单服务卡住不回：单次回调按 CallTimeout 放弃，记一次失败，不拖住这一轮。
	e := newEnv(t, processor.Config{CallTimeout: 20 * time.Millisecond})
	e.orders.OnHandlePaymentResult(testkit.BlockUntilDone())
	orderID := e.create(t, success)

	e.clock.Advance(delay)
	e.round(t)
	e.want(t, orderID, succeeded, false, 1)

	e.orders.OnHandlePaymentResult(nil)
	e.clock.Advance(400 * time.Millisecond)
	e.round(t)
	e.want(t, orderID, succeeded, true, 1)
}

func TestUnknownDispositionIsRetried(t *testing.T) {
	e := newEnv(t, processor.Config{})
	e.orders.OnHandlePaymentResult(testkit.Reply(orderv1.PaymentDisposition_PAYMENT_DISPOSITION_UNSPECIFIED))
	orderID := e.create(t, success)

	e.clock.Advance(delay)
	e.round(t)
	e.want(t, orderID, succeeded, false, 1)
}

func TestBatchSizeLimitsEachRound(t *testing.T) {
	e := newEnv(t, processor.Config{BatchSize: 2})
	orders := []int64{e.create(t, success), e.create(t, success), e.create(t, success)}
	e.clock.Advance(delay)

	e.round(t)
	e.wantCalls(t, 2)
	e.want(t, orders[2], pending, false, 0)

	e.round(t)
	e.wantCalls(t, 3)
	for _, id := range orders {
		e.want(t, id, succeeded, true, 0)
	}
}
