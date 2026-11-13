package consumer_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/payment"
	"github.com/palemoky/oppenheimer/services/payment/internal/consumer"
	"github.com/palemoky/oppenheimer/services/payment/internal/store"
	"github.com/palemoky/oppenheimer/services/payment/internal/testkit"
)

const (
	pending   = paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING
	succeeded = paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	failed    = paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED
	refunded  = paymentv1.PaymentStatus_PAYMENT_STATUS_REFUNDED

	accepted       = eventv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED
	refundRequired = eventv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED
)

var t0 = time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)

// logs 收集日志记录，用来断言副作用（退款记录、error 日志）的次数。
type logs struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (l *logs) Enabled(context.Context, slog.Level) bool { return true }
func (l *logs) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recs = append(l.recs, r.Clone())
	return nil
}
func (l *logs) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logs) WithGroup(string) slog.Handler      { return l }

// count 返回 message 为 msg 的日志条数。
func (l *logs) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, r := range l.recs {
		if r.Message == msg {
			n++
		}
	}
	return n
}

func (l *logs) errors() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, r := range l.recs {
		if r.Level >= slog.LevelError {
			n++
		}
	}
	return n
}

type env struct {
	db    *sql.DB
	store *store.Store
	clock *testkit.Clock
	logs  *logs
	h     *consumer.Handler
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testdb.New(t, payment.Migrations, payment.MigrationsDir)
	e := &env{db: db, store: store.New(db), clock: testkit.NewClock(t0), logs: &logs{}}
	e.h = consumer.New(e.store, slog.New(e.logs), e.clock.Now)
	return e
}

func (e *env) insert(t *testing.T, id int64, status paymentv1.PaymentStatus) store.Payment {
	t.Helper()
	p := store.Payment{
		PaymentID: id,
		OrderID:   id * 10,
		UserID:    7,
		Status:    status,
		Amount:    1999,
		Currency:  "CNY",
		Outcome:   paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_SUCCESS,
		CreatedAt: t0,
	}
	if err := e.store.Insert(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	return p
}

func (e *env) get(t *testing.T, id int64) store.Payment {
	t.Helper()
	p, err := e.store.GetByID(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (e *env) want(t *testing.T, id int64, status paymentv1.PaymentStatus, notified bool) {
	t.Helper()
	p := e.get(t, id)
	if p.Status != status || p.Notified != notified {
		t.Fatalf("payment %d = {%v, notified %v}, want {%v, %v}", id, p.Status, p.Notified, status, notified)
	}
}

func reply(p store.Payment, d eventv1.PaymentDisposition, reason string) *eventv1.OrderEvent {
	return &eventv1.OrderEvent{
		OrderId: p.OrderID,
		UserId:  p.UserID,
		Body: &eventv1.OrderEvent_PaymentResultHandled{PaymentResultHandled: &eventv1.PaymentResultHandled{
			PaymentId:   p.PaymentID,
			Disposition: d,
			Reason:      reason,
		}},
	}
}

func (e *env) handle(t *testing.T, ev *eventv1.OrderEvent) {
	t.Helper()
	if err := e.h.HandleOrderEvent(t.Context(), ev); err != nil {
		t.Fatalf("HandleOrderEvent = %v", err)
	}
}

// handleTwice 处理同一事件两次，断言第二次不改变任何东西：行（含 updated_at）和日志都与一次相同。
func (e *env) handleTwice(t *testing.T, ev *eventv1.OrderEvent) {
	t.Helper()
	id := ev.GetPaymentResultHandled().GetPaymentId()
	e.handle(t, ev)
	once := e.get(t, id)
	e.logs.mu.Lock()
	nlogs := len(e.logs.recs)
	e.logs.mu.Unlock()

	// 推进时钟：第二次若重写了行，updated_at 会变。
	e.clock.Advance(time.Minute)
	e.handle(t, ev)
	if twice := e.get(t, id); twice != once {
		t.Errorf("second delivery changed the payment:\n once %+v\ntwice %+v", once, twice)
	}
	e.logs.mu.Lock()
	defer e.logs.mu.Unlock()
	if len(e.logs.recs) != nlogs {
		t.Errorf("second delivery logged %d more records", len(e.logs.recs)-nlogs)
	}
}

func TestAcceptedMarksNotifiedOnce(t *testing.T) {
	for _, status := range []paymentv1.PaymentStatus{succeeded, failed} {
		t.Run(status.String(), func(t *testing.T) {
			e := newEnv(t)
			p := e.insert(t, 1, status)
			e.clock.Advance(time.Second)
			e.handleTwice(t, reply(p, accepted, ""))
			e.want(t, p.PaymentID, status, true)
			if got := e.get(t, p.PaymentID); !got.UpdatedAt.Equal(t0.Add(time.Second)) {
				t.Errorf("updated_at = %v, want %v", got.UpdatedAt, t0.Add(time.Second))
			}
			if n := e.logs.errors(); n != 0 {
				t.Errorf("%d error logs, want 0", n)
			}
		})
	}
}

func TestRefundRequiredRefundsSucceededOnce(t *testing.T) {
	e := newEnv(t)
	p := e.insert(t, 1, succeeded)
	e.handleTwice(t, reply(p, refundRequired, "ORDER_NOT_PAYABLE"))
	e.want(t, p.PaymentID, refunded, true)
	if n := e.logs.count("payment refunded"); n != 1 {
		t.Errorf("%d refunds, want 1", n)
	}
	// ORDER_NOT_PAYABLE 是正常的业务情况（例如订单已超时关闭），不是缺陷。
	if n := e.logs.errors(); n != 0 {
		t.Errorf("%d error logs, want 0", n)
	}
}

func TestRefundRequiredForFailedPaymentOnlyMarks(t *testing.T) {
	e := newEnv(t)
	p := e.insert(t, 1, failed)
	e.handleTwice(t, reply(p, refundRequired, "ORDER_NOT_PAYABLE"))
	// 失败的支付没有钱可退，状态保持 FAILED。
	e.want(t, p.PaymentID, failed, true)
	if n := e.logs.count("payment refunded"); n != 0 {
		t.Errorf("%d refunds for failed payment, want 0", n)
	}
}

func TestRejectionReasonsLogError(t *testing.T) {
	for _, reason := range []string{errx.ReasonAmountMismatch, errx.ReasonOrderNotFound} {
		t.Run(reason, func(t *testing.T) {
			e := newEnv(t)
			ok := e.insert(t, 1, succeeded)
			bad := e.insert(t, 2, failed)
			e.handleTwice(t, reply(ok, refundRequired, reason))
			e.handleTwice(t, reply(bad, refundRequired, reason))
			e.want(t, ok.PaymentID, refunded, true)
			e.want(t, bad.PaymentID, failed, true)
			// 每笔支付记一次，重复投递不重复记。
			if n := e.logs.count("payment rejected by order service"); n != 2 {
				t.Errorf("%d rejection error logs, want 2", n)
			}
			if n := e.logs.count("payment refunded"); n != 1 {
				t.Errorf("%d refunds, want 1", n)
			}
		})
	}
}

func TestMissingPaymentIsPermanent(t *testing.T) {
	e := newEnv(t)
	ghost := store.Payment{PaymentID: 404, OrderID: 4040, UserID: 7}
	for _, d := range []eventv1.PaymentDisposition{accepted, refundRequired} {
		err := e.h.HandleOrderEvent(t.Context(), reply(ghost, d, ""))
		if !kafkax.IsPermanent(err) || !errors.Is(err, store.ErrNotFound) {
			t.Errorf("%v for missing payment = %v, want permanent ErrNotFound", d, err)
		}
	}
}

func TestInvalidRepliesArePermanent(t *testing.T) {
	e := newEnv(t)
	p := e.insert(t, 1, succeeded)

	wrongOrder := reply(p, accepted, "")
	wrongOrder.OrderId = p.OrderID + 1
	noPayment := reply(p, accepted, "")
	noPayment.GetPaymentResultHandled().PaymentId = 0
	unknown := reply(p, eventv1.PaymentDisposition_PAYMENT_DISPOSITION_UNSPECIFIED, "")

	for name, ev := range map[string]*eventv1.OrderEvent{
		"wrong order": wrongOrder, "no payment id": noPayment, "unknown disposition": unknown,
	} {
		if err := e.h.HandleOrderEvent(t.Context(), ev); !kafkax.IsPermanent(err) {
			t.Errorf("%s: err = %v, want permanent", name, err)
		}
	}
	// 不合法的答复不能被当作已答复。
	e.want(t, p.PaymentID, succeeded, false)
}

func TestReplyBeforeResultVisibleIsRetried(t *testing.T) {
	e := newEnv(t)
	p := e.insert(t, 1, pending)

	// 答复先于本地终态可见：暂时性错误，消费者重试；不能把 PENDING 标记为已答复。
	err := e.h.HandleOrderEvent(t.Context(), reply(p, accepted, ""))
	if err == nil || kafkax.IsPermanent(err) || !errors.Is(err, consumer.ErrPaymentPending) {
		t.Fatalf("err = %v, want retryable ErrPaymentPending", err)
	}
	e.want(t, p.PaymentID, pending, false)

	// 结果可见之后，重试成功。
	testdb.MustExec(t, e.db, "UPDATE payments SET status = ? WHERE payment_id = ?", int32(succeeded), p.PaymentID)
	e.handle(t, reply(p, accepted, ""))
	e.want(t, p.PaymentID, succeeded, true)
}

func TestOtherEventsSkipped(t *testing.T) {
	e := newEnv(t)
	p := e.insert(t, 1, succeeded)
	for _, ev := range []*eventv1.OrderEvent{
		{OrderId: p.OrderID, Body: &eventv1.OrderEvent_Paid{Paid: &eventv1.OrderPaid{PaymentId: p.PaymentID}}},
		{OrderId: p.OrderID, Body: &eventv1.OrderEvent_Cancelled{Cancelled: &eventv1.OrderCancelled{}}},
		{OrderId: p.OrderID, Body: &eventv1.OrderEvent_Closed{Closed: &eventv1.OrderClosed{}}},
		{OrderId: p.OrderID, Body: &eventv1.OrderEvent_Failed{Failed: &eventv1.OrderFailed{Reason: "X"}}},
		{OrderId: p.OrderID}, // 新版本加了本服务不认识的事件
	} {
		e.handle(t, ev)
	}
	e.want(t, p.PaymentID, succeeded, false)
}

func TestConflictingRepliesFirstWins(t *testing.T) {
	t.Run("accepted then refund", func(t *testing.T) {
		e := newEnv(t)
		p := e.insert(t, 1, succeeded)
		e.handle(t, reply(p, accepted, ""))
		e.handle(t, reply(p, refundRequired, "ORDER_NOT_PAYABLE"))
		// 订单已经接受了这笔钱，不能再退。
		e.want(t, p.PaymentID, succeeded, true)
		if n := e.logs.count("payment refunded"); n != 0 {
			t.Errorf("%d refunds, want 0", n)
		}
		if n := e.logs.errors(); n != 1 {
			t.Errorf("%d error logs for conflicting reply, want 1", n)
		}
	})
	t.Run("refund then accepted", func(t *testing.T) {
		e := newEnv(t)
		p := e.insert(t, 1, succeeded)
		e.handle(t, reply(p, refundRequired, "ORDER_NOT_PAYABLE"))
		e.handle(t, reply(p, accepted, ""))
		e.want(t, p.PaymentID, refunded, true)
		if n := e.logs.errors(); n != 1 {
			t.Errorf("%d error logs for conflicting reply, want 1", n)
		}
	})
}

func TestConcurrentDuplicateRepliesRefundOnce(t *testing.T) {
	e := newEnv(t)
	const n = 20
	pays := make([]store.Payment, n)
	for i := range pays {
		pays[i] = e.insert(t, int64(i+1), succeeded)
	}

	// 至少一次投递 + 再平衡时两个实例并发处理同一条消息：每笔支付只退一次。
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 4 {
		for _, p := range pays {
			wg.Go(func() {
				<-start
				if err := e.h.HandleOrderEvent(t.Context(), reply(p, refundRequired, "ORDER_NOT_PAYABLE")); err != nil {
					t.Error(err)
				}
			})
		}
	}
	close(start)
	wg.Wait()

	for _, p := range pays {
		e.want(t, p.PaymentID, refunded, true)
	}
	if got := e.logs.count("payment refunded"); got != n {
		t.Errorf("%d refunds, want %d", got, n)
	}
	if got := e.logs.errors(); got != 0 {
		t.Errorf("%d error logs, want 0", got)
	}
}

func TestConcurrentConflictingRepliesSettleConsistently(t *testing.T) {
	e := newEnv(t)
	p := e.insert(t, 1, succeeded)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 16 {
		d := accepted
		if i%2 == 1 {
			d = refundRequired
		}
		wg.Go(func() {
			<-start
			if err := e.h.HandleOrderEvent(t.Context(), reply(p, d, "ORDER_NOT_PAYABLE")); err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	wg.Wait()

	// 两种答复里只有一个生效：要么接受（SUCCEEDED），要么退款（REFUNDED，且只退一次）。
	got := e.get(t, p.PaymentID)
	if !got.Notified {
		t.Fatal("payment not notified")
	}
	refunds := e.logs.count("payment refunded")
	switch got.Status {
	case succeeded:
		if refunds != 0 {
			t.Errorf("accepted payment refunded %d times", refunds)
		}
	case refunded:
		if refunds != 1 {
			t.Errorf("%d refunds, want 1", refunds)
		}
	default:
		t.Errorf("status = %v", got.Status)
	}
}

func TestKafkaAdapter(t *testing.T) {
	e := newEnv(t)
	p := e.insert(t, 1, succeeded)
	handler := e.h.Kafka(time.Second)

	if err := handler(t.Context(), kafkax.Message{Value: []byte("not a proto \xff\xff")}); !kafkax.IsPermanent(err) {
		t.Errorf("bad payload: err = %v, want permanent", err)
	}

	value, err := proto.Marshal(reply(p, accepted, ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler(t.Context(), kafkax.Message{Value: value}); err != nil {
		t.Fatalf("handler = %v", err)
	}
	e.want(t, p.PaymentID, succeeded, true)

	// 消费者的 ctx 被强制取消时，handler 不能继续做事。
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	q := e.insert(t, 2, succeeded)
	value, err = proto.Marshal(reply(q, accepted, ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler(ctx, kafkax.Message{Value: value}); err == nil {
		t.Error("handler with cancelled ctx = nil, want error")
	}
	e.want(t, q.PaymentID, succeeded, false)
}
