package store_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/services/payment"
	"github.com/palemoky/Oppenheimer/services/payment/internal/store"
)

const (
	pending   = paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING
	succeeded = paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	failed    = paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED
	refunded  = paymentv1.PaymentStatus_PAYMENT_STATUS_REFUNDED
)

var t0 = time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)

func newStore(t *testing.T) (*store.Store, func(query string, args ...any)) {
	t.Helper()
	db := testdb.New(t, payment.Migrations, payment.MigrationsDir)
	return store.New(db), func(query string, args ...any) { testdb.MustExec(t, db, query, args...) }
}

func insert(t *testing.T, st *store.Store, id int64, status paymentv1.PaymentStatus) store.Payment {
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
	if err := st.Insert(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	return p
}

func get(t *testing.T, st *store.Store, orderID int64) store.Payment {
	t.Helper()
	p, err := st.GetByOrderID(t.Context(), orderID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestInsertAndGetRoundTrip(t *testing.T) {
	st, _ := newStore(t)
	want := insert(t, st, 1, pending)

	got := get(t, st, want.OrderID)
	want.UpdatedAt = want.CreatedAt
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}

	if _, err := st.GetByOrderID(t.Context(), 999); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetByOrderID(missing) = %v, want ErrNotFound", err)
	}
}

func TestInsertSameOrderIsDuplicate(t *testing.T) {
	st, _ := newStore(t)
	p := insert(t, st, 1, pending)
	p.PaymentID = 2
	if err := st.Insert(t.Context(), p); !errors.Is(err, store.ErrDuplicate) {
		t.Errorf("Insert(same order) = %v, want ErrDuplicate", err)
	}
}

// runConcurrently 让 n 个 goroutine 同时执行 fn，返回其中返回 true 的个数。
func runConcurrently(t *testing.T, n int, fn func(i int) (bool, error)) int {
	t.Helper()
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		wins  atomic.Int32
	)
	for i := range n {
		wg.Go(func() {
			<-start
			ok, err := fn(i)
			if err != nil {
				t.Error(err)
			}
			if ok {
				wins.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	return int(wins.Load())
}

func TestCompletePendingConcurrentOnlyOneWins(t *testing.T) {
	st, _ := newStore(t)
	p := insert(t, st, 1, pending)

	// 一半想改成功、一半想改失败，互斥的转换只能有一个生效。
	var winner atomic.Int32
	wins := runConcurrently(t, 16, func(i int) (bool, error) {
		to := succeeded
		if i%2 == 1 {
			to = failed
		}
		ok, err := st.CompletePending(t.Context(), p.PaymentID, to, t0.Add(time.Second))
		if ok {
			winner.Store(int32(to))
		}
		return ok, err
	})
	if wins != 1 {
		t.Fatalf("%d callers completed the payment, want 1", wins)
	}
	if got := get(t, st, p.OrderID); int32(got.Status) != winner.Load() {
		t.Errorf("status = %v, want winner %v", got.Status, paymentv1.PaymentStatus(winner.Load()))
	}
}

func TestRefundConcurrentOnlyOnce(t *testing.T) {
	st, _ := newStore(t)
	p := insert(t, st, 1, succeeded)

	wins := runConcurrently(t, 16, func(int) (bool, error) {
		return st.RefundAndMarkNotified(t.Context(), p.PaymentID, t0.Add(time.Second))
	})
	if wins != 1 {
		t.Fatalf("%d callers refunded, want 1", wins)
	}
	got := get(t, st, p.OrderID)
	if got.Status != refunded || !got.Notified {
		t.Errorf("status = %v notified = %v, want REFUNDED and notified", got.Status, got.Notified)
	}
}

func TestRefundRequiresSucceeded(t *testing.T) {
	st, _ := newStore(t)
	for i, s := range []paymentv1.PaymentStatus{pending, failed, refunded} {
		p := insert(t, st, int64(i+1), s)
		ok, err := st.RefundAndMarkNotified(t.Context(), p.PaymentID, t0)
		if err != nil || ok {
			t.Errorf("refund %v = (%v, %v), want (false, nil)", s, ok, err)
		}
	}
}

func TestMarkNotifiedVersusRefundOnlyOneWins(t *testing.T) {
	st, _ := newStore(t)
	p := insert(t, st, 1, succeeded)

	wins := runConcurrently(t, 16, func(i int) (bool, error) {
		if i%2 == 0 {
			return st.MarkNotified(t.Context(), p.PaymentID, succeeded, t0)
		}
		return st.RefundAndMarkNotified(t.Context(), p.PaymentID, t0)
	})
	if wins != 1 {
		t.Errorf("%d callers finished notification, want 1", wins)
	}
}

func TestIncrNotifyAttemptsConcurrentCountsOnce(t *testing.T) {
	st, _ := newStore(t)
	p := insert(t, st, 1, failed)

	// 多个处理者基于同一次读取各自失败了一次，只应记一次。
	wins := runConcurrently(t, 16, func(int) (bool, error) {
		return st.IncrNotifyAttempts(t.Context(), p.PaymentID, failed, 0, t0)
	})
	if wins != 1 {
		t.Fatalf("%d callers incremented, want 1", wins)
	}
	if got := get(t, st, p.OrderID); got.NotifyAttempts != 1 {
		t.Errorf("notify_attempts = %d, want 1", got.NotifyAttempts)
	}
}

func TestListPendingCreatedBefore(t *testing.T) {
	st, exec := newStore(t)
	early := insert(t, st, 1, pending)
	late := insert(t, st, 2, pending)
	exec("UPDATE payments SET created_at = ? WHERE payment_id = ?", t0.Add(time.Second), late.PaymentID)
	insert(t, st, 3, succeeded)

	got, err := st.ListPendingCreatedBefore(t.Context(), t0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PaymentID != early.PaymentID {
		t.Errorf("got %+v, want only payment %d", got, early.PaymentID)
	}
}

func TestListUnnotifiedDueBackoff(t *testing.T) {
	const (
		base = 200 * time.Millisecond
		maxD = 30 * time.Second
	)
	tests := []struct {
		attempts int
		delay    time.Duration
	}{
		{0, 0},
		{1, 400 * time.Millisecond},
		{2, 800 * time.Millisecond},
		{7, 25600 * time.Millisecond},
		{8, maxD},
		{1000, maxD},
	}
	for _, tt := range tests {
		st, exec := newStore(t)
		p := insert(t, st, 1, succeeded)
		exec("UPDATE payments SET notify_attempts = ?, updated_at = ? WHERE payment_id = ?", tt.attempts, t0, p.PaymentID)

		if tt.delay > 0 {
			got, err := st.ListUnnotifiedDue(t.Context(), t0.Add(tt.delay-time.Microsecond), base, maxD, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 0 {
				t.Errorf("attempts=%d: due before %v", tt.attempts, tt.delay)
			}
		}
		got, err := st.ListUnnotifiedDue(t.Context(), t0.Add(tt.delay), base, maxD, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 {
			t.Errorf("attempts=%d: not due at %v", tt.attempts, tt.delay)
		}
	}
}

func TestListUnnotifiedDueSkipsOthers(t *testing.T) {
	st, exec := newStore(t)
	insert(t, st, 1, pending)
	insert(t, st, 2, refunded)
	done := insert(t, st, 3, succeeded)
	exec("UPDATE payments SET notified = 1 WHERE payment_id = ?", done.PaymentID)
	want := insert(t, st, 4, failed)

	got, err := st.ListUnnotifiedDue(t.Context(), t0, time.Millisecond, time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PaymentID != want.PaymentID {
		t.Errorf("got %+v, want only payment %d", got, want.PaymentID)
	}
}
