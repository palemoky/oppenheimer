package store_test

import (
	"database/sql"
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

	byID, err := st.GetByID(t.Context(), want.PaymentID)
	if err != nil {
		t.Fatal(err)
	}
	if byID != want {
		t.Errorf("GetByID = %+v, want %+v", byID, want)
	}
	if _, err := st.GetByID(t.Context(), 999); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetByID(missing) = %v, want ErrNotFound", err)
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
		var ok bool
		err := st.InTx(t.Context(), func(tx *sql.Tx) error {
			var err error
			ok, err = st.CompletePending(t.Context(), tx, p.PaymentID, to, t0.Add(time.Second))
			return err
		})
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

func TestCompletePendingRolledBackWithTx(t *testing.T) {
	st, _ := newStore(t)
	p := insert(t, st, 1, pending)

	// 状态变更跟随事务：事务回滚（例如写 outbox 失败）时支付单仍是 PENDING，下一轮会重做。
	boom := errors.New("boom")
	err := st.InTx(t.Context(), func(tx *sql.Tx) error {
		ok, err := st.CompletePending(t.Context(), tx, p.PaymentID, succeeded, t0)
		if err != nil || !ok {
			t.Fatalf("CompletePending = (%v, %v), want (true, nil)", ok, err)
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("InTx = %v, want boom", err)
	}
	if got := get(t, st, p.OrderID); got.Status != pending {
		t.Errorf("status = %v after rollback, want PENDING", got.Status)
	}
}

func TestMarkNotifiedRequiresStatus(t *testing.T) {
	st, _ := newStore(t)
	p := insert(t, st, 1, failed)

	// 答复针对的是读到的状态；状态对不上时不标记。
	if ok, err := st.MarkNotified(t.Context(), p.PaymentID, succeeded, t0); err != nil || ok {
		t.Errorf("MarkNotified(wrong status) = (%v, %v), want (false, nil)", ok, err)
	}
	if ok, err := st.MarkNotified(t.Context(), p.PaymentID, failed, t0); err != nil || !ok {
		t.Errorf("MarkNotified = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := st.MarkNotified(t.Context(), p.PaymentID, failed, t0); err != nil || ok {
		t.Errorf("MarkNotified(again) = (%v, %v), want (false, nil)", ok, err)
	}
}

func TestRefundAfterNotifiedIsNoop(t *testing.T) {
	st, _ := newStore(t)
	p := insert(t, st, 1, succeeded)
	if ok, err := st.MarkNotified(t.Context(), p.PaymentID, succeeded, t0); err != nil || !ok {
		t.Fatalf("MarkNotified = (%v, %v)", ok, err)
	}
	// 已被订单服务接受的支付不能再被退款。
	if ok, err := st.RefundAndMarkNotified(t.Context(), p.PaymentID, t0); err != nil || ok {
		t.Errorf("refund after accepted = (%v, %v), want (false, nil)", ok, err)
	}
	if got := get(t, st, p.OrderID); got.Status != succeeded {
		t.Errorf("status = %v, want SUCCEEDED", got.Status)
	}
}
