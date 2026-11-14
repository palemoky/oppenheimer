package processor

import (
	"log/slog"
	"testing"
	"time"

	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/outbox"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/payment"
	"github.com/palemoky/oppenheimer/services/payment/internal/store"
)

// 读到 PENDING 之后、条件更新之前，另一个实例已经完成了转换：这次更新影响 0 行，不能写事件。
// 并发测试里这种交错不一定每次都出现，这里用一份过期的读取结果确定性地构造它。
func TestCompleteLostRaceWritesNoEvent(t *testing.T) {
	db := testdb.New(t, payment.Migrations, payment.MigrationsDir)
	st := store.New(db)
	p, err := New(st, nil, slog.New(slog.DiscardHandler), Config{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)
	stale := store.Payment{
		PaymentID: 1, OrderID: 10, UserID: 7, Status: paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING,
		Amount: 100, Currency: "CNY", Outcome: paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_SUCCESS, CreatedAt: now,
	}
	if err := st.Insert(t.Context(), stale); err != nil {
		t.Fatal(err)
	}
	testdb.MustExec(t, db, "UPDATE payments SET status = ? WHERE payment_id = ?",
		int32(paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED), stale.PaymentID)

	ok, err := p.complete(t.Context(), stale, now)
	if err != nil || ok {
		t.Fatalf("complete(stale) = (%v, %v), want (false, nil)", ok, err)
	}
	if n, err := outbox.Pending(t.Context(), db); err != nil || n != 0 {
		t.Errorf("outbox pending = (%d, %v), want (0, nil)", n, err)
	}
}
