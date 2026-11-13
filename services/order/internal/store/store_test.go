package store_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/sharding"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/services/order"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

type fixture struct {
	dbs    []*sql.DB
	router *sharding.Router
	store  *store.Store
	ids    *idgen.Generator
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	router, err := sharding.New(sharding.Config{Slots: 8, DBs: 2})
	if err != nil {
		t.Fatal(err)
	}
	dbs := make([]*sql.DB, router.DBs())
	for i := range dbs {
		dbs[i] = testdb.NewTemplate(t, order.Migrations, order.MigrationsDir, order.MigrationData{Slots: router.SlotsOf(i)})
	}
	st, err := store.New(dbs, router)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := idgen.New(0)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{dbs: dbs, router: router, store: st, ids: ids}
}

func (f *fixture) newOrder(t *testing.T, userID int64, key string) *store.Order {
	t.Helper()
	id, err := f.ids.Next(userID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	return &store.Order{
		OrderID:        id,
		UserID:         userID,
		Status:         orderv1.OrderStatus_ORDER_STATUS_CREATING,
		Currency:       "CNY",
		IdempotencyKey: key,
		RequestHash:    make([]byte, 32),
		ExpireAt:       now.Add(15 * time.Minute),
		CreatedAt:      now,
		UpdatedAt:      now,
		Items:          []store.Item{{SKUID: 1, Quantity: 1}},
	}
}

// rows 返回 slot 上 base 表里 order_id 为 orderID 的行数。
func (f *fixture) rows(t *testing.T, base string, slot int, orderID int64) int {
	t.Helper()
	var n int
	q := "SELECT COUNT(*) FROM " + sharding.Table(base, slot) + " WHERE order_id = ?"
	if err := f.dbs[f.router.DB(slot)].QueryRowContext(t.Context(), q, orderID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestNewValidatesDatabases(t *testing.T) {
	t.Parallel()
	router, err := sharding.New(sharding.Config{Slots: 8, DBs: 2})
	if err != nil {
		t.Fatal(err)
	}
	db := &sql.DB{}
	for name, dbs := range map[string][]*sql.DB{
		"too few":  {db},
		"too many": {db, db, db},
		"nil db":   {db, nil},
	} {
		if _, err := store.New(dbs, router); err == nil {
			t.Errorf("%s: New succeeded, want error", name)
		}
	}
	if _, err := store.New([]*sql.DB{db, db}, nil); err == nil {
		t.Error("nil router: New succeeded, want error")
	}
}

// 订单写进按用户算出的那个 slot 的表，其他 slot 的表里没有；按订单号和按幂等键都能读到。
func TestInsertOrderGoesToUserSlotOnly(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	for user := int64(1); user <= 16; user++ {
		o := f.newOrder(t, user, "k")
		if err := f.store.InsertOrder(t.Context(), o); err != nil {
			t.Fatalf("insert order of user %d: %v", user, err)
		}
		want := f.router.ForUser(user)
		for slot := range f.router.Slots() {
			wantRows := 0
			if slot == want {
				wantRows = 1
			}
			for _, base := range []string{"orders", "order_items"} {
				if n := f.rows(t, base, slot, o.OrderID); n != wantRows {
					t.Errorf("user %d: %s rows = %d, want %d", user, sharding.Table(base, slot), n, wantRows)
				}
			}
		}
		got, err := f.store.GetOrder(t.Context(), o.OrderID)
		if err != nil || got.UserID != user {
			t.Fatalf("GetOrder(%d) = %+v, %v", o.OrderID, got, err)
		}
		byKey, err := f.store.GetOrderByIdempotencyKey(t.Context(), user, "k")
		if err != nil || byKey.OrderID != o.OrderID {
			t.Fatalf("GetOrderByIdempotencyKey(%d) = %+v, %v", user, byKey, err)
		}
	}
}

// 订单号的基因和 user_id 不在同一个 slot：插入失败，什么都不写。
func TestInsertOrderRejectsGeneMismatch(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	o := f.newOrder(t, 1, "k")
	o.UserID = 2
	if err := f.store.InsertOrder(t.Context(), o); !errors.Is(err, store.ErrWrongShard) {
		t.Fatalf("InsertOrder = %v, want ErrWrongShard", err)
	}
	for slot := range f.router.Slots() {
		if n := f.rows(t, "orders", slot, o.OrderID); n != 0 {
			t.Errorf("%s has %d rows of the rejected order", sharding.Table("orders", slot), n)
		}
	}
}

// 事务绑定在一个 slot 上，操作别的 slot 的订单报错。两个订单分别在同库的不同 slot、不同库。
func TestTxRejectsOrderOfAnotherSlot(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	a := f.newOrder(t, 1, "a")     // slot 1，库 1
	same := f.newOrder(t, 3, "b")  // slot 3，库 1
	other := f.newOrder(t, 2, "c") // slot 2，库 0
	for _, o := range []*store.Order{a, same, other} {
		if err := f.store.InsertOrder(t.Context(), o); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	for _, foreign := range []*store.Order{same, other} {
		ops := map[string]func(tx *store.Tx) error{
			"LockOrder": func(tx *store.Tx) error { _, err := tx.LockOrder(t.Context(), foreign.OrderID); return err },
			"MarkPaid":  func(tx *store.Tx) error { _, err := tx.MarkPaid(t.Context(), foreign.OrderID, 1, now); return err },
			"MarkCreateFailed": func(tx *store.Tx) error {
				_, err := tx.MarkCreateFailed(t.Context(), foreign.OrderID, "x", now)
				return err
			},
			"Cancel": func(tx *store.Tx) error {
				_, err := tx.Cancel(t.Context(), foreign.OrderID, foreign.UserID, now)
				return err
			},
			"CloseExpired": func(tx *store.Tx) error { _, err := tx.CloseExpired(t.Context(), foreign.OrderID, now); return err },
			"InsertPaymentResult": func(tx *store.Tx) error {
				return tx.InsertPaymentResult(t.Context(), &store.PaymentResult{PaymentID: 1, OrderID: foreign.OrderID}, now)
			},
		}
		for name, op := range ops {
			err := f.store.InTx(t.Context(), a.OrderID, func(tx *store.Tx) error {
				if tx.Slot() != f.router.ForOrder(a.OrderID) {
					t.Errorf("tx slot = %d, want %d", tx.Slot(), f.router.ForOrder(a.OrderID))
				}
				return op(tx)
			})
			if !errors.Is(err, store.ErrWrongShard) {
				t.Errorf("%s on order in slot %d from tx on slot %d: err = %v, want ErrWrongShard",
					name, f.router.ForOrder(foreign.OrderID), f.router.ForOrder(a.OrderID), err)
			}
		}
		// 被拒绝的操作什么都没改。
		got, err := f.store.GetOrder(t.Context(), foreign.OrderID)
		if err != nil || got.Status != orderv1.OrderStatus_ORDER_STATUS_CREATING {
			t.Errorf("foreign order after rejected ops = %+v, %v", got, err)
		}
	}

	// 对照：本 slot 的订单照常操作。
	err := f.store.InTx(t.Context(), a.OrderID, func(tx *store.Tx) error {
		ok, err := tx.MarkCreateFailed(t.Context(), a.OrderID, "x", now)
		if err == nil && !ok {
			err = errors.New("mark failed affected no rows")
		}
		return err
	})
	if err != nil {
		t.Fatalf("own-slot op: %v", err)
	}
}

// 后台扫描按 slot 进行，每个 slot 只返回自己的订单；越界的 slot 报错。
func TestListBySlot(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	for user := int64(1); user <= 16; user++ {
		if err := f.store.InsertOrder(t.Context(), f.newOrder(t, user, "k")); err != nil {
			t.Fatal(err)
		}
	}
	later := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	for slot := range f.router.Slots() {
		refs, err := f.store.ListStaleCreating(t.Context(), slot, later, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(refs) != 2 {
			t.Errorf("slot %d: %d stale orders, want 2", slot, len(refs))
		}
		for _, r := range refs {
			if f.router.ForOrder(r.OrderID) != slot || f.router.ForUser(r.UserID) != slot {
				t.Errorf("slot %d returned order %d of user %d", slot, r.OrderID, r.UserID)
			}
		}
		if _, err := f.store.ListExpired(t.Context(), slot, later, 100); err != nil {
			t.Fatal(err)
		}
	}
	for _, slot := range []int{-1, f.router.Slots()} {
		if _, err := f.store.ListExpired(t.Context(), slot, later, 1); !errors.Is(err, store.ErrWrongShard) {
			t.Errorf("ListExpired(slot %d) = %v, want ErrWrongShard", slot, err)
		}
		if _, err := f.store.ListStaleCreating(t.Context(), slot, later, 1); !errors.Is(err, store.ErrWrongShard) {
			t.Errorf("ListStaleCreating(slot %d) = %v, want ErrWrongShard", slot, err)
		}
	}
}
