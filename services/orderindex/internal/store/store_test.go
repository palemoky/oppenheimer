package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/storetest"
)

var base = time.Date(2026, 6, 1, 12, 0, 0, 123456000, time.UTC)

func row(merchant, order, version int64, status int32) store.Row {
	return store.Row{
		MerchantID:  merchant,
		OrderID:     order,
		UserID:      order * 10,
		Status:      status,
		TotalAmount: 100 * version,
		Currency:    "CNY",
		CreatedAt:   base,
		Version:     version,
	}
}

func mustUpsert(t *testing.T, st *store.Store, r store.Row, at time.Time) store.Outcome {
	t.Helper()
	o, err := st.Upsert(t.Context(), r, at)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func mustGet(t *testing.T, st *store.Store, merchant, order int64) store.Row {
	t.Helper()
	r, err := st.Get(t.Context(), merchant, order)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// sameImage 比较除 SyncedAt 以外的字段。
func sameImage(a, b store.Row) bool {
	a.SyncedAt, b.SyncedAt = time.Time{}, time.Time{}
	return a.MerchantID == b.MerchantID && a.OrderID == b.OrderID && a.UserID == b.UserID &&
		a.Status == b.Status && a.TotalAmount == b.TotalAmount && a.Currency == b.Currency &&
		a.CreatedAt.Equal(b.CreatedAt) && a.PaidAt.Equal(b.PaidAt) && a.Version == b.Version
}

// newerVersionWins 用 exec 先写 v1、再写 v2，返回 v2 有没有完整地覆盖 v1。
func newerVersionWins(ctx context.Context, st *store.Store, exec func(store.Row, time.Time) error, merchant, order int64) error {
	v1 := row(merchant, order, 1, 2)
	v2 := row(merchant, order, 2, 3)
	v2.UserID++
	v2.Currency = "USD"
	v2.CreatedAt = base.Add(time.Second)
	v2.PaidAt = base.Add(time.Minute)
	t1, t2 := base.Add(time.Hour), base.Add(2*time.Hour)
	if err := exec(v1, t1); err != nil {
		return err
	}
	if err := exec(v2, t2); err != nil {
		return err
	}
	got, err := st.Get(ctx, merchant, order)
	if err != nil {
		return err
	}
	if !sameImage(got, v2) || !got.SyncedAt.Equal(t2) {
		return fmt.Errorf("row = %+v, want image %+v synced at %v", got, v2, t2)
	}
	return nil
}

func rawExec(db *sql.DB, query string) func(store.Row, time.Time) error {
	return func(r store.Row, at time.Time) error {
		var paid sql.NullTime
		if !r.PaidAt.IsZero() {
			paid = sql.NullTime{Time: r.PaidAt, Valid: true}
		}
		_, err := db.ExecContext(context.Background(), query, r.MerchantID, r.OrderID, r.UserID, r.Status,
			r.TotalAmount, r.Currency, r.CreatedAt, paid, r.Version, at)
		return err
	}
}

// ON DUPLICATE KEY UPDATE 的赋值从左到右执行，src_version 必须最后赋值。
// 这个测试有两半：真实的语句必须让新版本完整覆盖旧版本；把 src_version 挪到最前面的变体必须做不到，
// 证明前一半确实能发现这种改动（如果有人把它挪到前面，前一半就会失败）。
func TestUpsertAssignmentOrder(t *testing.T) {
	t.Parallel()
	st, dbs := storetest.New(t)
	const merchant = 7
	slot := st.Router().ForMerchant(merchant)
	db := dbs[st.Router().DB(slot)]
	stmt := st.UpsertSQL(slot)

	viaStore := func(r store.Row, at time.Time) error { _, err := st.Upsert(t.Context(), r, at); return err }
	if err := newerVersionWins(t.Context(), st, viaStore, merchant, 1); err != nil {
		t.Fatalf("real statement: %v", err)
	}

	// 静态检查：最后一个赋值是 src_version。
	_, update, ok := strings.Cut(stmt, "ON DUPLICATE KEY UPDATE")
	if !ok {
		t.Fatal("upsert has no ON DUPLICATE KEY UPDATE")
	}
	assigns := strings.Split(update, ",\n")
	if last := strings.TrimSpace(assigns[len(assigns)-1]); !strings.HasPrefix(last, "src_version") {
		t.Fatalf("last assignment is %q, want src_version", last)
	}

	// 变体：src_version 挪到第一个。
	lastAssign := assigns[len(assigns)-1]
	moved := slices.Concat([]string{lastAssign}, assigns[:len(assigns)-1])
	mutated := strings.Replace(stmt, update, "\n"+strings.Join(moved, ",\n"), 1)
	if mutated == stmt {
		t.Fatal("failed to build the mutated statement")
	}
	if err := newerVersionWins(t.Context(), st, rawExec(db, mutated), merchant, 2); err == nil {
		t.Fatal("statement with src_version assigned first still passes; the check is not sensitive to the order")
	}
}

// 同一个版本再写一次（重复消息）：行完全不变，synced_at 也不变，即使内容不同。
func TestUpsertDuplicateKeepsRow(t *testing.T) {
	t.Parallel()
	st, _ := storetest.New(t)
	r := row(3, 10, 5, 2)
	t1 := base.Add(time.Hour)
	if o := mustUpsert(t, st, r, t1); o != store.Inserted {
		t.Fatalf("first upsert = %v, want inserted", o)
	}
	dup := r
	dup.Status = 3
	if o := mustUpsert(t, st, dup, t1.Add(time.Hour)); o != store.Stale {
		t.Fatalf("duplicate upsert = %v, want stale", o)
	}
	if o := mustUpsert(t, st, r, t1.Add(2*time.Hour)); o != store.Stale {
		t.Fatalf("identical upsert = %v, want stale", o)
	}
	got := mustGet(t, st, 3, 10)
	if !sameImage(got, r) || !got.SyncedAt.Equal(t1) {
		t.Fatalf("row = %+v, want %+v synced at %v", got, r, t1)
	}
}

// 旧版本晚到：被忽略。新版本：覆盖，包括 paid_at 从 NULL 变成有值。
func TestUpsertVersionOrdering(t *testing.T) {
	t.Parallel()
	st, _ := storetest.New(t)
	v3 := row(4, 20, 3, 3)
	v3.PaidAt = base.Add(time.Minute)
	if o := mustUpsert(t, st, v3, base); o != store.Inserted {
		t.Fatalf("v3 = %v, want inserted", o)
	}
	if o := mustUpsert(t, st, row(4, 20, 2, 2), base); o != store.Stale {
		t.Fatalf("v2 after v3 = %v, want stale", o)
	}
	if got := mustGet(t, st, 4, 20); !sameImage(got, v3) {
		t.Fatalf("row = %+v, want %+v", got, v3)
	}
	v4 := row(4, 20, 4, 5)
	if o := mustUpsert(t, st, v4, base); o != store.Updated {
		t.Fatalf("v4 = %v, want updated", o)
	}
	if got := mustGet(t, st, 4, 20); !sameImage(got, v4) || !got.PaidAt.IsZero() {
		t.Fatalf("row = %+v, want %+v", got, v4)
	}
	if _, err := st.Get(t.Context(), 4, 21); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get missing = %v, want ErrNotFound", err)
	}
}

// 两种列表查询都必须走索引：不带状态走主键，带状态走 idx_merchant_status。
func TestListUsesIndex(t *testing.T) {
	t.Parallel()
	st, _ := storetest.New(t)
	for i := range int64(50) {
		mustUpsert(t, st, row(5, i+1, 1, int32(2+i%3)), base)
		mustUpsert(t, st, row(9, i+1, 1, 2), base) // 同一个 slot 的另一个商家
	}
	for _, c := range []struct {
		status int32
		want   string
	}{{0, "PRIMARY"}, {3, "idx_merchant_status"}} {
		p, err := st.ExplainList(t.Context(), store.ListQuery{MerchantID: 5, Status: c.status, Limit: 21})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("status=%d: key=%s extra=%s", c.status, p.Key, p.Extra)
		if p.Key != c.want || strings.Contains(p.Extra, "filesort") {
			t.Errorf("status=%d plan = %+v, want key %q without filesort", c.status, p, c.want)
		}
	}
}
