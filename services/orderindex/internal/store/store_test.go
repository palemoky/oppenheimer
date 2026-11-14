package store_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
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

// mustUpsert 用 UpsertRows 写 rows，返回结果。
func mustUpsert(t *testing.T, st *store.Store, at time.Time, rows ...store.Row) store.WriteResult {
	t.Helper()
	res, err := st.UpsertRows(t.Context(), rows, at)
	if err != nil {
		t.Fatal(err)
	}
	return res
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

// upsertFunc 把一组行写进索引（一条语句或 UpsertRows）。
type upsertFunc func(rows []store.Row, at time.Time) error

// newerVersionWins 用 exec 先把 orders 这几个订单的 v1 写成一条多行语句、再写 v2，
// 返回 v2 有没有逐列完整地覆盖每一行的 v1。
func newerVersionWins(ctx context.Context, st *store.Store, exec upsertFunc, merchant int64, orders ...int64) error {
	var v1s, v2s []store.Row
	for _, o := range orders {
		v1 := row(merchant, o, 1, 2)
		v2 := row(merchant, o, 2, 3)
		v2.UserID++
		v2.Currency = "USD"
		v2.CreatedAt = base.Add(time.Second)
		v2.PaidAt = base.Add(time.Minute)
		v1s, v2s = append(v1s, v1), append(v2s, v2)
	}
	t1, t2 := base.Add(time.Hour), base.Add(2*time.Hour)
	if err := exec(v1s, t1); err != nil {
		return err
	}
	if err := exec(v2s, t2); err != nil {
		return err
	}
	for _, v2 := range v2s {
		got, err := st.Get(ctx, merchant, v2.OrderID)
		if err != nil {
			return err
		}
		if !sameImage(got, v2) || !got.SyncedAt.Equal(t2) {
			return fmt.Errorf("order %d: row = %+v, want image %+v synced at %v", v2.OrderID, got, v2, t2)
		}
	}
	return nil
}

// rawExec 直接执行 query（一条带 len(rows) 个元组的写入语句）。
func rawExec(db *sql.DB, query string) upsertFunc {
	return func(rows []store.Row, at time.Time) error {
		var args []any
		for _, r := range rows {
			var paid sql.NullTime
			if !r.PaidAt.IsZero() {
				paid = sql.NullTime{Time: r.PaidAt, Valid: true}
			}
			args = append(args, r.MerchantID, r.OrderID, r.UserID, r.Status, r.TotalAmount, r.Currency, r.CreatedAt, paid, r.Version, at)
		}
		_, err := db.ExecContext(context.Background(), query, args...)
		return err
	}
}

// ON DUPLICATE KEY UPDATE 的赋值从左到右执行，src_version 必须最后赋值，多行语句里对每一行都是如此。
// 这个测试有两半：真实的多行语句必须让新版本完整覆盖旧版本；把 src_version 挪到最前面的变体必须做不到，
// 证明前一半确实能发现这种改动（如果有人把它挪到前面，前一半就会失败）。
func TestUpsertAssignmentOrder(t *testing.T) {
	t.Parallel()
	st, dbs := storetest.New(t)
	const (
		merchant = 7
		rows     = 3 // 多行语句
	)
	slot := st.Router().ForMerchant(merchant)
	db := dbs[st.Router().DB(slot)]
	stmt := st.UpsertSQL(slot, rows)

	viaStore := func(rs []store.Row, at time.Time) error { _, err := st.UpsertRows(t.Context(), rs, at); return err }
	if err := newerVersionWins(t.Context(), st, viaStore, merchant, 1, 2, 3); err != nil {
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
	if err := newerVersionWins(t.Context(), st, rawExec(db, mutated), merchant, 11, 12, 13); err == nil {
		t.Fatal("statement with src_version assigned first still passes; the check is not sensitive to the order")
	}
}

// 同一个版本再写一次（重复消息）：行完全不变，synced_at 也不变，即使内容不同。受影响行数 1、0、0。
func TestUpsertDuplicateKeepsRow(t *testing.T) {
	t.Parallel()
	st, _ := storetest.New(t)
	r := row(3, 10, 5, 2)
	t1 := base.Add(time.Hour)
	if res := mustUpsert(t, st, t1, r); res != (store.WriteResult{Rows: 1, Affected: 1, Changed: 1}) {
		t.Fatalf("first upsert = %+v, want one insert", res)
	}
	dup := r
	dup.Status = 3
	if res := mustUpsert(t, st, t1.Add(time.Hour), dup); res != (store.WriteResult{Rows: 1}) {
		t.Fatalf("duplicate upsert = %+v, want stale", res)
	}
	if res := mustUpsert(t, st, t1.Add(2*time.Hour), r); res != (store.WriteResult{Rows: 1}) {
		t.Fatalf("identical upsert = %+v, want stale", res)
	}
	got := mustGet(t, st, 3, 10)
	if !sameImage(got, r) || !got.SyncedAt.Equal(t1) {
		t.Fatalf("row = %+v, want %+v synced at %v", got, r, t1)
	}
}

// 旧版本晚到：被忽略。新版本：覆盖，包括 paid_at 从有值变成 NULL。单行时受影响行数是精确的 1、0、2。
func TestUpsertVersionOrdering(t *testing.T) {
	t.Parallel()
	st, _ := storetest.New(t)
	v3 := row(4, 20, 3, 3)
	v3.PaidAt = base.Add(time.Minute)
	if res := mustUpsert(t, st, base, v3); res.Affected != 1 || res.Changed != 1 {
		t.Fatalf("v3 = %+v, want inserted", res)
	}
	if res := mustUpsert(t, st, base, row(4, 20, 2, 2)); res.Affected != 0 || res.Changed != 0 {
		t.Fatalf("v2 after v3 = %+v, want stale", res)
	}
	if got := mustGet(t, st, 4, 20); !sameImage(got, v3) {
		t.Fatalf("row = %+v, want %+v", got, v3)
	}
	v4 := row(4, 20, 4, 5)
	if res := mustUpsert(t, st, base, v4); res.Affected != 2 || res.Changed != 1 {
		t.Fatalf("v4 = %+v, want updated", res)
	}
	if got := mustGet(t, st, 4, 20); !sameImage(got, v4) || !got.PaidAt.IsZero() {
		t.Fatalf("row = %+v, want %+v", got, v4)
	}
	if _, err := st.Get(t.Context(), 4, 21); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get missing = %v, want ErrNotFound", err)
	}
}

// 一条多行语句里混着新插入、被覆盖、版本旧的行：每一行各自按版本比较；受影响行数 = 插入 + 2 × 覆盖，
// Changed 的估计取 min(受影响行数, 行数)，这里同时有覆盖和旧版本，估计偏大 min(覆盖, 旧版本)。
func TestUpsertRowsMixedOutcomes(t *testing.T) {
	t.Parallel()
	st, _ := storetest.New(t)
	const m = 6 // 同一个 slot，同一条语句
	mustUpsert(t, st, base, row(m, 1, 5, 2), row(m, 2, 5, 2), row(m, 3, 5, 2))
	batch := []store.Row{
		row(m, 3, 4, 4), // 旧版本：不变
		row(m, 1, 6, 3), // 覆盖
		row(m, 4, 1, 2), // 插入
		row(m, 2, 5, 9), // 同版本：不变
		row(m, 5, 1, 2), // 插入
	}
	res := mustUpsert(t, st, base.Add(time.Hour), batch...)
	// I=2、U=1、S=2：Affected = 2 + 2×1 = 4，估计 min(4, 5) = 4，真实是 3。
	if want := (store.WriteResult{Rows: 5, Affected: 4, Changed: 4}); res != want {
		t.Errorf("result = %+v, want %+v", res, want)
	}
	for _, want := range []store.Row{row(m, 1, 6, 3), row(m, 2, 5, 2), row(m, 3, 5, 2), row(m, 4, 1, 2), row(m, 5, 1, 2)} {
		if got := mustGet(t, st, m, want.OrderID); !sameImage(got, want) {
			t.Errorf("order %d: row = %+v, want %+v", want.OrderID, got, want)
		}
	}

	// 两种估计精确的情况：没有旧版本（只有插入和覆盖），和没有覆盖（只有插入和旧版本）。
	if res := mustUpsert(t, st, base, row(m, 1, 7, 2), row(m, 6, 1, 2)); res.Changed != 2 || res.Affected != 3 {
		t.Errorf("insert+update: %+v, want changed 2", res)
	}
	if res := mustUpsert(t, st, base, row(m, 1, 1, 2), row(m, 7, 1, 2), row(m, 8, 1, 2)); res.Changed != 2 || res.Affected != 2 {
		t.Errorf("insert+stale: %+v, want changed 2", res)
	}
}

// 行数超过一条语句的上限时切成几条，跨 4 个 slot、2 个库；乱序输入照样写对。
// 同一个订单在一次写入里出现两次是调用方的错误：返回错误，什么都不写。
func TestUpsertRowsSplitsAndRejectsDuplicates(t *testing.T) {
	t.Parallel()
	st, _ := storetest.New(t)
	store.SetStatementRows(st, 3)
	var rows []store.Row
	for m := int64(1); m <= 8; m++ { // 商家 1..8 覆盖 4 个 slot
		for o := range int64(5) {
			rows = append(rows, row(m, 100*m+o, 1, 2))
		}
	}
	rand.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	res := mustUpsert(t, st, base, rows...)
	if want := int64(len(rows)); res.Rows != want || res.Affected != want || res.Changed != want {
		t.Errorf("result = %+v, want %d inserts", res, want)
	}
	for _, r := range rows {
		if got := mustGet(t, st, r.MerchantID, r.OrderID); !sameImage(got, r) {
			t.Errorf("order %d: row = %+v", r.OrderID, got)
		}
	}

	dup := []store.Row{row(2, 900, 1, 2), row(3, 901, 1, 2), row(2, 900, 2, 2)}
	if _, err := st.UpsertRows(t.Context(), dup, base); err == nil {
		t.Fatal("duplicate keys accepted")
	}
	for _, r := range dup {
		if _, err := st.Get(t.Context(), r.MerchantID, r.OrderID); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("order %d written despite the error: %v", r.OrderID, err)
		}
	}
	if res, err := st.UpsertRows(t.Context(), nil, base); err != nil || res != (store.WriteResult{}) {
		t.Errorf("empty write = %+v, %v", res, err)
	}
}

// 两种列表查询都必须走索引：不带状态走主键，带状态走 idx_merchant_status。
func TestListUsesIndex(t *testing.T) {
	t.Parallel()
	st, _ := storetest.New(t)
	for i := range int64(50) {
		mustUpsert(t, st, base, row(5, i+1, 1, int32(2+i%3)), row(9, i+1, 1, 2)) // 9 和 5 在同一个 slot
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
