package store_test

import (
	"database/sql"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

var keySeq atomic.Int64

// insertN 给 userID 插入 n 个订单，返回订单号（从旧到新）。status 为 nil 时都是 CREATING。
func (f *fixture) insertN(t *testing.T, userID int64, n int, status func(i int) orderv1.OrderStatus) []int64 {
	t.Helper()
	ids := make([]int64, 0, n)
	for i := range n {
		o := f.newOrder(t, userID, "list-"+strconv.FormatInt(keySeq.Add(1), 10))
		if status != nil {
			o.Status = status(i)
		}
		if err := f.store.InsertOrder(t.Context(), o); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, o.OrderID)
	}
	return ids
}

// listAll 按 pageSize 一页一页翻到底，返回依次拿到的订单号。
func (f *fixture) listAll(t *testing.T, q store.UserOrdersQuery) []int64 {
	t.Helper()
	var got []int64
	for range 1000 {
		page, more, err := f.store.ListUserOrders(t.Context(), q, store.Primary)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) > q.Limit {
			t.Fatalf("page has %d rows, limit %d", len(page), q.Limit)
		}
		for _, o := range page {
			got = append(got, o.OrderID)
		}
		if !more {
			return got
		}
		if len(page) == 0 {
			t.Fatal("more is true but the page is empty")
		}
		q.BeforeOrderID = page[len(page)-1].OrderID
	}
	t.Fatal("paging did not terminate")
	return nil
}

func reversed(ids []int64) []int64 {
	out := slices.Clone(ids)
	slices.Reverse(out)
	return out
}

// 两个落在同一个 slot 的用户（user_id 相差 slot 数），各自翻页只看到自己的订单，不重不漏，从新到旧。
func TestListUserOrdersPaging(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const alice, bob = 3, 3 + 8
	if f.router.ForUser(alice) != f.router.ForUser(bob) {
		t.Fatal("test users must share a slot")
	}
	aliceIDs := f.insertN(t, alice, 7, nil)
	bobIDs := f.insertN(t, bob, 4, nil)

	for _, size := range []int{1, 2, 3, 7, 8, 100} {
		if got := f.listAll(t, store.UserOrdersQuery{UserID: alice, Limit: size}); !slices.Equal(got, reversed(aliceIDs)) {
			t.Errorf("alice, page size %d: got %v, want %v", size, got, reversed(aliceIDs))
		}
	}
	if got := f.listAll(t, store.UserOrdersQuery{UserID: bob, Limit: 3}); !slices.Equal(got, reversed(bobIDs)) {
		t.Errorf("bob: got %v, want %v", got, reversed(bobIDs))
	}

	// 恰好取完时 more 为 false，不会多给一个空的下一页。
	page, more, err := f.store.ListUserOrders(t.Context(), store.UserOrdersQuery{UserID: bob, Limit: 4}, store.Primary)
	if err != nil || len(page) != 4 || more {
		t.Errorf("exact page: %d rows, more %v, err %v", len(page), more, err)
	}

	// 只读列表需要的列，值与写入的一致。
	o := page[0]
	if o.OrderID != bobIDs[3] || o.Status != orderv1.OrderStatus_ORDER_STATUS_CREATING || o.Currency != "CNY" ||
		o.CreatedAt.IsZero() || o.ExpireAt.Sub(o.CreatedAt) != 15*time.Minute || !o.PaidAt.IsZero() {
		t.Errorf("summary = %+v", o)
	}
}

// 翻页中途插入新订单：新订单号更大，排在已翻过的前面，不会挤进后面的页，后面的页不重不漏。
func TestListUserOrdersInsertWhilePaging(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const user = 5
	ids := f.insertN(t, user, 6, nil)

	q := store.UserOrdersQuery{UserID: user, Limit: 2}
	var got []int64
	for i := 0; ; i++ {
		page, more, err := f.store.ListUserOrders(t.Context(), q, store.Primary)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page {
			got = append(got, o.OrderID)
		}
		if i == 0 {
			f.insertN(t, user, 3, nil)
		}
		if !more {
			break
		}
		q.BeforeOrderID = page[len(page)-1].OrderID
	}
	if !slices.Equal(got, reversed(ids)) {
		t.Errorf("got %v, want %v", got, reversed(ids))
	}
	// 从头再翻能看到新插入的。
	if all := f.listAll(t, store.UserOrdersQuery{UserID: user, Limit: 4}); len(all) != 9 || !slices.Equal(all[3:], reversed(ids)) {
		t.Errorf("after insert: %v", all)
	}
}

func TestListUserOrdersStatusFilter(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const user, other = 6, 6 + 8
	statuses := []orderv1.OrderStatus{
		orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT,
		orderv1.OrderStatus_ORDER_STATUS_PAID,
		orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT,
		orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
		orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT,
	}
	ids := f.insertN(t, user, len(statuses), func(i int) orderv1.OrderStatus { return statuses[i] })
	// 同 slot 的另一个用户也有待支付的订单，不能被筛进来。
	f.insertN(t, other, 2, func(int) orderv1.OrderStatus { return orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT })

	got := f.listAll(t, store.UserOrdersQuery{UserID: user, Status: orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT, Limit: 2})
	if want := []int64{ids[4], ids[2], ids[0]}; !slices.Equal(got, want) {
		t.Errorf("pending: got %v, want %v", got, want)
	}
	got = f.listAll(t, store.UserOrdersQuery{UserID: user, Status: orderv1.OrderStatus_ORDER_STATUS_PAID, Limit: 2})
	if want := []int64{ids[1]}; !slices.Equal(got, want) {
		t.Errorf("paid: got %v, want %v", got, want)
	}
	got = f.listAll(t, store.UserOrdersQuery{UserID: user, Status: orderv1.OrderStatus_ORDER_STATUS_CLOSED, Limit: 2})
	if len(got) != 0 {
		t.Errorf("closed: got %v", got)
	}
}

func TestListUserOrdersEmpty(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.insertN(t, 2+8, 2, nil) // 同 slot 的别人有订单
	page, more, err := f.store.ListUserOrders(t.Context(), store.UserOrdersQuery{UserID: 2, Limit: 20}, store.Primary)
	if err != nil || len(page) != 0 || more {
		t.Errorf("got %d rows, more %v, err %v", len(page), more, err)
	}
	// 游标比所有订单都小：空页。
	page, more, err = f.store.ListUserOrders(t.Context(), store.UserOrdersQuery{UserID: 2 + 8, Limit: 20, BeforeOrderID: 1}, store.Primary)
	if err != nil || len(page) != 0 || more {
		t.Errorf("before 1: got %d rows, more %v, err %v", len(page), more, err)
	}
	for _, q := range []store.UserOrdersQuery{{UserID: 0, Limit: 1}, {UserID: 1, Limit: 0}} {
		if _, _, err := f.store.ListUserOrders(t.Context(), q, store.Primary); err == nil {
			t.Errorf("%+v: want error", q)
		}
	}
}

// 每种条件组合都走 idx_orders_user，并且不排序（索引本身就是要的顺序）。
// 带状态筛选时是在索引范围里回表过滤，而不是改用 (status, expire_at)。
func TestListUserOrdersUsesUserIndex(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const user = 7
	// 让 (status, expire_at) 看起来比按用户更有选择性：别的用户有大量其他状态的订单。
	f.insertN(t, user, 3, nil)
	f.insertN(t, user+8, 30, nil)
	slot := f.router.ForUser(user)
	db := f.dbs[f.router.DB(slot)]
	if _, err := db.ExecContext(t.Context(), "ANALYZE TABLE "+sharding.Table("orders", slot)); err != nil {
		t.Fatal(err)
	}
	for _, q := range []store.UserOrdersQuery{
		{UserID: user, Limit: 20},
		{UserID: user, Limit: 20, BeforeOrderID: 1 << 62},
		{UserID: user, Limit: 20, Status: orderv1.OrderStatus_ORDER_STATUS_PAID},
		{UserID: user, Limit: 20, Status: orderv1.OrderStatus_ORDER_STATUS_PAID, BeforeOrderID: 1 << 62},
	} {
		query, args := store.UserOrdersSQL(sharding.Table("orders", slot), q)
		key, extra := explain(t, db, query, args...)
		if key != "idx_orders_user" {
			t.Errorf("%+v: key = %q, want idx_orders_user", q, key)
		}
		if strings.Contains(extra, "filesort") {
			t.Errorf("%+v: extra = %q, must not sort", q, extra)
		}
	}
}

// explain 返回单表查询 EXPLAIN 的 key 和 Extra 列。
func explain(t *testing.T, db *sql.DB, query string, args ...any) (key, extra string) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "EXPLAIN FORMAT=TRADITIONAL "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("EXPLAIN returned no rows")
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatal(err)
	}
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "key":
			key = vals[i].String
		case "extra":
			extra = vals[i].String
		}
	}
	if rows.Next() {
		t.Fatal("EXPLAIN returned more than one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return key, extra
}
