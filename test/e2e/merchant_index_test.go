package e2e

// 商家维度的订单索引（ADR-0014）：订单库的 binlog 经 CDC 和 orderindex 同步成按商家分片的索引表，
// GET /api/v1/merchants/{merchant_id}/orders 读它。索引是最终一致的，所以这里所有“等索引追上”的地方
// 都是带上限的轮询，失败时打印最后一次看到的内容。
//
// 并行性：用例并行跑、共用一套服务和库，每个用例用自己独有的商家号和 SKU，断言只针对自己的商家。

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

const (
	merchantIndexDSNsEnv = "E2E_MERCHANT_INDEX_DSNS"
	orderDSNsEnv         = "E2E_ORDER_DSNS"

	// indexWait 是等索引追上的上限。正常几百毫秒，留足余量给慢机器。
	indexWait = 10 * time.Second
)

// 商家号以时间为基数，重复运行时不会撞上上一轮留下的索引行。
var nextMerchant atomic.Int64

func init() {
	nextMerchant.Store(time.Now().UnixMilli() % 1_000_000_000 * 1000)
}

func newMerchant() int64 { return nextMerchant.Add(1) }

// newMerchantSKU 插入一个属于 merchant 的 SKU。
func newMerchantSKU(t testing.TB, merchant, available int64) int64 {
	t.Helper()
	id := newSKU(t, available)
	if _, err := inventoryDB.ExecContext(t.Context(),
		"UPDATE sku_stock SET merchant_id = ? WHERE sku_id = ?", merchant, id); err != nil {
		t.Fatalf("set merchant of sku %d: %v", id, err)
	}
	return id
}

// ---------- 列表接口 ----------

type merchantOrder struct {
	OrderID    string `json:"order_id"`
	UserID     string `json:"user_id"`
	Status     string `json:"status"`
	Total      money  `json:"total"`
	CreateTime string `json:"create_time"`
	PayTime    string `json:"pay_time"`
}

type merchantOrderPage struct {
	Orders     []merchantOrder `json:"orders"`
	NextCursor string          `json:"next_cursor"`
}

func listMerchantPage(ctx context.Context, merchant int64, status string, limit int, cursor string) (merchantOrderPage, error) {
	q := url.Values{}
	if status != "" {
		q.Set("status", status)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	path := "/api/v1/merchants/" + strconv.FormatInt(merchant, 10) + "/orders"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	r, err := call(ctx, http.MethodGet, path, 1, "", nil)
	if err != nil {
		return merchantOrderPage{}, fmt.Errorf("list merchant %d orders: %w", merchant, err)
	}
	if r.status != http.StatusOK {
		return merchantOrderPage{}, fmt.Errorf("list merchant %d orders: status %d: %s", merchant, r.status, r.body)
	}
	var p merchantOrderPage
	if err := json.Unmarshal(r.body, &p); err != nil {
		return merchantOrderPage{}, fmt.Errorf("decode merchant %d orders: %w: %s", merchant, err, r.body)
	}
	return p, nil
}

func mustListMerchant(t testing.TB, merchant int64, status string, limit int, cursor string) merchantOrderPage {
	t.Helper()
	p, err := listMerchantPage(t.Context(), merchant, status, limit, cursor)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// waitMerchantOrders 轮询商家的第一页（limit=100），直到 cond 为真，返回最后一次看到的订单（按订单号倒序）。
// 超时时的失败信息里带着最后一次看到的内容。
func waitMerchantOrders(t testing.TB, merchant int64, what string, cond func(orders []merchantOrder) bool) []merchantOrder {
	t.Helper()
	var last []merchantOrder
	var lastErr error
	deadline := time.Now().Add(indexWait)
	for {
		var p merchantOrderPage
		p, lastErr = listMerchantPage(t.Context(), merchant, "", 100, "")
		if lastErr == nil {
			last = p.Orders
			if cond(last) {
				return last
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s; merchant %d last saw %s (last error: %v)",
				indexWait, what, merchant, describeOrders(last), lastErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func describeOrders(orders []merchantOrder) string {
	parts := make([]string, len(orders))
	for i, o := range orders {
		parts[i] = o.OrderID + ":" + o.Status
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func findOrder(orders []merchantOrder, id string) (merchantOrder, bool) {
	i := slices.IndexFunc(orders, func(o merchantOrder) bool { return o.OrderID == id })
	if i < 0 {
		return merchantOrder{}, false
	}
	return orders[i], true
}

func hasOrder(orders []merchantOrder, id string) bool {
	_, ok := findOrder(orders, id)
	return ok
}

func payOrder(t testing.TB, o order, user int64) {
	t.Helper()
	r := mustCall(t, http.MethodPost, "/api/v1/orders/"+o.OrderID+"/pay", user, "", nil)
	if r.status != http.StatusAccepted {
		t.Fatalf("pay %s: status %d: %s", o.OrderID, r.status, r.body)
	}
}

// ---------- 用例 ----------

// 下单、支付、取消之后，商家列表里出现这些订单，状态跟上，字段正确；status 筛选只返回对应状态。
// 同时量出“从下单请求返回到订单出现在列表里”的延迟（设计要求的同步延迟测量）。
func TestMerchantListShowsOrdersAndFollowsStatus(t *testing.T) {
	t.Parallel()
	merchant := newMerchant()
	sku := newMerchantSKU(t, merchant, 20)
	uPaid, uCancel, uPending := newUser(), newUser(), newUser()
	const qty = 2

	// 待支付的订单最晚：支付时限只有几秒，它会在 payTimeout 之后被关闭，列表里先后会看到
	// pending_payment 和 closed。所以最后创建它，并只要求列表里“出现过”待支付。
	paid := mustCreateOrder(t, uPaid, sku, qty)
	cancelled := mustCreateOrder(t, uCancel, sku, qty)
	payOrder(t, paid, uPaid)
	cancelOrder(t, uCancel, cancelled.OrderID)

	id, err := submitOrder(t.Context(), uPending, sku, qty)
	if err != nil {
		t.Fatal(err)
	}
	submitted := time.Now()
	// 不等落库：下面的轮询从请求返回的那一刻开始，订单一进索引（只有预占成功的订单才进）就计时停止。
	pending := order{OrderID: id}

	var seenPending bool
	var latency time.Duration
	orders := waitMerchantOrders(t, merchant, "paid, cancelled and pending orders to show up", func(orders []merchantOrder) bool {
		if hasOrder(orders, pending.OrderID) && !seenPending {
			seenPending = true
			latency = time.Since(submitted)
		}
		p, okP := findOrder(orders, paid.OrderID)
		c, okC := findOrder(orders, cancelled.OrderID)
		return seenPending && okP && okC && p.Status == "paid" && c.Status == "cancelled"
	})
	t.Logf("order submit returned -> first visible in the merchant list: %v (end to end: landing the order + CDC + index write; polled every 50ms)", latency.Round(time.Millisecond))

	if len(orders) != 3 {
		t.Errorf("merchant %d has %d orders in the list, want exactly 3: %s", merchant, len(orders), describeOrders(orders))
	}
	for _, tc := range []struct {
		o    order
		user int64
		want string
	}{{paid, uPaid, "paid"}, {cancelled, uCancel, "cancelled"}} {
		got, _ := findOrder(orders, tc.o.OrderID)
		if got.Status != tc.want {
			t.Errorf("order %s status = %q, want %q", tc.o.OrderID, got.Status, tc.want)
		}
		if got.UserID != strconv.FormatInt(tc.user, 10) {
			t.Errorf("order %s user_id = %q, want %d", tc.o.OrderID, got.UserID, tc.user)
		}
		if want := (money{Amount: qty * unitPrice, Currency: "CNY"}); got.Total != want {
			t.Errorf("order %s total = %+v, want %+v", tc.o.OrderID, got.Total, want)
		}
		if got.CreateTime == "" {
			t.Errorf("order %s has no create_time", tc.o.OrderID)
		}
		if hasPay := got.PayTime != ""; hasPay != (tc.want == "paid") {
			t.Errorf("order %s (%s) pay_time = %q; only paid orders have one", tc.o.OrderID, tc.want, got.PayTime)
		}
	}
	gotPending, _ := findOrder(orders, pending.OrderID)
	if gotPending.Status != "pending_payment" && gotPending.Status != "closed" {
		t.Errorf("unpaid order %s status = %q, want pending_payment (or closed once the deadline passed)", pending.OrderID, gotPending.Status)
	}
	if gotPending.PayTime != "" {
		t.Errorf("unpaid order %s has pay_time %q", pending.OrderID, gotPending.PayTime)
	}

	// 按订单号倒序。
	if !slices.IsSortedFunc(orders, func(a, b merchantOrder) int { return -strings.Compare(padID(a.OrderID), padID(b.OrderID)) }) {
		t.Errorf("orders are not in descending order: %s", describeOrders(orders))
	}

	// status 筛选。
	for _, tc := range []struct {
		status string
		want   []string
	}{{"paid", []string{paid.OrderID}}, {"cancelled", []string{cancelled.OrderID}}} {
		p := mustListMerchant(t, merchant, tc.status, 0, "")
		var got []string
		for _, o := range p.Orders {
			got = append(got, o.OrderID)
			if o.Status != tc.status {
				t.Errorf("status=%s returned order %s with status %s", tc.status, o.OrderID, o.Status)
			}
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("status=%s returned %v, want %v", tc.status, got, tc.want)
		}
	}
}

// padID 把订单号左补零到定长，使字符串比较等价于数值比较。
func padID(s string) string { return strings.Repeat("0", 20-len(s)) + s }

// 不同商家互不可见：各自的列表里只有自己的订单。
func TestMerchantListsAreIsolated(t *testing.T) {
	t.Parallel()
	mA, mB := newMerchant(), newMerchant()
	skuA, skuB := newMerchantSKU(t, mA, 5), newMerchantSKU(t, mB, 5)
	oA := mustCreateOrder(t, newUser(), skuA, 1)
	oB := mustCreateOrder(t, newUser(), skuB, 1)

	waitMerchantOrders(t, mA, "merchant A's order", func(os []merchantOrder) bool { return hasOrder(os, oA.OrderID) })
	waitMerchantOrders(t, mB, "merchant B's order", func(os []merchantOrder) bool { return hasOrder(os, oB.OrderID) })

	if got := mustListMerchant(t, mA, "", 100, "").Orders; len(got) != 1 || got[0].OrderID != oA.OrderID {
		t.Errorf("merchant A sees %s, want only %s", describeOrders(got), oA.OrderID)
	}
	if got := mustListMerchant(t, mB, "", 100, "").Orders; len(got) != 1 || got[0].OrderID != oB.OrderID {
		t.Errorf("merchant B sees %s, want only %s", describeOrders(got), oB.OrderID)
	}
	// 没有订单的商家：空数组，不是错误。
	if got := mustListMerchant(t, newMerchant(), "", 0, "").Orders; len(got) != 0 {
		t.Errorf("merchant without orders sees %s", describeOrders(got))
	}
}

// 分页：limit=2 走完，不重不漏，订单号严格递减，最后一页没有 next_cursor。
func TestMerchantListPagination(t *testing.T) {
	t.Parallel()
	const total, pageSize = 5, 2
	merchant := newMerchant()
	sku := newMerchantSKU(t, merchant, 50)

	ids := make([]string, total)
	var wg sync.WaitGroup
	for i := range total {
		user := newUser()
		wg.Go(func() {
			id, err := submitOrder(t.Context(), user, sku, 1)
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = id
			// 只提交不等落库：waitLanded 会 t.Fatal，不能在这个 goroutine 里调用。
			// 落库由列表出现来保证（只有预占成功的订单才进索引）。
		})
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	waitMerchantOrders(t, merchant, "all orders to show up", func(os []merchantOrder) bool { return len(os) >= total })

	var seen []string
	cursor := ""
	for page := 1; ; page++ {
		if page > total {
			t.Fatalf("still paging after %d pages, seen %v", page-1, seen)
		}
		p := mustListMerchant(t, merchant, "", pageSize, cursor)
		if len(p.Orders) == 0 || len(p.Orders) > pageSize {
			t.Fatalf("page %d has %d orders, want 1..%d", page, len(p.Orders), pageSize)
		}
		for _, o := range p.Orders {
			seen = append(seen, o.OrderID)
		}
		if p.NextCursor == "" {
			if len(seen) != total {
				t.Errorf("last page reached after %d orders, want %d", len(seen), total)
			}
			break
		}
		if len(p.Orders) != pageSize {
			t.Errorf("page %d is short (%d) but has a next_cursor", page, len(p.Orders))
		}
		cursor = p.NextCursor
	}

	if len(slices.Compact(slices.Sorted(slices.Values(seen)))) != len(seen) {
		t.Errorf("duplicate orders across pages: %v", seen)
	}
	want := slices.Clone(ids)
	slices.Sort(want)
	got := slices.Clone(seen)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("paged through %v, submitted %v", got, want)
	}
	for i := 1; i < len(seen); i++ {
		if padID(seen[i-1]) <= padID(seen[i]) {
			t.Errorf("order ids not strictly descending: %v", seen)
			break
		}
	}
}

// 一个订单只属于一个商家：买了两个不同商家的 SKU，请求照常受理，随后订单失败，库存归还，
// 两个商家的列表里都没有它。
func TestMixedMerchantOrderFails(t *testing.T) {
	t.Parallel()
	mA, mB := newMerchant(), newMerchant()
	skuA, skuB := newMerchantSKU(t, mA, 5), newMerchantSKU(t, mB, 5)
	user := newUser()

	body := map[string]any{"items": []map[string]any{
		{"sku_id": strconv.FormatInt(skuA, 10), "quantity": 1},
		{"sku_id": strconv.FormatInt(skuB, 10), "quantity": 2},
	}}
	r := mustCall(t, http.MethodPost, "/api/v1/orders", user, newKey(), body)
	if r.status != http.StatusAccepted {
		t.Fatalf("create mixed order: status %d: %s", r.status, r.body)
	}
	var accepted order
	r.decode(t, &accepted)
	if accepted.Status != "creating" {
		t.Fatalf("accepted order status = %q, want creating", accepted.Status)
	}

	eventually(t, 10*time.Second, "mixed-merchant order to fail", func() bool {
		o := getOrder(t, user, accepted.OrderID)
		if o.Status == "pending_payment" || o.Status == "paid" {
			t.Fatalf("mixed-merchant order became %q, want failed", o.Status)
		}
		return o.Status == "failed"
	})
	wantStock(t, skuA, stock{Available: 5})
	wantStock(t, skuB, stock{Available: 5})

	// 失败原因 GetOrder 没有返回；有订单库的连接时直接读库断言，订单也没有写入商家。
	if dbs := openOrderDBs(t); dbs != nil {
		id, _ := strconv.ParseInt(accepted.OrderID, 10, 64)
		var reason sql.NullString
		var merchant sql.NullInt64
		table := orderTable(t, user)
		err := dbs.forUser(user).QueryRowContext(t.Context(),
			"SELECT fail_reason, merchant_id FROM "+table+" WHERE order_id = ?", id).Scan(&reason, &merchant)
		if err != nil {
			t.Fatalf("read %s of order %s: %v", table, accepted.OrderID, err)
		}
		if reason.String != "MIXED_MERCHANTS" {
			t.Errorf("fail_reason = %q, want MIXED_MERCHANTS", reason.String)
		}
		if merchant.Valid {
			t.Errorf("failed order has merchant_id %d, want NULL", merchant.Int64)
		}
	}

	// 两个商家的列表里都不会出现它。给索引一小段时间（正常几百毫秒就跟上）后仍然没有。
	deadline := time.Now().Add(1500 * time.Millisecond)
	for {
		for _, m := range []int64{mA, mB} {
			if got := mustListMerchant(t, m, "", 100, "").Orders; len(got) != 0 {
				t.Fatalf("merchant %d lists %s, want none (the order failed)", m, describeOrders(got))
			}
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 索引与订单表一致：等索引追上后，该商家在各 orders_N 里的订单与 merchant_orders_{slot} 里的行
// 订单号集合相同，且 status、total_amount、version == src_version 都相等。
func TestMerchantIndexMatchesOrderTables(t *testing.T) {
	t.Parallel()
	orderDBs := openOrderDBs(t)
	indexDBs := openMerchantIndexDBs(t)
	if orderDBs == nil || indexDBs == nil {
		t.Skipf("%s and %s are required; run via `make e2e`", orderDSNsEnv, merchantIndexDSNsEnv)
	}
	merchant := newMerchant()
	sku := newMerchantSKU(t, merchant, 20)
	uPaid, uCancel, uClose, uOpen := newUser(), newUser(), newUser(), newUser()

	paid := mustCreateOrder(t, uPaid, sku, 1)
	cancelled := mustCreateOrder(t, uCancel, sku, 2)
	closing := mustCreateOrder(t, uClose, sku, 3)
	open := mustCreateOrder(t, uOpen, sku, 1) // 同样会在支付时限后被关闭
	payOrder(t, paid, uPaid)
	cancelOrder(t, uCancel, cancelled.OrderID)

	// 先等订单本身落定：已支付、已取消，其余两个等超时关闭。之后订单表不再变化，索引只需要追上。
	eventually(t, 10*time.Second, "paid order to be paid", func() bool { return getOrder(t, uPaid, paid.OrderID).Status == "paid" })
	for _, c := range []struct {
		o    order
		user int64
	}{{closing, uClose}, {open, uOpen}} {
		eventually(t, payTimeout+10*time.Second, "order "+c.o.OrderID+" to be closed", func() bool {
			return getOrder(t, c.user, c.o.OrderID).Status == "closed"
		})
	}

	var diff string
	deadline := time.Now().Add(indexWait + 5*time.Second)
	for {
		var err error
		diff, err = compareIndex(t.Context(), orderDBs, indexDBs, merchant, 4)
		if err != nil {
			t.Fatal(err)
		}
		if diff == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("index did not catch up with the order tables for merchant %d: %s", merchant, diff)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ---------- 直接读库 ----------

type rowKey struct {
	status  int
	total   int64
	version int64
}

// compareIndex 读出商家在订单库和索引库里的全部行并比较。wantOrders 是订单侧应有的订单数；
// 订单侧数量不对（还在落库）时也算没追上，返回描述。一致时返回空串。
func compareIndex(ctx context.Context, orders *orderShards, idx []*sql.DB, merchant int64, wantOrders int) (string, error) {
	left := map[int64]rowKey{}
	for db, conn := range orders.dbs {
		for _, slot := range orders.router.SlotsOf(db) {
			if err := readRows(ctx, conn, sharding.Table("orders", slot), "version", merchant, left); err != nil {
				return "", fmt.Errorf("order db %d: %w", db, err)
			}
		}
	}

	ir, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil {
		return "", fmt.Errorf("build merchant index router: %w", err)
	}
	slot := ir.ForMerchant(merchant)
	if len(idx) != ir.DBs() {
		return "", fmt.Errorf("%s has %d DSNs, want %d", merchantIndexDSNsEnv, len(idx), ir.DBs())
	}
	table := sharding.Table("merchant_orders", slot)
	right := map[int64]rowKey{}
	if err := readRows(ctx, idx[ir.DB(slot)], table, "src_version", merchant, right); err != nil {
		return "", fmt.Errorf("index db %d: %w", ir.DB(slot), err)
	}

	var problems []string
	if len(left) != wantOrders {
		problems = append(problems, fmt.Sprintf("order tables have %d orders with this merchant, want %d", len(left), wantOrders))
	}
	for id, l := range left {
		r, ok := right[id]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("order %d {%+v} missing from %s", id, l, table))
		case l != r:
			problems = append(problems, fmt.Sprintf("order %d: orders %+v, index %+v", id, l, r))
		}
	}
	for id, r := range right {
		if _, ok := left[id]; !ok {
			problems = append(problems, fmt.Sprintf("order %d {%+v} only in %s", id, r, table))
		}
	}
	slices.Sort(problems)
	return strings.Join(problems, "; "), nil
}

// readRows 把 table 里属于 merchant 的行读进 into。versionCol 是版本号列的名字（orders.version 或 src_version）。
func readRows(ctx context.Context, db *sql.DB, table, versionCol string, merchant int64, into map[int64]rowKey) error {
	rows, err := db.QueryContext(ctx,
		"SELECT order_id, status, total_amount, "+versionCol+" FROM "+table+" WHERE merchant_id = ?", merchant)
	if err != nil {
		return fmt.Errorf("query %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var k rowKey
		if err := rows.Scan(&id, &k.status, &k.total, &k.version); err != nil {
			return fmt.Errorf("scan %s: %w", table, err)
		}
		into[id] = k
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s: %w", table, err)
	}
	return nil
}

// orderShards 是订单库的两个连接和它们的分片布局。
type orderShards struct {
	dbs    []*sql.DB
	router *sharding.Router
}

func (o *orderShards) forUser(user int64) *sql.DB { return o.dbs[o.router.DB(o.router.ForUser(user))] }

func orderTable(t testing.TB, user int64) string {
	t.Helper()
	r, err := sharding.New(sharding.OrderLayout())
	if err != nil {
		t.Fatal(err)
	}
	return sharding.Table("orders", r.ForUser(user))
}

// openOrderDBs 连上 E2E_ORDER_DSNS 的两个订单库；变量没设置时返回 nil。
func openOrderDBs(t testing.TB) *orderShards {
	t.Helper()
	dsns := splitDSNs(os.Getenv(orderDSNsEnv))
	if len(dsns) == 0 {
		return nil
	}
	r, err := sharding.New(sharding.OrderLayout())
	if err != nil {
		t.Fatal(err)
	}
	if len(dsns) != r.DBs() {
		t.Fatalf("%s has %d DSNs, want %d", orderDSNsEnv, len(dsns), r.DBs())
	}
	return &orderShards{dbs: openAll(t, orderDSNsEnv, dsns), router: r}
}

// openMerchantIndexDBs 连上 E2E_MERCHANT_INDEX_DSNS 的两个索引库；变量没设置时返回 nil。
func openMerchantIndexDBs(t testing.TB) []*sql.DB {
	t.Helper()
	dsns := splitDSNs(os.Getenv(merchantIndexDSNsEnv))
	if len(dsns) == 0 {
		return nil
	}
	return openAll(t, merchantIndexDSNsEnv, dsns)
}

func openAll(t testing.TB, name string, dsns []string) []*sql.DB {
	t.Helper()
	dbs := make([]*sql.DB, 0, len(dsns))
	for i, dsn := range dsns {
		db, err := mysqlx.Open(t.Context(), mysqlx.Config{DSN: dsn, MaxOpenConns: 2})
		if err != nil {
			t.Fatalf("connect %s[%d]: %v", name, i, err)
		}
		t.Cleanup(func() { _ = db.Close() })
		dbs = append(dbs, db)
	}
	return dbs
}

func splitDSNs(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
