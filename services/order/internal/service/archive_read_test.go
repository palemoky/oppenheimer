package service

import (
	"cmp"
	"database/sql"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/services/order/internal/archive"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 本文件测试已归档订单的读路径：GetOrder 和 ListUserOrders 在热库没有时查冷库（设计文档 1c 第 6.6 节）。

// coldName 返回冷库的库名。
func (e *env) coldName(t *testing.T) string {
	t.Helper()
	var name string
	if err := e.coldDB.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&name); err != nil {
		t.Fatal(err)
	}
	return name
}

// copyToCold 只做归档任务的前半步：把订单三张表的行写进冷库，不删热库。
// 之后订单处于“已写冷、未删热”的状态，两边内容相同。
func (e *env) copyToCold(t *testing.T, orderID int64) {
	t.Helper()
	cold := e.coldName(t)
	hot := e.dbOf(orderID)
	const orderCols = `order_id, user_id, merchant_id, version, status, total_amount, currency, idempotency_key, request_hash,
		payment_id, fail_reason, expire_at, paid_at, created_at, updated_at`
	const itemCols = "order_id, sku_id, quantity, unit_price, created_at"
	const payCols = "order_id, payment_id, succeeded, amount, disposition, created_at"
	for _, q := range []string{
		"INSERT INTO `" + cold + "`.orders_archive (" + orderCols + ") SELECT " + orderCols + " FROM " + e.table("orders", orderID) + " WHERE order_id = ?",
		"INSERT INTO `" + cold + "`.order_items_archive (" + itemCols + ") SELECT " + itemCols + " FROM " + e.table("order_items", orderID) + " WHERE order_id = ?",
		"INSERT INTO `" + cold + "`.payment_results_archive (" + payCols + ") SELECT " + payCols + " FROM " + e.table("payment_results", orderID) + " WHERE order_id = ?",
	} {
		if _, err := hot.ExecContext(t.Context(), q, orderID); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// dropHot 做归档任务的后半步：从热库删掉订单的三张表的行（冷库里应当已经有了）。
func (e *env) dropHot(t *testing.T, orderID int64) {
	t.Helper()
	for _, base := range []string{"payment_results", "order_items", "orders"} {
		if _, err := e.dbOf(orderID).ExecContext(t.Context(), "DELETE FROM "+e.table(base, orderID)+" WHERE order_id = ?", orderID); err != nil {
			t.Fatal(err)
		}
	}
}

// ageTo 把服务时钟拨到恰好让订单号 firstNew 还算“热”、比它早至少一毫秒的订单号都算“可能已归档”的时刻。
func (e *env) ageTo(t *testing.T, firstNew int64) {
	t.Helper()
	e.clock.Advance(idgen.Time(firstNew).Add(archive.MinHotAge).Sub(e.clock.Now()))
	if now := e.clock.Now(); archive.MaybeCold(firstNew, now) || !archive.MaybeCold(idgen.LowerBound(idgen.Time(firstNew).Add(-time.Millisecond)), now) {
		t.Fatal("test setup: clock is not between the old and the new orders")
	}
}

// nextMilli 等到真实时钟走过订单号 id 所在的毫秒，保证之后生成的订单号的时间严格更晚。
func nextMilli(id int64) {
	for !time.Now().After(idgen.Time(id).Add(time.Millisecond)) {
		time.Sleep(time.Millisecond)
	}
}

// coldService 返回与 e.svc 共用热库依赖的 Service，冷库用 cold（可以为空），withCache 时带查询缓存。
// 读模式、从库、标记与 e.svc 相同。
func (e *env) coldService(t *testing.T, cold *store.Cold, withCache bool) *Service {
	t.Helper()
	d := Deps{
		Store: e.store, Gate: e.gate, Sender: e.sender, Inventory: e.inv,
		Logger: e.svc.logger, Marks: e.marks, Cold: cold,
	}
	if withCache {
		c, err := NewOrderCache(e.rdb, e.prefix, e.svc.logger)
		if err != nil {
			t.Fatal(err)
		}
		d.OrderCache = c
	}
	return mustNew(t, d, Config{Clock: e.clock.Now, OrderEventsTopic: testEventsTopic, Relays: e.kickers(), ReadMode: e.svc.readMode})
}

// mustPaid 下一个订单并付清，返回付清后的订单。
func (e *env) mustPaid(t *testing.T, userID int64, key string, paymentID int64) *orderv1.Order {
	t.Helper()
	o := e.mustPending(t, userID, key)
	got, err := e.pay(t, o.GetOrderId(), paymentID, o.GetTotal().GetAmount(), true)
	wantReply(t, got, err, accepted, "")
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PAID)
	return toProto(e.load(t, o.GetOrderId()))
}

func wantSameOrder(t *testing.T, what string, got, want *orderv1.Order) {
	t.Helper()
	if !proto.Equal(got, want) {
		t.Errorf("%s:\n got  %v\n want %v", what, got, want)
	}
}

// 归档前、已写冷未删热、归档后，GetOrder 返回的内容完全相同；带缓存时也一样，而且已归档的订单进缓存。
func TestGetOrderArchived(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	paid := e.mustPaid(t, 1, "paid", 7)
	cancelled := e.mustPending(t, 1, "cancelled")
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(cancelled.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	e.consumeEvents(t) // 库存服务结清、归还占用：已归档的订单早就没有占用了
	cached := e.coldService(t, store.NewCold(e.coldDB), true)

	for _, id := range []int64{paid.GetOrderId(), cancelled.GetOrderId()} {
		before, err := getOrder(t, e.svc, id, 1)
		if err != nil {
			t.Fatal(err)
		}
		if id == paid.GetOrderId() && before.GetPayTime() == nil {
			t.Fatal("paid order has no pay_time")
		}
		e.age(t)
		e.copyToCold(t, id)
		got, err := getOrder(t, e.svc, id, 1)
		if err != nil {
			t.Fatalf("written to cold, still hot: %v", err)
		}
		wantSameOrder(t, "written to cold, still hot", got, before)

		e.dropHot(t, id)
		for i, svc := range []*Service{e.svc, cached, cached} {
			got, err := getOrder(t, svc, id, 1)
			if err != nil {
				t.Fatalf("archived, read %d: %v", i, err)
			}
			wantSameOrder(t, "archived", got, before)
		}
		if _, ok := e.cacheEntry(t, id); !ok {
			t.Errorf("archived order %d is terminal but was not cached", id)
		}
	}
	// 已归档的已支付订单不可取消：与热库里的终态订单一样答 FAILED_PRECONDITION，而不是不存在。
	_, err := e.svc.CancelOrder(t.Context(), cancelReq(paid.GetOrderId(), 1))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderNotCancellable)
	e.wantNoHotRows(t)
}

// 别人的已归档订单、冷热两边都没有的旧订单号：都是 NOT_FOUND。
// CancelOrder 对已归档的订单与热库里的终态订单作答相同（取消已取消的成功，别人的不存在）。
func TestGetOrderArchivedNotFound(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	o := e.mustPending(t, 1, "k")
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	e.consumeEvents(t)
	e.age(t)
	e.moveToCold(t, o.GetOrderId())
	cached := e.coldService(t, store.NewCold(e.coldDB), true)

	oldID := idgen.LowerBound(e.clock.Now().Add(-2*archive.MinHotAge)) | 1
	for _, svc := range []*Service{e.svc, cached} {
		_, err := getOrder(t, svc, o.GetOrderId(), 2)
		wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
		_, err = getOrder(t, svc, oldID, 1)
		wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	}
	// 主人自己仍然查得到（上面别人的查询没有把它写成不存在）。
	mustGetStatus(t, cached, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)

	// 再取消一个已归档的已取消订单：与热库里一样按幂等成功作答，返回冷库里的订单。
	resp, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1))
	if err != nil || resp.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
		t.Fatalf("cancel archived cancelled order: %v, %v", resp, err)
	}
	_, err = e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 2))
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	e.wantNoHotRows(t)
}

// 冷库不可用时：新订单号查不到仍是 NOT_FOUND（根本不碰冷库）；旧订单号返回内部错误，不缓存成“不存在”，
// 冷库恢复后查得到。
func TestGetOrderColdStoreDown(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	o := e.mustPending(t, 1, "k")
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	e.consumeEvents(t)
	e.age(t)
	e.moveToCold(t, o.GetOrderId())
	healthy := store.NewCold(reopen(t, e.coldDB))
	if err := e.coldDB.Close(); err != nil {
		t.Fatal(err)
	}
	down := e.coldService(t, store.NewCold(e.coldDB), true)

	newID := idgen.LowerBound(e.clock.Now()) | 1
	if archive.MaybeCold(newID, e.clock.Now()) {
		t.Fatal("test setup: new id is old enough to be archived")
	}
	for _, svc := range []*Service{e.svc, down} {
		_, err := getOrder(t, svc, newID, 1)
		wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)

		_, err = getOrder(t, svc, o.GetOrderId(), 1)
		if errx.Code(err) != codes.Internal {
			t.Fatalf("archived order with the cold store down: err = %v, want Internal", err)
		}
	}
	if _, ok := e.cacheEntry(t, o.GetOrderId()); ok {
		t.Fatal("cold store error was cached")
	}

	recovered := e.coldService(t, healthy, true)
	mustGetStatus(t, recovered, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
}

// 读模式允许读从库、从库完全落后（空库）时，已归档的订单也查得到：从库没有 → 主库确认没有 → 冷库。
func TestGetOrderArchivedWithReplica(t *testing.T) {
	t.Parallel()
	for _, mode := range []ReadMode{ReadWindow, ReadGTID} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: mode, cold: true})
			o := e.mustPending(t, 1, "k")
			id := o.GetOrderId()
			if _, err := e.svc.CancelOrder(t.Context(), cancelReq(id, 1)); err != nil {
				t.Fatal(err)
			}
			e.consumeEvents(t) // 归还占用：已归档的订单早就没有占用了
			want := mustGetStatus(t, e.svc, id, 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
			e.dropMark(t, "pin", id)
			e.dropMark(t, "gtid", id)
			e.age(t)
			e.moveToCold(t, id)

			cached := e.coldService(t, store.NewCold(e.coldDB), true)
			for _, svc := range []*Service{e.svc, cached, cached} {
				got := mustGetStatus(t, svc, id, 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
				wantSameOrder(t, "archived via replica path", got, want)
			}
		})
	}
}

// ---------- ListUserOrders ----------

// listPage 是一次 ListUserOrders 的结果：订单号和下一页游标。
type listPage struct {
	ids  []int64
	next int64
}

// listPages 从 req 开始翻到底，返回每一页。
func listPages(t *testing.T, svc *Service, req *orderv1.ListUserOrdersRequest) []listPage {
	t.Helper()
	var pages []listPage
	for range 100 {
		resp, err := svc.ListUserOrders(t.Context(), req)
		if err != nil {
			t.Fatalf("ListUserOrders(before %d): %v", req.GetBeforeOrderId(), err)
		}
		var p listPage
		for _, o := range resp.GetOrders() {
			p.ids = append(p.ids, o.GetOrderId())
		}
		p.next = resp.GetNextBeforeOrderId()
		pages = append(pages, p)
		if p.next == 0 {
			return pages
		}
		req.BeforeOrderId = p.next
	}
	t.Fatal("paging did not terminate")
	return nil
}

// wantPaging 检查用页大小 size 翻页的结果正好是 want（按订单号倒序）：不重不漏、严格递减，
// 中间每页都满且有下一页，最后一页没有下一页，页数恰好是 ceil(len(want)/size)（至少一页）。
func wantPaging(t *testing.T, pages []listPage, want []int64, size int) {
	t.Helper()
	var got []int64
	for i, p := range pages {
		got = append(got, p.ids...)
		last := i == len(pages)-1
		switch {
		case last && p.next != 0:
			t.Errorf("last page %d has next %d", i, p.next)
		case !last && len(p.ids) != size:
			t.Errorf("page %d has %d orders, want a full page of %d", i, len(p.ids), size)
		case !last && p.next != p.ids[len(p.ids)-1]:
			t.Errorf("page %d next = %d, want its last order %d", i, p.next, p.ids[len(p.ids)-1])
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("size %d: got %v, want %v", size, got, want)
	}
	if n := max(1, (len(want)+size-1)/size); len(pages) != n {
		t.Errorf("size %d: %d pages, want %d", size, len(pages), n)
	}
}

// unionIDs 直接查热库和冷库，返回用户 userID（status 不为 UNSPECIFIED 时只看这个状态）的订单号的并集，按订单号倒序。
func (e *env) unionIDs(t *testing.T, userID int64, status orderv1.OrderStatus) []int64 {
	t.Helper()
	collect := func(db *sql.DB, table string) []int64 {
		q := "SELECT order_id FROM " + table + " WHERE user_id = ?"
		args := []any{userID}
		if status != orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED {
			q += " AND status = ?"
			args = append(args, int32(status))
		}
		rows, err := db.QueryContext(t.Context(), q, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return ids
	}
	slot := e.router.ForUser(userID)
	ids := slices.Concat(
		collect(e.dbs[e.router.DB(slot)], sharding.Table("orders", slot)),
		collect(e.coldDB, "orders_archive"),
	)
	slices.SortFunc(ids, func(a, b int64) int { return cmp.Compare(b, a) })
	return slices.Compact(ids)
}

// 热库、冷库、两边都有、卡在热库里的旧订单混在一起：任何页大小都精确地翻完“热 ∪ 冷”。
func TestListUserOrdersWithArchive(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	const user, neighbor = 4, 4 + 8 // 同一个 slot
	cancel := func(id int64) {
		t.Helper()
		if _, err := e.svc.CancelOrder(t.Context(), cancelReq(id, user)); err != nil {
			t.Fatal(err)
		}
	}
	// 旧的一批：old[0..3] 取消（终态），old[4] 留在待支付（卡住的旧订单，永远留在热库）。
	var old []int64
	for i := range 5 {
		old = append(old, e.mustPending(t, user, "old"+string(rune('a'+i))).GetOrderId())
	}
	for _, id := range old[:4] {
		cancel(id)
	}
	other := e.mustPending(t, neighbor, "n").GetOrderId()
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(other, neighbor)); err != nil {
		t.Fatal(err)
	}
	nextMilli(max(old[len(old)-1], other))
	// 新的一批：fresh[1]、fresh[3] 取消。
	var fresh []int64
	for i := range 4 {
		fresh = append(fresh, e.mustPending(t, user, "new"+string(rune('a'+i))).GetOrderId())
	}
	cancel(fresh[1])
	cancel(fresh[3])
	e.consumeEvents(t)

	e.ageTo(t, fresh[0])
	e.moveToCold(t, old[0])
	e.moveToCold(t, old[2])
	e.copyToCold(t, old[1]) // 已写冷、未删热
	e.copyToCold(t, old[3])
	e.moveToCold(t, other) // 邻居的已归档订单不能出现在 user 的列表里
	// old[4] 是旧的非终态订单，留在热库。

	all := e.unionIDs(t, user, orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED)
	if len(all) != 9 || slices.Contains(all, other) {
		t.Fatalf("test setup: union = %v", all)
	}
	for _, size := range []int{1, 2, 3, 4, 9, 10, MaxListPageSize} {
		wantPaging(t, listPages(t, e.svc, listReq(user, int32(size), 0)), all, size)
	}

	const cancelled = orderv1.OrderStatus_ORDER_STATUS_CANCELLED
	want := e.unionIDs(t, user, cancelled)
	if len(want) != 6 {
		t.Fatalf("test setup: cancelled union = %v", want)
	}
	for _, size := range []int{1, 2, 3, 6, 7} {
		req := &orderv1.ListUserOrdersRequest{UserId: user, PageSize: int32(size), Status: cancelled}
		wantPaging(t, listPages(t, e.svc, req), want, size)
	}

	// 归档订单的摘要与热库里时的一样（这里比较两边都有的那一条和只在冷库的那一条，都来自同一份数据）。
	resp, err := e.svc.ListUserOrders(t.Context(), listReq(user, MaxListPageSize, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range resp.GetOrders() {
		if s.GetOrderId() != old[0] && s.GetOrderId() != old[1] {
			continue
		}
		o := mustGetStatus(t, e.svc, s.GetOrderId(), user, cancelled)
		if s.GetStatus() != o.GetStatus() || !proto.Equal(s.GetTotal(), o.GetTotal()) ||
			!proto.Equal(s.GetCreateTime(), o.GetCreateTime()) || !proto.Equal(s.GetExpireTime(), o.GetExpireTime()) {
			t.Errorf("summary %v does not match order %v", s, o)
		}
	}
}

// 热库里新订单超过一页时，第一页只走热库：冷库不可用也成功。翻到要看冷库的那一页时返回错误，
// 而不是悄悄只返回热库的部分。
func TestListUserOrdersSkipsColdForFreshPage(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	for i := range 3 {
		e.mustPending(t, 1, "k"+string(rune('a'+i)))
	}
	if err := e.coldDB.Close(); err != nil {
		t.Fatal(err)
	}
	resp, err := e.svc.ListUserOrders(t.Context(), listReq(1, 2, 0))
	if err != nil {
		t.Fatalf("first page with the cold store down: %v", err)
	}
	if len(resp.GetOrders()) != 2 || resp.GetNextBeforeOrderId() == 0 {
		t.Fatalf("first page = %v, want 2 orders and a next page", resp)
	}
	// 第二页热库只剩一条，没有满页：后面有没有更多要看冷库。
	_, err = e.svc.ListUserOrders(t.Context(), listReq(1, 2, resp.GetNextBeforeOrderId()))
	if errx.Code(err) != codes.Internal {
		t.Fatalf("second page with the cold store down: err = %v, want Internal", err)
	}
	// 页大小超过热库的订单数，第一页就要看冷库。
	_, err = e.svc.ListUserOrders(t.Context(), listReq(1, 5, 0))
	if errx.Code(err) != codes.Internal {
		t.Fatalf("page larger than the hot orders with the cold store down: err = %v, want Internal", err)
	}
}

// 热库刚好一页、最后一条还是新订单，而冷库还有：不能因为最后一条是新的就跳过冷库，
// 第一页 more 为真，第二页拿到冷库的订单。
func TestListUserOrdersHotExactlyOnePage(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	old := e.mustPending(t, 1, "old").GetOrderId()
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(old, 1)); err != nil {
		t.Fatal(err)
	}
	e.consumeEvents(t)
	nextMilli(old)
	a := e.mustPending(t, 1, "a").GetOrderId()
	b := e.mustPending(t, 1, "b").GetOrderId()
	e.ageTo(t, a)
	e.moveToCold(t, old)

	pages := listPages(t, e.svc, listReq(1, 2, 0))
	wantPaging(t, pages, []int64{b, a, old}, 2)
}

// 没有配置冷库时，热库里没有的旧订单号就是不存在，列表只有热库：行为与归档功能之前相同。
func TestNoColdStoreIgnoresArchive(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	o := e.mustPending(t, 1, "k")
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	e.consumeEvents(t)
	e.age(t)
	e.moveToCold(t, o.GetOrderId())
	svc := e.coldService(t, nil, false)
	_, err := getOrder(t, svc, o.GetOrderId(), 1)
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	if got := listIDs(t, svc, listReq(1, 0, 0)); len(got) != 0 {
		t.Errorf("list without a cold store = %v, want empty", got)
	}
}
