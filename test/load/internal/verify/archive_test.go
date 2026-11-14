package verify

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// newArchiveDB 用 services/order 的冷库迁移建库，不依赖服务模块。分区边界与 order.NewArchiveMigrationData 一致。
func newArchiveDB(t *testing.T) *sql.DB {
	t.Helper()
	type partition struct {
		Name  string
		Below int64
	}
	var data struct{ Partitions []partition }
	for y := 2020; y <= 2029; y++ {
		data.Partitions = append(data.Partitions, partition{fmt.Sprintf("p%d", y), idgen.LowerBound(time.Date(y+1, 1, 1, 0, 0, 0, 0, time.UTC))})
	}
	fsys := os.DirFS(filepath.Join("..", "..", "..", "..", "services", "order"))
	return testdb.NewTemplate(t, fsys, "migrations_archive", data)
}

// archiveWorld 是热库（两个库）加冷库，带造数的辅助函数。
type archiveWorld struct {
	t    *testing.T
	hot  []*sql.DB
	cold *sql.DB
	r    *sharding.Router
	now  time.Time
}

func newArchiveWorld(t *testing.T) *archiveWorld {
	t.Helper()
	r, err := sharding.New(sharding.OrderLayout(2))
	if err != nil {
		t.Fatal(err)
	}
	return &archiveWorld{t: t, hot: newOrderDBs(t), cold: newArchiveDB(t), r: r, now: time.Now()}
}

// old、recent 返回订单号：时间在 100 天前、1 天前，i 区分不同的订单。
func (w *archiveWorld) old(i int64) int64 { return idgen.LowerBound(w.now.AddDate(0, 0, -100)) + i }

func (w *archiveWorld) recent(i int64) int64 { return idgen.LowerBound(w.now.AddDate(0, 0, -1)) + i }

func (w *archiveWorld) hotOrder(id, user int64, status int, merchant any) {
	slot := w.r.ForUser(user)
	exec(w.t, w.hot[w.r.DB(slot)], fmt.Sprintf(`INSERT INTO %s (order_id, user_id, merchant_id, version, status, total_amount, idempotency_key, request_hash, expire_at)
		VALUES (?, ?, ?, 2, ?, 100, ?, REPEAT('a', 32), NOW(6))`, sharding.Table("orders", slot)), id, user, merchant, status, fmt.Sprintf("k%d", id))
}

// hotChild 往热库某个用户所在的 slot 里写明细或支付结果行，不管有没有订单行。
func (w *archiveWorld) hotItem(id, user int64) {
	slot := w.r.ForUser(user)
	exec(w.t, w.hot[w.r.DB(slot)], fmt.Sprintf("INSERT INTO %s (order_id, sku_id, quantity) VALUES (?, 1, 1)", sharding.Table("order_items", slot)), id)
}

func (w *archiveWorld) hotPayment(id, user int64) {
	slot := w.r.ForUser(user)
	exec(w.t, w.hot[w.r.DB(slot)], fmt.Sprintf("INSERT INTO %s (payment_id, order_id, succeeded, amount, disposition) VALUES (?, ?, 1, 100, 1)",
		sharding.Table("payment_results", slot)), id, id)
}

func (w *archiveWorld) coldOrder(id, user int64, status int, merchant any) {
	exec(w.t, w.cold, `INSERT INTO orders_archive (order_id, user_id, merchant_id, version, status, total_amount, currency, idempotency_key,
		request_hash, expire_at, created_at, updated_at) VALUES (?, ?, ?, 2, ?, 100, 'CNY', ?, REPEAT('a', 32), NOW(6), NOW(6), NOW(6))`,
		id, user, merchant, status, fmt.Sprintf("k%d", id))
}

func (w *archiveWorld) coldItem(id int64) {
	exec(w.t, w.cold, "INSERT INTO order_items_archive (order_id, sku_id, quantity, unit_price, created_at) VALUES (?, 1, 1, 100, NOW(6))", id)
}

func (w *archiveWorld) coldPayment(id int64) {
	exec(w.t, w.cold, "INSERT INTO payment_results_archive (order_id, payment_id, succeeded, amount, disposition, created_at) VALUES (?, ?, 1, 100, 1, NOW(6))", id, id)
}

// archived 造一个完整归档的订单：冷库有订单、明细、支付结果。
func (w *archiveWorld) archived(id, user int64, status int) {
	w.coldOrder(id, user, status, nil)
	w.coldItem(id)
	w.coldPayment(id)
}

func (w *archiveWorld) check(inFlight bool) (*Result, *ArchiveData) {
	w.t.Helper()
	snap := &Snapshot{}
	if err := collectArchive(w.t.Context(), Sources{OrderDBs: w.hot, ArchiveDB: w.cold}, snap); err != nil {
		w.t.Fatal(err)
	}
	c := &checker{s: snap, archiveInFlight: inFlight}
	c.archive()
	return &c.res, snap.Archive
}

func hasViolation(res *Result, frag string) bool {
	for _, v := range res.Violations {
		if v.Check == CheckArchive && strings.Contains(v.Detail, frag) {
			return true
		}
	}
	return false
}

func TestArchiveCheck(t *testing.T) {
	w := newArchiveWorld(t)

	// 干净：两个已归档的订单（冷库有订单、明细、支付结果，热库什么都没有），两个还在热库的新订单（有明细和支付结果），
	// 还有一个只有明细、没有订单行的新订单（正在创建中，不算孤儿）。
	w.archived(w.old(1), 11, OrderPaid)
	w.archived(w.old(2), 12, OrderCancelled)
	w.coldOrder(w.old(3), 13, OrderClosed, nil) // 冷库订单没有支付结果是允许的（取消的订单）
	w.coldItem(w.old(3))
	for i, user := range []int64{21, 22} {
		id := w.recent(int64(i + 1))
		w.hotOrder(id, user, OrderPendingPayment, nil)
		w.hotItem(id, user)
		w.hotPayment(id, user)
	}
	w.hotItem(w.recent(9), 23)
	res, d := w.check(false)
	if res.Failed() || len(res.Notes) != 0 {
		t.Fatalf("clean world: violations %v, notes %v", res.Violations, res.Notes)
	}
	if d.ColdOrders != 3 || d.HotOrders != 2 {
		t.Fatalf("clean world: cold %d, hot %d orders", d.ColdOrders, d.HotOrders)
	}

	// 空冷库、空热库：通过，不报错。
	if res, _ := newArchiveWorld(t).check(false); res.Failed() {
		t.Fatalf("empty world: %v", res.Violations)
	}

	// 每一类问题造一个最小反例，累加在同一份数据上，逐步断言。
	// 1 冷库里有非终态订单。
	w.coldOrder(w.old(10), 31, OrderPendingPayment, nil)
	w.coldItem(w.old(10))
	res, d = w.check(false)
	if d.NonFinal.N != 1 || !strings.Contains(d.NonFinal.Examples[0], "PENDING_PAYMENT") || !hasViolation(res, "non-final") || len(res.Violations) != 1 {
		t.Fatalf("non-final: %+v %v", d.NonFinal, res.Violations)
	}
	// 2 冷库里有订单号时间太新的订单。
	w.coldOrder(w.recent(20), 32, OrderPaid, nil)
	w.coldItem(w.recent(20))
	res, d = w.check(false)
	if d.TooNew.N != 1 || !hasViolation(res, "created after") || len(res.Violations) != 2 {
		t.Fatalf("too new: %+v %v", d.TooNew, res.Violations)
	}
	// 3 同一个订单号热库冷库都有。归档已结束时失败，进行中时只记 note。
	dup := w.old(30)
	w.archived(dup, 33, OrderPaid)
	w.hotOrder(dup, 33, OrderPaid, nil)
	res, d = w.check(false)
	if d.Both.N != 1 || !hasViolation(res, "in both the hot and the cold db") || len(res.Violations) != 3 {
		t.Fatalf("both: %+v %v", d.Both, res.Violations)
	}
	res, d = w.check(true)
	if d.Both.N != 1 || hasViolation(res, "in both the hot and the cold db") || len(res.Violations) != 2 || len(res.Notes) != 1 {
		t.Fatalf("both, in flight: %+v %v %v", d.Both, res.Violations, res.Notes)
	}
	// 4 冷库订单没有明细；冷库明细、支付结果没有订单。
	w.coldOrder(w.old(40), 34, OrderPaid, nil)
	w.coldItem(w.old(41))
	w.coldPayment(w.old(42))
	res, d = w.check(false)
	if d.NoItems.N != 1 || d.ItemOrphans.N != 1 || d.PaymentOrphans.N != 1 || len(res.Violations) != 6 {
		t.Fatalf("cold children: %+v %+v %+v %v", d.NoItems, d.ItemOrphans, d.PaymentOrphans, res.Violations)
	}
	// 5 热库子表孤儿：老订单的明细、支付结果没有订单行。早于 30 天的才算，新订单的孤儿（上面的 recent(9)）不算。
	w.hotItem(w.old(50), 35)
	w.hotPayment(w.old(51), 36)
	res, d = w.check(false)
	if d.HotItemOrphans.N != 1 || d.HotPaymentOrphans.N != 1 || len(res.Violations) != 8 {
		t.Fatalf("hot orphans: %+v %+v %v", d.HotItemOrphans, d.HotPaymentOrphans, res.Violations)
	}
	if !strings.Contains(d.HotItemOrphans.Examples[0], strconv.FormatInt(w.old(50), 10)) || !strings.Contains(d.HotItemOrphans.Examples[0], "order_items_") {
		t.Fatalf("hot item orphan example = %v", d.HotItemOrphans.Examples)
	}
}

// 例子数封顶，且取订单号最小的几个。
func TestArchiveExamplesAreCapped(t *testing.T) {
	w := newArchiveWorld(t)
	for i := range int64(8) {
		w.coldOrder(w.old(i+1), 11, OrderPendingPayment, nil)
		w.coldItem(w.old(i + 1))
	}
	_, d := w.check(false)
	if d.NonFinal.N != 8 || len(d.NonFinal.Examples) != archiveExamples || !strings.Contains(d.NonFinal.Examples[0], strconv.FormatInt(w.old(1), 10)) {
		t.Fatalf("non-final: %+v", d.NonFinal)
	}
}

// 没给冷库：不读，第 21 条跳过。
func TestArchiveSkippedWithoutDB(t *testing.T) {
	snap := &Snapshot{}
	if err := collectArchive(t.Context(), Sources{}, snap); err != nil || snap.Archive != nil {
		t.Fatalf("collect without archive db: %v, %v", snap.Archive, err)
	}
	c := &checker{s: snap}
	c.archive()
	if c.res.Failed() {
		t.Fatal("check 21 failed without an archive db")
	}
}

// 第 20 条：订单在冷库、索引里有，不算多余；冷库热库都没有、索引里有，仍然算多余。
func TestMerchantIndexWithArchive(t *testing.T) {
	w := newArchiveWorld(t)
	idx := newMerchantIndexDBs(t)
	ir, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil {
		t.Fatal(err)
	}
	const m = 7
	addIndex := func(orderID int64, status int) {
		slot := ir.ForMerchant(m)
		exec(t, idx[ir.DB(slot)], fmt.Sprintf(`INSERT INTO %s (merchant_id, order_id, user_id, status, total_amount, currency, created_at, src_version, synced_at)
			VALUES (?, ?, 1, ?, 100, 'CNY', NOW(6), 2, NOW(6))`, sharding.Table("merchant_orders", slot)), m, orderID, status)
	}
	run := func(withArchive bool) *Result {
		src := Sources{OrderDBs: w.hot, MerchantIndexDBs: idx}
		if withArchive {
			src.ArchiveDB = w.cold
		}
		snap := &Snapshot{}
		if err := collectMerchantIndex(t.Context(), src, snap); err != nil {
			t.Fatal(err)
		}
		c := &checker{s: snap}
		c.merchantIndex()
		return &c.res
	}

	// 已归档：冷库有，热库没有，索引有。同一个订单热库冷库都有（归档进行中）也只算一个。
	w.coldOrder(w.old(1), 11, OrderPaid, m)
	addIndex(w.old(1), OrderPaid)
	both := w.old(2)
	w.coldOrder(both, 12, OrderCancelled, m)
	w.hotOrder(both, 12, OrderCancelled, m)
	addIndex(both, OrderCancelled)
	w.hotOrder(w.recent(3), 13, OrderPendingPayment, m)
	addIndex(w.recent(3), OrderPendingPayment)

	res := run(true)
	if res.Failed() || *res.Counts.MerchantIndex != (MerchantIndexCounts{Orders: 3, Rows: 3}) {
		t.Fatalf("with archive: violations %v, counts %+v", res.Violations, res.Counts.MerchantIndex)
	}
	// 不给冷库：行为和以前一样，已归档的订单在索引里是多余的。
	res = run(false)
	if c := res.Counts.MerchantIndex; c.Extra != 1 || c.Missing != 0 {
		t.Fatalf("without archive: %+v", c)
	}

	// 冷库热库都没有、索引里有：仍然多余。
	addIndex(w.old(4), OrderPaid)
	res = run(true)
	if c := res.Counts.MerchantIndex; c.Extra != 1 || c.Missing != 0 || !res.Failed() {
		t.Fatalf("index row without an order: %+v %v", c, res.Violations)
	}
	// 冷库里的订单索引里没有：缺失。
	w.coldOrder(w.old(5), 15, OrderPaid, m)
	res = run(true)
	if c := res.Counts.MerchantIndex; c.Extra != 1 || c.Missing != 1 {
		t.Fatalf("cold order not in the index: %+v", c)
	}
}
