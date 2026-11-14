package recon

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/merkle"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// want 是一次比对应当报告的差异：ID 的规范串 → 类别。
type want map[string]Class

func orderKey(o *testOrder) string { return strconv.FormatInt(o.id, 10) }

func itemKey(o *testOrder, it item) string { return fmt.Sprintf("%d|%d", o.id, it.sku) }

func payResultKey(o *testOrder) string { return fmt.Sprintf("%d|%d", o.id, o.paymentID) }

func itemsAll(o *testOrder, c Class) want {
	w := want{}
	for _, it := range o.items {
		w[itemKey(o, it)] = c
	}
	return w
}

func got(rep *ComparisonReport) want {
	w := want{}
	for _, f := range rep.Findings {
		w[f.ID] = f.Class
	}
	return w
}

type config struct {
	name string
	o    Options
}

// configs 是每个用例都要跑的三种做法：树（默认阈值，差异节点多半在高层就停）、
// 树（阈值 3，逼着 Diff 往下走到 TopShift 以下）、全量。三种的结果必须相同。
func configs(bound int64) []config {
	return []config{
		{"tree", Options{Bound: bound, Recheck: time.Millisecond}},
		{"tree-deep", Options{Bound: bound, MaxRows: 3, Recheck: time.Millisecond}},
		{"full", Options{Mode: ModeFull, Bound: bound, Recheck: time.Millisecond}},
	}
}

// compareAll 用三种做法比 c，断言每种报告的差异恰好是 w。
func compareAll(t *testing.T, c Comparison, bound int64, w want) []*ComparisonReport {
	t.Helper()
	return compareWith(t, c, configs(bound), w)
}

func compareWith(t *testing.T, c Comparison, cfs []config, w want) []*ComparisonReport {
	t.Helper()
	var reps []*ComparisonReport
	for _, cf := range cfs {
		rep, err := Compare(t.Context(), c, cf.o)
		if err != nil {
			t.Fatalf("%s %s: %v", c.Name, cf.name, err)
		}
		if g := got(rep); !maps.Equal(g, w) {
			t.Errorf("%s %s: got %v, want %v", c.Name, cf.name, g, w)
		}
		if rep.Unstable != 0 || rep.Dropped != 0 {
			t.Errorf("%s %s: unstable %d, dropped %d on recheck; nothing changes during the test", c.Name, cf.name, rep.Unstable, rep.Dropped)
		}
		n := 0
		for _, k := range rep.Counts {
			n += k
		}
		if n != len(rep.Findings) || rep.Outstanding() != len(w) {
			t.Errorf("%s %s: counts %v, %d findings, outstanding %d", c.Name, cf.name, rep.Counts, len(rep.Findings), rep.Outstanding())
		}
		if len(w) == 0 && cf.o.Mode == ModeTree && !rep.RootsEqual {
			t.Errorf("%s %s: roots differ with no difference: %s vs %s", c.Name, cf.name, rep.Left.Root, rep.Right.Root)
		}
		if rep.Left.Statements == 0 || rep.Right.Statements == 0 {
			t.Errorf("%s %s: no statements counted", c.Name, cf.name)
		}
		t.Logf("%s %s: %.0f ms, statements %d + %d", c.Name, cf.name, rep.Timings.Total, rep.Left.Statements, rep.Right.Statements)
		reps = append(reps, rep)
	}
	return reps
}

type mutation struct {
	name string
	// do 改数据，返回每个比对（按名字）应当报告的差异；没写的比对应当没有差异。
	do func(f *fixture) map[string]want
	// check 对三种做法的报告做额外的检查。
	check func(t *testing.T, reps map[string][]*ComparisonReport)
}

func runMutations(t *testing.T, f *fixture, pairName string, ms []mutation) {
	t.Helper()
	p := f.pair(pairName)
	all := append([]mutation{{name: "consistent", do: func(*fixture) map[string]want { return nil }}}, ms...)
	for _, m := range all {
		t.Run(m.name, func(t *testing.T) {
			f.t = t
			f.reset()
			ws := m.do(f)
			reps := map[string][]*ComparisonReport{}
			for _, c := range p.Comparisons {
				w := ws[c.Name]
				if w == nil {
					w = want{}
				}
				reps[c.Name] = compareWith(t, c, configs(0), w)
			}
			if m.check != nil {
				m.check(t, reps)
			}
		})
	}
}

// otherSlotSameDB 返回和 o 在同一个库上的另一个 slot。
func (f *fixture) otherSlotSameDB(o *testOrder) int {
	s := f.slotOf(o)
	for _, x := range f.orderR.SlotsOf(f.orderR.DB(s)) {
		if x != s {
			return x
		}
	}
	f.t.Fatal("no other slot")
	return 0
}

func ordersTable(slot int) string { return sharding.Table("orders", slot) }

func TestReplica(t *testing.T) {
	t.Parallel()
	f := newFixture(t, true)
	replicaOf := func(o *testOrder) *sql.DB { return f.replicas[f.orderR.DB(f.slotOf(o))] }
	one := func(cmp, id string, c Class) map[string]want { return map[string]want{cmp: {id: c}} }
	runMutations(t, f, PairReplica, []mutation{
		{name: "row missing on replica", do: func(f *fixture) map[string]want {
			o := f.pick(0, where("sparse", false, statusPaid))
			f.exec(replicaOf(o), "DELETE FROM "+ordersTable(f.slotOf(o))+" WHERE order_id = ?", o.id)
			return one("replica/orders", orderKey(o), OnlyLeft)
		}},
		{name: "extra row on replica", do: func(f *fixture) map[string]want {
			o := &testOrder{user: 2000, status: statusPending, version: 1, created: denseStart.Add(301 * time.Millisecond)}
			o.id = f.newOrderID(o.created, o.user)
			var b inserter
			f.writeHot(&b, replicaOf(o), f.slotOf(o), o)
			b.flush(f.t)
			return one("replica/orders", orderKey(o), OnlyRight)
		}},
		{name: "amount", do: func(f *fixture) map[string]want {
			o := f.pick(7, where("dense", false, statusPaid))
			f.exec(replicaOf(o), "UPDATE "+ordersTable(f.slotOf(o))+" SET total_amount = total_amount + 1 WHERE order_id = ?", o.id)
			return one("replica/orders", orderKey(o), Mismatch)
		}},
		{name: "status", do: func(f *fixture) map[string]want {
			o := f.pick(3, where("dense", false, statusPending))
			f.exec(replicaOf(o), "UPDATE "+ordersTable(f.slotOf(o))+" SET status = 5 WHERE order_id = ?", o.id)
			return one("replica/orders", orderKey(o), Mismatch)
		}},
		{name: "version", do: func(f *fixture) map[string]want {
			o := f.pick(2, where("sparse", false, statusPaid))
			f.exec(replicaOf(o), "UPDATE "+ordersTable(f.slotOf(o))+" SET version = version + 1 WHERE order_id = ?", o.id)
			return one("replica/orders", orderKey(o), Mismatch)
		}},
		{name: "fail reason becomes NULL on primary", do: func(f *fixture) map[string]want {
			o := f.pick(4, where("dense", false, statusFailed))
			f.exec(f.primaryOf(o), "UPDATE "+ordersTable(f.slotOf(o))+" SET fail_reason = NULL WHERE order_id = ?", o.id)
			return one("replica/orders", orderKey(o), Mismatch)
		}},
		{name: "same row in two slots", do: func(f *fixture) map[string]want {
			o := f.pick(5, where("dense", false, statusPaid))
			f.exec(replicaOf(o), "INSERT INTO "+ordersTable(f.otherSlotSameDB(o))+" SELECT * FROM "+ordersTable(f.slotOf(o))+" WHERE order_id = ?", o.id)
			return one("replica/orders", orderKey(o), Duplicate)
		}},
		{name: "item quantity", do: func(f *fixture) map[string]want {
			o := f.pick(1, where("dense", false, statusPaid))
			it := o.items[0]
			f.exec(replicaOf(o), "UPDATE "+sharding.Table("order_items", f.slotOf(o))+" SET quantity = quantity + 1 WHERE order_id = ? AND sku_id = ?", o.id, it.sku)
			return one("replica/order_items", itemKey(o, it), Mismatch)
		}},
		{name: "payment result missing", do: func(f *fixture) map[string]want {
			o := f.pick(1, where("sparse", false, statusPaid))
			f.exec(replicaOf(o), "DELETE FROM "+sharding.Table("payment_results", f.slotOf(o))+" WHERE order_id = ?", o.id)
			return one("replica/payment_results", payResultKey(o), OnlyLeft)
		}},
	})
}

// depthCheck 断言阈值为 3 的树模式一路分到了 TopShift 以下（deep 为 true），或者没有（false）；
// 默认阈值时差异在根的子节点这一层就停（数据一共不到两千行）。
func depthCheck(cmp string, deep bool) func(t *testing.T, reps map[string][]*ComparisonReport) {
	return func(t *testing.T, reps map[string][]*ComparisonReport) {
		t.Helper()
		rs := reps[cmp]
		if n := len(rs[0].Levels); n != 1 {
			t.Errorf("default threshold: %d levels, want 1 (stop at the children of the root)", n)
		}
		lv := rs[1].Levels
		last := lv[len(lv)-1].Shift
		if deep != (last < TopShift) {
			t.Errorf("threshold 3: deepest level shift %d, want below %d: %v", last, TopShift, deep)
		}
		if deep && last != DefaultMinShift {
			t.Errorf("threshold 3: deepest level shift %d, want %d", last, DefaultMinShift)
		}
	}
}

func TestIndex(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	idx := func(o *testOrder, q string, args ...any) {
		db, table := f.indexDB(o.merchant)
		f.exec(db, strings.ReplaceAll(q, "{T}", table), args...)
	}
	one := func(id string, c Class) map[string]want { return map[string]want{PairIndex: {id: c}} }
	runMutations(t, f, PairIndex, []mutation{
		{name: "index row missing", do: func(f *fixture) map[string]want {
			o := f.pick(0, where("sparse", false, statusPaid))
			idx(o, "DELETE FROM {T} WHERE order_id = ?", o.id)
			return one(orderKey(o), OnlyLeft)
		}, check: depthCheck(PairIndex, false)},
		{name: "index row of an archived order missing", do: func(f *fixture) map[string]want {
			o := f.pick(1, where("sparse", true, statusPaid))
			idx(o, "DELETE FROM {T} WHERE order_id = ?", o.id)
			return one(orderKey(o), OnlyLeft)
		}},
		{name: "extra index rows", do: func(f *fixture) map[string]want {
			failed := f.pick(2, where("dense", false, statusFailed))
			ghost := f.newOrderID(denseStart.Add(500*time.Millisecond), 2001)
			db, table := f.indexDB(3)
			f.exec(db, "INSERT INTO "+table+" ("+indexCols+") VALUES (3, ?, ?, 3, 10, 'CNY', NOW(6), NULL, 1, NOW(6))", failed.id, failed.user)
			f.exec(db, "INSERT INTO "+table+" ("+indexCols+") VALUES (3, ?, 2001, 3, 10, 'CNY', NOW(6), NULL, 1, NOW(6))", ghost)
			return map[string]want{PairIndex: {orderKey(failed): OnlyRight, strconv.FormatInt(ghost, 10): OnlyRight}}
		}},
		{name: "amount", do: func(f *fixture) map[string]want {
			o := f.pick(10, where("dense", false, statusPaid))
			idx(o, "UPDATE {T} SET total_amount = total_amount + 100 WHERE order_id = ?", o.id)
			return one(orderKey(o), Mismatch)
		}, check: depthCheck(PairIndex, true)},
		{name: "status", do: func(f *fixture) map[string]want {
			o := f.pick(0, where("dense", false, statusCancelled))
			idx(o, "UPDATE {T} SET status = 3 WHERE order_id = ?", o.id)
			return one(orderKey(o), Mismatch)
		}},
		{name: "index version", do: func(f *fixture) map[string]want {
			o := f.pick(3, where("sparse", false, statusPaid))
			idx(o, "UPDATE {T} SET src_version = src_version - 1 WHERE order_id = ?", o.id)
			return one(orderKey(o), Mismatch)
		}},
		{name: "order version", do: func(f *fixture) map[string]want {
			o := f.pick(12, where("dense", false, statusPaid))
			f.exec(f.primaryOf(o), "UPDATE "+ordersTable(f.slotOf(o))+" SET version = version + 1 WHERE order_id = ?", o.id)
			return one(orderKey(o), Mismatch)
		}},
		{name: "archived order amount", do: func(f *fixture) map[string]want {
			o := f.pick(0, where("sparse", true, statusClosed))
			f.exec(f.archive, "UPDATE orders_archive SET total_amount = total_amount + 1 WHERE order_id = ?", o.id)
			return one(orderKey(o), Mismatch)
		}},
		{name: "same order under two merchants", do: func(f *fixture) map[string]want {
			o := f.pick(4, where("dense", false, statusPaid))
			other := o.merchant%6 + 1
			if f.indexSlot(other) == f.indexSlot(o.merchant) {
				t.Fatal("pick a merchant in another slot")
			}
			db, table := f.indexDB(other)
			f.exec(db, "INSERT INTO "+table+" ("+indexCols+") VALUES (?, ?, ?, ?, ?, 'CNY', NOW(6), NULL, ?, NOW(6))",
				other, o.id, o.user, o.status, o.total, o.version)
			return one(orderKey(o), Duplicate)
		}},
		{name: "several in the dense region", do: func(f *fixture) map[string]want {
			w := want{}
			for i := range 5 {
				o := f.pick(i*3, where("dense", false, statusPaid))
				idx(o, "UPDATE {T} SET user_id = user_id + 1 WHERE order_id = ?", o.id)
				w[orderKey(o)] = Mismatch
			}
			return map[string]want{PairIndex: w}
		}},
	})
}

func TestPayment(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	one := func(id string, c Class) map[string]want { return map[string]want{PairPayment: {id: c}} }
	runMutations(t, f, PairPayment, []mutation{
		{name: "paid order without a succeeded payment", do: func(f *fixture) map[string]want {
			o := f.pick(0, where("dense", false, statusPaid))
			f.exec(f.payment, "UPDATE payments SET status = ? WHERE order_id = ?", payFailed, o.id)
			return one(orderKey(o), OnlyLeft)
		}},
		{name: "succeeded payment for a closed order", do: func(f *fixture) map[string]want {
			o := f.pick(2, where("dense", false, statusPaid))
			f.exec(f.primaryOf(o), "UPDATE "+ordersTable(f.slotOf(o))+" SET status = ? WHERE order_id = ?", statusClosed, o.id)
			return one(orderKey(o), OnlyRight)
		}},
		{name: "amount", do: func(f *fixture) map[string]want {
			o := f.pick(1, where("sparse", false, statusPaid))
			f.exec(f.payment, "UPDATE payments SET amount = amount - 1 WHERE order_id = ?", o.id)
			return one(orderKey(o), Mismatch)
		}},
		{name: "archived order amount", do: func(f *fixture) map[string]want {
			o := f.pick(0, where("sparse", true, statusPaid))
			f.exec(f.payment, "UPDATE payments SET amount = amount + 1 WHERE order_id = ?", o.id)
			return one(orderKey(o), Mismatch)
		}},
		{name: "payment missing", do: func(f *fixture) map[string]want {
			o := f.pick(9, where("dense", false, statusPaid))
			f.exec(f.payment, "DELETE FROM payments WHERE order_id = ?", o.id)
			return one(orderKey(o), OnlyLeft)
		}},
		{name: "payment for no order", do: func(f *fixture) map[string]want {
			id := f.newOrderID(sparseStart.Add(100*time.Hour), 1500)
			f.exec(f.payment, "INSERT INTO payments (payment_id, order_id, user_id, status, amount, currency, outcome) VALUES (?, ?, 1500, ?, 100, 'CNY', 1)", id+1, id, paySucceeded)
			return one(strconv.FormatInt(id, 10), OnlyRight)
		}},
		{name: "order version is not compared", do: func(f *fixture) map[string]want {
			o := f.pick(3, where("dense", false, statusPaid))
			f.exec(f.primaryOf(o), "UPDATE "+ordersTable(f.slotOf(o))+" SET version = version + 1 WHERE order_id = ?", o.id)
			return nil
		}},
		{name: "same order in two slots", do: func(f *fixture) map[string]want {
			o := f.pick(6, where("dense", false, statusPaid))
			f.exec(f.primaryOf(o), "INSERT INTO "+ordersTable(f.otherSlotSameDB(o))+" SELECT * FROM "+ordersTable(f.slotOf(o))+" WHERE order_id = ?", o.id)
			return one(orderKey(o), Duplicate)
		}},
	})
}

func TestStock(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	items := func(o *testOrder, c Class) map[string]want { return map[string]want{PairStock: itemsAll(o, c)} }
	multi := func(pred func(*testOrder) bool) func(*testOrder) bool {
		return func(o *testOrder) bool { return pred(o) && len(o.items) > 1 }
	}
	runMutations(t, f, PairStock, []mutation{
		{name: "cancelled order still reserved", do: func(f *fixture) map[string]want {
			o := f.pick(0, multi(where("dense", false, statusCancelled)))
			f.exec(f.inventory, "UPDATE reservation SET status = ? WHERE order_id = ?", resReserved, o.id)
			return items(o, OnlyRight)
		}},
		{name: "archived cancelled order still reserved", do: func(f *fixture) map[string]want {
			o := f.pick(0, where("sparse", true, statusCancelled))
			f.exec(f.inventory, "UPDATE reservation SET status = ? WHERE order_id = ?", resReserved, o.id)
			return items(o, OnlyRight)
		}},
		{name: "paid order released", do: func(f *fixture) map[string]want {
			o := f.pick(0, multi(where("dense", false, statusPaid)))
			f.exec(f.inventory, "UPDATE reservation SET status = ? WHERE order_id = ?", resReleased, o.id)
			return items(o, OnlyLeft)
		}},
		{name: "pending order paid but still reserved", do: func(f *fixture) map[string]want {
			o := f.pick(0, multi(where("dense", false, statusPending)))
			f.exec(f.primaryOf(o), "UPDATE "+ordersTable(f.slotOf(o))+" SET status = ? WHERE order_id = ?", statusPaid, o.id)
			return items(o, Mismatch)
		}},
		{name: "quantity", do: func(f *fixture) map[string]want {
			o := f.pick(2, multi(where("sparse", false, statusPaid)))
			it := o.items[1]
			f.exec(f.inventory, "UPDATE reservation_item SET quantity = quantity + 1 WHERE order_id = ? AND sku_id = ?", o.id, it.sku)
			return map[string]want{PairStock: {itemKey(o, it): Mismatch}}
		}},
		{name: "reservation item missing", do: func(f *fixture) map[string]want {
			o := f.pick(4, multi(where("dense", false, statusPaid)))
			it := o.items[0]
			f.exec(f.inventory, "DELETE FROM reservation_item WHERE order_id = ? AND sku_id = ?", o.id, it.sku)
			return map[string]want{PairStock: {itemKey(o, it): OnlyLeft}}
		}},
		{name: "same order in two slots", do: func(f *fixture) map[string]want {
			o := f.pick(5, multi(where("dense", false, statusPaid)))
			db, s, s2 := f.primaryOf(o), f.slotOf(o), f.otherSlotSameDB(o)
			f.exec(db, "INSERT INTO "+ordersTable(s2)+" SELECT * FROM "+ordersTable(s)+" WHERE order_id = ?", o.id)
			f.exec(db, "INSERT INTO "+sharding.Table("order_items", s2)+" SELECT * FROM "+sharding.Table("order_items", s)+" WHERE order_id = ?", o.id)
			return items(o, Duplicate)
		}},
	})
}

// 水位线之后的差异不报告，行数也不算水位线之后的行。
func TestBound(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	late := f.pick(0, where("late", false, statusPaid))
	dense := f.pick(0, where("dense", false, statusPaid))
	for _, o := range []*testOrder{late, dense} {
		db, table := f.indexDB(o.merchant)
		f.exec(db, "UPDATE "+table+" SET total_amount = total_amount + 1 WHERE order_id = ?", o.id)
	}
	var rows uint64
	for _, o := range f.orders {
		if o.merchant != 0 && o.id < testBound {
			rows++
		}
	}
	c := f.pair(PairIndex).Comparisons[0]
	reps := compareAll(t, c, testBound, want{orderKey(dense): Mismatch})
	if reps[0].Left.Rows != rows || reps[2].Left.Rows != rows {
		t.Errorf("left rows: tree %d, full %d, want %d", reps[0].Left.Rows, reps[2].Left.Rows, rows)
	}
	// 不设上界时两个都报告。
	compareAll(t, c, 0, want{orderKey(dense): Mismatch, orderKey(late): Mismatch})
}

// 下界之前的差异不报告，行数也只算窗口里的行；窗口 [since, bound) 两头都生效。
func TestSince(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	sparse := f.pick(0, where("sparse", false, statusPaid))
	dense := f.pick(0, where("dense", false, statusPaid))
	late := f.pick(0, where("late", false, statusPaid))
	for _, o := range []*testOrder{sparse, dense, late} {
		db, table := f.indexDB(o.merchant)
		f.exec(db, "UPDATE "+table+" SET total_amount = total_amount + 1 WHERE order_id = ?", o.id)
	}
	// 在 sparse 之后、dense 之前。
	since := idgen.LowerBound(time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC))
	if sparse.id >= since || dense.id < since {
		t.Fatalf("fixture changed: sparse %d dense %d since %d", sparse.id, dense.id, since)
	}
	var rows uint64
	for _, o := range f.orders {
		if o.merchant != 0 && o.id >= since && o.id < testBound {
			rows++
		}
	}
	c := f.pair(PairIndex).Comparisons[0]
	for _, cf := range configs(testBound) {
		cf.o.Since = since
		rep := compareWith(t, c, []config{cf}, want{orderKey(dense): Mismatch})[0]
		if rep.Left.Rows != rows || rep.Right.Rows != rows {
			t.Errorf("%s: rows left %d right %d, want %d", cf.name, rep.Left.Rows, rep.Right.Rows, rows)
		}
	}
	// 只有下界：dense 和 late。
	for _, cf := range configs(0) {
		cf.o.Since = since
		compareWith(t, c, []config{cf}, want{orderKey(dense): Mismatch, orderKey(late): Mismatch})
	}
	// 空窗口和对着存下来的树都拒绝。
	if _, err := Compare(t.Context(), c, Options{Since: testBound, Bound: since}); err == nil {
		t.Error("empty window: no error")
	}
	ac := f.pair(PairArchive).Comparisons[0]
	tree, _, err := Snapshot(t.Context(), ac, testBound, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Compare(t.Context(), ac, Options{Since: since, Bound: testBound, Against: map[string]*merkle.Tree{ac.Name: tree}}); err == nil {
		t.Error("lower bound against a saved tree: no error")
	}
}

// 复查：逐行比之后、复查之前改回一致的丢掉，又变了的进 unstable，不动的确认。
func TestRecheck(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	fixed := f.pick(0, where("dense", false, statusPaid))
	moving := f.pick(1, where("dense", false, statusPaid))
	stays := f.pick(0, where("sparse", false, statusPaid))
	set := func(o *testOrder, col string, v int64) {
		db, table := f.indexDB(o.merchant)
		f.exec(db, "UPDATE "+table+" SET "+col+" = ? WHERE order_id = ?", v, o.id)
	}
	c := f.pair(PairIndex).Comparisons[0]
	for _, cf := range configs(0) {
		t.Run(cf.name, func(t *testing.T) {
			f.t = t
			f.reset()
			set(fixed, "total_amount", fixed.total+1)
			set(moving, "status", statusClosed)
			set(stays, "src_version", stays.version+7)
			o := cf.o
			o.Recheck = 10 * time.Millisecond
			hooks := 0
			o.BeforeRecheck = func(context.Context) {
				hooks++
				set(fixed, "total_amount", fixed.total)
				set(moving, "status", statusCancelled)
			}
			rep, err := Compare(t.Context(), c, o)
			if err != nil {
				t.Fatal(err)
			}
			if hooks != 1 {
				t.Fatalf("hook ran %d times", hooks)
			}
			if g := got(rep); !maps.Equal(g, want{orderKey(stays): Mismatch}) {
				t.Errorf("confirmed %v", g)
			}
			if rep.Unstable != 1 || rep.Dropped != 1 {
				t.Errorf("unstable %d, dropped %d, want 1 and 1", rep.Unstable, rep.Dropped)
			}
			if len(rep.UnstableExamples) != 1 || rep.UnstableExamples[0].OrderID != moving.id {
				t.Errorf("unstable examples %+v", rep.UnstableExamples)
			}
		})
	}

	// 不复查时第一次的结果直接算确认，钩子不执行。
	f.t = t
	f.reset()
	set(fixed, "total_amount", fixed.total+1)
	rep, err := Compare(t.Context(), c, Options{BeforeRecheck: func(context.Context) { t.Error("hook ran without recheck") }})
	if err != nil {
		t.Fatal(err)
	}
	if g := got(rep); !maps.Equal(g, want{orderKey(fixed): Mismatch}) {
		t.Errorf("without recheck: %v", g)
	}
}

// SQL 聚合出来的树和在 Go 里逐行算出来的相同。
func TestDigestsMatchGo(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	check := func(name string, c Comparison, canon func(*testOrder) (string, bool)) {
		t.Helper()
		want, err := merkle.NewTree(TopShift)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range f.orders {
			if s, ok := canon(o); ok && o.id < testBound {
				want.Add(keyOf(o.id), merkle.HashRow(s))
			}
		}
		got, _, err := Snapshot(t.Context(), c, testBound, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got.Root() != want.Root() || !slices.Equal(got.Leaves(), want.Leaves()) {
			t.Errorf("%s: SQL root %v, Go root %v", name, got.Root(), want.Root())
		}
		if want.Len() < 10 {
			t.Errorf("%s: only %d leaves", name, want.Len())
		}
	}
	check("index left", f.pair(PairIndex).Comparisons[0], func(o *testOrder) (string, bool) {
		if o.merchant == 0 {
			return "", false
		}
		var c merkle.Canon
		return c.Int(o.id).Int(o.merchant).Int(o.user).Int(int64(o.status)).Int(o.total).Int(o.version).String(), true
	})
	check("payment left", f.pair(PairPayment).Comparisons[0], func(o *testOrder) (string, bool) {
		if !o.paid() || o.region == "seeded" {
			return "", false
		}
		var c merkle.Canon
		return c.Int(o.id).Int(o.paymentID).Int(o.total).Bytes([]byte("CNY")).String(), true
	})
}

func TestArchive(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	p := f.pair(PairArchive)

	snap := &SnapshotFile{CreatedAt: time.Now().UTC(), Bound: testBound}
	for _, c := range p.Comparisons {
		tree, rep, err := Snapshot(t.Context(), c, testBound, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rep.Rows == 0 || rep.Statements == 0 {
			t.Fatalf("%s: snapshot report %+v", c.Name, rep)
		}
		if err := snap.Add(c.Name, tree); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := snap.Write(&buf); err != nil {
		t.Fatal(err)
	}
	read, trees, err := ReadSnapshot(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if read.Bound != testBound || len(trees) != len(p.Comparisons) {
		t.Fatalf("snapshot read back: bound %d, %d trees", read.Bound, len(trees))
	}

	// 归档几个订单：热库删、冷库插同样的行。
	var moved []*testOrder
	var b inserter
	for _, pred := range []func(*testOrder) bool{
		where("sparse", false, statusPaid), where("dense", false, statusClosed), where("dense", false, statusPaid),
	} {
		o := f.pick(0, pred)
		db, s := f.primaryOf(o), f.slotOf(o)
		for _, tb := range []string{"orders", "order_items", "payment_results"} {
			f.exec(db, "DELETE FROM "+sharding.Table(tb, s)+" WHERE order_id = ?", o.id)
		}
		f.writeCold(&b, o)
		moved = append(moved, o)
	}
	b.flush(t)

	compare := func() map[string]*ComparisonReport {
		out := map[string]*ComparisonReport{}
		for _, c := range p.Comparisons {
			rep, err := Compare(t.Context(), c, Options{Bound: read.Bound, Against: trees})
			if err != nil {
				t.Fatal(err)
			}
			out[c.Name] = rep
		}
		return out
	}
	for name, rep := range compare() {
		if !rep.RootsEqual || len(rep.Nodes) != 0 || rep.Outstanding() != 0 {
			t.Errorf("%s after archiving: roots equal %v, nodes %v", name, rep.RootsEqual, rep.Nodes)
		}
	}

	// 改掉冷库里的一行：报告的节点包含它。
	o := moved[1]
	f.exec(f.archive, "UPDATE orders_archive SET total_amount = total_amount + 1 WHERE order_id = ?", o.id)
	for name, rep := range compare() {
		if name != PairArchive+"/orders" {
			if rep.Outstanding() != 0 {
				t.Errorf("%s: %v", name, rep.Nodes)
			}
			continue
		}
		if !rep.NodeOnly || len(rep.Nodes) != 1 || rep.Outstanding() != 1 {
			t.Fatalf("%s: nodes %v", name, rep.Nodes)
		}
		n := rep.Nodes[0]
		if o.id < n.From || o.id > n.To || n.Node != merkle.NodeOf(keyOf(o.id), TopShift).String() {
			t.Errorf("node %+v does not hold order %d", n, o.id)
		}
	}

	// 右侧是文件的比对不支持全量模式，也必须给树。
	if _, err := Compare(t.Context(), p.Comparisons[0], Options{Mode: ModeFull, Against: trees}); err == nil {
		t.Error("full mode against a saved tree: no error")
	}
	if _, err := Compare(t.Context(), p.Comparisons[0], Options{}); err == nil {
		t.Error("no saved tree: no error")
	}
}

// 报告能序列化，文本里有差异的例子，Bytes 列解回了文本。
func TestReportOutput(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	o := f.pick(0, where("dense", false, statusPaid))
	f.exec(f.payment, "UPDATE payments SET currency = 'USD' WHERE order_id = ?", o.id)
	rep, err := Compare(t.Context(), f.pair(PairPayment).Comparisons[0], Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Examples) != 1 {
		t.Fatalf("examples %+v", rep.Examples)
	}
	ex := rep.Examples[0]
	if ex.Left[0].Values[2] != (Field{Name: "currency", Value: `"CNY"`}) || ex.Right[0].Values[2] != (Field{Name: "currency", Value: `"USD"`}) ||
		ex.ID[0].Name != "order_id" || ex.Left[0].Values[1].Name != "amount" {
		t.Errorf("example %+v", ex)
	}
	r := &Report{Results: []*ComparisonReport{rep}}
	var text, js bytes.Buffer
	if err := r.WriteText(&text); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "mismatch order "+orderKey(o)) {
		t.Errorf("text report:\n%s", text.String())
	}
	if err := json.MarshalWrite(&js, r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(js.String(), `"mismatch":1`) {
		t.Errorf("json report: %s", js.String())
	}
	if showBytes("00ff") != "0x00ff" || showBytes("414243") != `"ABC"` {
		t.Error("showBytes")
	}
}
