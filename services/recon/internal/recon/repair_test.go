package recon

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/sharding"
)

type indexRow struct {
	merchant, status, total, version int64
}

// indexRows 返回商家索引里订单 id 的全部行（各 slot）。
func (f *fixture) indexRows(id int64) []indexRow {
	f.t.Helper()
	var out []indexRow
	for slot := range f.indexR.Slots() {
		out = append(out, f.indexRowsIn(slot, id)...)
	}
	return out
}

func (f *fixture) indexRowsIn(slot int, id int64) []indexRow {
	f.t.Helper()
	db := f.index[f.indexR.DB(slot)]
	rows, err := db.QueryContext(f.t.Context(), "SELECT merchant_id, status, total_amount, src_version FROM "+
		sharding.Table("merchant_orders", slot)+" WHERE order_id = ?", id)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close() //nolint:errcheck // 测试
	var out []indexRow
	for rows.Next() {
		var r indexRow
		if err := rows.Scan(&r.merchant, &r.status, &r.total, &r.version); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return out
}

func (f *fixture) setIndex(o *testOrder, set string, args ...any) {
	f.t.Helper()
	db, table := f.indexDB(o.merchant)
	f.exec(db, "UPDATE "+table+" SET "+set+" WHERE order_id = ?", append(args, o.id)...)
}

func (f *fixture) setOrder(o *testOrder, set string, args ...any) {
	f.t.Helper()
	f.exec(f.primaryOf(o), "UPDATE "+ordersTable(f.slotOf(o))+" SET "+set+" WHERE order_id = ?", append(args, o.id)...)
}

func repairOnce(t *testing.T, f *fixture, o Options) *ComparisonReport {
	t.Helper()
	r, err := NewRepairer(f.topo)
	if err != nil {
		t.Fatal(err)
	}
	o.Repairer = r
	o.Recheck = time.Millisecond
	rep, err := Compare(t.Context(), f.pair(PairIndex).Comparisons[0], o)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Repair == nil {
		t.Fatal("no repair report")
	}
	return rep
}

func TestRepair(t *testing.T) {
	t.Parallel()
	f := newFixture(t, false)
	c := f.pair(PairIndex).Comparisons[0]

	t.Run("all kinds", func(t *testing.T) {
		f.t = t
		f.reset()
		missing := f.pick(0, where("sparse", false, statusPaid))
		coldMissing := f.pick(0, where("sparse", true, statusPaid))
		extra := f.pick(0, where("dense", false, statusFailed))
		amount := f.pick(0, where("dense", false, statusPaid))
		dup := f.pick(1, where("dense", false, statusPaid))
		for _, o := range []*testOrder{missing, coldMissing} {
			db, table := f.indexDB(o.merchant)
			f.exec(db, "DELETE FROM "+table+" WHERE order_id = ?", o.id)
		}
		db, table := f.indexDB(2)
		f.exec(db, "INSERT INTO "+table+" ("+indexCols+") VALUES (2, ?, ?, 3, 10, 'CNY', NOW(6), NULL, 1, NOW(6))", extra.id, extra.user)
		// 版本相同、内容不同：正是 orderindex 平时的 “>” 修不了的情况。
		f.setIndex(amount, "total_amount = total_amount + 5")
		other := dup.merchant%6 + 1
		db, table = f.indexDB(other)
		f.exec(db, "INSERT INTO "+table+" ("+indexCols+") VALUES (?, ?, ?, ?, ?, 'CNY', NOW(6), NULL, ?, NOW(6))",
			other, dup.id, dup.user, dup.status, dup.total, dup.version)

		rep := repairOnce(t, f, Options{MaxRows: 3})
		wantFound := want{
			orderKey(missing): OnlyLeft, orderKey(coldMissing): OnlyLeft, orderKey(extra): OnlyRight,
			orderKey(amount): Mismatch, orderKey(dup): Duplicate,
		}
		if g := got(rep); !maps.Equal(g, wantFound) {
			t.Fatalf("found %v, want %v", g, wantFound)
		}
		rp := rep.Repair
		if rp.Orders != 5 || rp.Fixed != 5 || rp.Remaining != 0 || rp.Skipped != 0 || rep.Outstanding() != 0 {
			t.Fatalf("repair %+v", rp)
		}
		// 两个补上的、一个改过的，加上重复的那个订单在它该在的 slot 里那一行（内容没变，synced_at 刷新了）；
		// 多出来的一行和另一个商家下的一行删掉。
		if rp.Upserted != 4 || rp.Deleted != 2 {
			t.Errorf("upserted %d, deleted %d", rp.Upserted, rp.Deleted)
		}
		if rows := f.indexRows(dup.id); len(rows) != 1 || rows[0].merchant != dup.merchant {
			t.Errorf("duplicate after repair: %+v", rows)
		}
		compareAll(t, c, 0, want{})
	})

	t.Run("order changes before repair", func(t *testing.T) {
		f.t = t
		f.reset()
		o := f.pick(2, where("dense", false, statusPaid))
		f.setIndex(o, "total_amount = total_amount + 5")
		// 复查之后、修复之前订单又变了（版本更高），CDC 还没追上：修复读到的是新版本，写的也是新版本。
		rep := repairOnce(t, f, Options{BeforeRepair: func(context.Context) {
			f.setOrder(o, "status = ?, version = version + 1", statusClosed) //nolint:contextcheck // 测试钩子，用测试自己的 context
		}})
		if rp := rep.Repair; rp.Fixed != 1 || rp.Remaining != 0 {
			t.Fatalf("repair %+v", rp)
		}
		if rows := f.indexRows(o.id); len(rows) != 1 || rows[0] != (indexRow{o.merchant, statusClosed, o.total, o.version + 1}) {
			t.Errorf("index rows %+v", rows)
		}
	})

	t.Run("index already newer", func(t *testing.T) {
		f.t = t
		f.reset()
		o := f.pick(3, where("dense", false, statusPaid))
		f.setIndex(o, "status = ?, src_version = src_version + 3", statusCancelled)
		rep := repairOnce(t, f, Options{})
		if rp := rep.Repair; rp.Fixed != 0 || rp.Remaining != 1 || rp.Upserted != 0 || rep.Outstanding() != 1 {
			t.Fatalf("repair %+v", rp)
		}
		if rows := f.indexRows(o.id); len(rows) != 1 || rows[0] != (indexRow{o.merchant, statusCancelled, o.total, o.version + 3}) {
			t.Errorf("index rows %+v", rows)
		}
	})

	t.Run("newer index row arrives during repair", func(t *testing.T) {
		f.t = t
		f.reset()
		o := f.pick(4, where("dense", false, statusPaid))
		f.setIndex(o, "total_amount = 1")
		// CDC 在修复之前写进了更高的版本（订单库这一行稍后也会到这个版本）：修复不能把它盖回去。
		rep := repairOnce(t, f, Options{BeforeRepair: func(context.Context) {
			f.setIndex(o, "status = ?, total_amount = ?, src_version = ?", statusClosed, o.total, o.version+1) //nolint:contextcheck // 同上
		}})
		if rp := rep.Repair; rp.Remaining != 1 || rp.Upserted != 0 {
			t.Fatalf("repair %+v", rp)
		}
		if rows := f.indexRows(o.id); len(rows) != 1 || rows[0] != (indexRow{o.merchant, statusClosed, o.total, o.version + 1}) {
			t.Errorf("index rows %+v", rows)
		}
	})
}

// 有从库时比对读从库，修复前的重读读主库。
func TestRepairReadsPrimary(t *testing.T) {
	t.Parallel()
	f := newFixture(t, true)
	o := f.pick(5, where("dense", false, statusPaid))
	// 主库已经到了 v+1（CLOSED），从库还在 v；索引也是 v+1 但内容错了。读从库修复的话 v < v+1 什么都不改。
	f.setOrder(o, "status = ?, version = version + 1", statusClosed)
	f.setIndex(o, "status = ?, src_version = src_version + 1", statusCancelled)
	rep := repairOnce(t, f, Options{})
	if g := got(rep); !maps.Equal(g, want{orderKey(o): Mismatch}) {
		t.Fatalf("found %v", g)
	}
	if rows := f.indexRows(o.id); len(rows) != 1 || rows[0] != (indexRow{o.merchant, statusClosed, o.total, o.version + 1}) {
		t.Errorf("index rows %+v", rows)
	}
	// 从库还没追上，所以重读仍然不一致。
	if rp := rep.Repair; rp.Upserted != 1 || rp.Remaining != 1 {
		t.Errorf("repair %+v", rp)
	}
}
