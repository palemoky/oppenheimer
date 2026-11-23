package stock

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/inventory"
)

// fixture 是一个真实结构的库存库，加上按库存服务的写法（同一个事务里改状态和记增量）写数据的几个动作。
// 这里不能直接用库存服务的代码（它在另一个模块的 internal 里），所以这些动作是照着它写的 SQL；
// “状态和增量在同一个事务里”这个前提本身由设计文档 3e 第 9 节“第 0 步”读代码核对。
type fixture struct {
	t    *testing.T
	db   *sql.DB
	now  time.Time
	next atomic.Int64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, db: testdb.New(t, inventory.Migrations, inventory.MigrationsDir), now: time.Now()}
	f.next.Store(idgen.LowerBound(f.now.Add(-time.Minute)))
	return f
}

func (f *fixture) sku(id, available int64) {
	f.t.Helper()
	testdb.MustExec(f.t, f.db, `INSERT INTO sku_stock (sku_id, name, unit_price, available) VALUES (?, 'sku', 100, ?)`, id, available)
}

// tx 跑一个事务；撞上死锁就重试（应用增量的范围锁和插入增量会互相等，库存服务自己也是重试）。
func (f *fixture) tx(fn func(ctx context.Context, tx *sql.Tx) error) error {
	ctx := f.t.Context()
	for {
		err := f.txOnce(ctx, fn)
		if err == nil || !strings.Contains(err.Error(), "Error 1213") {
			return err
		}
	}
}

func (f *fixture) txOnce(ctx context.Context, fn func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// reserve 预占：头记录、明细、增量在一个事务里。返回订单号。
func (f *fixture) reserve(sku, qty int64) (int64, error) {
	order := f.next.Add(1)
	return order, f.reserveAs(order, sku, qty)
}

func (f *fixture) reserveAs(order, sku, qty int64) error {
	return f.tx(func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO reservation (order_id, status) VALUES (?, 1)`, order); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO reservation_item (order_id, sku_id, quantity, unit_price, currency) VALUES (?, ?, ?, 100, 'CNY')`, order, sku, qty); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO stock_pending (sku_id, d_available, d_reserved, d_sold, order_id) VALUES (?, ?, ?, 0, ?)`, sku, -qty, qty, order)
		return err
	})
}

// settle 确认（to = 2）或释放（to = 3）：状态的条件更新和增量在一个事务里。
func (f *fixture) settle(order, sku, qty int64, to int) error {
	return f.tx(func(ctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE reservation SET status = ? WHERE order_id = ? AND status = 1`, to, order)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return nil // 已经结清过
		}
		if to == statusConfirmed {
			_, err = tx.ExecContext(ctx, `INSERT INTO stock_pending (sku_id, d_available, d_reserved, d_sold, order_id) VALUES (?, 0, ?, ?, ?)`, sku, -qty, qty, order)
		} else {
			_, err = tx.ExecContext(ctx, `INSERT INTO stock_pending (sku_id, d_available, d_reserved, d_sold, order_id) VALUES (?, ?, ?, 0, ?)`, sku, qty, -qty, order)
		}
		return err
	})
}

// apply 把一个 SKU 的增量合并进 sku_stock 并删掉，一个事务。
func (f *fixture) apply(sku int64) error {
	return f.tx(func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT id, d_available, d_reserved, d_sold FROM stock_pending WHERE sku_id = ? ORDER BY id FOR UPDATE`, sku)
		if err != nil {
			return err
		}
		defer rows.Close() //nolint:errcheck // 下面查过 rows.Err
		var (
			ids        []int64
			da, dr, ds int64
		)
		for rows.Next() {
			var id, a, r, s int64
			if err := rows.Scan(&id, &a, &r, &s); err != nil {
				return err
			}
			ids = append(ids, id)
			da, dr, ds = da+a, dr+r, ds+s
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sku_stock SET available = available + ?, reserved = reserved + ?, sold = sold + ? WHERE sku_id = ?`, da, dr, ds, sku); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM stock_pending WHERE sku_id = ? AND id <= ?`, sku, ids[len(ids)-1])
		return err
	})
}

func (f *fixture) must(err error) {
	f.t.Helper()
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) collect(full bool) Result {
	f.t.Helper()
	r, err := Collect(f.t.Context(), f.db, Options{Now: f.now, ReservedWindow: 2 * time.Hour, Full: full, StuckAfter: time.Hour})
	if err != nil {
		f.t.Fatal(err)
	}
	return r
}

func clean(r Result) bool {
	return len(r.Reserved) == 0 && len(r.Sold) == 0 && r.Stuck == 0
}

func TestConsistentWithAndWithoutPendingDeltas(t *testing.T) {
	f := newFixture(t)
	f.sku(1, 100)
	f.sku(2, 50)
	f.sku(3, 10) // 没有任何预占的 SKU

	a, err := f.reserve(1, 3)
	f.must(err)
	b, err := f.reserve(1, 2)
	f.must(err)
	c, err := f.reserve(2, 5)
	f.must(err)
	f.must(f.settle(a, 1, 3, statusConfirmed))
	f.must(f.settle(c, 2, 5, 3))
	_ = b // 仍然是 RESERVED

	// 增量全都没应用：计数要加上它们才对得上。
	if r := f.collect(true); !clean(r) || r.SKUs != 3 {
		t.Fatalf("before apply: %+v", r)
	}
	f.must(f.apply(1))
	// 应用了一半（SKU 2 的还没应用）。
	if r := f.collect(true); !clean(r) {
		t.Fatalf("half applied: %+v", r)
	}
	f.must(f.apply(2))
	if r := f.collect(true); !clean(r) {
		t.Fatalf("after apply: %+v", r)
	}
	if r := f.collect(false); !clean(r) || r.Full {
		t.Fatalf("quick round: %+v", r)
	}
}

func TestDefectsThatKeepTheTotalButBreakTheLedger(t *testing.T) {
	t.Run("delta applied twice", func(t *testing.T) {
		f := newFixture(t)
		f.sku(1, 100)
		_, err := f.reserve(1, 4)
		f.must(err)
		f.must(f.apply(1))
		// 同一条增量又应用了一次：总量不变（CHECK 拦不住），预占数多了 4。
		testdb.MustExec(t, f.db, `UPDATE sku_stock SET available = available - 4, reserved = reserved + 4 WHERE sku_id = 1`)
		r := f.collect(true)
		if len(r.Reserved) != 1 || r.Reserved[0] != (Diff{SKU: 1, Counter: 8, Ledger: 4}) || len(r.Sold) != 0 {
			t.Fatalf("%+v", r)
		}
		if Quantity(r.Reserved) != 4 {
			t.Errorf("quantity = %d", Quantity(r.Reserved))
		}
	})
	t.Run("delta lost", func(t *testing.T) {
		f := newFixture(t)
		f.sku(1, 100)
		a, err := f.reserve(1, 4)
		f.must(err)
		f.must(f.apply(1))
		f.must(f.settle(a, 1, 4, statusConfirmed))
		testdb.MustExec(t, f.db, `DELETE FROM stock_pending`) // 确认的那条增量丢了
		r := f.collect(true)
		// 流水说这 4 件已售、不再预占；计数还停在预占上。
		if len(r.Reserved) != 1 || r.Reserved[0] != (Diff{SKU: 1, Counter: 4, Ledger: 0}) ||
			len(r.Sold) != 1 || r.Sold[0] != (Diff{SKU: 1, Counter: 0, Ledger: 4}) {
			t.Fatalf("%+v", r)
		}
		// 快的一轮只看预占那一条。
		if q := f.collect(false); len(q.Reserved) != 1 || len(q.Sold) != 0 {
			t.Fatalf("quick: %+v", q)
		}
	})
	t.Run("status changed without a delta", func(t *testing.T) {
		f := newFixture(t)
		f.sku(1, 100)
		a, err := f.reserve(1, 2)
		f.must(err)
		testdb.MustExec(t, f.db, `UPDATE reservation SET status = 3 WHERE order_id = ?`, a) // 释放了却没有归还
		r := f.collect(true)
		if len(r.Reserved) != 1 || r.Reserved[0] != (Diff{SKU: 1, Counter: 2, Ledger: 0}) || len(r.Sold) != 0 {
			t.Fatalf("%+v", r)
		}
	})
	t.Run("ledger has a sku that sku_stock does not", func(t *testing.T) {
		f := newFixture(t)
		f.sku(1, 100)
		_, err := f.reserve(9, 1)
		f.must(err)
		testdb.MustExec(t, f.db, `DELETE FROM stock_pending`)
		r := f.collect(true)
		if len(r.Reserved) != 1 || r.Reserved[0] != (Diff{SKU: 9, Ledger: 1, NoStockRow: true}) {
			t.Fatalf("%+v", r)
		}
	})
}

func TestStuckReservation(t *testing.T) {
	f := newFixture(t)
	f.sku(1, 100)
	// 三个小时前预占的，一直没有确认也没有释放。
	old := idgen.LowerBound(f.now.Add(-3*time.Hour)) + 1
	f.must(f.reserveAs(old, 1, 2))
	f.must(f.apply(1))
	_, err := f.reserve(1, 1) // 一笔正常的在途预占
	f.must(err)

	// 快的一轮看不到窗口之外的那一笔：计数是 3，窗口里的流水只有 1。
	q := f.collect(false)
	if len(q.Reserved) != 1 || q.Reserved[0] != (Diff{SKU: 1, Counter: 3, Ledger: 1}) {
		t.Fatalf("quick: %+v", q)
	}
	// 完整的一轮说明原因：有一笔卡住的预占。
	r := f.collect(true)
	if r.Stuck != 1 || r.OldestStuck != old || len(r.Sold) != 0 {
		t.Fatalf("full: %+v", r)
	}
}

// 一边不停地预占、确认、释放、应用增量，一边反复核对：核对不能报任何差异。
// 这条测的就是“一个快照里没有在途的量”：读的不是同一个快照时，几乎每一轮都会抓到写了一半的状态。
func TestNoFalsePositivesUnderConcurrentWrites(t *testing.T) {
	f := newFixture(t)
	skus := []int64{1, 2, 3}
	for _, s := range skus {
		f.sku(s, 1_000_000)
	}

	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	var writes atomic.Int64
	fail := func(err error) {
		if err != nil && ctx.Err() == nil {
			t.Error(err)
			cancel()
		}
	}
	for w := range 6 {
		wg.Go(func() {
			for i := 0; ctx.Err() == nil; i++ {
				sku := skus[(w+i)%len(skus)]
				qty := int64(1 + i%3)
				order, err := f.reserve(sku, qty) //nolint:contextcheck // 夹具用测试自己的 ctx
				fail(err)
				switch i % 3 {
				case 0:
					fail(f.settle(order, sku, qty, statusConfirmed)) //nolint:contextcheck // 夹具用测试自己的 ctx
				case 1:
					fail(f.settle(order, sku, qty, 3)) //nolint:contextcheck // 夹具用测试自己的 ctx
				} // 其余的留在 RESERVED
				writes.Add(1)
			}
		})
	}
	wg.Go(func() {
		for i := 0; ctx.Err() == nil; i++ {
			fail(f.apply(skus[i%len(skus)])) //nolint:contextcheck // 夹具用测试自己的 ctx
		}
	})

	rounds := 0
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline) && ctx.Err() == nil; rounds++ {
		r, err := Collect(ctx, f.db, Options{Now: f.now, ReservedWindow: 2 * time.Hour, Full: true, StuckAfter: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		if !clean(r) {
			t.Fatalf("round %d reported differences while writers were running: %+v", rounds, r)
		}
	}
	cancel()
	wg.Wait()
	if rounds < 5 || writes.Load() < 50 {
		t.Fatalf("too little happened to mean anything: %d rounds, %d writes", rounds, writes.Load())
	}
	t.Logf("%d checks against %d concurrent writes", rounds, writes.Load())
}

func TestWatcher(t *testing.T) {
	f := newFixture(t)
	f.sku(1, 100)
	if _, err := NewWatcher(Config{}); err == nil {
		t.Error("missing database must be rejected")
	}
	if _, err := NewWatcher(Config{DB: f.db, StuckAfter: 3 * time.Hour}); err == nil {
		t.Error("stuck-after longer than the reserved window must be rejected")
	}

	clock := f.now
	w, err := NewWatcher(Config{DB: f.db, Logger: slog.New(slog.DiscardHandler), FullEvery: time.Hour, Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	if w.last.Load() != nil {
		t.Fatal("no result before the first round")
	}
	w.round(t.Context())
	st := w.last.Load()
	if st == nil || st.full == nil || !st.quick.Full {
		t.Fatalf("first round must be a full one: %+v", st)
	}
	// 一分钟后是快的一轮；完整一轮的结果留着。
	clock = clock.Add(time.Minute)
	w.round(t.Context())
	if st2 := w.last.Load(); st2.quick.Full || st2.full != st.full {
		t.Fatalf("second round: %+v", st2)
	}
	// 过了 FullEvery，又是完整的一轮。
	clock = clock.Add(time.Hour)
	w.round(t.Context())
	if st3 := w.last.Load(); !st3.quick.Full || st3.full == st.full {
		t.Fatalf("third round: %+v", st3)
	}

	// 失败的一轮不动上一次的结果，也不推进“上一次完整的一轮”。
	before := w.last.Load()
	clock = clock.Add(time.Hour)
	canceled, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
	defer cancel()
	<-canceled.Done()
	if _, err := w.Once(canceled, true); err == nil {
		t.Fatal("want an error from a dead context")
	}
	if w.last.Load() != before {
		t.Error("a failed round replaced the last result")
	}
}
