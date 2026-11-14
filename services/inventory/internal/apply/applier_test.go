package apply_test

import (
	"context"
	"database/sql"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/inventory"
	"github.com/palemoky/oppenheimer/services/inventory/internal/apply"
	"github.com/palemoky/oppenheimer/services/inventory/internal/store"
)

type fixture struct {
	db    *sql.DB
	store *store.Store
	now   atomic.Pointer[time.Time]
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	db := testdb.New(t, inventory.Migrations, inventory.MigrationsDir)
	f := &fixture{db: db, store: store.New(db)}
	start := time.Now()
	f.now.Store(&start)
	return f
}

func (f *fixture) clock() time.Time { return *f.now.Load() }

func (f *fixture) advance(d time.Duration) {
	t := f.clock().Add(d)
	f.now.Store(&t)
}

func (f *fixture) applier(t *testing.T, batch int) *apply.Applier {
	t.Helper()
	a, err := apply.New(f.store, apply.Config{Batch: batch, Now: f.clock})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (f *fixture) addSKU(t *testing.T, sku, available int64) {
	t.Helper()
	testdb.MustExec(t, f.db, `INSERT INTO sku_stock (sku_id, name, unit_price, available) VALUES (?, 'x', 100, ?)`, sku, available)
}

// raw 直接读 sku_stock，不加未应用的增量。
func (f *fixture) raw(t *testing.T, sku int64) (available, reserved, sold int64) {
	t.Helper()
	if err := f.db.QueryRowContext(t.Context(), `SELECT available, reserved, sold FROM sku_stock WHERE sku_id = ?`, sku).
		Scan(&available, &reserved, &sold); err != nil {
		t.Fatal(err)
	}
	return
}

func (f *fixture) pendingRows(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM stock_pending`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (f *fixture) pend(ctx context.Context, t testing.TB, fn func(tx *store.Tx) error) {
	t.Helper()
	if err := f.store.InTx(context.WithoutCancel(ctx), fn); err != nil {
		t.Errorf("pend: %v", err)
	}
}

func (f *fixture) effective(t *testing.T, sku int64) store.Stock {
	t.Helper()
	s, found, err := f.store.GetStock(t.Context(), sku)
	if err != nil || !found {
		t.Fatalf("GetStock(%d) = found %v, %v", sku, found, err)
	}
	return s
}

// 一批增量按 SKU 合并成一次更新；应用之后增量被删掉，再应用一次什么都不发生。
func TestApplyMergesPerSKUExactlyOnce(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	for sku := int64(1); sku <= 3; sku++ {
		f.addSKU(t, sku, 1000)
	}
	for i := range 300 {
		sku := int64(i%3) + 1
		f.pend(t.Context(), t, func(tx *store.Tx) error { return tx.PendReserve(t.Context(), int64(i+1), sku, 2) })
	}
	// 应用之前：sku_stock 没动，读真实的数已经扣掉了。
	if a, r, _ := f.raw(t, 1); a != 1000 || r != 0 {
		t.Fatalf("raw sku_stock moved before applying: available %d reserved %d", a, r)
	}
	if s := f.effective(t, 1); s.Available != 800 || s.Reserved != 200 {
		t.Fatalf("effective stock = available %d reserved %d, want 800 and 200", s.Available, s.Reserved)
	}

	res, err := f.store.ApplyPending(t.Context(), 1000, nil, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	if res.Fetched != 300 || res.Applied != 300 || res.SKUs != 3 || len(res.Stuck) != 0 {
		t.Fatalf("apply result = %+v, want 300 rows merged into 3 sku updates", res)
	}
	for sku := int64(1); sku <= 3; sku++ {
		if a, r, s := f.raw(t, sku); a != 800 || r != 200 || s != 0 {
			t.Fatalf("sku %d raw = %d/%d/%d, want 800/200/0", sku, a, r, s)
		}
		if s := f.effective(t, sku); s.Available != 800 || s.Reserved != 200 {
			t.Fatalf("sku %d effective changed by applying: %+v", sku, s)
		}
	}
	if n := f.pendingRows(t); n != 0 {
		t.Fatalf("%d pending rows left", n)
	}
	res, err = f.store.ApplyPending(t.Context(), 1000, nil, f.clock())
	if err != nil || res.Fetched != 0 {
		t.Fatalf("second apply = %+v, %v; want nothing to do", res, err)
	}
}

// 写入方和多个应用者同时运行：每笔增量恰好应用一次，任何时刻读到的真实的数都满足恒等式且不为负，
// sku_stock 自己的数也从不为负（先预占后释放的先后被保持）。
func TestConcurrentWritersAndAppliers(t *testing.T) {
	const (
		skus    = 4
		initial = 100000
		writers = 8
		orders  = 150 // 每个写入方
		qty     = 3
	)
	f := newFixture(t)
	for sku := int64(1); sku <= skus; sku++ {
		f.addSKU(t, sku, initial)
	}
	ctx := t.Context()

	var (
		stop     atomic.Bool
		appliers sync.WaitGroup
		readers  sync.WaitGroup
	)
	for range 3 {
		appliers.Go(func() {
			for !stop.Load() {
				// 小批量，让“一个订单的预占和释放落在不同批里”经常发生。
				res, err := f.store.ApplyPending(ctx, 7, nil, time.Now())
				if err != nil {
					t.Errorf("apply: %v", err)
					return
				}
				// 库存充足，没有任何 SKU 应该卡住。出现了就是增量的先后被打乱（例如释放先于它的预占被应用），
				// 某个数出现了一瞬间的负值。它之后会自己恢复，但每次都是一条误报的“超卖”。
				if len(res.Stuck) > 0 {
					t.Errorf("applier reported stuck skus with plenty of stock: %+v", res.Stuck)
					return
				}
			}
		})
	}
	readers.Go(func() {
		for !stop.Load() {
			for sku := int64(1); sku <= skus; sku++ {
				s, _, err := f.store.GetStock(ctx, sku)
				if err != nil {
					t.Errorf("GetStock: %v", err)
					return
				}
				if s.Available+s.Reserved+s.Sold != initial || s.Available < 0 || s.Reserved < 0 || s.Sold < 0 {
					t.Errorf("sku %d effective = %d/%d/%d, breaks the invariant (total %d)", sku, s.Available, s.Reserved, s.Sold, initial)
					return
				}
			}
		}
	})

	// 每个订单：预占，然后一半确认、一半释放。三步是三个事务，和真实的路径一样。
	var writersWG sync.WaitGroup
	var confirmed [skus + 1]atomic.Int64
	for w := range writers {
		writersWG.Go(func() {
			for i := range orders {
				order := int64(w*orders + i + 1)
				sku := order%skus + 1
				f.pend(ctx, t, func(tx *store.Tx) error { return tx.PendReserve(ctx, order, sku, qty) })
				if order%2 == 0 {
					f.pend(ctx, t, func(tx *store.Tx) error { return tx.PendConfirm(ctx, order, sku, qty) })
					confirmed[sku].Add(qty)
				} else {
					f.pend(ctx, t, func(tx *store.Tx) error { return tx.PendRelease(ctx, order, sku, qty) })
				}
			}
		})
	}
	writersWG.Wait()
	stop.Store(true)
	appliers.Wait()
	readers.Wait()

	if err := f.applier(t, 500).Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n := f.pendingRows(t); n != 0 {
		t.Fatalf("%d pending rows left after draining", n)
	}
	for sku := int64(1); sku <= skus; sku++ {
		sold := confirmed[sku].Load()
		if a, r, s := f.raw(t, sku); a != initial-sold || r != 0 || s != sold {
			t.Errorf("sku %d raw = %d/%d/%d, want %d/0/%d", sku, a, r, s, initial-sold, sold)
		}
	}
}

// 闸门放行的数量超过 MySQL 里有的：那个 SKU 的增量不应用、原样留着，其他 SKU 照常；
// 读真实的数如实给出负数；库存补上之后自动恢复。
func TestOversoldSKUIsStuckAndOthersProceed(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.addSKU(t, 1, 2) // 只有 2 件
	f.addSKU(t, 2, 100)
	for i := range 5 {
		f.pend(t.Context(), t, func(tx *store.Tx) error { return tx.PendReserve(t.Context(), int64(i+1), 1, 1) })
		f.pend(t.Context(), t, func(tx *store.Tx) error { return tx.PendReserve(t.Context(), int64(100+i), 2, 1) })
	}
	a := f.applier(t, 100)
	if err := a.Round(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := a.Stuck(); !slices.Equal(got, []int64{1}) {
		t.Fatalf("stuck = %v, want [1]", got)
	}
	if av, r, _ := f.raw(t, 1); av != 2 || r != 0 {
		t.Fatalf("stuck sku was partially applied: raw %d/%d", av, r)
	}
	if av, r, _ := f.raw(t, 2); av != 95 || r != 5 {
		t.Fatalf("healthy sku raw = %d/%d, want 95/5", av, r)
	}
	if n := f.pendingRows(t); n != 5 {
		t.Fatalf("%d pending rows, want the 5 of the stuck sku", n)
	}
	if s := f.effective(t, 1); s.Available != -3 || s.Reserved != 5 {
		t.Fatalf("effective stock of the oversold sku = %d/%d, want -3/5 (the truth, not clamped)", s.Available, s.Reserved)
	}

	// 卡住期间别的 SKU 的新增量照常应用，卡住的那几行不会挡在前面。
	f.pend(t.Context(), t, func(tx *store.Tx) error { return tx.PendReserve(t.Context(), 200, 2, 1) })
	if err := a.Round(t.Context()); err != nil {
		t.Fatal(err)
	}
	if av, _, _ := f.raw(t, 2); av != 94 {
		t.Fatalf("healthy sku available = %d while another sku is stuck, want 94", av)
	}

	// 人把库存补上；到了重试的时间，增量被应用，SKU 恢复。
	testdb.MustExec(t, f.db, `UPDATE sku_stock SET available = 10 WHERE sku_id = 1`)
	if err := a.Round(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(a.Stuck()) != 1 {
		t.Fatal("stuck sku was retried before the retry interval")
	}
	f.advance(apply.DefaultStuckRetry + time.Millisecond)
	if err := a.Round(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(a.Stuck()) != 0 || f.pendingRows(t) != 0 {
		t.Fatalf("after fixing the stock: stuck %v, %d pending rows", a.Stuck(), f.pendingRows(t))
	}
	if av, r, _ := f.raw(t, 1); av != 5 || r != 5 {
		t.Fatalf("recovered sku raw = %d/%d, want 5/5", av, r)
	}
}

// sku_stock 里没有这一行的增量同样卡住，不会被悄悄删掉。
func TestDeltaForMissingSKUIsKept(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.addSKU(t, 1, 10)
	f.pend(t.Context(), t, func(tx *store.Tx) error { return tx.PendReserve(t.Context(), 1, 1, 1) })
	f.pend(t.Context(), t, func(tx *store.Tx) error { return tx.PendReserve(t.Context(), 2, 99, 1) })
	// 同一批里先预占后释放，合计为零：也要能发现行不存在。
	f.pend(t.Context(), t, func(tx *store.Tx) error { return tx.PendReserve(t.Context(), 3, 98, 1) })
	f.pend(t.Context(), t, func(tx *store.Tx) error { return tx.PendRelease(t.Context(), 3, 98, 1) })

	res, err := f.store.ApplyPending(t.Context(), 100, nil, f.clock())
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 1 || len(res.Stuck) != 2 || res.Stuck[0].SKUID != 98 || res.Stuck[1].SKUID != 99 {
		t.Fatalf("apply result = %+v, want 1 applied and skus 98, 99 stuck", res)
	}
	if n := f.pendingRows(t); n != 3 {
		t.Fatalf("%d pending rows, want the 3 of the missing skus", n)
	}
}
