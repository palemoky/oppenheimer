package service

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/pkg/testredis"
	"github.com/palemoky/oppenheimer/services/inventory"
	"github.com/palemoky/oppenheimer/services/inventory/internal/apply"
	"github.com/palemoky/oppenheimer/services/inventory/internal/store"
)

type env struct {
	db    *sql.DB
	store *store.Store
	svc   *Service
	gate  *stockgate.Gate
	rdb   redis.UniversalClient

	// totals 是 seed 记下的每个 SKU 的入库总量，测试结束时的检查用它核对 sku_stock 的原始值。
	totals map[int64]int64
	// wantStuck 是测试显式声明“结束时应当卡住”的 SKU，见 expectStuck。
	wantStuck []int64
}

// testMaxOpenConns 是测试库连接池的上限。并发的测试一次起几十个 goroutine，
// 不设上限时每个都会新开连接，几个包并行跑时会撞上 MySQL 的 max_connections。
const testMaxOpenConns = 40

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testdb.New(t, inventory.Migrations, inventory.MigrationsDir)
	db.SetMaxOpenConns(testMaxOpenConns)
	rdb, prefix := testredis.New(t)
	gate := stockgate.New(rdb, prefix)
	st := store.New(db)
	e := &env{
		db:     db,
		store:  st,
		svc:    New(st, gate, slog.New(slog.NewTextHandler(t.Output(), nil))),
		gate:   gate,
		rdb:    rdb,
		totals: map[int64]int64{},
	}
	// 测试环境里没有后台的应用者，预占、确认、释放只留下 stock_pending 里的增量（ADR-0018）。
	// 每个测试结束时把增量全部应用一遍，再核对 sku_stock 的原始值：这给每个测试都加了一条
	// “应用之后账是平的”的检查。断言大多经 GetStock 读真实的数，看不出增量能不能真的应用上
	// （例如某个数会被应用成负数而卡住），这一步补上。
	// 在 newEnv 里注册，比 seed 注册的检查晚执行（Cleanup 后注册的先执行），两者互不影响。
	t.Cleanup(func() { e.checkApplied(t) })
	return e
}

// noRetry 关掉死锁重试。并发测试用它证明场景本身不会死锁，而不是靠重试掩盖。
func (e *env) noRetry() { e.svc.txRetry.MaxAttempts = 1 }

// endApplyBatch 是测试结束时应用增量所用的批大小。取得很小，让同一个订单的预占和释放、
// 确认经常落在不同的批里：设计要求先后被保持时，无论怎么分批 sku_stock 都不会出现负数。
const endApplyBatch = 3

// applier 返回一个新的应用者。每次都是新的，不记得之前卡住的 SKU。
func (e *env) applier(t *testing.T, batch int) *apply.Applier {
	t.Helper()
	a, err := apply.New(e.store, apply.Config{Batch: batch, Logger: slog.New(slog.NewTextHandler(t.Output(), nil))})
	if err != nil {
		t.Fatalf("new applier: %v", err)
	}
	return a
}

// applyAll 把当前全部增量应用到 sku_stock，并断言没有 SKU 卡住、stock_pending 被清空。
// 用于测试中途“让应用者追平”之后再断言原始值。期望卡住的测试直接用 applier 和 Drain。
func (e *env) applyAll(t *testing.T) {
	t.Helper()
	a := e.applier(t, apply.DefaultBatch)
	if err := a.Drain(t.Context()); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if stuck := a.Stuck(); len(stuck) > 0 {
		t.Fatalf("stuck skus after draining: %v", stuck)
	}
	if n := e.pendingRows(t, 0); n != 0 {
		t.Fatalf("%d stock_pending rows left after draining", n)
	}
}

// expectStuck 声明这个测试结束时 skus 应当卡住（MySQL 里的库存不够应用它们的增量）。
// 测试结束时的检查会核对卡住的恰好是这些 SKU，且它们的增量原样留在表里。
func (e *env) expectStuck(skus ...int64) {
	e.wantStuck = slices.Sorted(slices.Values(append(e.wantStuck, skus...)))
}

// checkApplied 是每个测试结束时的检查，见 newEnv。
func (e *env) checkApplied(t *testing.T) {
	t.Helper()
	// Cleanup 运行前 t.Context() 已被取消，这里只需要它携带的值。
	ctx := context.WithoutCancel(t.Context())
	a := e.applier(t, endApplyBatch)
	if err := a.Drain(ctx); err != nil {
		t.Errorf("end of test: drain: %v", err)
		return
	}
	if stuck := a.Stuck(); !slices.Equal(stuck, e.wantStuck) {
		t.Errorf("end of test: stuck skus = %v, want %v", stuck, e.wantStuck)
	}
	// 留在表里的增量只能属于卡住的 SKU。
	left, err := e.pendingSKUs(ctx)
	if err != nil {
		t.Errorf("end of test: %v", err)
		return
	}
	if !slices.Equal(left, e.wantStuck) {
		t.Errorf("end of test: stock_pending has rows of skus %v, want only the stuck %v", left, e.wantStuck)
	}

	// sku_stock 的原始值：不为负（CHECK 约束本来也保证），总量没变。
	// 每笔增量自身三项之和为零，所以卡住的 SKU 的原始总量也必须等于入库量。
	raws, err := e.db.QueryContext(ctx, `SELECT sku_id, available, reserved, sold FROM sku_stock ORDER BY sku_id`)
	if err != nil {
		t.Errorf("end of test: select sku_stock: %v", err)
		return
	}
	defer raws.Close()
	for raws.Next() {
		var sku, av, re, so int64
		if err := raws.Scan(&sku, &av, &re, &so); err != nil {
			t.Errorf("end of test: scan sku_stock: %v", err)
			return
		}
		if av < 0 || re < 0 || so < 0 {
			t.Errorf("end of test: sku %d raw = %d/%d/%d, negative", sku, av, re, so)
		}
		if total, ok := e.totals[sku]; ok && av+re+so != total {
			t.Errorf("end of test: sku %d raw available+reserved+sold = %d, want %d", sku, av+re+so, total)
		}
	}
	if err := raws.Err(); err != nil {
		t.Errorf("end of test: iterate sku_stock: %v", err)
	}
}

// pendingSKUs 返回 stock_pending 里有行的 SKU，升序。
func (e *env) pendingSKUs(ctx context.Context) ([]int64, error) {
	rows, err := e.db.QueryContext(ctx, `SELECT DISTINCT sku_id FROM stock_pending ORDER BY sku_id`)
	if err != nil {
		return nil, fmt.Errorf("select stock_pending skus: %w", err)
	}
	defer rows.Close()
	var skus []int64
	for rows.Next() {
		var sku int64
		if err := rows.Scan(&sku); err != nil {
			return nil, fmt.Errorf("scan stock_pending sku: %w", err)
		}
		skus = append(skus, sku)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stock_pending skus: %w", err)
	}
	return skus, nil
}

// raw 直接读 sku_stock，不加未应用的增量。
func (e *env) raw(t *testing.T, sku int64) (available, reserved, sold int64) {
	t.Helper()
	if err := e.db.QueryRowContext(t.Context(), `SELECT available, reserved, sold FROM sku_stock WHERE sku_id = ?`, sku).
		Scan(&available, &reserved, &sold); err != nil {
		t.Fatalf("raw sku_stock %d: %v", sku, err)
	}
	return available, reserved, sold
}

// assertRaw 断言 sku_stock 的原始值。
func (e *env) assertRaw(t *testing.T, sku, available, reserved, sold int64) {
	t.Helper()
	if a, r, s := e.raw(t, sku); a != available || r != reserved || s != sold {
		t.Errorf("sku %d raw = (%d, %d, %d), want (%d, %d, %d)", sku, a, r, s, available, reserved, sold)
	}
}

// pendingRows 返回 stock_pending 的行数；orderID > 0 时只数这个订单的。
func (e *env) pendingRows(t *testing.T, orderID int64) int {
	t.Helper()
	q, args := `SELECT COUNT(*) FROM stock_pending`, []any{}
	if orderID > 0 {
		q, args = q+` WHERE order_id = ?`, append(args, orderID)
	}
	var n int
	if err := e.db.QueryRowContext(t.Context(), q, args...).Scan(&n); err != nil {
		t.Fatalf("count stock_pending: %v", err)
	}
	return n
}

// seed 入库一个 SKU，并在测试结束时断言它的三个量（真实的数）都不为负、总和等于入库量。
// 每个测试都经由它准备数据，因此每个测试都会检查恒等式。
func (e *env) seed(t *testing.T, skuID, price, total int64) {
	t.Helper()
	testdb.MustExec(t, e.db,
		`INSERT INTO sku_stock (sku_id, name, unit_price, available) VALUES (?, ?, ?, ?)`,
		skuID, "sku", price, total)
	e.totals[skuID] = total
	t.Cleanup(func() {
		// Cleanup 运行前 t.Context() 已被取消，这里只需要它携带的值。
		s := e.stock(context.WithoutCancel(t.Context()), t, skuID)
		// 期望卡住的 SKU 就是超卖了的，真实的 available 本来就是负的；总量仍然要对。
		if !slices.Contains(e.wantStuck, skuID) && (s.GetAvailable() < 0 || s.GetReserved() < 0 || s.GetSold() < 0) {
			t.Errorf("sku %d has negative stock: %v", skuID, s)
		}
		// 读 e.totals 而不是闭包里的 total：测试可以在中途声明入库总量变了（例如手工改库制造偏差）。
		if sum, want := s.GetAvailable()+s.GetReserved()+s.GetSold(), e.totals[skuID]; sum != want {
			t.Errorf("sku %d: available+reserved+sold = %d, want %d", skuID, sum, want)
		}
	})
}

func (e *env) stock(ctx context.Context, t *testing.T, skuID int64) *inventoryv1.GetStockResponse {
	t.Helper()
	resp, err := e.svc.GetStock(ctx, &inventoryv1.GetStockRequest{SkuId: skuID})
	if err != nil {
		t.Fatalf("GetStock(%d): %v", skuID, err)
	}
	return resp
}

func (e *env) assertStock(t *testing.T, skuID, available, reserved, sold int64) {
	t.Helper()
	s := e.stock(t.Context(), t, skuID)
	if s.GetAvailable() != available || s.GetReserved() != reserved || s.GetSold() != sold {
		t.Errorf("sku %d = (available %d, reserved %d, sold %d), want (%d, %d, %d)",
			skuID, s.GetAvailable(), s.GetReserved(), s.GetSold(), available, reserved, sold)
	}
}

func (e *env) reserve(ctx context.Context, orderID int64, items ...int64) (*inventoryv1.ReserveResponse, error) {
	req := &inventoryv1.ReserveRequest{OrderId: orderID}
	for i := 0; i+1 < len(items); i += 2 {
		req.Items = append(req.Items, &inventoryv1.ReserveItem{SkuId: items[i], Quantity: int32(items[i+1])})
	}
	return e.svc.Reserve(ctx, req)
}

func (e *env) confirm(ctx context.Context, orderID int64) error {
	_, err := e.svc.Confirm(ctx, &inventoryv1.ConfirmRequest{OrderId: orderID})
	return err
}

func (e *env) release(ctx context.Context, orderID int64) error {
	_, err := e.svc.Release(ctx, &inventoryv1.ReleaseRequest{OrderId: orderID})
	return err
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertErr(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	if status.Code(err) != code || !errx.Is(err, reason) {
		t.Fatalf("error = %v, want code %s reason %s", err, code, reason)
	}
}

// countRows 返回一个订单在 reservation、reservation_item、stock_pending 里各有几行。
func (e *env) countRows(t *testing.T, orderID int64) (reservations, items, pending int) {
	t.Helper()
	if err := e.db.QueryRowContext(t.Context(), `
		SELECT (SELECT COUNT(*) FROM reservation WHERE order_id = ?),
		       (SELECT COUNT(*) FROM reservation_item WHERE order_id = ?),
		       (SELECT COUNT(*) FROM stock_pending WHERE order_id = ?)`, orderID, orderID, orderID,
	).Scan(&reservations, &items, &pending); err != nil {
		t.Fatalf("count rows of order %d: %v", orderID, err)
	}
	return reservations, items, pending
}

// TestReserveConcurrentFlashSale：大量订单并发预占同一个 SKU。
//
// 以前（ADR-0018 之前）这里是“库存 10、60 人抢：恰好 10 个成功，其余 INSUFFICIENT_STOCK，不超卖”。
// 那时 Reserve 在事务里对 sku_stock 做 available >= ? 的条件更新，由 MySQL 这一侧阻止超卖。
// 现在 Reserve 只记增量、不看可售数，这条性质不再由 inventory 保证：它由库存闸门保证
// （order 受理时 pkg/stockgate 的 Hold 原子地扣可售，闸门没放行的订单不会来预占），不在本服务的测试范围内。
// MySQL 这一侧从“阻止”超卖变成“发现”超卖，见 TestReserveBeyondMySQLStockSucceedsAndSticks。
//
// 新语义下应当成立的是：并发预占全部成功，预占记录和增量各恰好一份；sku_stock 在应用之前不动，
// 应用之后等于初始值减去总量。
func TestReserveConcurrentFlashSale(t *testing.T) {
	e := newEnv(t)
	const stock, buyers = 100, 60
	e.seed(t, 1, 100, stock)

	errs := make([]error, buyers)
	var wg sync.WaitGroup
	for i := range buyers {
		wg.Go(func() {
			_, errs[i] = e.reserve(t.Context(), int64(1000+i), 1, 1)
		})
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("order %d: %v", 1000+i, err)
		}
	}
	for i := range buyers {
		if r, it, p := e.countRows(t, int64(1000+i)); r != 1 || it != 1 || p != 1 {
			t.Errorf("order %d: reservation %d, items %d, pending %d; want one each", 1000+i, r, it, p)
		}
	}
	e.assertRaw(t, 1, stock, 0, 0)
	e.assertStock(t, 1, stock-buyers, buyers, 0)

	e.applyAll(t)
	e.assertRaw(t, 1, stock-buyers, buyers, 0)
	e.assertStock(t, 1, stock-buyers, buyers, 0)
}

// TestReserveBeyondMySQLStockSucceedsAndSticks 钉住 ADR-0018 的语义变化（设计文档 1d 的 3.4）：
// MySQL 里的库存不够时，走真实路径的 Reserve 照常成功（以前返回 INSUFFICIENT_STOCK）；
// GetStock 如实给出负的可售数；应用者把这个 SKU 报成卡住，它的增量原样留着，其他 SKU 不受影响。
func TestReserveBeyondMySQLStockSucceedsAndSticks(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 2)
	e.seed(t, 2, 100, 100)
	for i := range 5 {
		_, err := e.reserve(t.Context(), int64(100+i), 1, 1)
		mustOK(t, err)
		_, err = e.reserve(t.Context(), int64(200+i), 2, 1)
		mustOK(t, err)
	}
	e.assertStock(t, 1, -3, 5, 0)

	a := e.applier(t, apply.DefaultBatch)
	if err := a.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := a.Stuck(); !slices.Equal(got, []int64{1}) {
		t.Fatalf("stuck = %v, want [1]", got)
	}
	e.assertRaw(t, 1, 2, 0, 0)
	e.assertRaw(t, 2, 95, 5, 0)
	if n := e.pendingRows(t, 0); n != 5 {
		t.Errorf("%d pending rows, want the 5 of the stuck sku", n)
	}
	e.assertStock(t, 1, -3, 5, 0)
	e.expectStuck(1)
}

// TestReserveReverseOrderNoDeadlock：两个 SKU 以相反的顺序出现在并发的订单里，不靠重试也不死锁。
//
// 以前要防的是 sku_stock 两行的锁按相反顺序获取（靠按 sku_id 排序消除）。ADR-0018 之后 Reserve
// 不再锁 sku_stock，这条风险本身没有了；测试保留，守的是 reservation_item 和 stock_pending 的插入
// 在任意顺序下同样不会互相死锁，以及以后有人把锁加回来时排序仍然有效。
func TestReserveReverseOrderNoDeadlock(t *testing.T) {
	e := newEnv(t)
	e.noRetry()
	const orders = 40
	e.seed(t, 1, 100, orders)
	e.seed(t, 2, 200, orders)

	errs := make([]error, orders)
	var wg sync.WaitGroup
	for i := range orders {
		wg.Go(func() {
			if i%2 == 0 {
				_, errs[i] = e.reserve(t.Context(), int64(1000+i), 1, 1, 2, 1)
			} else {
				_, errs[i] = e.reserve(t.Context(), int64(1000+i), 2, 1, 1, 1)
			}
		})
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("order %d: %v", 1000+i, err)
		}
	}
	e.assertStock(t, 1, 0, orders, 0)
	e.assertStock(t, 2, 0, orders, 0)
}

// TestReserveAllOrNothing：一个订单的预占全有或全无，失败时头记录、明细、库存增量都不留。
//
// 以前第一步是“第二个 SKU 库存不足 → 整笔失败，第一个 SKU 的扣减随之回滚”。ADR-0018 之后 Reserve
// 不再因库存不足失败，能让整笔预占失败的只剩 SKU 不存在和多商家，性质不变，改用这两种失败来验证。
// 现在失败发生在写任何增量之前，回滚的是头记录；仍然逐表确认没有留下这个订单的行，
// 防止以后有人把检查挪到写增量之后。
func TestReserveAllOrNothing(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	e.seed(t, 2, 200, 1)
	e.seed(t, 3, 300, 10)
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET merchant_id = 9 WHERE sku_id = 3`)

	failures := []struct {
		name   string
		items  []int64
		code   codes.Code
		reason string
	}{
		// 不存在的 SKU 排在后面：前面的 SKU 已经读过、校验过，仍然要整笔回滚。
		{"second sku not found", []int64{1, 5, 999, 1}, codes.NotFound, errx.ReasonSKUNotFound},
		{"mixed merchants", []int64{1, 5, 3, 1}, codes.FailedPrecondition, errx.ReasonMixedMerchants},
	}
	for _, f := range failures {
		_, err := e.reserve(t.Context(), 1, f.items...)
		assertErr(t, err, f.code, f.reason)
		if r, it, p := e.countRows(t, 1); r+it+p != 0 {
			t.Errorf("%s: order 1 left reservation %d, items %d, pending %d; want none", f.name, r, it, p)
		}
		e.assertStock(t, 1, 10, 0, 0)
		e.assertStock(t, 3, 10, 0, 0)
	}

	// 头记录也随失败回滚了，同一订单之后仍可正常预占。
	resp, err := e.reserve(t.Context(), 1, 1, 5, 2, 1)
	mustOK(t, err)
	if len(resp.GetItems()) != 2 {
		t.Fatalf("items = %v, want 2 items", resp.GetItems())
	}
	if r, it, p := e.countRows(t, 1); r != 1 || it != 2 || p != 2 {
		t.Errorf("order 1: reservation %d, items %d, pending %d; want 1, 2, 2", r, it, p)
	}
	e.assertStock(t, 1, 5, 5, 0)
	e.assertStock(t, 2, 0, 1, 0)
}

func TestReserveConcurrentDuplicate(t *testing.T) {
	e := newEnv(t)
	e.noRetry()
	e.seed(t, 1, 100, 100)
	e.seed(t, 2, 200, 100)
	const calls = 20

	resps := make([]*inventoryv1.ReserveResponse, calls)
	errs := make([]error, calls)
	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() {
			resps[i], errs[i] = e.reserve(t.Context(), 42, 1, 3, 2, 4)
		})
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if !proto.Equal(resps[i], resps[0]) {
			t.Errorf("call %d response = %v, want %v", i, resps[i], resps[0])
		}
	}
	e.assertStock(t, 1, 97, 3, 0)
	e.assertStock(t, 2, 96, 4, 0)
}

func TestReserveIdempotent(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	e.seed(t, 2, 250, 10)

	first, err := e.reserve(t.Context(), 7, 2, 1, 1, 2)
	mustOK(t, err)
	want := &inventoryv1.ReserveResponse{MerchantId: 1, Items: []*inventoryv1.ReservedItem{
		{SkuId: 1, Quantity: 2, UnitPrice: toProtoMoney(store.Price{Amount: 100, Currency: "CNY"})},
		{SkuId: 2, Quantity: 1, UnitPrice: toProtoMoney(store.Price{Amount: 250, Currency: "CNY"})},
	}}
	if !proto.Equal(first, want) {
		t.Fatalf("Reserve = %v, want %v", first, want)
	}

	// 项的顺序不同但内容相同，仍是同一个请求。
	again, err := e.reserve(t.Context(), 7, 1, 2, 2, 1)
	mustOK(t, err)
	if !proto.Equal(again, first) {
		t.Errorf("repeated Reserve = %v, want %v", again, first)
	}
	e.assertStock(t, 1, 8, 2, 0)
	e.assertStock(t, 2, 9, 1, 0)

	mismatches := map[string][]int64{
		"different quantity": {1, 3, 2, 1},
		"missing item":       {1, 2},
		"extra item":         {1, 2, 2, 1, 3, 1},
		"different sku":      {1, 2, 3, 1},
	}
	for name, items := range mismatches {
		t.Run(name, func(t *testing.T) {
			_, err := e.reserve(t.Context(), 7, items...)
			assertErr(t, err, codes.AlreadyExists, errx.ReasonReservationMismatch)
		})
	}
	e.assertStock(t, 1, 8, 2, 0)
	e.assertStock(t, 2, 9, 1, 0)

	// 确认之后重复 Reserve 仍返回原结果。
	mustOK(t, e.confirm(t.Context(), 7))
	again, err = e.reserve(t.Context(), 7, 1, 2, 2, 1)
	mustOK(t, err)
	if !proto.Equal(again, first) {
		t.Errorf("Reserve after Confirm = %v, want %v", again, first)
	}
	e.assertStock(t, 1, 8, 0, 2)
	e.assertStock(t, 2, 9, 0, 1)
}

func TestConfirmIdempotent(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	_, err := e.reserve(t.Context(), 7, 1, 3)
	mustOK(t, err)
	for range 3 {
		mustOK(t, e.confirm(t.Context(), 7))
	}
	e.assertStock(t, 1, 7, 0, 3)

	assertErr(t, e.release(t.Context(), 7), codes.FailedPrecondition, errx.ReasonReservationConfirmed)
	e.assertStock(t, 1, 7, 0, 3)
}

func TestConfirmErrors(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	assertErr(t, e.confirm(t.Context(), 7), codes.NotFound, errx.ReasonReservationNotFound)

	_, err := e.reserve(t.Context(), 8, 1, 3)
	mustOK(t, err)
	mustOK(t, e.release(t.Context(), 8))
	assertErr(t, e.confirm(t.Context(), 8), codes.FailedPrecondition, errx.ReasonReservationReleased)
	e.assertStock(t, 1, 10, 0, 0)
}

func TestReleaseIdempotent(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	_, err := e.reserve(t.Context(), 7, 1, 3)
	mustOK(t, err)
	for range 3 {
		mustOK(t, e.release(t.Context(), 7))
	}
	e.assertStock(t, 1, 10, 0, 0)

	_, err = e.reserve(t.Context(), 7, 1, 3)
	assertErr(t, err, codes.FailedPrecondition, errx.ReasonReservationReleased)
	e.assertStock(t, 1, 10, 0, 0)
}

func TestReleaseConcurrentDuplicate(t *testing.T) {
	e := newEnv(t)
	e.noRetry()
	e.seed(t, 1, 100, 10)
	_, err := e.reserve(t.Context(), 7, 1, 3)
	mustOK(t, err)

	const calls = 10
	errs := make([]error, calls)
	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() { errs[i] = e.release(t.Context(), 7) })
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("call %d: %v", i, err)
		}
	}
	e.assertStock(t, 1, 10, 0, 0)
}

func TestReleaseBeforeReserve(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	// 空回滚：没有预占也成功，并留下标记。
	mustOK(t, e.release(t.Context(), 7))
	mustOK(t, e.release(t.Context(), 7))

	// 迟到的 Reserve 被标记挡住，不会悬挂一份没人释放的库存。
	_, err := e.reserve(t.Context(), 7, 1, 3)
	assertErr(t, err, codes.FailedPrecondition, errx.ReasonReservationReleased)
	e.assertStock(t, 1, 10, 0, 0)

	assertErr(t, e.confirm(t.Context(), 7), codes.FailedPrecondition, errx.ReasonReservationReleased)
}

func TestReserveRaceWithRelease(t *testing.T) {
	e := newEnv(t)
	e.noRetry()
	const orders = 30
	e.seed(t, 1, 100, orders)

	reserveErrs := make([]error, orders)
	releaseErrs := make([]error, orders)
	var wg sync.WaitGroup
	for i := range orders {
		orderID := int64(1000 + i)
		wg.Go(func() { _, reserveErrs[i] = e.reserve(t.Context(), orderID, 1, 1) })
		wg.Go(func() { releaseErrs[i] = e.release(t.Context(), orderID) })
	}
	wg.Wait()

	for i := range orders {
		if releaseErrs[i] != nil {
			t.Errorf("order %d Release: %v", 1000+i, releaseErrs[i])
		}
		// Reserve 要么先到并随后被释放，要么被空回滚的标记挡住。
		if err := reserveErrs[i]; err != nil && !errx.Is(err, errx.ReasonReservationReleased) {
			t.Errorf("order %d Reserve: %v", 1000+i, err)
		}
	}
	// 无论谁先到，最终都不能有悬挂的预占。
	e.assertStock(t, 1, orders, 0, 0)
}

func TestConfirmRaceWithRelease(t *testing.T) {
	e := newEnv(t)
	e.noRetry()
	const orders = 30
	e.seed(t, 1, 100, orders)
	for i := range orders {
		_, err := e.reserve(t.Context(), int64(1000+i), 1, 1)
		mustOK(t, err)
	}

	confirmErrs := make([]error, orders)
	releaseErrs := make([]error, orders)
	var wg sync.WaitGroup
	for i := range orders {
		orderID := int64(1000 + i)
		wg.Go(func() { confirmErrs[i] = e.confirm(t.Context(), orderID) })
		wg.Go(func() { releaseErrs[i] = e.release(t.Context(), orderID) })
	}
	wg.Wait()

	var confirmed int64
	for i := range orders {
		cErr, rErr := confirmErrs[i], releaseErrs[i]
		switch {
		case cErr == nil && errx.Is(rErr, errx.ReasonReservationConfirmed):
			confirmed++
		case rErr == nil && errx.Is(cErr, errx.ReasonReservationReleased):
		default:
			t.Errorf("order %d: Confirm = %v, Release = %v, want exactly one to succeed", 1000+i, cErr, rErr)
		}
	}
	e.assertStock(t, 1, orders-confirmed, 0, confirmed)
}

func TestValidation(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	reserves := map[string]*inventoryv1.ReserveRequest{
		"zero order id":  {OrderId: 0, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: 1}}},
		"no items":       {OrderId: 1},
		"zero quantity":  {OrderId: 1, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: 0}}},
		"negative qty":   {OrderId: 1, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: -1}}},
		"duplicate skus": {OrderId: 1, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: 1}, {SkuId: 1, Quantity: 2}}},
	}
	for name, req := range reserves {
		t.Run("reserve "+name, func(t *testing.T) {
			_, err := e.svc.Reserve(t.Context(), req)
			assertErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
		})
	}
	assertErr(t, e.confirm(t.Context(), 0), codes.InvalidArgument, errx.ReasonInvalidArgument)
	assertErr(t, e.release(t.Context(), -1), codes.InvalidArgument, errx.ReasonInvalidArgument)
	e.assertStock(t, 1, 10, 0, 0)
}

func TestGetStock(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 199, 10)

	got := e.stock(t.Context(), t, 1)
	want := &inventoryv1.GetStockResponse{
		SkuId: 1, Available: 10,
		UnitPrice: toProtoMoney(store.Price{Amount: 199, Currency: "CNY"}),
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetStock = %v, want %v", got, want)
	}

	_, err := e.svc.GetStock(t.Context(), &inventoryv1.GetStockRequest{SkuId: 2})
	assertErr(t, err, codes.NotFound, errx.ReasonSKUNotFound)
}

// TestReserveConcurrentDuplicateRejected：同一个订单的并发重复 Reserve，插入头记录的那个回滚了。
//
// 每个插入者都回滚，排队等在唯一键上的重复请求随之继承同一个间隙锁、又都要插入，互相死锁（见 defaultTxRetry）。
// 这是唯一要靠死锁重试兜底的场景，结果仍必须全部是业务错误，且不留下任何行。
// 以前让插入者回滚的是库存不足；ADR-0018 之后 Reserve 不再因库存不足失败，改用 SKU 不存在触发同一条路径。
func TestReserveConcurrentDuplicateRejected(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 2)
	const calls = 10

	errs := make([]error, calls)
	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() { _, errs[i] = e.reserve(t.Context(), 42, 1, 3, 999, 1) })
	}
	wg.Wait()

	for i, err := range errs {
		if !errx.Is(err, errx.ReasonSKUNotFound) {
			t.Errorf("call %d: %v, want SKU_NOT_FOUND", i, err)
		}
	}
	if r, it, p := e.countRows(t, 42); r+it+p != 0 {
		t.Errorf("order 42 left reservation %d, items %d, pending %d; want none", r, it, p)
	}
	e.assertStock(t, 1, 2, 0, 0)
}

// Reserve 返回 SKU 所属的商家并记在预占头上，重复调用返回同一个值。
func TestReserveReturnsMerchant(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	e.seed(t, 2, 100, 10)
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET merchant_id = 9 WHERE sku_id IN (1, 2)`)

	first, err := e.reserve(t.Context(), 7, 1, 1, 2, 1)
	mustOK(t, err)
	if first.GetMerchantId() != 9 {
		t.Fatalf("merchant_id = %d, want 9", first.GetMerchantId())
	}
	again, err := e.reserve(t.Context(), 7, 1, 1, 2, 1)
	mustOK(t, err)
	if !proto.Equal(again, first) {
		t.Errorf("repeated Reserve = %v, want %v", again, first)
	}

	// 加 merchant_id 这一列之前写下的预占头上没有商家，重复调用时从 SKU 上取。
	testdb.MustExec(t, e.db, `UPDATE reservation SET merchant_id = NULL WHERE order_id = 7`)
	legacy, err := e.reserve(t.Context(), 7, 1, 1, 2, 1)
	mustOK(t, err)
	if !proto.Equal(legacy, first) {
		t.Errorf("Reserve on a reservation without merchant = %v, want %v", legacy, first)
	}
	e.assertStock(t, 1, 9, 1, 0)
	e.assertStock(t, 2, 9, 1, 0)
}

// 一个订单只属于一个商家：SKU 属于不同商家时整笔预占被拒绝，不留头记录，库存不变，
// 之后同一个订单号换成单商家的内容可以预占成功。
func TestReserveMixedMerchantsRejected(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	e.seed(t, 2, 100, 10)
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET merchant_id = 9 WHERE sku_id = 2`)

	_, err := e.reserve(t.Context(), 7, 1, 1, 2, 1)
	assertErr(t, err, codes.FailedPrecondition, errx.ReasonMixedMerchants)
	e.assertStock(t, 1, 10, 0, 0)
	e.assertStock(t, 2, 10, 0, 0)

	// 重复调用得到同样的拒绝。
	_, err = e.reserve(t.Context(), 7, 1, 1, 2, 1)
	assertErr(t, err, codes.FailedPrecondition, errx.ReasonMixedMerchants)

	resp, err := e.reserve(t.Context(), 7, 2, 3)
	mustOK(t, err)
	if resp.GetMerchantId() != 9 {
		t.Errorf("merchant_id = %d, want 9", resp.GetMerchantId())
	}
	e.assertStock(t, 2, 7, 3, 0)
}
