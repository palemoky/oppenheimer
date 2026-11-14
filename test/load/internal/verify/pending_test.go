package verify

import (
	"errors"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/pkg/testredis"
)

func hasViol(res *Result, check int, subject, frag string) bool {
	for _, v := range res.Violations {
		if v.Check == check && (subject == "" || v.Subject == subject) && strings.Contains(v.Detail, frag) {
			return true
		}
	}
	return false
}

func TestStockPendingCleanWorldPasses(t *testing.T) {
	in := world()
	in.Snapshot.Pending = map[int64]PendingSKU{}
	in.Snapshot.RawStock = map[int64]SKUStock{1001: {SKUID: 1001, Available: 8, Sold: 2}}
	if res := Check(in); res.Failed() {
		t.Fatalf("unexpected violations: %v", res.Violations)
	}
}

// 有未应用的增量但账是平的：真实的数与订单对得上，只有第 22 条失败；原始值不平衡（恒等式）仍会被第 1 条报出。
func TestStockPendingUnappliedButBalanced(t *testing.T) {
	in := world()
	// 真实数 available 8 sold 2；sku_stock 里还停在“订单 1 的确认没有应用”：available 8 reserved 2 sold 0，
	// 增量 d_reserved -2、d_sold +2 还在队列里。
	in.Snapshot.RawStock = map[int64]SKUStock{1001: {SKUID: 1001, Available: 8, Reserved: 2}}
	in.Snapshot.Pending = map[int64]PendingSKU{1001: {SKUID: 1001, Rows: 1, DReserved: -2, DSold: 2, Oldest: "2026-01-01 00:00:00.000000", OldestAge: 3 * time.Second}}
	res := Check(in)
	if got := res.FailedChecks(); len(got) != 1 || got[0] != CheckStockPending {
		t.Fatalf("failed checks = %v, want only %d: %v", got, CheckStockPending, res.Violations)
	}
	if !hasViol(res, CheckStockPending, "sku 1001", "would not make a number negative") ||
		!hasViol(res, CheckStockPending, "sku 1001", "oldest written 2026-01-01") {
		t.Errorf("violations = %v", res.Violations)
	}
}

func TestStockPendingOversoldIsReported(t *testing.T) {
	in := world()
	// 真实数 available -1：闸门多放行了 1 件。sku_stock 里 available 0，队列里还有 d_available -1。
	in.Snapshot.Stock = map[int64]SKUStock{1001: {SKUID: 1001, Available: -1, Reserved: 1, Sold: 2}}
	in.Snapshot.RawStock = map[int64]SKUStock{1001: {SKUID: 1001, Available: 0, Sold: 2}}
	in.Snapshot.Initial = map[int64]int64{1001: 2}
	in.Snapshot.Pending = map[int64]PendingSKU{
		1001: {SKUID: 1001, Rows: 2, DAvailable: -1, DReserved: 1, Oldest: "2026-01-01 00:00:00.000000"},
		1002: {SKUID: 1002, Rows: 1, DAvailable: -1, DReserved: 1}, // 不在 sku_stock 里
	}
	res := Check(in)
	if !hasViol(res, CheckStockPending, "stock_pending", "OVERSOLD") ||
		!hasViol(res, CheckStockPending, "sku 1001", "available 0-1 = -1") ||
		!hasViol(res, CheckStockPending, "sku 1002", "not in sku_stock") {
		t.Fatalf("violations = %v", res.Violations)
	}
	if !hasViol(res, 3, "sku 1001", "negative") {
		t.Errorf("check 3 should also see the negative true value: %v", res.Violations)
	}
}

func TestStockPendingExamplesAreCapped(t *testing.T) {
	in := world()
	in.Snapshot.Pending = map[int64]PendingSKU{}
	for i := range int64(pendingExamples + 7) {
		in.Snapshot.Pending[2000+i] = PendingSKU{SKUID: 2000 + i, Rows: 1}
	}
	n := 0
	for _, v := range Check(in).Violations {
		if v.Check == CheckStockPending {
			n++
		}
	}
	if n != pendingExamples+1 { // 一条汇总加上限内的 SKU
		t.Errorf("check 22 violations = %d, want %d", n, pendingExamples+1)
	}
}

func TestStockRawIdentityViolation(t *testing.T) {
	in := world()
	in.Snapshot.RawStock = map[int64]SKUStock{1001: {SKUID: 1001, Available: 9, Sold: 2}}
	res := Check(in)
	if !hasViol(res, 1, "sku 1001", "sku_stock row itself") {
		t.Fatalf("violations = %v", res.Violations)
	}
}

// 00005 之前的旧库：只有一张最小的 sku_stock，没有 stock_pending。
const oldInventoryMigration = `-- +goose Up
CREATE TABLE sku_stock (sku_id BIGINT NOT NULL PRIMARY KEY, available BIGINT NOT NULL, reserved BIGINT NOT NULL DEFAULT 0, sold BIGINT NOT NULL DEFAULT 0);
-- +goose Down
`

func TestCollectStockPending(t *testing.T) {
	inv := newDB(t, "inventory") // 用 00005 迁移建的 stock_pending，和服务保持一致
	exec(t, inv, "INSERT INTO sku_stock (sku_id, name, unit_price, available, reserved, sold) VALUES (1001, 'x', 100, 10, 0, 0), (1002, 'y', 100, 1, 0, 0), (1003, 'z', 100, 5, 0, 0)")
	exec(t, inv, `INSERT INTO stock_pending (sku_id, d_available, d_reserved, d_sold, order_id) VALUES
		(1001, -3, 3, 0, 1), (1001, 0, -3, 3, 1),
		(1002, -5, 5, 0, 2),
		(1004, -1, 1, 0, 3)`)

	s := &Snapshot{}
	if err := collectStock(t.Context(), inv, s); err != nil {
		t.Fatal(err)
	}
	if got := s.RawStock[1001]; got.Available != 10 || got.Reserved != 0 || got.Sold != 0 {
		t.Errorf("raw 1001 = %+v", got)
	}
	if got := s.Stock[1001]; got.Available != 7 || got.Reserved != 0 || got.Sold != 3 {
		t.Errorf("true 1001 = %+v", got)
	}
	if got := s.Stock[1002]; got.Available != -4 || got.Reserved != 5 {
		t.Errorf("true 1002 = %+v", got)
	}
	if got := s.Stock[1003]; got.Available != 5 {
		t.Errorf("true 1003 = %+v", got)
	}
	if _, ok := s.Stock[1004]; ok {
		t.Error("sku 1004 is not in sku_stock, must not appear in Stock")
	}
	if p := s.Pending[1001]; p.Rows != 2 || p.DAvailable != -3 || p.DSold != 3 || p.Oldest == "" {
		t.Errorf("pending 1001 = %+v", p)
	}
	if _, ok := s.Pending[1003]; ok || len(s.Pending) != 3 {
		t.Errorf("pending = %+v", s.Pending)
	}

	// 走完整的检查：1002 应用后 available 变负。
	in := Input{Snapshot: s}
	s.Initial = map[int64]int64{1001: 10, 1002: 1, 1003: 5}
	res := Check(in)
	if !hasViol(res, CheckStockPending, "sku 1002", "available 1-5 = -4") || !hasViol(res, CheckStockPending, "sku 1004", "not in sku_stock") {
		t.Errorf("violations = %v", res.Violations)
	}
}

func TestCollectStockPendingMissingTable(t *testing.T) {
	inv := testdb.New(t, fstest.MapFS{"m/00001_init.sql": {Data: []byte(oldInventoryMigration)}}, "m")
	err := collectStock(t.Context(), inv, &Snapshot{})
	if !errors.Is(err, errNoStockPending) {
		t.Fatalf("collectStock error = %v, want errNoStockPending", err)
	}
	if _, err := probePending(t.Context(), inv); !errors.Is(err, errNoStockPending) {
		t.Fatalf("probePending error = %v, want errNoStockPending", err)
	}
}

func TestWaitQuietStockPending(t *testing.T) {
	inv, pay := newDB(t, "inventory"), newDB(t, "payment")
	rdb, prefix := testredis.New(t)
	src := Sources{InventoryDB: inv, PaymentDB: pay, OrderDBs: newOrderDBs(t), Redis: rdb, RedisPrefix: prefix}

	// 空队列：静止。
	if _, _, err := WaitQuiet(t.Context(), src, 5*time.Second, 50*time.Millisecond, discardLogger()); err != nil {
		t.Fatalf("empty stock_pending: %v", err)
	}

	// 队列不空：行数是否变化决定是继续等还是判定卡住。
	exec(t, inv, "INSERT INTO sku_stock (sku_id, name, unit_price, available) VALUES (1002, 'y', 100, 1)")
	exec(t, inv, "INSERT INTO stock_pending (sku_id, d_available, d_reserved, d_sold, order_id) VALUES (1002, -5, 5, 0, 2), (1002, 0, 0, 0, 3)")
	q, err := Probe(t.Context(), src)
	if err != nil {
		t.Fatal(err)
	}
	if q.StockPending.Rows != 2 || len(q.StockPending.SKUs) != 1 || q.StockPending.SKUs[0] != 1002 {
		t.Fatalf("stock pending = %+v", q.StockPending)
	}
	if p := q.Problems(); len(p) != 1 || !strings.Contains(p[0], "stock_pending has 2 rows not applied") || !strings.Contains(p[0], "1002") {
		t.Fatalf("problems = %v", p)
	}

	old := stuckPolls
	stuckPolls = 3
	t.Cleanup(func() { stuckPolls = old })
	start := time.Now()
	_, last, err := WaitQuiet(t.Context(), src, 30*time.Second, 20*time.Millisecond, discardLogger())
	if !errors.Is(err, ErrStockPendingStuck) {
		t.Fatalf("WaitQuiet error = %v, want ErrStockPendingStuck", err)
	}
	if time.Since(start) > 10*time.Second || last.StockPending.Rows != 2 {
		t.Errorf("took %s, last = %+v", time.Since(start), last.StockPending)
	}
}
