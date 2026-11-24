package reconcile

import (
	"database/sql"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/pkg/testredis"
	"github.com/palemoky/oppenheimer/services/inventory"
	"github.com/palemoky/oppenheimer/services/inventory/internal/store"
)

const interval = 5 * time.Second

type env struct {
	t    *testing.T
	db   *sql.DB
	gate *stockgate.Gate
	r    *Reconciler
	// clock 是对账器看到的时间，每轮推进一个间隔，模拟按时运行的相邻轮次。
	clock time.Time
}

func newEnv(t *testing.T) *env {
	db := testdb.New(t, inventory.Migrations, inventory.MigrationsDir)
	rdb, prefix := testredis.New(t)
	gate := stockgate.New(rdb, prefix)
	e := &env{t: t, db: db, gate: gate, clock: time.Unix(1_700_000_000, 0)}
	e.r = New(gate, store.New(db), Config{
		Interval: interval,
		Logger:   slog.New(slog.NewTextHandler(t.Output(), nil)),
	})
	e.r.now = func() time.Time { return e.clock }
	return e
}

// seed 入库一个 SKU 并按 MySQL 的 available 装载进闸门，和预热做的事一样。
func (e *env) seed(sku, available int64) {
	e.t.Helper()
	testdb.MustExec(e.t, e.db, `INSERT INTO sku_stock (sku_id, name, unit_price, available) VALUES (?, 'sku', 100, ?)`, sku, available)
	if _, err := e.gate.Load(e.t.Context(), sku, available, 0); err != nil {
		e.t.Fatal(err)
	}
}

// setMySQL 直接改 MySQL 的数量，制造偏差（模拟漏掉的结清、丢数据等）。
func (e *env) setMySQL(sku, available, reserved int64) {
	e.t.Helper()
	testdb.MustExec(e.t, e.db, `UPDATE sku_stock SET available = ?, reserved = ?, stocked = available + reserved + sold WHERE sku_id = ?`, available, reserved, sku)
}

// round 推进一个间隔并执行一轮。
func (e *env) round() {
	e.t.Helper()
	e.clock = e.clock.Add(interval)
	if err := e.r.Round(e.t.Context()); err != nil {
		e.t.Fatalf("round: %v", err)
	}
}

func (e *env) avail(sku int64) int64 {
	e.t.Helper()
	st, ok, err := e.gate.Stock(e.t.Context(), sku)
	if err != nil || !ok {
		e.t.Fatalf("gate stock %d: loaded %v, err %v", sku, ok, err)
	}
	return st.Avail
}

func (e *env) assertAvail(step string, sku, want int64) {
	e.t.Helper()
	if got := e.avail(sku); got != want {
		e.t.Errorf("%s: avail = %d, want %d", step, got, want)
	}
}

func TestTransientDeviationNotCorrected(t *testing.T) {
	e := newEnv(t)
	e.seed(1, 10)

	// 例如 Confirm 已提交、Settle 还没执行：MySQL 一侧少了 1，只持续两轮就恢复。
	e.setMySQL(1, 9, 0)
	e.round()
	e.round()
	e.setMySQL(1, 10, 0)
	for i := range 5 {
		e.round()
		e.assertAvail("round "+strconv.Itoa(i), 1, 10)
	}
}

func TestStableDeviationCorrectedOnThirdRound(t *testing.T) {
	e := newEnv(t)
	e.seed(1, 10)
	e.seed(2, 10)

	// SKU 1 偏大 2（闸门比 MySQL 多），SKU 2 偏小 3。
	e.setMySQL(1, 8, 0)
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET available = 13, stocked = available + reserved + sold WHERE sku_id = 2`)

	e.round()
	e.round()
	e.assertAvail("round 2", 1, 10)
	e.assertAvail("round 2", 2, 10)
	e.round()
	e.assertAvail("round 3", 1, 8)
	e.assertAvail("round 3", 2, 13)
	// 修正之后偏差为 0，之后的轮次什么都不做。
	for range 4 {
		e.round()
	}
	e.assertAvail("after", 1, 8)
	e.assertAvail("after", 2, 13)
}

func TestChangingDeviationRestartsCount(t *testing.T) {
	e := newEnv(t)
	e.seed(1, 10)

	steps := []struct {
		available int64 // 本轮 MySQL 的 available，偏差 = 10 - available
		avail     int64 // 本轮之后闸门的 avail
	}{
		{9, 10}, // d=1，第 1 次
		{8, 10}, // d=2，值变了，重新计数
		{9, 10}, // d=1，重新从 1 数
		{9, 10}, // d=1，第 2 次
		{9, 9},  // d=1，第 3 次，修正
	}
	for i, s := range steps {
		e.setMySQL(1, s.available, 0)
		e.round()
		e.assertAvail("step "+strconv.Itoa(i), 1, s.avail)
	}
}

func TestCorrectionCappedByAvailable(t *testing.T) {
	e := newEnv(t)
	e.seed(1, 10)

	// 闸门偏小 5，但 MySQL 一侧有 reserved：按增量修会到 10 + 5 = 15，上限截在 available。
	e.setMySQL(1, 12, 3)
	for range 3 {
		e.round()
	}
	e.assertAvail("capped", 1, 12)
}

// TestRedisDataLoss 模拟 Redis 丢了全部数据后重新装载：在途订单的占用没了，held = 0，
// MySQL 却还有它们的预占。avail 不能被调到 available 之上；在途订单陆续结束时跟着调高。
func TestRedisDataLoss(t *testing.T) {
	e := newEnv(t)
	// 入库 10，3 个在途订单各预占 1，MySQL available 7、reserved 3。重新装载取 available。
	testdb.MustExec(t, e.db, `INSERT INTO sku_stock (sku_id, name, unit_price, available, reserved) VALUES (1, 'sku', 100, 7, 3)`)
	if _, err := e.gate.Load(t.Context(), 1, 7, 0); err != nil {
		t.Fatal(err)
	}

	// d = 7 - 10 = -3，稳定，但上限是 available：avail 保持 7。
	for i := range 6 {
		e.round()
		e.assertAvail("loaded, round "+strconv.Itoa(i), 1, 7)
	}

	// 一个在途订单被取消：MySQL 释放 1；闸门的归还因为占用不存在什么都不做。
	e.setMySQL(1, 8, 2)
	for range 3 {
		e.round()
	}
	e.assertAvail("one released", 1, 8)

	// 一个支付（reserved 转 sold，结清同样什么都不做），一个关闭。
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET available = 9, reserved = 0, sold = 1, stocked = available + reserved + sold WHERE sku_id = 1`)
	for range 3 {
		e.round()
	}
	e.assertAvail("all done", 1, 9)
	// 收敛之后不再变化。
	for range 3 {
		e.round()
	}
	e.assertAvail("converged", 1, 9)
}

// TestCorrectionWithConcurrentHolds：修正与并发的受理同时发生。修正是增量，不会抹掉期间的扣减，
// 最终闸门的 avail + held 恰好等于 MySQL 一侧。
func TestCorrectionWithConcurrentHolds(t *testing.T) {
	e := newEnv(t)
	e.seed(1, 200)
	// 闸门偏大 20。受理（Hold）只在 Redis 内部挪动数量，不改变偏差，所以偏差在几轮之间保持稳定。
	e.setMySQL(1, 180, 0)

	var (
		wg   sync.WaitGroup
		held atomic.Int64
		stop atomic.Bool
		next atomic.Int64
	)
	for range 8 {
		wg.Go(func() {
			for !stop.Load() && next.Load() < 150 {
				id := next.Add(1)
				res, err := e.gate.Hold(t.Context(), stockgate.HoldRequest{
					OrderID: id, UserID: id, IdempotencyKey: "k" + strconv.FormatInt(id, 10), RequestHash: []byte("h"),
					Items: []stockgate.Item{{SKUID: 1, Quantity: 1}}, Now: time.Now(), IdempotencyTTL: time.Hour,
				})
				if err != nil {
					t.Errorf("hold: %v", err)
					return
				}
				if res.Outcome == stockgate.Held {
					held.Add(1)
				}
			}
		})
	}
	for range 3 {
		e.round()
	}
	stop.Store(true)
	wg.Wait()

	st, _, err := e.gate.Stock(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if st.Held != held.Load() {
		t.Errorf("held = %d, want %d", st.Held, held.Load())
	}
	if st.Avail+st.Held != 180 {
		t.Errorf("avail + held = %d + %d, want 180", st.Avail, st.Held)
	}
}

func TestSKUMissingInMySQLNotTouched(t *testing.T) {
	e := newEnv(t)
	if _, err := e.gate.Load(t.Context(), 42, 5, 0); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		e.round()
	}
	e.assertAvail("missing in mysql", 42, 5)
}

// TestSkippedRoundsResetCount：中间有轮次没跑（失去 leader 一段时间、进程停顿），
// 之前的观测和现在的不相邻，不能拼成“连续 3 轮”。
func TestSkippedRoundsResetCount(t *testing.T) {
	e := newEnv(t)
	e.seed(1, 10)
	e.setMySQL(1, 9, 0)

	e.round()
	e.round()
	e.clock = e.clock.Add(10 * interval)
	e.round()
	e.assertAvail("after gap", 1, 10)
	e.round()
	e.round()
	e.assertAvail("three adjacent rounds", 1, 9)
}

func TestLoop(t *testing.T) {
	e := newEnv(t)
	e.seed(1, 10)
	e.setMySQL(1, 6, 0)

	r := New(e.gate, store.New(e.db), Config{Interval: 10 * time.Millisecond, Logger: e.r.logger})
	loop, err := NewLoop(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- loop.Start(t.Context()) }()

	deadline := time.Now().Add(5 * time.Second)
	for e.avail(1) != 6 {
		if time.Now().After(deadline) {
			t.Fatalf("avail = %d, not corrected in time", e.avail(1))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := loop.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// 防止：在 MySQL 里改了限购数量，闸门却一直用旧值（订单服务只在 SKU 未装载时才触发装载）。
// 同步限购不能动闸门里的数量。
func TestPurchaseLimitSynced(t *testing.T) {
	e := newEnv(t)
	e.seed(1, 10)
	if _, err := e.gate.Hold(t.Context(), stockgate.HoldRequest{
		OrderID: 7, UserID: 100, IdempotencyKey: "k", RequestHash: []byte("h"),
		Items: []stockgate.Item{{SKUID: 1, Quantity: 4}}, Now: time.Now(), IdempotencyTTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	testdb.MustExec(t, e.db, `UPDATE sku_stock SET purchase_limit = 3 WHERE sku_id = 1`)

	e.round()
	st, _, err := e.gate.Stock(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	// 占用还没落到 MySQL（available 仍是 10），闸门里是 avail=6 held=4：恒等式成立，数量不应被改动。
	if st.Limit != 3 || st.Avail != 6 || st.Held != 4 {
		t.Fatalf("gate stock = %+v, want limit=3 avail=6 held=4", st)
	}
}
