package seed

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// newOrderDBs 用 order 服务的迁移建 n 个订单库。
func newOrderDBs(t *testing.T, n int) []*sql.DB {
	t.Helper()
	r, err := sharding.New(sharding.OrderLayout(n))
	if err != nil {
		t.Fatal(err)
	}
	fsys := os.DirFS(filepath.Join("..", "..", "..", "..", "services", "order"))
	dbs := make([]*sql.DB, n)
	for i := range dbs {
		dbs[i] = testdb.NewTemplate(t, fsys, "migrations", struct{ Slots []int }{r.SlotsOf(i)})
	}
	return dbs
}

func smallParams() Params {
	p := testParams()
	p.Total = 3_000
	p.From = time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	p.To = p.From.Add(20 * day)
	return p
}

// digest 返回全部订单和明细的行数和一个与行顺序无关的校验和。
func digest(t *testing.T, dbs []*sql.DB) (orders, items int64, sum string) {
	t.Helper()
	r, err := sharding.New(sharding.OrderLayout(len(dbs)))
	if err != nil {
		t.Fatal(err)
	}
	var parts []string
	for slot := range r.Slots() {
		db := dbs[r.DB(slot)]
		var n, x int64
		if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*), COALESCE(BIT_XOR(CRC32(CONCAT_WS('|', order_id, user_id,
			IFNULL(merchant_id, 'n'), version, status, total_amount, currency, idempotency_key, HEX(request_hash),
			IFNULL(payment_id, 'n'), IFNULL(fail_reason, 'n'), expire_at, IFNULL(paid_at, 'n'), created_at, updated_at))), 0)
			FROM `+sharding.Table("orders", slot)).Scan(&n, &x); err != nil {
			t.Fatal(err)
		}
		orders += n
		parts = append(parts, strconv.FormatInt(x, 10))
		if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*), COALESCE(BIT_XOR(CRC32(CONCAT_WS('|', order_id, sku_id,
			quantity, unit_price, created_at))), 0) FROM `+sharding.Table("order_items", slot)).Scan(&n, &x); err != nil {
			t.Fatal(err)
		}
		items += n
		parts = append(parts, strconv.FormatInt(x, 10))
	}
	return orders, items, strings.Join(parts, ",")
}

func TestRunWritesAllOrders(t *testing.T) {
	dbs := newOrderDBs(t, 2)
	p := smallParams()
	res, err := Run(t.Context(), Config{DBs: dbs, Params: p, BatchOrders: 200})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Complete() || res.Interrupted || res.Orders != p.Total || res.ChunksDone != 8*p.Days() {
		t.Fatalf("result %+v", res)
	}
	orders, items, _ := digest(t, dbs)
	if orders != p.Total || items != res.Items {
		t.Fatalf("db has %d orders %d items, want %d %d", orders, items, p.Total, res.Items)
	}

	// 库里的金额与明细之和一致，状态和 paid_at 一致。
	r, _ := sharding.New(sharding.OrderLayout(2))
	for slot := range 8 {
		var bad int
		if err := dbs[r.DB(slot)].QueryRowContext(t.Context(), `SELECT COUNT(*) FROM `+sharding.Table("orders", slot)+` o
			WHERE o.total_amount <> (SELECT COALESCE(SUM(i.unit_price * i.quantity), 0) FROM `+sharding.Table("order_items", slot)+` i
			                        WHERE i.order_id = o.order_id)
			   OR (o.status = 3) <> (o.paid_at IS NOT NULL)
			   OR (o.order_id & 7) <> ? OR (o.user_id & 7) <> ?`, slot, slot).Scan(&bad); err != nil {
			t.Fatal(err)
		}
		if bad != 0 {
			t.Errorf("slot %d: %d inconsistent rows", slot, bad)
		}
	}

	// 同样的参数再跑一次：全部跳过，什么都不写。
	res2, err := Run(t.Context(), Config{DBs: dbs, Params: p})
	if err != nil {
		t.Fatal(err)
	}
	if res2.Orders != 0 || res2.ChunksSkipped != res.ChunksTotal || res2.OrdersBefore != p.Total {
		t.Fatalf("rerun result %+v", res2)
	}
}

// 中途取消后续跑，结果与一次跑完的完全相同。
func TestRunResumesAfterCancel(t *testing.T) {
	p := smallParams()

	whole := newOrderDBs(t, 2)
	if _, err := Run(t.Context(), Config{DBs: whole, Params: p, BatchOrders: 100}); err != nil {
		t.Fatal(err)
	}
	wantOrders, wantItems, wantSum := digest(t, whole)

	parts := newOrderDBs(t, 2)
	for round := 0; ; round++ {
		if round > 50 {
			t.Fatal("did not finish")
		}
		ctx, cancel := context.WithCancel(t.Context())
		var commits atomic.Int64
		res, err := Run(ctx, Config{DBs: parts, Params: p, BatchOrders: 100, afterCommit: func() {
			if commits.Add(1) == 3 {
				cancel()
			}
		}})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if res.Complete() {
			if round == 0 {
				t.Fatal("first round was not interrupted")
			}
			break
		}
		if !res.Interrupted {
			t.Fatalf("round %d: incomplete but not interrupted: %+v", round, res)
		}
	}
	gotOrders, gotItems, gotSum := digest(t, parts)
	if gotOrders != wantOrders || gotItems != wantItems || gotSum != wantSum {
		t.Fatalf("resumed run: %d orders %d items sum %s; one-shot run: %d %d %s",
			gotOrders, gotItems, gotSum, wantOrders, wantItems, wantSum)
	}
}

func TestRunRejectsChangedParams(t *testing.T) {
	dbs := newOrderDBs(t, 2)
	p := smallParams()
	ctx, cancel := context.WithCancel(t.Context())
	_, err := Run(ctx, Config{DBs: dbs, Params: p, BatchOrders: 100, afterCommit: cancel})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*Params){
		"seed":  func(p *Params) { p.Seed++ },
		"total": func(p *Params) { p.Total++ },
		"range": func(p *Params) { p.To = p.To.Add(day) },
	} {
		q := p
		mut(&q)
		if _, err := Run(t.Context(), Config{DBs: dbs, Params: q}); !errors.Is(err, ErrParamsMismatch) {
			t.Errorf("%s changed: err = %v, want ErrParamsMismatch", name, err)
		}
	}
	// 原参数仍可续跑。
	res, err := Run(t.Context(), Config{DBs: dbs, Params: p})
	if err != nil || !res.Complete() {
		t.Fatalf("resume with the original params: %+v, %v", res, err)
	}
}

func TestRunRequiresTables(t *testing.T) {
	db, _ := testdb.Empty(t)
	p := smallParams()
	p.DBs = 1
	if _, err := Run(t.Context(), Config{DBs: []*sql.DB{db}, Params: p}); !errors.Is(err, ErrTablesMissing) {
		t.Fatalf("err = %v, want ErrTablesMissing", err)
	}
	var n int
	if err := db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("Run created %d tables in a db without order tables", n)
	}
}

func TestRunRejectsSwappedDSNs(t *testing.T) {
	dbs := newOrderDBs(t, 2)
	for i, db := range dbs {
		if _, err := db.ExecContext(t.Context(),
			"INSERT INTO shard_meta (id, db_index, db_count, slot_count, slots) VALUES (1, ?, 2, 8, '')", i); err != nil {
			t.Fatal(err)
		}
	}
	p := smallParams()
	_, err := Run(t.Context(), Config{DBs: []*sql.DB{dbs[1], dbs[0]}, Params: p})
	if err == nil || !strings.Contains(err.Error(), "check the dsn list") {
		t.Fatalf("err = %v, want a dsn order error", err)
	}
}

func TestLimiter(t *testing.T) {
	l := newLimiter(1000) // 1ms 一单
	start := time.Now()
	for range 5 {
		if err := l.wait(t.Context(), 10); err != nil {
			t.Fatal(err)
		}
	}
	// 5 批共 50 单，第 5 批在第 40ms 开始。
	if el := time.Since(start); el < 35*time.Millisecond {
		t.Errorf("50 orders at 1000/s took %s", el)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := l.wait(ctx, 1000); err == nil {
		t.Error("wait ignored cancellation")
	}
}
