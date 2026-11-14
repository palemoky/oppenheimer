package seedreport

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/test/load/internal/seed"
)

func newDBs(t *testing.T, service string, layout sharding.Config) []*sql.DB {
	t.Helper()
	r, err := sharding.New(layout)
	if err != nil {
		t.Fatal(err)
	}
	fsys := os.DirFS(filepath.Join("..", "..", "..", "..", "services", service))
	dbs := make([]*sql.DB, r.DBs())
	for i := range dbs {
		dbs[i] = testdb.NewTemplate(t, fsys, "migrations", struct{ Slots []int }{r.SlotsOf(i)})
	}
	return dbs
}

type indexRow struct {
	m, o, u int64
	st      int8
	total   int64
	cur     string
	created time.Time
	paid    sql.NullTime
	ver     int64
}

// readIndexRows 读一个订单 slot 里有商家的订单，取索引表要的列。
func readIndexRows(t *testing.T, db *sql.DB, slot int) []indexRow {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT merchant_id, order_id, user_id, status, total_amount,
		currency, created_at, paid_at, version FROM `+sharding.Table("orders", slot)+` WHERE merchant_id IS NOT NULL`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var rs []indexRow
	for rows.Next() {
		var r indexRow
		if err := rows.Scan(&r.m, &r.o, &r.u, &r.st, &r.total, &r.cur, &r.created, &r.paid, &r.ver); err != nil {
			t.Fatal(err)
		}
		rs = append(rs, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return rs
}

// fillIndex 把订单库里有商家的订单抄进商家索引，代替 CDC。
func fillIndex(t *testing.T, ords, idx []*sql.DB) {
	t.Helper()
	or, _ := sharding.New(sharding.OrderLayout(len(ords)))
	ir, _ := sharding.New(sharding.MerchantIndexLayout())
	for slot := range or.Slots() {
		for _, r := range readIndexRows(t, ords[or.DB(slot)], slot) {
			ms := ir.ForMerchant(r.m)
			testdb.MustExec(t, idx[ir.DB(ms)], `INSERT INTO `+sharding.Table("merchant_orders", ms)+`
				(merchant_id, order_id, user_id, status, total_amount, currency, created_at, paid_at, src_version, synced_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, r.m, r.o, r.u, r.st, r.total, r.cur, r.created, r.paid, r.ver, r.created)
		}
	}
}

func TestGenerate(t *testing.T) {
	ords := newDBs(t, "order", sharding.OrderLayout(2))
	idx := newDBs(t, "orderindex", sharding.MerchantIndexLayout())
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	p := seed.Params{
		Total: 4_000, Seed: 7, From: from, To: from.AddDate(0, 0, 30),
		Users: 2_000, UserS: 1.05, UserV: 2, Merchants: 40, MerchantS: 1.3, MerchantV: 1, DBs: 2,
	}
	if _, err := seed.Run(t.Context(), seed.Config{DBs: ords, Params: p}); err != nil {
		t.Fatal(err)
	}
	fillIndex(t, ords, idx)

	rep, err := Generate(t.Context(), Config{
		OrderDBs: ords, IndexDBs: idx, Runs: 30, CompareRuns: 3, DeepPage: 5, PageSize: 20, Analyze: true, Seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	var orderRows int64
	for _, ts := range rep.OrderTables {
		if ts.Table == sharding.Table("orders", ts.Slot) {
			orderRows += ts.Rows
		}
	}
	if orderRows != p.Total || len(rep.OrderTables) != 16 || len(rep.IndexTables) != 4 {
		t.Fatalf("order rows %d, %d order tables, %d index tables", orderRows, len(rep.OrderTables), len(rep.IndexTables))
	}
	if len(rep.HeavyUsers) == 0 || len(rep.BigMerchant) == 0 {
		t.Fatal("no heavy users or big merchants")
	}
	names := map[string]QueryStat{}
	for _, q := range rep.Queries {
		names[q.Name] = q
		if q.Runs != 30 || q.P50Ms <= 0 || q.MaxMs < q.P99Ms || q.P99Ms < q.P50Ms || len(q.Explain) == 0 {
			t.Errorf("query %+v", q)
		}
	}
	if len(rep.Queries) != 7 {
		t.Errorf("%d query classes, want 7", len(rep.Queries))
	}
	if q := names["user list, first page"]; q.Explain[0].Key != "idx_orders_user" || q.AvgRows <= 0 {
		t.Errorf("user list: %+v", q)
	}
	if q := names["order by id"]; q.Explain[0].Key != "PRIMARY" || q.AvgRows != 1 {
		t.Errorf("order by id: %+v", q)
	}
	if len(rep.Compare) != 2 {
		t.Fatalf("%d comparisons, want 2", len(rep.Compare))
	}
	for _, c := range rep.Compare {
		if !c.Same || c.CursorRows > c.OffsetRows {
			t.Errorf("comparison %+v", c)
		}
	}
	if g := rep.Compare[1]; g.Offset != 80 || g.OffsetRows != 8*100 || g.CursorRows != 8*20 {
		t.Errorf("scatter-gather: %+v", g)
	}

	var buf bytes.Buffer
	if err := rep.WriteText(&buf); err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + buf.String())
	for _, want := range []string{"balance", "query latency", "offset vs cursor", "idx_orders_user"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("text report lacks %q", want)
		}
	}
}
