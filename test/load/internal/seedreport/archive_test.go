package seedreport

import (
	"bytes"
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// newArchiveDB 用 services/order 的冷库迁移建库，分区边界与 order.NewArchiveMigrationData 一致。
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
	return testdb.NewTemplate(t, os.DirFS(filepath.Join("..", "..", "..", "..", "services", "order")), "migrations_archive", data)
}

// 热库被归档掏空、冷库里有数据：报告照常生成，有冷库一节。
func TestGenerateArchive(t *testing.T) {
	ords := newDBs(t, "order", sharding.OrderLayout(2))
	cold := newArchiveDB(t)

	base := idgen.LowerBound(time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC))
	const orders = 300
	for i := range int64(orders) {
		id, user := base+i<<10, 1+i%25 // 25 个用户，每人 12 个订单
		testdb.MustExec(t, cold, `INSERT INTO orders_archive (order_id, user_id, version, status, total_amount, currency, idempotency_key,
			request_hash, expire_at, created_at, updated_at) VALUES (?, ?, 2, 3, 100, 'CNY', ?, REPEAT('a', 32), NOW(6), NOW(6), NOW(6))`,
			id, user, fmt.Sprintf("k%d", id))
		testdb.MustExec(t, cold, "INSERT INTO order_items_archive (order_id, sku_id, quantity, unit_price, created_at) VALUES (?, 1, 1, 100, NOW(6))", id)
	}

	rep, err := Generate(t.Context(), Config{OrderDBs: ords, ArchiveDB: cold, Runs: 20, CompareRuns: 2, DeepPage: 5, PageSize: 20, Analyze: true, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Queries) != 0 || !strings.Contains(strings.Join(rep.Notes, "\n"), "no orders in the order dbs") {
		t.Errorf("hot queries %d, notes %v", len(rep.Queries), rep.Notes)
	}
	a := rep.Archive
	if a == nil || len(a.Tables) != 3 || a.Tables[0].Rows != orders || a.Tables[1].Rows != orders || a.Tables[2].Rows != 0 {
		t.Fatalf("archive tables: %+v", a)
	}
	if a.Tables[0].DataBytes <= 0 || a.Tables[0].BytesPerRow <= 0 || a.Tables[2].BytesPerRow != 0 {
		t.Errorf("archive sizes: %+v", a.Tables)
	}
	if len(a.Partitions) != 3*11 {
		t.Errorf("%d partitions, want 33", len(a.Partitions))
	}
	var est int64
	for _, p := range a.Partitions {
		if p.Table == "orders_archive" {
			est += p.EstRows
		}
	}
	if est == 0 {
		t.Error("no partition has estimated rows")
	}
	if len(a.Compression) != 3 || a.Compression[0].HotRows != 0 || a.Compression[0].Ratio != 0 {
		t.Errorf("compression: %+v", a.Compression)
	}

	byName := map[string]QueryStat{}
	for _, q := range a.Queries {
		byName[q.Name] = q
		if q.Runs != 20 || q.P50Ms <= 0 || len(q.Explain) == 0 {
			t.Errorf("cold query %+v", q)
		}
	}
	if len(a.Queries) != 3 {
		t.Fatalf("%d cold query classes, want 3: %v", len(a.Queries), byName)
	}
	if q := byName["cold: order by id"]; q.Explain[0].Key != "PRIMARY" || q.Explain[0].Partitions != "p2025" || q.AvgRows != 1 {
		t.Errorf("cold order by id: %+v", q)
	}
	if q := byName["cold: user list, first page"]; q.Explain[0].Key != "idx_orders_user" || q.AvgRows != 12 {
		t.Errorf("cold user list: %+v", q)
	}
	if q := byName["cold: user list, cursor page (median)"]; q.Explain[0].Key != "idx_orders_user" || q.AvgRows <= 0 || q.AvgRows >= 20 {
		t.Errorf("cold cursor page: %+v", q)
	}

	var buf bytes.Buffer
	if err := rep.WriteText(&buf); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"cold archive: tables", "ESTIMATES", "hot/cold", "partitions=p2025", "cold: order by id"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("text report lacks %q", want)
		}
	}
	js, err := json.Marshal(rep)
	if err != nil || !strings.Contains(string(js), `"archive"`) {
		t.Fatalf("json: %v %.80s", err, js)
	}
}

// 热库和冷库都是空的：不报错，不除零，查询跳过。
func TestGenerateEmpty(t *testing.T) {
	ords := newDBs(t, "order", sharding.OrderLayout(2))
	rep, err := Generate(t.Context(), Config{OrderDBs: ords, ArchiveDB: newArchiveDB(t), Runs: 5, CompareRuns: 2, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Queries) != 0 || len(rep.Archive.Queries) != 0 || rep.Archive.Tables[0].Rows != 0 {
		t.Fatalf("report of empty dbs: %+v", rep.Archive)
	}
	var buf bytes.Buffer
	if err := rep.WriteText(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "no orders in the cold db") {
		t.Errorf("text lacks the cold note:\n%s", buf.String())
	}
	// 不给冷库时没有冷库一节，和以前一样。
	rep, err = Generate(t.Context(), Config{OrderDBs: ords, Runs: 5, CompareRuns: 2, Seed: 1})
	if err != nil || rep.Archive != nil {
		t.Fatalf("without archive db: %v, %+v", err, rep.Archive)
	}
}
