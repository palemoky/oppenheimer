package order

import (
	"database/sql"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/outbox"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// 迁移文件里的 outbox 建表语句必须与 outbox.Schema 一致：SQL 文件引用不了 Go 常量，
// 只能抄一份，这里防止两边改了一边。比较时忽略空白差异；迁移文件为了可重入多了 IF NOT EXISTS，
// 比较前给 Schema 补上。
func TestOutboxMigrationMatchesSchema(t *testing.T) {
	t.Parallel()
	rendered, err := mysqlx.RenderMigrations(Migrations, MigrationsDir, MigrationData{Slots: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := fs.ReadFile(rendered, "00001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	const plain, reentrant = "CREATE TABLE outbox", "CREATE TABLE IF NOT EXISTS outbox"
	rest, ok := strings.CutPrefix(outbox.Schema, plain)
	if !ok {
		t.Fatalf("outbox.Schema does not start with %q", plain)
	}
	normalize := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	if !strings.Contains(normalize(string(b)), normalize(reentrant+rest)+";") {
		t.Errorf("00001_init.sql does not contain outbox.Schema (with IF NOT EXISTS) verbatim")
	}
}

func tables(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT table_name FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name <> 'goose_db_version'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(names)
	return names
}

// 按生产布局（sharding.OrderLayout）迁移各个库，每个库只有自己的 4 组表加 outbox 和 shard_meta。
func TestMigrationsCreateOnlyOwnSlots(t *testing.T) {
	t.Parallel()
	r, err := sharding.New(sharding.OrderLayout(2))
	if err != nil {
		t.Fatal(err)
	}
	for db := range r.DBs() {
		conn := testdb.NewTemplate(t, Migrations, MigrationsDir, MigrationData{Slots: r.SlotsOf(db)})
		want := []string{"outbox", "shard_meta"}
		for _, s := range r.SlotsOf(db) {
			for _, base := range []string{"orders", "order_items", "payment_results"} {
				want = append(want, sharding.Table(base, s))
			}
		}
		slices.Sort(want)
		if got := tables(t, conn); !slices.Equal(got, want) {
			t.Errorf("db %d tables = %v, want %v", db, got, want)
		}
	}
}

// 模拟上次迁移中途被杀：MySQL 的 DDL 逐条隐式提交，版本里前几张表已经建好、版本号没记。
// 下次启动整版重跑，必须能成功并补齐其余的表。
func TestMigrationResumesAfterPartialRun(t *testing.T) {
	t.Parallel()
	r, err := sharding.New(sharding.OrderLayout(2))
	if err != nil {
		t.Fatal(err)
	}
	data := MigrationData{Slots: r.SlotsOf(0)}
	rendered, err := mysqlx.RenderMigrations(Migrations, MigrationsDir, data)
	if err != nil {
		t.Fatal(err)
	}
	b, err := fs.ReadFile(rendered, "00001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	up, _, ok := strings.Cut(string(b), "-- +goose Down")
	if !ok {
		t.Fatal("00001_init.sql has no Down section")
	}
	var creates []string
	for stmt := range strings.SplitSeq(up, ";") {
		if strings.Contains(stmt, "CREATE TABLE") {
			creates = append(creates, stmt)
		}
	}
	if want := 3*len(data.Slots) + 1; len(creates) != want {
		t.Fatalf("found %d CREATE TABLE statements, want %d", len(creates), want)
	}

	db, _ := testdb.Empty(t)
	partial := len(creates) / 2
	for _, stmt := range creates[:partial] {
		testdb.MustExec(t, db, stmt)
	}
	if got := tables(t, db); len(got) != partial {
		t.Fatalf("pre-created %d tables, want %d", len(got), partial)
	}

	if err := mysqlx.MigrateTemplate(t.Context(), db, Migrations, MigrationsDir, data); err != nil {
		t.Fatalf("migrate after partial run: %v", err)
	}
	// 00001 的表全部补齐，另加 00003 的 shard_meta。
	if got := len(tables(t, db)); got != len(creates)+1 {
		t.Errorf("tables after resume = %d, want %d", got, len(creates)+1)
	}
}

func hasColumn(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	var n int
	err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`, table, column).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// 00002 给每张 orders 表加两列。模拟它中途被杀：一部分表已经改完、其余没改、版本号没记。
// MySQL 没有 ADD COLUMN IF NOT EXISTS，整版重跑要靠迁移里对 information_schema 的判断。
func TestAddColumnMigrationResumesAfterPartialRun(t *testing.T) {
	t.Parallel()
	r, err := sharding.New(sharding.OrderLayout(2))
	if err != nil {
		t.Fatal(err)
	}
	data := MigrationData{Slots: r.SlotsOf(0)}
	db := testdb.NewTemplate(t, Migrations, MigrationsDir, data)

	// 回到“00002 跑了一半”：去掉它和之后各版本的记录（00002 没跑完时后面的版本不可能已经记上，
	// 留着的话 goose 会因为出现了比库里版本号更低的未执行迁移而拒绝），再把后一半表的两列删掉。
	// 后面的版本会跟着重跑一遍，它们同样必须可重入。
	testdb.MustExec(t, db, `DELETE FROM goose_db_version WHERE version_id >= 2`)
	undone := data.Slots[len(data.Slots)/2:]
	for _, s := range undone {
		testdb.MustExec(t, db, "ALTER TABLE "+sharding.Table("orders", s)+" DROP COLUMN merchant_id, DROP COLUMN version")
	}

	if err := mysqlx.MigrateTemplate(t.Context(), db, Migrations, MigrationsDir, data); err != nil {
		t.Fatalf("migrate after partial run: %v", err)
	}
	for _, s := range data.Slots {
		for _, col := range []string{"merchant_id", "version"} {
			if !hasColumn(t, db, sharding.Table("orders", s), col) {
				t.Errorf("%s.%s missing after resume", sharding.Table("orders", s), col)
			}
		}
	}
}
