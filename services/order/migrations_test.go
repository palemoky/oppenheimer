package order

import (
	"database/sql"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/pkg/outbox"
	"github.com/palemoky/Oppenheimer/pkg/sharding"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
)

// 迁移文件里的 outbox 建表语句必须与 outbox.Schema 一致：SQL 文件引用不了 Go 常量，
// 只能抄一份，这里防止两边改了一边。比较时忽略空白差异。
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
	normalize := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	if !strings.Contains(normalize(string(b)), normalize(outbox.Schema)+";") {
		t.Errorf("00001_init.sql does not contain outbox.Schema verbatim")
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

// 按生产布局（8 个 slot、2 个库）迁移两个库，每个库只有自己的 4 组表加一张 outbox。
func TestMigrationsCreateOnlyOwnSlots(t *testing.T) {
	t.Parallel()
	r, err := sharding.New(sharding.Config{Slots: 8, DBs: 2})
	if err != nil {
		t.Fatal(err)
	}
	for db := range r.DBs() {
		conn := testdb.NewTemplate(t, Migrations, MigrationsDir, MigrationData{Slots: r.SlotsOf(db)})
		want := []string{"outbox"}
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
