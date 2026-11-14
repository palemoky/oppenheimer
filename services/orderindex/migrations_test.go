package orderindex

import (
	"database/sql"
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

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

// 按生产布局（sharding.MerchantIndexLayout）迁移各个库，每个库只有自己 slot 的索引表。
func TestMigrationsCreateOnlyOwnSlots(t *testing.T) {
	t.Parallel()
	r, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil {
		t.Fatal(err)
	}
	for db := range r.DBs() {
		conn := testdb.NewTemplate(t, Migrations, MigrationsDir, MigrationData{Slots: r.SlotsOf(db)})
		var want []string
		for _, s := range r.SlotsOf(db) {
			want = append(want, sharding.Table("merchant_orders", s))
		}
		slices.Sort(want)
		if got := tables(t, conn); !slices.Equal(got, want) {
			t.Errorf("db %d tables = %v, want %v", db, got, want)
		}
	}
}

// 模拟上次迁移中途被杀：前一半表已经建好、版本号没记。整版重跑必须成功并补齐其余的表。
func TestMigrationResumesAfterPartialRun(t *testing.T) {
	t.Parallel()
	r, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil {
		t.Fatal(err)
	}
	data := MigrationData{Slots: r.SlotsOf(1)}
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
	if len(creates) != len(data.Slots) {
		t.Fatalf("found %d CREATE TABLE statements, want %d", len(creates), len(data.Slots))
	}

	db, _ := testdb.Empty(t)
	testdb.MustExec(t, db, creates[0])
	if err := mysqlx.MigrateTemplate(t.Context(), db, Migrations, MigrationsDir, data); err != nil {
		t.Fatalf("migrate after partial run: %v", err)
	}
	if got := len(tables(t, db)); got != len(creates) {
		t.Errorf("tables after resume = %d, want %d", got, len(creates))
	}
}
