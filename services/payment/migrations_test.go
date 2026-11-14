package payment

import (
	"database/sql"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/outbox"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// 迁移跑完之后，outbox 的表结构必须与 outbox.Schema 一致：SQL 文件引用不了 Go 常量，表是分两个迁移文件
// 建成现在这样的（00002 建表，00003 加 trace_context），这里防止两边改了一边。
func TestOutboxMigrationMatchesSchema(t *testing.T) {
	t.Parallel()
	migrated := testdb.New(t, Migrations, MigrationsDir)
	plain, _ := testdb.Empty(t)
	testdb.MustExec(t, plain, outbox.Schema)
	if got, want := showCreateOutbox(t, migrated), showCreateOutbox(t, plain); got != want {
		t.Errorf("outbox after migrations differs from outbox.Schema\nmigrated:\n%s\nschema:\n%s", got, want)
	}
}

func showCreateOutbox(t *testing.T, db *sql.DB) string {
	t.Helper()
	var name, ddl string
	if err := db.QueryRowContext(t.Context(), "SHOW CREATE TABLE outbox").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	return ddl
}
