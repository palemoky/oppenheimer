package order

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/palemoky/Oppenheimer/pkg/outbox"
)

// 迁移文件里的 outbox 建表语句必须与 outbox.Schema 一致：SQL 文件引用不了 Go 常量，
// 只能抄一份，这里防止两边改了一边。比较时忽略空白差异。
func TestOutboxMigrationMatchesSchema(t *testing.T) {
	t.Parallel()
	b, err := fs.ReadFile(Migrations, MigrationsDir+"/00002_outbox.sql")
	if err != nil {
		t.Fatal(err)
	}
	normalize := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	if !strings.Contains(normalize(string(b)), normalize(outbox.Schema)+";") {
		t.Errorf("00002_outbox.sql does not contain outbox.Schema verbatim")
	}
}
