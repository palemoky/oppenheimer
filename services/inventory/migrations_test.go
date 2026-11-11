package inventory

import (
	"strings"
	"testing"

	"github.com/palemoky/Oppenheimer/pkg/outbox"
)

// outbox 中继按固定的表结构读写，迁移里的建表语句必须与 outbox.Schema 一字不差。
func TestOutboxMigrationMatchesSchema(t *testing.T) {
	b, err := Migrations.ReadFile(MigrationsDir + "/00002_outbox.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if !strings.Contains(string(b), outbox.Schema+";") {
		t.Errorf("00002_outbox.sql does not contain outbox.Schema verbatim")
	}
}
