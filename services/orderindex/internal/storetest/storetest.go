// Package storetest 为集成测试准备按生产布局（sharding.MerchantIndexLayout）迁移好的索引库。
package storetest

import (
	"database/sql"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/orderindex"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

// New 为每个索引库新建一个临时库（见 testdb.NewTemplate），返回 Store 和各库的连接池（下标是库号）。
// 没有配置测试 MySQL 时跳过测试。
func New(t testing.TB) (*store.Store, []*sql.DB) {
	t.Helper()
	r, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil {
		t.Fatal(err)
	}
	dbs := make([]*sql.DB, r.DBs())
	for i := range dbs {
		dbs[i] = testdb.NewTemplate(t, orderindex.Migrations, orderindex.MigrationsDir,
			orderindex.MigrationData{Slots: r.SlotsOf(i)})
	}
	st, err := store.New(dbs, r)
	if err != nil {
		t.Fatal(err)
	}
	return st, dbs
}
