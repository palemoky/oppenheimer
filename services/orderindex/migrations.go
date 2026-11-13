package orderindex

import "embed"

// Migrations 包含 migrations 目录下的全部 goose 迁移。它们是 text/template，
// 要用 mysqlx.MigrateTemplate 以 MigrationData 渲染后应用，每个库渲染出本库 slot 的表。
//
//go:embed migrations/*.sql
var Migrations embed.FS

// MigrationsDir 是迁移文件在 Migrations 中的目录。
const MigrationsDir = "migrations"

// MigrationData 是迁移模板的数据。
type MigrationData struct {
	// Slots 是本库上的逻辑分片，取自 sharding.Router.SlotsOf（布局是 sharding.MerchantIndexLayout）。
	Slots []int
}
