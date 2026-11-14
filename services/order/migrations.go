// Package order 导出 order 服务的数据库迁移文件。
package order

import (
	"embed"
	"fmt"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
)

// Migrations 包含 migrations 目录下的全部 goose 迁移。它们是 text/template，
// 要用 mysqlx.MigrateTemplate 以 MigrationData 渲染后应用，每个库渲染出本库 slot 的表。
//
//go:embed migrations/*.sql
var Migrations embed.FS

// MigrationsDir 是迁移文件在 Migrations 中的目录。
const MigrationsDir = "migrations"

// MigrationData 是迁移模板的数据。
type MigrationData struct {
	// Slots 是本库上的逻辑分片，取自 sharding.Router.SlotsOf。
	Slots []int
}

// ArchiveMigrations 包含冷库（归档库）的 goose 迁移，同样是 text/template，
// 用 mysqlx.MigrateTemplate 以 NewArchiveMigrationData() 渲染后应用。设计文档 1c 第 6.5 节。
//
//go:embed migrations_archive/*.sql
var ArchiveMigrations embed.FS

// ArchiveMigrationsDir 是冷库迁移文件在 ArchiveMigrations 中的目录。
const ArchiveMigrationsDir = "migrations_archive"

// ArchivePartition 是冷库表的一个按年分区。
type ArchivePartition struct {
	// Name 是分区名，如 p2024。
	Name string
	// Below 是分区的上界（不含）：下一年 1 月 1 日 UTC 对应的订单号下界。
	Below int64
}

// ArchiveMigrationData 是冷库迁移模板的数据。
type ArchiveMigrationData struct {
	Partitions []ArchivePartition
}

// 冷库按年分区的范围。最后一年之后的订单落进 pmax；要给之后的年份单独建分区时，
// 新增一个迁移用 REORGANIZE PARTITION 把 pmax 拆开，不要改这两个常量（已经建好的表不会跟着变）。
const (
	archiveFirstYear = 2020 // idgen.Epoch 所在的年份
	archiveLastYear  = 2029
)

// NewArchiveMigrationData 返回冷库迁移的模板数据。结果是确定的，不依赖当前时间。
func NewArchiveMigrationData() ArchiveMigrationData {
	var d ArchiveMigrationData
	for y := archiveFirstYear; y <= archiveLastYear; y++ {
		d.Partitions = append(d.Partitions, ArchivePartition{
			Name:  fmt.Sprintf("p%d", y),
			Below: idgen.LowerBound(time.Date(y+1, 1, 1, 0, 0, 0, 0, time.UTC)),
		})
	}
	return d
}
