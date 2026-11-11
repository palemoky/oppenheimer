// Package order 导出 order 服务的数据库迁移文件。
package order

import "embed"

// Migrations 包含 migrations 目录下的全部 goose 迁移。
//
//go:embed migrations/*.sql
var Migrations embed.FS

// MigrationsDir 是迁移文件在 Migrations 中的目录。
const MigrationsDir = "migrations"
