// Package testdb 为集成测试提供隔离的 MySQL 数据库。
//
// 测试连接由环境变量 OPPENHEIMER_TEST_MYSQL_DSN 指定的 MySQL 实例（本地用 make up 启动的那个），
// 每次调用 New 都新建一个随机命名的库并应用迁移，测试结束后删除，因此测试之间互不影响、可以并行。
package testdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
)

// DSNEnv 是指定测试用 MySQL 的环境变量。DSN 里的库名会被忽略。
const DSNEnv = "OPPENHEIMER_TEST_MYSQL_DSN"

// New 返回一个连到全新数据库的连接池，数据库已应用 migrations 中 dir 目录下的迁移。
//
// 未设置 DSNEnv 时跳过测试；但在 CI 里（CI 环境变量非空）视为失败，
// 避免集成测试因配置遗漏而被悄悄跳过。
func New(t testing.TB, migrations fs.FS, dir string) *sql.DB {
	t.Helper()
	db, name := Empty(t)
	if err := mysqlx.Migrate(t.Context(), db, migrations, dir); err != nil {
		t.Fatalf("migrate %s: %v", name, err)
	}
	return db
}

// NewTemplate 与 New 相同，但迁移文件先按 data 渲染（见 mysqlx.MigrateTemplate），用于分库分表的服务。
// 每次调用都新建一个库，需要多个库时调用多次。
func NewTemplate(t testing.TB, migrations fs.FS, dir string, data any) *sql.DB {
	t.Helper()
	db, name := Empty(t)
	if err := mysqlx.MigrateTemplate(t.Context(), db, migrations, dir, data); err != nil {
		t.Fatalf("migrate %s: %v", name, err)
	}
	return db
}

// Empty 返回一个连到全新空库的连接池和库名，不应用任何迁移。跳过与失败的规则与 New 相同。
func Empty(t testing.TB) (*sql.DB, string) {
	t.Helper()

	dsn := os.Getenv(DSNEnv)
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatalf("%s is not set in CI", DSNEnv)
		}
		t.Skipf("%s is not set; run `make up` and `make test`", DSNEnv)
	}

	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse %s: %v", DSNEnv, err)
	}

	// t.Context() 在测试函数返回时就取消，而清理要在那之后执行，所以清理用独立的 context。
	ctx := t.Context()

	cfg.DBName = ""
	admin, err := mysqlx.Open(ctx, mysqlx.Config{DSN: cfg.FormatDSN(), MaxOpenConns: 2})
	if err != nil {
		t.Fatalf("connect to test mysql: %v", err)
	}

	name := "test_" + rand.Text()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+name+"`"); err != nil {
		_ = admin.Close()
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+name+"`"); err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
		_ = admin.Close()
	})

	cfg.DBName = name
	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: cfg.FormatDSN()})
	if err != nil {
		t.Fatalf("connect to %s: %v", name, err)
	}
	// 后注册的先执行：先关连接池，再删库。
	t.Cleanup(func() { _ = db.Close() })
	return db, name
}

// MustExec 执行一条语句，失败则终止测试。用于准备测试数据。
func MustExec(t testing.TB, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", fmt.Sprintf("%.60s", query), err)
	}
}
