// Package mysqlx 封装 MySQL 的连接、事务、迁移和常见错误判断。
package mysqlx

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"testing/fstest"
	"text/template"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// MySQL 错误码。
const (
	errDuplicateEntry  = 1062
	errLockWaitTimeout = 1205
	errDeadlock        = 1213
	errCheckViolated   = 3819
	errNoSuchTable     = 1146
)

// Config 是连接池配置。零值字段使用默认值。
type Config struct {
	// DSN 采用 go-sql-driver/mysql 的格式，例如 user:pass@tcp(127.0.0.1:3306)/dbname。
	DSN string
	// MaxOpenConns 默认 50。连接池上限要与 MySQL 的 max_connections 和服务副本数一起算。
	MaxOpenConns int
	// MaxIdleConns 默认等于 MaxOpenConns，避免突发流量下反复建连。
	MaxIdleConns int
	// ConnMaxLifetime 默认 30 分钟，需小于 MySQL 的 wait_timeout 和中间代理的空闲超时。
	ConnMaxLifetime time.Duration
	// ConnMaxIdleTime 默认 5 分钟。
	ConnMaxIdleTime time.Duration
}

// Open 打开连接池并验证连通性。
//
// 无论 DSN 怎么写，都强制以下设置，保证各服务行为一致：
// 时间按 UTC 读写并解析为 time.Time；UPDATE 的受影响行数取“实际被修改的行数”
// （驱动默认行为，这里显式固定），因为条件更新靠它判断状态转换是否由本次调用完成。
func Open(ctx context.Context, cfg Config) (*sql.DB, error) {
	db, err := OpenLazy(cfg)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysqlx: ping: %w", err)
	}
	return db, nil
}

// OpenLazy 和 Open 一样配置连接池，但不验证连通性：数据库此刻连不上也返回连接池，第一次使用时才建连接。
//
// 给可选的依赖用（从库、冷库，设计文档 1d 的 6.10）：它们在运行中坏了都能降级，启动时坏着也不该让服务起不来。
// 自己的主库仍然用 Open，连不上就启动失败。只有 DSN 本身写错才返回错误。
func OpenLazy(cfg Config) (*sql.DB, error) {
	parsed, err := mysql.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("mysqlx: parse dsn: %w", err)
	}
	parsed.ParseTime = true
	parsed.Loc = time.UTC
	if parsed.Params == nil {
		parsed.Params = map[string]string{}
	}
	parsed.Params["time_zone"] = "'+00:00'"
	parsed.ClientFoundRows = false
	if parsed.Timeout == 0 {
		parsed.Timeout = 5 * time.Second
	}

	connector, err := mysql.NewConnector(parsed)
	if err != nil {
		return nil, fmt.Errorf("mysqlx: new connector: %w", err)
	}
	db := sql.OpenDB(connector)

	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 50
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = maxOpen
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(orDefault(cfg.ConnMaxLifetime, 30*time.Minute))
	db.SetConnMaxIdleTime(orDefault(cfg.ConnMaxIdleTime, 5*time.Minute))
	return db, nil
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

// InTx 在事务里执行 fn。fn 返回错误或 panic 时回滚，否则提交。
//
// fn 里只应有数据库操作。不要在事务里发起 RPC 或其他慢操作：事务持有行锁，
// 持锁时间直接决定并发能力。
func InTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysqlx: begin: %w", err)
	}

	committed := false
	defer func() {
		if committed {
			return
		}
		// 到这里要么 fn 返回了错误，要么正在 panic。回滚失败（通常是连接已断，
		// 服务端会自行回滚）不应盖过原始错误。
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) && err != nil {
			err = errors.Join(err, fmt.Errorf("mysqlx: rollback: %w", rbErr))
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("mysqlx: commit: %w", err)
	}
	committed = true
	return nil
}

// IsDuplicateKey 判断 err 是否为唯一键冲突。
func IsDuplicateKey(err error) bool { return hasNumber(err, errDuplicateEntry) }

// IsDeadlock 判断 err 是否为死锁。InnoDB 检测到死锁后会回滚其中一个事务，整个事务可以重试。
func IsDeadlock(err error) bool { return hasNumber(err, errDeadlock) }

// IsLockWaitTimeout 判断 err 是否为等待行锁超时。默认只回滚当前语句而不是整个事务。
func IsLockWaitTimeout(err error) bool { return hasNumber(err, errLockWaitTimeout) }

// IsCheckViolation 判断 err 是否为违反 CHECK 约束。
func IsCheckViolation(err error) bool { return hasNumber(err, errCheckViolated) }

// IsNoSuchTable 判断 err 是否为表不存在。
//
// 订单库扩容时，要搬走的分片表在旧库上被改名作为围栏（ADR-0015），之后按旧路由来的读写得到这个错误。
// 它表示“这个分片暂时不可用”，调用方应当稍后重试，不是数据或代码的问题。
func IsNoSuchTable(err error) bool { return hasNumber(err, errNoSuchTable) }

func hasNumber(err error, number uint16) bool {
	myErr, ok := errors.AsType[*mysql.MySQLError](err)
	return ok && myErr.Number == number
}

// Migrate 把 fsys 中 dir 目录下的 goose 迁移应用到最新版本。
//
// 多个实例同时启动时，迁移前先在一条专用连接上用 GET_LOCK 拿本库的迁移锁，同一个库上
// 同一时刻只有一个实例在执行，其余的等它做完再看到“已是最新”。等锁最多 60 秒，
// 超时返回 ErrMigrationLockTimeout。
//
// 锁只防并发，不防中途崩溃：MySQL 的 DDL 隐式提交，一个版本里有多条 DDL 时，进程在中间被杀
// 会留下部分表而版本号没记。迁移文件要写成可重入的（CREATE TABLE IF NOT EXISTS 等），
// 下次启动重跑这个版本才能成功。
func Migrate(ctx context.Context, db *sql.DB, fsys fs.FS, dir string) error {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		return fmt.Errorf("mysqlx: migrations dir %q: %w", dir, err)
	}
	return migrate(ctx, db, sub, migrationLockTimeout)
}

// MigrateTemplate 把 fsys 中 dir 下的迁移文件当作 text/template 渲染（数据为 data），再应用到最新版本。
//
// 用于分库分表：同一组迁移文件按每个库上的分片渲染出不同的建表语句（例如用 {{range .Slots}}
// 为每个分片建一组表）。渲染结果放在内存里交给 goose，版本号仍按文件名，每个库各自记录。
// 模板引用了 data 里不存在的字段时返回错误，不会渲染出半截 SQL。
//
// 已应用过的版本不会因为 data 变了而重新执行：goose 只看版本号。改变一个库上有哪些分片
// 是一次数据迁移，不能靠改模板数据完成。
func MigrateTemplate(ctx context.Context, db *sql.DB, fsys fs.FS, dir string, data any) error {
	rendered, err := RenderMigrations(fsys, dir, data)
	if err != nil {
		return err
	}
	return migrate(ctx, db, rendered, migrationLockTimeout)
}

// RenderMigrations 把 fsys 中 dir 下的每个文件当作 text/template 渲染，返回以文件名为路径的内存文件系统。
// 只处理 dir 这一层的普通文件（goose 也只读这一层）。
func RenderMigrations(fsys fs.FS, dir string, data any) (fs.FS, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("mysqlx: read migrations dir %q: %w", dir, err)
	}
	// fstest.MapFS 虽在 testing/ 下，但只依赖 testing/iotest 和基础库，不会把 testing 包
	// （以及它注册的 -test.* 命令行参数）带进生产二进制，已用 go list -deps 确认。
	out := fstest.MapFS{}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		name := e.Name()
		src, err := fs.ReadFile(fsys, path.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("mysqlx: read migration %s: %w", name, err)
		}
		tmpl, err := template.New(name).Option("missingkey=error").Parse(string(src))
		if err != nil {
			return nil, fmt.Errorf("mysqlx: parse migration template %s: %w", name, err)
		}
		var b bytes.Buffer
		if err := tmpl.Execute(&b, data); err != nil {
			return nil, fmt.Errorf("mysqlx: render migration %s: %w", name, err)
		}
		out[name] = &fstest.MapFile{Data: b.Bytes(), Mode: 0o444}
	}
	return out, nil
}

func migrate(ctx context.Context, db *sql.DB, fsys fs.FS, lockTimeout time.Duration) error {
	provider, err := goose.NewProvider(goose.DialectMySQL, db, fsys,
		goose.WithSlog(slog.New(slog.DiscardHandler)),
		goose.WithSessionLocker(getLockLocker{timeout: lockTimeout}),
	)
	if err != nil {
		return fmt.Errorf("mysqlx: new migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("mysqlx: migrate up: %w", err)
	}
	return nil
}

// migrationLockTimeout 是等待迁移锁的上限。正常的迁移几秒内做完；等这么久还拿不到，
// 多半是持锁的实例卡住了，与其无限等下去不如启动失败，让人看到。
const migrationLockTimeout = 60 * time.Second

// ErrMigrationLockTimeout 表示在时限内没有拿到迁移锁。
var ErrMigrationLockTimeout = errors.New("mysqlx: timed out waiting for migration lock")

// getLockLocker 用 MySQL 的 GET_LOCK 实现 goose 的 SessionLocker。goose v3.30.0 只为 MySQL 提供了
// 基于表和租约的 Locker，需要额外的锁表和心跳；GET_LOCK 是会话级的，连接断开（包括进程被杀）
// 时服务端自动释放，不会留下需要人工清理的死锁。goose 拿锁和执行迁移用的是同一条连接。
//
// GET_LOCK 的等待时间只能精确到秒，不足一秒按一秒算。
type getLockLocker struct {
	timeout time.Duration
}

var _ lock.SessionLocker = getLockLocker{}

func (l getLockLocker) SessionLock(ctx context.Context, conn *sql.Conn) error {
	name, err := migrationLockName(ctx, conn)
	if err != nil {
		return err
	}
	// GET_LOCK 返回 1 表示拿到，0 表示超时，NULL 表示出错（例如被 KILL）。
	var got sql.NullInt64
	secs := max(1, int(l.timeout/time.Second))
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, secs).Scan(&got); err != nil {
		return fmt.Errorf("mysqlx: get migration lock %q: %w", name, err)
	}
	switch {
	case !got.Valid:
		return fmt.Errorf("mysqlx: get migration lock %q: GET_LOCK returned NULL", name)
	case got.Int64 != 1:
		return fmt.Errorf("%w %q after %ds", ErrMigrationLockTimeout, name, secs)
	}
	return nil
}

func (getLockLocker) SessionUnlock(ctx context.Context, conn *sql.Conn) error {
	name, err := migrationLockName(ctx, conn)
	if err != nil {
		return err
	}
	// 返回 1 才是本连接持有并释放了；0 或 NULL 说明锁早已不在本连接上，迁移期间的互斥可能没有成立。
	var released sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", name).Scan(&released); err != nil {
		return fmt.Errorf("mysqlx: release migration lock %q: %w", name, err)
	}
	if !released.Valid || released.Int64 != 1 {
		return fmt.Errorf("mysqlx: release migration lock %q: not held by this session", name)
	}
	return nil
}

// migrationLockName 返回当前库的迁移锁名。GET_LOCK 的锁是实例级的，锁名带库名，
// 不同库（例如 order 的两个分库在同一个实例上时）的迁移互不阻塞。
func migrationLockName(ctx context.Context, conn *sql.Conn) (string, error) {
	var dbName sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName); err != nil {
		return "", fmt.Errorf("mysqlx: current database: %w", err)
	}
	if !dbName.Valid || dbName.String == "" {
		return "", errors.New("mysqlx: migrate: no database selected in DSN")
	}
	return lockNameFor(dbName.String), nil
}

// lockNameFor 返回库 dbName 的迁移锁名。锁名上限 64 字符，库名本身最长也是 64，
// 加上前缀超长时改用库名的哈希。
func lockNameFor(dbName string) string {
	const prefix = "goose_migrate:"
	const maxLen = 64
	if name := prefix + dbName; len(name) <= maxLen {
		return name
	}
	sum := sha256.Sum256([]byte(dbName))
	return prefix + hex.EncodeToString(sum[:])[:maxLen-len(prefix)]
}
