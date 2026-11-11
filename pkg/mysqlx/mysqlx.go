// Package mysqlx 封装 MySQL 的连接、事务、迁移和常见错误判断。
package mysqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pressly/goose/v3"
)

// MySQL 错误码。
const (
	errDuplicateEntry  = 1062
	errLockWaitTimeout = 1205
	errDeadlock        = 1213
	errCheckViolated   = 3819
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

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysqlx: ping: %w", err)
	}
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

func hasNumber(err error, number uint16) bool {
	myErr, ok := errors.AsType[*mysql.MySQLError](err)
	return ok && myErr.Number == number
}

// Migrate 把 fsys 中 dir 目录下的 goose 迁移应用到最新版本。
// 多个实例同时启动时，goose 用数据库的咨询锁保证只有一个在执行。
func Migrate(ctx context.Context, db *sql.DB, fsys fs.FS, dir string) error {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		return fmt.Errorf("mysqlx: migrations dir %q: %w", dir, err)
	}
	provider, err := goose.NewProvider(goose.DialectMySQL, db, sub,
		goose.WithSlog(slog.New(slog.DiscardHandler)),
	)
	if err != nil {
		return fmt.Errorf("mysqlx: new migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("mysqlx: migrate up: %w", err)
	}
	return nil
}
