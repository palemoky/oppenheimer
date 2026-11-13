package mysqlx

import (
	"context"
	"database/sql"
	"io/fs"
	"time"
)

// MigrateWithLockTimeout 与 Migrate 相同，但等锁时限可调，用于测试等锁超时。
func MigrateWithLockTimeout(ctx context.Context, db *sql.DB, fsys fs.FS, dir string, timeout time.Duration) error {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		return err
	}
	return migrate(ctx, db, sub, timeout)
}

// LockNameFor 导出 lockNameFor 供测试使用。
var LockNameFor = lockNameFor
