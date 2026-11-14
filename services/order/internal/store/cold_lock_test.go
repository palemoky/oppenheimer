package store_test

import (
	"errors"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 归档任务的互斥锁：第二个拿不到，释放之后能再拿；另一个库（另一套环境）上的锁互不影响。
func TestLockArchiverIsExclusivePerDatabase(t *testing.T) {
	t.Parallel()
	newCold := func() *store.Cold {
		return store.NewCold(testdb.NewTemplate(t, order.ArchiveMigrations, order.ArchiveMigrationsDir, order.NewArchiveMigrationData()))
	}
	a, b := newCold(), newCold()

	release, err := a.LockArchiver(t.Context())
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	if _, err := a.LockArchiver(t.Context()); !errors.Is(err, store.ErrArchiverRunning) {
		t.Fatalf("second lock on the same database = %v, want ErrArchiverRunning", err)
	}
	releaseB, err := b.LockArchiver(t.Context())
	if err != nil {
		t.Fatalf("lock on another database: %v", err)
	}
	releaseB()

	release()
	release, err = a.LockArchiver(t.Context())
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	release()
}
