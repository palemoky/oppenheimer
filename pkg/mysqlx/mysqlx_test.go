package mysqlx_test

import (
	"database/sql"
	"embed"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
)

//go:embed testdata/migrations/*.sql
var migrations embed.FS

const migrationsDir = "testdata/migrations"

func counterValue(t *testing.T, db *sql.DB, id int64) (value int64, found bool) {
	t.Helper()
	err := db.QueryRowContext(t.Context(), "SELECT value FROM counter WHERE id = ?", id).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return value, true
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := testdb.New(t, migrations, migrationsDir)
	// testdb.New 已经迁移过一次，再来一次应该什么都不做。
	if err := mysqlx.Migrate(t.Context(), db, migrations, migrationsDir); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
}

func TestInTxCommitsOnSuccess(t *testing.T) {
	db := testdb.New(t, migrations, migrationsDir)

	err := mysqlx.InTx(t.Context(), db, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), "INSERT INTO counter (id, value) VALUES (1, 10)")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := counterValue(t, db, 1); !ok || v != 10 {
		t.Errorf("counter = %d (found=%v), want 10", v, ok)
	}
}

func TestInTxRollsBackOnError(t *testing.T) {
	db := testdb.New(t, migrations, migrationsDir)
	boom := errors.New("boom")

	err := mysqlx.InTx(t.Context(), db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), "INSERT INTO counter (id, value) VALUES (1, 10)"); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("InTx = %v, want boom", err)
	}
	if _, ok := counterValue(t, db, 1); ok {
		t.Error("row exists after rollback")
	}
}

func TestInTxRollsBackOnPanic(t *testing.T) {
	db := testdb.New(t, migrations, migrationsDir)

	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic did not propagate")
			}
		}()
		_ = mysqlx.InTx(t.Context(), db, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(t.Context(), "INSERT INTO counter (id, value) VALUES (1, 10)"); err != nil {
				t.Error(err)
			}
			panic("boom")
		})
	}()

	if _, ok := counterValue(t, db, 1); ok {
		t.Error("row exists after panic")
	}
	// 连接必须已归还，否则连接池会泄漏。
	if open := db.Stats().InUse; open != 0 {
		t.Errorf("%d connections still in use after panic", open)
	}
}

func TestErrorClassification(t *testing.T) {
	db := testdb.New(t, migrations, migrationsDir)
	testdb.MustExec(t, db, "INSERT INTO counter (id, value) VALUES (1, 0)")

	_, err := db.ExecContext(t.Context(), "INSERT INTO counter (id, value) VALUES (1, 5)")
	if !mysqlx.IsDuplicateKey(err) {
		t.Errorf("duplicate insert: IsDuplicateKey(%v) = false", err)
	}

	_, err = db.ExecContext(t.Context(), "UPDATE counter SET value = value - 1 WHERE id = 1")
	if !mysqlx.IsCheckViolation(err) {
		t.Errorf("negative value: IsCheckViolation(%v) = false", err)
	}

	if mysqlx.IsDuplicateKey(nil) || mysqlx.IsDeadlock(errors.New("x")) {
		t.Error("classification matched a non-MySQL error")
	}
}

// 两个事务以相反顺序更新两行，InnoDB 会检测到死锁并回滚其中一个。
func TestIsDeadlock(t *testing.T) {
	db := testdb.New(t, migrations, migrationsDir)
	testdb.MustExec(t, db, "INSERT INTO counter (id, value) VALUES (1, 0), (2, 0)")

	// 两个事务都拿到第一把锁之后，才去请求对方持有的那一把。
	var firstLocked sync.WaitGroup
	firstLocked.Add(2)
	proceed := make(chan struct{})

	run := func(first, second int64) error {
		return mysqlx.InTx(t.Context(), db, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(t.Context(), "UPDATE counter SET value = value + 1 WHERE id = ?", first); err != nil {
				firstLocked.Done()
				return err
			}
			firstLocked.Done()
			<-proceed
			_, err := tx.ExecContext(t.Context(), "UPDATE counter SET value = value + 1 WHERE id = ?", second)
			return err
		})
	}

	errs := make([]error, 2)
	var wg sync.WaitGroup
	wg.Go(func() { errs[0] = run(1, 2) })
	wg.Go(func() { errs[1] = run(2, 1) })

	firstLocked.Wait()
	close(proceed)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("transactions did not finish; deadlock was not detected")
	}

	deadlocks := 0
	for _, err := range errs {
		switch {
		case err == nil:
		case mysqlx.IsDeadlock(err):
			deadlocks++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if deadlocks != 1 {
		t.Errorf("got %d deadlock victims, want exactly 1 (errs: %v)", deadlocks, errs)
	}
}
