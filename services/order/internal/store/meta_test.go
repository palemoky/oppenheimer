package store_test

import (
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

func newShardDBs(t *testing.T, n int) ([]*sql.DB, *sharding.Router) {
	t.Helper()
	router, err := sharding.New(sharding.OrderLayout(n))
	if err != nil {
		t.Fatal(err)
	}
	dbs := make([]*sql.DB, n)
	for i := range dbs {
		dbs[i] = testdb.NewTemplate(t, order.Migrations, order.MigrationsDir, order.MigrationData{Slots: router.SlotsOf(i)})
	}
	return dbs, router
}

// 空库第一次启动时写入身份，之后每次启动核对通过。
func TestEnsureShardMetaInitializesAndAccepts(t *testing.T) {
	t.Parallel()
	dbs, router := newShardDBs(t, 2)
	for range 2 {
		if err := store.EnsureShardMeta(t.Context(), dbs, router); err != nil {
			t.Fatal(err)
		}
	}
	var (
		idx, count, slotCount int
		slots                 string
	)
	if err := dbs[1].QueryRowContext(t.Context(),
		`SELECT db_index, db_count, slot_count, slots FROM shard_meta`).Scan(&idx, &count, &slotCount, &slots); err != nil {
		t.Fatal(err)
	}
	if idx != 1 || count != 2 || slotCount != 8 || slots != "1,3,5,7" {
		t.Errorf("shard_meta of db 1 = (%d, %d, %d, %q)", idx, count, slotCount, slots)
	}
}

// DSN 的顺序写反：每个库里记的序号与它在列表里的位置不同。
func TestEnsureShardMetaRejectsSwappedDBs(t *testing.T) {
	t.Parallel()
	dbs, router := newShardDBs(t, 2)
	if err := store.EnsureShardMeta(t.Context(), dbs, router); err != nil {
		t.Fatal(err)
	}
	swapped := []*sql.DB{dbs[1], dbs[0]}
	if err := store.EnsureShardMeta(t.Context(), swapped, router); !errors.Is(err, store.ErrShardMetaMismatch) {
		t.Fatalf("err = %v, want ErrShardMetaMismatch", err)
	}
}

// 库数与数据不符：按 2 个库初始化过的库，不能被当成 1 个库或 4 个库的布局来用。
func TestEnsureShardMetaRejectsWrongDBCount(t *testing.T) {
	t.Parallel()
	dbs, router := newShardDBs(t, 2)
	if err := store.EnsureShardMeta(t.Context(), dbs, router); err != nil {
		t.Fatal(err)
	}

	one, err := sharding.New(sharding.OrderLayout(1))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureShardMeta(t.Context(), dbs[:1], one); !errors.Is(err, store.ErrShardMetaMismatch) {
		t.Errorf("1-db layout: err = %v, want ErrShardMetaMismatch", err)
	}

	// 扩容后的配置连上没有扩容的库：新加的两个库是空的，旧的两个库会被拦下。
	// 空库排在前面也不能先被写入：配置被拒绝时不留下任何改动。
	more, four := newShardDBs(t, 4)
	mixed := []*sql.DB{more[0], more[1], dbs[0], dbs[1]}
	if err := store.EnsureShardMeta(t.Context(), mixed, four); !errors.Is(err, store.ErrShardMetaMismatch) {
		t.Errorf("4-db layout: err = %v, want ErrShardMetaMismatch", err)
	}
	for i, db := range more[:2] {
		var n int
		if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM shard_meta`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("empty db %d was initialized by a rejected configuration", i)
		}
	}
	if err := store.EnsureShardMeta(t.Context(), dbs, router); err != nil {
		t.Errorf("original layout no longer accepted after a rejected check: %v", err)
	}

	if err := store.EnsureShardMeta(t.Context(), dbs[:1], router); err == nil {
		t.Error("fewer dbs than the layout accepted")
	}
}

// 多个实例同时第一次启动：只有一个写入成功，所有实例都核对通过，表里只有一行。
func TestEnsureShardMetaConcurrentInit(t *testing.T) {
	t.Parallel()
	dbs, router := newShardDBs(t, 2)
	var (
		wg   sync.WaitGroup
		errs = make([]error, 8)
	)
	for i := range errs {
		wg.Go(func() { errs[i] = store.EnsureShardMeta(t.Context(), dbs, router) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("instance %d: %v", i, err)
		}
	}
	for i, db := range dbs {
		var n int
		if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM shard_meta`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("db %d has %d shard_meta rows, want 1", i, n)
		}
	}
}
