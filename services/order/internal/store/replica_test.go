package store_test

import (
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 测试不搭真正的复制：“空从库”是另一组只跑了迁移的库（延迟无限大），
// “同库从库”是连到主库同一个库的另一个连接池（已追上）。

// emptyReplicas 返回一组只跑了迁移、没有数据的库。
func (f *fixture) emptyReplicas(t *testing.T) []*sql.DB {
	t.Helper()
	dbs := make([]*sql.DB, f.router.DBs())
	for i := range dbs {
		dbs[i] = testdb.NewTemplate(t, order.Migrations, order.MigrationsDir, order.MigrationData{Slots: f.router.SlotsOf(i)})
	}
	return dbs
}

// sameReplicas 返回连到各主库同一个库的另一组连接池。
func (f *fixture) sameReplicas(t *testing.T) []*sql.DB {
	t.Helper()
	out := make([]*sql.DB, 0, len(f.dbs))
	for _, db := range f.dbs {
		var name string
		if err := db.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&name); err != nil {
			t.Fatal(err)
		}
		cfg, err := mysql.ParseDSN(os.Getenv(testdb.DSNEnv))
		if err != nil {
			t.Fatal(err)
		}
		cfg.DBName = name
		other, err := mysqlx.Open(t.Context(), mysqlx.Config{DSN: cfg.FormatDSN(), MaxOpenConns: 4})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = other.Close() })
		out = append(out, other)
	}
	return out
}

func (f *fixture) withReplicas(t *testing.T, replicas []*sql.DB) *store.Store {
	t.Helper()
	st, err := store.New(f.dbs, f.router, store.WithReplicas(replicas))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestWithReplicasValidates(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	db := &sql.DB{}
	for name, rs := range map[string][]*sql.DB{
		"empty":     {},
		"too few":   {db},
		"too many":  {db, db, db},
		"nil entry": {db, nil},
	} {
		if _, err := store.New(f.dbs, f.router, store.WithReplicas(rs)); err == nil {
			t.Errorf("%s: New succeeded, want error", name)
		}
	}
	if f.store.HasReplicas() {
		t.Error("store without WithReplicas reports replicas")
	}
	if st := f.withReplicas(t, []*sql.DB{db, db}); !st.HasReplicas() {
		t.Error("store with replicas reports none")
	}
}

func TestGetOrderFromReplica(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	o := f.newOrder(t, 1, "k")
	if err := f.store.InsertOrder(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	st := f.withReplicas(t, f.emptyReplicas(t))
	if _, err := st.GetOrderFromReplica(t.Context(), o.OrderID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetOrderFromReplica on empty replica = %v, want ErrNotFound", err)
	}
	// 其余读方法照旧读主库。
	if _, err := st.GetOrder(t.Context(), o.OrderID); err != nil {
		t.Errorf("GetOrder: %v", err)
	}
	if _, err := st.GetOrderByIdempotencyKey(t.Context(), 1, "k"); err != nil {
		t.Errorf("GetOrderByIdempotencyKey: %v", err)
	}
	// 没有从库时读主库。
	got, err := f.store.GetOrderFromReplica(t.Context(), o.OrderID)
	if err != nil {
		t.Fatalf("GetOrderFromReplica without replicas: %v", err)
	}
	if got.OrderID != o.OrderID || len(got.Items) != 1 {
		t.Errorf("got %+v", got)
	}
}

func TestListBySource(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	creating := f.newOrder(t, 1, "c")
	pending := f.newOrder(t, 2, "p")
	for _, o := range []*store.Order{creating, pending} {
		if err := f.store.InsertOrder(t.Context(), o); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := f.store.MarkReserved(t.Context(), pending.OrderID, 100, "CNY", pending.Items, pending.UpdatedAt); err != nil || !ok {
		t.Fatalf("MarkReserved = %v, %v", ok, err)
	}
	later := pending.ExpireAt.Add(time.Second)
	empty := f.withReplicas(t, f.emptyReplicas(t))

	count := func(st *store.Store, src store.Source) (expired, stale int) {
		t.Helper()
		for slot := range f.router.Slots() {
			e, err := st.ListExpired(t.Context(), slot, later, 100, src)
			if err != nil {
				t.Fatal(err)
			}
			s, err := st.ListStaleCreating(t.Context(), slot, later, 100, src)
			if err != nil {
				t.Fatal(err)
			}
			expired += len(e)
			stale += len(s)
		}
		return expired, stale
	}
	for _, c := range []struct {
		name           string
		st             *store.Store
		src            store.Source
		expired, stale int
	}{
		{"primary with replicas", empty, store.Primary, 1, 1},
		{"empty replica", empty, store.Replica, 0, 0},
		{"replica without replicas reads primary", f.store, store.Replica, 1, 1},
	} {
		if e, s := count(c.st, c.src); e != c.expired || s != c.stale {
			t.Errorf("%s: expired %d stale %d, want %d %d", c.name, e, s, c.expired, c.stale)
		}
	}
}

func TestPrimaryPosition(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	o := f.newOrder(t, 1, "k")
	p1, err := f.store.PrimaryPosition(t.Context(), o.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if p1.GTID == "" || strings.ContainsFunc(p1.GTID, func(r rune) bool { return r == ' ' || r == '\n' || r == '\t' }) {
		t.Errorf("GTID = %q, want non-empty without whitespace", p1.GTID)
	}
	if err := f.store.InsertOrder(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	p2, err := f.store.PrimaryPosition(t.Context(), o.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	if p2.At <= p1.At {
		t.Errorf("At did not advance: %d then %d", p1.At, p2.At)
	}
	if now := time.Now().UnixMicro(); p2.At < now-time.Minute.Microseconds() || p2.At > now+time.Minute.Microseconds() {
		t.Errorf("At = %d, local clock %d: not Unix microseconds?", p2.At, now)
	}
}

func TestWaitReplica(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	o := f.newOrder(t, 1, "k")
	if err := f.store.InsertOrder(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	pos, err := f.store.PrimaryPosition(t.Context(), o.OrderID)
	if err != nil {
		t.Fatal(err)
	}
	st := f.withReplicas(t, f.sameReplicas(t))

	start := time.Now()
	ok, err := st.WaitReplica(t.Context(), o.OrderID, pos.GTID, time.Second)
	if err != nil || !ok {
		t.Fatalf("WaitReplica(current) = %v, %v; want true", ok, err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("WaitReplica(current) took %s, want immediate", d)
	}

	// 永远到不了的位置：时限到后返回 false，不报错。
	uuid, _, _ := strings.Cut(pos.GTID, ":")
	const timeout = 200 * time.Millisecond
	start = time.Now()
	ok, err = st.WaitReplica(t.Context(), o.OrderID, uuid+":9000000000000000000", timeout)
	d := time.Since(start)
	if err != nil || ok {
		t.Fatalf("WaitReplica(future) = %v, %v; want false, nil", ok, err)
	}
	if d < timeout-20*time.Millisecond || d > timeout+time.Second {
		t.Errorf("WaitReplica(future) took %s, want about %s", d, timeout)
	}

	// 0 在 MySQL 里表示无限等待，必须拒绝。
	if _, err := st.WaitReplica(t.Context(), o.OrderID, pos.GTID, 0); err == nil {
		t.Error("WaitReplica with zero timeout succeeded, want error")
	}
	if _, err := f.store.WaitReplica(t.Context(), o.OrderID, pos.GTID, time.Second); !errors.Is(err, store.ErrNoReplica) {
		t.Errorf("WaitReplica without replicas = %v, want ErrNoReplica", err)
	}
}
