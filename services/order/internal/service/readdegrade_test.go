package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/breaker"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/order/internal/archive"
	"github.com/palemoky/oppenheimer/services/order/internal/readmark"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 读路径的降级（设计文档 1d 的 6.7、6.8）：从库的健康探测、读己之写的标记经过闸门的熔断器、冷库的熔断器和后台迁移、
// 冷库不可用时的列表。

// ---------- 从库 ----------

// laggingTarget 把主库位置换成一个永远等不到的集合，模拟从库落后超过最大延迟。
type laggingTarget struct {
	*store.Store
	lagging atomic.Bool
}

func (l *laggingTarget) PrimaryPositionOfDB(ctx context.Context, db int) (store.Position, error) {
	p, err := l.Store.PrimaryPositionOfDB(ctx, db)
	if err == nil && l.lagging.Load() {
		p.GTID = futureGTID(p.GTID)
	}
	return p, err
}

func wantHealthy(t *testing.T, svc *Service, db int, want bool) {
	t.Helper()
	if got := svc.replicaHealthy(db); got != want {
		_, detail := svc.replicas[db].describe()
		t.Fatalf("replica of db %d healthy = %v (%s), want %v", db, got, detail, want)
	}
}

// 探测的三种结果：追上了是通过，落后、出错是不通过。初始不健康，连续两次通过才恢复；
// 健康时一次不通过就立刻变成不健康。
func TestReplicaProbe(t *testing.T) {
	t.Parallel()
	const maxLag = 50 * time.Millisecond

	t.Run("caught up and lagging", func(t *testing.T) {
		t.Parallel()
		e := newEnvWith(t, envOptions{replica: sameReplica, readMode: ReadWindow})
		svc := newUnprobed(t, Deps{Store: e.store, Marks: e.marks, Logger: e.svc.logger}, Config{Clock: e.clock.Now, ReadMode: ReadWindow})
		p, err := svc.ReplicaProber(time.Hour, maxLag)
		if err != nil {
			t.Fatal(err)
		}
		target := &laggingTarget{Store: e.store}
		p.target = target

		wantHealthy(t, svc, 0, false) // 初始不健康
		p.ProbeOnce(t.Context(), 0)
		wantHealthy(t, svc, 0, false) // 一次通过不够
		p.ProbeOnce(t.Context(), 0)
		wantHealthy(t, svc, 0, true)
		wantHealthy(t, svc, 1, false) // 各库各自探测

		target.lagging.Store(true)
		start := time.Now()
		p.ProbeOnce(t.Context(), 0)
		if d := time.Since(start); d < maxLag {
			t.Errorf("probe of a lagging replica returned after %s, want at least the max lag %s", d, maxLag)
		}
		wantHealthy(t, svc, 0, false) // 一次落后就不健康
		if st := svc.replicaStatus(); !st.Degraded || !strings.Contains(st.Detail, "lag over") {
			t.Errorf("status = %+v, want degraded with the lag", st)
		}

		target.lagging.Store(false)
		p.ProbeOnce(t.Context(), 0)
		target.lagging.Store(true)
		p.ProbeOnce(t.Context(), 0) // 通过、落后：计数清零
		target.lagging.Store(false)
		p.ProbeOnce(t.Context(), 0)
		wantHealthy(t, svc, 0, false)
		p.ProbeOnce(t.Context(), 0)
		wantHealthy(t, svc, 0, true)
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()
		e := newEnvWith(t, envOptions{replica: brokenReplica})
		svc := e.svc // mustNew 把从库标成了健康
		p, err := svc.ReplicaProber(time.Hour, maxLag)
		if err != nil {
			t.Fatal(err)
		}
		wantHealthy(t, svc, 0, true)
		p.ProbeOnce(t.Context(), 0)
		wantHealthy(t, svc, 0, false)
		if st := svc.replicaStatus(); !st.Degraded || !strings.Contains(st.Detail, "probe failed") {
			t.Errorf("status = %+v, want degraded with the probe error", st)
		}
	})

	t.Run("no replicas", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t)
		if _, err := e.svc.ReplicaProber(0, 0); err == nil {
			t.Error("ReplicaProber without replicas succeeded, want error")
		}
		if st := e.svc.replicaStatus(); st.Degraded || !strings.Contains(st.Detail, "no replicas") {
			t.Errorf("status without replicas = %+v, want not degraded and saying there are none", st)
		}
	})
}

// 各库的探测互不影响：0 号库的从库挂住时，1 号库的从库照样在两次探测之后变成健康，不用等 0 号库的探测超时。
func TestReplicaProbeIsPerDB(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	st, err := store.New(e.dbs, e.router, store.WithReplicas([]*sql.DB{hungDB(t), reopen(t, e.dbs[1])}))
	if err != nil {
		t.Fatal(err)
	}
	svc := newUnprobed(t, Deps{Store: st, Logger: e.svc.logger}, Config{Clock: e.clock.Now})
	const maxLag = 200 * time.Millisecond
	p, err := svc.ReplicaProber(10*time.Millisecond, maxLag)
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- p.Start(t.Context()) }()
	start := time.Now()
	for !svc.replicaHealthy(1) {
		if time.Since(start) > maxLag+replicaProbeSlack {
			t.Fatalf("replica of db 1 not healthy after %s: blocked by the hung replica of db 0", time.Since(start))
		}
		time.Sleep(5 * time.Millisecond)
	}
	wantHealthy(t, svc, 0, false)
	if err := p.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

// staleReplicaScene 是一个“从库上的行是旧的”订单：从库停在 PENDING_PAYMENT，主库上已经取消，pin 已经到期。
// 读从库会读到 PENDING_PAYMENT，读主库会读到 CANCELLED。
func staleReplicaScene(t *testing.T, mode ReadMode) (*env, int64) {
	t.Helper()
	e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: mode})
	o := e.mustPending(t, 1, "k")
	id := o.GetOrderId()
	e.copyToReplica(t, id)
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(id, 1)); err != nil {
		t.Fatal(err)
	}
	e.dropMark(t, "pin", id)
	e.dropMark(t, "gtid", id)
	return e, id
}

const (
	pendingPayment = orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT
	cancelledOrder = orderv1.OrderStatus_ORDER_STATUS_CANCELLED
)

// 从库不健康时展示读直接读主库；开关关掉时也一样。从库健康、开关打开时读从库（旧行）作对照。
func TestUnhealthyReplicaReadsPrimary(t *testing.T) {
	t.Parallel()
	for _, mode := range []ReadMode{ReadWindow, ReadGTID} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			e, id := staleReplicaScene(t, mode)
			mustGetStatus(t, e.svc, id, 1, pendingPayment) // 对照：健康的从库被读到了

			unprobed := newUnprobed(t, Deps{Store: e.store, Gate: e.gate, Logger: e.svc.logger, Marks: e.marks},
				Config{Clock: e.clock.Now, ReadMode: mode})
			mustGetStatus(t, unprobed, id, 1, cancelledOrder)

			if err := e.svc.degrade.SetMode(SwitchReplicaRead, degrade.ModeOff); err != nil {
				t.Fatal(err)
			}
			mustGetStatus(t, e.svc, id, 1, cancelledOrder)
			if err := e.svc.degrade.SetMode(SwitchReplicaRead, degrade.ModeAuto); err != nil {
				t.Fatal(err)
			}
			mustGetStatus(t, e.svc, id, 1, pendingPayment)
		})
	}
}

// 从库挂住时：不健康的从库根本不碰（包括 ReadGTID 下的 WaitReplica），展示读和 sweeper 都立刻读主库。
func TestUnhealthyReplicaIsNotTouched(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, envOptions{readMode: ReadPrimary})
	o := e.mustPending(t, 1, "k")
	c := e.mustCreating(t, 2, "k")
	hung := hungDB(t)
	st, err := store.New(e.dbs, e.router, store.WithReplicas([]*sql.DB{hung, hung}))
	if err != nil {
		t.Fatal(err)
	}
	svc := newUnprobed(t, Deps{Store: st, Gate: e.gate, Sender: e.sender, Inventory: e.inv, Logger: e.svc.logger, Marks: e.marks},
		Config{Clock: e.clock.Now, ReadMode: ReadGTID, OrderEventsTopic: testEventsTopic, Relays: e.kickers()})
	// 有 GTID 标记、没有 pin：健康的话会先在从库上 WaitReplica。
	if err := e.marks.Stamp(t.Context(), o.GetOrderId(), store.Position{GTID: "x:1", At: time.Now().UnixMicro()}, time.Minute); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	mustGetStatus(t, svc, o.GetOrderId(), 1, pendingPayment)
	if d := time.Since(start); d > replicaTimeout/2 {
		t.Errorf("display read with an unhealthy hung replica took %s, want it to skip the replica", d)
	}

	const slotTimeout = 400 * time.Millisecond
	sw, err := NewSweeper(svc, SweeperConfig{SlotTimeout: slotTimeout})
	if err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	start = time.Now()
	sw.RunOnce(t.Context())
	// 每个 slot 先试从库的话至少耗 slotTimeout/2；整轮不到一个 slotTimeout 说明一次都没试。
	if d := time.Since(start); d > slotTimeout {
		t.Errorf("sweep with an unhealthy hung replica took %s, want it to skip the replica", d)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
	wantStatus(t, e.load(t, c.OrderID), orderv1.OrderStatus_ORDER_STATUS_FAILED)
}

// 请求路径上读从库超时：这一次退回主库，并立刻把从库标成不健康，下一次直接读主库；只记一条 WARN。
func TestReplicaReadFailureMarksUnhealthy(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	p := e.mustPending(t, 1, "k2")
	if e.store.DBOfOrder(o.GetOrderId()) != e.store.DBOfOrder(p.GetOrderId()) {
		t.Fatal("test setup: orders of one user on different dbs")
	}
	db := e.store.DBOfOrder(o.GetOrderId())
	hung := hungDB(t)
	st, err := store.New(e.dbs, e.router, store.WithReplicas([]*sql.DB{hung, hung}))
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordHandler{}
	svc := e.serviceWith(t, func(d *Deps, c *Config) {
		d.Store = st
		d.Logger = slog.New(rec)
		c.ReadMode = ReadWindow
	})
	e.dropMark(t, "pin", o.GetOrderId())
	e.dropMark(t, "pin", p.GetOrderId())
	wantHealthy(t, svc, db, true)

	start := time.Now()
	mustGetStatus(t, svc, o.GetOrderId(), 1, pendingPayment)
	if d := time.Since(start); d < replicaTimeout-50*time.Millisecond {
		t.Fatalf("first read took %s, want it to wait for the hung replica (%s)", d, replicaTimeout)
	}
	wantHealthy(t, svc, db, false)
	start = time.Now()
	mustGetStatus(t, svc, p.GetOrderId(), 1, pendingPayment)
	if d := time.Since(start); d > replicaTimeout/2 {
		t.Errorf("second read took %s, want it to skip the replica marked unhealthy", d)
	}
	rec.wantLogs(t, 0, 1)
	if st := svc.replicaStatus(); !st.Degraded || !strings.Contains(st.Detail, "read failed") {
		t.Errorf("status = %+v, want degraded by the failed read", st)
	}
}

// 读己之写的标记和闸门在同一个 Redis 上，经过闸门的熔断器：打开之后读标记、pin 都不再访问 Redis（不再等 markTimeout），
// 展示读按“读不到标记”读主库，也不逐条记 WARN。
func TestMarksSkipRedisWhenGateBreakerOpen(t *testing.T) {
	t.Parallel()
	e, id := staleReplicaScene(t, ReadWindow)
	rec := &recordHandler{}
	svc := degradeService(t, e, 1, func(d *Deps, _ *Config) {
		d.Marks = readmark.New(blackHoleRedis(t), e.prefix)
		d.Logger = slog.New(rec)
	})
	// 第一次：读标记卡住 markTimeout，算一次失败，熔断器打开；读主库。
	start := time.Now()
	mustGetStatus(t, svc, id, 1, cancelledOrder)
	if d := time.Since(start); d < markTimeout-50*time.Millisecond {
		t.Fatalf("first read took %s, want it to wait for the stuck marks (%s)", d, markTimeout)
	}
	if st := svc.gateBreaker.State(); st != breaker.Open {
		t.Fatalf("gate breaker = %s after a stuck marks read, want open", st)
	}
	rec.reset()
	start = time.Now()
	for range 5 {
		mustGetStatus(t, svc, id, 1, cancelledOrder)
	}
	// 取消已取消的订单：事务前的 pin 跳过，不等 markTimeout。
	if _, err := svc.CancelOrder(t.Context(), cancelReq(id, 1)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > markTimeout {
		t.Errorf("reads and a write with the gate breaker open took %s, want them not to wait for Redis", d)
	}
	rec.wantLogs(t, 0, 0)
}

// 闸门的熔断器刚恢复：故障期间的写入没有标记，max(pin, gtid) 之内展示读读主库；过了这段时间才读从库。
func TestReplicaDistrustedRightAfterGateRecovery(t *testing.T) {
	t.Parallel()
	for _, mode := range []ReadMode{ReadWindow, ReadGTID} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			e, id := staleReplicaScene(t, mode)
			svc := degradeService(t, e, 1, nil)
			mustGetStatus(t, svc, id, 1, pendingPayment) // 对照：读从库
			svc.gateBreaker.Report(false)
			svc.gateBreaker.Report(true)
			if !svc.gateRecoveredWithin(time.Second) {
				t.Fatal("test setup: gate breaker did not recover")
			}
			mustGetStatus(t, svc, id, 1, cancelledOrder)
			if st := svc.replicaStatus(); !st.Degraded || !strings.Contains(st.Detail, "marks=distrusted") {
				t.Errorf("status = %+v, want degraded with the marks distrusted", st)
			}
			window := svc.marksDistrustWindow()
			if want := map[ReadMode]time.Duration{ReadWindow: DefaultReadPinTTL, ReadGTID: DefaultReadGTIDTTL}[mode]; window != want {
				t.Errorf("distrust window = %s, want %s", window, want)
			}
			e.clock.Advance(window - time.Millisecond)
			mustGetStatus(t, svc, id, 1, cancelledOrder)
			e.clock.Advance(time.Millisecond)
			mustGetStatus(t, svc, id, 1, pendingPayment)
		})
	}
}

// ---------- 冷库 ----------

// archivedScene 准备一个已归档的订单 old（只在冷库）和一个近期订单 fresh（只在热库），时钟拨到 old 可能已归档、fresh 一定没有。
func archivedScene(t *testing.T) (e *env, old, fresh int64) {
	t.Helper()
	e = newColdEnv(t)
	old = e.mustPending(t, 1, "old").GetOrderId()
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(old, 1)); err != nil {
		t.Fatal(err)
	}
	e.consumeEvents(t)
	e.moveToCold(t, old)
	nextMilli(old)
	fresh = e.mustPending(t, 1, "fresh").GetOrderId()
	e.ageTo(t, fresh)
	return e, old, fresh
}

// 启动时冷库还没迁移：旧订单的查询立刻返回降级错误，不访问冷库（冷库其实是好的）；近期订单照常。
// 后台迁移失败时记 WARN 并重试，成功之后旧订单查得到。
func TestArchiveUnavailableUntilMigrated(t *testing.T) {
	t.Parallel()
	e, old, fresh := archivedScene(t)
	var calls atomic.Int32
	rec := &recordHandler{}
	svc := e.serviceWith(t, func(d *Deps, _ *Config) {
		d.Logger = slog.New(rec)
		d.ColdMigrate = func(ctx context.Context) error {
			if calls.Add(1) == 1 {
				return errors.New("archive db unreachable")
			}
			return mysqlx.MigrateTemplate(ctx, e.coldDB, order.ArchiveMigrations, order.ArchiveMigrationsDir,
				order.NewArchiveMigrationData())
		}
	})
	_, err := getOrder(t, svc, old, 1)
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonArchiveUnavailable)
	mustGetStatus(t, svc, fresh, 1, pendingPayment)
	if st := svc.archiveStatus(); !st.Degraded || !strings.Contains(st.Detail, "migrated=false") {
		t.Errorf("status before migration = %+v, want degraded and not migrated", st)
	}

	m := svc.ArchiveMigrator()
	if m == nil {
		t.Fatal("no archive migrator with ColdMigrate set")
	}
	start := time.Now()
	if err := m.Start(t.Context()); err != nil { // 第一次失败，退避约 1 秒后第二次成功，Start 返回
		t.Fatal(err)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("migrate called %d times, want 2", n)
	}
	if d := time.Since(start); d < archiveMigrateBackoff.Initial/2 {
		t.Errorf("retried after %s, want a backoff", d)
	}
	if warns := rec.at(slog.LevelWarn); len(warns) != 1 || !strings.Contains(warns[0], "archive migration failed") {
		t.Errorf("WARN logs = %q, want one for the failed migration", warns)
	}
	mustGetStatus(t, svc, old, 1, cancelledOrder)
	if st := svc.archiveStatus(); st.Degraded {
		t.Errorf("status after migration = %+v, want not degraded", st)
	}
	if err := m.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	// 没有 ColdMigrate 时没有迁移组件，冷库一开始就可用。
	if e.svc.ArchiveMigrator() != nil {
		t.Error("archive migrator without ColdMigrate, want nil")
	}
}

// 冷库挂住：熔断器打开之前旧订单的查询各等一次 coldTimeout，之后立刻返回降级错误；近期订单不受影响；
// 降级错误不进负缓存。冷库恢复、冷却时间过去之后，半开的探测成功，旧订单又查得到。
func TestArchiveBreaker(t *testing.T) {
	t.Parallel()
	e, old, fresh := archivedScene(t)
	rec := &recordHandler{}
	svc := degradeService(t, e, 2, func(d *Deps, _ *Config) {
		d.Logger = slog.New(rec)
		c, err := NewOrderCache(e.rdb, e.prefix, e.svc.logger, nil)
		if err != nil {
			t.Fatal(err)
		}
		d.OrderCache = c
	})
	unlock := lockTable(t, e.coldDB, store.ColdOrders)

	for i := range 2 {
		start := time.Now()
		_, err := getOrder(t, svc, old, 1)
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonArchiveUnavailable)
		if d := time.Since(start); d < coldTimeout-50*time.Millisecond {
			t.Fatalf("query %d took %s, want it to wait for the hung archive (%s)", i, d, coldTimeout)
		}
	}
	if st := svc.coldBreaker.State(); st != breaker.Open {
		t.Fatalf("archive breaker = %s, want open", st)
	}
	rec.reset()
	start := time.Now()
	for range 5 {
		_, err := getOrder(t, svc, old, 1)
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonArchiveUnavailable)
		mustGetStatus(t, svc, fresh, 1, pendingPayment)
	}
	if d := time.Since(start); d > coldTimeout/2 {
		t.Errorf("queries with the archive breaker open took %s, want them not to touch the archive", d)
	}
	rec.wantLogs(t, 0, 0) // 打开期间不逐条记
	if _, ok := e.cacheEntry(t, old); ok {
		t.Fatal("archive failure was cached as not found")
	}
	if st := svc.archiveStatus(); !st.Degraded || !strings.Contains(st.Detail, "breaker=open") {
		t.Errorf("status = %+v, want degraded with the breaker open", st)
	}

	unlock()
	e.clock.Advance(testCooldown)
	mustGetStatus(t, svc, old, 1, cancelledOrder)
	if st := svc.coldBreaker.State(); st != breaker.Closed {
		t.Errorf("archive breaker = %s after a successful probe, want closed", st)
	}
}

// 开关 archive_read 关掉：和熔断器打开一样，旧订单立刻返回降级错误，近期订单照常；打开后恢复。
func TestArchiveSwitch(t *testing.T) {
	t.Parallel()
	e, old, fresh := archivedScene(t)
	if err := e.svc.degrade.SetMode(SwitchArchiveRead, degrade.ModeOff); err != nil {
		t.Fatal(err)
	}
	_, err := getOrder(t, e.svc, old, 1)
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonArchiveUnavailable)
	mustGetStatus(t, e.svc, fresh, 1, pendingPayment)
	if err := e.svc.degrade.SetMode(SwitchArchiveRead, degrade.ModeAuto); err != nil {
		t.Fatal(err)
	}
	mustGetStatus(t, e.svc, old, 1, cancelledOrder)
}

// 支付结果路径查冷库经过同一个熔断器：打开时返回错误让消费者重试（不是“订单不存在”、不要求退款、不写答复），
// 恢复后给出正常的答复。
func TestPaymentOnArchivedOrderWithArchiveBreakerOpen(t *testing.T) {
	t.Parallel()
	e, old, _ := archivedScene(t)
	svc := degradeService(t, e, 1, nil)
	svc.coldBreaker.Report(false)
	if st := svc.coldBreaker.State(); st != breaker.Open {
		t.Fatalf("test setup: archive breaker = %s, want open", st)
	}
	// 开关不管支付结果路径：关掉也照常查（这里熔断器打开，所以仍然失败）。
	if err := svc.degrade.SetMode(SwitchArchiveRead, degrade.ModeOff); err != nil {
		t.Fatal(err)
	}
	outbox := e.outboxRows(t)
	ev := paymentEvent(old, 31, 100, true)
	err := svc.HandlePaymentEvent(t.Context(), ev)
	if err == nil || !errors.Is(err, breaker.ErrOpen) || errors.Is(err, store.ErrNotFound) || isArchiveUnavailable(err) {
		t.Fatalf("payment on an archived order with the breaker open = %v, want a retryable breaker error", err)
	}
	if n := e.outboxRows(t); n != outbox {
		t.Fatalf("outbox grew from %d to %d, want no reply while the archive is unavailable", outbox, n)
	}
	e.clock.Advance(testCooldown)
	if err := svc.HandlePaymentEvent(t.Context(), ev); err != nil {
		t.Fatal(err)
	}
	r := e.reply(t, 31)
	if r.GetDisposition() != refundRequired || r.GetReason() != errx.ReasonOrderNotPayable {
		t.Errorf("reply = %s %q, want REFUND_REQUIRED %q", r.GetDisposition(), r.GetReason(), errx.ReasonOrderNotPayable)
	}
}

// ---------- 冷库不可用时的列表 ----------

// listScene 是一个热库和冷库都有数据的用户：旧的一批里有只在冷库的、两边都有的、卡在热库的，新的一批都在热库。
// 返回用户号、全部订单（热 ∪ 冷，倒序）和新的那一批（倒序）。
func listScene(t *testing.T, e *env) (user int64, all, fresh []int64) {
	t.Helper()
	user = 4
	var old []int64
	for i := range 4 {
		id := e.mustPending(t, user, "old"+string(rune('a'+i))).GetOrderId()
		old = append(old, id)
	}
	for _, id := range old[:3] {
		if _, err := e.svc.CancelOrder(t.Context(), cancelReq(id, user)); err != nil {
			t.Fatal(err)
		}
	}
	nextMilli(old[len(old)-1])
	for i := range 5 {
		fresh = append(fresh, e.mustPending(t, user, "new"+string(rune('a'+i))).GetOrderId())
	}
	e.consumeEvents(t)
	e.ageTo(t, fresh[0])
	e.moveToCold(t, old[0])
	e.moveToCold(t, old[2])
	e.copyToCold(t, old[1]) // 两边都有
	// old[3] 是旧的非终态订单，留在热库。
	all = e.unionIDs(t, user, orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED)
	slices.Reverse(fresh)
	if len(all) != 9 || !slices.Equal(all[:5], fresh) {
		t.Fatalf("test setup: union = %v, fresh = %v", all, fresh)
	}
	return user, all, fresh
}

// 冷库不可用时，列表返回的是冷库正常时完整结果的精确前缀（新于冷热分界的那些），最后一段带 archive_unavailable
// 和游标；之前不需要冷库的页照常。接着取：冷库还不可用时答降级错误，恢复之后用同一个游标取到完整的后续。
func TestListUserOrdersArchiveUnavailable(t *testing.T) {
	t.Parallel()
	for _, how := range []string{"switch off", "archive down"} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()
			e := newColdEnv(t)
			user, all, fresh := listScene(t, e)
			down := e.serviceWith(t, func(d *Deps, _ *Config) {
				if how == "archive down" {
					d.Cold = store.NewCold(brokenDB(t))
				}
			})
			if how == "switch off" {
				if err := down.degrade.SetMode(SwitchArchiveRead, degrade.ModeOff); err != nil {
					t.Fatal(err)
				}
			}
			for _, size := range []int{1, 2, 3, 5, 6, 9, MaxListPageSize} {
				var (
					got    []int64
					before int64
					marked bool
				)
				for range 20 {
					resp, err := down.ListUserOrders(t.Context(), listReq(user, int32(size), before))
					if err != nil {
						wantErr(t, err, codes.FailedPrecondition, errx.ReasonArchiveUnavailable)
						break
					}
					if marked {
						t.Fatalf("size %d: got a page after the archive_unavailable one", size)
					}
					for _, o := range resp.GetOrders() {
						got = append(got, o.GetOrderId())
					}
					marked = resp.GetArchiveUnavailable()
					if marked && resp.GetNextBeforeOrderId() != got[len(got)-1] {
						t.Fatalf("size %d: cursor %d, want the last returned order %d", size, resp.GetNextBeforeOrderId(), got[len(got)-1])
					}
					if !marked && len(resp.GetOrders()) != size {
						t.Fatalf("size %d: a complete page has %d orders", size, len(resp.GetOrders()))
					}
					before = resp.GetNextBeforeOrderId()
					if before == 0 {
						t.Fatalf("size %d: paging ended without reaching the archive", size)
					}
				}
				// 精确：恰好是完整结果的前 len(fresh) 条（新于冷热分界的全部订单），不多不少。
				// 冷热分界恰好落在页的边界上时（size 5），最后一页是完整的正常页，下一次直接答降级错误，没有带标记的那一段。
				if !slices.Equal(got, all[:len(fresh)]) || marked != (len(fresh)%size != 0) {
					t.Fatalf("size %d: degraded list = %v (marked %v), want the exact prefix %v", size, got, marked, all[:len(fresh)])
				}
				// 游标接着用：冷库恢复后（e.svc 的冷库是好的）取到完整的后续。
				rest := listIDs(t, e.svc, listReq(user, int32(size), before))
				if full := slices.Concat(got, rest); !slices.Equal(full, all) {
					t.Fatalf("size %d: degraded prefix + rest after recovery = %v, want %v", size, full, all)
				}
			}
		})
	}
}

// 用户只有旧订单（或者游标已经翻过了冷热分界）：能确定的一段为空，答降级错误而不是空列表。
func TestListUserOrdersArchiveUnavailableNothingFresh(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	old := e.mustPending(t, 1, "old").GetOrderId()
	e.age(t)
	if err := e.svc.degrade.SetMode(SwitchArchiveRead, degrade.ModeOff); err != nil {
		t.Fatal(err)
	}
	_, err := e.svc.ListUserOrders(t.Context(), listReq(1, 0, 0))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonArchiveUnavailable)
	// 对照：old 仍在热库，开关打开时列得出来。
	if err := e.svc.degrade.SetMode(SwitchArchiveRead, degrade.ModeAuto); err != nil {
		t.Fatal(err)
	}
	if got := listIDs(t, e.svc, listReq(1, 0, 0)); !slices.Equal(got, []int64{old}) {
		t.Errorf("list = %v, want [%d]", got, old)
	}
}

// 只有“冷库不可用”才返回前一段；名额满、调用方取消仍然按原来的方式返回错误。
func TestListUserOrdersOtherErrorsAreNotDegraded(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	user, _, _ := listScene(t, e)
	svc := e.serviceWith(t, func(d *Deps, c *Config) {
		d.Cold = store.NewCold(hungDB(t))
		c.ArchiveMaxConcurrency = 1
	})
	stuck := make(chan error, 1)
	go func() {
		_, err := svc.ListUserOrders(t.Context(), listReq(user, MaxListPageSize, 0))
		stuck <- err
	}()
	waitInFlight(t, svc.limits.cold, 1)
	_, err := svc.ListUserOrders(t.Context(), listReq(user, MaxListPageSize, 0))
	wantErr(t, err, codes.ResourceExhausted, ReasonReadBusy)
	if err := <-stuck; err != nil {
		t.Fatalf("stuck list after the archive timed out: %v, want the fresh prefix", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() {
		_, err := svc.ListUserOrders(ctx, listReq(user, MaxListPageSize, 0))
		errc <- err
	}()
	waitInFlight(t, svc.limits.cold, 1)
	cancel()
	wantErr(t, <-errc, codes.Canceled, "")
}

// 近期订单号（新于冷热分界）在冷库不可用时完全不受影响：不查冷库，答“不存在”时照常进负缓存。
func TestFreshOrderIgnoresArchiveState(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	svc := e.serviceWith(t, func(d *Deps, _ *Config) {
		d.Cold = store.NewCold(hungDB(t))
		d.ColdMigrate = func(context.Context) error { return errors.New("never") }
		c, err := NewOrderCache(e.rdb, e.prefix, e.svc.logger, nil)
		if err != nil {
			t.Fatal(err)
		}
		d.OrderCache = c
	})
	newID := idgen.LowerBound(e.clock.Now()) | 1
	if archive.MaybeCold(newID, e.clock.Now()) {
		t.Fatal("test setup: new id is old enough to be archived")
	}
	_, err := getOrder(t, svc, newID, 1)
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	if _, ok := e.cacheEntry(t, newID); !ok {
		t.Error("not found for a fresh id was not negatively cached")
	}
}
