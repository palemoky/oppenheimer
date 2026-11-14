package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/bulkhead"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 本文件测试进程内的服务保护（设计文档 1d 的 5.6、5.7、5.9）：展示读的并发上限、冷库的上限和时限、
// 从库和闸门的时限、调用方取消和超时时的错误映射。
//
// 让查询“停在持有名额的状态”用两种确定的办法，不靠 sleep：
//   - 对订单所在 slot 的表加 LOCK TABLES ... WRITE：别的会话读这张表会一直等，而同一个库上别的 slot 的表照常读写。
//   - 只有一个连接、且这个连接被占住的连接池（hungDB）：任何查询都拿不到连接，只能等到 ctx 到期。

// ---------- 测试工具 ----------

// logRecord 是 recordHandler 记下的一条日志。
type logRecord struct {
	level slog.Level
	msg   string
}

// recordHandler 记下每条日志的级别和消息，用来断言“没有 ERROR”“有一条 WARN”。并发安全。
type recordHandler struct {
	mu   sync.Mutex
	recs []logRecord
}

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, logRecord{level: r.Level, msg: r.Message})
	return nil
}

func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(string) slog.Handler      { return h }

// at 返回级别恰好为 level 的日志消息。
func (h *recordHandler) at(level slog.Level) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.recs {
		if r.level == level {
			out = append(out, r.msg)
		}
	}
	return out
}

func (h *recordHandler) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = nil
}

// wantLogs 断言 ERROR 和 WARN 的条数。
func (h *recordHandler) wantLogs(t *testing.T, errorsWant, warnsWant int) {
	t.Helper()
	if got := h.at(slog.LevelError); len(got) != errorsWant {
		t.Errorf("ERROR logs = %q, want %d", got, errorsWant)
	}
	if got := h.at(slog.LevelWarn); len(got) != warnsWant {
		t.Errorf("WARN logs = %q, want %d", got, warnsWant)
	}
}

// serviceWith 返回与 e.svc 共用依赖的另一个 Service，mod 可以改依赖和配置。
// 名额是每个 Service 自己的，要观察名额就要用同一个 Service 发起所有查询。
func (e *env) serviceWith(t *testing.T, mod func(d *Deps, c *Config)) *Service {
	t.Helper()
	ids, err := idgen.New(3)
	if err != nil {
		t.Fatal(err)
	}
	d := Deps{
		Store: e.store, Gate: e.gate, Sender: e.sender, Inventory: e.inv, IDs: ids,
		Logger: e.svc.logger, Marks: e.marks,
	}
	if e.coldDB != nil {
		d.Cold = store.NewCold(e.coldDB)
	}
	c := Config{
		Clock:            e.clock.Now,
		OrderEventsTopic: testEventsTopic,
		SubmitTopic:      testSubmitTopic,
		CloseDelayTopic:  testCloseTopic,
		Relays:           e.kickers(),
		ReadMode:         e.svc.readMode,
	}
	mod(&d, &c)
	return mustNew(t, d, c)
}

// lockTable 在 db 上用一个专门的连接对 table 加写锁，返回解锁函数（测试结束时也会解锁）。
// 加锁期间别的连接读这张表会一直等，用来让查询确定地停在“已经占了名额、正在等数据库”的状态。
func lockTable(t *testing.T, db *sql.DB, table string) (unlock func()) {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(t.Context(), "LOCK TABLES "+table+" WRITE"); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	var once sync.Once
	unlock = func() {
		once.Do(func() {
			if _, err := conn.ExecContext(context.WithoutCancel(t.Context()), "UNLOCK TABLES"); err != nil {
				t.Errorf("unlock tables: %v", err)
			}
			_ = conn.Close()
		})
	}
	t.Cleanup(unlock)
	return unlock
}

// hungDB 返回一个只有一个连接、且这个连接已被占住的连接池：任何查询都拿不到连接，等到 ctx 到期为止。
func hungDB(t *testing.T) *sql.DB {
	t.Helper()
	db, _ := testdb.Empty(t)
	db.SetMaxOpenConns(1)
	held, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	return db
}

// waitInFlight 等到 b 恰好有 n 个名额被占。条件满足就返回，不是固定的 sleep。
func waitInFlight(t *testing.T, b *bulkhead.Bulkhead, n int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for b.InFlight() != n {
		if time.Now().After(deadline) {
			t.Fatalf("in-flight = %d, want %d", b.InFlight(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// userOn 返回第一个落在 db 号库、且 slot 不在 except 里的用户。订单和用户在同一个 slot（订单号带用户的基因）。
func userOn(t *testing.T, router *sharding.Router, db int, except ...int) int64 {
	t.Helper()
	for u := int64(1); u < 100000; u++ {
		slot := router.ForUser(u)
		if router.DB(slot) != db {
			continue
		}
		skip := false
		for _, s := range except {
			skip = skip || s == slot
		}
		if !skip {
			return u
		}
	}
	t.Fatal("no user found")
	return 0
}

// getAsync 在另一个协程里查询，结果从返回的 channel 里取。
func getAsync(ctx context.Context, svc *Service, orderID, userID int64) <-chan error {
	ch := make(chan error, 1)
	go func() {
		_, err := svc.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: orderID, UserId: userID})
		ch <- err
	}()
	return ch
}

// ---------- 展示读的并发上限 ----------

// limitScene 是名额测试的布局：0 号库上 locked 用户的订单所在的表被锁住，free 用户在 0 号库的另一张表上，
// other 用户在 1 号库上。
type limitScene struct {
	e                   *env
	svc                 *Service
	locked, free, other int64
	lockedA, lockedB    *orderv1.Order // locked 用户的两个已落库订单
	lockedC             *orderv1.Order // locked 用户的第三个已落库订单，名额满时去查它
	freeOrder, otherOrd *orderv1.Order
	unlock              func()
	blockedA, blockedB  <-chan error
}

// newLimitScene 建好订单，锁住 locked 用户的订单表，让两个查询占满 0 号库的两个名额。
func newLimitScene(t *testing.T, withCache bool) *limitScene {
	t.Helper()
	e := newEnv(t)
	sc := &limitScene{e: e}
	sc.svc = e.serviceWith(t, func(d *Deps, c *Config) {
		c.ReadMaxConcurrency = 2
		if withCache {
			oc, err := NewOrderCache(e.rdb, e.prefix, e.svc.logger)
			if err != nil {
				t.Fatal(err)
			}
			d.OrderCache = oc
		}
	})
	sc.locked = userOn(t, e.router, 0)
	sc.free = userOn(t, e.router, 0, e.router.ForUser(sc.locked))
	sc.other = userOn(t, e.router, 1)
	sc.lockedA = e.mustPending(t, sc.locked, "a")
	sc.lockedB = e.mustPending(t, sc.locked, "b")
	sc.lockedC = e.mustPending(t, sc.locked, "c")
	sc.freeOrder = e.mustPending(t, sc.free, "f")
	sc.otherOrd = e.mustPending(t, sc.other, "o")

	sc.unlock = lockTable(t, e.dbs[0], sharding.Table("orders", e.router.ForUser(sc.locked)))
	sc.blockedA = getAsync(t.Context(), sc.svc, sc.lockedA.GetOrderId(), sc.locked)
	sc.blockedB = getAsync(t.Context(), sc.svc, sc.lockedB.GetOrderId(), sc.locked)
	waitInFlight(t, sc.svc.limits.db(0), 2)
	return sc
}

// release 解锁，等两个被挡住的查询成功返回、名额归还。
func (sc *limitScene) release(t *testing.T) {
	t.Helper()
	sc.unlock()
	for _, ch := range []<-chan error{sc.blockedA, sc.blockedB} {
		if err := <-ch; err != nil {
			t.Fatalf("blocked query after unlock: %v", err)
		}
	}
	waitInFlight(t, sc.svc.limits.db(0), 0)
}

// 名额占满时第三个展示读立刻得到 RESOURCE_EXHAUSTED / READ_BUSY；同一时刻落库、取消不占名额，照常成功；
// 列表也受同一个名额约束。放开之后恢复。
func TestDisplayReadLimit(t *testing.T) {
	t.Parallel()
	sc := newLimitScene(t, false)
	e := sc.e

	_, err := getOrder(t, sc.svc, sc.lockedC.GetOrderId(), sc.locked)
	wantErr(t, err, codes.ResourceExhausted, ReasonReadBusy)
	// 同一个库上别的表的订单也一样：名额是按库的，不是按表。
	_, err = getOrder(t, sc.svc, sc.freeOrder.GetOrderId(), sc.free)
	wantErr(t, err, codes.ResourceExhausted, ReasonReadBusy)
	_, err = sc.svc.ListUserOrders(t.Context(), &orderv1.ListUserOrdersRequest{UserId: sc.free})
	wantErr(t, err, codes.ResourceExhausted, ReasonReadBusy)

	// 写路径不占名额：在 0 号库上落一单、取消一单（都在没锁的表上），都成功。
	o := e.submit(t, sc.free, "landed while full", items(skuA, 1))
	if err := sc.svc.SubmitHandler(0)(t.Context(), e.submitMessage(t, o.GetOrderId())); err != nil {
		t.Fatalf("land while the read limit is full: %v", err)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	resp, err := sc.svc.CancelOrder(t.Context(), cancelReq(sc.freeOrder.GetOrderId(), sc.free))
	if err != nil {
		t.Fatalf("cancel while the read limit is full: %v", err)
	}
	if resp.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
		t.Fatalf("cancelled order status = %s", resp.GetOrder().GetStatus())
	}
	if got := sc.svc.limits.db(0).InFlight(); got != 2 {
		t.Fatalf("in-flight = %d after writes, want 2 (writes must not take read slots)", got)
	}

	sc.release(t)
	mustGetStatus(t, sc.svc, sc.lockedC.GetOrderId(), sc.locked, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if _, err := sc.svc.ListUserOrders(t.Context(), &orderv1.ListUserOrdersRequest{UserId: sc.free}); err != nil {
		t.Fatalf("list after release: %v", err)
	}
	if got := sc.svc.limits.db(0).Rejected(); got != 3 {
		t.Errorf("rejected = %d, want 3", got)
	}
}

// 名额按库：0 号库占满时，1 号库上的订单照常能查、能列。
func TestDisplayReadLimitIsPerDB(t *testing.T) {
	t.Parallel()
	sc := newLimitScene(t, false)
	mustGetStatus(t, sc.svc, sc.otherOrd.GetOrderId(), sc.other, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	resp, err := sc.svc.ListUserOrders(t.Context(), &orderv1.ListUserOrdersRequest{UserId: sc.other})
	if err != nil || len(resp.GetOrders()) != 1 {
		t.Fatalf("list on db 1 while db 0 is full: %v, %v", resp, err)
	}
	if got := sc.svc.limits.db(1).InFlight(); got != 0 {
		t.Fatalf("db 1 in-flight = %d after the reads, want 0", got)
	}
	sc.release(t)
}

// 被名额拒绝的查询不会被缓存成“不存在”：放开之后查得到。
func TestDisplayReadLimitIsNotNegativeCached(t *testing.T) {
	t.Parallel()
	sc := newLimitScene(t, true)
	id := sc.lockedC.GetOrderId()
	for range 2 {
		_, err := getOrder(t, sc.svc, id, sc.locked)
		wantErr(t, err, codes.ResourceExhausted, ReasonReadBusy)
	}
	if _, ok := sc.e.cacheEntry(t, id); ok {
		t.Fatal("a rejected query was cached")
	}
	sc.release(t)
	mustGetStatus(t, sc.svc, id, sc.locked, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
}

// 排队中的订单（占用未认领）被查询时不读订单库，也就不占名额：名额满时照样答 CREATING。
func TestQueuedOrderDoesNotTakeReadSlot(t *testing.T) {
	t.Parallel()
	sc := newLimitScene(t, false)
	queued := sc.e.submit(t, sc.locked, "queued", items(skuA, 1))
	mustGetStatus(t, sc.svc, queued.GetOrderId(), sc.locked, orderv1.OrderStatus_ORDER_STATUS_CREATING)
	if got := sc.svc.limits.db(0).Rejected(); got != 0 {
		t.Fatalf("rejected = %d, want 0", got)
	}
	sc.release(t)
}

// 汇总日志：有拒绝的那一段记一条 WARN，没有拒绝的那一段不输出。
func TestReadLimitReporter(t *testing.T) {
	t.Parallel()
	rec := &recordHandler{}
	limits, err := newReadLimits(2, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	svc := &Service{limits: limits, logger: slog.New(rec)}
	r := svc.ReadLimitReporter(time.Hour)
	release, err := acquire(limits.db(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquire(limits.db(1)); !errors.Is(err, errReadBusy) {
		t.Fatalf("second acquire: %v, want read busy", err)
	}
	release()
	r.Flush(t.Context())
	r.Flush(t.Context())
	if got := rec.at(slog.LevelWarn); len(got) != 1 {
		t.Fatalf("WARN logs = %q, want exactly one summary", got)
	}
}

// ---------- 冷库的上限和时限 ----------

// 冷库卡住时：旧订单的查询约 1 秒后答 UNAVAILABLE（这一跳的时限，记 WARN、不记 ERROR），而不是等到调用方的截止时间；
// 它在等的时候冷库名额已满，另一个旧订单的查询立刻得到 RESOURCE_EXHAUSTED；新订单不碰冷库，不受影响。
func TestColdReadLimitAndTimeout(t *testing.T) {
	t.Parallel()
	e := newColdEnv(t)
	old := e.mustPending(t, 1, "old")
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(old.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	e.consumeEvents(t)
	e.moveToCold(t, old.GetOrderId())
	nextMilli(old.GetOrderId())
	fresh := e.mustPending(t, 1, "fresh")
	e.ageTo(t, fresh.GetOrderId())

	rec := &recordHandler{}
	svc := e.serviceWith(t, func(d *Deps, c *Config) {
		d.Cold = store.NewCold(hungDB(t))
		d.Logger = slog.New(rec)
		c.ArchiveMaxConcurrency = 1
		c.ReadMaxConcurrency = 4
	})

	start := time.Now()
	stuck := getAsync(t.Context(), svc, old.GetOrderId(), 1)
	waitInFlight(t, svc.limits.cold, 1)

	_, err := getOrder(t, svc, old.GetOrderId(), 1)
	wantErr(t, err, codes.ResourceExhausted, ReasonReadBusy)
	_, err = svc.ListUserOrders(t.Context(), &orderv1.ListUserOrdersRequest{UserId: 1})
	wantErr(t, err, codes.ResourceExhausted, ReasonReadBusy)
	mustGetStatus(t, svc, fresh.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)

	err = <-stuck
	elapsed := time.Since(start)
	wantErr(t, err, codes.Unavailable, "")
	if elapsed < coldTimeout-50*time.Millisecond || elapsed > coldTimeout+time.Second {
		t.Errorf("stuck cold read returned after %s, want about %s", elapsed, coldTimeout)
	}
	rec.wantLogs(t, 0, 1)
	waitInFlight(t, svc.limits.cold, 0)
	if got := svc.limits.db(0).InFlight() + svc.limits.db(1).InFlight(); got != 0 {
		t.Errorf("order db read slots still taken: %d", got)
	}
}

// ---------- 从库和闸门的时限 ----------

// 从库卡住时，展示读最多等 replicaTimeout 就退回主库并成功。
func TestReplicaTimeoutFallsBackToPrimary(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	hung := hungDB(t)
	st, err := store.New(e.dbs, e.router, store.WithReplicas([]*sql.DB{hung, hung}))
	if err != nil {
		t.Fatal(err)
	}
	svc := e.serviceWith(t, func(d *Deps, c *Config) {
		d.Store = st
		c.ReadMode = ReadWindow
		c.ReadMaxConcurrency = 2
	})
	// 没有 pin（落库时打的 pin 在 Redis 里，删掉模拟它到期），这次展示读会去读从库。
	e.dropMark(t, "pin", o.GetOrderId())
	start := time.Now()
	mustGetStatus(t, svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if elapsed := time.Since(start); elapsed < replicaTimeout-50*time.Millisecond || elapsed > replicaTimeout+time.Second {
		t.Errorf("display read with a hung replica took %s, want about %s", elapsed, replicaTimeout)
	}
}

// blackHoleRedis 返回一个连得上、但从不回复的 Redis 客户端：模拟 Redis 卡住。
// 不用 CLIENT PAUSE：测试共用一个 Redis 实例，它会卡住所有并行的测试。
func blackHoleRedis(t *testing.T) *redis.Client {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	// 和 redisx.Open 一样遵守 ctx 的时限，读超时保持 1 秒：生效的必须是调用点的时限。
	client := redis.NewClient(&redis.Options{
		Addr: ln.Addr().String(), ReadTimeout: time.Second, WriteTimeout: time.Second,
		ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() {
		_ = client.Close()
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return client
}

// Redis 卡住时，查询已落库的订单在 gateTimeout 之后退回只读订单行，照常成功；耗时是几百毫秒而不是秒级。
// 查不到行的订单答 UNAVAILABLE。受理在 gateTimeout 之后答 UNAVAILABLE。
func TestGateTimeout(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	svc := e.serviceWith(t, func(d *Deps, _ *Config) {
		d.Gate = stockgate.New(blackHoleRedis(t), e.prefix)
	})

	start := time.Now()
	mustGetStatus(t, svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if elapsed := time.Since(start); elapsed < gateTimeout-50*time.Millisecond || elapsed > gateTimeout+500*time.Millisecond {
		t.Errorf("GetOrder with a stuck gate took %s, want about %s", elapsed, gateTimeout)
	}
	_, err := getOrder(t, svc, 424242, 1)
	wantErr(t, err, codes.Unavailable, "")

	start = time.Now()
	_, err = svc.SubmitOrder(t.Context(), submitReq(1, "stuck", items(skuA, 1)))
	wantErr(t, err, codes.Unavailable, "")
	// Hold 一次 gateTimeout，撤销（abandon）再一次。
	if elapsed := time.Since(start); elapsed > 2*gateTimeout+500*time.Millisecond {
		t.Errorf("SubmitOrder with a stuck gate took %s, want about %s", elapsed, 2*gateTimeout)
	}
}

// ---------- 调用方取消、超时的错误映射 ----------

// hungStore 返回一个每个库都卡住的 store（见 hungDB）。
func hungStore(t *testing.T, e *env) *store.Store {
	t.Helper()
	hung := hungDB(t)
	st, err := store.New([]*sql.DB{hung, hung}, e.router)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestContextErrorMapping(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k") // 占用已认领：查询会读订单库
	id := o.GetOrderId()
	db := e.store.DBOfOrder(id)

	t.Run("caller canceled", func(t *testing.T) {
		t.Parallel()
		rec := &recordHandler{}
		svc := e.serviceWith(t, func(d *Deps, c *Config) {
			d.Store, d.Logger, c.ReadMaxConcurrency = hungStore(t, e), slog.New(rec), 4
		})
		ctx, cancel := context.WithCancel(t.Context())
		ch := getAsync(ctx, svc, id, 1)
		waitInFlight(t, svc.limits.db(db), 1) // 已经在等订单库了
		cancel()
		err := <-ch
		wantErr(t, err, codes.Canceled, "")
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v does not unwrap to context.Canceled", err)
		}
		rec.wantLogs(t, 0, 0)

		// 一开始就取消的请求（读闸门就失败了）也一样。
		rec.reset()
		_, err = svc.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: id, UserId: 1})
		wantErr(t, err, codes.Canceled, "")
		rec.wantLogs(t, 0, 0)
	})

	t.Run("caller deadline", func(t *testing.T) {
		t.Parallel()
		rec := &recordHandler{}
		svc := e.serviceWith(t, func(d *Deps, _ *Config) {
			d.Store, d.Logger = hungStore(t, e), slog.New(rec)
		})
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		_, err := svc.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: id, UserId: 1})
		wantErr(t, err, codes.DeadlineExceeded, "")
		rec.wantLogs(t, 0, 1)
	})

	t.Run("hop timeout", func(t *testing.T) {
		t.Parallel()
		rec := &recordHandler{}
		svc := e.serviceWith(t, func(d *Deps, _ *Config) { d.Logger = slog.New(rec) })
		// 请求的 ctx 还有效，错误来自这一跳自己的时限。
		err := svc.internal(t.Context(), "op", hopErr(expiredHop(t), context.DeadlineExceeded))
		wantErr(t, err, codes.Unavailable, "")
		// 驱动在超时后返回的不一定是 context.DeadlineExceeded，按 ctx 的状态标记过的同样识别。
		err = svc.internal(t.Context(), "op", hopErr(expiredHop(t), errors.New("i/o timeout")))
		wantErr(t, err, codes.Unavailable, "")
		rec.wantLogs(t, 0, 2)
	})

	t.Run("internal error", func(t *testing.T) {
		t.Parallel()
		rec := &recordHandler{}
		closed := reopen(t, e.dbs[0])
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		st, err := store.New([]*sql.DB{closed, closed}, e.router)
		if err != nil {
			t.Fatal(err)
		}
		svc := e.serviceWith(t, func(d *Deps, _ *Config) { d.Store, d.Logger = st, slog.New(rec) })
		_, err = svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: id, UserId: 1})
		wantErr(t, err, codes.Internal, "")
		rec.wantLogs(t, 1, 0)
	})

	t.Run("shard unavailable keeps its own answer", func(t *testing.T) {
		t.Parallel()
		rec := &recordHandler{}
		svc := e.serviceWith(t, func(d *Deps, _ *Config) { d.Logger = slog.New(rec) })
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		// 表不存在是 MySQL 明确的答复，先于取消判断。
		err := svc.internal(ctx, "op", &mysql.MySQLError{Number: 1146, Message: "Table doesn't exist"})
		if errx.Code(err) != codes.Unavailable {
			t.Fatalf("err = %v, want Unavailable", err)
		}
		rec.wantLogs(t, 0, 1)
	})
}

// expiredHop 返回一个因为自己的时限而到期的 hop ctx（调用方的 ctx 仍然有效）。
func expiredHop(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := hopContext(t.Context(), time.Nanosecond)
	t.Cleanup(cancel)
	<-ctx.Done()
	return ctx
}

// ---------- 消费者路径 ----------

// 落库的 handler 在处理途中被取消（进程收到停止信号）：返回的错误不是 Permanent（不进死信），
// 同一条消息重投之后照常落库。
func TestSubmitHandlerCanceledMidwayIsRetried(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 1, "k", items(skuA, 2))
	msg := e.submitMessage(t, o.GetOrderId())

	ctx, cancel := context.WithCancel(t.Context())
	// Reserve 照常成功，之后的写库遇到已取消的 ctx。
	e.inv.inject("Reserve", fault{before: func(int64) { cancel() }})
	err := e.svc.SubmitHandler(0)(ctx, msg)
	if err == nil || kafkax.IsPermanent(err) {
		t.Fatalf("canceled landing returned %v (permanent=%v), want a retryable error", err, kafkax.IsPermanent(err))
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want it to carry context.Canceled", err)
	}

	if err := e.svc.SubmitHandler(0)(t.Context(), msg); err != nil {
		t.Fatalf("redelivered message: %v", err)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)

	// 一开始就取消（还没认领）也一样：认领失败是可重试的错误。
	o2 := e.submit(t, 1, "k2", items(skuA, 1))
	canceled, cancel2 := context.WithCancel(t.Context())
	cancel2()
	err = e.svc.SubmitHandler(0)(canceled, e.submitMessage(t, o2.GetOrderId()))
	if err == nil || kafkax.IsPermanent(err) {
		t.Fatalf("landing with a canceled ctx returned %v, want a retryable error", err)
	}
	e.mustLand(t, o2.GetOrderId())
	wantStatus(t, e.load(t, o2.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
}
