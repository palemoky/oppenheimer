package service

import (
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

// 读己之写的测试不搭真正的复制，“从库”用两种极端代替（见 replicaKind）：
//   - 空从库（延迟无限大）。从库查不到行时 fetch 会按占用的认领状态退回主库或合成 CREATING，
//     所以单靠空从库从 GetOrder 的结果上看不出读走了哪边。需要证明路由时，用 copyToReplica 在空从库里
//     放一份旧行，再在主库上推进状态：读到旧状态说明走了从库，读到新状态说明走了主库。
//   - 同库从库（已追上）：读到的和主库一样。
//
// 这里验证不了的：真实延迟下 WAIT_FOR_EXECUTED_GTID_SET 等到一半追上、或等满时限后退回主库的时序。
// 空从库和主库在同一个实例上，等任何真实存在的位置都会立即返回“已追上”；超时退回主库这条分支只能用
// 伪造的、永远等不到的位置触发。真实的主从延迟留给端到端测试。

func (e *env) markKey(kind string, orderID int64) string {
	return e.prefix + "order:rw:" + kind + ":" + strconv.FormatInt(orderID, 10)
}

func (e *env) hasMark(t *testing.T, kind string, orderID int64) bool {
	t.Helper()
	n, err := e.rdb.Exists(t.Context(), e.markKey(kind, orderID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// dropMark 删掉订单的某种标记，模拟它到期。
func (e *env) dropMark(t *testing.T, kind string, orderID int64) {
	t.Helper()
	if err := e.rdb.Del(t.Context(), e.markKey(kind, orderID)).Err(); err != nil {
		t.Fatal(err)
	}
}

// markKeys 返回本测试前缀下的全部标记 key。
func (e *env) markKeys(t *testing.T) []string {
	t.Helper()
	keys, err := e.rdb.Keys(t.Context(), e.prefix+"order:rw:*").Result()
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestNewChecksReadMode(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, envOptions{replica: emptyReplica})
	plain := newEnv(t)
	for name, c := range map[string]struct {
		deps Deps
		mode ReadMode
	}{
		"window without replicas": {Deps{Store: plain.store, Marks: e.marks}, ReadWindow},
		"gtid without replicas":   {Deps{Store: plain.store, Marks: e.marks}, ReadGTID},
		"window without store":    {Deps{Marks: e.marks}, ReadWindow},
		"gtid without marks":      {Deps{Store: e.store}, ReadGTID},
		"unknown mode":            {Deps{Store: e.store, Marks: e.marks}, ReadMode(9)},
	} {
		if _, err := New(c.deps, Config{ReadMode: c.mode}); err == nil {
			t.Errorf("%s: New succeeded, want error", name)
		}
	}
	// 只读主库时什么都不需要；有从库也可以（只是不用）。
	for _, d := range []Deps{{}, {Store: e.store}} {
		if _, err := New(d, Config{}); err != nil {
			t.Errorf("ReadPrimary: %v", err)
		}
	}
}

// ReadPrimary：有从库也不读，也不碰标记。
func TestReadPrimaryIgnoresReplicaAndMarks(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, envOptions{replica: emptyReplica})
	o := e.mustPending(t, 1, "k")
	mustGetStatus(t, e.svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	mustGetStatus(t, e.svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	if keys := e.markKeys(t); len(keys) != 0 {
		t.Errorf("marks written in ReadPrimary: %v", keys)
	}
}

// ReadWindow：pin 还在时读主库，看到最新状态。
func TestReadWindowPinReadsPrimary(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: ReadWindow})
	o := e.mustPending(t, 1, "k")
	id := o.GetOrderId()
	e.copyToReplica(t, id) // 从库停在 PENDING_PAYMENT
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(id, 1)); err != nil {
		t.Fatal(err)
	}
	if !e.hasMark(t, "pin", id) {
		t.Fatal("no pin after cancel")
	}
	if e.hasMark(t, "gtid", id) {
		t.Error("gtid mark written in ReadWindow")
	}
	mustGetStatus(t, e.svc, id, 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
}

// 已知局限，不是期望的行为：pin 到期而从库上的行是旧的（延迟超过了窗口），展示读就读到旧状态。
// 这条同时证明 pin 到期后的读确实走了从库。
func TestReadWindowStaleReplicaRowIsKnownLimitation(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: ReadWindow})
	o := e.mustPending(t, 1, "k")
	id := o.GetOrderId()
	e.copyToReplica(t, id)
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(id, 1)); err != nil {
		t.Fatal(err)
	}
	e.dropMark(t, "pin", id)
	mustGetStatus(t, e.svc, id, 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
}

// 从库没有行、占用已被认领（订单已落库）：以主库为准，答 PENDING_PAYMENT 而不是从占用合成的 CREATING，
// 状态不会往回退。
func TestDisplayReadClaimedHoldReadsPrimary(t *testing.T) {
	t.Parallel()
	for _, mode := range []ReadMode{ReadWindow, ReadGTID} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: mode})
			o := e.mustPending(t, 1, "k")
			id := o.GetOrderId()
			if h, ok := e.hold(t, id); !ok || !h.Claimed {
				t.Fatalf("hold = %+v, %v; want claimed", h, ok)
			}
			e.dropMark(t, "pin", id)
			e.dropMark(t, "gtid", id)
			mustGetStatus(t, e.svc, id, 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
		})
	}
}

// 占用未被认领（只受理、还没落库）：直接从占用合成 CREATING，不读主库。
// 用一个主库连不上的 store 证明这一点：读了主库就会报 Internal。
func TestDisplayReadUnclaimedHoldSkipsPrimary(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: ReadWindow})
	o := e.submit(t, 1, "k", items(skuA, 1))
	broken := make([]*sql.DB, e.router.DBs())
	for i := range broken {
		broken[i] = brokenDB(t)
	}
	st, err := store.New(broken, e.router, store.WithReplicas(e.replicas))
	if err != nil {
		t.Fatal(err)
	}
	svc := mustNew(t, Deps{Store: st, Gate: e.gate, Logger: e.svc.logger, Marks: e.marks},
		Config{Clock: e.clock.Now, ReadMode: ReadWindow})
	mustGetStatus(t, svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CREATING)
	// 对照：同一个订单落库（认领）之后，这个 service 要去主库确认，于是报错。
	e.mustLand(t, o.GetOrderId())
	e.dropMark(t, "pin", o.GetOrderId())
	if _, err := getOrder(t, svc, o.GetOrderId(), 1); errx.Code(err) != codes.Internal {
		t.Fatalf("GetOrder of claimed order with broken primary = %v, want Internal", err)
	}
}

// 订单不会消失：走到终态、占用归还、标记到期、从库没有这一行，GetOrder 仍由主库复查答出终态，
// 而不是 NOT_FOUND；带缓存时也不会把“不存在”写进缓存。
func TestDisplayReadNeverLosesOrder(t *testing.T) {
	t.Parallel()
	for _, mode := range []ReadMode{ReadWindow, ReadGTID} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: mode})
			o := e.mustPending(t, 1, "k")
			id := o.GetOrderId()
			if _, err := e.svc.CancelOrder(t.Context(), cancelReq(id, 1)); err != nil {
				t.Fatal(err)
			}
			e.consumeEvents(t) // 库存服务释放并归还占用
			if _, ok := e.hold(t, id); ok {
				t.Fatal("hold still present after cancel was consumed")
			}
			e.dropMark(t, "pin", id)
			e.dropMark(t, "gtid", id)

			cached := e.readModeService(t, mode, true)
			for _, svc := range []*Service{e.svc, cached, cached} {
				mustGetStatus(t, svc, id, 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
			}
		})
	}
}

// readModeService 返回与 e.svc 共用依赖、读模式为 mode 的 Service，withCache 时带查询缓存。
func (e *env) readModeService(t *testing.T, mode ReadMode, withCache bool) *Service {
	t.Helper()
	d := Deps{Store: e.store, Gate: e.gate, Sender: e.sender, Inventory: e.inv, Logger: e.svc.logger, Marks: e.marks}
	if withCache {
		c, err := NewOrderCache(e.rdb, e.prefix, e.svc.logger)
		if err != nil {
			t.Fatal(err)
		}
		d.OrderCache = c
	}
	return mustNew(t, d, Config{Clock: e.clock.Now, OrderEventsTopic: testEventsTopic, Relays: e.kickers(), ReadMode: mode})
}

// 做决定的读一律走主库：CancelOrder 的返回值、受理时幂等键重复后的查询。
// pin 设为 1 毫秒，写完就到期；从库里放一份 PENDING_PAYMENT 的旧行，这些读如果走了从库就会读到它。
func TestDecisionReadsUsePrimary(t *testing.T) {
	t.Parallel()
	for _, mode := range []ReadMode{ReadPrimary, ReadWindow, ReadGTID} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: mode, pinTTL: time.Millisecond})
			o := e.mustPending(t, 1, "k")
			e.copyToReplica(t, o.GetOrderId())

			c, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1))
			if err != nil {
				t.Fatalf("CancelOrder: %v", err)
			}
			if got := c.GetOrder().GetStatus(); got != orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
				t.Errorf("cancel returned %s, want CANCELLED", got)
			}
			time.Sleep(5 * time.Millisecond)

			// 幂等键重复：返回主库上的 CANCELLED。
			resp, err := e.svc.SubmitOrder(t.Context(), submitReq(1, "k", items(skuA, 2)))
			if err != nil {
				t.Fatal(err)
			}
			if got := resp.GetOrder().GetStatus(); got != orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
				t.Errorf("duplicate submit status = %s, want CANCELLED", got)
			}
		})
	}
}

// ReadGTID：写入后有 GTID 标记；pin 到期后先等从库追上再读从库。
func TestReadGTIDWaitsForReplica(t *testing.T) {
	t.Parallel()

	t.Run("caught up replica", func(t *testing.T) {
		t.Parallel()
		e := newEnvWith(t, envOptions{replica: sameReplica, readMode: ReadGTID})
		o := e.mustPending(t, 1, "k")
		id := o.GetOrderId()
		if !e.hasMark(t, "pin", id) || !e.hasMark(t, "gtid", id) {
			t.Fatal("want both pin and gtid marks after landing")
		}
		e.dropMark(t, "pin", id)
		mustGetStatus(t, e.svc, id, 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
		// 取消后标记的位置往前走了，读到的仍是最新。
		if _, err := e.svc.CancelOrder(t.Context(), cancelReq(id, 1)); err != nil {
			t.Fatal(err)
		}
		e.dropMark(t, "pin", id)
		mustGetStatus(t, e.svc, id, 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	})

	// 空从库与主库同实例：等待立即返回“已追上”，于是读从库里的旧行。
	// 这只证明“等到了就读从库”这条路径接通了，证明不了真实延迟下的新鲜度（真实从库等到了就是新的）。
	t.Run("wait passes then reads replica", func(t *testing.T) {
		t.Parallel()
		e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: ReadGTID})
		o := e.mustPending(t, 1, "k")
		e.copyToReplica(t, o.GetOrderId())
		if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
			t.Fatal(err)
		}
		e.dropMark(t, "pin", o.GetOrderId())
		mustGetStatus(t, e.svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	})

	// 等不到（伪造一个永远到不了的位置）：等满时限后读主库。
	t.Run("wait times out then reads primary", func(t *testing.T) {
		t.Parallel()
		const wait = 50 * time.Millisecond
		e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: ReadGTID, gtidWait: wait})
		o := e.mustPending(t, 1, "k")
		id := o.GetOrderId()
		e.copyToReplica(t, id)
		if _, err := e.svc.CancelOrder(t.Context(), cancelReq(id, 1)); err != nil {
			t.Fatal(err)
		}
		pos, err := e.store.PrimaryPosition(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		e.dropMark(t, "pin", id)
		if err := e.marks.Stamp(t.Context(), id, store.Position{GTID: futureGTID(pos.GTID), At: pos.At + 1}, time.Minute); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		mustGetStatus(t, e.svc, id, 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
		if d := time.Since(start); d < wait {
			t.Errorf("GetOrder took %s, want at least the wait %s", d, wait)
		}
	})

	// 位置标记坏了（从库拒绝这个集合）：报错，读主库。
	t.Run("wait fails then reads primary", func(t *testing.T) {
		t.Parallel()
		e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: ReadGTID})
		o := e.mustPending(t, 1, "k")
		id := o.GetOrderId()
		e.copyToReplica(t, id)
		if _, err := e.svc.CancelOrder(t.Context(), cancelReq(id, 1)); err != nil {
			t.Fatal(err)
		}
		e.dropMark(t, "pin", id)
		if err := e.marks.Stamp(t.Context(), id, store.Position{GTID: "garbage", At: time.Now().UnixMicro() * 2}, time.Minute); err != nil {
			t.Fatal(err)
		}
		mustGetStatus(t, e.svc, id, 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
	})
}

// futureGTID 把 gtid 里第一个来源的事务号换成一个永远到不了的值。
func futureGTID(gtid string) string {
	uuid, _, _ := strings.Cut(gtid, ":")
	return uuid + ":9000000000000000000"
}

// Redis 不可用：写入照常成功（标记失败只记日志），展示读退回主库。
func TestReadMarksUnavailable(t *testing.T) {
	t.Parallel()
	broken := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = broken.Close() })
	e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: ReadGTID, marksRDB: broken})
	o := e.mustPending(t, 1, "k")
	mustGetStatus(t, e.svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	e.copyToReplica(t, o.GetOrderId())
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(o.GetOrderId(), 1)); err != nil {
		t.Fatal(err)
	}
	// 读从库的话会是旧行里的 PENDING_PAYMENT。
	mustGetStatus(t, e.svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_CANCELLED)
}

// sweeper 在从库上找候选：从库旧时这一轮看不到主库上已过期的订单，也不出错。
func TestSweeperStaleReplicaSkipsThisRound(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: ReadWindow})
	o := e.mustPending(t, 1, "k")
	c := e.mustCreating(t, 2, "k")
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	wantStatus(t, e.load(t, c.OrderID), orderv1.OrderStatus_ORDER_STATUS_CREATING)
	if n := len(e.allEvents(t)); n != 0 {
		t.Errorf("events = %d, want 0", n)
	}
}

// 从库查询失败：退回主库，订单照常被关闭、置为失败。
func TestSweeperFallsBackToPrimary(t *testing.T) {
	t.Parallel()
	e := newEnvWith(t, envOptions{replica: brokenReplica})
	o := e.mustPending(t, 1, "k")
	c := e.mustCreating(t, 2, "k")
	e.clock.Advance(DefaultPaymentTimeout + time.Second)
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
	wantStatus(t, e.load(t, c.OrderID), orderv1.OrderStatus_ORDER_STATUS_FAILED)
	e.wantEvents(t, o.GetOrderId(), "closed")
	e.wantEvents(t, c.OrderID, "failed")
}

// 从库挂起（连得上但不响应）：从库那次查询只用 slot 剩余时限的一半，之后还来得及退回主库，
// 主库上过期的订单和创建超时的订单在这一轮里照常处理，不会因为从库挂着而每轮都被跳过。
func TestSweeperHungReplicaFallsBackInTime(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")
	c := e.mustCreating(t, 2, "k")
	e.clock.Advance(DefaultPaymentTimeout + time.Second)

	// 连接池只有一个连接且被占住，从库查询拿不到连接，只能等到 ctx 超时。两个库共用这一个挂起的“从库”。
	hung, _ := testdb.Empty(t)
	hung.SetMaxOpenConns(1)
	held, err := hung.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	st, err := store.New(e.dbs, e.router, store.WithReplicas([]*sql.DB{hung, hung}))
	if err != nil {
		t.Fatal(err)
	}
	svc := mustNew(t, Deps{Store: st, Gate: e.gate, Sender: e.sender, Inventory: e.inv, Logger: e.svc.logger}, Config{
		Clock:            e.clock.Now,
		OrderEventsTopic: testEventsTopic,
		Relays:           e.kickers(),
	})
	const slotTimeout = 400 * time.Millisecond
	sw, err := NewSweeper(svc, SweeperConfig{SlotTimeout: slotTimeout})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	sw.RunOnce(t.Context())
	// 每个 slot 最多耗一个 SlotTimeout（两次从库等待合计约四分之三）；留出余量，只排除“卡住不返回”。
	if elapsed, limit := time.Since(start), 2*slotTimeout*time.Duration(e.router.Slots()); elapsed > limit {
		t.Errorf("RunOnce took %s with a hung replica, want under %s", elapsed, limit)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_CLOSED)
	wantStatus(t, e.load(t, c.OrderID), orderv1.OrderStatus_ORDER_STATUS_FAILED)
	e.wantEvents(t, o.GetOrderId(), "closed")
	e.wantEvents(t, c.OrderID, "failed")
}
