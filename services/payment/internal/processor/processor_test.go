package processor_test

import (
	"context"
	"database/sql"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/api/eventx"
	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/outbox"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/payment"
	"github.com/palemoky/oppenheimer/services/payment/internal/processor"
	"github.com/palemoky/oppenheimer/services/payment/internal/service"
	"github.com/palemoky/oppenheimer/services/payment/internal/store"
	"github.com/palemoky/oppenheimer/services/payment/internal/testkit"
)

const (
	userID = int64(42)
	amount = int64(5000)
	delay  = 100 * time.Millisecond
	prefix = "test."
)

const (
	pending   = paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING
	succeeded = paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	failed    = paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED
)

const (
	success = paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_SUCCESS
	failure = paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_FAILURE
)

// kicker 记录 Kick 的次数，并在被调用时检查事件是否已经提交（从另一个连接可见）。
type kicker struct {
	t       *testing.T
	db      *sql.DB
	kicks   atomic.Int32
	visible atomic.Bool
}

func (k *kicker) Kick() {
	k.kicks.Add(1)
	n, err := outbox.Pending(k.t.Context(), k.db)
	if err != nil {
		k.t.Error(err)
		return
	}
	k.visible.Store(n > 0)
}

type env struct {
	db     *sql.DB
	store  *store.Store
	svc    *service.Service
	orders *testkit.Orders
	clock  *testkit.Clock
	logger *slog.Logger
	kicker *kicker
	cfg    processor.Config
	proc   *processor.Processor
	nextID int64
}

func newEnv(t *testing.T, cfg processor.Config) *env {
	t.Helper()
	db := testdb.New(t, payment.Migrations, payment.MigrationsDir)
	ids, err := idgen.New(0)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{
		db:     db,
		store:  store.New(db),
		orders: testkit.NewOrders(),
		clock:  testkit.NewClock(time.Date(2026, 3, 1, 8, 0, 0, 0, time.UTC)),
		logger: slog.New(slog.DiscardHandler),
		kicker: &kicker{t: t, db: db},
		nextID: 1000,
	}
	e.svc = service.New(e.store, e.orders, ids, e.clock.Now, e.logger)
	cfg.Now = e.clock.Now
	cfg.ResultDelay = delay
	cfg.TopicPrefix = prefix
	e.cfg = cfg
	e.proc = e.newProcessor(t)
	return e
}

func (e *env) newProcessor(t *testing.T) *processor.Processor {
	t.Helper()
	p, err := processor.New(e.store, e.kicker, e.logger, e.cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// create 新建一个待支付订单并为它发起支付，返回订单号。
func (e *env) create(t *testing.T, outcome paymentv1.SimulatedOutcome) int64 {
	t.Helper()
	e.nextID++
	e.orders.PutOrder(testkit.PendingOrder(e.nextID, userID, amount))
	_, err := e.svc.CreatePayment(t.Context(), &paymentv1.CreatePaymentRequest{OrderId: e.nextID, UserId: userID, Outcome: outcome})
	if err != nil {
		t.Fatal(err)
	}
	return e.nextID
}

func (e *env) round(t *testing.T) {
	t.Helper()
	if err := e.proc.RunOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func (e *env) get(t *testing.T, orderID int64) store.Payment {
	t.Helper()
	p, err := e.store.GetByOrderID(t.Context(), orderID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (e *env) want(t *testing.T, orderID int64, status paymentv1.PaymentStatus) {
	t.Helper()
	p := e.get(t, orderID)
	// 处理器只出结果，不会标记 notified：那要等订单服务答复。
	if p.Status != status || p.Notified {
		t.Fatalf("payment = {status %v, notified %v}, want {%v, false}", p.Status, p.Notified, status)
	}
}

type event struct {
	topic string
	key   string
	ev    *eventv1.PaymentEvent
}

// events 读出 outbox 里的全部消息并解码。
func (e *env) events(t *testing.T) []event {
	t.Helper()
	rows, err := e.db.QueryContext(t.Context(), "SELECT topic, msg_key, payload FROM outbox ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close() //nolint:errcheck // 测试里的只读查询
	var out []event
	for rows.Next() {
		var (
			ev      event
			key     []byte
			payload []byte
		)
		if err := rows.Scan(&ev.topic, &key, &payload); err != nil {
			t.Fatal(err)
		}
		ev.key = string(key)
		if ev.ev, err = eventx.UnmarshalPaymentEvent(payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func (e *env) wantEvents(t *testing.T, n int) []event {
	t.Helper()
	evs := e.events(t)
	if len(evs) != n {
		t.Fatalf("%d events in outbox, want %d", len(evs), n)
	}
	return evs
}

func checkCompleted(t *testing.T, got event, p store.Payment, wantSucceeded bool, at time.Time) {
	t.Helper()
	if got.topic != prefix+eventx.PaymentEvents || got.key != strconv.FormatInt(p.OrderID, 10) {
		t.Errorf("event topic/key = %q/%q, want %q/%d", got.topic, got.key, prefix+eventx.PaymentEvents, p.OrderID)
	}
	ev := got.ev
	c := ev.GetCompleted()
	if ev.GetPaymentId() != p.PaymentID || ev.GetOrderId() != p.OrderID || ev.GetUserId() != userID ||
		c == nil || c.GetSucceeded() != wantSucceeded ||
		c.GetAmount().GetAmount() != amount || c.GetAmount().GetCurrency() != "CNY" {
		t.Errorf("event = %v, want payment %d order %d user %d succeeded %v amount %d CNY",
			ev, p.PaymentID, p.OrderID, userID, wantSucceeded, amount)
	}
	if !ev.GetOccurTime().AsTime().Equal(at) {
		t.Errorf("occur_time = %v, want %v", ev.GetOccurTime().AsTime(), at)
	}
}

func TestResultAfterDelayPublishesOneEvent(t *testing.T) {
	e := newEnv(t, processor.Config{})
	orderID := e.create(t, success)

	e.clock.Advance(delay - time.Microsecond)
	e.round(t)
	e.want(t, orderID, pending)
	e.wantEvents(t, 0)
	if n := e.kicker.kicks.Load(); n != 0 {
		t.Errorf("%d kicks without events, want 0", n)
	}

	e.clock.Advance(time.Microsecond)
	e.round(t)
	e.want(t, orderID, succeeded)
	evs := e.wantEvents(t, 1)
	checkCompleted(t, evs[0], e.get(t, orderID), true, e.clock.Now())

	// 提交之后才 Kick：中继在别的连接上，Kick 时必须已经能看到事件。
	if n := e.kicker.kicks.Load(); n != 1 {
		t.Errorf("%d kicks, want 1", n)
	}
	if !e.kicker.visible.Load() {
		t.Error("event was not visible to other connections when Kick was called")
	}

	// 已出结果的不再处理，也不再产生事件。
	e.clock.Advance(time.Hour)
	e.round(t)
	e.wantEvents(t, 1)
	if n := e.kicker.kicks.Load(); n != 1 {
		t.Errorf("%d kicks after idle round, want 1", n)
	}
}

func TestFailureOutcomePublishesFailed(t *testing.T) {
	e := newEnv(t, processor.Config{})
	orderID := e.create(t, failure)

	e.clock.Advance(delay)
	e.round(t)
	e.want(t, orderID, failed)
	evs := e.wantEvents(t, 1)
	checkCompleted(t, evs[0], e.get(t, orderID), false, e.clock.Now())
}

func TestConcurrentProcessorsPublishOneEventPerPayment(t *testing.T) {
	e := newEnv(t, processor.Config{})
	const n = 20
	orders := make(map[int64]bool, n)
	for i := range n {
		outcome := success
		if i%3 == 0 {
			outcome = failure
		}
		orders[e.create(t, outcome)] = outcome == success
	}
	e.clock.Advance(delay)

	// 模拟多个实例同时运行：同一笔支付会被多个实例同时读到，但只有完成转换的那个写事件。
	procs := make([]*processor.Processor, 4)
	for i := range procs {
		procs[i] = e.newProcessor(t)
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, p := range procs {
		wg.Go(func() {
			<-start
			for range 3 {
				if err := p.RunOnce(t.Context()); err != nil {
					t.Error(err)
				}
			}
		})
	}
	close(start)
	wg.Wait()

	evs := e.wantEvents(t, n)
	seen := make(map[int64]int, n)
	for _, ev := range evs {
		orderID := ev.ev.GetOrderId()
		seen[orderID]++
		wantOK, ok := orders[orderID]
		if !ok {
			t.Fatalf("event for unknown order %d", orderID)
		}
		if ev.ev.GetCompleted().GetSucceeded() != wantOK {
			t.Errorf("order %d: succeeded = %v, want %v", orderID, !wantOK, wantOK)
		}
	}
	for orderID, wantOK := range orders {
		if seen[orderID] != 1 {
			t.Errorf("order %d: %d events, want 1", orderID, seen[orderID])
		}
		want := failed
		if wantOK {
			want = succeeded
		}
		e.want(t, orderID, want)
	}
}

func TestOutboxFailureRollsBackResult(t *testing.T) {
	e := newEnv(t, processor.Config{})
	orderID := e.create(t, success)
	e.clock.Advance(delay)

	// 写事件失败时状态变更一起回滚：不会出现“已出结果但永远不会通知订单服务”的支付。
	testdb.MustExec(t, e.db, "RENAME TABLE outbox TO outbox_away")
	e.round(t)
	e.want(t, orderID, pending)
	if n := e.kicker.kicks.Load(); n != 0 {
		t.Errorf("%d kicks after failed round, want 0", n)
	}

	testdb.MustExec(t, e.db, "RENAME TABLE outbox_away TO outbox")
	e.round(t)
	e.want(t, orderID, succeeded)
	e.wantEvents(t, 1)
}

func TestBatchSizeLimitsEachRound(t *testing.T) {
	e := newEnv(t, processor.Config{BatchSize: 2})
	orders := []int64{e.create(t, success), e.create(t, success), e.create(t, success)}
	e.clock.Advance(delay)

	e.round(t)
	e.wantEvents(t, 2)
	e.want(t, orders[2], pending)

	e.round(t)
	e.wantEvents(t, 3)
	for _, id := range orders {
		e.want(t, id, succeeded)
	}
}

// 一轮里的支付单并发处理：每笔恰好一条事件，状态各自正确，Kick 仍然只有一次、而且在全部提交之后。
// 并发度比批小，名额要被反复复用；带 -race 跑。
func TestRoundCompletesBatchConcurrently(t *testing.T) {
	e := newEnv(t, processor.Config{BatchSize: 64, Concurrency: 8})
	const n = 50
	orders := make(map[int64]bool, n)
	for i := range n {
		outcome := success
		if i%4 == 0 {
			outcome = failure
		}
		orders[e.create(t, outcome)] = outcome == success
	}
	e.clock.Advance(delay)

	e.round(t)

	evs := e.wantEvents(t, n)
	seen := make(map[int64]int, n)
	for _, ev := range evs {
		seen[ev.ev.GetOrderId()]++
	}
	for orderID, wantOK := range orders {
		if seen[orderID] != 1 {
			t.Errorf("order %d: %d events, want 1", orderID, seen[orderID])
		}
		want := failed
		if wantOK {
			want = succeeded
		}
		e.want(t, orderID, want)
	}
	if got := e.kicker.kicks.Load(); got != 1 {
		t.Errorf("kicks = %d, want 1 for the whole round", got)
	}
	if !e.kicker.visible.Load() {
		t.Error("Kick ran before the events were committed")
	}
}

// 并发的一轮里有一笔写不了事件：只有它留在 PENDING，同批的其他笔不受影响，下一轮它自己补上。
// 用触发器让某一个订单的 outbox 插入失败，其余的照常。
func TestOneFailureDoesNotAffectTheRestOfTheBatch(t *testing.T) {
	e := newEnv(t, processor.Config{Concurrency: 4})
	orders := make([]int64, 12)
	for i := range orders {
		orders[i] = e.create(t, success)
	}
	bad := orders[5]
	testdb.MustExec(t, e.db, "CREATE TRIGGER outbox_reject BEFORE INSERT ON outbox FOR EACH ROW "+
		"BEGIN IF NEW.msg_key = '"+strconv.FormatInt(bad, 10)+"' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'rejected by test'; END IF; END")
	e.clock.Advance(delay)

	e.round(t)
	e.wantEvents(t, len(orders)-1)
	for _, id := range orders {
		if id == bad {
			e.want(t, id, pending)
		} else {
			e.want(t, id, succeeded)
		}
	}

	testdb.MustExec(t, e.db, "DROP TRIGGER outbox_reject")
	e.round(t)
	e.wantEvents(t, len(orders))
	e.want(t, bad, succeeded)
}

// 积压超过一批时不等 Interval：取满一批并且出了结果，就紧接着再跑一轮。
// Interval 设成一小时，除了启动时的那一轮，后面的轮次只可能来自处理器自己的 Kick。
func TestFullBatchRunsNextRoundWithoutWaiting(t *testing.T) {
	e := newEnv(t, processor.Config{Interval: time.Hour, BatchSize: 5, Concurrency: 2})
	const n = 23
	for range n {
		e.create(t, success)
	}
	e.clock.Advance(delay)

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := e.proc.Start(t.Context()); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(func() {
		if err := e.proc.Stop(context.WithoutCancel(t.Context())); err != nil {
			t.Error(err)
		}
		wg.Wait()
	})

	waitFor(t, "backlog drained", func() bool { return len(e.events(t)) == n })
}

// 整批都失败时不能自己接着再跑：那会变成不停打数据库的循环。退回到按 Interval 重试。
func TestFailedBatchDoesNotSpin(t *testing.T) {
	g := &guard{}
	g.allow.Store(true)
	e := newEnv(t, processor.Config{Interval: time.Hour, BatchSize: 3, Concurrency: 3, Guard: g})
	for range 3 {
		e.create(t, success)
	}
	testdb.MustExec(t, e.db, "RENAME TABLE outbox TO outbox_away")
	e.clock.Advance(delay)

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := e.proc.Start(t.Context()); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(func() {
		if err := e.proc.Stop(context.WithoutCancel(t.Context())); err != nil {
			t.Error(err)
		}
		wg.Wait()
	})

	// 每一轮都先问 Guard，所以问过的次数就是轮数。启动时那一轮跑完之后留足时间，看有没有第二轮。
	waitFor(t, "first round", func() bool { return g.calls.Load() >= 1 })
	time.Sleep(300 * time.Millisecond)
	if got := g.calls.Load(); got != 1 {
		t.Errorf("%d rounds ran with every payment failing, want only the first", got)
	}
	testdb.MustExec(t, e.db, "RENAME TABLE outbox_away TO outbox")
}

// guard 是可切换的 worker.Guard，同时记录被问了多少次。
type guard struct {
	allow atomic.Bool
	calls atomic.Int32
}

func (g *guard) Allow(context.Context) (bool, error) {
	g.calls.Add(1)
	return g.allow.Load(), nil
}

func TestGuardGatesProcessing(t *testing.T) {
	g := &guard{}
	e := newEnv(t, processor.Config{Interval: 10 * time.Millisecond, Guard: g})
	orderID := e.create(t, success)
	e.clock.Advance(delay)

	// Start 阻塞到 Stop，放进 goroutine；Cleanup 里 Stop 并等它退出。
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := e.proc.Start(t.Context()); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(func() {
		if err := e.proc.Stop(context.WithoutCancel(t.Context())); err != nil {
			t.Error(err)
		}
		wg.Wait()
	})

	// Guard 拒绝时：确认它被问过好几轮，支付单仍是待处理，没有事件。
	waitFor(t, "guard asked 3 times", func() bool { return g.calls.Load() >= 3 })
	e.want(t, orderID, pending)
	e.wantEvents(t, 0)

	g.allow.Store(true)
	waitFor(t, "payment processed", func() bool { return e.get(t, orderID).Status == succeeded })
	e.want(t, orderID, succeeded)
	e.wantEvents(t, 1)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
