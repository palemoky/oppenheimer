package service

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"

	"github.com/palemoky/oppenheimer/api/eventx"
	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/pkg/testredis"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/order/internal/readmark"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 测试用商品。
const (
	skuA       = 1 // 单价 100
	skuB       = 2 // 单价 250
	skuScarce  = 3 // 只有 1 件
	skuHuge    = 4 // 单价接近 int64 上限的一半
	skuMissing = 99

	priceA = 100
	priceB = 250
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeRelay 记录 Kick 的次数。
type fakeRelay struct{ kicks atomic.Int64 }

func (r *fakeRelay) Kick() { r.kicks.Add(1) }

// 测试用的 topic 名，带前缀以确认 Service 用的是配置的 topic 而不是默认值。
const (
	testEventsTopic = "test." + eventx.OrderEvents
	testSubmitTopic = "test." + eventx.OrderSubmit
	testCloseTopic  = "test." + eventx.OrderCloseDelay
)

// sendFault 决定一次 Send 的结果。persist[i] 为 true 的记录照样“写进去”（被记下来），
// 然后返回 errs。用来模拟“发送失败但消息其实到了”。
type sendFault struct {
	persist []bool
	errs    []error
	// after 在记录之后、返回之前调用，用来在“消息已到、发送方还没拿到结果”的窗口里制造并发。
	after func(recs []kafkax.Record)
}

var errSend = errors.New("fake: produce failed")

// fakeSender 是内存里的 Kafka：记下发出的每条记录，可以按顺序注入失败。
type fakeSender struct {
	mu     sync.Mutex
	recs   []kafkax.Record
	sends  int
	faults []sendFault
}

func (f *fakeSender) inject(fs ...sendFault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults = append(f.faults, fs...)
}

func (f *fakeSender) Send(ctx context.Context, recs ...kafkax.Record) ([]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.sends++
	var ft *sendFault
	if len(f.faults) > 0 {
		ft = &f.faults[0]
		f.faults = f.faults[1:]
	}
	if ft == nil {
		f.recs = append(f.recs, recs...)
		f.mu.Unlock()
		return make([]error, len(recs)), nil
	}
	for i, r := range recs {
		if i < len(ft.persist) && ft.persist[i] {
			f.recs = append(f.recs, r)
		}
	}
	f.mu.Unlock()
	if ft.after != nil {
		ft.after(recs)
	}
	errs := make([]error, len(recs))
	copy(errs, ft.errs)
	return errs, errors.Join(errs...)
}

func (f *fakeSender) sendCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sends
}

// records 返回发往 topic 的全部记录。
func (f *fakeSender) records(topic string) []kafkax.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []kafkax.Record
	for _, r := range f.recs {
		if r.Topic == topic {
			out = append(out, r)
		}
	}
	return out
}

// 测试用与生产相同的分片布局（sharding.OrderLayout）。
const testSlots = sharding.OrderSlots

func newRouter(t testing.TB) *sharding.Router {
	t.Helper()
	r, err := sharding.New(sharding.OrderLayout(2))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// newShardDBs 建 router.DBs() 个临时库，各自跑与生产一样的模板迁移。
func newShardDBs(t testing.TB, router *sharding.Router) []*sql.DB {
	t.Helper()
	dbs := make([]*sql.DB, router.DBs())
	for i := range dbs {
		dbs[i] = testdb.NewTemplate(t, order.Migrations, order.MigrationsDir, order.MigrationData{Slots: router.SlotsOf(i)})
	}
	return dbs
}

type env struct {
	dbs     []*sql.DB // 下标是库号
	router  *sharding.Router
	store   *store.Store
	rdb     *redis.Client
	prefix  string
	gate    *stockgate.Gate
	sender  *fakeSender
	inv     *fakeInventory
	clock   *fakeClock
	relays  []*fakeRelay // 下标是库号
	svc     *Service
	sweeper *Sweeper
	reaper  *Reaper
	// replicas 是 store 的“从库”（见 envOptions），没有从库时为空。
	replicas []*sql.DB
	marks    *readmark.Marks
	// coldDB 是冷库（见 envOptions.cold），没有时为空。
	coldDB *sql.DB

	// consumed[db] 是 consumeEvents 在第 db 个库的 outbox 里已经处理到的行数；
	// simulated 是它代替库存服务发起的调用次数。
	mu        sync.Mutex
	consumed  []int
	simulated map[string]int
}

// kickers 把各库的 fakeRelay 转成 Config.Relays。
func (e *env) kickers() []Kicker {
	out := make([]Kicker, len(e.relays))
	for i, r := range e.relays {
		out[i] = r
	}
	return out
}

// kicks 返回所有库的中继被 Kick 的总次数。
func (e *env) kicks() int64 {
	var n int64
	for _, r := range e.relays {
		n += r.kicks.Load()
	}
	return n
}

// dbOf 返回订单所在的库。
func (e *env) dbOf(orderID int64) *sql.DB { return e.dbs[e.router.DB(e.router.ForOrder(orderID))] }

// table 返回订单所在 slot 上的物理表名。
func (e *env) table(base string, orderID int64) string {
	return sharding.Table(base, e.router.ForOrder(orderID))
}

// replicaKind 是测试环境里“从库”的接法。测试不搭真正的复制，用两种极端代替：
type replicaKind int

const (
	noReplica replicaKind = iota
	// emptyReplica 接到另一组只跑了迁移、没有数据的库：等价于延迟无限大的从库。
	emptyReplica
	// sameReplica 接到与主库同一个库的另一个连接池：等价于已经追上的从库。
	sameReplica
	// brokenReplica 接到连不上的地址：任何查询都失败。
	brokenReplica
)

type envOptions struct {
	replica  replicaKind
	readMode ReadMode
	// marksRDB 是标记用的 Redis 客户端，为空时用闸门那个。给一个连不上的客户端可以模拟 Redis 不可用。
	marksRDB *redis.Client
	// pinTTL、gtidWait 对应 Config 的同名字段，零值用默认值。
	pinTTL   time.Duration
	gtidWait time.Duration
	// cold 为 true 时建一个冷库并接到 Service 上。
	cold bool
	// ids 非空时代替默认的发号器。
	ids IDGenerator
	// readMax、archiveMax 对应 Config 的 ReadMaxConcurrency、ArchiveMaxConcurrency，零值不限制。
	readMax, archiveMax int
	// logHandler 非空时代替默认的日志输出，用来断言日志级别。
	logHandler slog.Handler
}

func newEnv(t *testing.T) *env {
	t.Helper()
	return newEnvWith(t, envOptions{})
}

// reopen 返回连到 db 所在的同一个库的另一个连接池。
func reopen(t testing.TB, db *sql.DB) *sql.DB {
	t.Helper()
	var name string
	if err := db.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&name); err != nil {
		t.Fatal(err)
	}
	cfg, err := mysql.ParseDSN(os.Getenv(testdb.DSNEnv))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = name
	cfg.ParseTime = true
	other, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	return other
}

func newEnvWith(t *testing.T, opts envOptions) *env {
	t.Helper()
	router := newRouter(t)
	dbs := newShardDBs(t, router)
	var replicas []*sql.DB
	switch opts.replica {
	case noReplica:
	case emptyReplica:
		replicas = newShardDBs(t, router)
	case sameReplica:
		for _, db := range dbs {
			replicas = append(replicas, reopen(t, db))
		}
	case brokenReplica:
		for range dbs {
			replicas = append(replicas, brokenDB(t))
		}
	}
	rdb, prefix := testredis.New(t)
	gate := stockgate.New(rdb, prefix)
	inv := newFakeInventory(gate)
	inv.addSKU(skuA, priceA, 1000)
	inv.addSKU(skuB, priceB, 1000)
	inv.addSKU(skuScarce, 10, 1)
	inv.addSKU(skuHuge, 1<<62, 1000)

	ids, err := idgen.New(0)
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{t: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
	logger := slog.New(cmp.Or[slog.Handler](opts.logHandler, slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug})))
	var storeOpts []store.Option
	if replicas != nil {
		storeOpts = append(storeOpts, store.WithReplicas(replicas))
	}
	st, err := store.New(dbs, router, storeOpts...)
	if err != nil {
		t.Fatal(err)
	}
	marks := readmark.New(cmp.Or(opts.marksRDB, rdb), prefix)
	relays := make([]*fakeRelay, router.DBs())
	for i := range relays {
		relays[i] = &fakeRelay{}
	}
	sender := &fakeSender{}
	e := &env{dbs: dbs, router: router, relays: relays, replicas: replicas, marks: marks}
	var cold *store.Cold
	if opts.cold {
		e.coldDB = testdb.NewTemplate(t, order.ArchiveMigrations, order.ArchiveMigrationsDir, order.NewArchiveMigrationData())
		cold = store.NewCold(e.coldDB)
	}
	svc := mustNew(t, Deps{Store: st, Gate: gate, Sender: sender, Inventory: inv, IDs: cmp.Or[IDGenerator](opts.ids, ids), Logger: logger, Marks: marks, Cold: cold}, Config{
		Clock:            clock.Now,
		OrderEventsTopic: testEventsTopic,
		SubmitTopic:      testSubmitTopic,
		CloseDelayTopic:  testCloseTopic,
		Relays:           e.kickers(),
		ReadMode:         opts.readMode,
		ReadPinTTL:       opts.pinTTL,
		ReadGTIDWait:     opts.gtidWait,

		ReadMaxConcurrency:    opts.readMax,
		ArchiveMaxConcurrency: opts.archiveMax,
	})
	sweeper, err := NewSweeper(svc, SweeperConfig{})
	if err != nil {
		t.Fatal(err)
	}
	reaper, err := NewReaper(svc, ReaperConfig{})
	if err != nil {
		t.Fatal(err)
	}
	e.store = st
	e.rdb = rdb
	e.prefix = prefix
	e.gate = gate
	e.sender = sender
	e.inv = inv
	e.clock = clock
	e.svc = svc
	e.sweeper = sweeper
	e.reaper = reaper
	e.consumed = make([]int, router.DBs())
	e.simulated = map[string]int{}
	// 1b 起订单服务只同步调用 Reserve，确认和释放全部交给库存服务消费事件完成。
	// 每个测试结束时检查这一点，任何一条路径偷偷调回库存都会被发现。
	t.Cleanup(func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		for _, m := range []string{"Confirm", "Release"} {
			if n := inv.callCount(m) - e.simulated[m]; n != 0 {
				t.Errorf("order service called inventory.%s %d times, want 0", m, n)
			}
		}
	})
	return e
}

// consumeEvents 扮演库存服务：按顺序消费各库 outbox 里新增的事件，
// paid 确认预占后结清占用，cancelled、closed、failed 释放预占后归还占用（先 MySQL 后 Redis）。
// 用来验证“事件发出去之后库存不会泄漏”。每个库的 outbox 各自有序，同一订单的事件都在一个库里。
func (e *env) consumeEvents(t *testing.T) {
	t.Helper()
	byDB := e.eventsByDB(t)
	e.mu.Lock()
	defer e.mu.Unlock()
	for db, evs := range byDB {
		e.consumeLocked(t, evs[e.consumed[db]:])
		e.consumed[db] = len(evs)
	}
}

func (e *env) consumeLocked(t *testing.T, evs []*eventv1.OrderEvent) {
	t.Helper()
	for _, ev := range evs {
		var err error
		switch ev.GetBody().(type) {
		case *eventv1.OrderEvent_Paid:
			e.simulated["Confirm"]++
			if _, err = e.inv.Confirm(t.Context(), &inventoryv1.ConfirmRequest{OrderId: ev.GetOrderId()}); err == nil {
				_, err = e.gate.Settle(t.Context(), ev.GetOrderId())
			}
		case *eventv1.OrderEvent_Cancelled, *eventv1.OrderEvent_Closed, *eventv1.OrderEvent_Failed:
			e.simulated["Release"]++
			if _, err = e.inv.Release(t.Context(), &inventoryv1.ReleaseRequest{OrderId: ev.GetOrderId()}); err == nil {
				_, err = e.gate.Return(t.Context(), ev.GetOrderId())
			}
		}
		if err != nil {
			t.Errorf("inventory rejected event for order %d: %v", ev.GetOrderId(), err)
		}
	}
}

func items(pairs ...int64) []*orderv1.SubmitOrderItem {
	var out []*orderv1.SubmitOrderItem
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &orderv1.SubmitOrderItem{SkuId: pairs[i], Quantity: int32(pairs[i+1])})
	}
	return out
}

func submitReq(userID int64, key string, it []*orderv1.SubmitOrderItem) *orderv1.SubmitOrderRequest {
	return &orderv1.SubmitOrderRequest{UserId: userID, IdempotencyKey: key, Items: it}
}

// submit 受理一个下单请求，要求成功并返回 CREATING 的订单。
func (e *env) submit(t *testing.T, userID int64, key string, it []*orderv1.SubmitOrderItem) *orderv1.Order {
	t.Helper()
	resp, err := e.svc.SubmitOrder(t.Context(), submitReq(userID, key, it))
	if err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}
	return resp.GetOrder()
}

// submitMessage 返回受理时为 orderID 发出的 order.submit 消息（作为消费者收到的 kafkax.Message）。
func (e *env) submitMessage(t *testing.T, orderID int64) kafkax.Message {
	t.Helper()
	for _, r := range e.sender.records(testSubmitTopic) {
		if string(r.Key) == strconv.FormatInt(orderID, 10) {
			return kafkax.Message{Topic: r.Topic, Key: r.Key, Value: r.Value, Headers: r.Headers}
		}
	}
	t.Fatalf("no order.submit message for order %d", orderID)
	return kafkax.Message{}
}

// land 扮演 order.submit 的消费者处理 orderID 的消息一次，经过 Kafka 适配层（解码、时限、死信判断）。
func (e *env) land(t *testing.T, orderID int64) error {
	t.Helper()
	return e.svc.SubmitHandler(0)(t.Context(), e.submitMessage(t, orderID))
}

func (e *env) mustLand(t *testing.T, orderID int64) {
	t.Helper()
	if err := e.land(t, orderID); err != nil {
		t.Fatalf("land order %d: %v", orderID, err)
	}
}

// mustPending 下一个 skuA×2 的订单、落库，并确认它进入待支付。
func (e *env) mustPending(t *testing.T, userID int64, key string) *orderv1.Order {
	t.Helper()
	o := e.submit(t, userID, key, items(skuA, 2))
	e.mustLand(t, o.GetOrderId())
	stored := e.load(t, o.GetOrderId())
	wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	return toProto(stored)
}

// mustCreating 下一个 skuA×1 的订单并落库，但让 Reserve 超时，订单停在 CREATING。
func (e *env) mustCreating(t *testing.T, userID int64, key string) *store.Order {
	t.Helper()
	o := e.submit(t, userID, key, items(skuA, 1))
	e.inv.inject("Reserve", fault{err: errTimeout})
	if err := e.land(t, o.GetOrderId()); !errors.Is(err, errReserveUnknown) {
		t.Fatalf("land err = %v, want reserve outcome unknown", err)
	}
	stored := e.load(t, o.GetOrderId())
	wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_CREATING)
	return stored
}

func (e *env) load(t *testing.T, orderID int64) *store.Order {
	t.Helper()
	o, err := e.store.GetOrder(t.Context(), orderID)
	if err != nil {
		t.Fatalf("load order %d: %v", orderID, err)
	}
	return o
}

// count 对所有 slot 上的 base 表（orders、order_items、payment_results）执行
// SELECT COUNT(*) ... where 并求和。where 为空表示不加条件。
func (e *env) count(t *testing.T, base, where string, args ...any) int {
	t.Helper()
	total := 0
	for slot := range e.router.Slots() {
		total += e.countSlot(t, base, slot, where, args...)
	}
	return total
}

// countSlot 是 count 在一个 slot 上的部分。
func (e *env) countSlot(t *testing.T, base string, slot int, where string, args ...any) int {
	t.Helper()
	q := "SELECT COUNT(*) FROM " + sharding.Table(base, slot)
	if where != "" {
		q += " WHERE " + where
	}
	var n int
	if err := e.dbs[e.router.DB(slot)].QueryRowContext(t.Context(), q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *env) orderRows(t *testing.T) int {
	t.Helper()
	return e.count(t, "orders", "")
}

// wantGate 断言闸门里 sku 的可放行数量和占用数量。
func (e *env) wantGate(t *testing.T, sku, avail, held int64) {
	t.Helper()
	st, ok, err := e.gate.Stock(t.Context(), sku)
	if err != nil || !ok {
		t.Fatalf("gate stock of sku %d: loaded=%v err=%v", sku, ok, err)
	}
	if st.Avail != avail || st.Held != held {
		t.Fatalf("gate sku %d = avail %d held %d, want avail %d held %d", sku, st.Avail, st.Held, avail, held)
	}
}

// wantBalanced 断言稳定状态下的恒等式（设计文档 6.6）：闸门的 avail + held 等于库存的 available + reserved。
func (e *env) wantBalanced(t *testing.T, sku int64) {
	t.Helper()
	st, ok, err := e.gate.Stock(t.Context(), sku)
	if err != nil || !ok {
		t.Fatalf("gate stock of sku %d: loaded=%v err=%v", sku, ok, err)
	}
	s := e.inv.stock(sku)
	if st.Avail+st.Held != s.available+s.reserved {
		t.Fatalf("sku %d unbalanced: gate avail %d + held %d != inventory available %d + reserved %d",
			sku, st.Avail, st.Held, s.available, s.reserved)
	}
}

// hold 读 orderID 的占用。
func (e *env) hold(t *testing.T, orderID int64) (stockgate.HoldInfo, bool) {
	t.Helper()
	h, ok, err := e.gate.Get(t.Context(), orderID)
	if err != nil {
		t.Fatal(err)
	}
	return h, ok
}

// expireIdempotencyKeys 删掉闸门里的全部幂等键，模拟它们过了保留时长。
func (e *env) expireIdempotencyKeys(t *testing.T) {
	t.Helper()
	iter := e.rdb.Scan(t.Context(), 0, e.prefix+"gate:idem:*", 100).Iterator()
	for iter.Next(t.Context()) {
		if err := e.rdb.Del(t.Context(), iter.Val()).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
}

func paymentEvent(orderID, paymentID, amount int64, succeeded bool) *eventv1.PaymentEvent {
	return &eventv1.PaymentEvent{
		PaymentId: paymentID,
		OrderId:   orderID,
		UserId:    1,
		Body: &eventv1.PaymentEvent_Completed{Completed: &eventv1.PaymentCompleted{
			Succeeded: succeeded,
			Amount:    &commonv1.Money{Amount: amount, Currency: "CNY"},
		}},
	}
}

// pay 投递一条支付结果，返回 outbox 里对这笔支付的答复。答复必须恰好有一条。
func (e *env) pay(t *testing.T, orderID, paymentID, amount int64, succeeded bool) (*eventv1.PaymentResultHandled, error) {
	t.Helper()
	return e.deliver(t, paymentEvent(orderID, paymentID, amount, succeeded))
}

func (e *env) deliver(t *testing.T, ev *eventv1.PaymentEvent) (*eventv1.PaymentResultHandled, error) {
	t.Helper()
	if err := e.svc.HandlePaymentEvent(t.Context(), ev); err != nil {
		return nil, err
	}
	return e.reply(t, ev.GetPaymentId()), nil
}

// reply 返回 outbox 里对 paymentID 的唯一一条答复。
func (e *env) reply(t *testing.T, paymentID int64) *eventv1.PaymentResultHandled {
	t.Helper()
	var found []*eventv1.PaymentResultHandled
	for _, ev := range e.allEvents(t) {
		if h := ev.GetPaymentResultHandled(); h != nil && h.GetPaymentId() == paymentID {
			found = append(found, h)
		}
	}
	if len(found) != 1 {
		t.Fatalf("payment %d has %d replies in outbox, want 1", paymentID, len(found))
	}
	return found[0]
}

// allEvents 返回所有库 outbox 里的全部 OrderEvent，并检查 topic 和 key。
// 每个库内按写入顺序，库与库之间依次排列：只有同一订单的事件之间有先后可言，它们都在一个库里。
func (e *env) allEvents(t *testing.T) []*eventv1.OrderEvent {
	t.Helper()
	var all []*eventv1.OrderEvent
	for _, evs := range e.eventsByDB(t) {
		all = append(all, evs...)
	}
	return all
}

// eventsByDB 按库返回 outbox 里的 OrderEvent，每个库内按写入顺序。
// 同时检查每个事件都写在它的订单所在的库里。
func (e *env) eventsByDB(t *testing.T) [][]*eventv1.OrderEvent {
	t.Helper()
	out := make([][]*eventv1.OrderEvent, len(e.dbs))
	for i := range e.dbs {
		out[i] = e.outboxEvents(t, i)
		for _, ev := range out[i] {
			if db := e.router.DB(e.router.ForOrder(ev.GetOrderId())); db != i {
				t.Errorf("event of order %d is in db %d's outbox, order lives in db %d", ev.GetOrderId(), i, db)
			}
		}
	}
	return out
}

func (e *env) outboxEvents(t *testing.T, db int) []*eventv1.OrderEvent {
	t.Helper()
	rows, err := e.dbs[db].QueryContext(t.Context(), "SELECT topic, msg_key, payload FROM outbox ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var evs []*eventv1.OrderEvent
	for rows.Next() {
		var (
			topic        string
			key, payload []byte
		)
		if err := rows.Scan(&topic, &key, &payload); err != nil {
			t.Fatal(err)
		}
		ev, err := eventx.UnmarshalOrderEvent(payload)
		if err != nil {
			t.Fatal(err)
		}
		if topic != testEventsTopic {
			t.Errorf("outbox topic = %q, want %q", topic, testEventsTopic)
		}
		if want := strconv.FormatInt(ev.GetOrderId(), 10); string(key) != want {
			t.Errorf("outbox key = %q, want %q", key, want)
		}
		evs = append(evs, ev)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return evs
}

// eventKinds 按写入顺序返回某个订单的事件种类。
func (e *env) eventKinds(t *testing.T, orderID int64) []string {
	t.Helper()
	var kinds []string
	for _, ev := range e.allEvents(t) {
		if ev.GetOrderId() != orderID {
			continue
		}
		switch ev.GetBody().(type) {
		case *eventv1.OrderEvent_Paid:
			kinds = append(kinds, "paid")
		case *eventv1.OrderEvent_Cancelled:
			kinds = append(kinds, "cancelled")
		case *eventv1.OrderEvent_Closed:
			kinds = append(kinds, "closed")
		case *eventv1.OrderEvent_Failed:
			kinds = append(kinds, "failed")
		case *eventv1.OrderEvent_PaymentResultHandled:
			kinds = append(kinds, "handled")
		default:
			kinds = append(kinds, "unknown")
		}
	}
	return kinds
}

func (e *env) wantEvents(t *testing.T, orderID int64, want ...string) {
	t.Helper()
	if got := e.eventKinds(t, orderID); !slices.Equal(got, want) {
		t.Fatalf("order %d events = %v, want %v", orderID, got, want)
	}
}

// outboxRows 返回所有库 outbox 的行数之和。
func (e *env) outboxRows(t *testing.T) int {
	t.Helper()
	total := 0
	for _, db := range e.dbs {
		var n int
		if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM outbox").Scan(&n); err != nil {
			t.Fatal(err)
		}
		total += n
	}
	return total
}

func wantErr(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	if errx.Code(err) != code || errx.Reason(err) != reason {
		t.Fatalf("err = %v (code %s, reason %q), want code %s reason %q",
			err, errx.Code(err), errx.Reason(err), code, reason)
	}
}

func wantStatus(t *testing.T, o *store.Order, st orderv1.OrderStatus) {
	t.Helper()
	if o.Status != st {
		t.Fatalf("order %d: status=%s, want %s", o.OrderID, o.Status, st)
	}
}

func wantStock(t *testing.T, inv *fakeInventory, sku, available, reserved, sold int64) {
	t.Helper()
	s := inv.stock(sku)
	if s.available != available || s.reserved != reserved || s.sold != sold {
		t.Fatalf("sku %d stock = %d/%d/%d, want %d/%d/%d (available/reserved/sold)",
			sku, s.available, s.reserved, s.sold, available, reserved, sold)
	}
}

func refOf(o *orderv1.Order) store.OrderRef {
	return store.OrderRef{OrderID: o.GetOrderId(), UserID: o.GetUserId()}
}

// mustNew 是 New 的测试版本：配置有误时终止测试。
func mustNew(t testing.TB, d Deps, cfg Config) *Service {
	t.Helper()
	svc, err := New(d, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// copyToReplica 把主库上 orderID 当前的行（orders 和 order_items）原样复制到“空从库”对应的表里，
// 用来构造“从库上有一份旧数据”：之后在主库上推进状态，从库停在复制时的样子。
// 只用于 emptyReplica：两组库在同一个实例上，可以跨库 INSERT ... SELECT。
func (e *env) copyToReplica(t *testing.T, orderID int64) {
	t.Helper()
	db := e.router.DB(e.router.ForOrder(orderID))
	var primary string
	if err := e.dbs[db].QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&primary); err != nil {
		t.Fatal(err)
	}
	for _, base := range []string{"orders", "order_items"} {
		tb := e.table(base, orderID)
		testdb.MustExec(t, e.replicas[db],
			"INSERT INTO "+tb+" SELECT * FROM `"+primary+"`."+tb+" WHERE order_id = ?", orderID)
	}
}
