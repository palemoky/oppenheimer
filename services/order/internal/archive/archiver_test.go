package archive_test

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/order/internal/archive"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// now 是测试里归档任务的时钟。截止点是 now - 90 天。
var (
	now       = time.Date(2026, 11, 14, 3, 0, 0, 0, time.UTC)
	olderThan = archive.DefaultArchiveAge
	cutoff    = now.Add(-olderThan)
)

var (
	terminal = []orderv1.OrderStatus{
		orderv1.OrderStatus_ORDER_STATUS_PAID, orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
		orderv1.OrderStatus_ORDER_STATUS_CLOSED, orderv1.OrderStatus_ORDER_STATUS_FAILED,
	}
	nonTerminal = []orderv1.OrderStatus{
		orderv1.OrderStatus_ORDER_STATUS_CREATING, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT,
	}
)

type fixture struct {
	hot    []*sql.DB
	router *sharding.Router
	st     *store.Store
	coldDB *sql.DB
	cold   *store.Cold

	seq     atomic.Int64 // 订单号里的序号，保证测试里的订单号不重复
	payment atomic.Int64

	mu       sync.Mutex
	payments map[int64]int64 // 测试中成功提交的支付结果：payment_id -> order_id
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	router, err := sharding.New(sharding.OrderLayout(2))
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{router: router, payments: map[int64]int64{}}
	f.hot = make([]*sql.DB, router.DBs())
	for i := range f.hot {
		f.hot[i] = testdb.NewTemplate(t, order.Migrations, order.MigrationsDir, order.MigrationData{Slots: router.SlotsOf(i)})
	}
	if f.st, err = store.New(f.hot, router); err != nil {
		t.Fatal(err)
	}
	f.coldDB = testdb.NewTemplate(t, order.ArchiveMigrations, order.ArchiveMigrationsDir, order.NewArchiveMigrationData())
	f.cold = store.NewCold(f.coldDB)
	f.payment.Store(1_000_000)
	return f
}

func (f *fixture) archiver(t *testing.T, cfg archive.Config) *archive.Archiver {
	t.Helper()
	cfg.OlderThan = olderThan
	cfg.Now = func() time.Time { return now }
	cfg.Logger = slog.New(slog.NewTextHandler(t.Output(), nil))
	a, err := archive.New(f.st, f.cold, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// addOrder 插入一个受理时间为 at 的订单并改成 status。variant 决定可空列是否为 NULL、明细行数、
// 支付结果行数，让各种组合都出现。返回订单号。
func (f *fixture) addOrder(t *testing.T, userID int64, at time.Time, status orderv1.OrderStatus, variant int) int64 {
	t.Helper()
	id := idgen.LowerBound(at) | f.seq.Add(1)<<6 | idgen.GeneOf(userID)
	at = at.Add(time.Duration(variant%997) * time.Microsecond) // 小数秒
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = byte(variant + i)
	}
	if variant%2 == 0 {
		hash[31] = 0
	}
	items := make([]store.Item, 1+variant%3)
	for i := range items {
		items[i] = store.Item{SKUID: int64(10 - i), Quantity: int32(i + 1)}
	}
	o := &store.Order{
		OrderID: id, UserID: userID, Status: orderv1.OrderStatus_ORDER_STATUS_CREATING,
		Currency: "CNY", IdempotencyKey: fmt.Sprintf("key-%d", id), RequestHash: hash,
		ExpireAt: at.Add(15 * time.Minute), CreatedAt: at, UpdatedAt: at, Items: items,
	}
	if err := f.st.InsertOrder(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	slot := f.router.ForOrder(id)
	db := f.hot[f.router.DB(slot)]

	var merchant, paymentID, reason, paidAt any // nil 表示 NULL
	if variant%4 != 0 {
		merchant = int64(variant % 3) // 包括非 NULL 的 0
	}
	switch status {
	case orderv1.OrderStatus_ORDER_STATUS_PAID:
		paymentID, paidAt = f.payment.Add(1), at.Add(2*time.Minute+333*time.Microsecond)
	case orderv1.OrderStatus_ORDER_STATUS_FAILED:
		reason = "OUT_OF_STOCK"
		if variant%2 == 0 {
			reason = "" // 非 NULL 的空串
		}
	default:
	}
	testdb.MustExec(t, db, "UPDATE "+sharding.Table("orders", slot)+
		` SET status = ?, merchant_id = ?, payment_id = ?, fail_reason = ?, paid_at = ?, total_amount = ?,
		version = ?, updated_at = ? WHERE order_id = ?`,
		int32(status), merchant, paymentID, reason, paidAt, 100*(variant+1), variant%5+1, at.Add(5*time.Minute), id)
	testdb.MustExec(t, db, "UPDATE "+sharding.Table("order_items", slot)+" SET unit_price = 100 + sku_id WHERE order_id = ?", id)

	insertPay := func(pid int64, succeeded bool, d eventv1.PaymentDisposition) {
		testdb.MustExec(t, db, "INSERT INTO "+sharding.Table("payment_results", slot)+
			" (payment_id, order_id, succeeded, amount, disposition, created_at) VALUES (?, ?, ?, ?, ?, ?)",
			pid, id, succeeded, 100*(variant+1), int32(d), at.Add(3*time.Minute+7*time.Microsecond))
	}
	if pid, ok := paymentID.(int64); ok {
		insertPay(pid, true, eventv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED)
	}
	if status == orderv1.OrderStatus_ORDER_STATUS_CANCELLED && variant%2 == 1 {
		// 取消之后才到的支付成功，要求退款。
		insertPay(f.payment.Add(1), true, eventv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED)
	}
	if variant%5 == 0 {
		insertPay(f.payment.Add(1), false, eventv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED)
	}
	return id
}

// seed 造一组订单，返回应当被归档的和应当留下的订单号。
//   - 早于截止点的终态订单：归档；
//   - 早于截止点的非终态订单、晚于截止点的终态订单（包括恰好在截止点上的）：留下。
func (f *fixture) seed(t *testing.T, users, perUser int) (archivable, kept []int64) {
	t.Helper()
	old := cutoff.Add(-100 * 24 * time.Hour)
	variant := 0
	for u := range users {
		user := int64(u + 1)
		for range perUser {
			variant++
			archivable = append(archivable, f.addOrder(t, user, old.Add(time.Duration(variant)*time.Hour), terminal[variant%len(terminal)], variant))
		}
		variant++
		archivable = append(archivable, f.addOrder(t, user, cutoff.Add(-time.Millisecond), terminal[variant%len(terminal)], variant))
		kept = append(kept,
			f.addOrder(t, user, old, nonTerminal[variant%len(nonTerminal)], variant),
			f.addOrder(t, user, cutoff, terminal[variant%len(terminal)], variant),
			f.addOrder(t, user, now.Add(-40*24*time.Hour), terminal[(variant+1)%len(terminal)], variant+1),
		)
	}
	return archivable, kept
}

// table 是一张表在冷热两边的列（不含冷库的 archived_at）和 order_id 所在的列。
type table struct {
	base, cold string
	cols       string
	orderCol   int
}

var tables = []table{
	{"orders", store.ColdOrders, "order_id, user_id, merchant_id, version, status, total_amount, currency, idempotency_key, " +
		"request_hash, payment_id, fail_reason, expire_at, paid_at, created_at, updated_at", 0},
	{"order_items", store.ColdItems, "order_id, sku_id, quantity, unit_price, created_at", 0},
	{"payment_results", store.ColdPayments, "payment_id, order_id, succeeded, amount, disposition, created_at", 1},
}

// snapshot 是三张表的全部行，按表名、订单号分组，每行是全部列的文本（区分 NULL）。
type snapshot map[string]map[int64][]string

func dumpRows(t *testing.T, db *sql.DB, query string, orderCol int, into map[int64][]string) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	vals := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range vals {
		dest[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = "NULL"
			if v.Valid {
				parts[i] = fmt.Sprintf("%q", v.String)
			}
		}
		var id int64
		if _, err := fmt.Sscan(vals[orderCol].String, &id); err != nil {
			t.Fatal(err)
		}
		into[id] = append(into[id], strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) hotSnapshot(t *testing.T) snapshot {
	t.Helper()
	s := snapshot{}
	for _, tb := range tables {
		s[tb.base] = map[int64][]string{}
		for slot := range f.router.Slots() {
			dumpRows(t, f.hot[f.router.DB(slot)], "SELECT "+tb.cols+" FROM "+sharding.Table(tb.base, slot)+" ORDER BY "+tb.cols,
				tb.orderCol, s[tb.base])
		}
	}
	return s
}

func (f *fixture) coldSnapshot(t *testing.T) snapshot {
	t.Helper()
	s := snapshot{}
	for _, tb := range tables {
		s[tb.base] = map[int64][]string{}
		dumpRows(t, f.coldDB, "SELECT "+tb.cols+" FROM "+tb.cold+" ORDER BY "+tb.cols, tb.orderCol, s[tb.base])
	}
	return s
}

func (s snapshot) rows() int {
	n := 0
	for _, byOrder := range s {
		for _, rs := range byOrder {
			n += len(rs)
		}
	}
	return n
}

// checkMoved 检查从 before 到 (hot, cold) 的搬运：每个订单要么完整地留在热库、内容不变，要么完整地
// 离开热库、冷库里是它原样的三张表。冷库里还有留在热库的订单时（中断在写冷和删热之间），内容也必须相同。
// 两张快照的行数守恒。返回离开热库的订单。
func checkMoved(t *testing.T, before, hot, cold snapshot) map[int64]bool {
	t.Helper()
	moved := map[int64]bool{}
	for id := range before["orders"] {
		inHot := len(hot["orders"][id]) > 0
		inCold := len(cold["orders"][id]) > 0
		if !inHot && !inCold {
			t.Errorf("order %d is in neither hot nor cold", id)
			continue
		}
		for _, tb := range tables {
			want := before[tb.base][id]
			if inHot && !slices.Equal(hot[tb.base][id], want) {
				t.Errorf("order %d: hot %s changed:\n got %v\nwant %v", id, tb.base, hot[tb.base][id], want)
			}
			if inCold && !slices.Equal(cold[tb.base][id], want) {
				t.Errorf("order %d: cold %s differs from hot before archiving:\n got %v\nwant %v", id, tb.base, cold[tb.base][id], want)
			}
		}
		if !inHot {
			moved[id] = true
		}
	}
	for _, tb := range tables {
		for id := range cold[tb.base] {
			if _, ok := before["orders"][id]; !ok {
				t.Errorf("cold %s has rows of unknown order %d", tb.base, id)
			}
		}
	}
	// 行数守恒：归档前热库的行数 = 归档后热库的行数 + 冷库里已经离开热库的订单的行数。
	for _, tb := range tables {
		var b, h, c int
		for _, rs := range before[tb.base] {
			b += len(rs)
		}
		for _, rs := range hot[tb.base] {
			h += len(rs)
		}
		for id, rs := range cold[tb.base] {
			if moved[id] {
				c += len(rs)
			}
		}
		if b != h+c {
			t.Errorf("%s: %d rows before, %d hot + %d cold after", tb.base, b, h, c)
		}
	}
	return moved
}

func sortedKeys(m map[int64]bool) []int64 { return slices.Sorted(maps.Keys(m)) }

func checkArchivedExactly(t *testing.T, moved map[int64]bool, archivable []int64) {
	t.Helper()
	if got, want := sortedKeys(moved), slices.Sorted(slices.Values(archivable)); !slices.Equal(got, want) {
		t.Errorf("archived %d orders %v,\nwant %d %v", len(got), got, len(want), want)
	}
}

// 冷库里不能有留在热库的订单（完整地跑完之后，没有被跳过的订单时）。
func checkDisjoint(t *testing.T, hot, cold snapshot) {
	t.Helper()
	for id := range cold["orders"] {
		if len(hot["orders"][id]) > 0 {
			t.Errorf("order %d is in both hot and cold", id)
		}
	}
}

func TestNewRejectsShortCutoff(t *testing.T) {
	t.Parallel()
	st, err := store.New([]*sql.DB{{}}, mustRouter(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	cold := store.NewCold(&sql.DB{})
	for _, d := range []time.Duration{0, archive.MinHotAge, archive.MinArchiveAge - time.Second} {
		if _, err := archive.New(st, cold, archive.Config{OlderThan: d}); err == nil {
			t.Errorf("New(OlderThan=%s) succeeded, want error", d)
		}
	}
	if _, err := archive.New(st, cold, archive.Config{OlderThan: archive.MinArchiveAge}); err != nil {
		t.Errorf("New(OlderThan=MinArchiveAge) = %v", err)
	}
	if _, err := archive.New(st, cold, archive.Config{OlderThan: olderThan, BatchSize: store.MaxArchiveBatch + 1}); err == nil {
		t.Error("New with an oversized batch succeeded")
	}
}

func mustRouter(t *testing.T, dbs int) *sharding.Router {
	t.Helper()
	r, err := sharding.New(sharding.OrderLayout(dbs))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// 归档一遍：可归档的全部搬到冷库、内容原样，其余的原样留在热库。
func TestRunArchivesOldTerminalOrders(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	archivable, kept := f.seed(t, 16, 4)
	before := f.hotSnapshot(t)

	rep, err := f.archiver(t, archive.Config{BatchSize: 7}).Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	hot, cold := f.hotSnapshot(t), f.coldSnapshot(t)
	moved := checkMoved(t, before, hot, cold)
	checkArchivedExactly(t, moved, archivable)
	checkDisjoint(t, hot, cold)
	for _, id := range kept {
		if moved[id] {
			t.Errorf("order %d should have stayed in hot", id)
		}
	}

	if rep.Total.Archived != int64(len(archivable)) || rep.Total.Scanned != int64(len(archivable)) || rep.Total.Skipped != 0 {
		t.Errorf("report total = %+v, want %d archived", rep.Total, len(archivable))
	}
	// 每个用户一个早于截止点的非终态订单。
	if rep.Total.NonTerminal != 16 {
		t.Errorf("non-terminal = %d, want 16", rep.Total.NonTerminal)
	}
	if rep.Below != idgen.LowerBound(cutoff) || len(rep.Slots) != f.router.Slots() {
		t.Errorf("report below = %d, slots = %d", rep.Below, len(rep.Slots))
	}
	for _, s := range rep.Slots {
		if s.Counts.Batches == 0 || s.DB != f.router.DB(s.Slot) {
			t.Errorf("slot report %+v", s)
		}
	}
}

// 在“写冷之后、删热之前”中断，再完整跑一遍，结果与一次跑完相同；再跑一遍是空操作。
func TestRunIsReentrant(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	archivable, _ := f.seed(t, 16, 3)
	before := f.hotSnapshot(t)

	crash := errors.New("simulated crash")
	a := f.archiver(t, archive.Config{BatchSize: 5})
	var calls atomic.Int64
	archive.SetAfterColdWrite(a, func(int, []int64) error {
		// 让每个库先完整做一批，然后在第三批写冷之后“崩溃”。
		if calls.Add(1) >= 3 {
			return crash
		}
		return nil
	})
	if _, err := a.Run(t.Context()); !errors.Is(err, crash) {
		t.Fatalf("Run = %v, want the simulated crash", err)
	}
	hot, cold := f.hotSnapshot(t), f.coldSnapshot(t)
	checkMoved(t, before, hot, cold)
	var both int
	for id := range cold["orders"] {
		if len(hot["orders"][id]) > 0 {
			both++
		}
	}
	if both == 0 {
		t.Fatal("the crash left no order in both hot and cold; the test did not interrupt between the steps")
	}

	if _, err := f.archiver(t, archive.Config{BatchSize: 5}).Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	hot, cold = f.hotSnapshot(t), f.coldSnapshot(t)
	checkArchivedExactly(t, checkMoved(t, before, hot, cold), archivable)
	checkDisjoint(t, hot, cold)

	rep, err := f.archiver(t, archive.Config{BatchSize: 5}).Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Total.Scanned != 0 || rep.Total.Archived != 0 || rep.Total.Batches != 0 {
		t.Errorf("third run did work: %+v", rep.Total)
	}
	hot2, cold2 := f.hotSnapshot(t), f.coldSnapshot(t)
	if hot2.rows() != hot.rows() || cold2.rows() != cold.rows() {
		t.Errorf("third run changed rows: hot %d -> %d, cold %d -> %d", hot.rows(), hot2.rows(), cold.rows(), cold2.rows())
	}
}

// 归档进行的同时，对同一批订单投递“取消之后才到的支付结果”。每一条成功提交的处理记录都必须
// 留在它的订单所在的一边：订单还在热库，记录就在热库；订单已经离开热库，记录就在冷库。
//
// 钩子在每批写冷之后、删热之前给这一批的第一个订单插一条记录，确定性地制造“写冷之后子表变了”；
// 后台的 goroutine 随机地插，覆盖其他时机。删除时只比订单行（不在锁里重读子表）的实现会把钩子插的
// 记录和订单一起删掉，而冷库里没有它，这个测试会失败。
func TestRunConcurrentPaymentResults(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	old := cutoff.Add(-30 * 24 * time.Hour)
	var orders []int64
	for u := range 32 {
		for i := range 6 {
			orders = append(orders, f.addOrder(t, int64(u+1), old.Add(time.Duration(i)*time.Minute),
				orderv1.OrderStatus_ORDER_STATUS_CANCELLED, u*6+i))
		}
	}
	ctx := t.Context()

	a := f.archiver(t, archive.Config{BatchSize: 6})
	var injected atomic.Int64
	archive.SetAfterColdWrite(a, func(_ int, ids []int64) error {
		ok, err := f.pay(ctx, ids[0])
		if ok {
			injected.Add(1)
		}
		return err
	})

	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				if _, err := f.pay(ctx, orders[rand.IntN(len(orders))]); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	rep, err := a.Run(ctx)
	close(done)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if injected.Load() == 0 {
		t.Fatal("the hook injected no payment result")
	}
	// 钩子插的每一条都让那个订单在删除时摘要不符而留下。
	if rep.Total.Skipped < injected.Load() {
		t.Errorf("skipped %d orders, want at least the %d with injected payment results", rep.Total.Skipped, injected.Load())
	}

	hot, cold := f.hotSnapshot(t), f.coldSnapshot(t)
	inHot := func(id int64) bool { return len(hot["orders"][id]) > 0 }
	for _, id := range orders {
		if !inHot(id) && len(cold["orders"][id]) == 0 {
			t.Errorf("order %d is in neither hot nor cold", id)
		}
	}
	hasPayment := func(s snapshot, orderID, paymentID int64) bool {
		prefix := strconv.Quote(strconv.FormatInt(paymentID, 10)) + "|"
		return slices.ContainsFunc(s["payment_results"][orderID], func(r string) bool { return strings.HasPrefix(r, prefix) })
	}
	// 热库里不能有订单已经离开热库的记录（孤儿）。
	for id := range hot["payment_results"] {
		if !inHot(id) {
			t.Errorf("hot has payment results of order %d, which left hot", id)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var lost int
	for pid, oid := range f.payments {
		side, s := "hot", hot
		if !inHot(oid) {
			side, s = "cold", cold
		}
		if !hasPayment(s, oid, pid) {
			lost++
			t.Errorf("payment result %d of order %d is missing from %s, where the order is", pid, oid, side)
		}
	}
	t.Logf("%d payment results committed, %d injected, %d archived, %d skipped, %d lost",
		len(f.payments), injected.Load(), rep.Total.Archived, rep.Total.Skipped, lost)
}

// pay 模拟处理一笔取消之后才到的支付成功：锁订单行，订单还在就插一条要求退款的记录。
// 返回是否提交了记录。
func (f *fixture) pay(ctx context.Context, orderID int64) (bool, error) {
	pid := f.payment.Add(1)
	var committed bool
	err := f.st.InTx(ctx, orderID, func(tx *store.Tx) error {
		committed = false
		if _, err := tx.LockOrder(ctx, orderID); errors.Is(err, store.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		if err := tx.InsertPaymentResult(ctx, &store.PaymentResult{
			PaymentID: pid, OrderID: orderID, Succeeded: true, Amount: 100,
			Disposition: eventv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED,
		}, now); err != nil {
			return err
		}
		committed = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("pay order %d: %w", orderID, err)
	}
	if committed {
		f.mu.Lock()
		f.payments[pid] = orderID
		f.mu.Unlock()
	}
	return committed, nil
}

// DryRun 只数不改。
func TestRunDryRun(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	archivable, _ := f.seed(t, 8, 3)
	before := f.hotSnapshot(t)

	rep, err := f.archiver(t, archive.Config{DryRun: true}).Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !rep.DryRun || rep.Total.Archivable != int64(len(archivable)) || rep.Total.NonTerminal != 8 || rep.Total.Archived != 0 {
		t.Errorf("dry run report = %+v, want %d archivable and 8 non-terminal", rep.Total, len(archivable))
	}
	if _, err := json.Marshal(rep); err != nil {
		t.Errorf("report does not marshal to JSON: %v", err)
	}
	hot, cold := f.hotSnapshot(t), f.coldSnapshot(t)
	if cold.rows() != 0 {
		t.Errorf("dry run wrote %d rows to cold", cold.rows())
	}
	for _, tb := range tables {
		if !maps.EqualFunc(before[tb.base], hot[tb.base], slices.Equal) {
			t.Errorf("dry run changed hot %s", tb.base)
		}
	}
}

// ctx 在一批的中间取消：这一批做完再返回，返回 ctx 的错误和已完成部分的报告；没有半批。
func TestRunStopsAfterCurrentBatchOnCancel(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	archivable, _ := f.seed(t, 16, 4)
	before := f.hotSnapshot(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	a := f.archiver(t, archive.Config{BatchSize: 3})
	archive.SetAfterColdWrite(a, func(int, []int64) error {
		cancel() // 正在一批的中间：已经写了冷，还没校验和删热
		return nil
	})
	rep, err := a.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	hot, cold := f.hotSnapshot(t), f.coldSnapshot(t)
	moved := checkMoved(t, before, hot, cold)
	// 被打断的那一批也删完了：冷库里没有还留在热库的订单。
	checkDisjoint(t, hot, cold)
	if len(moved) == 0 || len(moved) >= len(archivable) {
		t.Errorf("moved %d of %d orders, want some but not all", len(moved), len(archivable))
	}
	if rep == nil || rep.Total.Archived != int64(len(moved)) {
		t.Errorf("report archived %+v, want %d", rep, len(moved))
	}
}

// 空间回收能跑通，每张热表都有结果。
func TestReclaim(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.seed(t, 8, 2)
	a := f.archiver(t, archive.Config{})
	if _, err := a.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	rep, err := a.Reclaim(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := 3 * f.router.Slots(); len(rep.Tables) != want {
		t.Fatalf("reclaimed %d tables, want %d", len(rep.Tables), want)
	}
	for _, r := range rep.Tables {
		if r.BeforeBytes <= 0 || r.AfterBytes <= 0 {
			t.Errorf("table %s: sizes %d -> %d", r.Table, r.BeforeBytes, r.AfterBytes)
		}
	}
}
