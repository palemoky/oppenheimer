package recon

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/inventory"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/orderindex"
	"github.com/palemoky/oppenheimer/services/payment"
)

// 本文件是集成测试的数据：一套和真实环境同构的库（订单库 2 个、可选的从库 2 个、冷库、商家索引库 2 个、
// 支付库、库存库），各用各服务自己的迁移建表，再造一份五对都一致的数据。

const (
	statusPending   = 2
	statusPaid      = 3
	statusCancelled = 4
	statusClosed    = 5
	statusFailed    = 6

	payPending   = 1
	paySucceeded = 2
	payFailed    = 3

	resReserved  = 1
	resConfirmed = 2
	resReleased  = 3
)

type item struct{ sku, qty, price int64 }

type testOrder struct {
	id, user, merchant int64 // merchant 为 0 表示 NULL
	status             int
	total              int64
	version            int64
	paymentID          int64 // 0 表示 NULL
	failReason         string
	items              []item
	cold               bool
	payStatus          int // 0 表示没有支付单
	resStatus          int // 0 表示没有预占
	created            time.Time
	region             string // sparse、dense、late、seeded
}

func (o *testOrder) paid() bool { return o.status == statusPaid }

type fixture struct {
	t          *testing.T
	primaries  []*sql.DB
	replicas   []*sql.DB
	archive    *sql.DB
	index      []*sql.DB
	payment    *sql.DB
	inventory  *sql.DB
	orderR     *sharding.Router
	indexR     *sharding.Router
	orders     []*testOrder
	topo       Topology
	nextSerial int
}

var (
	// 稀的区域：两年里每 11 天左右一单。
	sparseStart = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	// 密的区域：一秒内 120 单，每 7 毫秒一单。
	denseStart = time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	// 水位线之后的订单。
	lateStart = time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC)
	// testBound 在 dense 之后、late 之前。
	testBound = idgen.LowerBound(time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC))
)

func newFixture(t *testing.T, withReplicas bool) *fixture {
	t.Helper()
	or, err := sharding.New(sharding.OrderLayout(2))
	if err != nil {
		t.Fatal(err)
	}
	ir, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, orderR: or, indexR: ir}
	for i := range 2 {
		f.primaries = append(f.primaries, testdb.NewTemplate(t, order.Migrations, order.MigrationsDir, order.MigrationData{Slots: or.SlotsOf(i)}))
		if withReplicas {
			f.replicas = append(f.replicas, testdb.NewTemplate(t, order.Migrations, order.MigrationsDir, order.MigrationData{Slots: or.SlotsOf(i)}))
		}
		f.index = append(f.index, testdb.NewTemplate(t, orderindex.Migrations, orderindex.MigrationsDir, orderindex.MigrationData{Slots: ir.SlotsOf(i)}))
	}
	f.archive = testdb.NewTemplate(t, order.ArchiveMigrations, order.ArchiveMigrationsDir, order.NewArchiveMigrationData())
	f.payment = testdb.New(t, payment.Migrations, payment.MigrationsDir)
	f.inventory = testdb.New(t, inventory.Migrations, inventory.MigrationsDir)

	wrap := func(name string, i int, db *sql.DB) *DB { return &DB{Name: fmt.Sprintf("%s_%d", name, i), Conn: db} }
	for i, db := range f.primaries {
		f.topo.OrderPrimaries = append(f.topo.OrderPrimaries, wrap("order", i, db))
	}
	for i, db := range f.replicas {
		f.topo.OrderReplicas = append(f.topo.OrderReplicas, wrap("order_replica", i, db))
	}
	for i, db := range f.index {
		f.topo.MerchantIndex = append(f.topo.MerchantIndex, wrap("merchant_index", i, db))
	}
	f.topo.Archive = &DB{Name: "archive", Conn: f.archive}
	f.topo.Payment = &DB{Name: "payment", Conn: f.payment}
	f.topo.Inventory = &DB{Name: "inventory", Conn: f.inventory}

	f.generate()
	f.reset()
	return f
}

// newOrderID 按 idgen 的布局造订单号：时间、机器号、序号、用户的分片基因。
func (f *fixture) newOrderID(at time.Time, user int64) int64 {
	n := int64(f.nextSerial)
	f.nextSerial++
	return idgen.LowerBound(at) | (n>>10&63)<<16 | (n&1023)<<6 | idgen.GeneOf(user)
}

func (f *fixture) addOrder(i int, at time.Time, region string, user int64) *testOrder {
	o := &testOrder{user: user, created: at, region: region, version: 2 + int64(i%4)}
	o.id = f.newOrderID(at, user)
	switch i % 10 {
	case 0, 1, 2, 3, 4, 8:
		o.status, o.payStatus, o.resStatus = statusPaid, paySucceeded, resConfirmed
	case 5:
		o.status, o.payStatus, o.resStatus = statusPending, payPending, resReserved
	case 6:
		o.status, o.resStatus = statusCancelled, resReleased
	case 7:
		o.status, o.payStatus, o.resStatus = statusClosed, payFailed, resReleased
	case 9:
		o.status, o.failReason = statusFailed, "NO_STOCK"
	}
	if o.status != statusFailed {
		o.merchant = 1 + int64(i%6)
		for j := range 1 + i%3 {
			it := item{sku: 100 + int64(j)*10 + int64(i%7), qty: 1 + int64((i+j)%3), price: 500 + 37*int64(j) + int64(i)}
			o.items = append(o.items, it)
			o.total += it.qty * it.price
		}
	}
	if o.paid() {
		o.paymentID = o.id + 1
	}
	f.orders = append(f.orders, o)
	return o
}

func (f *fixture) generate() {
	for i := range 60 {
		o := f.addOrder(i, sparseStart.Add(time.Duration(i)*(11*24*time.Hour+3*time.Hour)), "sparse", 1000+int64(i))
		// 稀的区域里每三单有一单（终态的）已经归档。
		o.cold = i%3 == 0 && o.status != statusPending
	}
	for i := range 120 {
		f.addOrder(i, denseStart.Add(time.Duration(i)*7*time.Millisecond), "dense", 2000+int64(i))
	}
	for i := range 6 {
		f.addOrder(i, lateStart.Add(time.Duration(i)*time.Hour), "late", 3000+int64(i))
	}
	// 造数工具造的订单：只在订单库和商家索引里，payment、stock 两对排除它们。
	for i := range 2 {
		o := &testOrder{
			user: seededUserBase + int64(i), merchant: 1, status: statusPaid, total: 999, version: 1,
			created: sparseStart.Add(400 * 24 * time.Hour), region: "seeded",
		}
		o.id = f.newOrderID(o.created, o.user)
		o.paymentID = o.id + 1
		o.items = []item{{sku: 100, qty: 1, price: 999}}
		f.orders = append(f.orders, o)
	}
}

// pick 返回第 n 个（从 0 开始）满足条件的订单。
func (f *fixture) pick(n int, pred func(*testOrder) bool) *testOrder {
	f.t.Helper()
	for _, o := range f.orders {
		if pred(o) {
			if n == 0 {
				return o
			}
			n--
		}
	}
	f.t.Fatal("no such order")
	return nil
}

func where(region string, cold bool, status int) func(*testOrder) bool {
	return func(o *testOrder) bool { return o.region == region && o.cold == cold && o.status == status }
}

// inserter 把要插入的行按表攒起来，一张表一条多行 INSERT。
type inserter struct {
	keys []insKey
	rows map[insKey][][]any
}

type insKey struct {
	db          *sql.DB
	table, cols string
}

func (b *inserter) add(db *sql.DB, table, cols string, vals ...any) {
	if b.rows == nil {
		b.rows = map[insKey][][]any{}
	}
	k := insKey{db: db, table: table, cols: cols}
	if _, ok := b.rows[k]; !ok {
		b.keys = append(b.keys, k)
	}
	b.rows[k] = append(b.rows[k], vals)
}

func (b *inserter) flush(t *testing.T) {
	t.Helper()
	for _, k := range b.keys {
		rows := b.rows[k]
		tuple := "(" + placeholders(len(rows[0])) + ")"
		var args []any
		tuples := make([]string, len(rows))
		for i, r := range rows {
			tuples[i] = tuple
			args = append(args, r...)
		}
		testdb.MustExec(t, k.db, "INSERT INTO "+k.table+" ("+k.cols+") VALUES "+strings.Join(tuples, ", "), args...)
	}
}

func orNull(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullStr(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func requestHash(id int64) []byte {
	h := sha256.Sum256(binary.BigEndian.AppendUint64(nil, uint64(id))) //nolint:gosec // 测试里的订单号不为负
	return h[:]
}

func (f *fixture) slotOf(o *testOrder) int { return f.orderR.ForUser(o.user) }

func (f *fixture) primaryOf(o *testOrder) *sql.DB { return f.primaries[f.orderR.DB(f.slotOf(o))] }

func (f *fixture) indexSlot(merchant int64) int { return f.indexR.ForMerchant(merchant) }

func (f *fixture) indexDB(merchant int64) (*sql.DB, string) {
	s := f.indexSlot(merchant)
	return f.index[f.indexR.DB(s)], sharding.Table("merchant_orders", s)
}

const (
	hotOrderCols  = "order_id, user_id, merchant_id, status, total_amount, currency, idempotency_key, request_hash, payment_id, fail_reason, expire_at, version, created_at"
	coldOrderCols = "order_id, user_id, merchant_id, version, status, total_amount, currency, idempotency_key, request_hash, payment_id, fail_reason, expire_at, paid_at, created_at, updated_at"
	indexCols     = "merchant_id, order_id, user_id, status, total_amount, currency, created_at, paid_at, src_version, synced_at"
)

// writeHot 把订单写进一个热库（主库或从库）的 slot 表里。
func (f *fixture) writeHot(b *inserter, db *sql.DB, slot int, o *testOrder) {
	b.add(db, sharding.Table("orders", slot), hotOrderCols,
		o.id, o.user, orNull(o.merchant), o.status, o.total, "CNY", fmt.Sprintf("k-%d", o.id), requestHash(o.id),
		orNull(o.paymentID), nullStr(o.failReason), o.created.Add(15*time.Minute), o.version, o.created)
	for _, it := range o.items {
		b.add(db, sharding.Table("order_items", slot), "order_id, sku_id, quantity, unit_price", o.id, it.sku, it.qty, it.price)
	}
	if o.paid() {
		b.add(db, sharding.Table("payment_results", slot), "payment_id, order_id, succeeded, amount, disposition",
			o.paymentID, o.id, 1, o.total, 1)
	}
}

func (f *fixture) writeCold(b *inserter, o *testOrder) {
	b.add(f.archive, "orders_archive", coldOrderCols,
		o.id, o.user, orNull(o.merchant), o.version, o.status, o.total, "CNY", fmt.Sprintf("k-%d", o.id), requestHash(o.id),
		orNull(o.paymentID), nullStr(o.failReason), o.created.Add(15*time.Minute), nil, o.created, o.created)
	for _, it := range o.items {
		b.add(f.archive, "order_items_archive", "order_id, sku_id, quantity, unit_price, created_at", o.id, it.sku, it.qty, it.price, o.created)
	}
	if o.paid() {
		b.add(f.archive, "payment_results_archive", "order_id, payment_id, succeeded, amount, disposition, created_at",
			o.id, o.paymentID, 1, o.total, 1, o.created)
	}
}

func (f *fixture) writeIndex(b *inserter, o *testOrder) {
	if o.merchant == 0 {
		return
	}
	db, table := f.indexDB(o.merchant)
	b.add(db, table, indexCols, o.merchant, o.id, o.user, o.status, o.total, "CNY", o.created, nil, o.version, o.created)
}

// reset 清空全部表，重新写入一致的数据。
func (f *fixture) reset() {
	t := f.t
	t.Helper()
	truncate := func(db *sql.DB, tables ...string) {
		for _, tb := range tables {
			testdb.MustExec(t, db, "DELETE FROM "+tb) // 表都很小，DELETE 比 TRUNCATE（重建表）快得多
		}
	}
	for i := range 2 {
		for _, s := range f.orderR.SlotsOf(i) {
			tables := []string{sharding.Table("orders", s), sharding.Table("order_items", s), sharding.Table("payment_results", s)}
			truncate(f.primaries[i], tables...)
			if f.replicas != nil {
				truncate(f.replicas[i], tables...)
			}
		}
		for _, s := range f.indexR.SlotsOf(i) {
			truncate(f.index[i], sharding.Table("merchant_orders", s))
		}
	}
	truncate(f.archive, "orders_archive", "order_items_archive", "payment_results_archive")
	truncate(f.payment, "payments")
	truncate(f.inventory, "reservation", "reservation_item")

	var b inserter
	for _, o := range f.orders {
		if o.cold {
			f.writeCold(&b, o)
		} else {
			slot := f.slotOf(o)
			f.writeHot(&b, f.primaries[f.orderR.DB(slot)], slot, o)
			if f.replicas != nil {
				f.writeHot(&b, f.replicas[f.orderR.DB(slot)], slot, o)
			}
		}
		f.writeIndex(&b, o)
		if o.region == "seeded" {
			continue
		}
		if o.payStatus != 0 {
			amount := o.total
			b.add(f.payment, "payments", "payment_id, order_id, user_id, status, amount, currency, outcome",
				o.id+1, o.id, o.user, o.payStatus, amount, "CNY", 1)
		}
		if o.resStatus != 0 {
			b.add(f.inventory, "reservation", "order_id, status, merchant_id", o.id, o.resStatus, o.merchant)
			for _, it := range o.items {
				b.add(f.inventory, "reservation_item", "order_id, sku_id, quantity, unit_price, currency", o.id, it.sku, it.qty, it.price, "CNY")
			}
		}
	}
	b.flush(t)
}

func (f *fixture) exec(db *sql.DB, q string, args ...any) {
	f.t.Helper()
	testdb.MustExec(f.t, db, q, args...)
}

// pair 构造一个对账对象。
func (f *fixture) pair(name string) Pair {
	f.t.Helper()
	p, err := Build(name, f.topo)
	if err != nil {
		f.t.Fatal(err)
	}
	return p
}
