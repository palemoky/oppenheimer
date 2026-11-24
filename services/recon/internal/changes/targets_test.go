package changes

import (
	"bytes"
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/orderindex"
)

// 这些测试对着各服务自己的迁移建出来的真实表：从库（订单库的表结构，2 个库）、商家索引（2 个库）、冷库。
// 映像按 CDC 的真实输出造（services/cdc/internal/capture/value.go）：整数是 int，CHAR/VARCHAR 是 str，
// BINARY 是 raw，DATETIME(6) 是带 6 位小数的文本（小数是 0 也写出来），NULL 是没有设置 kind 的 Value。

type targetEnv struct {
	or, ir   *sharding.Router
	replicas []*sql.DB
	index    []*sql.DB
	cold     *sql.DB
}

func newTargetEnv(t *testing.T) *targetEnv {
	t.Helper()
	or, err := sharding.New(sharding.OrderLayout(2))
	if err != nil {
		t.Fatal(err)
	}
	ir, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil {
		t.Fatal(err)
	}
	e := &targetEnv{or: or, ir: ir}
	for i := range 2 {
		e.replicas = append(e.replicas, testdb.NewTemplate(t, order.Migrations, order.MigrationsDir, order.MigrationData{Slots: or.SlotsOf(i)}))
		e.index = append(e.index, testdb.NewTemplate(t, orderindex.Migrations, orderindex.MigrationsDir, orderindex.MigrationData{Slots: ir.SlotsOf(i)}))
	}
	e.cold = testdb.NewTemplate(t, order.ArchiveMigrations, order.ArchiveMigrationsDir, order.NewArchiveMigrationData())
	return e
}

// ord 是订单表的一行。时间是 CDC 的文本形式，paidAt、failReason 为空串、paymentID 为 0 表示 NULL。
type ord struct {
	id, user, merchant, version, status, total int64
	currency, idem                             string
	hash                                       []byte
	paymentID                                  int64
	failReason                                 string
	expireAt, paidAt, createdAt, updatedAt     string
}

// newOrd 造第 i 个订单。订单号的低 3 位是 i，所以连续的 i 落在不同的 slot（两个库各一半）；
// 商家号连续，商家索引的 slot 也是轮着的。
func newOrd(i int) ord {
	return ord{
		id: 1<<20 + int64(i), user: 7, merchant: 100 + int64(i), version: 3, status: 3, total: 12345,
		currency: "CNY", idem: "idem-" + string(rune('a'+i%26)), hash: bytes.Repeat([]byte{0xab, 0x00}, 16),
		paymentID: 9000 + int64(i), failReason: "",
		expireAt:  "2026-06-01 12:30:00.000000",
		paidAt:    "2026-06-01 12:05:00.123456",
		createdAt: "2026-06-01 12:00:00.000000", // 小数秒是 0
		updatedAt: "2026-06-01 12:05:00.123456",
	}
}

func cdcInt(n int64) *cdcv1.Value  { return &cdcv1.Value{Kind: &cdcv1.Value_Int{Int: n}} }
func cdcStr(s string) *cdcv1.Value { return &cdcv1.Value{Kind: &cdcv1.Value_Str{Str: s}} }
func cdcRaw(b []byte) *cdcv1.Value { return &cdcv1.Value{Kind: &cdcv1.Value_Raw{Raw: b}} }
func cdcNull() *cdcv1.Value        { return &cdcv1.Value{} }

func cdcNullableInt(n int64) *cdcv1.Value {
	if n == 0 {
		return cdcNull()
	}
	return cdcInt(n)
}

func cdcNullableStr(s string) *cdcv1.Value {
	if s == "" {
		return cdcNull()
	}
	return cdcStr(s)
}

// img 是这一行的 CDC 映像（orders_N 的全部列）。
func (o ord) img() map[string]*cdcv1.Value {
	return map[string]*cdcv1.Value{
		"order_id": cdcInt(o.id), "user_id": cdcInt(o.user), "merchant_id": cdcNullableInt(o.merchant),
		"version": cdcInt(o.version), "status": cdcInt(o.status), "total_amount": cdcInt(o.total),
		"currency": cdcStr(o.currency), "idempotency_key": cdcStr(o.idem), "request_hash": cdcRaw(o.hash),
		"payment_id": cdcNullableInt(o.paymentID), "fail_reason": cdcNullableStr(o.failReason),
		"expire_at": cdcStr(o.expireAt), "paid_at": cdcNullableStr(o.paidAt),
		"created_at": cdcStr(o.createdAt), "updated_at": cdcStr(o.updatedAt),
	}
}

func (e *targetEnv) change(o ord, deleted bool) Change {
	return Change{OrderID: o.id, Version: o.version, MerchantID: o.merchant, Slot: e.or.ForOrder(o.id), Deleted: deleted, Image: o.img()}
}

func nullable[T comparable](v T) any {
	var zero T
	if v == zero {
		return nil
	}
	return v
}

const orderInsertColumns = `order_id, user_id, merchant_id, version, status, total_amount, currency, idempotency_key,
	request_hash, payment_id, fail_reason, expire_at, paid_at, created_at, updated_at`

func insertOrd(t *testing.T, db *sql.DB, table string, o ord) {
	t.Helper()
	testdb.MustExec(t, db, "INSERT INTO "+table+" ("+orderInsertColumns+") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		o.id, o.user, nullable(o.merchant), o.version, o.status, o.total, o.currency, o.idem,
		o.hash, nullable(o.paymentID), nullable(o.failReason), o.expireAt, nullable(o.paidAt), o.createdAt, o.updatedAt)
}

func (e *targetEnv) insertReplica(t *testing.T, o ord) {
	t.Helper()
	slot := e.or.ForOrder(o.id)
	insertOrd(t, e.replicas[e.or.DB(slot)], sharding.Table("orders", slot), o)
}

func (e *targetEnv) insertCold(t *testing.T, o ord) {
	t.Helper()
	insertOrd(t, e.cold, "orders_archive", o)
}

func (e *targetEnv) insertIndex(t *testing.T, o ord) {
	t.Helper()
	slot := e.ir.ForMerchant(o.merchant)
	testdb.MustExec(t, e.index[e.ir.DB(slot)], "INSERT INTO "+sharding.Table("merchant_orders", slot)+
		` (merchant_id, order_id, user_id, status, total_amount, currency, created_at, paid_at, src_version, synced_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '2026-06-01 13:00:00')`,
		o.merchant, o.id, o.user, o.status, o.total, o.currency, o.createdAt, nullable(o.paidAt), o.version)
}

// targetCase 是一条变更和派生端的那一行。derived 为 nil 表示派生端没有这一行。
type targetCase struct {
	name    string
	derived func(o *ord) // 改派生端的那一行；nil 表示和映像相同
	missing bool
	image   func(o *ord, img map[string]*cdcv1.Value) // 改映像；nil 表示不改
	deleted bool
	want    Verdict
	// detail 是 Detail 里必须出现的片段。
	detail string
}

// runCases 每个用例一个订单，派生端按用例写好，然后一批核对，并检查这一批跨了两个库。
func runCases(t *testing.T, e *targetEnv, target Target, insert func(*testing.T, ord), dbOf func(ord) int, cases []targetCase) {
	t.Helper()
	cs := make([]Change, len(cases))
	dbs := map[int]bool{}
	for i, tc := range cases {
		o := newOrd(i)
		d := o
		if tc.derived != nil {
			tc.derived(&d)
		}
		if !tc.missing {
			insert(t, d)
		}
		cs[i] = e.change(o, tc.deleted)
		if tc.image != nil {
			tc.image(&o, cs[i].Image)
		}
		if !target.Wants(cs[i]) {
			t.Fatalf("%s: target does not want case %q", target.Name(), tc.name)
		}
		dbs[dbOf(o)] = true
	}
	if len(dbs) < 2 {
		t.Fatalf("the batch covers %d databases, want at least 2", len(dbs))
	}
	got, err := target.Check(t.Context(), cs)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(cs) {
		t.Fatalf("got %d results for %d changes", len(got), len(cs))
	}
	for i, tc := range cases {
		if got[i].Verdict != tc.want || !strings.Contains(got[i].Detail, tc.detail) {
			t.Errorf("%s: %q = %d %q, want %d containing %q", target.Name(), tc.name, got[i].Verdict, got[i].Detail, tc.want, tc.detail)
		}
		if tc.want == OK && got[i].Detail != "" {
			t.Errorf("%s: %q: detail %q on OK", target.Name(), tc.name, got[i].Detail)
		}
	}
}

// columnCases 是同一个版本下每种类型的列各有一个不同。
func columnCases(want Verdict) []targetCase {
	return []targetCase{
		{name: "整数列不同", derived: func(o *ord) { o.total++ }, want: want, detail: "total_amount"},
		{name: "字符列不同", derived: func(o *ord) { o.currency = "USD" }, want: want, detail: "currency"},
		{name: "时间列差一微秒", derived: func(o *ord) { o.createdAt = "2026-06-01 12:00:00.000001" }, want: want, detail: "created_at"},
		{name: "派生端非 NULL 映像 NULL", image: func(_ *ord, img map[string]*cdcv1.Value) { img["paid_at"] = cdcNull() }, want: want, detail: "paid_at"},
		{name: "派生端 NULL 映像非 NULL", derived: func(o *ord) { o.paidAt = "" }, want: want, detail: "paid_at: derived NULL"},
		{name: "映像的时间解析不了", image: func(_ *ord, img map[string]*cdcv1.Value) { img["created_at"] = cdcStr("not a time") }, want: want, detail: "created_at"},
		{name: "映像的类型不对", image: func(_ *ord, img map[string]*cdcv1.Value) { img["currency"] = cdcInt(1) }, want: want, detail: "currency"},
	}
}

// orderOnlyColumnCases 是商家索引里没有的列，从库和冷库要比。
func orderOnlyColumnCases(want Verdict) []targetCase {
	return []targetCase{
		{name: "二进制列不同", derived: func(o *ord) { o.hash = bytes.Repeat([]byte{0xab}, 32) }, want: want, detail: "request_hash"},
		{name: "可空的整数列 NULL 对非 NULL", image: func(_ *ord, img map[string]*cdcv1.Value) { img["payment_id"] = cdcNull() }, want: want, detail: "payment_id"},
		{name: "可空的字符列非 NULL 对 NULL", image: func(_ *ord, img map[string]*cdcv1.Value) { img["fail_reason"] = cdcStr("NO_STOCK") }, want: want, detail: "fail_reason"},
		{name: "商家列不同", derived: func(o *ord) { o.merchant = 0 }, want: want, detail: "merchant_id"},
		{name: "更新时间不同", derived: func(o *ord) { o.updatedAt = "2026-06-01 12:05:01.123456" }, want: want, detail: "updated_at"},
	}
}

func TestIndexTarget(t *testing.T) {
	t.Parallel()
	e := newTargetEnv(t)
	target, err := NewIndexTarget(e.index, e.ir)
	if err != nil {
		t.Fatal(err)
	}
	if target.Name() != "index" {
		t.Errorf("Name() = %q", target.Name())
	}
	cases := []targetCase{
		{name: "一致", want: OK},
		{name: "派生端版本更高", derived: func(o *ord) { o.version++; o.status = 5 }, want: OK},
		{name: "派生端版本更低", derived: func(o *ord) { o.version--; o.status = 2 }, want: Behind, detail: "derived version 2"},
		{name: "没有这一行", missing: true, want: Behind, detail: "not found"},
		{name: "归档的 DELETE 版本相同", deleted: true, want: OK},
		{name: "归档的 DELETE 索引没追上", deleted: true, derived: func(o *ord) { o.version-- }, want: Behind},
		// 索引里没有的列不比。
		{name: "索引没有的列不同", image: func(_ *ord, img map[string]*cdcv1.Value) { img["request_hash"] = cdcRaw([]byte{1}) }, want: OK},
		{name: "映像里缺一列", image: func(_ *ord, img map[string]*cdcv1.Value) { delete(img, "currency") }, derived: func(o *ord) { o.currency = "USD" }, want: OK},
		{name: "用户列不同", derived: func(o *ord) { o.user = 8 }, want: Mismatch, detail: "user_id: derived 8, change 7"},
		{name: "状态列不同", derived: func(o *ord) { o.status = 4 }, want: Mismatch, detail: "status"},
	}
	cases = append(cases, columnCases(Mismatch)...)
	runCases(t, e, target, e.insertIndex, func(o ord) int { return e.ir.DB(e.ir.ForMerchant(o.merchant)) }, cases)
}

func TestIndexTargetWants(t *testing.T) {
	t.Parallel()
	target, err := NewIndexTarget([]*sql.DB{{}, {}}, mustRouter(t, sharding.MerchantIndexLayout()))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		c    Change
		want bool
	}{
		{Change{OrderID: 1, MerchantID: 5}, true},
		{Change{OrderID: 1, MerchantID: 5, Deleted: true}, true},
		{Change{OrderID: 1}, false},
		{Change{OrderID: 1, Deleted: true}, false},
	} {
		if got := target.Wants(tt.c); got != tt.want {
			t.Errorf("Wants(%+v) = %t, want %t", tt.c, got, tt.want)
		}
	}
}

func TestReplicaTarget(t *testing.T) {
	t.Parallel()
	e := newTargetEnv(t)
	target, err := NewReplicaTarget(e.replicas, e.or)
	if err != nil {
		t.Fatal(err)
	}
	if target.Name() != "replica" {
		t.Errorf("Name() = %q", target.Name())
	}
	cases := []targetCase{
		{name: "一致", want: OK},
		{name: "没有商家的订单", derived: func(o *ord) { o.merchant = 0 }, image: func(_ *ord, img map[string]*cdcv1.Value) { img["merchant_id"] = cdcNull() }, want: OK},
		{name: "派生端版本更高", derived: func(o *ord) { o.version++; o.status = 5 }, want: OK},
		{name: "派生端版本更低", derived: func(o *ord) { o.version--; o.status = 2 }, want: Behind, detail: "derived version 2"},
		{name: "没有这一行", missing: true, want: Behind, detail: "not found"},
		{name: "DELETE 从库上还在", deleted: true, want: Behind, detail: "still on the replica"},
		{name: "DELETE 从库上已经没有", deleted: true, missing: true, want: OK},
		{name: "映像里缺一列", image: func(_ *ord, img map[string]*cdcv1.Value) { delete(img, "idempotency_key") }, derived: func(o *ord) { o.idem = "other" }, want: OK},
		{name: "映像里多一列", image: func(_ *ord, img map[string]*cdcv1.Value) { img["new_column"] = cdcInt(1) }, want: OK},
	}
	cases = append(cases, columnCases(Mismatch)...)
	cases = append(cases, orderOnlyColumnCases(Mismatch)...)
	runCases(t, e, target, e.insertReplica, func(o ord) int { return e.or.DB(e.or.ForOrder(o.id)) }, cases)
}

func TestReplicaTargetRejectsSlotOutOfRange(t *testing.T) {
	t.Parallel()
	target, err := NewReplicaTarget([]*sql.DB{{}, {}}, mustRouter(t, sharding.OrderLayout(2)))
	if err != nil {
		t.Fatal(err)
	}
	if !target.Wants(Change{OrderID: 1}) || !target.Wants(Change{OrderID: 1, Deleted: true}) {
		t.Error("replica should want every change")
	}
	if _, err := target.Check(t.Context(), []Change{{OrderID: 1, Version: 1, Slot: 8}}); err == nil {
		t.Error("slot out of range should be an error")
	}
}

func TestArchiveTarget(t *testing.T) {
	t.Parallel()
	e := newTargetEnv(t)
	target, err := NewArchiveTarget(e.cold)
	if err != nil {
		t.Fatal(err)
	}
	if target.Name() != "archive" {
		t.Errorf("Name() = %q", target.Name())
	}
	if target.Wants(Change{OrderID: 1, MerchantID: 2}) || !target.Wants(Change{OrderID: 1, Deleted: true}) {
		t.Error("archive should want only deletes")
	}
	cases := []targetCase{
		{name: "相同", deleted: true, want: OK},
		{name: "没有商家的订单相同", deleted: true, derived: func(o *ord) { o.merchant = 0 }, image: func(_ *ord, img map[string]*cdcv1.Value) { img["merchant_id"] = cdcNull() }, want: OK},
		{name: "冷库里没有", deleted: true, missing: true, want: Mismatch, detail: "not found in the cold database"},
		// 冷库没有 Behind：版本不同两个方向都是不一致。
		{name: "冷库的版本更高", deleted: true, derived: func(o *ord) { o.version++ }, want: Mismatch, detail: "version: derived 4, change 3"},
		{name: "冷库的版本更低", deleted: true, derived: func(o *ord) { o.version-- }, want: Mismatch, detail: "version: derived 2, change 3"},
	}
	for _, tc := range append(columnCases(Mismatch), orderOnlyColumnCases(Mismatch)...) {
		tc.deleted = true
		cases = append(cases, tc)
	}
	// 冷库只有一个库，runCases 要求跨两个库，这里用订单号的 slot 所在的库凑数：检查的是一批里混着多个 slot。
	runCases(t, e, target, e.insertCold, func(o ord) int { return e.or.DB(e.or.ForOrder(o.id)) }, cases)
}

// TestDatetimeMatchesCDCText 钉住：库里的 DATETIME(6) 和 CDC 文本形式的同一时刻比较相等（包括小数秒是 0），
// 差一微秒就不相等；不管连接的 DSN 有没有开 parseTime、loc 是什么、走不走文本协议。
func TestDatetimeMatchesCDCText(t *testing.T) {
	t.Parallel()
	e := newTargetEnv(t)
	same := newOrd(0) // created_at 小数秒是 0，paid_at、updated_at 有 6 位小数
	same.expireAt = "2026-12-31 23:59:59.999999"
	off := newOrd(1)
	e.insertReplica(t, same)
	e.insertReplica(t, off)
	cs := []Change{e.change(same, false), e.change(off, false)}
	cs[1].Image["created_at"] = cdcStr("2026-06-01 12:00:00.000001")

	dsn := os.Getenv(testdb.DSNEnv)
	variants := map[string]func(*mysql.Config){
		"mysqlx 的默认（parseTime、UTC）": nil,
		"不解析时间":                     func(c *mysql.Config) { c.ParseTime = false },
		"不解析时间、文本协议":                func(c *mysql.Config) { c.ParseTime, c.InterpolateParams = false, true },
		"解析时间、东八区":                  func(c *mysql.Config) { c.ParseTime, c.Loc = true, time.FixedZone("UTC+8", 8*3600) },
	}
	for name, adjust := range variants {
		dbs := e.replicas
		if adjust != nil {
			dbs = make([]*sql.DB, len(e.replicas))
			for i, src := range e.replicas {
				dbs[i] = reopen(t, dsn, src, adjust)
			}
		}
		target, err := NewReplicaTarget(dbs, e.or)
		if err != nil {
			t.Fatal(err)
		}
		got, err := target.Check(t.Context(), cs)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got[0].Verdict != OK {
			t.Errorf("%s: same instant = %d %q, want OK", name, got[0].Verdict, got[0].Detail)
		}
		if got[1].Verdict != Mismatch || !strings.Contains(got[1].Detail, "created_at: derived 2026-06-01 12:00:00.000000, change 2026-06-01 12:00:00.000001") {
			t.Errorf("%s: one microsecond off = %d %q, want Mismatch on created_at", name, got[1].Verdict, got[1].Detail)
		}
	}
}

// reopen 用另一套连接参数连到 src 所在的库。
func reopen(t *testing.T, dsn string, src *sql.DB, adjust func(*mysql.Config)) *sql.DB {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&cfg.DBName); err != nil {
		t.Fatal(err)
	}
	adjust(cfg)
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestCheckFailsWhenDatabaseIsClosed(t *testing.T) {
	t.Parallel()
	e := newTargetEnv(t)
	o := newOrd(0)
	e.insertReplica(t, o)
	e.insertIndex(t, o)
	e.insertCold(t, o)
	for _, db := range append(append(append([]*sql.DB{}, e.replicas...), e.index...), e.cold) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	index, err := NewIndexTarget(e.index, e.ir)
	if err != nil {
		t.Fatal(err)
	}
	replica, err := NewReplicaTarget(e.replicas, e.or)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := NewArchiveTarget(e.cold)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []Target{index, replica, archive} {
		c := e.change(o, target.Name() == "archive")
		if out, err := target.Check(t.Context(), []Change{c}); err == nil {
			t.Errorf("%s: Check on a closed database = %+v, want an error", target.Name(), out)
		}
	}
}

// TestTargetsAreSafeForConcurrentUse 让多个协程同时核对（带 -race 跑）。
func TestTargetsAreSafeForConcurrentUse(t *testing.T) {
	t.Parallel()
	e := newTargetEnv(t)
	var cs []Change
	for i := range 16 {
		o := newOrd(i)
		e.insertReplica(t, o)
		e.insertIndex(t, o)
		e.insertCold(t, o)
		cs = append(cs, e.change(o, i%2 == 1))
	}
	index, err := NewIndexTarget(e.index, e.ir)
	if err != nil {
		t.Fatal(err)
	}
	replica, err := NewReplicaTarget(e.replicas, e.or)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := NewArchiveTarget(e.cold)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		for _, target := range []Target{index, replica, archive} {
			wg.Go(func() {
				batch := latestPerOrder(target, cs)
				got, err := target.Check(t.Context(), batch)
				if err != nil {
					t.Error(err)
					return
				}
				for i, r := range got {
					want := OK
					if target.Name() == "replica" && batch[i].Deleted {
						want = Behind // 从库上这一行还在
					}
					if r.Verdict != want {
						t.Errorf("%s: order %d = %d %q, want %d", target.Name(), batch[i].OrderID, r.Verdict, r.Detail, want)
					}
				}
			})
		}
	}
	wg.Wait()
}

func TestNewTargetsValidate(t *testing.T) {
	t.Parallel()
	ir := mustRouter(t, sharding.MerchantIndexLayout())
	or := mustRouter(t, sharding.OrderLayout(2))
	if _, err := NewIndexTarget([]*sql.DB{{}}, ir); err == nil {
		t.Error("index: wrong number of databases should be rejected")
	}
	if _, err := NewIndexTarget([]*sql.DB{{}, nil}, ir); err == nil {
		t.Error("index: nil database should be rejected")
	}
	if _, err := NewReplicaTarget([]*sql.DB{{}, {}}, nil); err == nil {
		t.Error("replica: nil router should be rejected")
	}
	if _, err := NewReplicaTarget([]*sql.DB{{}, {}, {}}, or); err == nil {
		t.Error("replica: wrong number of databases should be rejected")
	}
	if _, err := NewArchiveTarget(nil); err == nil {
		t.Error("archive: nil database should be rejected")
	}
}

func mustRouter(t *testing.T, cfg sharding.Config) *sharding.Router {
	t.Helper()
	r, err := sharding.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
