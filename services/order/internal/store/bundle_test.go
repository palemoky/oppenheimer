package store_test

import (
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/services/order"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// oldAt 是测试订单的受理时间，带微秒级的小数秒。
var oldAt = time.Date(2025, 3, 1, 8, 30, 15, 123456000, time.UTC)

func newCold(t *testing.T) *store.Cold {
	t.Helper()
	return store.NewCold(testdb.NewTemplate(t, order.ArchiveMigrations, order.ArchiveMigrationsDir, order.NewArchiveMigrationData()))
}

// oldOrder 插入一个受理时间为 oldAt 的订单（seq 区分同一用户的订单），再用 SQL 改成 status，
// 并按 nulls 决定可空列是 NULL 还是非 NULL 的零值/非零值。返回订单号。
func (f *fixture) oldOrder(t *testing.T, userID, seq int64, status orderv1.OrderStatus, nulls bool) int64 {
	t.Helper()
	id := idgen.LowerBound(oldAt) | seq<<6 | idgen.GeneOf(userID)
	hash := make([]byte, 32)
	for i := range 31 {
		hash[i] = byte(i + 1)
	}
	// 最后一个字节是 0x00：BINARY(32) 用 0x00 填充，要确认冷热两边原样保存、摘要也算上它。
	o := &store.Order{
		OrderID: id, UserID: userID, Status: orderv1.OrderStatus_ORDER_STATUS_CREATING,
		Currency: "CNY", IdempotencyKey: "k" + string(rune('a'+seq)), RequestHash: hash,
		ExpireAt: oldAt.Add(15 * time.Minute), CreatedAt: oldAt, UpdatedAt: oldAt,
		Items: []store.Item{{SKUID: 7, Quantity: 2}, {SKUID: 3, Quantity: 1}},
	}
	if err := f.store.InsertOrder(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	slot := f.router.ForOrder(id)
	db := f.dbs[f.router.DB(slot)]
	var merchant, payment, reason, paid any // nil 表示 NULL
	if !nulls {
		merchant, payment, reason, paid = int64(0), 900+seq, "", oldAt.Add(3*time.Minute+789*time.Microsecond)
	}
	testdb.MustExec(t, db, "UPDATE "+sharding.Table("orders", slot)+
		` SET status = ?, merchant_id = ?, payment_id = ?, fail_reason = ?, paid_at = ?, version = 3, total_amount = 990,
		updated_at = ? WHERE order_id = ?`,
		int32(status), merchant, payment, reason, paid, oldAt.Add(5*time.Minute+1*time.Microsecond), id)
	if !nulls {
		testdb.MustExec(t, db, "INSERT INTO "+sharding.Table("payment_results", slot)+
			" (payment_id, order_id, succeeded, amount, disposition, created_at) VALUES (?, ?, 1, 990, 2, ?)",
			900+seq, id, oldAt.Add(4*time.Minute+5*time.Microsecond))
	}
	return id
}

// 同一个 bundle 从热库读、写进冷库再读回来，摘要相同；内容也相同。
func TestBundleDigestSameHotAndCold(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	cold := newCold(t)
	const user = 5
	ids := []int64{
		f.oldOrder(t, user, 1, orderv1.OrderStatus_ORDER_STATUS_PAID, false),
		f.oldOrder(t, user, 2, orderv1.OrderStatus_ORDER_STATUS_FAILED, true),
	}
	slot := f.router.ForUser(user)
	hot, err := f.store.ReadBundles(t.Context(), slot, ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(hot) != 2 || len(hot[0].Items) != 2 || len(hot[0].Payments) != 1 || len(hot[1].Payments) != 0 {
		t.Fatalf("hot bundles = %+v", hot)
	}
	if h := hot[0].Order.RequestHash; len(h) != 32 || h[31] != 0 {
		t.Fatalf("request_hash = %x, want 32 bytes ending in 00", h)
	}
	if !hot[0].Order.MerchantID.Valid || hot[1].Order.MerchantID.Valid || !hot[0].Order.FailReason.Valid {
		t.Fatalf("nullable columns read wrong: %+v / %+v", hot[0].Order, hot[1].Order)
	}
	if err := cold.UpsertBundles(t.Context(), hot); err != nil {
		t.Fatal(err)
	}
	// 再写一遍：整行覆盖，结果相同。
	if err := cold.UpsertBundles(t.Context(), hot); err != nil {
		t.Fatal(err)
	}
	back, err := cold.ReadBundles(t.Context(), ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != len(hot) {
		t.Fatalf("read back %d bundles, want %d", len(back), len(hot))
	}
	for i := range hot {
		if hot[i].Digest() != back[i].Digest() {
			t.Errorf("order %d: hot digest %s, cold %s\nhot  %+v\ncold %+v",
				hot[i].Order.OrderID, hot[i].Digest(), back[i].Digest(), hot[i], back[i])
		}
	}
}

func baseBundle() store.Bundle {
	at := oldAt
	return store.Bundle{
		Order: store.OrderRow{
			OrderID: 1 << 30, UserID: 5, MerchantID: sql.Null[int64]{V: 0, Valid: true}, Version: 3,
			Status: orderv1.OrderStatus_ORDER_STATUS_PAID, TotalAmount: 990, Currency: "CNY", IdempotencyKey: "k",
			RequestHash: []byte{1, 2, 0}, PaymentID: sql.Null[int64]{V: 9, Valid: true},
			FailReason: sql.Null[string]{V: "", Valid: true}, ExpireAt: at.Add(time.Minute),
			PaidAt: sql.Null[time.Time]{V: at.Add(2 * time.Minute), Valid: true}, CreatedAt: at, UpdatedAt: at.Add(3 * time.Minute),
		},
		Items: []store.ItemRow{
			{OrderID: 1 << 30, SKUID: 1, Quantity: 2, UnitPrice: 100, CreatedAt: at},
			{OrderID: 1 << 30, SKUID: 2, Quantity: 1, UnitPrice: 790, CreatedAt: at},
		},
		Payments: []store.PaymentRow{
			{PaymentID: 9, OrderID: 1 << 30, Succeeded: true, Amount: 990, Disposition: eventv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED, CreatedAt: at},
		},
	}
}

// 任何一列变了摘要就变；NULL 和零值不同；字段之间没有拼接歧义。
func TestBundleDigestCoversEveryColumn(t *testing.T) {
	t.Parallel()
	base := baseBundle()
	want := base.Digest()
	us := time.Microsecond
	for name, mutate := range map[string]func(b *store.Bundle){
		"order_id":               func(b *store.Bundle) { b.Order.OrderID++ },
		"user_id":                func(b *store.Bundle) { b.Order.UserID++ },
		"merchant_id value":      func(b *store.Bundle) { b.Order.MerchantID.V = 1 },
		"merchant_id null":       func(b *store.Bundle) { b.Order.MerchantID.Valid = false },
		"version":                func(b *store.Bundle) { b.Order.Version++ },
		"status":                 func(b *store.Bundle) { b.Order.Status = orderv1.OrderStatus_ORDER_STATUS_CANCELLED },
		"total_amount":           func(b *store.Bundle) { b.Order.TotalAmount++ },
		"currency":               func(b *store.Bundle) { b.Order.Currency = "USD" },
		"idempotency_key":        func(b *store.Bundle) { b.Order.IdempotencyKey = "K" },
		"request_hash":           func(b *store.Bundle) { b.Order.RequestHash = []byte{1, 2, 1} },
		"request_hash trailing0": func(b *store.Bundle) { b.Order.RequestHash = []byte{1, 2} },
		"payment_id value":       func(b *store.Bundle) { b.Order.PaymentID.V++ },
		"payment_id null":        func(b *store.Bundle) { b.Order.PaymentID = sql.Null[int64]{} },
		"fail_reason value":      func(b *store.Bundle) { b.Order.FailReason.V = "x" },
		"fail_reason null":       func(b *store.Bundle) { b.Order.FailReason = sql.Null[string]{} },
		"expire_at":              func(b *store.Bundle) { b.Order.ExpireAt = b.Order.ExpireAt.Add(us) },
		"paid_at value":          func(b *store.Bundle) { b.Order.PaidAt.V = b.Order.PaidAt.V.Add(us) },
		"paid_at null":           func(b *store.Bundle) { b.Order.PaidAt = sql.Null[time.Time]{} },
		"paid_at zero":           func(b *store.Bundle) { b.Order.PaidAt.V = time.Time{} },
		"created_at":             func(b *store.Bundle) { b.Order.CreatedAt = b.Order.CreatedAt.Add(us) },
		"updated_at":             func(b *store.Bundle) { b.Order.UpdatedAt = b.Order.UpdatedAt.Add(us) },
		// 相邻两个字符串之间挪一个字符：没有长度前缀时这两种拼接是一样的。
		"currency/key boundary": func(b *store.Bundle) { b.Order.Currency, b.Order.IdempotencyKey = "CN", "Yk" },
		"item order_id":         func(b *store.Bundle) { b.Items[0].OrderID++ },
		"item sku_id":           func(b *store.Bundle) { b.Items[0].SKUID = 3 },
		"item quantity":         func(b *store.Bundle) { b.Items[0].Quantity++ },
		"item unit_price":       func(b *store.Bundle) { b.Items[0].UnitPrice++ },
		"item created_at":       func(b *store.Bundle) { b.Items[0].CreatedAt = b.Items[0].CreatedAt.Add(us) },
		"item removed":          func(b *store.Bundle) { b.Items = b.Items[:1] },
		"payment payment_id":    func(b *store.Bundle) { b.Payments[0].PaymentID++ },
		"payment order_id":      func(b *store.Bundle) { b.Payments[0].OrderID++ },
		"payment succeeded":     func(b *store.Bundle) { b.Payments[0].Succeeded = false },
		"payment amount":        func(b *store.Bundle) { b.Payments[0].Amount++ },
		"payment disposition": func(b *store.Bundle) {
			b.Payments[0].Disposition = eventv1.PaymentDisposition_PAYMENT_DISPOSITION_REFUND_REQUIRED
		},
		"payment created_at": func(b *store.Bundle) { b.Payments[0].CreatedAt = b.Payments[0].CreatedAt.Add(us) },
		"payment added": func(b *store.Bundle) {
			b.Payments = append(b.Payments, store.PaymentRow{PaymentID: 10, OrderID: b.Order.OrderID, CreatedAt: oldAt})
		},
		// 一行明细挪成一行支付结果：行数前缀把它们分开。
		"rows moved between tables": func(b *store.Bundle) { b.Items, b.Payments = b.Items[:1], nil },
	} {
		b := baseBundle()
		b.Items = slices.Clone(b.Items)
		b.Payments = slices.Clone(b.Payments)
		mutate(&b)
		if b.Digest() == want {
			t.Errorf("%s: digest did not change", name)
		}
	}

	// 与内容无关的差别不改变摘要：子表行的顺序、时区、微秒以下的部分。
	same := baseBundle()
	slices.Reverse(same.Items)
	shanghai := time.FixedZone("UTC+8", 8*3600)
	same.Order.CreatedAt = same.Order.CreatedAt.In(shanghai).Add(999 * time.Nanosecond)
	if same.Digest() != want {
		t.Error("digest depends on item order, time zone or sub-microsecond part")
	}
}

// 列出只返回终态、早于上界、晚于游标的订单；计数分开终态和非终态。
func TestListArchivableAndCountBelow(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const user = 9
	slot := f.router.ForUser(user)
	paid := f.oldOrder(t, user, 1, orderv1.OrderStatus_ORDER_STATUS_PAID, false)
	pending := f.oldOrder(t, user, 2, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT, true)
	closed := f.oldOrder(t, user, 3, orderv1.OrderStatus_ORDER_STATUS_CLOSED, true)
	_ = pending
	below := idgen.LowerBound(oldAt.Add(time.Millisecond))

	got, err := f.store.ListArchivable(t.Context(), slot, 0, below, 10)
	if err != nil || !slices.Equal(got, []int64{paid, closed}) {
		t.Fatalf("ListArchivable = %v, %v; want [%d %d]", got, err, paid, closed)
	}
	got, err = f.store.ListArchivable(t.Context(), slot, paid, below, 10)
	if err != nil || !slices.Equal(got, []int64{closed}) {
		t.Fatalf("ListArchivable after cursor = %v, %v", got, err)
	}
	got, err = f.store.ListArchivable(t.Context(), slot, 0, idgen.LowerBound(oldAt), 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("ListArchivable below the orders = %v, %v; want none", got, err)
	}
	terminal, nonTerminal, err := f.store.CountBelow(t.Context(), slot, below)
	if err != nil || terminal != 2 || nonTerminal != 1 {
		t.Fatalf("CountBelow = %d, %d, %v; want 2, 1", terminal, nonTerminal, err)
	}
}

// 期望摘要不对的不删，报告为不符；对的删掉三张表的行；热库里没有的报告为 Gone。
func TestDeleteArchivedVerifiesDigest(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	const user = 3
	slot := f.router.ForUser(user)
	good := f.oldOrder(t, user, 1, orderv1.OrderStatus_ORDER_STATUS_PAID, false)
	bad := f.oldOrder(t, user, 2, orderv1.OrderStatus_ORDER_STATUS_CANCELLED, false)
	gone := idgen.LowerBound(oldAt) | 99<<6 | idgen.GeneOf(user)
	bundles, err := f.store.ReadBundles(t.Context(), slot, []int64{good, bad})
	if err != nil || len(bundles) != 2 {
		t.Fatalf("ReadBundles = %d, %v", len(bundles), err)
	}
	wrong := bundles[1].Digest()
	wrong[0] ^= 1
	res, err := f.store.DeleteArchived(t.Context(), slot, map[int64]store.Digest{
		good: bundles[0].Digest(), bad: wrong, gone: {},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Deleted, []int64{good}) || !slices.Equal(res.Mismatched, []int64{bad}) || !slices.Equal(res.Gone, []int64{gone}) {
		t.Fatalf("DeleteArchived = %+v", res)
	}
	for _, base := range []string{"orders", "order_items", "payment_results"} {
		if n := f.rows(t, base, slot, good); n != 0 {
			t.Errorf("%s still has %d rows of the deleted order", base, n)
		}
		if n := f.rows(t, base, slot, bad); n == 0 {
			t.Errorf("%s lost the rows of the mismatched order", base)
		}
	}
}

func TestArchiveRejectsBadInput(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	cold := newCold(t)
	const user = 3
	slot := f.router.ForUser(user)
	pending := f.oldOrder(t, user, 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT, true)
	b, err := f.store.ReadBundles(t.Context(), slot, []int64{pending})
	if err != nil {
		t.Fatal(err)
	}
	if err := cold.UpsertBundles(t.Context(), b); !errors.Is(err, store.ErrNotTerminal) {
		t.Errorf("UpsertBundles(non-terminal) = %v, want ErrNotTerminal", err)
	}
	if _, err := f.store.ReadBundles(t.Context(), (slot+1)%f.router.Slots(), []int64{pending}); !errors.Is(err, store.ErrWrongShard) {
		t.Errorf("ReadBundles(wrong slot) = %v, want ErrWrongShard", err)
	}
	if _, err := f.store.DeleteArchived(t.Context(), (slot+1)%f.router.Slots(), map[int64]store.Digest{pending: {}}); !errors.Is(err, store.ErrWrongShard) {
		t.Errorf("DeleteArchived(wrong slot) = %v, want ErrWrongShard", err)
	}
	if _, err := f.store.ReadBundles(t.Context(), slot, nil); err == nil {
		t.Error("ReadBundles(empty) succeeded")
	}
	// 非终态的订单即使摘要对也不删。
	res, err := f.store.DeleteArchived(t.Context(), slot, map[int64]store.Digest{pending: b[0].Digest()})
	if err != nil || len(res.Deleted) != 0 || !slices.Equal(res.Mismatched, []int64{pending}) {
		t.Errorf("DeleteArchived(non-terminal) = %+v, %v", res, err)
	}
}
