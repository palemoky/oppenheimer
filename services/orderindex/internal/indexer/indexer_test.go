package indexer

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/storetest"
)

func vInt(n int64) *cdcv1.Value  { return &cdcv1.Value{Kind: &cdcv1.Value_Int{Int: n}} }
func vStr(s string) *cdcv1.Value { return &cdcv1.Value{Kind: &cdcv1.Value_Str{Str: s}} }
func vNull() *cdcv1.Value        { return &cdcv1.Value{} }

// image 返回订单 order 在版本 version 上的 after 映像（orders_N 的全部列，外加一个不认识的列）。
// 每个版本的状态、金额、paid_at 都不同，便于判断最后落下的是哪个版本。
func image(merchant, order, version int64) map[string]*cdcv1.Value {
	paid := vNull()
	if version%2 == 0 {
		paid = vStr(fmt.Sprintf("2026-06-01 12:%02d:00.5", version%60))
	}
	merchantV := vNull()
	if merchant != 0 {
		merchantV = vInt(merchant)
	}
	return map[string]*cdcv1.Value{
		"order_id":        vInt(order),
		"user_id":         vInt(order * 10),
		"merchant_id":     merchantV,
		"version":         vInt(version),
		"status":          vInt(1 + version%6),
		"total_amount":    vInt(1000 + version),
		"currency":        vStr("CNY"),
		"idempotency_key": vStr("k" + strconv.FormatInt(order, 10)),
		"request_hash":    {Kind: &cdcv1.Value_Raw{Raw: make([]byte, 32)}},
		"payment_id":      vNull(),
		"fail_reason":     vNull(),
		"expire_at":       vStr("2026-06-01 12:15:00.000000"),
		"paid_at":         paid,
		"created_at":      vStr("2026-06-01 12:00:00.123456"),
		"updated_at":      vStr("2026-06-01 12:00:01.000000"),
		"some_new_column": vStr("ignored"),
	}
}

func change(op cdcv1.Op, after map[string]*cdcv1.Value) *cdcv1.RowChange {
	return &cdcv1.RowChange{
		Source:       "order-0",
		Table:        "orders",
		Slot:         1,
		Op:           op,
		After:        after,
		Gtid:         "uuid:1",
		CommitMicros: time.Now().Add(-50 * time.Millisecond).UnixMicro(),
	}
}

func wantRow(t *testing.T, merchant, order, version int64) store.Row {
	t.Helper()
	r, ok, err := decodeRow(image(merchant, order, version))
	if err != nil || !ok {
		t.Fatalf("decode image: ok=%v err=%v", ok, err)
	}
	return r
}

func sameImage(a, b store.Row) bool {
	return a.MerchantID == b.MerchantID && a.OrderID == b.OrderID && a.UserID == b.UserID &&
		a.Status == b.Status && a.TotalAmount == b.TotalAmount && a.Currency == b.Currency &&
		a.CreatedAt.Equal(b.CreatedAt) && a.PaidAt.Equal(b.PaidAt) && a.Version == b.Version
}

func newIndexer(t *testing.T) (*Indexer, *store.Store) {
	t.Helper()
	st, _ := storetest.New(t)
	ix, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	return ix, st
}

func TestDecodeDatetimeFractions(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		in   string
		want time.Time
	}{
		{"2026-06-01 12:00:00", time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)},
		{"2026-06-01 12:00:00.1", time.Date(2026, 6, 1, 12, 0, 0, 100_000_000, time.UTC)},
		{"2026-06-01 12:00:00.123", time.Date(2026, 6, 1, 12, 0, 0, 123_000_000, time.UTC)},
		{"2026-06-01 12:00:00.123456", time.Date(2026, 6, 1, 12, 0, 0, 123_456_000, time.UTC)},
		{"2026-06-01 12:00:00.000000", time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)},
	} {
		img := image(1, 1, 2)
		img["created_at"] = vStr(c.in)
		img["paid_at"] = vStr(c.in)
		r, _, err := decodeRow(img)
		if err != nil {
			t.Fatalf("%q: %v", c.in, err)
		}
		if !r.CreatedAt.Equal(c.want) || r.CreatedAt.Location() != time.UTC || !r.PaidAt.Equal(c.want) {
			t.Errorf("%q: created_at=%v paid_at=%v, want %v UTC", c.in, r.CreatedAt, r.PaidAt, c.want)
		}
	}
	img := image(1, 1, 1)
	img["paid_at"] = vNull()
	if r, _, err := decodeRow(img); err != nil || !r.PaidAt.IsZero() {
		t.Errorf("NULL paid_at: paid=%v err=%v", r.PaidAt, err)
	}
}

func TestDecodeSchemaErrors(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(map[string]*cdcv1.Value){
		"missing version":      func(m map[string]*cdcv1.Value) { delete(m, "version") },
		"missing merchant_id":  func(m map[string]*cdcv1.Value) { delete(m, "merchant_id") },
		"missing paid_at":      func(m map[string]*cdcv1.Value) { delete(m, "paid_at") },
		"renamed status":       func(m map[string]*cdcv1.Value) { m["state"] = m["status"]; delete(m, "status") },
		"status as string":     func(m map[string]*cdcv1.Value) { m["status"] = vStr("PAID") },
		"NULL total_amount":    func(m map[string]*cdcv1.Value) { m["total_amount"] = vNull() },
		"currency as int":      func(m map[string]*cdcv1.Value) { m["currency"] = vInt(156) },
		"bad created_at":       func(m map[string]*cdcv1.Value) { m["created_at"] = vStr("2026-06-01T12:00:00Z") },
		"zero merchant_id":     func(m map[string]*cdcv1.Value) { m["merchant_id"] = vInt(0) },
		"status out of range":  func(m map[string]*cdcv1.Value) { m["status"] = vInt(300) },
		"uint overflow":        func(m map[string]*cdcv1.Value) { m["user_id"] = &cdcv1.Value{Kind: &cdcv1.Value_Uint{Uint: 1 << 63}} },
		"datetime as raw byte": func(m map[string]*cdcv1.Value) { m["paid_at"] = &cdcv1.Value{Kind: &cdcv1.Value_Raw{Raw: []byte("x")}} },
	} {
		img := image(1, 1, 2)
		mutate(img)
		if _, _, err := decodeRow(img); !errors.Is(err, ErrSchema) {
			t.Errorf("%s: err = %v, want ErrSchema", name, err)
		}
	}
	// 无符号整数在范围内是可以的。
	img := image(1, 1, 2)
	img["user_id"] = &cdcv1.Value{Kind: &cdcv1.Value_Uint{Uint: 42}}
	if r, _, err := decodeRow(img); err != nil || r.UserID != 42 {
		t.Errorf("uint user_id: %v %v", r.UserID, err)
	}
}

// apply 把 msgs 作为一批应用，返回这一批的统计。
func apply(t *testing.T, ix *Indexer, msgs ...*cdcv1.RowChange) Counts {
	t.Helper()
	c, err := ix.Apply(t.Context(), msgs)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// message 把一条变更序列化成 kafka 消息，offset 只用来在日志里区分。
func message(t testing.TB, m *cdcv1.RowChange, offset int64) kafkax.Message {
	t.Helper()
	v, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return kafkax.Message{Topic: "cdc.order.orders", Offset: offset, Value: v}
}

// oneByOne 模拟 kafkax 一批失败后的逐条回退：每条消息单独作为一条的批交给 h，返回每条的结果（不重试）。
func oneByOne(t *testing.T, h kafkax.BatchHandler, msgs []kafkax.Message) []error {
	t.Helper()
	errs := make([]error, len(msgs))
	for i, m := range msgs {
		errs[i] = h(t.Context(), []kafkax.Message{m})
	}
	return errs
}

// 1 + 6：一批里混着 INSERT、UPDATE、READ、DELETE、没有商家的、别的表、不认识的 op、同一个订单的多个版本
// （顺序打乱）。结束后每个订单逐列等于它的最高版本；没有商家的不在索引里；归档的（DELETE）行在索引里。
// 索引原本是空的，每一行都是插入，Changed 的估计在这种情况下是精确的，所以统计逐项精确。
func TestBatchMixedOps(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	other := change(cdcv1.Op_OP_INSERT, image(3, 60, 1))
	other.Table = "order_items"
	msgs := []*cdcv1.RowChange{
		change(cdcv1.Op_OP_INSERT, image(1, 10, 1)),
		change(cdcv1.Op_OP_UPDATE, image(1, 10, 3)),
		change(cdcv1.Op_OP_READ, image(1, 10, 2)),
		change(cdcv1.Op_OP_UPDATE, image(1, 10, 3)), // 重复的最高版本
		change(cdcv1.Op_OP_INSERT, image(2, 20, 1)),
		change(cdcv1.Op_OP_READ, image(3, 30, 4)),
		change(cdcv1.Op_OP_UPDATE, image(4, 40, 4)),
		deletion(image(4, 40, 5)), // 归档，最高版本
		change(cdcv1.Op_OP_UPDATE, image(4, 40, 3)),
		deletion(image(5, 50, 2)),                   // 只剩 DELETE 的归档订单（重建）
		change(cdcv1.Op_OP_INSERT, image(0, 70, 1)), // 没有商家
		deletion(image(0, 71, 2)),                   // 没有商家的归档
		other,
		change(cdcv1.Op(99), image(3, 61, 1)),
	}
	rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
	got := apply(t, ix, msgs...)

	want := map[[2]int64]int64{{1, 10}: 3, {2, 20}: 1, {3, 30}: 4, {4, 40}: 5, {5, 50}: 2}
	for k, v := range want {
		r, err := st.Get(t.Context(), k[0], k[1])
		if err != nil {
			t.Fatalf("order %d: %v", k[1], err)
		}
		if w := wantRow(t, k[0], k[1], v); !sameImage(r, w) {
			t.Errorf("order %d: row = %+v, want v%d %+v", k[1], r, v, w)
		}
	}
	for _, k := range [][2]int64{{3, 60}, {3, 61}, {1, 70}, {1, 71}} {
		if _, err := st.Get(t.Context(), k[0], k[1]); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("order %d was written: %v", k[1], err)
		}
	}
	// 有商家的消息 10 条，合并成 5 行写入；5 行都是插入。延迟只算写进索引的（有商家的）消息。
	wantCounts := Counts{Changed: 5, Stale: 5, Written: 5, Merged: 5, SkippedNoMerchant: 2, Ignored: 2, Archived: 3, LagCount: 10}
	got.LagSum, got.LagMax = 0, 0
	if got != wantCounts {
		t.Errorf("counts = %+v\nwant     %+v", got, wantCounts)
	}
	if s := ix.Stats().take(); s.Changed != 5 || s.LagCount != 10 || s.LagMax <= 0 || s.LagSum <= 0 {
		t.Errorf("stats = %+v", s)
	}
}

// 2：版本比较在多行语句里仍然成立。索引里已有 v5（和别的订单一起写在同一条多行语句里）：
// 一批带着同一订单的 v3、v4 → 行逐列不变（含 synced_at）；带着 v6 → 逐列变成 v6。
// 如果把 src_version 挪到最前面赋值，v6 那一步只有版本号会变，这里会失败（TestUpsertAssignmentOrder 专门钉住）。
func TestBatchVersionCompare(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	first := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	ix.now = func() time.Time { return first }
	// 商家 1 和 5 在同一个 slot：每一批都是同一张表上的多行语句。
	apply(t, ix, change(cdcv1.Op_OP_UPDATE, image(1, 100, 5)), change(cdcv1.Op_OP_UPDATE, image(5, 101, 1)))
	before, err := st.Get(t.Context(), 1, 100)
	if err != nil {
		t.Fatal(err)
	}

	ix.now = func() time.Time { return first.Add(time.Hour) }
	c := apply(t, ix,
		change(cdcv1.Op_OP_UPDATE, image(1, 100, 3)),
		change(cdcv1.Op_OP_UPDATE, image(5, 101, 2)),
		change(cdcv1.Op_OP_UPDATE, image(1, 100, 4)),
	)
	if after, err := st.Get(t.Context(), 1, 100); err != nil || after != before {
		t.Errorf("v3, v4 over v5 changed the row:\n got %+v (%v)\nwant %+v", after, err, before)
	}
	if got, err := st.Get(t.Context(), 5, 101); err != nil || !sameImage(got, wantRow(t, 5, 101, 2)) {
		t.Errorf("order 101 = %+v, %v; want v2", got, err)
	}
	// 写了 2 行（v4 合并掉 v3）：101 覆盖、100 没变，Affected 2，估计 min(2, 2) = 2，比真实的 1 多 1。
	if c.Written != 2 || c.Merged != 1 || c.Changed+c.Stale != 3 || c.Changed < 1 || c.Changed > 2 {
		t.Errorf("counts = %+v", c)
	}

	c = apply(t, ix, change(cdcv1.Op_OP_UPDATE, image(1, 100, 6)), change(cdcv1.Op_OP_UPDATE, image(1, 100, 3)),
		change(cdcv1.Op_OP_UPDATE, image(5, 101, 2)))
	got, err := st.Get(t.Context(), 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if want := wantRow(t, 1, 100, 6); !sameImage(got, want) || !got.SyncedAt.Equal(first.Add(time.Hour)) {
		t.Errorf("row = %+v, want v6 %+v synced at %v", got, want, first.Add(time.Hour))
	}
	if c.Written != 2 || c.Merged != 1 {
		t.Errorf("counts = %+v", c)
	}
}

// 同一条消息重放：第二次不改行，synced_at 不变。
func TestDuplicateMessageKeepsSyncedAt(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	first := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	ix.now = func() time.Time { return first }
	msg := change(cdcv1.Op_OP_UPDATE, image(2, 50, 3))
	if c := apply(t, ix, msg); c.Changed != 1 || c.Stale != 0 {
		t.Fatalf("first apply = %+v", c)
	}
	ix.now = func() time.Time { return first.Add(time.Hour) }
	if c := apply(t, ix, msg); c.Changed != 0 || c.Stale != 1 {
		t.Fatalf("replay = %+v", c)
	}
	got, err := st.Get(t.Context(), 2, 50)
	if err != nil {
		t.Fatal(err)
	}
	if !got.SyncedAt.Equal(first) {
		t.Errorf("synced_at = %v, want %v", got.SyncedAt, first)
	}
}

// merchant_id 为 NULL 跳过；别的表、不认识的 op 忽略；都不写库。一批里只有这些时不碰数据库。
func TestSkipsAndIgnores(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	other := change(cdcv1.Op_OP_INSERT, image(3, 60, 1))
	other.Table = "order_items"
	unknownOp := change(cdcv1.Op(99), image(3, 61, 1))
	c := apply(t, ix,
		change(cdcv1.Op_OP_INSERT, image(0, 63, 0)),
		change(cdcv1.Op_OP_READ, image(0, 64, 0)),
		other,
		unknownOp,
	)
	if c != (Counts{SkippedNoMerchant: 2, Ignored: 2}) {
		t.Errorf("counts = %+v", c)
	}
	for _, order := range []int64{60, 61} {
		if _, err := st.Get(t.Context(), 3, order); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("order %d was written: %v", order, err)
		}
	}
	w := ix.Stats().take()
	if w.SkippedNoMerchant != 2 || w.Archived != 0 || w.Ignored != 2 || w.Changed+w.Stale != 0 {
		t.Errorf("stats = %+v", w)
	}
}

// 缺少需要的列：返回 ErrSchema（不是 Permanent，kafkax 会原地重试而不是进死信），不写库。
// 反序列化失败才是 Permanent。都是只有一条消息的批，含义与按批之前逐条的 handler 相同。
func TestHandlerErrors(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	h := ix.Handler(DefaultWriteTimeout)

	img := image(5, 70, 2)
	delete(img, "total_amount")
	err := h(t.Context(), []kafkax.Message{message(t, change(cdcv1.Op_OP_UPDATE, img), 0)})
	if !errors.Is(err, ErrSchema) || kafkax.IsPermanent(err) {
		t.Fatalf("missing column: err = %v, want non-permanent ErrSchema", err)
	}
	if _, err := st.Get(t.Context(), 5, 70); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("row written despite schema error: %v", err)
	}

	err = h(t.Context(), []kafkax.Message{{Value: []byte{0xff, 0xff, 0xff}}})
	if !kafkax.IsPermanent(err) {
		t.Fatalf("garbage: err = %v, want permanent", err)
	}

	if err := h(t.Context(), []kafkax.Message{message(t, change(cdcv1.Op_OP_INSERT, image(5, 71, 1)), 0)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(t.Context(), 5, 71); err != nil {
		t.Fatal(err)
	}
}

// goodBatch 返回一批好消息（4 个商家、跨两个库）和它们对应的最终行，用来夹带一条坏消息。
func goodBatch(base int64) (msgs []*cdcv1.RowChange, keys [][2]int64) {
	for i := range int64(6) {
		m, o := 1+i%4, base+i
		msgs = append(msgs, change(cdcv1.Op_OP_UPDATE, image(m, o, 2)))
		keys = append(keys, [2]int64{m, o})
	}
	return msgs, keys
}

// 3 + 4：一批里夹着一条坏消息时整批返回错误、一行都不写；kafkax 逐条回退之后好的都写进去，
// 坏的那条单独返回同类的错误。
//   - 缺列、缺映像（ErrSchema）：非 Permanent，逐条时原地重试、分区停住。
//   - 反序列化不了：Permanent，逐条时进死信、分区继续。
func TestBatchWithBadMessageFallsBack(t *testing.T) {
	t.Parallel()
	missingColumn := image(2, 9001, 3)
	delete(missingColumn, "currency")
	cases := []struct {
		name      string
		bad       func(t *testing.T) kafkax.Message
		permanent bool
	}{
		{"missing column", func(t *testing.T) kafkax.Message {
			return message(t, change(cdcv1.Op_OP_UPDATE, missingColumn), 0)
		}, false},
		{"insert without after", func(t *testing.T) kafkax.Message {
			return message(t, change(cdcv1.Op_OP_INSERT, nil), 0)
		}, false},
		{"delete without before", func(t *testing.T) kafkax.Message {
			return message(t, deletion(nil), 0)
		}, false},
		{"not a row change", func(*testing.T) kafkax.Message {
			return kafkax.Message{Value: []byte{0xff, 0xff, 0xff}}
		}, true},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ix, st := newIndexer(t)
			h := ix.Handler(DefaultWriteTimeout)
			good, keys := goodBatch(int64(1000 * (i + 1)))
			var batch []kafkax.Message
			for j, m := range good {
				batch = append(batch, message(t, m, int64(j)))
			}
			badAt := 3
			batch = slices.Insert(batch, badAt, c.bad(t))

			checkErr := func(what string, err error) {
				t.Helper()
				if kafkax.IsPermanent(err) != c.permanent || (!c.permanent && !errors.Is(err, ErrSchema)) {
					t.Errorf("%s: err = %v, want permanent=%v", what, err, c.permanent)
				}
			}
			checkErr("whole batch", h(t.Context(), batch))
			for _, k := range keys {
				if _, err := st.Get(t.Context(), k[0], k[1]); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("order %d written by a failed batch: %v", k[1], err)
				}
			}
			if s := ix.Stats().take(); s != (Counts{}) {
				t.Errorf("failed batch was counted: %+v", s)
			}

			for j, err := range oneByOne(t, h, batch) {
				if j == badAt {
					checkErr("bad message alone", err)
				} else if err != nil {
					t.Errorf("good message %d alone: %v", j, err)
				}
			}
			for _, k := range keys {
				if r, err := st.Get(t.Context(), k[0], k[1]); err != nil || !sameImage(r, wantRow(t, k[0], k[1], 2)) {
					t.Errorf("order %d after fallback = %+v, %v", k[1], r, err)
				}
			}
			if s := ix.Stats().take(); s.Changed != int64(len(keys)) || s.Written != int64(len(keys)) {
				t.Errorf("stats after fallback = %+v", s)
			}
		})
	}
}

// 两个库之一写失败（这里用一张被删掉的表模拟）时整批返回错误、不计数；另一个库已提交的那一半在重做时是空操作。
func TestBatchPartialFailureIsRedoable(t *testing.T) {
	t.Parallel()
	st, dbs := storetest.New(t)
	ix, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := st.Router()
	// 找两个在不同库上的商家。
	var m0, m1 int64
	for m := int64(1); m0 == 0 || m1 == 0; m++ {
		if r.DB(r.ForMerchant(m)) == 0 && m0 == 0 {
			m0 = m
		} else if r.DB(r.ForMerchant(m)) == 1 && m1 == 0 {
			m1 = m
		}
	}
	table := sharding.Table(store.TableBase, r.ForMerchant(m1))
	renamed := table + "_gone"
	if _, err := dbs[1].ExecContext(t.Context(), "RENAME TABLE "+table+" TO "+renamed); err != nil { //nolint:gosec // 表名只来自 sharding.Table
		t.Fatal(err)
	}
	batch := []*cdcv1.RowChange{change(cdcv1.Op_OP_UPDATE, image(m0, 1, 3)), change(cdcv1.Op_OP_UPDATE, image(m1, 2, 3))}
	if _, err := ix.Apply(t.Context(), batch); err == nil || kafkax.IsPermanent(err) {
		t.Fatalf("apply with a missing table: err = %v, want a retryable error", err)
	}
	if s := ix.Stats().take(); s != (Counts{}) {
		t.Errorf("failed batch was counted: %+v", s)
	}
	if _, err := st.Get(t.Context(), m0, 1); err != nil {
		t.Fatalf("the other database should have committed its half: %v", err)
	}
	if _, err := dbs[1].ExecContext(t.Context(), "RENAME TABLE "+renamed+" TO "+table); err != nil { //nolint:gosec // 同上
		t.Fatal(err)
	}
	c := apply(t, ix, batch...)
	if c.Written != 2 || c.Changed != 1 || c.Stale != 1 {
		t.Errorf("redo counts = %+v, want one change and one no-op", c)
	}
	for _, k := range [][2]int64{{m0, 1}, {m1, 2}} {
		if got, err := st.Get(t.Context(), k[0], k[1]); err != nil || !sameImage(got, wantRow(t, k[0], k[1], 3)) {
			t.Errorf("order %d = %+v, %v", k[1], got, err)
		}
	}
}

// deletion 返回归档产生的 DELETE：只有 before，没有 after。
func deletion(before map[string]*cdcv1.Value) *cdcv1.RowChange {
	ch := change(cdcv1.Op_OP_DELETE, nil)
	ch.Before = before
	return ch
}

// 终态的版本：image 里 status = 1 + version%6，版本 5 是 6（终态之一），具体取值对索引无所谓，
// 只要 DELETE 的 before 原样落进索引。
const finalVersion = 5

// 索引里已经是版本 v：同一订单 v 的 DELETE 是空操作，行逐列不变（含 synced_at），归为 Stale，归档计数加一。
func TestDeleteOfCurrentVersionIsNoop(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	first := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	ix.now = func() time.Time { return first }
	if c := apply(t, ix, change(cdcv1.Op_OP_UPDATE, image(7, 80, finalVersion))); c.Changed != 1 {
		t.Fatalf("update = %+v", c)
	}
	before, err := st.Get(t.Context(), 7, 80)
	if err != nil {
		t.Fatal(err)
	}
	ix.Stats().take()

	ix.now = func() time.Time { return first.Add(time.Hour) }
	if c := apply(t, ix, deletion(image(7, 80, finalVersion))); c.Stale != 1 || c.Changed != 0 {
		t.Fatalf("delete = %+v; want Stale", c)
	}
	after, err := st.Get(t.Context(), 7, 80)
	if err != nil {
		t.Fatalf("index row gone after DELETE: %v", err)
	}
	if after != before {
		t.Errorf("row changed by DELETE:\n got %+v\nwant %+v", after, before)
	}
	if w := ix.Stats().take(); w.Stale != 1 || w.Archived != 1 || w.Changed+w.SkippedNoMerchant+w.Ignored != 0 {
		t.Errorf("stats = %+v", w)
	}
}

// 索引为空（从头重建，压缩后的 topic 里这个归档订单只剩 DELETE）：DELETE 的 before 建出索引行，
// 商家列表能查到它。
func TestDeleteRebuildsMissingRow(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	if c := apply(t, ix, deletion(image(8, 90, finalVersion))); c.Changed != 1 {
		t.Fatalf("delete = %+v; want Changed", c)
	}
	got, err := st.Get(t.Context(), 8, 90)
	if err != nil {
		t.Fatal(err)
	}
	if want := wantRow(t, 8, 90, finalVersion); !sameImage(got, want) {
		t.Errorf("row = %+v, want %+v", got, want)
	}
	if w := ix.Stats().take(); w.Changed != 1 || w.Archived != 1 || w.LagCount != 1 {
		t.Errorf("stats = %+v", w)
	}

	for _, q := range []store.ListQuery{
		{MerchantID: 8, Limit: 10},
		{MerchantID: 8, Status: got.Status, Limit: 10},
	} {
		rows, err := st.List(t.Context(), q)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].OrderID != 90 || !sameImage(rows[0], got) {
			t.Errorf("list %+v = %+v, want the archived order", q, rows)
		}
	}
}

// 乱序：DELETE（v=5）先到，迟到的 UPDATE（v=3）后到，索引停在 v=5；DELETE 也不会把更高版本拉回去。
func TestDeleteOrdersByVersion(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	if c := apply(t, ix, deletion(image(9, 100, 5))); c.Changed != 1 {
		t.Fatalf("delete = %+v", c)
	}
	if c := apply(t, ix, change(cdcv1.Op_OP_UPDATE, image(9, 100, 3))); c.Stale != 1 {
		t.Fatalf("late update = %+v; want Stale", c)
	}
	got, err := st.Get(t.Context(), 9, 100)
	if err != nil {
		t.Fatal(err)
	}
	if want := wantRow(t, 9, 100, 5); !sameImage(got, want) {
		t.Errorf("row = %+v, want v5 %+v", got, want)
	}

	// 反过来：索引已是 v=6，旧的 DELETE（v=4，重放的一段）不改行。
	apply(t, ix, change(cdcv1.Op_OP_UPDATE, image(9, 101, 6)))
	if c := apply(t, ix, deletion(image(9, 101, 4))); c.Stale != 1 {
		t.Fatalf("old delete = %+v; want Stale", c)
	}
	if got, err := st.Get(t.Context(), 9, 101); err != nil || !sameImage(got, wantRow(t, 9, 101, 6)) {
		t.Errorf("row = %+v, %v; want v6", got, err)
	}
}

// before 里 merchant_id 为 NULL：和 after 一样跳过，不建行，归档仍计数。
func TestDeleteWithoutMerchantSkipped(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	if c := apply(t, ix, deletion(image(0, 110, 3))); c.SkippedNoMerchant != 1 {
		t.Fatalf("delete = %+v; want SkippedNoMerchant", c)
	}
	rows, err := st.List(t.Context(), store.ListQuery{MerchantID: 1, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("rows written: %+v", rows)
	}
	if w := ix.Stats().take(); w.SkippedNoMerchant != 1 || w.Archived != 1 || w.Changed+w.Stale != 0 {
		t.Errorf("stats = %+v", w)
	}
}

// 没有 before 的 DELETE 和没有 after 的 INSERT 走同一条错误路径：非 Permanent 的 ErrSchema（原地重试、
// 分区停住），不写库、不计数。
func TestImageMissingIsSchemaError(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	h := ix.Handler(DefaultWriteTimeout)
	for name, msg := range map[string]*cdcv1.RowChange{
		"insert without after": change(cdcv1.Op_OP_INSERT, nil),
		"delete without before": func() *cdcv1.RowChange {
			ch := deletion(nil)
			ch.After = image(10, 120, 2) // 有 after 也没用：DELETE 只看 before
			return ch
		}(),
	} {
		err := h(t.Context(), []kafkax.Message{message(t, msg, 0)})
		if !errors.Is(err, ErrSchema) || kafkax.IsPermanent(err) {
			t.Errorf("%s: err = %v, want non-permanent ErrSchema", name, err)
		}
	}
	if _, err := st.Get(t.Context(), 10, 120); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("row written from after of a DELETE: %v", err)
	}
	if w := ix.Stats().take(); !w.empty() || w.Archived != 0 {
		t.Errorf("stats = %+v", w)
	}
}
