package indexer

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
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

// 多个商家、多个订单，每个订单版本 1..N 的映像（INSERT、UPDATE、READ 混着）打乱顺序、带重复，
// 多个 goroutine 并发应用，最后每一行都等于版本 N 的映像。
func TestConvergesUnderReorderDuplicatesAndConcurrency(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	const (
		merchants = 4 // 落在 4 个不同的 slot、2 个库上
		orders    = 6
		versions  = 12
		workers   = 8
	)
	var msgs []*cdcv1.RowChange
	ops := []cdcv1.Op{cdcv1.Op_OP_INSERT, cdcv1.Op_OP_UPDATE, cdcv1.Op_OP_READ}
	for m := range int64(merchants) {
		for o := range int64(orders) {
			order := (m+1)*1000 + o
			for v := range int64(versions) {
				for range 1 + rand.IntN(3) { // 每个版本 1 到 3 份
					msgs = append(msgs, change(ops[rand.IntN(len(ops))], image(m+1, order, v+1)))
				}
			}
		}
	}
	rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })

	ch := make(chan *cdcv1.RowChange)
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Go(func() {
			for m := range ch {
				// 并发写同一个订单的同一行在生产里不会发生（同一订单在同一分区串行处理），这里故意制造。
				// InnoDB 对并发的 INSERT ... ON DUPLICATE KEY UPDATE 可能判死锁回滚其中一条，
				// 生产里由 kafkax 原地重试，这里照做。
				for {
					_, err := ix.Apply(t.Context(), m)
					if err == nil {
						break
					}
					if !mysqlx.IsDeadlock(err) {
						errs <- err
						return
					}
				}
			}
		})
	}
	for _, m := range msgs {
		ch <- m
	}
	close(ch)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	w := ix.Stats().take()
	if w.Changed+w.Stale != int64(len(msgs)) {
		t.Errorf("changed %d + stale %d != %d messages", w.Changed, w.Stale, len(msgs))
	}
	if w.Changed < merchants*orders || w.Changed > merchants*orders*versions {
		t.Errorf("changed = %d, want within [%d, %d]", w.Changed, merchants*orders, merchants*orders*versions)
	}
	if w.LagCount != int64(len(msgs)) || w.LagMax <= 0 || w.LagSum <= 0 {
		t.Errorf("lag stats = %+v", w)
	}
	for m := range int64(merchants) {
		for o := range int64(orders) {
			order := (m+1)*1000 + o
			got, err := st.Get(t.Context(), m+1, order)
			if err != nil {
				t.Fatal(err)
			}
			if want := wantRow(t, m+1, order, versions); !sameImage(got, want) {
				t.Errorf("order %d: row = %+v, want %+v", order, got, want)
			}
		}
	}
}

// 同一条消息重放：第二次不改行，synced_at 不变。
func TestDuplicateMessageKeepsSyncedAt(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	first := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	ix.now = func() time.Time { return first }
	msg := change(cdcv1.Op_OP_UPDATE, image(2, 50, 3))
	if r, err := ix.Apply(t.Context(), msg); err != nil || r != Changed {
		t.Fatalf("first apply = %v, %v", r, err)
	}
	ix.now = func() time.Time { return first.Add(time.Hour) }
	if r, err := ix.Apply(t.Context(), msg); err != nil || r != Stale {
		t.Fatalf("replay = %v, %v", r, err)
	}
	got, err := st.Get(t.Context(), 2, 50)
	if err != nil {
		t.Fatal(err)
	}
	if !got.SyncedAt.Equal(first) {
		t.Errorf("synced_at = %v, want %v", got.SyncedAt, first)
	}
}

// merchant_id 为 NULL 跳过；DELETE 忽略并计数；别的表、不认识的 op 忽略；都不写库。
func TestSkipsAndIgnores(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	other := change(cdcv1.Op_OP_INSERT, image(3, 60, 1))
	other.Table = "order_items"
	unknownOp := change(cdcv1.Op(99), image(3, 61, 1))
	del := &cdcv1.RowChange{Table: "orders", Op: cdcv1.Op_OP_DELETE, Before: image(3, 62, 4)}
	cases := []struct {
		msg  *cdcv1.RowChange
		want Result
	}{
		{change(cdcv1.Op_OP_INSERT, image(0, 63, 0)), SkippedNoMerchant},
		{change(cdcv1.Op_OP_READ, image(0, 64, 0)), SkippedNoMerchant},
		{del, IgnoredDelete},
		{other, Ignored},
		{unknownOp, Ignored},
	}
	for i, c := range cases {
		if r, err := ix.Apply(t.Context(), c.msg); err != nil || r != c.want {
			t.Errorf("case %d: %v, %v; want %v", i, r, err, c.want)
		}
	}
	for _, order := range []int64{60, 61, 62} {
		if _, err := st.Get(t.Context(), 3, order); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("order %d was written: %v", order, err)
		}
	}
	w := ix.Stats().take()
	if w.SkippedNoMerchant != 2 || w.Deletes != 1 || w.Ignored != 2 || w.Changed+w.Stale != 0 {
		t.Errorf("stats = %+v", w)
	}
}

// 缺少需要的列：返回 ErrSchema（不是 Permanent，kafkax 会原地重试而不是进死信），不写库。
// 反序列化失败才是 Permanent。
func TestHandlerErrors(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	h := ix.Handler(DefaultWriteTimeout)

	img := image(5, 70, 2)
	delete(img, "total_amount")
	value, err := proto.Marshal(change(cdcv1.Op_OP_UPDATE, img))
	if err != nil {
		t.Fatal(err)
	}
	err = h(t.Context(), kafkax.Message{Value: value})
	if !errors.Is(err, ErrSchema) || kafkax.IsPermanent(err) {
		t.Fatalf("missing column: err = %v, want non-permanent ErrSchema", err)
	}
	if _, err := st.Get(t.Context(), 5, 70); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("row written despite schema error: %v", err)
	}

	err = h(t.Context(), kafkax.Message{Value: []byte{0xff, 0xff, 0xff}})
	if !kafkax.IsPermanent(err) {
		t.Fatalf("garbage: err = %v, want permanent", err)
	}

	value, err = proto.Marshal(change(cdcv1.Op_OP_INSERT, image(5, 71, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if err := h(t.Context(), kafkax.Message{Value: value}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(t.Context(), 5, 71); err != nil {
		t.Fatal(err)
	}
}
