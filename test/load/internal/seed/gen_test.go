package seed

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

func testParams() Params {
	return Params{
		Total: 20_000, Seed: 42,
		From: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2024, 1, 11, 0, 0, 0, 0, time.UTC),
		Users: 5_000, UserS: 1.05, UserV: 10,
		Merchants: 50, MerchantS: 1.3, MerchantV: 1,
		DBs: 2,
	}
}

func TestValidate(t *testing.T) {
	p := testParams()
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*Params){
		"zero total":      func(p *Params) { p.Total = 0 },
		"before epoch":    func(p *Params) { p.From = idgen.Epoch.Add(-day) },
		"not midnight":    func(p *Params) { p.To = p.To.Add(time.Hour) },
		"empty range":     func(p *Params) { p.To = p.From },
		"zipf s":          func(p *Params) { p.UserS = 1 },
		"zipf v":          func(p *Params) { p.MerchantV = 0.5 },
		"dbs not pow2":    func(p *Params) { p.DBs = 3 },
		"zero merchants":  func(p *Params) { p.Merchants = 0 },
		"too many users":  func(p *Params) { p.Users = 1 << 40 },
		"dbs above slots": func(p *Params) { p.DBs = 16 },
	} {
		q := testParams()
		mut(&q)
		if q.Validate() == nil {
			t.Errorf("%s: Validate passed", name)
		}
	}
}

func TestFingerprintChangesWithParams(t *testing.T) {
	p := testParams()
	q := p
	q.Seed++
	r := p
	r.To = r.To.Add(day)
	s := p
	s.Total++
	fps := []string{p.Fingerprint(), q.Fingerprint(), r.Fingerprint(), s.Fingerprint()}
	slices.Sort(fps)
	if len(slices.Compact(fps)) != 4 {
		t.Errorf("fingerprints collide: %v", fps)
	}
}

// 各天之和、各块之和都等于总数，且不依赖天数能否整除。
func TestChunkCountsSumToTotal(t *testing.T) {
	for _, total := range []int64{1, 7, 9_999, 20_000} {
		p := testParams()
		p.Total = total
		p.To = p.From.Add(7 * day)
		var byDay, byChunk int64
		for d := range p.Days() {
			byDay += p.DayTotal(d)
			for slot := range sharding.OrderSlots {
				byChunk += int64(len(p.ChunkUsers(slot, d)))
			}
		}
		if byDay != total || byChunk != total {
			t.Errorf("total %d: days sum to %d, chunks to %d", total, byDay, byChunk)
		}
	}
}

func TestGenerateChunkIsDeterministic(t *testing.T) {
	p := testParams()
	a, err := p.GenerateChunk(3, 4)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.GenerateChunk(3, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) == 0 || !reflect.DeepEqual(a, b) {
		t.Fatalf("two generations differ (len %d, %d)", len(a), len(b))
	}
	q := p
	q.Seed++
	c, err := q.GenerateChunk(3, 4)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(a, c) {
		t.Error("a different seed generated the same chunk")
	}
}

// 每个块里的每一行都和真实流程写出的行自洽。
func TestGeneratedRowsAreConsistent(t *testing.T) {
	p := testParams()
	statuses := map[int8]int{}
	seen := map[int64]bool{}
	payments := map[int64]bool{}
	var total int
	for d := range p.Days() {
		dayStart, dayEnd := p.DayStart(d), p.DayStart(d+1)
		for slot := range sharding.OrderSlots {
			orders, err := p.GenerateChunk(slot, d)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.IsSortedFunc(orders, func(a, b Order) int { return int(a.OrderID - b.OrderID) }) {
				t.Errorf("slot %d day %d: order ids not ascending", slot, d)
			}
			for _, o := range orders {
				total++
				checkOrder(t, o, slot, dayStart, dayEnd)
				statuses[o.Status]++
				if seen[o.OrderID] {
					t.Errorf("duplicate order id %d", o.OrderID)
				}
				seen[o.OrderID] = true
				if o.PaymentID != 0 {
					if payments[o.PaymentID] {
						t.Errorf("duplicate payment id %d", o.PaymentID)
					}
					payments[o.PaymentID] = true
				}
			}
		}
	}
	if total != int(p.Total) {
		t.Fatalf("generated %d orders, want %d", total, p.Total)
	}
	// 比例大致符合 70/20/8/2。
	for st, want := range map[int8]float64{statusPaid: 0.70, statusClosed: 0.20, statusCancelled: 0.08, statusFailed: 0.02} {
		got := float64(statuses[st]) / float64(total)
		if got < want*0.85 || got > want*1.15 {
			t.Errorf("status %d: ratio %.3f, want about %.2f", st, got, want)
		}
	}
}

func checkOrder(t *testing.T, o Order, slot int, dayStart, dayEnd time.Time) {
	t.Helper()
	fail := func(format string, args ...any) {
		t.Helper()
		t.Errorf("order %d (status %d): "+format, append([]any{o.OrderID, o.Status}, args...)...)
	}
	if SlotOf(o.UserID) != slot || int(idgen.Gene(o.OrderID))&(sharding.OrderSlots-1) != slot {
		fail("user %d / order gene not in slot %d", o.UserID, slot)
	}
	if o.UserID < UserBase {
		fail("user %d below the reserved range", o.UserID)
	}
	if !idgen.Time(o.OrderID).Equal(o.CreatedAt) {
		fail("created_at %s != order id time %s", o.CreatedAt, idgen.Time(o.OrderID))
	}
	if o.CreatedAt.Before(dayStart) || !o.CreatedAt.Before(dayEnd) {
		fail("created_at %s outside the chunk's day", o.CreatedAt)
	}
	if !o.ExpireAt.Equal(o.CreatedAt.Add(PaymentTimeout)) {
		fail("expire_at %s", o.ExpireAt)
	}
	if !o.UpdatedAt.After(o.CreatedAt) {
		fail("updated_at %s not after created_at", o.UpdatedAt)
	}
	if o.Currency != "CNY" || len(o.RequestHash) != 32 || o.IdempotencyKey == "" || len(o.IdempotencyKey) > 64 {
		fail("bad currency/hash/key")
	}
	if !slices.Equal(o.RequestHash, requestHash(o.Items)) {
		fail("request hash does not match the items")
	}
	if len(o.Items) < 1 || len(o.Items) > SKUsPerMerchant {
		fail("%d items", len(o.Items))
	}
	var sum int64
	merchants := map[int64]bool{}
	for i, it := range o.Items {
		if i > 0 && it.SKUID <= o.Items[i-1].SKUID {
			fail("items not sorted by sku")
		}
		if it.Quantity <= 0 {
			fail("quantity %d", it.Quantity)
		}
		merchants[MerchantOfSKU(it.SKUID)] = true
		sum += it.UnitPrice * int64(it.Quantity)
	}
	if len(merchants) != 1 {
		fail("items from %d merchants", len(merchants))
	}
	if sum != o.TotalAmount {
		fail("items sum to %d, total_amount %d", sum, o.TotalAmount)
	}
	paid := o.Status == statusPaid
	if paid != (o.PaymentID != 0) || paid != !o.PaidAt.IsZero() {
		fail("payment_id %d paid_at %s", o.PaymentID, o.PaidAt)
	}
	if paid && (!o.PaidAt.Equal(o.UpdatedAt) || !o.PaidAt.Before(o.ExpireAt) || idgen.Gene(o.PaymentID) != idgen.GeneOf(o.UserID)) {
		fail("paid_at %s updated_at %s expire_at %s payment %d", o.PaidAt, o.UpdatedAt, o.ExpireAt, o.PaymentID)
	}

	switch o.Status {
	case statusFailed:
		if o.Version != 1 || o.MerchantID != 0 || o.TotalAmount != 0 || o.FailReason == "" {
			fail("failed row: version %d merchant %d total %d reason %q", o.Version, o.MerchantID, o.TotalAmount, o.FailReason)
		}
		for _, it := range o.Items {
			if it.UnitPrice != 0 {
				fail("failed order has unit price %d", it.UnitPrice)
			}
		}
		if o.FailReason == errx.ReasonCreateTimeout && o.UpdatedAt.Sub(o.CreatedAt) < createTimeout {
			fail("create timeout after only %s", o.UpdatedAt.Sub(o.CreatedAt))
		}
	case statusPaid, statusCancelled, statusClosed:
		if o.Version != 2 || o.FailReason != "" || o.TotalAmount <= 0 {
			fail("version %d reason %q total %d", o.Version, o.FailReason, o.TotalAmount)
		}
		if o.MerchantID != MerchantOfSKU(o.Items[0].SKUID) {
			fail("merchant %d does not own sku %d", o.MerchantID, o.Items[0].SKUID)
		}
		for _, it := range o.Items {
			if it.UnitPrice != UnitPrice(it.SKUID) || it.UnitPrice <= 0 {
				fail("unit price %d for sku %d", it.UnitPrice, it.SKUID)
			}
		}
		if o.Status == statusClosed && !o.UpdatedAt.After(o.ExpireAt) {
			fail("closed at %s, not after expire_at %s", o.UpdatedAt, o.ExpireAt)
		}
		if o.Status == statusCancelled && !o.UpdatedAt.Before(o.ExpireAt) {
			fail("cancelled at %s, after expire_at %s", o.UpdatedAt, o.ExpireAt)
		}
	default:
		fail("unexpected status")
	}
}

// 用户按 Zipf 抽取：最重的用户明显多于平均。
func TestUsersAreSkewed(t *testing.T) {
	p := testParams()
	counts := map[int64]int{}
	for d := range p.Days() {
		for _, u := range p.dayUsers(d) {
			counts[u]++
		}
	}
	top := slices.Max(slices.Collect(func(yield func(int) bool) {
		for _, c := range counts {
			if !yield(c) {
				return
			}
		}
	}))
	avg := float64(p.Total) / float64(len(counts))
	if float64(top) < 20*avg {
		t.Errorf("top user has %d orders, average %.1f: not skewed", top, avg)
	}
}

// 每毫秒序号用完时假时钟往前拨，不真的睡，订单号仍然唯一递增。
func TestOrderIDsSurviveSequenceOverflow(t *testing.T) {
	c := &fakeClock{t: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
	g, err := idgen.New(OrderWorkerID, idgen.WithClock(c.now, c.sleep))
	if err != nil {
		t.Fatal(err)
	}
	var last int64
	for range 3000 {
		id, err := g.Next(UserBase)
		if err != nil {
			t.Fatal(err)
		}
		if id <= last {
			t.Fatalf("id %d not above %d", id, last)
		}
		last = id
	}
	if got := idgen.Time(last).Sub(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)); got < 2*time.Millisecond {
		t.Errorf("clock advanced by %s, want at least 2ms", got)
	}
}
