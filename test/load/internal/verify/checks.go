package verify

import (
	"cmp"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
)

// CheckLogCorrections 是“库存服务日志里没有出现过修正”这条检查的编号（设计里没有编号，排在 18 之后）。
const CheckLogCorrections = 19

// CheckMerchantIndex 是“商家索引与订单表一致”这条检查的编号，只在给了索引库时执行。
const CheckMerchantIndex = 20

// Violation 是一条不满足的检查。
type Violation struct {
	Check   int
	Subject string // 订单号、SKU、支付单号或 topic
	Detail  string
}

func (v Violation) String() string {
	return fmt.Sprintf("[%d] %s: %s", v.Check, v.Subject, v.Detail)
}

// Counts 是只计数、不断言的统计（第 15、16 条等）。
type Counts struct {
	// Accepted 是提交得到 201/202 的不同订单号数。
	Accepted int
	// AcceptedInDB 是其中在订单库里的数量。
	AcceptedInDB int
	// Reaped 是第 15 条的“受理了但库里没有、闸门里也没有占用”的订单号数。
	Reaped int
	// ReapedResubmitted 是 Reaped 中流程后来用同一个幂等键拿到了另一个订单的数量。
	ReapedResubmitted int
	// Unknown 是第 16 条：结果未知的请求数，按操作分。
	Unknown map[ledger.Op]int
	// HTTP5xx 是 5xx 的请求数，按操作分。和 unknown 一样当作“可能执行了”，不做断言。
	HTTP5xx map[ledger.Op]int
	// GaveUp 是提交重试用尽、客户端一直没拿到订单号、且至少一次结果未知或 5xx 的流程数。
	GaveUp int
	// GaveUpFound 是其中按 (user_id, 幂等键) 在订单库里找到订单的数量，按订单状态分。
	GaveUpFound map[string]int
	// OrdersByStatus 是订单库里各状态的订单数。
	OrdersByStatus map[string]int
	// OrdersWithoutFlow 是订单库里按 (user_id, 幂等键) 找不到任何账本流程的订单数。
	OrdersWithoutFlow int
	// Corrections、Capped 是日志里闸门对账修正、截住的次数。
	Corrections int
	Capped      int
	DLQ         int
	// MerchantIndex 是第 20 条的统计；没有索引库时为 nil（跳过）。
	MerchantIndex *MerchantIndexCounts
}

// MerchantIndexCounts 是第 20 条的统计。
type MerchantIndexCounts struct {
	Orders    int // 订单侧有商家的订单数
	Rows      int // 索引侧的行数
	Missing   int // 订单侧有、索引侧没有
	Extra     int // 索引侧有、订单侧没有（含同一订单出现多行的多余行）
	Mismatch  int // 两边都有但 status、total_amount、版本、merchant_id 不等
	Misplaced int // 索引行所在的 slot 不是按 merchant_id 算出的 slot
}

// Result 是一次对账的结果。
type Result struct {
	Violations []Violation
	Counts     Counts
	// Notes 是不算违规但需要人看的信息（死信内容、被截住的偏差等）。
	Notes []string
}

// Failed 报告是否有违规。
func (r *Result) Failed() bool { return len(r.Violations) > 0 }

// FailedChecks 返回有违规的检查编号，升序。
func (r *Result) FailedChecks() []int {
	set := map[int]bool{}
	for _, v := range r.Violations {
		set[v.Check] = true
	}
	return slices.Sorted(maps.Keys(set))
}

type checker struct {
	s   *Snapshot
	led []ledger.Entry
	res Result
	// archiveInFlight 为 true 时，热库冷库同时有同一个订单只记 note（归档任务正在跑）。
	archiveInFlight bool
}

func (c *checker) fail(check int, subject, format string, args ...any) {
	c.res.Violations = append(c.res.Violations, Violation{Check: check, Subject: subject, Detail: fmt.Sprintf(format, args...)})
}

func (c *checker) note(format string, args ...any) {
	c.res.Notes = append(c.res.Notes, fmt.Sprintf(format, args...))
}

func orderSubj(id int64) string { return "order " + strconv.FormatInt(id, 10) }
func skuSubj(id int64) string   { return "sku " + strconv.FormatInt(id, 10) }

// Check 逐条执行 7.3.3 的检查，不在第一条失败时停下。
func Check(in Input) *Result {
	c := &checker{s: in.Snapshot, led: in.Ledger, archiveInFlight: in.ArchiveInFlight}
	c.res.Counts.Unknown = map[ledger.Op]int{}
	c.res.Counts.HTTP5xx = map[ledger.Op]int{}
	c.res.Counts.GaveUpFound = map[string]int{}
	c.res.Counts.OrdersByStatus = map[string]int{}
	for _, o := range c.s.Orders {
		c.res.Counts.OrdersByStatus[OrderStatusName(o.Status)]++
	}

	c.stockInternal()  // 1–3
	c.orderStock()     // 4–6
	c.orderPayment()   // 7–10
	c.gateStock()      // 11–12
	c.ledgerServer()   // 13–16
	c.infrastructure() // 17–18
	c.logs()           // 19
	c.merchantIndex()  // 20
	c.archive()        // 21

	slices.SortStableFunc(c.res.Violations, func(a, b Violation) int {
		return cmp.Or(cmp.Compare(a.Check, b.Check), cmp.Compare(a.Subject, b.Subject))
	})
	return &c.res
}

// paidQty 返回 PAID 订单里各 SKU 的数量之和。
func (c *checker) paidQty() map[int64]int64 {
	out := map[int64]int64{}
	for _, o := range c.s.Orders {
		if o.Status != OrderPaid {
			continue
		}
		for sku, q := range o.Items {
			out[sku] += q
		}
	}
	return out
}

func sortedKeys[K cmp.Ordered, V any](m map[K]V) []K { return slices.Sorted(maps.Keys(m)) }

// ---------- 1–3 库存内部 ----------

func (c *checker) stockInternal() {
	for _, sku := range sortedKeys(c.s.Initial) {
		st, ok := c.s.Stock[sku]
		if !ok {
			c.fail(1, skuSubj(sku), "listed in -initial but not in sku_stock")
			continue
		}
		if total := st.Available + st.Reserved + st.Sold; total != c.s.Initial[sku] {
			c.fail(1, skuSubj(sku), "available %d + reserved %d + sold %d = %d, initial %d",
				st.Available, st.Reserved, st.Sold, total, c.s.Initial[sku])
		}
	}
	for _, sku := range sortedKeys(c.s.Stock) {
		st := c.s.Stock[sku]
		if _, ok := c.s.Initial[sku]; !ok {
			c.fail(1, skuSubj(sku), "in sku_stock but no initial stock given (-initial)")
		}
		if st.Reserved != 0 {
			c.fail(2, skuSubj(sku), "reserved = %d, want 0", st.Reserved)
		}
		if st.Available < 0 || st.Reserved < 0 || st.Sold < 0 {
			c.fail(3, skuSubj(sku), "negative quantity: available %d, reserved %d, sold %d", st.Available, st.Reserved, st.Sold)
		}
	}
}

// ---------- 4–6 订单与库存 ----------

func (c *checker) orderStock() {
	paid := c.paidQty()
	for _, sku := range sortedKeys(c.s.Stock) {
		if got, want := c.s.Stock[sku].Sold, paid[sku]; got != want {
			c.fail(4, skuSubj(sku), "sold = %d, PAID orders hold %d", got, want)
		}
	}
	for _, sku := range sortedKeys(paid) {
		if _, ok := c.s.Stock[sku]; !ok {
			c.fail(4, skuSubj(sku), "PAID orders hold %d of a sku missing from sku_stock", paid[sku])
		}
	}

	for _, id := range sortedKeys(c.s.Orders) {
		o := c.s.Orders[id]
		r, has := c.s.Reservations[id]
		rs := "none"
		if has {
			rs = ReservationStatusName(r.Status)
		}
		switch o.Status {
		case OrderPaid:
			if !has || r.Status != ReservationConfirmed {
				c.fail(5, orderSubj(id), "order PAID, reservation %s, want CONFIRMED", rs)
			}
		case OrderCancelled, OrderClosed:
			if !has || r.Status != ReservationReleased {
				c.fail(5, orderSubj(id), "order %s, reservation %s, want RELEASED", OrderStatusName(o.Status), rs)
			}
		case OrderFailed:
			if has && r.Status != ReservationReleased {
				c.fail(5, orderSubj(id), "order FAILED, reservation %s, want none or RELEASED", rs)
			}
		default:
			c.fail(5, orderSubj(id), "order is %s after the system went quiet (reservation %s)", OrderStatusName(o.Status), rs)
		}
		// 已确认或预占中的预占记录，其商品和数量要与订单一致（释放的记录可能是空回滚标记，没有明细）。
		if has && r.Status != ReservationReleased && !maps.Equal(r.Items, o.Items) {
			c.fail(5, orderSubj(id), "reservation items %v differ from order items %v", r.Items, o.Items)
		}
	}

	for _, id := range sortedKeys(c.s.Reservations) {
		r := c.s.Reservations[id]
		if r.Status == ReservationReleased {
			continue
		}
		if _, ok := c.s.Orders[id]; !ok {
			c.fail(6, orderSubj(id), "reservation %s has no order", ReservationStatusName(r.Status))
		}
	}
}

// ---------- 7–10 订单与支付 ----------

func (c *checker) orderPayment() {
	byOrder := map[int64][]Payment{}
	for _, p := range c.s.Payments {
		byOrder[p.OrderID] = append(byOrder[p.OrderID], p)
	}

	for _, id := range sortedKeys(c.s.Orders) {
		o := c.s.Orders[id]
		ps := byOrder[id]
		if o.Status == OrderPaid {
			var ok []Payment
			for _, p := range ps {
				if p.Status == PaymentSucceeded {
					ok = append(ok, p)
				}
			}
			if len(ok) != 1 {
				c.fail(7, orderSubj(id), "PAID order has %d succeeded payments (of %d)", len(ok), len(ps))
				continue
			}
			p := ok[0]
			if p.Amount != o.TotalAmount || p.Currency != o.Currency {
				c.fail(7, orderSubj(id), "payment %d is %d %s, order is %d %s", p.PaymentID, p.Amount, p.Currency, o.TotalAmount, o.Currency)
			}
			if o.PaymentID != p.PaymentID {
				c.fail(7, orderSubj(id), "orders.payment_id = %d, succeeded payment is %d", o.PaymentID, p.PaymentID)
			}
			continue
		}
		for _, p := range ps {
			if p.Status != PaymentFailed && p.Status != PaymentRefunded {
				c.fail(9, orderSubj(id), "order %s has payment %d in %s, want FAILED or REFUNDED",
					OrderStatusName(o.Status), p.PaymentID, PaymentStatusName(p.Status))
			}
		}
	}

	for _, p := range c.s.Payments {
		o, ok := c.s.Orders[p.OrderID]
		switch {
		case p.Status == PaymentSucceeded && !ok:
			c.fail(8, "payment "+strconv.FormatInt(p.PaymentID, 10), "succeeded, order %d does not exist", p.OrderID)
		case p.Status == PaymentSucceeded && o.Status != OrderPaid:
			c.fail(8, "payment "+strconv.FormatInt(p.PaymentID, 10), "succeeded, order %d is %s", p.OrderID, OrderStatusName(o.Status))
		case !ok && p.Status != PaymentFailed && p.Status != PaymentRefunded:
			c.fail(9, "payment "+strconv.FormatInt(p.PaymentID, 10), "order %d does not exist, payment is %s", p.OrderID, PaymentStatusName(p.Status))
		}
		if p.Status != PaymentPending && !p.Notified {
			c.fail(10, "payment "+strconv.FormatInt(p.PaymentID, 10), "%s but notified = 0 (order %d)", PaymentStatusName(p.Status), p.OrderID)
		}
	}
}

// ---------- 11–12 闸门与库存 ----------

func (c *checker) gateStock() {
	for _, sku := range sortedKeys(c.s.Gate) {
		g := c.s.Gate[sku]
		st, ok := c.s.Stock[sku]
		if !ok {
			c.fail(11, skuSubj(sku), "loaded in the gate but not in sku_stock")
			continue
		}
		if g.Avail+g.Held != st.Available+st.Reserved {
			c.fail(11, skuSubj(sku), "gate avail %d + held %d = %d, mysql available %d + reserved %d = %d",
				g.Avail, g.Held, g.Avail+g.Held, st.Available, st.Reserved, st.Available+st.Reserved)
		}
		if g.Held != 0 {
			c.fail(11, skuSubj(sku), "gate held = %d, want 0", g.Held)
		}
	}
	for _, sku := range sortedKeys(c.s.Stock) {
		if _, ok := c.s.Gate[sku]; !ok {
			c.note("sku %d is not loaded in the gate, checks 11 and 12 skipped for it", sku)
		}
	}
	for _, id := range sortedKeys(c.s.Holds) {
		c.fail(11, orderSubj(id), "hold still exists (%s)", c.holdDesc(id))
	}
	for _, id := range c.s.Unclaimed {
		c.fail(11, orderSubj(id), "still in gate:holds (%s)", c.holdDesc(id))
	}
	for _, id := range c.s.Claimed {
		c.fail(11, orderSubj(id), "still in gate:claimed (%s)", c.holdDesc(id))
	}

	// 12：限购计数 = 该用户在该 SKU 上 PAID 订单的数量之和（静止后没有占用中的）。
	want := map[int64]map[int64]int64{}
	for _, o := range c.s.Orders {
		if o.Status != OrderPaid {
			continue
		}
		for sku, q := range o.Items {
			if want[sku] == nil {
				want[sku] = map[int64]int64{}
			}
			want[sku][o.UserID] += q
		}
	}
	for _, sku := range sortedKeys(c.s.Gate) {
		got := c.s.Bought[sku]
		users := map[int64]bool{}
		for u := range got {
			users[u] = true
		}
		for u := range want[sku] {
			users[u] = true
		}
		for _, u := range sortedKeys(users) {
			if g, w := got[u], want[sku][u]; g != w {
				c.fail(12, skuSubj(sku), "user %d bought counter %d, PAID orders hold %d", u, g, w)
			}
		}
	}
}

func (c *checker) holdDesc(id int64) string {
	if o, ok := c.s.Orders[id]; ok {
		return "order is " + OrderStatusName(o.Status)
	}
	return "no order row"
}

// ---------- 13–16 客户端账本与服务端 ----------

// flow 是账本里同一个幂等键的所有行。
type flow struct {
	key      string
	userID   int64
	orderIDs []int64 // 提交成功拿到过的订单号，按出现顺序去重
	gotOrder bool
	unsure   bool // 有提交结果未知或 5xx
}

func parseOrderID(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	id, err := strconv.ParseInt(s, 10, 64)
	return id, err == nil && id > 0
}

func (c *checker) ledgerServer() {
	flows := map[string]*flow{}
	var order []string
	accepted := map[int64]string{} // 订单号 → 流程的幂等键
	var acceptedOrder []int64

	for i, e := range c.led {
		f := flows[e.IdempotencyKey]
		if f == nil {
			f = &flow{key: e.IdempotencyKey, userID: e.UserID}
			flows[e.IdempotencyKey] = f
			order = append(order, e.IdempotencyKey)
		}
		if e.Outcome == ledger.OutcomeUnknown {
			c.res.Counts.Unknown[e.Op]++
		}
		if e.Outcome == ledger.OutcomeHTTPError && e.HTTPStatus >= 500 {
			c.res.Counts.HTTP5xx[e.Op]++
		}
		id, hasID := parseOrderID(e.OrderID)
		switch e.Op {
		case ledger.OpSubmit:
			switch {
			case e.Outcome == ledger.OutcomeOK && hasID:
				f.gotOrder = true
				if !slices.Contains(f.orderIDs, id) {
					f.orderIDs = append(f.orderIDs, id)
				}
				if e.HTTPStatus == http.StatusAccepted || e.HTTPStatus == http.StatusCreated {
					if _, seen := accepted[id]; !seen {
						accepted[id] = e.IdempotencyKey
						acceptedOrder = append(acceptedOrder, id)
					}
				}
			case e.Outcome == ledger.OutcomeUnknown,
				e.Outcome == ledger.OutcomeHTTPError && e.HTTPStatus >= 500:
				f.unsure = true
			case e.Outcome == ledger.OutcomeOK:
				c.fail(15, "ledger line "+strconv.Itoa(i+1), "submit ok without a valid order id %q", e.OrderID)
			}
		case ledger.OpCancel:
			// 14：取消接口返回 200 的订单，库里是 CANCELLED。
			if e.Outcome == ledger.OutcomeOK && e.HTTPStatus == http.StatusOK && hasID {
				o, ok := c.s.Orders[id]
				switch {
				case !ok:
					c.fail(14, orderSubj(id), "cancel returned 200, order not in the order dbs")
				case o.Status != OrderCancelled:
					c.fail(14, orderSubj(id), "cancel returned 200, order is %s", OrderStatusName(o.Status))
				}
			}
		case ledger.OpPollFinal, ledger.OpPay:
		}
		// 13：客户端看到过 paid 的订单，库里是 PAID。
		if e.OrderStatus == "paid" && hasID {
			o, ok := c.s.Orders[id]
			switch {
			case !ok:
				c.fail(13, orderSubj(id), "client saw paid (%s line %d), order not in the order dbs", e.Op, i+1)
			case o.Status != OrderPaid:
				c.fail(13, orderSubj(id), "client saw paid (%s line %d), order is %s", e.Op, i+1, OrderStatusName(o.Status))
			}
		}
	}

	// 15：提交返回 202 的订单，库里要么有，要么没有且闸门里没有它的占用。
	c.res.Counts.Accepted = len(acceptedOrder)
	for _, id := range acceptedOrder {
		f := flows[accepted[id]]
		if o, ok := c.s.Orders[id]; ok {
			c.res.Counts.AcceptedInDB++
			if o.UserID != f.userID || o.IdempotencyKey != f.key {
				c.fail(15, orderSubj(id), "accepted for user %d key %q, the order dbs have user %d key %q",
					f.userID, f.key, o.UserID, o.IdempotencyKey)
			}
			continue
		}
		if c.s.Holds[id] {
			c.fail(15, orderSubj(id), "accepted (202), not in the order dbs, hold still in the gate")
			continue
		}
		c.res.Counts.Reaped++
		if oid, ok := c.s.OrdersByIdem[IdemRef{UserID: f.userID, Key: f.key}]; ok && oid != id {
			c.res.Counts.ReapedResubmitted++
		}
	}

	// 提交重试用尽、没拿到订单号的流程：服务端可能有订单，按 (user_id, 幂等键) 去找。
	for _, k := range order {
		f := flows[k]
		if f.gotOrder || !f.unsure {
			continue
		}
		c.res.Counts.GaveUp++
		if oid, ok := c.s.OrdersByIdem[IdemRef{UserID: f.userID, Key: f.key}]; ok {
			c.res.Counts.GaveUpFound[OrderStatusName(c.s.Orders[oid].Status)]++
		}
	}

	for _, o := range c.s.Orders {
		if _, ok := flows[o.IdempotencyKey]; !ok {
			c.res.Counts.OrdersWithoutFlow++
		}
	}
	if n := c.res.Counts.OrdersWithoutFlow; n > 0 && len(c.led) > 0 {
		c.note("%d orders in the order dbs have an idempotency key that is not in the ledger", n)
	}
}

// ---------- 17–18 基础设施 ----------

func (c *checker) infrastructure() {
	for _, svc := range sortedKeys(c.s.OutboxPending) {
		if n := c.s.OutboxPending[svc]; n != 0 {
			c.fail(17, svc+" outbox", "%d unpublished rows", n)
		}
	}
	c.res.Counts.DLQ = len(c.s.DLQ)
	for _, m := range c.s.DLQ {
		c.fail(18, m.Topic, "partition %d offset %d key %s group %s attempts %s error %q body %s",
			m.Partition, m.Offset, m.Key, m.Headers["dlq-group"], m.Headers["dlq-attempts"], m.Headers["dlq-error"], m.Body)
	}
}

// ---------- 19 日志 ----------

func (c *checker) logs() {
	c.res.Counts.Corrections = len(c.s.Corrections)
	c.res.Counts.Capped = len(c.s.Capped)
	for _, line := range c.s.Corrections {
		c.fail(CheckLogCorrections, "inventory log", "gate reconciliation corrected a deviation: %s", line)
	}
	for _, line := range c.s.Capped {
		c.note("gate reconciliation saw a persistent deviation (capped, not corrected): %s", line)
	}
}

// ---------- 20 商家索引与订单表 ----------

// indexExamples 是第 20 条每一类最多列出的例子数。压测后出问题时可能有成千上万行，全列出来没法看。
const indexExamples = 5

// merchantIndexIssues 收集第 20 条一类问题：总数和前几个例子。
type merchantIndexIssues struct {
	n        int
	examples []string
}

func (m *merchantIndexIssues) add(format string, args ...any) {
	m.n++
	if len(m.examples) < indexExamples {
		m.examples = append(m.examples, fmt.Sprintf(format, args...))
	}
}

// merchantIndex 对比订单表里有商家的订单和索引表。没有索引库（Snapshot.MerchantIndex 为 nil）时跳过。
//
// 索引是最终一致的，所以只有在 CDC 和 orderindex 追平之后才有意义。
func (c *checker) merchantIndex() {
	d := c.s.MerchantIndex
	if d == nil {
		return
	}
	r, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil { // 布局是编译期常量，走不到这里
		c.fail(CheckMerchantIndex, "merchant index", "build shard router: %v", err)
		return
	}
	// 订单侧是热库并上冷库（归档后订单只在冷库里，索引行保留）。同一订单两边都有时取热库的。
	orders := make(map[int64]MerchantOrderRow, len(d.Orders)+len(d.ColdOrders))
	for _, o := range d.Orders {
		orders[o.OrderID] = o
	}
	for _, o := range d.ColdOrders {
		if _, ok := orders[o.OrderID]; !ok {
			orders[o.OrderID] = o
		}
	}
	cnt := &MerchantIndexCounts{Orders: len(orders), Rows: len(d.Index)}
	c.res.Counts.MerchantIndex = cnt

	var missing, extra, mismatch, misplaced merchantIndexIssues
	seen := make(map[int64]bool, len(d.Index))
	for _, row := range slices.SortedFunc(slices.Values(d.Index), func(a, b MerchantOrderRow) int {
		return cmp.Compare(a.OrderID, b.OrderID)
	}) {
		if want := r.ForMerchant(row.MerchantID); row.Slot != want {
			misplaced.add("%s in merchant_orders_%d, merchant %d belongs to slot %d", orderSubj(row.OrderID), row.Slot, row.MerchantID, want)
		}
		o, ok := orders[row.OrderID]
		switch {
		case !ok:
			extra.add("%s (merchant %d, status %s) not in the order dbs with a merchant", orderSubj(row.OrderID), row.MerchantID, OrderStatusName(row.Status))
			continue
		case seen[row.OrderID]:
			extra.add("%s has more than one index row (merchant %d in merchant_orders_%d)", orderSubj(row.OrderID), row.MerchantID, row.Slot)
			continue
		}
		seen[row.OrderID] = true
		if o.MerchantID != row.MerchantID || o.Status != row.Status || o.TotalAmount != row.TotalAmount || o.Version != row.Version {
			mismatch.add("%s order {merchant %d %s total %d version %d}, index {merchant %d %s total %d src_version %d}",
				orderSubj(o.OrderID), o.MerchantID, OrderStatusName(o.Status), o.TotalAmount, o.Version,
				row.MerchantID, OrderStatusName(row.Status), row.TotalAmount, row.Version)
		}
	}
	for _, o := range slices.SortedFunc(maps.Values(orders), func(a, b MerchantOrderRow) int {
		return cmp.Compare(a.OrderID, b.OrderID)
	}) {
		if !seen[o.OrderID] {
			missing.add("%s (merchant %d, %s, version %d) not in the index", orderSubj(o.OrderID), o.MerchantID, OrderStatusName(o.Status), o.Version)
		}
	}
	cnt.Missing, cnt.Extra, cnt.Mismatch, cnt.Misplaced = missing.n, extra.n, mismatch.n, misplaced.n

	for _, k := range []struct {
		what   string
		issues *merchantIndexIssues
	}{
		{"missing from the index", &missing},
		{"extra in the index", &extra},
		{"differ from the order table", &mismatch},
		{"in the wrong slot", &misplaced},
	} {
		if k.issues.n > 0 {
			c.fail(CheckMerchantIndex, "merchant index", "%d rows %s, e.g. %s", k.issues.n, k.what, strings.Join(k.issues.examples, "; "))
		}
	}
}
