package totals

import (
	"bytes"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// ---------- diffRows：逐笔的规则 ----------

func TestDiffRows(t *testing.T) {
	t.Parallel()
	const (
		pending, succeeded, failed, refunded = 1, 2, 3, 4
		accept, refund                       = 1, 2
	)
	pay := func(status int64, notified bool, amount int64) *PaymentRow {
		return &PaymentRow{OrderID: 77, Status: status, Notified: notified, Amount: amount}
	}
	ord := func(ok bool, disposition, amount int64) *OrderRow {
		return &OrderRow{OrderID: 77, Succeeded: ok, Disposition: disposition, Amount: amount}
	}
	tests := []struct {
		name  string
		pay   *PaymentRow
		ord   *OrderRow
		kind  FindingKind // 空表示不该报
		group Group
	}{
		{"成功且接受", pay(succeeded, true, 100), ord(true, accept, 100), "", 0},
		{"退款且要求退款", pay(refunded, true, 100), ord(true, refund, 100), "", 0},
		{"失败且知悉", pay(failed, true, 100), ord(false, accept, 100), "", 0},
		{"还没出结果，订单没有记录", pay(pending, false, 100), nil, "", 0},
		{"成功，订单还没处理", pay(succeeded, false, 100), nil, "", 0},
		{"失败，订单还没处理", pay(failed, false, 100), nil, "", 0},
		// 在途：订单已经处置，答复没到。
		{"订单接受了，答复没到", pay(succeeded, false, 100), ord(true, accept, 100), "", 0},
		{"订单要求退款，答复没到", pay(succeeded, false, 100), ord(true, refund, 100), "", 0},

		{"支付成功且已答复，订单没有记录", pay(succeeded, true, 100), nil, FindingPaymentOnly, GroupSucceeded},
		{"支付成功且已答复，订单记的是支付失败", pay(succeeded, true, 100), ord(false, accept, 100), FindingPaymentOnly, GroupSucceeded},
		{"已退款，订单没有记录", pay(refunded, true, 100), nil, FindingPaymentOnly, GroupRefunded},
		// 两边各在一组：记支付一侧的那一组。
		{"已退款，订单记的是接受", pay(refunded, true, 100), ord(true, accept, 100), FindingPaymentOnly, GroupRefunded},
		{"成功且已答复，订单记的是要求退款", pay(succeeded, true, 100), ord(true, refund, 100), FindingPaymentOnly, GroupSucceeded},

		{"订单接受了，支付库里没有这一笔", nil, ord(true, accept, 100), FindingOrderOnly, GroupSucceeded},
		{"订单要求退款，支付库里没有这一笔", nil, ord(true, refund, 100), FindingOrderOnly, GroupRefunded},
		{"订单接受了，支付一侧是失败且已答复", pay(failed, true, 100), ord(true, accept, 100), FindingOrderOnly, GroupSucceeded},

		{"成功且接受，金额不同", pay(succeeded, true, 100), ord(true, accept, 130), FindingAmount, GroupSucceeded},
		{"退款，金额不同", pay(refunded, true, 100), ord(true, refund, 90), FindingAmount, GroupRefunded},
		// 在途的不比金额：支付单还不在任何一组里。封口之后合计会看到。
		{"答复没到时金额不同不报", pay(succeeded, false, 100), ord(true, accept, 130), "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const id = 42
			p, o := map[int64]PaymentRow{}, map[int64]OrderRow{}
			if tt.pay != nil {
				p[id] = *tt.pay
			}
			if tt.ord != nil {
				o[id] = *tt.ord
			}
			got := diffRows(p, o)
			if tt.kind == "" {
				if len(got) != 0 {
					t.Fatalf("findings %+v, want none", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("%d findings, want 1", len(got))
			}
			f := got[0]
			if f.PaymentID != id || f.Kind != tt.kind || f.Group != tt.group || f.OrderID() != 77 ||
				(f.Payment != nil) != (tt.pay != nil) || (f.Order != nil) != (tt.ord != nil) {
				t.Errorf("finding %+v, want kind %s group %s", f, tt.kind, tt.group)
			}
		})
	}
}

// ---------- Drill：对着真实的表结构 ----------

func TestDrill(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// 第 0 分钟：一致的和在途的混着三笔有问题的。笔数多一些，让两个订单库都有数据。
	for i := range 12 {
		e.add(0, 100+int64(i), payAcceptedRow, orderAccepted)
	}
	e.add(0, 500, payRefundedRow, orderRefund)
	e.add(0, 600, payUnacked, orderAccepted)
	e.add(0, 700, payPendingRow, orderNone)
	missing := e.add(0, 250, payAcceptedRow, orderNone)
	unasked := e.add(0, 300, payRefundedRow, orderNone)
	wrongAmount := e.add(0, 400, payAcceptedRow, orderAccepted)
	testdb.MustExec(t, e.src.Payment, "UPDATE payments SET amount = 430 WHERE payment_id = ?", wrongAmount)
	// 第 1 分钟：不在这一段里，再怎么不一致也不该出现。
	e.add(1, 999, payAcceptedRow, orderNone)

	// 段里任何一个时刻都指的是这一段。
	got, err := Drill(t.Context(), e.src, e.base.Add(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]FindingKind{missing: FindingPaymentOnly, unasked: FindingPaymentOnly, wrongAmount: FindingAmount}
	if len(got) != len(want) {
		t.Fatalf("%d findings %+v, want %d", len(got), got, len(want))
	}
	for i, f := range got {
		if want[f.PaymentID] != f.Kind {
			t.Errorf("payment %d: kind %q, want %q", f.PaymentID, f.Kind, want[f.PaymentID])
		}
		if i > 0 && got[i-1].PaymentID >= f.PaymentID {
			t.Errorf("findings are not sorted by payment id")
		}
	}
	// 逐笔比合计细。这一段没封口（有没出结果的和答复没到的），合计对没封口的段只看“支付一侧多出来的”：
	// “成功”一组里支付一侧多的那一笔 250 和多记的 30 分，被订单一侧在途的那一笔 600 盖住了（笔数 14 对 14，
	// 金额支付一侧反而少），所以合计这时只报得出退款一组的 300；另外两笔要等这一段封口才显出来。
	// 逐笔的规则把“在途”细到每一笔，现在就三笔都找得到。
	if r := e.collect(0, 1); r.Groups[GroupSucceeded].Mismatch != (Sum{}) || r.Groups[GroupRefunded].Mismatch != (Sum{1, 300}) {
		t.Errorf("totals on the same open bucket: %+v, want only the refunded group to show {1 300}", r.Groups)
	}
	// 封口之后（在途的答复到了、没出结果的失败了）合计也看得到：笔数差 1（250 那一笔），金额差 250 + 30。
	testdb.MustExec(t, e.src.Payment, "UPDATE payments SET notified = 1 WHERE status = 2 AND notified = 0")
	testdb.MustExec(t, e.src.Payment, "UPDATE payments SET status = 3, notified = 1 WHERE status = 1")
	if r := e.collect(0, 1); r.Groups[GroupSucceeded].Mismatch != (Sum{1, 280}) || r.Groups[GroupRefunded].Mismatch != (Sum{1, 300}) {
		t.Errorf("totals once the bucket is closed: %+v, want succeeded {1 280} and refunded {1 300}", r.Groups)
	}

	if got, err := Drill(t.Context(), e.src, e.base.Add(7*time.Minute)); err != nil || len(got) != 0 {
		t.Errorf("empty bucket: findings %+v, error %v", got, err)
	}
	if _, err := Drill(t.Context(), Sources{}, e.base); err == nil {
		t.Error("Drill accepted empty sources")
	}
}

// ---------- Watcher：什么时候下钻 ----------

type drillCounts struct{ found, clean, failed int64 }

func readDrills(t *testing.T) drillCounts {
	t.Helper()
	var c drillCounts
	c.found, _ = metricValue(t, "recon.totals.drilldowns", map[string]string{"outcome": "found"})
	c.clean, _ = metricValue(t, "recon.totals.drilldowns", map[string]string{"outcome": "clean"})
	c.failed, _ = metricValue(t, "recon.totals.drilldowns", map[string]string{"outcome": "error"})
	return c
}

func (c drillCounts) sub(o drillCounts) drillCounts {
	return drillCounts{c.found - o.found, c.clean - o.clean, c.failed - o.failed}
}

// 连续两轮对不上才下钻，之后不再下钻；日志里有是哪一笔。
func TestWatcherDrillsOnceAfterTwoRounds(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	e := newEnv(t)
	e.add(0, 100, payAcceptedRow, orderAccepted)
	missing := e.add(0, 250, payAcceptedRow, orderNone)

	var logs bytes.Buffer
	now := e.base.Add(4 * time.Minute)
	w, err := NewWatcher(Config{Sources: e.src, Now: func() time.Time { return now }, Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	before := readDrills(t)
	w.round(t.Context())
	if d := readDrills(t).sub(before); d != (drillCounts{}) {
		t.Fatalf("drill-downs after the first mismatching check: %+v, want none", d)
	}
	w.round(t.Context())
	if d := readDrills(t).sub(before); d != (drillCounts{found: 1}) {
		t.Fatalf("drill-downs after the second mismatching check: %+v, want one found", d)
	}
	for range 3 {
		w.round(t.Context())
	}
	if d := readDrills(t).sub(before); d != (drillCounts{found: 1}) {
		t.Fatalf("drill-downs after five checks: %+v, want still one", d)
	}
	out := logs.String()
	for _, want := range []string{`"payment_id":` + strconv.FormatInt(missing, 10), `"kind":"payment_only"`, `"group":"succeeded"`, `"differing_payments":1`} {
		if !strings.Contains(out, want) {
			t.Errorf("log does not contain %s:\n%s", want, out)
		}
	}
}

// 中间有一轮对上了，“连续”要重新数。
func TestWatcherDrillStreakResets(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	e := newEnv(t)
	id := e.add(0, 250, payAcceptedRow, orderAccepted)
	setAmount := func(n int64) {
		testdb.MustExec(t, e.src.Payment, "UPDATE payments SET amount = ? WHERE payment_id = ?", n, id)
	}
	now := e.base.Add(4 * time.Minute)
	w, err := NewWatcher(Config{Sources: e.src, Now: func() time.Time { return now }, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	before := readDrills(t)
	setAmount(260)
	w.round(t.Context()) // 对不上，第 1 轮
	setAmount(250)
	w.round(t.Context()) // 对上了
	setAmount(260)
	w.round(t.Context()) // 又对不上，重新数的第 1 轮
	if d := readDrills(t).sub(before); d != (drillCounts{}) {
		t.Fatalf("drill-downs %+v, want none: the two mismatching checks were not consecutive", d)
	}
	w.round(t.Context())
	if d := readDrills(t).sub(before); d != (drillCounts{found: 1}) {
		t.Fatalf("drill-downs %+v, want one found", d)
	}
}

// 很多段同时对不上：一轮最多下钻 maxDrillsPerRound 段，从最早的开始，剩下的留给下一轮。
func TestWatcherDrillsAreCappedPerRound(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	e := newEnv(t)
	const buckets = maxDrillsPerRound + 2
	for m := range buckets {
		e.add(m, 100, payAcceptedRow, orderNone)
	}
	now := e.base.Add(10 * time.Minute)
	w, err := NewWatcher(Config{Sources: e.src, Now: func() time.Time { return now }, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	before := readDrills(t)
	w.round(t.Context())
	w.round(t.Context())
	if d := readDrills(t).sub(before); d != (drillCounts{found: maxDrillsPerRound}) {
		t.Fatalf("drill-downs after the second check: %+v, want %d", d, maxDrillsPerRound)
	}
	w.round(t.Context())
	w.round(t.Context())
	if d := readDrills(t).sub(before); d != (drillCounts{found: buckets}) {
		t.Fatalf("drill-downs after four checks: %+v, want %d (each bucket once)", d, buckets)
	}
}

// 逐笔没有差异记成 clean 并且算做完了；下钻自己失败记成 error，不算做完。
func TestWatcherDrillOutcomes(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	e := newEnv(t)
	e.add(0, 100, payAcceptedRow, orderAccepted)
	w, err := NewWatcher(Config{Sources: e.src, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	before := readDrills(t)
	if !w.drillOne(t.Context(), e.base) {
		t.Error("a drill-down that found nothing was reported as not done")
	}
	if d := readDrills(t).sub(before); d != (drillCounts{clean: 1}) {
		t.Fatalf("drill-downs %+v, want one clean", d)
	}
	if err := e.src.Payment.Close(); err != nil {
		t.Fatal(err)
	}
	if w.drillOne(t.Context(), e.base) {
		t.Error("a failed drill-down was reported as done")
	}
	if d := readDrills(t).sub(before); d != (drillCounts{clean: 1, failed: 1}) {
		t.Fatalf("drill-downs %+v, want one clean and one error", d)
	}
}
