package changes

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// 全局的 MeterProvider 是进程级的，只在这里设一次。计数是包级的、各用例共用，所以用例不并行，计数只看差值。
var metricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	otel.SetMeterProvider(mp)
	code := m.Run()
	_ = mp.Shutdown(context.Background())
	os.Exit(code)
}

func metricValue(t *testing.T, name string, want map[string]string) (v int64, found bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	match := func(set attribute.Set) bool {
		for k, val := range want {
			if got, ok := set.Value(attribute.Key(k)); !ok || got.String() != val {
				return false
			}
		}
		return true
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						v, found = v+dp.Value, true
					}
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						v, found = v+dp.Value, true
					}
				}
			}
		}
	}
	return v, found
}

// tally 是一个目标上四种结果的计数。
type tally struct{ ok, lagging, mismatch, skipped int64 }

func readTally(t *testing.T, target string) tally {
	t.Helper()
	get := func(result string) int64 {
		v, _ := metricValue(t, "recon.changes.checked", map[string]string{"target": target, "result": result})
		return v
	}
	return tally{get(resultOK), get(resultLagging), get(resultMismatch), get(resultSkipped)}
}

func (a tally) sub(b tally) tally {
	return tally{a.ok - b.ok, a.lagging - b.lagging, a.mismatch - b.mismatch, a.skipped - b.skipped}
}

// fakeTarget 是内存里的派生端：rows 是它现在有的行（订单号到版本和“内容是否不同”）。
type fakeTarget struct {
	name  string
	wants func(Change) bool

	mu    sync.Mutex
	rows  map[int64]Derived
	err   error
	calls int
	seen  [][]int64 // 每次 Check 收到的订单号
}

func newFake(name string) *fakeTarget { return &fakeTarget{name: name, rows: map[int64]Derived{}} }

func (f *fakeTarget) Name() string { return f.name }

func (f *fakeTarget) Wants(c Change) bool { return f.wants == nil || f.wants(c) }

func (f *fakeTarget) Check(_ context.Context, cs []Change) ([]Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	ids := make([]int64, len(cs))
	out := make([]Result, len(cs))
	for i, c := range cs {
		ids[i] = c.OrderID
		out[i] = Result{Verdict: Judge(c.Version, f.rows[c.OrderID]), Detail: f.rows[c.OrderID].Diff}
	}
	f.seen = append(f.seen, ids)
	return out, nil
}

func (f *fakeTarget) set(orderID, version int64, diff string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[orderID] = Derived{Found: true, Version: version, Diff: diff}
}

func (f *fakeTarget) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeTarget) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newChecker(t *testing.T, cfg Config) *Checker {
	t.Helper()
	cfg.Logger = slog.New(slog.DiscardHandler)
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// change 造一条在 commit 时刻提交的变更。
func change(orderID, version int64, commit time.Time) Change {
	return Change{OrderID: orderID, Version: version, MerchantID: 7, Commit: commit}
}

// 核对在提交满 Delay 之后才发生，等的是这一批里最晚的那一条；全量读到的行（没有提交时刻）不等。
func TestCheckWaitsForTheDelay(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		f := newFake("wait")
		f.set(1, 1, "")
		f.set(2, 1, "")
		c := newChecker(t, Config{Targets: []Target{f}, Delay: 10 * time.Second})
		start := time.Now()

		if err := c.Check(t.Context(), []Change{change(1, 1, start.Add(-8*time.Second)), change(2, 1, start.Add(-3*time.Second))}); err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(start); waited != 7*time.Second {
			t.Errorf("waited %s, want 7s (10s after the latest commit)", waited)
		}
		if got := c.VerifiedThrough(); !got.Equal(start.Add(-3 * time.Second)) {
			t.Errorf("verified through %s, want the latest commit %s", got, start.Add(-3*time.Second))
		}

		start = time.Now()
		if err := c.Check(t.Context(), []Change{change(1, 1, time.Time{})}); err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(start); waited != 0 {
			t.Errorf("waited %s for a snapshot row, want 0", waited)
		}
		// 早就提交的也不等，而且 verified_through 不往回走。
		if err := c.Check(t.Context(), []Change{change(1, 1, start.Add(-time.Hour))}); err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(start); waited != 0 {
			t.Errorf("waited %s for an old change, want 0", waited)
		}
		if got := c.VerifiedThrough(); got.Before(start.Add(-time.Minute)) {
			t.Errorf("verified through moved backwards to %s", got)
		}
	})
}

// 等的时候 ctx 取消要马上返回错误（消费者正在停，这一批不提交）。
func TestCheckStopsWaitingWhenCancelled(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		f := newFake("cancel")
		c := newChecker(t, Config{Targets: []Target{f}})
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(time.Second)
			cancel()
		}()
		err := c.Check(ctx, []Change{change(1, 1, time.Now())})
		if !errors.Is(err, context.Canceled) || f.callCount() != 0 {
			t.Errorf("error %v, target calls %d; want context.Canceled and no check", err, f.callCount())
		}
	})
}

// 第一次核对：通过的计 ok，同版本内容不同的马上计 mismatch，没追上的不计数、等复查。
func TestFirstCheckOutcomes(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		f := newFake("first")
		f.set(1, 3, "")       // 一致
		f.set(2, 9, "")       // 已经是更晚的版本
		f.set(3, 3, "status") // 同版本内容不同
		f.set(4, 2, "")       // 版本更早
		// 5 没有这一行
		c := newChecker(t, Config{Targets: []Target{f}})
		before := readTally(t, "first")
		old := time.Now().Add(-time.Minute)
		var batch []Change
		for id := int64(1); id <= 5; id++ {
			batch = append(batch, change(id, 3, old))
		}
		if err := c.Check(t.Context(), batch); err != nil {
			t.Fatal(err)
		}
		if got := readTally(t, "first").sub(before); got != (tally{ok: 2, mismatch: 1}) {
			t.Errorf("tally %+v, want 2 ok and 1 mismatch", got)
		}
		if c.Pending() != 2 {
			t.Errorf("pending %d, want 2", c.Pending())
		}
	})
}

// 没追上的在复查时追上了：计 ok，不报落后。
func TestBehindThenCatchesUp(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		f := newFake("catchup")
		c := newChecker(t, Config{Targets: []Target{f}})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()

		before := readTally(t, "catchup")
		if err := c.Check(t.Context(), []Change{change(1, 3, time.Now().Add(-time.Minute))}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(12 * time.Second) // 两次复查，都还没追上
		synctest.Wait()
		if got := readTally(t, "catchup").sub(before); got != (tally{}) || c.Pending() != 1 {
			t.Fatalf("while behind: tally %+v, pending %d; want nothing counted and 1 pending", got, c.Pending())
		}
		f.set(1, 3, "")
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if got := readTally(t, "catchup").sub(before); got != (tally{ok: 1}) || c.Pending() != 0 {
			t.Errorf("after catching up: tally %+v, pending %d; want 1 ok and nothing pending", got, c.Pending())
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

// 一直没追上：过了 LagAfter 报一次落后，之后不再复查它。追上之后内容不同的，复查时报不一致。
func TestBehindTooLongIsReportedOnce(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		f := newFake("lag")
		c := newChecker(t, Config{Targets: []Target{f}, LagAfter: 2 * time.Minute, RecheckEvery: 5 * time.Second})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()

		before := readTally(t, "lag")
		old := time.Now().Add(-time.Minute)
		if err := c.Check(t.Context(), []Change{change(1, 3, old), change(2, 3, old)}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		f.set(2, 3, "total_amount") // 2 追上了，但内容不对
		time.Sleep(55 * time.Second)
		synctest.Wait()
		if got := readTally(t, "lag").sub(before); got != (tally{mismatch: 1}) || c.Pending() != 1 {
			t.Fatalf("before LagAfter: tally %+v, pending %d; want 1 mismatch and 1 pending", got, c.Pending())
		}
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := readTally(t, "lag").sub(before); got != (tally{mismatch: 1, lagging: 1}) || c.Pending() != 0 {
			t.Fatalf("after LagAfter: tally %+v, pending %d; want 1 lagging and nothing pending", got, c.Pending())
		}
		calls := f.callCount()
		time.Sleep(time.Minute)
		synctest.Wait()
		if got := readTally(t, "lag").sub(before); got.lagging != 1 || f.callCount() != calls {
			t.Errorf("a reported change was checked again: tally %+v, %d more checks", got, f.callCount()-calls)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

// 同一个订单的新变更顶替等着的旧变更：派生端到了新版本，旧的不再单独报。
func TestNewerChangeReplacesPending(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		f := newFake("replace")
		c := newChecker(t, Config{Targets: []Target{f}})
		before := readTally(t, "replace")
		old := time.Now().Add(-time.Minute)

		if err := c.Check(t.Context(), []Change{change(1, 3, old), change(2, 3, old), change(3, 3, old)}); err != nil {
			t.Fatal(err)
		}
		if c.Pending() != 3 {
			t.Fatalf("pending %d, want 3", c.Pending())
		}
		// 1：新版本也没追上，等的是新的；2：派生端直接到了新版本；3：来了一条更旧的变更（重复投递），不动。
		f.set(2, 4, "")
		if err := c.Check(t.Context(), []Change{change(1, 4, old), change(2, 4, old), change(3, 2, old)}); err != nil {
			t.Fatal(err)
		}
		if got := readTally(t, "replace").sub(before); got != (tally{ok: 1}) || c.Pending() != 2 {
			t.Fatalf("tally %+v, pending %d; want 1 ok and 2 pending", got, c.Pending())
		}
		// 派生端只到了 3：订单 1 等的是 4，仍然没追上；订单 3 等的是 3，追上了。
		f.set(1, 3, "")
		f.set(3, 3, "")
		c.Recheck(t.Context())
		if got := readTally(t, "replace").sub(before); got != (tally{ok: 2}) || c.Pending() != 1 {
			t.Errorf("after recheck: tally %+v, pending %d; want 2 ok and 1 pending (order 1 waits for version 4)", got, c.Pending())
		}
	})
}

// 一批里同一个订单有几条变更时只核对版本最高的；目标不要的变更不给它。
func TestBatchKeepsLatestPerOrderAndFilters(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		all := newFake("all")
		onlyDeleted := newFake("deleted")
		onlyDeleted.wants = func(c Change) bool { return c.Deleted }
		for _, f := range []*fakeTarget{all, onlyDeleted} {
			f.set(1, 5, "")
			f.set(2, 1, "")
		}
		c := newChecker(t, Config{Targets: []Target{all, onlyDeleted}})
		beforeAll, beforeDeleted := readTally(t, "all"), readTally(t, "deleted")
		old := time.Now().Add(-time.Minute)
		gone := change(2, 1, old)
		gone.Deleted = true
		if err := c.Check(t.Context(), []Change{change(1, 3, old), change(1, 5, old), change(1, 4, old), gone}); err != nil {
			t.Fatal(err)
		}
		if got := readTally(t, "all").sub(beforeAll); got != (tally{ok: 2}) {
			t.Errorf("target all: tally %+v, want 2 ok (orders 1 and 2, one each)", got)
		}
		if got := readTally(t, "deleted").sub(beforeDeleted); got != (tally{ok: 1}) {
			t.Errorf("target deleted: tally %+v, want 1 ok", got)
		}
		if len(onlyDeleted.seen) != 1 || len(onlyDeleted.seen[0]) != 1 || onlyDeleted.seen[0][0] != 2 {
			t.Errorf("target deleted saw %v, want only order 2", onlyDeleted.seen)
		}
	})
}

// 派生端读不到：返回错误（消费者停在这一批上），什么都不计，verified_through 不动。复查时读不到的留到下一次。
func TestTargetErrorIsNotAVerdict(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		f := newFake("down")
		c := newChecker(t, Config{Targets: []Target{f}})
		before := readTally(t, "down")
		old := time.Now().Add(-time.Minute)

		if err := c.Check(t.Context(), []Change{change(1, 3, old)}); err != nil {
			t.Fatal(err)
		}
		through := c.VerifiedThrough()

		down := errors.New("connection refused")
		f.fail(down)
		err := c.Check(t.Context(), []Change{change(2, 3, old.Add(time.Second))})
		if !errors.Is(err, down) || kafkax.IsPermanent(err) {
			t.Fatalf("error %v, want the target's error and not a permanent one", err)
		}
		time.Sleep(5 * time.Minute)
		c.Recheck(t.Context())
		if got := readTally(t, "down").sub(before); got != (tally{}) || c.Pending() != 1 || !c.VerifiedThrough().Equal(through) {
			t.Errorf("while the target is down: tally %+v, pending %d, verified through %s; want nothing counted, 1 pending, %s",
				got, c.Pending(), c.VerifiedThrough(), through)
		}
		// 恢复之后照常复查：已经过了 LagAfter 仍然没追上，报落后。
		f.fail(nil)
		c.Recheck(t.Context())
		if got := readTally(t, "down").sub(before); got != (tally{lagging: 1}) {
			t.Errorf("after recovery: tally %+v, want 1 lagging", got)
		}
	})
}

// 开关关掉：只消费不查库，计 skipped，verified_through 不动，等着的留着。
func TestSwitchedOff(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		f := newFake("off")
		off := false
		c := newChecker(t, Config{Targets: []Target{f}, Off: func() bool { return off }})
		before := readTally(t, "off")
		old := time.Now().Add(-time.Minute)
		if err := c.Check(t.Context(), []Change{change(1, 3, old)}); err != nil {
			t.Fatal(err)
		}
		calls, through := f.callCount(), c.VerifiedThrough()

		off = true
		if err := c.Check(t.Context(), []Change{change(2, 3, time.Now()), change(3, 3, time.Now())}); err != nil {
			t.Fatal(err)
		}
		c.Recheck(t.Context())
		if got := readTally(t, "off").sub(before); got != (tally{skipped: 2}) || f.callCount() != calls ||
			!c.VerifiedThrough().Equal(through) || c.Pending() != 1 {
			t.Errorf("switched off: tally %+v, %d more checks, verified through %s, pending %d; want 2 skipped, no checks, %s, 1",
				got, f.callCount()-calls, c.VerifiedThrough(), c.Pending(), through)
		}
	})
}

// 等着复查的到了上限：新的直接报落后，不再记。
func TestPendingIsCapped(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		f := newFake("cap")
		c := newChecker(t, Config{Targets: []Target{f}, MaxPending: 2})
		before := readTally(t, "cap")
		old := time.Now().Add(-time.Minute)
		if err := c.Check(t.Context(), []Change{change(1, 1, old), change(2, 1, old), change(3, 1, old), change(4, 1, old)}); err != nil {
			t.Fatal(err)
		}
		if got := readTally(t, "cap").sub(before); got != (tally{lagging: 2}) || c.Pending() != 2 {
			t.Errorf("tally %+v, pending %d; want 2 lagging and 2 pending", got, c.Pending())
		}
	})
}

// Handler：解不开的消息计 skipped 并跳过，不返回错误（不挡分区、不进死信）；别的表的变更忽略。
func TestHandlerSkipsUndecodableMessages(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	synctest.Test(t, func(t *testing.T) {
		f := newFake("handler")
		f.set(1, 3, "")
		c := newChecker(t, Config{Targets: []Target{f}})
		before, beforeNone := readTally(t, "handler"), readTally(t, targetNone)

		marshal := func(ch *cdcv1.RowChange) []byte {
			b, err := proto.Marshal(ch)
			if err != nil {
				t.Fatal(err)
			}
			return b
		}
		noVersion := image(2, 1, nullVal())
		delete(noVersion, ColVersion)
		msgs := []kafkax.Message{
			{Value: marshal(&cdcv1.RowChange{Table: SourceTable, Op: cdcv1.Op_OP_UPDATE, After: image(1, 3, intVal(7))})},
			{Value: []byte{0xff, 0xff, 0xff}},
			{Value: marshal(&cdcv1.RowChange{Table: SourceTable, Op: cdcv1.Op_OP_UPDATE, After: noVersion})},
			{Value: marshal(&cdcv1.RowChange{Table: "order_items", Op: cdcv1.Op_OP_INSERT})},
		}
		if err := c.Handler()(t.Context(), msgs); err != nil {
			t.Fatalf("handler returned %v, want nil", err)
		}
		if got := readTally(t, "handler").sub(before); got != (tally{ok: 1}) {
			t.Errorf("target tally %+v, want 1 ok", got)
		}
		if got := readTally(t, targetNone).sub(beforeNone); got != (tally{skipped: 2}) {
			t.Errorf("undecodable tally %+v, want 2 skipped", got)
		}
	})
}

func TestNewValidates(t *testing.T) {
	t.Parallel()
	if _, err := New(Config{}); err == nil {
		t.Error("New accepted no targets")
	}
	if _, err := New(Config{Targets: []Target{newFake("a"), newFake("a")}}); err == nil {
		t.Error("New accepted two targets with the same name")
	}
	if _, err := New(Config{Targets: []Target{newFake(targetNone)}}); err == nil {
		t.Error("New accepted the reserved target name")
	}
}

// 瞬时值：还没核对过时不报 verified_through；之后报提交时刻的秒数和等着的条数。
func TestGauges(t *testing.T) { //nolint:paralleltest // 用到进程级的指标
	f := newFake("gauges")
	c := newChecker(t, Config{Targets: []Target{f}, Delay: time.Nanosecond})
	unregister, err := c.registerGauges()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unregister)
	if _, found := metricValue(t, "recon.changes.verified_through", nil); found {
		t.Fatal("verified_through reported before any check")
	}
	commit := time.Now().Add(-time.Hour).Truncate(time.Second)
	if err := c.Check(t.Context(), []Change{change(1, 1, commit)}); err != nil {
		t.Fatal(err)
	}
	if got, found := metricValue(t, "recon.changes.verified_through", nil); !found || got != commit.Unix() {
		t.Errorf("verified_through = %d (found %v), want %d", got, found, commit.Unix())
	}
	if got, _ := metricValue(t, "recon.changes.pending", nil); got != 1 {
		t.Errorf("pending = %d, want 1", got)
	}
}
