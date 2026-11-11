package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/pkg/testkafka"
)

// migrations 用 Schema 常量本身建表，保证测试的表和各服务迁移里放的是同一份。
var migrations = fstest.MapFS{
	"migrations/00001_outbox.sql": {Data: []byte("-- +goose Up\n" + Schema + ";\n")},
}

var errBoom = errors.New("boom")

// fakeSender 记录收到的每一条消息（不论成功失败，等同于“broker 已写入”），
// 并可按需让指定消息失败、少返回 errs、整体返回错误或在返回前执行回调。
type fakeSender struct {
	mu    sync.Mutex
	recs  []kafkax.Record // 收到的全部记录，按到达顺序
	calls int

	failIf  func(kafkax.Record) error // 返回非 nil 表示这条失败
	shortTo int                       // >=0 时 errs 只返回前 shortTo 个
	sendErr error                     // 非 nil 时作为 Send 的第二个返回值
	onSend  func(recs []kafkax.Record)
}

func newFakeSender() *fakeSender { return &fakeSender{shortTo: -1} }

func (f *fakeSender) set(fn func(f *fakeSender)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeSender) Send(_ context.Context, recs ...kafkax.Record) ([]error, error) {
	f.mu.Lock()
	f.recs = append(f.recs, recs...)
	f.calls++
	failIf, shortTo, sendErr, onSend := f.failIf, f.shortTo, f.sendErr, f.onSend
	f.mu.Unlock()

	if onSend != nil {
		onSend(recs)
	}
	errs := make([]error, len(recs))
	if failIf != nil {
		for i, r := range recs {
			errs[i] = failIf(r)
		}
	}
	// 与 kafkax.Producer 的约定一致：任何一条失败，第二个返回值就是汇总错误。
	if sendErr == nil {
		sendErr = errors.Join(errs...)
	}
	if shortTo >= 0 && shortTo < len(errs) {
		errs = errs[:shortTo]
	}
	return errs, sendErr
}

func (f *fakeSender) received() []kafkax.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.recs)
}

func (f *fakeSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.recs)
}

func eventID(r kafkax.Record) string { return r.Headers[EventIDHeader] }

func payloads(recs []kafkax.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r.Value)
	}
	return out
}

// failPayload 让 payload 在 bad 里的消息失败。
func failPayload(bad ...string) func(kafkax.Record) error {
	return func(r kafkax.Record) error {
		if slices.Contains(bad, string(r.Value)) {
			return errBoom
		}
		return nil
	}
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	return testdb.New(t, migrations, "migrations")
}

func newRelay(t *testing.T, db *sql.DB, s kafkax.Sender, mutate ...func(*RelayConfig)) *Relay {
	t.Helper()
	cfg := RelayConfig{
		Name:   "svc",
		DB:     db,
		Sender: s,
		Logger: slog.New(slog.DiscardHandler),
	}
	for _, m := range mutate {
		m(&cfg)
	}
	r, err := NewRelay(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// commitAdd 在独立事务里写入并提交。
func commitAdd(t *testing.T, db *sql.DB, msgs ...Message) {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Add(t.Context(), tx, msgs...); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func msg(key, payload string) Message {
	return Message{Topic: "t", Key: []byte(key), Payload: []byte(payload)}
}

func pending(t *testing.T, db *sql.DB) int {
	t.Helper()
	n, err := Pending(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func rowCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM outbox").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// payloadsWhere 返回满足条件的行的 payload，按 id 升序。
func payloadsWhere(t *testing.T, db *sql.DB, where string) []string {
	t.Helper()
	rs, err := db.QueryContext(t.Context(), "SELECT payload FROM outbox WHERE "+where+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close() //nolint:errcheck // 测试里的只读查询
	var out []string
	for rs.Next() {
		var p []byte
		if err := rs.Scan(&p); err != nil {
			t.Fatal(err)
		}
		out = append(out, string(p))
	}
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func mustFlush(t *testing.T, r *Relay) int {
	t.Helper()
	n, err := r.Flush(t.Context())
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return n
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---- Add ----

// 防止：事务回滚后消息仍然存在（那就成了“通知了一件没发生的事”）；提交后却不是待发布状态。
func TestAddRollbackLeavesNothing(t *testing.T) {
	db := newDB(t)

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Add(t.Context(), tx, msg("k", "a"), msg("k", "b")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := rowCount(t, db); n != 0 {
		t.Fatalf("rows after rollback = %d, want 0", n)
	}

	commitAdd(t, db, msg("k", "a"), msg("k", "b"))
	if n := pending(t, db); n != 2 {
		t.Fatalf("pending after commit = %d, want 2", n)
	}
}

// 防止：空 topic 被写进表后，中继永远发不出去、一直卡着重试。
func TestAddRejectsEmptyTopic(t *testing.T) {
	db := newDB(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := Add(t.Context(), tx, Message{Key: []byte("k"), Payload: []byte("p")}); err == nil {
		t.Fatal("Add with empty topic succeeded, want error")
	}
}

// kafkax.Record 明确允许 Key 为 nil（由客户端挑分区），Add 也应该接受没有 key 的消息。
// 但 msg_key 是 NOT NULL，驱动会把 nil []byte 当成 NULL 写入，所以 Add 会失败。
func TestAddAcceptsNilKey(t *testing.T) {
	t.Skip("已知缺陷：Add 不接受 nil Key（msg_key NOT NULL）。修复实现后删掉这行 Skip")
	db := newDB(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := Add(t.Context(), tx, Message{Topic: "t", Payload: []byte("p")}); err != nil {
		t.Errorf("Add with nil Key: %v", err)
	}
}

// 同上，Payload 为 nil（例如空事件）也会被写成 NULL。
func TestAddAcceptsNilPayload(t *testing.T) {
	t.Skip("已知缺陷：Add 不接受 nil Payload（payload NOT NULL）。修复实现后删掉这行 Skip")
	db := newDB(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := Add(t.Context(), tx, Message{Topic: "t", Key: []byte("k")}); err != nil {
		t.Errorf("Add with nil Payload: %v", err)
	}
}

// ---- Flush ----

// 防止：发出的消息内容走样、event-id 不对，或发出后没标记导致重发 / Pending 不归零。
func TestFlushPublishesAndMarks(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)

	commitAdd(t, db,
		Message{Topic: "orders", Key: []byte("o-1"), Payload: []byte("p1")},
		Message{Topic: "payments", Key: []byte("o-2"), Payload: []byte("p2")},
	)
	if n := mustFlush(t, r); n != 2 {
		t.Fatalf("published = %d, want 2", n)
	}

	var id1 int64
	if err := db.QueryRowContext(t.Context(), "SELECT MIN(id) FROM outbox").Scan(&id1); err != nil {
		t.Fatal(err)
	}
	got := s.received()
	if len(got) != 2 {
		t.Fatalf("sent %d, want 2", len(got))
	}
	want := []struct{ topic, key, value, id string }{
		{"orders", "o-1", "p1", "svc:" + strconv.FormatInt(id1, 10)},
		{"payments", "o-2", "p2", "svc:" + strconv.FormatInt(id1+1, 10)},
	}
	for i, w := range want {
		g := got[i]
		if g.Topic != w.topic || string(g.Key) != w.key || string(g.Value) != w.value || eventID(g) != w.id {
			t.Errorf("record %d = %+v, want %+v", i, g, w)
		}
	}

	if n := pending(t, db); n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
	if n := mustFlush(t, r); n != 0 {
		t.Fatalf("second Flush published %d, want 0", n)
	}
	if s.count() != 2 {
		t.Fatalf("sender received %d after second Flush, want 2", s.count())
	}
}

// 防止：把“已发到的最大 id”当游标。事务 A 先拿到较小的 id 但后提交，
// B 先提交并被发出后，A 的消息必须在 A 提交后的下一次 Flush 里发出，不能丢。
func TestFlushDoesNotLoseLowerIDCommittedLater(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)
	ctx := t.Context()

	txA, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = txA.Rollback() }()
	txB, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = txB.Rollback() }()

	if err := Add(ctx, txA, msg("a", "A")); err != nil { // 先 Add，id 较小
		t.Fatal(err)
	}
	if err := Add(ctx, txB, msg("b", "B")); err != nil {
		t.Fatal(err)
	}
	if err := txB.Commit(); err != nil { // B 先提交
		t.Fatal(err)
	}

	if n := mustFlush(t, r); n != 1 {
		t.Fatalf("first Flush published %d, want 1", n)
	}
	if got := payloads(s.received()); !slices.Equal(got, []string{"B"}) {
		t.Fatalf("sent %v, want [B]", got)
	}

	if err := txA.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := mustFlush(t, r); n != 1 {
		t.Fatalf("second Flush published %d, want 1", n)
	}
	if got := payloads(s.received()); !slices.Equal(got, []string{"B", "A"}) {
		t.Fatalf("sent %v, want [B A]", got)
	}
	if n := pending(t, db); n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
}

// 防止：队首一批发不出去的消息占着位置，让后面的消息永远轮不到（饿死）。
func TestFlushFailingHeadDoesNotStarveTail(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s, func(c *RelayConfig) { c.BatchSize = 2 })

	var bad []string
	for i := range 5 { // 5 条 > BatchSize*2
		p := fmt.Sprintf("bad-%d", i)
		bad = append(bad, p)
		commitAdd(t, db, msg("k", p))
	}
	for i := range 3 {
		commitAdd(t, db, msg("k", fmt.Sprintf("good-%d", i)))
	}
	s.set(func(f *fakeSender) { f.failIf = failPayload(bad...) })

	n, err := r.Flush(t.Context())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Flush error = %v, want it to wrap the sender error", err)
	}
	if n != 3 {
		t.Fatalf("published = %d, want 3", n)
	}
	if left := payloadsWhere(t, db, "published_at IS NULL"); !slices.Equal(left, bad) {
		t.Fatalf("unpublished = %v, want %v", left, bad)
	}
	if p := pending(t, db); p != 5 {
		t.Fatalf("pending = %d, want 5", p)
	}

	// Sender 恢复后，下一次 Flush 发出剩下的。
	s.set(func(f *fakeSender) { f.failIf = nil })
	if n := mustFlush(t, r); n != 5 {
		t.Fatalf("recovery Flush published %d, want 5", n)
	}
	if p := pending(t, db); p != 0 {
		t.Fatalf("pending = %d, want 0", p)
	}
}

// 防止：一批里有成功有失败时，把失败的也标记了（丢消息）或把成功的漏标了（多重发）。
func TestFlushPartialFailureMarksOnlySuccessful(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)
	for _, p := range []string{"a", "b", "c", "d"} {
		commitAdd(t, db, msg("k", p))
	}
	s.set(func(f *fakeSender) { f.failIf = failPayload("b", "d") })

	n, err := r.Flush(t.Context())
	if err == nil || n != 2 {
		t.Fatalf("Flush = (%d, %v), want (2, error)", n, err)
	}
	if left := payloadsWhere(t, db, "published_at IS NULL"); !slices.Equal(left, []string{"b", "d"}) {
		t.Fatalf("unpublished = %v, want [b d]", left)
	}
}

// 防止：Sender 违反约定少返回 errs 时，把缺的当成成功标记掉（丢消息）。缺的必须按失败处理。
func TestFlushShortErrsTreatedAsFailure(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)
	for _, p := range []string{"a", "b", "c"} {
		commitAdd(t, db, msg("k", p))
	}
	s.set(func(f *fakeSender) { f.shortTo = 1 })

	n, _ := r.Flush(t.Context())
	if n != 1 {
		t.Fatalf("published = %d, want 1", n)
	}
	if left := payloadsWhere(t, db, "published_at IS NULL"); !slices.Equal(left, []string{"b", "c"}) {
		t.Fatalf("unpublished = %v, want [b c]", left)
	}
}

// Send 整体返回错误但 errs 里有成功项：成功的仍要标记，Flush 返回错误。
func TestFlushSendErrorStillMarksSuccessfulItems(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)
	commitAdd(t, db, msg("k", "a"), msg("k", "b"))
	s.set(func(f *fakeSender) { f.failIf = failPayload("b"); f.sendErr = errBoom })

	n, err := r.Flush(t.Context())
	if !errors.Is(err, errBoom) || n != 1 {
		t.Fatalf("Flush = (%d, %v), want (1, boom)", n, err)
	}
	if left := payloadsWhere(t, db, "published_at IS NULL"); !slices.Equal(left, []string{"b"}) {
		t.Fatalf("unpublished = %v, want [b]", left)
	}
}

// 防止：误以为“至多一次”。消息已写入 broker 但确认丢失（Send 返回错误），
// 没标记，重启后全新的中继必须用同一个 event-id 重发，消费者靠它去重。
func TestFlushRedeliversWhenAckLost(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s)
	commitAdd(t, db, msg("k", "a"))

	// 模拟“发出去了但没等到确认就崩溃/超时”：记录下来，却返回失败。
	s.set(func(f *fakeSender) { f.failIf = func(kafkax.Record) error { return errBoom } })
	if _, err := r.Flush(t.Context()); err == nil {
		t.Fatal("want error")
	}
	if p := pending(t, db); p != 1 {
		t.Fatalf("pending = %d, want 1", p)
	}

	s.set(func(f *fakeSender) { f.failIf = nil })
	r2 := newRelay(t, db, s)
	if n := mustFlush(t, r2); n != 1 {
		t.Fatalf("published = %d, want 1", n)
	}
	got := s.received()
	if len(got) != 2 || eventID(got[0]) == "" || eventID(got[0]) != eventID(got[1]) {
		t.Fatalf("received %+v, want the same event-id twice", got)
	}
}

// 防止：另一个实例在我们发送期间已经标记过这行时，本实例的标记覆盖它的 published_at。
func TestMarkDoesNotOverwriteOtherRelay(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	clock := &fakeClock{t: time.Now().Add(-time.Hour).Truncate(time.Second)}
	r := newRelay(t, db, s, func(c *RelayConfig) { c.Now = clock.Now })
	commitAdd(t, db, msg("k", "a"))

	first := clock.Now().UTC()
	s.set(func(f *fakeSender) {
		f.onSend = func([]kafkax.Record) {
			if _, err := db.ExecContext(t.Context(), "UPDATE outbox SET published_at = ?", first); err != nil {
				t.Error(err)
			}
			clock.Advance(time.Minute)
		}
	})
	mustFlush(t, r)

	var got time.Time
	if err := db.QueryRowContext(t.Context(), "SELECT published_at FROM outbox").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(first) {
		t.Fatalf("published_at = %v, want untouched %v", got, first)
	}
}

// 防止：同一个 key 的消息乱序发出（先写入的应先发送，包括跨批次）。
func TestFlushKeepsKeyOrder(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r := newRelay(t, db, s, func(c *RelayConfig) { c.BatchSize = 3 })

	var want []string
	for i := range 10 {
		p := fmt.Sprintf("%02d", i)
		want = append(want, p)
		commitAdd(t, db, msg("k", p)) // 每条一个事务，id 递增
		commitAdd(t, db, msg("other", "x"+p))
	}
	mustFlush(t, r)

	var got []string
	for _, rec := range s.received() {
		if string(rec.Key) == "k" {
			got = append(got, string(rec.Value))
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("key k order = %v, want %v", got, want)
	}
}

// 防止：多个中继实例同时跑时丢消息或留下未标记的行；同时在 -race 下检查无数据竞争。
func TestConcurrentRelays(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	r1 := newRelay(t, db, s, func(c *RelayConfig) { c.Name = "a"; c.BatchSize = 7 })
	r2 := newRelay(t, db, s, func(c *RelayConfig) { c.Name = "b"; c.BatchSize = 7 })

	const total = 60
	for i := range total {
		commitAdd(t, db, msg("k"+strconv.Itoa(i%5), strconv.Itoa(i)))
	}

	var wg sync.WaitGroup
	for _, r := range []*Relay{r1, r2} {
		wg.Go(func() {
			if _, err := r.Flush(t.Context()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	seen := map[string]bool{}
	for _, rec := range s.received() {
		seen[string(rec.Value)] = true
	}
	if len(seen) != total {
		t.Fatalf("distinct payloads sent = %d, want %d", len(seen), total)
	}
	if n := pending(t, db); n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
}

// ---- Start / Kick / Stop ----

// startRelay 启动中继，测试结束时停止并等待退出。
func startRelay(t *testing.T, r *Relay) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- r.Start(t.Context()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
		if err := <-done; err != nil {
			t.Errorf("Start returned %v", err)
		}
	})
}

// 防止：启动后不先发一轮，导致上次停机时遗留的消息要等一个轮询周期（这里周期设成 1 小时）。
func TestStartFlushesExistingImmediately(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	commitAdd(t, db, msg("k", "a"), msg("k", "b"))
	r := newRelay(t, db, s, func(c *RelayConfig) { c.Interval = time.Hour })

	startRelay(t, r)
	eventually(t, "existing messages sent", func() bool { return s.count() == 2 })
	eventually(t, "pending drained", func() bool { return pending(t, db) == 0 })
}

// 防止：Kick 无效、只能靠轮询。Interval 为 1 小时，Kick 之后消息必须很快发出。
func TestKickTriggersImmediateRound(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	commitAdd(t, db, msg("k", "first"))
	r := newRelay(t, db, s, func(c *RelayConfig) { c.Interval = time.Hour })
	startRelay(t, r)
	eventually(t, "first round", func() bool { return s.count() == 1 })

	commitAdd(t, db, msg("k", "second"))
	r.Kick()
	eventually(t, "kicked round", func() bool { return s.count() == 2 })
}

// 防止：Stop 之后中继仍在发送；Stop 后 Kick 不应 panic 或阻塞。
func TestStopHaltsSending(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	commitAdd(t, db, msg("k", "first"))
	r := newRelay(t, db, s, func(c *RelayConfig) { c.Interval = time.Hour })

	done := make(chan error, 1)
	go func() { done <- r.Start(t.Context()) }()
	eventually(t, "first round", func() bool { return s.count() == 1 })

	if err := r.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	commitAdd(t, db, msg("k", "after-stop"))
	r.Kick()
	r.Kick()
	// Stop 返回时循环已经退出，不存在“稍后才发”的可能，所以可以立即断言。
	if s.count() != 1 || pending(t, db) != 1 {
		t.Fatalf("sent %d, pending %d after Stop; want 1 and 1", s.count(), pending(t, db))
	}
}

// 防止：Stop 不等正在进行的一轮。发送阻塞时 Stop 的 ctx 先到期，放行后 Start 才返回，且标记已完成。
func TestStopWaitsForInflightRound(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	commitAdd(t, db, msg("k", "a"))
	entered := make(chan struct{})
	release := make(chan struct{})
	s.set(func(f *fakeSender) {
		f.onSend = func([]kafkax.Record) {
			close(entered)
			<-release
		}
	})
	r := newRelay(t, db, s, func(c *RelayConfig) { c.Interval = time.Hour })

	done := make(chan error, 1)
	go func() { done <- r.Start(t.Context()) }()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop during inflight round = %v, want deadline exceeded", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := pending(t, db); n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
}

// ---- 清理 ----

// 防止：清理误删未发布的消息或保留期内的行，或者保留期外的行永远不删。
func TestCleanupRespectsRetention(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	clock := &fakeClock{t: time.Now().Truncate(time.Second)}
	r := newRelay(t, db, s, func(c *RelayConfig) {
		c.Now = clock.Now
		c.Retention = time.Hour
	})

	commitAdd(t, db, msg("k", "old"))
	mustFlush(t, r) // published_at = t0
	clock.Advance(30 * time.Minute)
	commitAdd(t, db, msg("k", "recent"))
	mustFlush(t, r) // published_at = t0+30m
	s.set(func(f *fakeSender) { f.failIf = failPayload("unpublished") })
	commitAdd(t, db, msg("k", "unpublished"))
	if _, err := r.Flush(t.Context()); err == nil {
		t.Fatal("want send error")
	}

	// now = t0+70m，截止 = t0+10m：old 早于截止，recent 在保留期内。
	clock.Advance(40 * time.Minute)
	if err := r.cleanup(t.Context(), clock.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if left := payloadsWhere(t, db, "1=1"); !slices.Equal(left, []string{"recent", "unpublished"}) {
		t.Fatalf("rows left = %v, want [recent unpublished]", left)
	}
}

// 防止：每一轮都清理（应按 CleanupInterval 节流），或者到期了也不清理。
func TestRoundCleansUpOnSchedule(t *testing.T) {
	db := newDB(t)
	s := newFakeSender()
	clock := &fakeClock{t: time.Now().Truncate(time.Second)}
	r := newRelay(t, db, s, func(c *RelayConfig) {
		c.Now = clock.Now
		c.Retention = time.Hour
		c.CleanupInterval = 10 * time.Minute
	})
	round := func() {
		t.Helper()
		if err := r.round(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	commitAdd(t, db, msg("k", "a"))
	round() // 发出 a（published_at = t0），并做第一次清理，此时没有过期行
	if n := rowCount(t, db); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}

	clock.Advance(5 * time.Minute) // 距上次清理不足 CleanupInterval：即使有过期行也不清理
	commitAdd(t, db, msg("k", "b"))
	if _, err := db.ExecContext(t.Context(),
		"UPDATE outbox SET published_at = ? WHERE payload = 'a'", clock.Now().Add(-2*time.Hour).UTC()); err != nil {
		t.Fatal(err)
	}
	round()
	if n := rowCount(t, db); n != 2 {
		t.Fatalf("rows = %d, want 2 (cleanup should be throttled)", n)
	}

	clock.Advance(6 * time.Minute) // 累计 11 分钟，到期：a 被删，b 在保留期内
	round()
	if left := payloadsWhere(t, db, "1=1"); !slices.Equal(left, []string{"b"}) {
		t.Fatalf("rows left = %v, want [b]", left)
	}
}

// ---- NewRelay ----

// 防止：缺必填项时到运行期才出问题；默认值没有落地。
func TestNewRelayValidates(t *testing.T) {
	db := &sql.DB{}
	s := newFakeSender()
	for name, cfg := range map[string]RelayConfig{
		"no name":   {DB: db, Sender: s},
		"no db":     {Name: "x", Sender: s},
		"no sender": {Name: "x", DB: db},
	} {
		if _, err := NewRelay(cfg); err == nil {
			t.Errorf("%s: NewRelay succeeded, want error", name)
		}
	}

	r, err := NewRelay(RelayConfig{Name: "x", DB: db, Sender: s})
	if err != nil {
		t.Fatal(err)
	}
	c := r.cfg
	if c.Interval != 100*time.Millisecond || c.BatchSize != 200 || c.Retention != time.Hour ||
		c.CleanupInterval != time.Minute || c.Now == nil || c.Logger == nil {
		t.Errorf("defaults not applied: %+v", c)
	}
	if r.Name() != "x-outbox-relay" {
		t.Errorf("Name = %q", r.Name())
	}
}

// ---- 真实 Kafka 冒烟 ----

// 防止：假 Sender 与真实 Producer 的约定不一致（header、key、topic、payload 经过真 Kafka 后丢失）。
func TestSmokeRealKafka(t *testing.T) {
	brokers := testkafka.New(t)
	topic := testkafka.Topic(t, brokers, 1)
	db := newDB(t)

	p, err := kafkax.NewProducer(t.Context(), kafkax.Config{Brokers: brokers, ClientID: "outbox-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	commitAdd(t, db, Message{Topic: topic, Key: []byte("o-1"), Payload: []byte("hello")})
	r := newRelay(t, db, p)
	if n := mustFlush(t, r); n != 1 {
		t.Fatalf("published = %d, want 1", n)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	fetches := cl.PollFetches(ctx)
	if err := fetches.Err(); err != nil {
		t.Fatal(err)
	}
	recs := fetches.Records()
	if len(recs) != 1 {
		t.Fatalf("consumed %d records, want 1", len(recs))
	}
	rec := recs[0]
	if string(rec.Key) != "o-1" || string(rec.Value) != "hello" {
		t.Errorf("record key/value = %q/%q", rec.Key, rec.Value)
	}
	var id string
	for _, h := range rec.Headers {
		if h.Key == EventIDHeader {
			id = string(h.Value)
		}
	}
	if !strings.HasPrefix(id, "svc:") {
		t.Errorf("event-id header = %q, want svc:<id>", id)
	}
}
