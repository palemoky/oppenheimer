package capture

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/testdb"
)

// checkRow 检查一行映像里各类型的值。
func checkRow(t *testing.T, img map[string]*cdcv1.Value, orderID int64) {
	t.Helper()
	wantCols := []string{
		"currency", "expire_at", "fail_reason", "merchant_id", "note", "order_id", "paid_at", "payload", "price",
		"qty", "ratio", "request_hash", "score", "status", "tag", "total_amount", "user_id", "version",
	}
	if got := slices.Sorted(maps.Keys(img)); !slices.Equal(got, wantCols) {
		t.Errorf("columns = %v, want %v", got, wantCols)
	}
	checks := []struct {
		col  string
		want *cdcv1.Value
	}{
		{"order_id", intValue(orderID)},
		{"status", intValue(2)},
		{"currency", strValue("USD")},
		{"request_hash", rawValue(requestHash)},
		{"fail_reason", &cdcv1.Value{}},
		{"merchant_id", &cdcv1.Value{}},
		{"expire_at", strValue("2026-06-01 12:00:00.123456")},
		{"qty", uintValue(4000000000)},
		{"ratio", &cdcv1.Value{Kind: &cdcv1.Value_Float{Float: 0.1}}},
		{"score", &cdcv1.Value{Kind: &cdcv1.Value_Float{Float: 0.1}}},
		{"price", strValue("12.30")},
		{"note", strValue("hello")},
		{"tag", rawValue([]byte{0, 1})},
		{"payload", rawValue([]byte{1, 2, 3})},
	}
	for _, c := range checks {
		if got := img[c.col]; !proto.Equal(got, c.want) {
			t.Errorf("column %s = %v, want %v", c.col, got, c.want)
		}
	}
}

func checkBinlogMeta(t *testing.T, m *cdcv1.RowChange, slot int32) {
	t.Helper()
	if m.GetSource() != "order-test" || m.GetTable() != "orders" || m.GetSlot() != slot {
		t.Errorf("source/table/slot = %q/%q/%d, want order-test/orders/%d", m.GetSource(), m.GetTable(), m.GetSlot(), slot)
	}
	if !strings.Contains(m.GetGtid(), ":") {
		t.Errorf("gtid = %q, want uuid:n", m.GetGtid())
	}
	if age := time.Since(time.UnixMicro(m.GetCommitMicros())); age < -time.Minute || age > time.Minute {
		t.Errorf("commit_micros = %d is %v away from now", m.GetCommitMicros(), age)
	}
}

func TestStreamsInsertUpdateDelete(t *testing.T) {
	t.Parallel()
	e := newEnv(t, "orders_0", "orders_3", "order_items_3")
	// 另一个 schema 里的同名表：不应产生消息。
	other, _ := testdb.Empty(t)
	testdb.MustExec(t, other, fmt.Sprintf(ordersDDL, "orders_3"))

	p := e.pipeline(serverIDs.Add(1))
	r := e.start(p)
	e.waitStreaming(r)
	c := e.collector()

	insertOrder(t, e.db, "orders_3", 101)
	testdb.MustExec(t, e.db, "UPDATE orders_3 SET status = 3, fail_reason = 'X', version = version + 1 WHERE order_id = 101")
	testdb.MustExec(t, e.db, "UPDATE orders_3 SET status = 4 WHERE order_id = 101") // 忘了递增版本
	testdb.MustExec(t, e.db, "DELETE FROM orders_3 WHERE order_id = 101")
	insertOrder(t, e.db, "order_items_3", 201)
	insertOrder(t, other, "orders_3", 301)
	// 一个事务里两行：每行一条消息。
	testdb.MustExec(t, e.db, `INSERT INTO orders_0 (order_id, user_id, status, request_hash, expire_at)
		VALUES (401, 1, 1, '', '2026-01-01 00:00:00'), (402, 1, 1, '', '2026-01-01 00:00:00')`)

	c.waitFor("all changes", func(ms []*cdcv1.RowChange) bool {
		return find(ms, 402, cdcv1.Op_OP_INSERT) != nil && find(ms, 401, cdcv1.Op_OP_INSERT) != nil
	})
	msgs := c.drain(500 * time.Millisecond)

	var ops []cdcv1.Op
	for _, m := range msgs {
		if orderIDOf(m) == 101 {
			ops = append(ops, m.GetOp())
		}
	}
	wantOps := []cdcv1.Op{cdcv1.Op_OP_INSERT, cdcv1.Op_OP_UPDATE, cdcv1.Op_OP_UPDATE, cdcv1.Op_OP_DELETE}
	if !slices.Equal(ops, wantOps) {
		t.Fatalf("ops for order 101 = %v, want %v", ops, wantOps)
	}
	if len(msgs) != 6 {
		t.Errorf("got %d messages, want 6 (other tables and schemas must be ignored)", len(msgs))
	}
	for i, m := range msgs {
		if want := strconv.FormatInt(orderIDOf(m), 10); c.keys[i] != want {
			t.Errorf("message %d key = %q, want %q", i, c.keys[i], want)
		}
	}

	var updates []*cdcv1.RowChange
	for _, m := range msgs {
		switch {
		case orderIDOf(m) != 101:
			checkBinlogMeta(t, m, 0)
		case m.GetOp() == cdcv1.Op_OP_INSERT:
			checkBinlogMeta(t, m, 3)
			if m.GetBefore() != nil {
				t.Errorf("insert has before: %v", m.GetBefore())
			}
			checkRow(t, m.GetAfter(), 101)
		case m.GetOp() == cdcv1.Op_OP_UPDATE:
			checkBinlogMeta(t, m, 3)
			updates = append(updates, m)
		case m.GetOp() == cdcv1.Op_OP_DELETE:
			checkBinlogMeta(t, m, 3)
			if m.GetAfter() != nil {
				t.Errorf("delete has after: %v", m.GetAfter())
			}
			if got := m.GetBefore()["status"].GetInt(); got != 4 {
				t.Errorf("delete before.status = %d, want 4", got)
			}
		}
	}
	u := updates[0]
	checkRow(t, u.GetBefore(), 101)
	if u.GetAfter()["status"].GetInt() != 3 || u.GetAfter()["fail_reason"].GetStr() != "X" || u.GetAfter()["version"].GetInt() != 1 {
		t.Errorf("update after = %v", u.GetAfter())
	}
	if u.GetGtid() == updates[1].GetGtid() {
		t.Errorf("two transactions share gtid %q", u.GetGtid())
	}

	if got := p.VersionAnomalies(); got != 1 {
		t.Errorf("VersionAnomalies() = %d, want 1", got)
	}
	if err := r.stop(t); err != nil {
		t.Errorf("Start returned %v after stop", err)
	}
}

// 先有数据再第一次启动：全量发 OP_READ，随后的变更作为增量出现；同一行从全量和 binlog 出来的映像相同。
// 然后 -reset-checkpoint（写墓碑）再启动，重新全量。
func TestSnapshotThenStreamAndReset(t *testing.T) {
	t.Parallel()
	e := newEnv(t, "orders_1", "orders_2")
	insertOrder(t, e.db, "orders_1", 11)
	insertOrder(t, e.db, "orders_2", 21)
	insertOrder(t, e.db, "orders_2", 22)

	serverID := serverIDs.Add(1)
	r := e.start(e.pipeline(serverID))
	c := e.collector()
	msgs := c.waitFor("snapshot", func(ms []*cdcv1.RowChange) bool { return len(ms) >= 3 })
	for _, m := range msgs {
		if m.GetOp() != cdcv1.Op_OP_READ || m.GetGtid() != "" || m.GetCommitMicros() != 0 || m.GetBefore() != nil {
			t.Errorf("snapshot message = op %v gtid %q commit %d", m.GetOp(), m.GetGtid(), m.GetCommitMicros())
		}
		checkRow(t, m.GetAfter(), orderIDOf(m))
	}
	if m := find(msgs, 22, cdcv1.Op_OP_READ); m == nil || m.GetSlot() != 2 {
		t.Errorf("order 22 snapshot = %v, want slot 2", m)
	}
	e.waitStreaming(r)

	// 更新的 before 是 binlog 给出的同一行映像，必须和全量读到的 after 完全相同。
	testdb.MustExec(t, e.db, "UPDATE orders_2 SET version = version + 1 WHERE order_id = 21")
	insertOrder(t, e.db, "orders_1", 12)
	msgs = c.waitFor("stream", func(ms []*cdcv1.RowChange) bool {
		return find(ms, 12, cdcv1.Op_OP_INSERT) != nil && find(ms, 21, cdcv1.Op_OP_UPDATE) != nil
	})
	firstRun := len(msgs)
	read := find(msgs, 21, cdcv1.Op_OP_READ)
	upd := find(msgs, 21, cdcv1.Op_OP_UPDATE)
	if upd == nil || !mapsEqual(read.GetAfter(), upd.GetBefore()) {
		t.Errorf("snapshot image %v\n!= binlog image %v", read.GetAfter(), upd.GetBefore())
	}
	ins := find(msgs, 12, cdcv1.Op_OP_INSERT)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}

	// 重做全量：所有现存的行再以 OP_READ 发一次，插入的行的映像和 binlog 的一致。
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := e.store.Reset(ctx, "order-test"); err != nil {
		t.Fatal(err)
	}
	if cp, err := e.store.Load(ctx, "order-test"); err != nil || cp != nil {
		t.Fatalf("Load after reset = %v, %v; want nil", cp, err)
	}
	r = e.start(e.pipeline(serverID))
	msgs = c.waitFor("second snapshot", func(ms []*cdcv1.RowChange) bool {
		n := 0
		for _, m := range ms {
			if m.GetOp() == cdcv1.Op_OP_READ {
				n++
			}
		}
		return n >= 7
	})
	var reads []*cdcv1.RowChange
	for _, m := range msgs[firstRun:] {
		if m.GetOp() == cdcv1.Op_OP_READ {
			reads = append(reads, m)
		}
	}
	if len(reads) != 4 {
		t.Fatalf("second snapshot sent %d rows, want 4", len(reads))
	}
	if again := find(reads, 12, cdcv1.Op_OP_READ); again == nil || !mapsEqual(again.GetAfter(), ins.GetAfter()) {
		t.Errorf("snapshot image %v\n!= binlog insert image %v", again.GetAfter(), ins.GetAfter())
	}
	e.waitStreaming(r)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
}

func mapsEqual(a, b map[string]*cdcv1.Value) bool {
	return maps.EqualFunc(a, b, func(x, y *cdcv1.Value) bool { return proto.Equal(x, y) })
}

// 全量读完第一批后停止，再启动从游标继续：每一行恰好发一次。
func TestSnapshotResumesFromCursor(t *testing.T) {
	t.Parallel()
	tables := []string{"orders_0", "orders_1", "orders_2"}
	e := newEnv(t, tables...)
	var want []int64
	for i, table := range tables {
		for j := range 5 {
			id := int64(i*100 + j + 1)
			insertOrder(t, e.db, table, id)
			want = append(want, id)
		}
	}

	serverID := serverIDs.Add(1)
	p := e.pipeline(serverID, withBatch(2))
	p.afterSnapshotBatch = p.requestStop
	r := e.start(p)
	if err := r.wait(); err != nil {
		t.Fatalf("first run: %v", err)
	}
	c := e.collector()
	msgs := c.drain(time.Second)
	if len(msgs) != 2 {
		t.Fatalf("first run sent %d rows, want one batch of 2", len(msgs))
	}
	cp, err := e.store.Load(t.Context(), "order-test")
	if err != nil {
		t.Fatal(err)
	}
	if cur := cp.GetSnapshot(); cur.GetTable() != "orders_0" || cur.GetLastKey() != 2 {
		t.Fatalf("cursor = %v, want orders_0/2", cur)
	}

	r = e.start(e.pipeline(serverID, withBatch(2)))
	c.waitFor("all rows", func(ms []*cdcv1.RowChange) bool { return len(ms) >= len(want) })
	e.waitStreaming(r)
	msgs = c.drain(500 * time.Millisecond)
	var got []int64
	for _, m := range msgs {
		got = append(got, orderIDOf(m))
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("rows sent = %v, want each of %v exactly once", got, want)
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
}

// 从位点重启不丢变更。优雅停止会落最后一次位点，不重发；崩溃时重发的只限最后一个位点之后的事务。
func TestRestartFromCheckpoint(t *testing.T) {
	t.Parallel()
	e := newEnv(t, "orders_0")
	serverID := serverIDs.Add(1)
	c := e.collector()
	next := int64(1)
	insertN := func(n int) (ids []int64) {
		for range n {
			insertOrder(t, e.db, "orders_0", next)
			ids = append(ids, next)
			next++
		}
		return ids
	}
	waitAll := func(desc string, ids []int64) {
		c.waitFor(desc, func(ms []*cdcv1.RowChange) bool {
			return find(ms, ids[len(ids)-1], cdcv1.Op_OP_INSERT) != nil
		})
	}

	// 第一段：优雅停止。
	r := e.start(e.pipeline(serverID))
	e.waitStreaming(r)
	first := insertN(20)
	waitAll("first", first)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	// 停止期间的写入，重启后要能读到。
	whileDown := insertN(5)

	// 第二段：读完之后等位点推进，再写一批，读到后立刻“崩溃”。
	r = e.start(e.pipeline(serverID))
	waitAll("while down", whileDown)
	second := insertN(20)
	waitAll("second", second)
	time.Sleep(500 * time.Millisecond) // 几个位点间隔，覆盖 second
	unsaved := insertN(5)
	waitAll("unsaved", unsaved)
	if err := r.crash(); err == nil {
		t.Log("crash returned nil; final checkpoint may have been written")
	}

	// 第三段：再启动，写最后一批。
	r = e.start(e.pipeline(serverID))
	last := insertN(5)
	waitAll("last", last)
	msgs := c.drain(500 * time.Millisecond)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}

	counts := map[int64]int{}
	for _, m := range msgs {
		counts[orderIDOf(m)]++
	}
	for id := range next - 1 {
		if counts[id+1] == 0 {
			t.Errorf("order %d was lost", id+1)
		}
	}
	// 重发只允许出现在崩溃前没来得及落位点的那一批。
	for id, n := range counts {
		if n > 1 && !slices.Contains(unsaved, id) {
			t.Errorf("order %d sent %d times; only the last batch before the crash may be resent", id, n)
		}
	}
}

// 位点之后的日志已被清掉（gtid_purged 不是位点的子集）时拒绝启动。
// 测试库的 gtid_purged 是空的，清日志又会影响别的测试，所以替换读 gtid_purged 的查询来模拟。
func TestRefusesPurgedCheckpoint(t *testing.T) {
	t.Parallel()
	e := newEnv(t, "orders_0")
	var executed string
	if err := e.db.QueryRowContext(t.Context(), "SELECT @@global.gtid_executed").Scan(&executed); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Save(t.Context(), "order-test", &cdcv1.Checkpoint{GtidSet: normalizeGTIDSet(executed)}); err != nil {
		t.Fatal(err)
	}

	p := e.pipeline(serverIDs.Add(1))
	p.purgedQuery = "SELECT '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5'"
	err := e.start(p).wait()
	if err == nil || !strings.Contains(err.Error(), "-reset-checkpoint=order-test") {
		t.Fatalf("Start = %v, want purged error suggesting -reset-checkpoint", err)
	}
}

func TestCheckpointStoreKeepsLatestPerKey(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := t.Context()
	for i, name := range []string{"a", "b", "a"} {
		if err := e.store.Save(ctx, name, &cdcv1.Checkpoint{GtidSet: fmt.Sprintf("u:%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	for name, want := range map[string]string{"a": "u:2", "b": "u:1"} {
		cp, err := e.store.Load(ctx, name)
		if err != nil || cp.GetGtidSet() != want {
			t.Errorf("Load(%s) = %v, %v; want %s", name, cp, err, want)
		}
	}
	if cp, err := e.store.Load(ctx, "c"); err != nil || cp != nil {
		t.Errorf("Load(c) = %v, %v; want nil", cp, err)
	}
}
