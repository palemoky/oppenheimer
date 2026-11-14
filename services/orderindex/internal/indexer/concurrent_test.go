package indexer

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/storetest"
)

// 本文件模拟 kafkax 按批消费（NewBatchConsumer）时对 Handler 的调用方式，验证多个分区同时写同一批索引表时
// 索引仍然收敛、统计与输入一致、SQL 层的锁冲突都是可重试的。
//
// 模拟遵守 kafkax 的保证：每个分区一个 worker，分区内按投递顺序一批接一批地处理，一批是分区里连续的一段；
// 一批失败时退回逐条处理，逐条时原地重试。不同分区之间任意并发。

const (
	concurrentOrders   = 500
	concurrentVersions = 5
	// 商家号 1..20：slot = merchant & 3（sharding.MerchantIndexLayout），覆盖 4 张表、2 个库。
	concurrentMerchants = 20
	// concurrentPartitions 和生产的 cdc.order.orders 分区数（eventx.Partitions）一致。
	concurrentPartitions = 4
	// concurrentMaxBatch 是这些测试里一批的上限。批的大小在 [1, 它] 里随机，覆盖单条的批和多行语句。
	concurrentMaxBatch = 50
	// concurrentPool 是并发测试里每个库的连接池上限。测试库和本机运行中的服务共用一个 MySQL 实例，
	// 这几个测试不并行、池也压小，避免把连接打满。4 个分区每批每库最多占一条连接，8 足够。
	concurrentPool = 8
)

// newCappedStore 和 storetest.New 一样，但把每个库的连接池上限设为 pool。
func newCappedStore(t *testing.T, pool int) (*store.Store, []*sql.DB) {
	t.Helper()
	st, dbs := storetest.New(t)
	for _, db := range dbs {
		db.SetMaxOpenConns(pool)
		db.SetMaxIdleConns(pool)
	}
	return st, dbs
}

// plannedOrder 是一个订单的全部变更：版本 1 是 INSERT，2..4 是 UPDATE，5 是 UPDATE 或归档的 DELETE。
type plannedOrder struct {
	merchant int64 // 0 表示 merchant_id 为 NULL，不进索引
	order    int64
	msgs     []*cdcv1.RowChange // 按版本升序，即源库上的提交顺序
	deleted  bool
}

// planOrders 生成 n 个订单：每 10 个里有 1 个没有商家，每 3 个里有 1 个最后被归档。
func planOrders(n int) []plannedOrder {
	out := make([]plannedOrder, n)
	for i := range n {
		p := plannedOrder{order: 1_000_000 + int64(i)*7, merchant: 1 + int64(i)%concurrentMerchants}
		if i%10 == 9 {
			p.merchant = 0
		}
		p.deleted = i%3 == 0
		for v := range int64(concurrentVersions) {
			version := v + 1
			img := image(p.merchant, p.order, version)
			switch {
			case version == 1:
				p.msgs = append(p.msgs, change(cdcv1.Op_OP_INSERT, img))
			case version == concurrentVersions && p.deleted:
				p.msgs = append(p.msgs, deletion(img))
			default:
				p.msgs = append(p.msgs, change(cdcv1.Op_OP_UPDATE, img))
			}
		}
		out[i] = p
	}
	return out
}

// delivery 是一次投递：订单号（决定分区）、序列化后的消息、原消息（算期望的统计用）。
type delivery struct {
	key int64
	msg kafkax.Message
	ch  *cdcv1.RowChange
}

func newDelivery(t testing.TB, key int64, m *cdcv1.RowChange) delivery {
	t.Helper()
	d := delivery{key: key, msg: message(t, m, 0), ch: m}
	d.msg.Key = []byte(strconv.FormatInt(key, 10))
	return d
}

// interleave 把每个订单各自有序的投递序列随机交织成一个投递流：每个订单内部的先后不变，
// 不同订单之间的先后是均匀随机的。
func interleave(rng *rand.Rand, perKey [][]delivery) []delivery {
	var owners []int
	for i, ds := range perKey {
		for range ds {
			owners = append(owners, i)
		}
	}
	rng.Shuffle(len(owners), func(i, j int) { owners[i], owners[j] = owners[j], owners[i] })
	next := make([]int, len(perKey))
	out := make([]delivery, 0, len(owners))
	for _, o := range owners {
		out = append(out, perKey[o][next[o]])
		next[o]++
	}
	return out
}

// partitionOf 是订单号到分区的映射（模拟按 key 哈希分区）。
func partitionOf(key int64) int { return int(key % concurrentPartitions) }

// splitPartitions 把投递流按 partition(d) 分到 concurrentPartitions 个分区，分区内保持投递顺序。
func splitPartitions(stream []delivery, partition func(delivery) int) [][]delivery {
	parts := make([][]delivery, concurrentPartitions)
	for _, d := range stream {
		p := partition(d)
		parts[p] = append(parts[p], d)
	}
	return parts
}

// cutBatches 把一个分区的投递流切成连续的批。rng 为 nil 时每批 maxBatch 条（最后一批可能少），
// 否则每批的大小在 [1, maxBatch] 里随机。
func cutBatches(rng *rand.Rand, part []delivery, maxBatch int) [][]delivery {
	var out [][]delivery
	for len(part) > 0 {
		n := maxBatch
		if rng != nil {
			n = 1 + rng.IntN(maxBatch)
		}
		n = min(n, len(part))
		out = append(out, part[:n])
		part = part[n:]
	}
	return out
}

// runStats 是一次模拟消费的过程数据。
type runStats struct {
	batchFailures atomic.Int64 // 多条的批失败、退回逐条的次数
	retries       atomic.Int64 // 逐条时因锁冲突原地重试的次数
}

// consumeBatch 像 kafkax 的 worker.runBatches 那样处理一批：多于一条时先整批调用；失败时逐条，
// 逐条时照 MaxAttempts 无限那样原地重试。这些测试的输入都是好消息，只容忍可重试的锁冲突
// （死锁、锁等待超时）：它们不能是 Permanent（否则会进死信、丢掉这个版本），其他任何错误都让测试失败。
func consumeBatch(t testing.TB, h kafkax.BatchHandler, batch []delivery, rs *runStats) bool {
	t.Helper()
	isLockConflict := func(err error) bool {
		return !kafkax.IsPermanent(err) && (mysqlx.IsDeadlock(err) || mysqlx.IsLockWaitTimeout(err))
	}
	if len(batch) > 1 {
		msgs := make([]kafkax.Message, len(batch))
		for i, d := range batch {
			msgs[i] = d.msg
		}
		err := h(t.Context(), msgs)
		if err == nil {
			return true
		}
		if !isLockConflict(err) {
			t.Errorf("batch of %d: unexpected error (permanent=%v): %v", len(batch), kafkax.IsPermanent(err), err)
			return false
		}
		rs.batchFailures.Add(1)
	}
	for _, d := range batch {
		for attempt := 1; ; attempt++ {
			err := h(t.Context(), []kafkax.Message{d.msg})
			if err == nil {
				break
			}
			if !isLockConflict(err) {
				t.Errorf("order %d: unexpected error (permanent=%v): %v", d.key, kafkax.IsPermanent(err), err)
				return false
			}
			if attempt >= 100 {
				t.Errorf("order %d: still failing after %d attempts: %v", d.key, attempt, err)
				return false
			}
			rs.retries.Add(1)
		}
	}
	return true
}

// runPartitions 每个分区一个 goroutine，按顺序处理各自的批，分区之间并行。
func runPartitions(t testing.TB, h kafkax.BatchHandler, parts [][][]delivery) *runStats {
	t.Helper()
	rs := &runStats{}
	var wg sync.WaitGroup
	for _, batches := range parts {
		wg.Go(func() {
			for _, b := range batches {
				if !consumeBatch(t, h, b, rs) {
					return
				}
			}
		})
	}
	wg.Wait()
	return rs
}

// verifyIndex 检查索引恰好包含有商家的订单（行数、商家归属），每一行逐列等于最高版本，
// 并且 4 张表都有数据（覆盖了全部 slot 和两个库）。
func verifyIndex(t *testing.T, st *store.Store, dbs []*sql.DB, orders []plannedOrder) {
	t.Helper()
	want := map[int64]int64{} // order -> merchant
	for _, p := range orders {
		if p.merchant != 0 {
			want[p.order] = p.merchant
		}
	}
	r := st.Router()
	got := map[int64]int64{}
	for slot := range r.Slots() {
		table := sharding.Table(store.TableBase, slot)
		n := 0
		for _, k := range scanIndexKeys(t, dbs[r.DB(slot)], table) {
			m, o := k[0], k[1]
			if r.ForMerchant(m) != slot {
				t.Errorf("order %d of merchant %d is in %s", o, m, table)
			}
			if _, dup := got[o]; dup {
				t.Errorf("order %d indexed twice", o)
			}
			got[o] = m
			n++
		}
		if n == 0 {
			t.Errorf("%s is empty: the test does not cover every slot", table)
		}
	}
	if len(got) != len(want) {
		t.Errorf("index has %d rows, want %d", len(got), len(want))
	}
	for o, m := range got {
		if wm, ok := want[o]; !ok || wm != m {
			t.Errorf("unexpected index row order=%d merchant=%d (want merchant %d, present=%v)", o, m, wm, ok)
		}
	}
	for _, p := range orders {
		if p.merchant == 0 {
			continue
		}
		row, err := st.Get(t.Context(), p.merchant, p.order)
		if err != nil {
			t.Errorf("order %d: %v", p.order, err)
			continue
		}
		if w := wantRow(t, p.merchant, p.order, concurrentVersions); !sameImage(row, w) {
			t.Errorf("order %d: row = %+v, want %+v", p.order, row, w)
		}
	}
}

// scanIndexKeys 返回一张索引表里全部行的 (merchant_id, order_id)。
func scanIndexKeys(t *testing.T, db *sql.DB, table string) [][2]int64 {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT merchant_id, order_id FROM "+table) //nolint:gosec // 表名只来自 sharding.Table
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys [][2]int64
	for rows.Next() {
		var k [2]int64
		if err := rows.Scan(&k[0], &k[1]); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return keys
}

// perOrderInOrder 返回每个订单按版本顺序的投递序列。
func perOrderInOrder(t testing.TB, orders []plannedOrder) [][]delivery {
	perKey := make([][]delivery, len(orders))
	for i, p := range orders {
		for _, m := range p.msgs {
			perKey[i] = append(perKey[i], newDelivery(t, p.order, m))
		}
	}
	return perKey
}

// messageCounts 是与写库无关、只由输入决定的统计：精确值。Changed+Stale 放在 Indexed 里。
type messageCounts struct {
	Indexed, Skipped, Archived int64
}

// hasMerchant 报告这条变更（INSERT/UPDATE 看 after，DELETE 看 before）带不带商家号。
func hasMerchant(ch *cdcv1.RowChange) bool {
	img := ch.GetAfter()
	if ch.GetOp() == cdcv1.Op_OP_DELETE {
		img = ch.GetBefore()
	}
	return img["merchant_id"].GetKind() != nil
}

func countMessages(stream []delivery) messageCounts {
	var c messageCounts
	for _, d := range stream {
		if d.ch.GetOp() == cdcv1.Op_OP_DELETE {
			c.Archived++
		}
		if hasMerchant(d.ch) {
			c.Indexed++
		} else {
			c.Skipped++
		}
	}
	return c
}

// checkMessageCounts 断言统计里与写库无关的部分逐项精确，Changed + Stale 等于有商家的消息数。
func checkMessageCounts(t *testing.T, got Counts, want messageCounts) {
	t.Helper()
	if got.Changed+got.Stale != want.Indexed || got.Written+got.Merged != want.Indexed ||
		got.SkippedNoMerchant != want.Skipped || got.Archived != want.Archived || got.Ignored != 0 ||
		got.LagCount != want.Indexed {
		t.Errorf("stats = %+v\nwant changed+stale = written+merged = lag = %d, skipped %d, archived %d",
			got, want.Indexed, want.Skipped, want.Archived)
	}
}

// 5 + 6：4 个分区各自按批处理（批的大小随机），分区之间订单不重叠、索引表重叠（每个分区都写全部 4 张表）。
// 同一订单的消息保持顺序。索引里每个订单是最高版本。
// 统计逐项精确：每一批写进去的每一行都比索引里已有的新（同一订单只在一个分区、按顺序到达），
// 所以没有“写了没变”的行，Changed 的估计等于真实值：Changed = Written，Stale = Merged。
// Written 由切好的批算出来：每一批里有商家的不同订单数。
func TestPartitionsInOrder(t *testing.T) {
	st, dbs := newCappedStore(t, concurrentPool)
	ix, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	rng := seededRand(t)
	orders := planOrders(concurrentOrders)
	stream := interleave(rng, perOrderInOrder(t, orders))
	parts := make([][][]delivery, 0, concurrentPartitions)
	var written int64
	for _, part := range splitPartitions(stream, func(d delivery) int { return partitionOf(d.key) }) {
		batches := cutBatches(rng, part, concurrentMaxBatch)
		for _, b := range batches {
			distinct := map[int64]bool{}
			for _, d := range b {
				if hasMerchant(d.ch) {
					distinct[d.key] = true
				}
			}
			written += int64(len(distinct))
		}
		parts = append(parts, batches)
	}

	rs := runPartitions(t, ix.Handler(DefaultWriteTimeout), parts)
	if t.Failed() {
		t.FailNow()
	}
	t.Logf("batch failures %d, lock-conflict retries %d", rs.batchFailures.Load(), rs.retries.Load())
	verifyIndex(t, st, dbs, orders)

	got, want := ix.Stats().take(), countMessages(stream)
	checkMessageCounts(t, got, want)
	if rs.batchFailures.Load() > 0 {
		// 退回逐条之后写入的行数和切批时算的不一样了（还可能有一个库已提交的一半被重做成空操作），只查总数。
		t.Logf("a batch fell back to one-by-one; skipping the exact written/changed check")
		return
	}
	if got.Written != written || got.Changed != written || got.Stale != want.Indexed-written || got.Merged != got.Stale {
		t.Errorf("stats = %+v\nwant written = changed = %d, stale = merged = %d", got, written, want.Indexed-written)
	}
}

// 5：重放跨分区。每个订单按顺序投给自己的分区；另外从一个随机的版本 r 起按顺序把 r..5 再投给下一个分区
// （模拟再平衡时两个消费者都处理过同一段）。于是同一个订单会同时出现在两个分区的批里，
// 两个事务会抢同一批行的锁。允许出现死锁、锁等待超时（重试后成功），最终每个订单仍是最高版本；
// 与写库无关的统计逐项精确。
func TestPartitionsReplayAcrossPartitions(t *testing.T) {
	st, dbs := newCappedStore(t, concurrentPool)
	ix, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	rng := seededRand(t)
	orders := planOrders(concurrentOrders)
	perKey := perOrderInOrder(t, orders)
	for i, p := range orders {
		from := rng.IntN(len(p.msgs))
		for _, m := range p.msgs[from:] {
			perKey[i] = append(perKey[i], newDelivery(t, p.order, m))
		}
	}
	// 交织成一个流，每个订单的前 len(msgs) 条投给自己的分区，之后的重放投给下一个分区。
	var owners []int
	for i, ds := range perKey {
		for range ds {
			owners = append(owners, i)
		}
	}
	rng.Shuffle(len(owners), func(i, j int) { owners[i], owners[j] = owners[j], owners[i] })
	var (
		stream   []delivery
		split    = make([][]delivery, concurrentPartitions)
		next     = make([]int, len(perKey))
		replayed int
	)
	for _, o := range owners {
		d := perKey[o][next[o]]
		p := partitionOf(d.key)
		if next[o] >= len(orders[o].msgs) {
			p = (p + 1) % concurrentPartitions
			replayed++
		}
		split[p] = append(split[p], d)
		stream = append(stream, d)
		next[o]++
	}
	parts := make([][][]delivery, len(split))
	for i, part := range split {
		parts[i] = cutBatches(rng, part, concurrentMaxBatch)
	}

	rs := runPartitions(t, ix.Handler(DefaultWriteTimeout), parts)
	if t.Failed() {
		t.FailNow()
	}
	t.Logf("%d replayed messages; batch failures %d, lock-conflict retries %d",
		replayed, rs.batchFailures.Load(), rs.retries.Load())
	verifyIndex(t, st, dbs, orders)
	got := ix.Stats().take()
	checkMessageCounts(t, got, countMessages(stream))
}

// 2：每条消息重复投递 1 到 3 次，同一订单内的顺序也打乱（等价于旧版本在新版本之后又来一遍），
// 分区自己的订单不重叠，批的大小随机。结果与按序投递相同。统计里能精确断言的是与写库无关的部分；
// Changed 是估计，只断言范围：每个订单至少改一次（不会偏小），至多等于写入的行数。
func TestPartitionsDuplicatesAndReorder(t *testing.T) {
	st, dbs := newCappedStore(t, concurrentPool)
	ix, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	rng := seededRand(t)
	orders := planOrders(concurrentOrders)
	perKey := make([][]delivery, len(orders))
	var merchantCount int64
	for i, p := range orders {
		if p.merchant != 0 {
			merchantCount++
		}
		for _, m := range p.msgs {
			for range 1 + rng.IntN(3) {
				perKey[i] = append(perKey[i], newDelivery(t, p.order, m))
			}
		}
		rng.Shuffle(len(perKey[i]), func(a, b int) { perKey[i][a], perKey[i][b] = perKey[i][b], perKey[i][a] })
	}
	stream := interleave(rng, perKey)
	var parts [][][]delivery
	for _, part := range splitPartitions(stream, func(d delivery) int { return partitionOf(d.key) }) {
		parts = append(parts, cutBatches(rng, part, concurrentMaxBatch))
	}
	rs := runPartitions(t, ix.Handler(DefaultWriteTimeout), parts)
	if t.Failed() {
		t.FailNow()
	}
	t.Logf("batch failures %d, lock-conflict retries %d", rs.batchFailures.Load(), rs.retries.Load())
	verifyIndex(t, st, dbs, orders)

	got := ix.Stats().take()
	checkMessageCounts(t, got, countMessages(stream))
	if got.Changed < merchantCount || got.Changed > got.Written {
		t.Errorf("changed = %d, want within [%d, written %d]", got.Changed, merchantCount, got.Written)
	}
}

// 5（极端情况）：不按分区，8 个 goroutine 从同一个打乱、带重复的消息池里随机取批，
// 同一个订单的不同版本会同时出现在多个事务里、批之间任意重叠。生产里不会这样（同一订单只在一个分区），
// 这里用来确认按版本的 upsert 在最坏的并发下也收敛，锁冲突都是可重试的。
func TestConvergesUnderReorderDuplicatesAndConcurrency(t *testing.T) {
	st, dbs := newCappedStore(t, concurrentPool)
	ix, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	rng := seededRand(t)
	orders := planOrders(200)
	var pool []delivery
	for _, p := range orders {
		for _, m := range p.msgs {
			for range 1 + rng.IntN(3) {
				pool = append(pool, newDelivery(t, p.order, m))
			}
		}
	}
	rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	batches := cutBatches(rng, pool, 30)
	ch := make(chan []delivery)
	rs := &runStats{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for b := range ch {
				consumeBatch(t, ix.Handler(DefaultWriteTimeout), b, rs)
			}
		})
	}
	for _, b := range batches {
		ch <- b
	}
	close(ch)
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	t.Logf("batch failures %d, lock-conflict retries %d", rs.batchFailures.Load(), rs.retries.Load())
	verifyIndex(t, st, dbs, orders)
	checkMessageCounts(t, ix.Stats().take(), countMessages(pool))
}

func seededRand(t testing.TB) *rand.Rand {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	return rand.New(rand.NewPCG(seed, seed))
}

// 不同批大小下的吞吐，只输出不断言（测试机的绝对速度不稳定）。4 个分区并行，和生产的分区数一致；
// 连接池每库 concurrentPool。
//
// 输入是两轮：先每个订单一条 INSERT（v1），再每个订单一条 UPDATE（v2），每轮内订单之间无序。
// 这样一批里几乎没有同一订单的多个版本可合并，量到的是“写库行数”的吞吐，而不是合并省掉的写入；
// 第一轮全是插入、第二轮全是覆盖，两种写都量到了。
//
// 规模取得小，因为 make test 和 CI 每次都会跑它，批大小 1 那一档占了大部分时间。
// 要看更稳定的数字可以临时调大 orders，并去掉 -race 跑（竞态检测会把每条的耗时放大）。
func TestThroughputByBatchSize(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput measurement skipped in -short mode")
	}
	const orders = 2000
	rng := seededRand(t)
	var stream []delivery
	for v := int64(1); v <= 2; v++ {
		var round []delivery
		for i := range int64(orders) {
			order := 2_000_000 + i
			op := cdcv1.Op_OP_INSERT
			if v > 1 {
				op = cdcv1.Op_OP_UPDATE
			}
			round = append(round, newDelivery(t, order, change(op, image(1+i%concurrentMerchants, order, v))))
		}
		rng.Shuffle(len(round), func(i, j int) { round[i], round[j] = round[j], round[i] })
		stream = append(stream, round...)
	}
	for _, size := range []int{1, 50, 500} {
		t.Run(fmt.Sprintf("batch=%d", size), func(t *testing.T) {
			st, _ := newCappedStore(t, concurrentPool)
			ix, err := New(st, nil)
			if err != nil {
				t.Fatal(err)
			}
			var parts [][][]delivery
			for _, part := range splitPartitions(stream, func(d delivery) int { return partitionOf(d.key) }) {
				parts = append(parts, cutBatches(nil, part, size))
			}
			start := time.Now()
			rs := runPartitions(t, ix.Handler(DefaultWriteTimeout), parts)
			elapsed := time.Since(start)
			w := ix.Stats().take()
			t.Logf("batch %3d, %d partitions, pool %d/db: %d msgs, %d rows written in %v = %.0f rows/s "+
				"(batch failures %d, lock-conflict retries %d)",
				size, concurrentPartitions, concurrentPool, len(stream), w.Written, elapsed.Round(time.Millisecond),
				float64(w.Written)/elapsed.Seconds(), rs.batchFailures.Load(), rs.retries.Load())
		})
	}
}
