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

	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/storetest"
)

// 本文件模拟 kafkax 分区内按 key 并发（ConsumerConfig.Concurrency > 1）时对 Handler 的调用方式，
// 验证并发下索引仍然收敛、统计准确、SQL 层不出未处理的死锁。
//
// 模拟遵守 kafkax 的保证：同一个订单号（key）的消息只在一条 lane 上按投递顺序串行处理，可以重复投递；
// 不同订单号之间任意并发、任意先后。

const (
	concurrentOrders   = 500
	concurrentLanes    = 32
	concurrentVersions = 5
	// 商家号 1..20：slot = merchant & 3（sharding.MerchantIndexLayout），覆盖 4 张表、2 个库。
	concurrentMerchants = 20
	// concurrentPool 是并发测试里每个库的连接池上限。测试库和本机运行中的服务共用一个 MySQL 实例
	// （max_connections 151），testdb 默认的池（50）乘上两个库、几个并行的测试会把连接打满，
	// 连带让运行中的服务报 Too many connections。所以这几个测试不并行，池也压到 16：
	// 32 条 lane 对 2 × 16 个连接，和生产 64 路对 2 × 25 的比例相近，照样会在池上排队。
	concurrentPool = 16
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

// delivery 是一次投递：订单号（决定 lane）和序列化后的消息。
type delivery struct {
	key   int64
	value []byte
	msg   *cdcv1.RowChange
}

func newDelivery(t testing.TB, key int64, m *cdcv1.RowChange) delivery {
	t.Helper()
	v, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return delivery{key: key, value: v, msg: m}
}

// interleave 把每个订单各自有序的投递序列随机交织成一个分区里的投递流：每个订单内部的先后不变，
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

// runLanes 像 kafkax 那样处理投递流：按 key 分到 lanes 条 lane，lane 内按投递顺序串行，lane 之间并行。
// 处理失败时照 kafkax（MaxAttempts 无限）原地重试，但只容忍可重试的锁冲突（死锁、锁等待超时）：
// 它们不能是 Permanent（否则会进死信、丢掉这个版本），其他任何错误都让测试失败。
// 返回因锁冲突重试的次数。
func runLanes(t testing.TB, h kafkax.Handler, stream []delivery, lanes int) (retries int64) {
	t.Helper()
	queues := make([][]delivery, lanes)
	for _, d := range stream {
		l := int(d.key % int64(lanes))
		queues[l] = append(queues[l], d)
	}
	var (
		wg      sync.WaitGroup
		retried atomic.Int64
	)
	for _, q := range queues {
		wg.Go(func() {
			for _, d := range q {
				msg := kafkax.Message{Key: []byte(strconv.FormatInt(d.key, 10)), Value: d.value}
				for attempt := 1; ; attempt++ {
					err := h(t.Context(), msg)
					if err == nil {
						break
					}
					lockConflict := mysqlx.IsDeadlock(err) || mysqlx.IsLockWaitTimeout(err)
					if kafkax.IsPermanent(err) || !lockConflict {
						t.Errorf("order %d: unexpected error (permanent=%v): %v", d.key, kafkax.IsPermanent(err), err)
						return
					}
					if attempt >= 100 {
						t.Errorf("order %d: still failing after %d attempts: %v", d.key, attempt, err)
						return
					}
					retried.Add(1)
				}
			}
		})
	}
	wg.Wait()
	return retried.Load()
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

// inOrderStream 是无重复的输入：每个订单的 5 条变更按版本顺序，订单之间随机交织。
func inOrderStream(t testing.TB, rng *rand.Rand, orders []plannedOrder) []delivery {
	perKey := make([][]delivery, len(orders))
	for i, p := range orders {
		for _, m := range p.msgs {
			perKey[i] = append(perKey[i], newDelivery(t, p.order, m))
		}
	}
	return interleave(rng, perKey)
}

// expectedStats 按输入算出精确的统计（只适用于每个订单的投递版本严格递增、没有重复的输入）。
func expectedStats(orders []plannedOrder) window {
	var w window
	for _, p := range orders {
		for _, m := range p.msgs {
			if m.GetOp() == cdcv1.Op_OP_DELETE {
				w.Archived++
			}
			if p.merchant == 0 {
				w.SkippedNoMerchant++
				continue
			}
			w.Changed++
			w.LagCount++
		}
	}
	return w
}

func seededRand(t testing.TB) *rand.Rand {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	return rand.New(rand.NewPCG(seed, seed))
}

// 1 + 3：同一订单的消息保持顺序、32 条 lane 并发，索引里每个订单是最高版本；统计逐项精确。
func TestConcurrentLanesInOrder(t *testing.T) {
	st, dbs := newCappedStore(t, concurrentPool)
	ix, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	rng := seededRand(t)
	orders := planOrders(concurrentOrders)
	retries := runLanes(t, ix.Handler(DefaultWriteTimeout), inOrderStream(t, rng, orders), concurrentLanes)
	if t.Failed() {
		t.FailNow()
	}
	t.Logf("lock-conflict retries: %d", retries)
	verifyIndex(t, st, dbs, orders)

	got, want := ix.Stats().take(), expectedStats(orders)
	if got.Changed != want.Changed || got.Stale != want.Stale || got.SkippedNoMerchant != want.SkippedNoMerchant ||
		got.Ignored != 0 || got.Archived != want.Archived || got.LagCount != want.LagCount {
		t.Errorf("stats = %+v\nwant   %+v", got, want)
	}
}

// 3：重放（kafkax 重启或再平衡后从较早的位点重投）：每个订单按顺序投完之后，再从一个随机的版本 r 起
// 按顺序重投 r..5。重投的每条都不高于索引里已有的版本，所以 Stale 恰好等于重投的条数，Changed 不变。
func TestConcurrentLanesReplayStatsExact(t *testing.T) {
	st, dbs := newCappedStore(t, concurrentPool)
	ix, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	rng := seededRand(t)
	orders := planOrders(concurrentOrders)
	want := expectedStats(orders)
	perKey := make([][]delivery, len(orders))
	for i, p := range orders {
		for _, m := range p.msgs {
			perKey[i] = append(perKey[i], newDelivery(t, p.order, m))
		}
		from := rng.IntN(len(p.msgs))
		for _, m := range p.msgs[from:] {
			perKey[i] = append(perKey[i], newDelivery(t, p.order, m))
			if m.GetOp() == cdcv1.Op_OP_DELETE {
				want.Archived++
			}
			if p.merchant == 0 {
				want.SkippedNoMerchant++
			} else {
				want.Stale++
				want.LagCount++
			}
		}
	}
	runLanes(t, ix.Handler(DefaultWriteTimeout), interleave(rng, perKey), concurrentLanes)
	if t.Failed() {
		t.FailNow()
	}
	verifyIndex(t, st, dbs, orders)
	got := ix.Stats().take()
	if got.Changed != want.Changed || got.Stale != want.Stale || got.SkippedNoMerchant != want.SkippedNoMerchant ||
		got.Ignored != 0 || got.Archived != want.Archived || got.LagCount != want.LagCount {
		t.Errorf("stats = %+v\nwant   %+v", got, want)
	}
}

// 2：每条消息重复投递 1 到 3 次，同一订单内的顺序也打乱（等价于旧版本在新版本之后又来一遍），
// 结果与按序投递相同。统计里能精确断言的是与版本无关的部分。
func TestConcurrentLanesDuplicatesAndReorder(t *testing.T) {
	st, dbs := newCappedStore(t, concurrentPool)
	ix, err := New(st, nil)
	if err != nil {
		t.Fatal(err)
	}
	rng := seededRand(t)
	orders := planOrders(concurrentOrders)
	var (
		perKey        = make([][]delivery, len(orders))
		withMerchant  int64
		skipped       int64
		archived      int64
		merchantCount int64
	)
	for i, p := range orders {
		if p.merchant != 0 {
			merchantCount++
		}
		for _, m := range p.msgs {
			for range 1 + rng.IntN(3) {
				perKey[i] = append(perKey[i], newDelivery(t, p.order, m))
				if m.GetOp() == cdcv1.Op_OP_DELETE {
					archived++
				}
				if p.merchant == 0 {
					skipped++
				} else {
					withMerchant++
				}
			}
		}
		rng.Shuffle(len(perKey[i]), func(a, b int) { perKey[i][a], perKey[i][b] = perKey[i][b], perKey[i][a] })
	}
	retries := runLanes(t, ix.Handler(DefaultWriteTimeout), interleave(rng, perKey), concurrentLanes)
	if t.Failed() {
		t.FailNow()
	}
	t.Logf("lock-conflict retries: %d", retries)
	verifyIndex(t, st, dbs, orders)

	got := ix.Stats().take()
	if got.Changed+got.Stale != withMerchant || got.SkippedNoMerchant != skipped || got.Archived != archived ||
		got.Ignored != 0 || got.LagCount != withMerchant {
		t.Errorf("stats = %+v; want changed+stale=%d skipped=%d archived=%d", got, withMerchant, skipped, archived)
	}
	// 每个订单至少改一次（第一条到达的），至多 5 次（每个版本各一次）。
	if got.Changed < merchantCount || got.Changed > merchantCount*concurrentVersions {
		t.Errorf("changed = %d, want within [%d, %d]", got.Changed, merchantCount, merchantCount*concurrentVersions)
	}
}

// 不同并发度下的相对吞吐，只输出不断言（测试机的绝对速度不稳定）。连接池按生产默认（每库 25）设置，
// 并发度超过池子时多出来的写入在 database/sql 的池上排队，和生产一样。
//
// 生产的“同时在途写入”是 INDEX_CONCURRENCY × 分区数：改动前是 1 × 4 = 4，默认值下是 16 × 4 = 64。
//
// 规模取得小（3000 条），因为 make test 和 CI 每次都会跑它，并发度 1 那一档占了大部分时间。
// 要看更稳定的数字可以临时调大 orders，并去掉 -race 跑（竞态检测会把每条的耗时放大）。
func TestThroughputByConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput measurement skipped in -short mode")
	}
	const (
		orders = 600
		pool   = 25
	)
	rng := seededRand(t)
	plan := planOrders(orders)
	stream := inOrderStream(t, rng, plan)
	for _, c := range []int{1, 4, 16, 32, 64} {
		t.Run(fmt.Sprintf("concurrency=%d", c), func(t *testing.T) {
			st, _ := newCappedStore(t, pool)
			ix, err := New(st, nil)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			retries := runLanes(t, ix.Handler(DefaultWriteTimeout), stream, c)
			elapsed := time.Since(start)
			t.Logf("concurrency %2d, pool %d/db: %d msgs in %v = %.0f msgs/s (lock-conflict retries %d)",
				c, pool, len(stream), elapsed.Round(time.Millisecond), float64(len(stream))/elapsed.Seconds(), retries)
		})
	}
}
