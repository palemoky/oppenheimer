package verify

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// CheckArchive 是“冷热归档一致”这条检查的编号，只在给了冷库时执行。
const CheckArchive = 21

// ArchiveMinAge 是冷库里订单的最小年龄。归档任务只搬受理超过 90 天的订单，这里用 30 天，
// 留出配置调整和时钟偏差的余量：冷库里出现比它还新的订单，一定是搬错了。
const ArchiveMinAge = 30 * 24 * time.Hour

// archiveExamples 是第 21 条每一类最多列出的例子数。
const archiveExamples = 5

// idBatch 是按主键范围分批读订单号时每批的行数。
const idBatch = 100_000

const (
	coldOrders   = "orders_archive"
	coldItems    = "order_items_archive"
	coldPayments = "payment_results_archive"
)

// finalStatusList 是终态的 SQL 列表：冷库里只能有这些状态的订单。
var finalStatusList = fmt.Sprintf("%d, %d, %d, %d", OrderPaid, OrderCancelled, OrderClosed, OrderFailed)

// scanIDs 按主键升序、分批（keyset 分页，不用偏移量）读出 table 里的 order_id，对每个调用 fn。
// 每批是一条独立的短查询，不会因为客户端处理得慢而占着一个长查询。
func scanIDs(ctx context.Context, db *sql.DB, table string, fn func(id int64)) error {
	last := int64(math.MinInt64)
	for {
		n, err := func() (int, error) {
			rows, err := db.QueryContext(ctx, "SELECT order_id FROM "+table+" WHERE order_id > ? ORDER BY order_id LIMIT ?", last, idBatch) //nolint:gosec // 表名是常量或来自 sharding.Table
			if err != nil {
				return 0, fmt.Errorf("scan ids of %s: %w", table, err)
			}
			defer rows.Close()
			n := 0
			for rows.Next() {
				if err := rows.Scan(&last); err != nil {
					return 0, fmt.Errorf("scan ids of %s: %w", table, err)
				}
				fn(last)
				n++
			}
			if err := rows.Err(); err != nil {
				return 0, fmt.Errorf("scan ids of %s: %w", table, err)
			}
			return n, nil
		}()
		if err != nil {
			return err
		}
		if n < idBatch {
			return nil
		}
	}
}

// fillIssue 记下一类问题的总数 n，n 非零时再用 exampleQuery 取前几个例子。
// exampleQuery 返回两列：order_id 和一段说明文字，要自带 ORDER BY order_id LIMIT。
func fillIssue(ctx context.Context, db *sql.DB, is *Issues, n int64, exampleQuery string, args []any, describe func(id int64, extra string) string) error {
	is.N = int(n)
	if n == 0 {
		return nil
	}
	rows, err := db.QueryContext(ctx, exampleQuery, args...)
	if err != nil {
		return fmt.Errorf("examples %q: %w", exampleQuery, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id    int64
			extra string
		)
		if err := rows.Scan(&id, &extra); err != nil {
			return fmt.Errorf("scan examples %q: %w", exampleQuery, err)
		}
		is.Examples = append(is.Examples, describe(id, extra))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("examples %q: %w", exampleQuery, err)
	}
	return nil
}

func plainOrder(id int64, extra string) string {
	if extra == "" {
		return orderSubj(id)
	}
	return orderSubj(id) + " " + extra
}

// collectArchive 读冷库和热库，算出第 21 条各类问题的数量和例子。没有给冷库时什么也不读，Snapshot.Archive 保持 nil。
//
// 全部是集合运算的 SQL 或分批的主键范围扫描，没有逐行查询。跨库的“热库冷库同时有”在这里做：
// 热库各 slot 的订单号读进一个排好序的 []int64（500 万行约 40 MB），冷库的订单号按序分批读出，两边做归并比较。
func collectArchive(ctx context.Context, src Sources, s *Snapshot) error {
	cold := src.ArchiveDB
	if cold == nil {
		return nil
	}
	cutoff := time.Now().Add(-ArchiveMinAge)
	bound := idgen.LowerBound(cutoff)
	d := &ArchiveData{Cutoff: cutoff}

	// 1、2：一次扫描同时数两类。SUM 在空表上是 NULL。
	var nonFinal, tooNew int64
	if err := cold.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(status NOT IN (`+finalStatusList+`)), 0),
		COALESCE(SUM(order_id >= ?), 0) FROM `+coldOrders, bound).Scan(&d.ColdOrders, &nonFinal, &tooNew); err != nil {
		return fmt.Errorf("count cold orders: %w", err)
	}
	if err := fillIssue(ctx, cold, &d.NonFinal, nonFinal,
		`SELECT order_id, CAST(status AS CHAR) FROM `+coldOrders+` WHERE status NOT IN (`+finalStatusList+`) ORDER BY order_id LIMIT ?`,
		[]any{archiveExamples}, func(id int64, st string) string {
			n := 0
			_, _ = fmt.Sscan(st, &n)
			return fmt.Sprintf("%s (%s)", orderSubj(id), OrderStatusName(n))
		}); err != nil {
		return err
	}
	if err := fillIssue(ctx, cold, &d.TooNew, tooNew,
		`SELECT order_id, '' FROM `+coldOrders+` WHERE order_id >= ? ORDER BY order_id LIMIT ?`,
		[]any{bound, archiveExamples}, func(id int64, _ string) string {
			return fmt.Sprintf("%s (created %s)", orderSubj(id), idgen.Time(id).UTC().Format(time.DateTime))
		}); err != nil {
		return err
	}

	// 4：冷库内部。订单没有明细；明细、支付结果没有订单。
	var n int64
	if err := cold.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+coldOrders+` o
		WHERE NOT EXISTS (SELECT 1 FROM `+coldItems+` i WHERE i.order_id = o.order_id)`).Scan(&n); err != nil {
		return fmt.Errorf("count cold orders without items: %w", err)
	}
	if err := fillIssue(ctx, cold, &d.NoItems, n, `SELECT o.order_id, '' FROM `+coldOrders+` o
		WHERE NOT EXISTS (SELECT 1 FROM `+coldItems+` i WHERE i.order_id = o.order_id) ORDER BY o.order_id LIMIT ?`,
		[]any{archiveExamples}, plainOrder); err != nil {
		return err
	}
	for _, o := range []struct {
		table string
		dst   *Issues
	}{{coldItems, &d.ItemOrphans}, {coldPayments, &d.PaymentOrphans}} {
		n, ids, err := orphanIDs(ctx, cold, o.table, coldOrders, "")
		if err != nil {
			return err
		}
		o.dst.N = int(n)
		for _, id := range ids {
			o.dst.Examples = append(o.dst.Examples, orderSubj(id))
		}
	}

	// 5：热库各 slot 的子表孤儿，只看订单号早于 cutoff 的行，不和正在创建中的订单冲突。
	shards, err := orderShards(src)
	if err != nil {
		return err
	}
	var hotIDs []int64
	hotItems, hotPayments := &orphanSet{}, &orphanSet{}
	for db, slots := range shards {
		conn := src.OrderDBs[db]
		for _, slot := range slots {
			orders := sharding.Table("orders", slot)
			if err := scanIDs(ctx, conn, orders, func(id int64) { hotIDs = append(hotIDs, id) }); err != nil {
				return err
			}
			for _, o := range []struct {
				table string
				set   *orphanSet
			}{{sharding.Table("order_items", slot), hotItems}, {sharding.Table("payment_results", slot), hotPayments}} {
				n, ids, err := orphanIDs(ctx, conn, o.table, orders, " AND c.order_id < "+strconv.FormatInt(bound, 10))
				if err != nil {
					return err
				}
				o.set.add(o.table, n, ids)
			}
		}
	}
	d.HotItemOrphans, d.HotPaymentOrphans = hotItems.issues(), hotPayments.issues()
	d.HotOrders = int64(len(hotIDs))

	// 3：归并。各 slot 的订单号互不相交，排序后整体有序。
	slices.Sort(hotIDs)
	j := 0
	if err := scanIDs(ctx, cold, coldOrders, func(id int64) {
		for j < len(hotIDs) && hotIDs[j] < id {
			j++
		}
		if j < len(hotIDs) && hotIDs[j] == id {
			d.Both.add("%s", orderSubj(id))
		}
	}); err != nil {
		return err
	}
	s.Archive = d
	return nil
}

// orphanIDs 找出 child 表里有、parent 表里没有对应订单号的订单号：返回不同订单号的总数和最小的几个。
// extra 是追加在子表上的条件（以 AND 开头，子表别名是 c）。
func orphanIDs(ctx context.Context, db *sql.DB, child, parent, extra string) (int64, []int64, error) {
	from := ` FROM ` + child + ` c WHERE NOT EXISTS (SELECT 1 FROM ` + parent + ` p WHERE p.order_id = c.order_id)` + extra //nolint:gosec // 表名是常量或来自 sharding.Table
	var n int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT c.order_id)`+from).Scan(&n); err != nil {
		return 0, nil, fmt.Errorf("count orphans of %s: %w", child, err)
	}
	if n == 0 {
		return 0, nil, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT c.order_id`+from+` ORDER BY c.order_id LIMIT ?`, archiveExamples) //nolint:gosec // 表名是常量或来自 sharding.Table
	if err != nil {
		return 0, nil, fmt.Errorf("orphan examples of %s: %w", child, err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, nil, fmt.Errorf("scan orphan examples of %s: %w", child, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("orphan examples of %s: %w", child, err)
	}
	return n, ids, nil
}

// orphanExample 是一个孤儿订单号和它所在的子表。
type orphanExample struct {
	id    int64
	table string
}

// orphanSet 把多张分片表上的同类孤儿合并成一类：总数相加，例子取整体最小的几个。
type orphanSet struct {
	n        int64
	examples []orphanExample
}

func (o *orphanSet) add(table string, n int64, ids []int64) {
	o.n += n
	for _, id := range ids {
		o.examples = append(o.examples, orphanExample{id, table})
	}
}

func (o *orphanSet) issues() Issues {
	slices.SortFunc(o.examples, func(a, b orphanExample) int { return cmp.Compare(a.id, b.id) })
	is := Issues{N: int(o.n)}
	for _, e := range o.examples[:min(len(o.examples), archiveExamples)] {
		is.Examples = append(is.Examples, fmt.Sprintf("%s (in %s)", orderSubj(e.id), e.table))
	}
	return is
}

// ---------- 检查 ----------

// archive 是第 21 条：冷热归档一致。没有冷库（Snapshot.Archive 为 nil）时跳过。
func (c *checker) archive() {
	d := c.s.Archive
	if d == nil {
		return
	}
	for _, k := range []struct {
		what   string
		is     Issues
		settle bool // 只在归档已结束时才算问题
	}{
		{"non-final orders in the cold db", d.NonFinal, false},
		{"cold orders created after " + d.Cutoff.UTC().Format(time.DateOnly) + " (now - 30 days)", d.TooNew, false},
		{"order ids in both the hot and the cold db", d.Both, true},
		{"cold orders without any order_items_archive row", d.NoItems, false},
		{"order ids with order_items_archive rows but no cold order", d.ItemOrphans, false},
		{"order ids with payment_results_archive rows but no cold order", d.PaymentOrphans, false},
		{"order ids with hot order_items rows but no order row", d.HotItemOrphans, false},
		{"order ids with hot payment_results rows but no order row", d.HotPaymentOrphans, false},
	} {
		if k.is.N == 0 {
			continue
		}
		if k.settle && c.archiveInFlight {
			c.note("%d %s (-archive-settled=false, not a failure while the archive job is running)", k.is.N, k.what)
			continue
		}
		c.fail(CheckArchive, "cold archive", "%d %s, e.g. %s", k.is.N, k.what, strings.Join(k.is.Examples, "; "))
	}
}
