// Package store 封装 orderindex 对商家索引库的全部 SQL 访问。
//
// 索引按 merchant_id 分成若干逻辑分片（slot），分布在多个库上（sharding.MerchantIndexLayout，ADR-0014）。
// 每个方法按商家号（sharding.Router.ForMerchant）定位到一个 slot，只读写这个 slot 的表。
// 拼进 SQL 的表名只由 sharding.Table 生成，在 New 里预先渲染好，不拼接任何外部输入。
package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// TableBase 是索引表的基础名，物理表名是 sharding.Table(TableBase, slot)。
const TableBase = "merchant_orders"

// ErrNotFound 表示要找的行不存在。
var ErrNotFound = errors.New("store: not found")

// Row 是索引表的一行：一个订单在 Version 这个版本上的映像。
type Row struct {
	MerchantID  int64
	OrderID     int64
	UserID      int64
	Status      int32 // 与 proto OrderStatus 的数值一致
	TotalAmount int64 // 最小货币单位（分）
	Currency    string
	CreatedAt   time.Time // UTC
	PaidAt      time.Time // UTC；零值表示 NULL（未支付）
	// Version 是源行的 version 列，写入 src_version。
	Version int64
	// SyncedAt 是这一行最后一次被同步写入的时间。只在读出时有值，写入时忽略（由 Upsert 的参数决定）。
	SyncedAt time.Time
}

// Store 访问分片后的商家索引库。并发安全。
type Store struct {
	dbs    []*sql.DB
	router *sharding.Router
	stmts  []statements // 下标是 slot
	// stmtRows 是一条写入语句最多带的行数，默认 MaxStatementRows。只有测试会改小它，用来覆盖切分。
	stmtRows int
}

// statements 是一个 slot 上预先渲染好的语句，表名全部来自 sharding.Table。
type statements struct {
	// upsertHead、upsertTail 是多行写入语句在 VALUES 元组前后的两段，中间按行数拼 (?, ...) 元组。
	upsertHead, upsertTail  string
	list, listByStatus, get string
}

// upsertColumns 是写入语句每行的列数（占位符个数）。
const upsertColumns = 10

// MaxStatementRows 是一条写入语句最多带的行数。
//
// MySQL 的预处理语句最多 65535 个占位符（协议里参数个数是 2 字节），每行 upsertColumns 个，
// 所以一条语句最多 6553 行。驱动没有开 interpolateParams，参数走服务端预处理，这个限制是硬的。
// 超过它的一组行在 UpsertRows 里切成几条语句，放在同一个事务里。
const MaxStatementRows = math.MaxUint16 / upsertColumns

// upsertHeadTemplate、upsertTailTemplate 是索引表的多行写入语句，{T} 换成物理表名，
// 中间是 n 个 (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 元组（见 upsertSQL）。
//
// 幂等和定序全靠版本号（设计文档 1c 4.6）：消息至少一次投递，会重复、会重放旧的一段，全量和增量会交叠，
// 所以不依赖到达顺序，只接受 src_version 更大的映像。版本相同或更小时每一列都保持原值，行完全不变，
// 受影响行数为 0（mysqlx.Open 固定了“受影响行数 = 实际改变的行数”），synced_at 也不变。
// 多行语句对每一行各自执行一遍 ON DUPLICATE KEY UPDATE，new 指的是这一行自己的 VALUES 元组，
// 所以版本比较对每一行独立成立，和单行语句完全一样。
//
// ON DUPLICATE KEY UPDATE 的赋值从左到右执行，后面的表达式读到的是前面已经改过的值。
// src_version 必须最后赋值：放在前面的话，后面每一列的比较都变成“新版本大于新版本”，永远为假，
// 行就只有版本号在涨。TestUpsertAssignmentOrder 钉住这一点。
//
// merchant_id 是主键的一部分，写入后不变（设计文档 1c 4.2），不在更新列表里。
const (
	upsertHeadTemplate = `INSERT INTO {T} (merchant_id, order_id, user_id, status, total_amount, currency,
    created_at, paid_at, src_version, synced_at)
VALUES `
	upsertTailTemplate = `
AS new
ON DUPLICATE KEY UPDATE
    user_id      = IF(new.src_version > {T}.src_version, new.user_id,      {T}.user_id),
    status       = IF(new.src_version > {T}.src_version, new.status,       {T}.status),
    total_amount = IF(new.src_version > {T}.src_version, new.total_amount, {T}.total_amount),
    currency     = IF(new.src_version > {T}.src_version, new.currency,     {T}.currency),
    created_at   = IF(new.src_version > {T}.src_version, new.created_at,   {T}.created_at),
    paid_at      = IF(new.src_version > {T}.src_version, new.paid_at,      {T}.paid_at),
    synced_at    = IF(new.src_version > {T}.src_version, new.synced_at,    {T}.synced_at),
    src_version  = IF(new.src_version > {T}.src_version, new.src_version,  {T}.src_version)`
	upsertTuple = `(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

const rowColumns = `merchant_id, order_id, user_id, status, total_amount, currency, created_at, paid_at, src_version, synced_at`

// 列表查询。游标为 0 时用 math.MaxInt64 代替，两种情况共用一条语句。
// 不带状态时是主键 (merchant_id, order_id) 上的一段范围倒序扫描；带状态时是
// idx_merchant_status (merchant_id, status, order_id) 上的一段范围倒序扫描。两种都不排序（没有 filesort），
// 代价只和返回的行数成正比，翻到多深都一样。
//
// 两条都用 FORCE INDEX 固定索引。本机实测（MySQL 9.7，表刚建、统计信息很少）优化器两个方向都选错过：
//   - 带状态时选 PRIMARY：ORDER BY ... LIMIT 的启发式认为沿主键倒序扫、边扫边过滤、凑够 LIMIT 条就停更便宜。
//     状态常见时不慢，状态稀少时（老商家几十万个已支付订单里找几个待支付的）要把这个商家的全部历史扫一遍。
//   - 不带状态时选 idx_merchant_status：按 merchant_id 取出这个商家的全部订单再 filesort，同样和商家的订单总数成正比。
//
// 列表页要的是稳定的计划，不需要优化器在这里择优。TestListUsesIndex 用 EXPLAIN 钉住计划。
const (
	listTemplate = `SELECT ` + rowColumns + ` FROM {T} FORCE INDEX (PRIMARY)
WHERE merchant_id = ? AND order_id < ?
ORDER BY order_id DESC LIMIT ?`
	listByStatusTemplate = `SELECT ` + rowColumns + ` FROM {T} FORCE INDEX (idx_merchant_status)
WHERE merchant_id = ? AND status = ? AND order_id < ?
ORDER BY order_id DESC LIMIT ?`
	getTemplate = `SELECT ` + rowColumns + ` FROM {T} WHERE merchant_id = ? AND order_id = ?`
)

func render(tmpl, table string) string { return strings.ReplaceAll(tmpl, "{T}", table) }

// New 返回使用 dbs 的 Store，dbs[i] 是第 i 个索引库。库的个数必须等于 router.DBs()。
func New(dbs []*sql.DB, router *sharding.Router) (*Store, error) {
	if router == nil {
		return nil, errors.New("store: nil router")
	}
	if len(dbs) != router.DBs() {
		return nil, fmt.Errorf("store: got %d databases, sharding layout has %d", len(dbs), router.DBs())
	}
	for i, db := range dbs {
		if db == nil {
			return nil, fmt.Errorf("store: database %d is nil", i)
		}
	}
	stmts := make([]statements, router.Slots())
	for slot := range stmts {
		t := sharding.Table(TableBase, slot)
		stmts[slot] = statements{
			upsertHead:   render(upsertHeadTemplate, t),
			upsertTail:   render(upsertTailTemplate, t),
			list:         render(listTemplate, t),
			listByStatus: render(listByStatusTemplate, t),
			get:          render(getTemplate, t),
		}
	}
	return &Store{dbs: dbs, router: router, stmts: stmts, stmtRows: MaxStatementRows}, nil
}

// Router 返回 Store 使用的分片布局。
func (s *Store) Router() *sharding.Router { return s.router }

// locate 返回商家所在 slot 的库和语句。
func (s *Store) locate(merchantID int64) (*sql.DB, *statements) {
	slot := s.router.ForMerchant(merchantID)
	return s.dbs[s.router.DB(slot)], &s.stmts[slot]
}

// UpsertSQL 返回 slot 上带 n 行的写入语句，只用于测试和排查。
func (s *Store) UpsertSQL(slot, n int) string { return upsertSQL(&s.stmts[slot], n) }

// upsertSQL 拼出带 n 个 VALUES 元组的写入语句。表名部分在 New 里渲染好，这里只重复固定的占位符元组。
func upsertSQL(st *statements, n int) string {
	var b strings.Builder
	b.Grow(len(st.upsertHead) + n*(len(upsertTuple)+2) + len(st.upsertTail))
	b.WriteString(st.upsertHead)
	for i := range n {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(upsertTuple)
	}
	b.WriteString(st.upsertTail)
	return b.String()
}

// WriteResult 是一次 UpsertRows 的结果。
type WriteResult struct {
	// Rows 是写入（发给 MySQL）的行数。
	Rows int64
	// Affected 是各条语句受影响行数之和。ON DUPLICATE KEY UPDATE 下每一行的贡献是：新插入 1，
	// 被更高版本覆盖 2，版本不高、行没变 0（mysqlx.Open 固定了 ClientFoundRows=false，即“实际改变的行数”）。
	// 所以 Affected = 插入数 + 2 × 更新数，只看它分不开插入和更新。
	Affected int64
	// Changed 是实际改变了索引的行数（插入数 + 更新数）的估计：每条语句取 min(受影响行数, 行数) 再求和。
	//
	// 一条语句里插入 I 行、更新 U 行、没变 S 行时，Rows = I+U+S，Affected = I+2U，
	// 真实的 I+U 落在 [ceil(Affected/2), min(Affected, Rows)] 里。估计取上界，它在两种常见情况下是精确的：
	//   - S = 0（正常消费，没有重放）：min 取到 Rows = I+U；
	//   - U = 0（从头重建，只有插入和重放的旧版本）：min 取到 Affected = I。
	// 只有同一条语句里同时有“被覆盖”和“没变”的行时才偏大，偏大的量是 min(U, S)。
	// 要精确就得逐行看结果，多行语句给不了，为统计再读一次库不值得。
	Changed int64
}

func (r *WriteResult) add(o WriteResult) {
	r.Rows += o.Rows
	r.Affected += o.Affected
	r.Changed += o.Changed
}

// slotRows 是同一个 slot 上要写的一组行，按主键升序。
type slotRows struct {
	slot int
	rows []Row
}

// UpsertRows 把 rows 写进各自商家所在的 slot，每一行只有版本大于已有行时才覆盖。syncedAt 写入 synced_at。
// rows 里同一个 (merchant_id, order_id) 不能出现两次（调用方先合并，只留版本最高的）：出现时返回错误，什么也不写。
// rows 为空时什么都不做。rows 本身不被修改。
//
// 写法：按库分组，每个库一个事务；库内按 slot 升序，每个 slot 一条多行语句，超过 MaxStatementRows 行时
// 切成几条。一个库上只有一条语句时不开显式事务，直接自动提交，省掉 BEGIN 和 COMMIT 两次往返：
// 没有积压时一批只有一条消息，这是最常见的情况。
//
// 两个库并行写、各自提交，不是原子的：一个成功一个失败时返回错误，调用方整批重做，
// 成功的那一半靠版本比较成为空操作。返回错误时 WriteResult 为零值，不区分哪个库成功了。
//
// 锁（REPEATABLE READ）：多行语句逐行加锁，锁持有到事务提交。撞上已有行时对主键上那一行加 next-key 排他锁
// （行和它前面的间隙），新行要在间隙里加插入意向锁，会被别人的间隙锁挡住。不同分区的批会同时写同一张表，
// 如果两个事务以相反的顺序碰到两把锁（一个先 A 后 B，另一个先 B 后 A）就会死锁。
// 所以每条语句里的行、每个事务里的表都按同一个全序排列：slot 升序，表内按主键 (merchant_id, order_id) 升序。
// 这样在主键上每个事务都只会去等比自己已持有的锁更靠后的位置，形不成环。
// 这只是把死锁降到最低，不是消除：二级索引 idx_merchant_status 按 (merchant_id, status, order_id) 排列，
// 改状态时在它上面加锁的顺序跟着主键走，不是它自己的顺序，仍可能交叉。
// 出现死锁或锁等待超时时整个事务回滚（锁等待超时 InnoDB 默认只回滚语句，这里由 InTx 回滚整个事务），
// 错误原样包装返回，不是 Permanent，由消费者重试。
func (s *Store) UpsertRows(ctx context.Context, rows []Row, syncedAt time.Time) (WriteResult, error) {
	if len(rows) == 0 {
		return WriteResult{}, nil
	}
	sorted := slices.Clone(rows)
	slotOf := func(r Row) int { return s.router.ForMerchant(r.MerchantID) }
	slices.SortFunc(sorted, func(a, b Row) int {
		return cmp.Or(cmp.Compare(slotOf(a), slotOf(b)),
			cmp.Compare(a.MerchantID, b.MerchantID), cmp.Compare(a.OrderID, b.OrderID))
	})
	// 排好序后按库、按 slot 切成连续的几段。
	perDB := make([][]slotRows, len(s.dbs))
	for i := 0; i < len(sorted); {
		slot := slotOf(sorted[i])
		j := i + 1
		for ; j < len(sorted) && slotOf(sorted[j]) == slot; j++ {
			if a, b := sorted[j-1], sorted[j]; a.MerchantID == b.MerchantID && a.OrderID == b.OrderID {
				return WriteResult{}, fmt.Errorf("store: order %d of merchant %d appears more than once in one write",
					b.OrderID, b.MerchantID)
			}
		}
		db := s.router.DB(slot)
		perDB[db] = append(perDB[db], slotRows{slot: slot, rows: sorted[i:j]})
		i = j
	}

	results := make([]WriteResult, len(s.dbs))
	errs := make([]error, len(s.dbs))
	var wg sync.WaitGroup
	for i, groups := range perDB {
		if len(groups) == 0 {
			continue
		}
		wg.Go(func() { results[i], errs[i] = s.writeDB(ctx, i, groups, syncedAt) })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return WriteResult{}, err
	}
	var total WriteResult
	for _, r := range results {
		total.add(r)
	}
	return total, nil
}

// writeDB 把一个库上的几组行写进去：只有一条语句时自动提交，否则放在一个事务里。
func (s *Store) writeDB(ctx context.Context, dbIndex int, groups []slotRows, syncedAt time.Time) (WriteResult, error) {
	var chunks []slotRows
	for _, g := range groups {
		for c := range slices.Chunk(g.rows, s.stmtRows) {
			chunks = append(chunks, slotRows{slot: g.slot, rows: c})
		}
	}
	db := s.dbs[dbIndex]
	if len(chunks) == 1 {
		return s.execUpsert(ctx, db, chunks[0], syncedAt)
	}
	var total WriteResult
	err := mysqlx.InTx(ctx, db, func(tx *sql.Tx) error {
		for _, c := range chunks {
			r, err := s.execUpsert(ctx, tx, c, syncedAt)
			if err != nil {
				return err
			}
			total.add(r)
		}
		return nil
	})
	if err != nil {
		return WriteResult{}, fmt.Errorf("store: merchant index db %d: %w", dbIndex, err)
	}
	return total, nil
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// execUpsert 用一条多行语句写一组行（同一个 slot、已按主键排序、不超过 stmtRows 行）。
func (s *Store) execUpsert(ctx context.Context, ex execer, c slotRows, syncedAt time.Time) (WriteResult, error) {
	args := make([]any, 0, len(c.rows)*upsertColumns)
	at := syncedAt.UTC()
	for _, r := range c.rows {
		var paidAt sql.NullTime
		if !r.PaidAt.IsZero() {
			paidAt = sql.NullTime{Time: r.PaidAt.UTC(), Valid: true}
		}
		args = append(args, r.MerchantID, r.OrderID, r.UserID, r.Status, r.TotalAmount, r.Currency,
			r.CreatedAt.UTC(), paidAt, r.Version, at)
	}
	table := sharding.Table(TableBase, c.slot)
	res, err := ex.ExecContext(ctx, upsertSQL(&s.stmts[c.slot], len(c.rows)), args...)
	if err != nil {
		return WriteResult{}, fmt.Errorf("store: upsert %d rows into %s (orders %d..%d): %w",
			len(c.rows), table, c.rows[0].OrderID, c.rows[len(c.rows)-1].OrderID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return WriteResult{}, fmt.Errorf("store: rows affected of %s: %w", table, err)
	}
	rows := int64(len(c.rows))
	if n < 0 || n > 2*rows {
		return WriteResult{}, fmt.Errorf("store: upsert %d rows into %s: unexpected rows affected %d", rows, table, n)
	}
	return WriteResult{Rows: rows, Affected: n, Changed: min(n, rows)}, nil
}

// ListQuery 是一次列表查询的条件。
type ListQuery struct {
	MerchantID int64
	// Status 为 0 表示不筛选。
	Status int32
	// BeforeOrderID 为 0 表示从最新的开始。
	BeforeOrderID int64
	// Limit 是最多返回的行数，必须大于 0。
	Limit int
}

// List 按 order_id 倒序返回商家的订单，只含 order_id < BeforeOrderID 的。
func (s *Store) List(ctx context.Context, q ListQuery) ([]Row, error) {
	if q.Limit <= 0 {
		return nil, fmt.Errorf("store: list limit %d must be positive", q.Limit)
	}
	before := q.BeforeOrderID
	if before == 0 {
		before = math.MaxInt64
	}
	db, st := s.locate(q.MerchantID)
	var (
		rows *sql.Rows
		err  error
	)
	if q.Status == 0 {
		rows, err = db.QueryContext(ctx, st.list, q.MerchantID, before, q.Limit)
	} else {
		rows, err = db.QueryContext(ctx, st.listByStatus, q.MerchantID, q.Status, before, q.Limit)
	}
	if err != nil {
		return nil, fmt.Errorf("store: list orders of merchant %d: %w", q.MerchantID, err)
	}
	defer rows.Close()
	out := make([]Row, 0, q.Limit)
	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list orders of merchant %d: %w", q.MerchantID, err)
	}
	return out, nil
}

// Get 返回一行，不存在时返回 ErrNotFound。
func (s *Store) Get(ctx context.Context, merchantID, orderID int64) (Row, error) {
	db, st := s.locate(merchantID)
	r, err := scanRow(db.QueryRowContext(ctx, st.get, merchantID, orderID))
	if errors.Is(err, sql.ErrNoRows) {
		return Row{}, ErrNotFound
	}
	return r, err
}

// Plan 是 EXPLAIN（传统格式）里的两列。
type Plan struct {
	Key   string // 使用的索引
	Extra string // 例如 "Using where; Backward index scan"；出现 "Using filesort" 说明在排序
}

// ExplainList 返回列表查询的执行计划，只用于测试和排查。
func (s *Store) ExplainList(ctx context.Context, q ListQuery) (Plan, error) {
	db, st := s.locate(q.MerchantID)
	const explain = "EXPLAIN FORMAT=TRADITIONAL "
	var (
		query = explain + st.list //nolint:gosec // 表名只来自 sharding.Table，不含外部输入
		args  = []any{q.MerchantID, int64(math.MaxInt64), q.Limit}
	)
	if q.Status != 0 {
		query = explain + st.listByStatus
		args = []any{q.MerchantID, q.Status, int64(math.MaxInt64), q.Limit}
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return Plan{}, fmt.Errorf("store: explain: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return Plan{}, fmt.Errorf("store: explain columns: %w", err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Plan{}, fmt.Errorf("store: explain: %w", err)
		}
		return Plan{}, errors.New("store: explain returned no rows")
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return Plan{}, fmt.Errorf("store: scan explain: %w", err)
	}
	var p Plan
	for i, c := range cols {
		switch c {
		case "key":
			p.Key = vals[i].String
		case "Extra":
			p.Extra = vals[i].String
		}
	}
	if err := rows.Close(); err != nil {
		return Plan{}, fmt.Errorf("store: explain: %w", err)
	}
	if err := rows.Err(); err != nil {
		return Plan{}, fmt.Errorf("store: explain: %w", err)
	}
	return p, nil
}

type scanner interface{ Scan(dest ...any) error }

func scanRow(sc scanner) (Row, error) {
	var (
		r      Row
		paidAt sql.NullTime
	)
	if err := sc.Scan(&r.MerchantID, &r.OrderID, &r.UserID, &r.Status, &r.TotalAmount, &r.Currency,
		&r.CreatedAt, &paidAt, &r.Version, &r.SyncedAt); err != nil {
		return Row{}, fmt.Errorf("store: scan row: %w", err)
	}
	if paidAt.Valid {
		r.PaidAt = paidAt.Time
	}
	return r, nil
}
