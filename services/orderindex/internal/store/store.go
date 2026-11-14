// Package store 封装 orderindex 对商家索引库的全部 SQL 访问。
//
// 索引按 merchant_id 分成若干逻辑分片（slot），分布在多个库上（sharding.MerchantIndexLayout，ADR-0014）。
// 每个方法按商家号（sharding.Router.ForMerchant）定位到一个 slot，只读写这个 slot 的表。
// 拼进 SQL 的表名只由 sharding.Table 生成，在 New 里预先渲染好，不拼接任何外部输入。
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

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

// Outcome 是一次 Upsert 的结果。
type Outcome int

const (
	// Inserted 表示新插入了一行。
	Inserted Outcome = iota + 1
	// Updated 表示已有的行被更高版本的映像覆盖。
	Updated
	// Stale 表示已有的行版本不低于这次的映像，行没有任何变化（重复或乱序到达的旧消息）。
	Stale
)

func (o Outcome) String() string {
	switch o {
	case Inserted:
		return "inserted"
	case Updated:
		return "updated"
	case Stale:
		return "stale"
	default:
		return fmt.Sprintf("Outcome(%d)", int(o))
	}
}

// Changed 报告这次写入是否改了行。
func (o Outcome) Changed() bool { return o == Inserted || o == Updated }

// Store 访问分片后的商家索引库。并发安全。
type Store struct {
	dbs    []*sql.DB
	router *sharding.Router
	stmts  []statements // 下标是 slot
}

// statements 是一个 slot 上预先渲染好的语句，表名全部来自 sharding.Table。
type statements struct {
	upsert, list, listByStatus, get string
}

// upsertTemplate 是索引表的写入语句，{T} 换成物理表名。
//
// 幂等和定序全靠版本号（设计文档 1c 4.6）：消息至少一次投递，会重复、会重放旧的一段，全量和增量会交叠，
// 所以不依赖到达顺序，只接受 src_version 更大的映像。版本相同或更小时每一列都保持原值，行完全不变，
// 受影响行数为 0（mysqlx.Open 固定了“受影响行数 = 实际改变的行数”），synced_at 也不变。
//
// ON DUPLICATE KEY UPDATE 的赋值从左到右执行，后面的表达式读到的是前面已经改过的值。
// src_version 必须最后赋值：放在前面的话，后面每一列的比较都变成“新版本大于新版本”，永远为假，
// 行就只有版本号在涨。TestUpsertAssignmentOrder 钉住这一点。
//
// merchant_id 是主键的一部分，写入后不变（设计文档 1c 4.2），不在更新列表里。
const upsertTemplate = `INSERT INTO {T} (merchant_id, order_id, user_id, status, total_amount, currency,
    created_at, paid_at, src_version, synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS new
ON DUPLICATE KEY UPDATE
    user_id      = IF(new.src_version > {T}.src_version, new.user_id,      {T}.user_id),
    status       = IF(new.src_version > {T}.src_version, new.status,       {T}.status),
    total_amount = IF(new.src_version > {T}.src_version, new.total_amount, {T}.total_amount),
    currency     = IF(new.src_version > {T}.src_version, new.currency,     {T}.currency),
    created_at   = IF(new.src_version > {T}.src_version, new.created_at,   {T}.created_at),
    paid_at      = IF(new.src_version > {T}.src_version, new.paid_at,      {T}.paid_at),
    synced_at    = IF(new.src_version > {T}.src_version, new.synced_at,    {T}.synced_at),
    src_version  = IF(new.src_version > {T}.src_version, new.src_version,  {T}.src_version)`

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
			upsert:       render(upsertTemplate, t),
			list:         render(listTemplate, t),
			listByStatus: render(listByStatusTemplate, t),
			get:          render(getTemplate, t),
		}
	}
	return &Store{dbs: dbs, router: router, stmts: stmts}, nil
}

// Router 返回 Store 使用的分片布局。
func (s *Store) Router() *sharding.Router { return s.router }

// locate 返回商家所在 slot 的库和语句。
func (s *Store) locate(merchantID int64) (*sql.DB, *statements) {
	slot := s.router.ForMerchant(merchantID)
	return s.dbs[s.router.DB(slot)], &s.stmts[slot]
}

// UpsertSQL 返回 slot 上的写入语句，只用于测试和排查。
func (s *Store) UpsertSQL(slot int) string { return s.stmts[slot].upsert }

// Upsert 把 r 写进它的商家所在的 slot，只有 r.Version 大于已有行的版本时才覆盖。syncedAt 写入 synced_at。
func (s *Store) Upsert(ctx context.Context, r Row, syncedAt time.Time) (Outcome, error) {
	db, st := s.locate(r.MerchantID)
	var paidAt sql.NullTime
	if !r.PaidAt.IsZero() {
		paidAt = sql.NullTime{Time: r.PaidAt.UTC(), Valid: true}
	}
	res, err := db.ExecContext(ctx, st.upsert,
		r.MerchantID, r.OrderID, r.UserID, r.Status, r.TotalAmount, r.Currency,
		r.CreatedAt.UTC(), paidAt, r.Version, syncedAt.UTC())
	if err != nil {
		return 0, fmt.Errorf("store: upsert order %d of merchant %d: %w", r.OrderID, r.MerchantID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: rows affected: %w", err)
	}
	// INSERT ... ON DUPLICATE KEY UPDATE 的受影响行数：1 插入，2 更新了已有行，0 已有行没变。
	switch n {
	case 0:
		return Stale, nil
	case 1:
		return Inserted, nil
	case 2:
		return Updated, nil
	default:
		return 0, fmt.Errorf("store: upsert order %d: unexpected rows affected %d", r.OrderID, n)
	}
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
