// Package seedreport 在灌数之后量分片的均匀度和各条查询路径的延迟（设计文档 1c 的 5.3），只读。
package seedreport

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// Config 配置一次报告。
type Config struct {
	// OrderDBs 是订单库，OrderDBs[i] 是 order_db_i。
	OrderDBs []*sql.DB
	// IndexDBs 是商家索引库，可选；为空时跳过商家相关的统计和查询。
	IndexDBs []*sql.DB
	// Runs 是每类查询的次数，默认 2000。
	Runs int
	// CompareRuns 是偏移量对游标每种写法的次数，默认 20。
	CompareRuns int
	// DeepPage 是“深页”的页码（从 1 数），默认 500。
	DeepPage int
	// PageSize 是每页条数，默认 20。
	PageSize int
	// TopN 是取多少个重度用户和大商家，默认 5。
	TopN int
	// Analyze 为 true 时先对每张表 ANALYZE TABLE，使 information_schema 里的大小是新的。
	Analyze bool
	// Seed 是随机取参数的种子。
	Seed uint64
	// Log 接收进度行，nil 表示丢弃。
	Log io.Writer
}

func (c *Config) defaults() {
	c.Runs = cmp.Or(c.Runs, 2000)
	c.CompareRuns = cmp.Or(c.CompareRuns, 20)
	c.DeepPage = cmp.Or(c.DeepPage, 500)
	c.PageSize = cmp.Or(c.PageSize, 20)
	c.TopN = cmp.Or(c.TopN, 5)
	if c.Log == nil {
		c.Log = io.Discard
	}
}

// Report 是报告的全部内容。
type Report struct {
	OrderTables []TableStat  `json:"order_tables"`
	IndexTables []TableStat  `json:"index_tables,omitempty"`
	Balance     []Balance    `json:"balance"`
	HeavyUsers  []Top        `json:"heavy_users"`
	BigMerchant []Top        `json:"big_merchants,omitempty"`
	Queries     []QueryStat  `json:"queries"`
	Compare     []Comparison `json:"offset_vs_cursor"`
	Notes       []string     `json:"notes,omitempty"`
}

// TableStat 是一张表的行数和大小。行数是 COUNT(*) 的精确值；大小取自 information_schema.tables，
// 来自 InnoDB 的持久化统计，不先 ANALYZE 时可能是旧的。
type TableStat struct {
	DB         int    `json:"db"`
	Slot       int    `json:"slot"`
	Table      string `json:"table"`
	Rows       int64  `json:"rows"`
	DataBytes  int64  `json:"data_bytes"`
	IndexBytes int64  `json:"index_bytes"`
}

// Balance 是一组数的最大值、最小值和两者之比（最小值为 0 时比值记 0）。
type Balance struct {
	Name  string  `json:"name"`
	Max   int64   `json:"max"`
	Min   int64   `json:"min"`
	Ratio float64 `json:"ratio"`
}

// Top 是一个重度用户或大商家及其订单数。
type Top struct {
	ID     int64 `json:"id"`
	Slot   int   `json:"slot"`
	Orders int64 `json:"orders"`
}

// QueryStat 是一类查询的延迟分布和执行计划。
type QueryStat struct {
	Name    string       `json:"name"`
	SQL     string       `json:"sql"`
	Runs    int          `json:"runs"`
	P50Ms   float64      `json:"p50_ms"`
	P99Ms   float64      `json:"p99_ms"`
	MaxMs   float64      `json:"max_ms"`
	AvgRows float64      `json:"avg_rows"`
	Explain []ExplainRow `json:"explain"`
}

// ExplainRow 是 EXPLAIN 的一行里关心的列。
type ExplainRow struct {
	Table    string  `json:"table"`
	Type     string  `json:"type"`
	Key      string  `json:"key"`
	Rows     int64   `json:"rows"`
	Filtered float64 `json:"filtered"`
	Extra    string  `json:"extra"`
}

// Comparison 是同一个深页用偏移量和用游标两种写法的耗时对比。
type Comparison struct {
	Name string `json:"name"`
	// Page 和 Offset 是实际比较的页码和偏移量（数据不够深时比要求的浅）。
	Page        int     `json:"page"`
	Offset      int     `json:"offset"`
	Runs        int     `json:"runs"`
	OffsetP50Ms float64 `json:"offset_p50_ms"`
	OffsetMaxMs float64 `json:"offset_max_ms"`
	CursorP50Ms float64 `json:"cursor_p50_ms"`
	CursorMaxMs float64 `json:"cursor_max_ms"`
	// OffsetRows、CursorRows 是一次查询从数据库取回的行数（散发时是各分片之和）。
	OffsetRows int64 `json:"offset_rows"`
	CursorRows int64 `json:"cursor_rows"`
	// Same 表示两种写法取到的这一页完全相同。
	Same bool `json:"same"`
}

type gen struct {
	cfg   Config
	or    *sharding.Router
	ir    *sharding.Router // 没有索引库时为 nil
	r     *rand.Rand
	rep   *Report
	slots []slotInfo // 下标是订单 slot
}

type slotInfo struct {
	db       *sql.DB
	min, max int64
	rows     int64
}

type sample struct {
	orderID, userID, merchantID int64
}

// Generate 生成报告。
func Generate(ctx context.Context, cfg Config) (*Report, error) {
	cfg.defaults()
	or, err := sharding.New(sharding.OrderLayout(len(cfg.OrderDBs)))
	if err != nil {
		return nil, fmt.Errorf("order layout: %w", err)
	}
	g := &gen{cfg: cfg, or: or, r: rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x5eed)), rep: &Report{}} //nolint:gosec // 取查询参数，不涉及安全
	if len(cfg.IndexDBs) > 0 {
		lay := sharding.MerchantIndexLayout()
		if len(cfg.IndexDBs) != lay.DBs {
			return nil, fmt.Errorf("got %d merchant index dbs, layout has %d", len(cfg.IndexDBs), lay.DBs)
		}
		if g.ir, err = sharding.New(lay); err != nil {
			return nil, err
		}
	}
	steps := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"table stats", g.tableStats},
		{"top users and merchants", g.tops},
		{"queries", g.queries},
		{"offset vs cursor", g.compare},
	}
	for _, s := range steps {
		fmt.Fprintf(cfg.Log, "seed-report: %s...\n", s.name)
		if err := s.fn(ctx); err != nil {
			return g.rep, fmt.Errorf("%s: %w", s.name, err)
		}
	}
	return g.rep, nil
}

// ---------- 均匀度 ----------

func (g *gen) tableStats(ctx context.Context) error {
	g.slots = make([]slotInfo, g.or.Slots())
	var orders, items []TableStat
	for i, db := range g.cfg.OrderDBs {
		for _, slot := range g.or.SlotsOf(i) {
			ot, err := g.tableStat(ctx, db, i, slot, sharding.Table("orders", slot))
			if err != nil {
				return err
			}
			it, err := g.tableStat(ctx, db, i, slot, sharding.Table("order_items", slot))
			if err != nil {
				return err
			}
			orders, items = append(orders, ot), append(items, it)
			si := slotInfo{db: db, rows: ot.Rows}
			if err := db.QueryRowContext(ctx, "SELECT COALESCE(MIN(order_id), 0), COALESCE(MAX(order_id), 0) FROM "+ot.Table).
				Scan(&si.min, &si.max); err != nil {
				return fmt.Errorf("min/max of %s: %w", ot.Table, err)
			}
			g.slots[slot] = si
		}
	}
	bySlot := func(a, b TableStat) int { return cmp.Compare(a.Slot, b.Slot) }
	slices.SortFunc(orders, bySlot)
	slices.SortFunc(items, bySlot)
	g.rep.OrderTables = slices.Concat(orders, items)
	g.rep.Balance = append(g.rep.Balance,
		balance("order rows per slot", orders, func(t TableStat) int64 { return t.Rows }),
		balance("order data+index bytes per slot", orders, func(t TableStat) int64 { return t.DataBytes + t.IndexBytes }),
		balance("item rows per slot", items, func(t TableStat) int64 { return t.Rows }),
	)
	perDB := make([]TableStat, len(g.cfg.OrderDBs))
	for _, t := range g.rep.OrderTables {
		perDB[t.DB].DataBytes += t.DataBytes + t.IndexBytes
	}
	g.rep.Balance = append(g.rep.Balance, balance("order db bytes (orders+items) per db", perDB, func(t TableStat) int64 { return t.DataBytes }))

	if g.ir == nil {
		g.rep.Notes = append(g.rep.Notes, "merchant index skipped: no -merchant-index-dsns")
		return nil
	}
	for i, db := range g.cfg.IndexDBs {
		for _, slot := range g.ir.SlotsOf(i) {
			t, err := g.tableStat(ctx, db, i, slot, sharding.Table("merchant_orders", slot))
			if err != nil {
				return err
			}
			g.rep.IndexTables = append(g.rep.IndexTables, t)
		}
	}
	slices.SortFunc(g.rep.IndexTables, bySlot)
	g.rep.Balance = append(g.rep.Balance,
		balance("merchant index rows per slot", g.rep.IndexTables, func(t TableStat) int64 { return t.Rows }),
		balance("merchant index bytes per slot", g.rep.IndexTables, func(t TableStat) int64 { return t.DataBytes + t.IndexBytes }),
	)
	return nil
}

// tableStat 读一张表的精确行数和 information_schema 里的大小。表名只来自 sharding.Table。
func (g *gen) tableStat(ctx context.Context, db *sql.DB, dbIdx, slot int, table string) (TableStat, error) {
	t := TableStat{DB: dbIdx, Slot: slot, Table: table}
	// 统计信息默认在 information_schema 里缓存一天（information_schema_stats_expiry），
	// 用一个专门的连接把它设成 0，读到的是 InnoDB 当前的持久化统计。
	conn, err := db.Conn(ctx)
	if err != nil {
		return t, fmt.Errorf("conn: %w", err)
	}
	defer conn.Close()
	if g.cfg.Analyze {
		// LOCAL：不写 binlog，不让统计信息的刷新流到从库和 CDC。
		if _, err := conn.ExecContext(ctx, "ANALYZE LOCAL TABLE "+table); err != nil {
			return t, fmt.Errorf("analyze %s: %w", table, err)
		}
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		return t, fmt.Errorf("set stats expiry: %w", err)
	}
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&t.Rows); err != nil {
		return t, fmt.Errorf("count %s: %w", table, err)
	}
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(data_length, 0), COALESCE(index_length, 0) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = ?`, table).Scan(&t.DataBytes, &t.IndexBytes); err != nil {
		return t, fmt.Errorf("size of %s: %w", table, err)
	}
	return t, nil
}

func balance[T any](name string, xs []T, f func(T) int64) Balance {
	b := Balance{Name: name}
	if len(xs) == 0 {
		return b
	}
	b.Max, b.Min = math.MinInt64, math.MaxInt64
	for _, x := range xs {
		b.Max, b.Min = max(b.Max, f(x)), min(b.Min, f(x))
	}
	if b.Min > 0 {
		b.Ratio = float64(b.Max) / float64(b.Min)
	}
	return b
}

// ---------- 重度用户和大商家 ----------

func (g *gen) tops(ctx context.Context) error {
	for slot, si := range g.slots {
		top, err := topBy(ctx, si.db, slot, "user_id", sharding.Table("orders", slot), g.cfg.TopN)
		if err != nil {
			return err
		}
		g.rep.HeavyUsers = append(g.rep.HeavyUsers, top...)
	}
	g.rep.HeavyUsers = topN(g.rep.HeavyUsers, g.cfg.TopN)
	if g.ir == nil {
		return nil
	}
	for slot := range g.ir.Slots() {
		top, err := topBy(ctx, g.indexDB(slot), slot, "merchant_id", sharding.Table("merchant_orders", slot), g.cfg.TopN)
		if err != nil {
			return err
		}
		g.rep.BigMerchant = append(g.rep.BigMerchant, top...)
	}
	g.rep.BigMerchant = topN(g.rep.BigMerchant, g.cfg.TopN)
	return nil
}

func topBy(ctx context.Context, db *sql.DB, slot int, col, table string, n int) ([]Top, error) {
	//nolint:gosec // col 是常量，table 只来自 sharding.Table
	rows, err := db.QueryContext(ctx, "SELECT "+col+", COUNT(*) c FROM "+table+" GROUP BY "+col+" ORDER BY c DESC LIMIT ?", n)
	if err != nil {
		return nil, fmt.Errorf("top %s of %s: %w", col, table, err)
	}
	defer rows.Close()
	var out []Top
	for rows.Next() {
		t := Top{Slot: slot}
		if err := rows.Scan(&t.ID, &t.Orders); err != nil {
			return nil, fmt.Errorf("scan top: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("top %s of %s: %w", col, table, err)
	}
	return out, nil
}

func topN(ts []Top, n int) []Top {
	slices.SortFunc(ts, func(a, b Top) int { return cmp.Or(cmp.Compare(b.Orders, a.Orders), cmp.Compare(a.ID, b.ID)) })
	return ts[:min(n, len(ts))]
}

func (g *gen) indexDB(slot int) *sql.DB { return g.cfg.IndexDBs[g.ir.DB(slot)] }

// ---------- 查询延迟 ----------

// 查询语句与服务里的一致：用户列表见 services/order/internal/store/userlist.go，
// 商家列表见 services/orderindex/internal/store/store.go。都用 FORCE INDEX 固定计划。
const (
	getOrderCols = `order_id, user_id, merchant_id, version, status, total_amount, currency, idempotency_key, request_hash,
	payment_id, fail_reason, expire_at, paid_at, created_at, updated_at`
	userListCols     = "order_id, status, total_amount, currency, expire_at, paid_at, created_at"
	merchantListCols = "merchant_id, order_id, user_id, status, total_amount, currency, created_at, paid_at, src_version, synced_at"
)

func getOrderSQL(slot int) string {
	return "SELECT " + getOrderCols + " FROM " + sharding.Table("orders", slot) + " WHERE order_id = ?"
}

// userListSQL 生成用户列表的语句：WHERE user_id = ? [AND status = ?] [AND order_id < ?] ORDER BY order_id DESC LIMIT n。
func userListSQL(slot int, userID int64, status int8, before int64, limit int) (string, []any) {
	q := "SELECT " + userListCols + " FROM " + sharding.Table("orders", slot) + " FORCE INDEX (idx_orders_user) WHERE user_id = ?"
	args := []any{userID}
	if status != 0 {
		q += " AND status = ?"
		args = append(args, status)
	}
	if before > 0 {
		q += " AND order_id < ?"
		args = append(args, before)
	}
	return q + " ORDER BY order_id DESC LIMIT ?", append(args, limit)
}

func merchantListSQL(slot int, merchantID, before int64, limit int) (string, []any) {
	if before <= 0 {
		before = math.MaxInt64
	}
	return "SELECT " + merchantListCols + " FROM " + sharding.Table("merchant_orders", slot) +
			" FORCE INDEX (PRIMARY) WHERE merchant_id = ? AND order_id < ? ORDER BY order_id DESC LIMIT ?",
		[]any{merchantID, before, limit}
}

type call struct {
	db    *sql.DB
	query string
	args  []any
}

// samples 随机取 n 个订单：先按行数加权选 slot，再在这个 slot 的订单号范围里取一个随机值，取不小于它的第一个订单。
// 订单号里是时间，所以这近似于按时间均匀取；取到的用户按下单次数加权（活跃用户被取到的机会多）。
func (g *gen) samples(ctx context.Context, n int) ([]sample, error) {
	var total int64
	for _, si := range g.slots {
		total += si.rows
	}
	if total == 0 {
		return nil, errors.New("no orders")
	}
	out := make([]sample, 0, n)
	for range n {
		pick := g.r.Int64N(total)
		slot := 0
		for ; pick >= g.slots[slot].rows; slot++ {
			pick -= g.slots[slot].rows
		}
		si := g.slots[slot]
		from := si.min + g.r.Int64N(si.max-si.min+1)
		var s sample
		if err := si.db.QueryRowContext(ctx, "SELECT order_id, user_id, COALESCE(merchant_id, 0) FROM "+sharding.Table("orders", slot)+
			" WHERE order_id >= ? ORDER BY order_id LIMIT 1", from).Scan(&s.orderID, &s.userID, &s.merchantID); err != nil {
			return nil, fmt.Errorf("sample slot %d: %w", slot, err)
		}
		out = append(out, s)
	}
	return out, nil
}

var finalStatuses = []int8{
	int8(orderv1.OrderStatus_ORDER_STATUS_PAID), int8(orderv1.OrderStatus_ORDER_STATUS_CANCELLED),
	int8(orderv1.OrderStatus_ORDER_STATUS_CLOSED), int8(orderv1.OrderStatus_ORDER_STATUS_FAILED),
}

func (g *gen) queries(ctx context.Context) error {
	n, ps := g.cfg.Runs, g.cfg.PageSize
	ss, err := g.samples(ctx, n)
	if err != nil {
		return err
	}
	pick := func() sample { return ss[g.r.IntN(len(ss))] }
	slotOfUser := func(u int64) int { return g.or.ForUser(u) }
	userCall := func(u int64, status int8, before int64) call {
		slot := slotOfUser(u)
		q, args := userListSQL(slot, u, status, before, ps)
		return call{g.slots[slot].db, q, args}
	}

	type class struct {
		name string
		next func(i int) (call, error)
	}
	classes := []class{
		{"order by id", func(i int) (call, error) {
			s := ss[i%len(ss)]
			slot := g.or.ForOrder(s.orderID)
			return call{g.slots[slot].db, getOrderSQL(slot), []any{s.orderID}}, nil
		}},
		{"user list, first page", func(int) (call, error) { return userCall(pick().userID, 0, 0), nil }},
		{"user list, status filter, first page", func(int) (call, error) {
			return userCall(pick().userID, finalStatuses[g.r.IntN(len(finalStatuses))], 0), nil
		}},
	}

	// 重度用户的深页：先用偏移量找到第 DeepPage 页之前的最后一个订单号当游标，测的是带游标的查询。
	if len(g.rep.HeavyUsers) > 0 {
		cursors := make([]int64, len(g.rep.HeavyUsers))
		for i, h := range g.rep.HeavyUsers {
			depth := deepOffset(g.cfg.DeepPage, ps, h.Orders)
			if cursors[i], err = g.userCursor(ctx, h.ID, depth); err != nil {
				return err
			}
		}
		classes = append(classes,
			class{fmt.Sprintf("heavy user, page %d (cursor)", g.cfg.DeepPage), func(int) (call, error) {
				i := g.r.IntN(len(cursors))
				return userCall(g.rep.HeavyUsers[i].ID, 0, cursors[i]), nil
			}},
			class{"heavy user, status filter, first page", func(int) (call, error) {
				h := g.rep.HeavyUsers[g.r.IntN(len(g.rep.HeavyUsers))]
				return userCall(h.ID, finalStatuses[g.r.IntN(len(finalStatuses))], 0), nil
			}},
		)
	}

	if g.ir != nil {
		var merchants []int64
		for _, s := range ss {
			if s.merchantID != 0 {
				merchants = append(merchants, s.merchantID)
			}
		}
		merchantCall := func(m, before int64) call {
			slot := g.ir.ForMerchant(m)
			q, args := merchantListSQL(slot, m, before, ps)
			return call{g.indexDB(slot), q, args}
		}
		if len(merchants) > 0 {
			classes = append(classes, class{"merchant list, first page", func(int) (call, error) {
				return merchantCall(merchants[g.r.IntN(len(merchants))], 0), nil
			}})
		}
		if len(g.rep.BigMerchant) > 0 {
			cursors := make([]int64, len(g.rep.BigMerchant))
			for i, m := range g.rep.BigMerchant {
				depth := deepOffset(g.cfg.DeepPage, ps, m.Orders)
				if cursors[i], err = g.merchantCursor(ctx, m.ID, depth); err != nil {
					return err
				}
			}
			classes = append(classes, class{fmt.Sprintf("big merchant, page %d (cursor)", g.cfg.DeepPage), func(int) (call, error) {
				i := g.r.IntN(len(cursors))
				return merchantCall(g.rep.BigMerchant[i].ID, cursors[i]), nil
			}})
		}
	}

	for _, c := range classes {
		fmt.Fprintf(g.cfg.Log, "seed-report:   %s x%d\n", c.name, n)
		st := QueryStat{Name: c.name, Runs: n}
		lat := make([]time.Duration, 0, n)
		var rowsTotal int64
		for i := range n {
			cl, err := c.next(i)
			if err != nil {
				return err
			}
			if i == 0 {
				st.SQL = cl.query
				if st.Explain, err = explain(ctx, cl); err != nil {
					return fmt.Errorf("%s: %w", c.name, err)
				}
			}
			d, nr, err := timeQuery(ctx, cl)
			if err != nil {
				return fmt.Errorf("%s: %w", c.name, err)
			}
			lat = append(lat, d)
			rowsTotal += nr
		}
		st.P50Ms, st.P99Ms, st.MaxMs = percentiles(lat)
		st.AvgRows = float64(rowsTotal) / float64(n)
		g.rep.Queries = append(g.rep.Queries, st)
	}
	return nil
}

// deepOffset 返回第 page 页的偏移量，数据不够深时退到最后一整页。
func deepOffset(page, size int, total int64) int {
	return max(0, min((page-1)*size, int(total)-size))
}

// userCursor 返回用户第 depth 个订单之前（按订单号倒序）的那个订单号，用作游标；depth 为 0 时返回 0（第一页）。
func (g *gen) userCursor(ctx context.Context, userID int64, depth int) (int64, error) {
	if depth == 0 {
		return 0, nil
	}
	slot := g.or.ForUser(userID)
	var id int64
	err := g.slots[slot].db.QueryRowContext(ctx, "SELECT order_id FROM "+sharding.Table("orders", slot)+
		" FORCE INDEX (idx_orders_user) WHERE user_id = ? ORDER BY order_id DESC LIMIT 1 OFFSET ?", userID, depth-1).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("cursor of user %d at %d: %w", userID, depth, err)
	}
	return id, nil
}

func (g *gen) merchantCursor(ctx context.Context, merchantID int64, depth int) (int64, error) {
	if depth == 0 {
		return 0, nil
	}
	slot := g.ir.ForMerchant(merchantID)
	var id int64
	err := g.indexDB(slot).QueryRowContext(ctx, "SELECT order_id FROM "+sharding.Table("merchant_orders", slot)+
		" WHERE merchant_id = ? ORDER BY order_id DESC LIMIT 1 OFFSET ?", merchantID, depth-1).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("cursor of merchant %d at %d: %w", merchantID, depth, err)
	}
	return id, nil
}

// timeQuery 执行一次查询并读完全部结果，返回耗时和行数。
func timeQuery(ctx context.Context, c call) (time.Duration, int64, error) {
	start := time.Now()
	rows, err := c.db.QueryContext(ctx, c.query, c.args...)
	if err != nil {
		return 0, 0, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	var n int64
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("read rows: %w", err)
	}
	return time.Since(start), n, nil
}

// explain 执行 EXPLAIN（显式用传统格式：MySQL 9.7 默认是 TREE，没有分列的 key、rows），取出每一行的表、访问类型、索引、估计行数、过滤比例和 Extra。
func explain(ctx context.Context, c call) ([]ExplainRow, error) {
	rows, err := c.db.QueryContext(ctx, "EXPLAIN FORMAT=TRADITIONAL "+c.query, c.args...)
	if err != nil {
		return nil, fmt.Errorf("explain: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("explain columns: %w", err)
	}
	var out []ExplainRow
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan explain: %w", err)
		}
		var e ExplainRow
		for i, col := range cols {
			v := vals[i].String
			switch col {
			case "table":
				e.Table = v
			case "type":
				e.Type = v
			case "key":
				e.Key = v
			case "rows":
				e.Rows, _ = strconv.ParseInt(v, 10, 64)
			case "filtered":
				e.Filtered, _ = strconv.ParseFloat(v, 64)
			case "Extra":
				e.Extra = v
			}
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("explain rows: %w", err)
	}
	return out, nil
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// percentiles 返回 p50、p99 和最大值（毫秒），用最近秩法。
func percentiles(lat []time.Duration) (p50, p99, mx float64) {
	if len(lat) == 0 {
		return 0, 0, 0
	}
	s := slices.Clone(lat)
	slices.Sort(s)
	at := func(p float64) float64 {
		i := int(math.Ceil(p*float64(len(s)))) - 1
		return ms(s[max(0, min(i, len(s)-1))])
	}
	return at(0.50), at(0.99), ms(s[len(s)-1])
}

// ---------- 偏移量对游标 ----------

// listRow 是全局最近订单列表的一行。
type listRow struct {
	OrderID, UserID int64
	Status          int8
	TotalAmount     int64
	CreatedAt       time.Time
}

func listRowKey(r listRow) int64 { return r.OrderID }

func (g *gen) compare(ctx context.Context) error {
	ps, runs := g.cfg.PageSize, g.cfg.CompareRuns

	// 一、单个分片内：最重的用户，同一个深页。
	if len(g.rep.HeavyUsers) > 0 {
		h := g.rep.HeavyUsers[0]
		off := deepOffset(g.cfg.DeepPage, ps, h.Orders)
		cursor, err := g.userCursor(ctx, h.ID, off)
		if err != nil {
			return err
		}
		db := g.slots[h.Slot].db
		oq, oargs := userListSQL(h.Slot, h.ID, 0, 0, ps)
		oq += " OFFSET ?"
		oargs = append(oargs, off)
		cq, cargs := userListSQL(h.Slot, h.ID, 0, cursor, ps)
		c := Comparison{Name: fmt.Sprintf("single shard: user %d (%d orders)", h.ID, h.Orders), Page: off/ps + 1, Offset: off, Runs: runs}
		ol, ot, err := runIDs(ctx, call{db, oq, oargs}, runs)
		if err != nil {
			return err
		}
		cl, ct, err := runIDs(ctx, call{db, cq, cargs}, runs)
		if err != nil {
			return err
		}
		c.OffsetP50Ms, _, c.OffsetMaxMs = percentiles(ol)
		c.CursorP50Ms, _, c.CursorMaxMs = percentiles(cl)
		c.OffsetRows, c.CursorRows = int64(len(ot)), int64(len(ct))
		c.Same = slices.Equal(ot, ct)
		g.rep.Compare = append(g.rep.Compare, c)
	}

	// 二、全部 slot 散发加归并：全站最近订单的同一个深页。
	var total int64
	for _, si := range g.slots {
		total += si.rows
	}
	off := deepOffset(g.cfg.DeepPage, ps, total)
	if off == 0 {
		return nil
	}
	c := Comparison{Name: fmt.Sprintf("scatter-gather over %d slots: latest orders", len(g.slots)), Page: off/ps + 1, Offset: off, Runs: runs}
	var (
		offLat, curLat []time.Duration
		offPage        []listRow
		cursor         int64
	)
	for range runs {
		start := time.Now()
		lists, fetched, err := g.scatter(ctx, 0, off+ps)
		if err != nil {
			return err
		}
		offPage = MergeDesc(lists, listRowKey, off, ps)
		offLat = append(offLat, time.Since(start))
		c.OffsetRows = fetched
		if cursor == 0 {
			cursor = MergeDesc(lists, listRowKey, off-1, 1)[0].OrderID
		}
	}
	var curPage []listRow
	for range runs {
		start := time.Now()
		lists, fetched, err := g.scatter(ctx, cursor, ps)
		if err != nil {
			return err
		}
		curPage = MergeDesc(lists, listRowKey, 0, ps)
		curLat = append(curLat, time.Since(start))
		c.CursorRows = fetched
	}
	c.OffsetP50Ms, _, c.OffsetMaxMs = percentiles(offLat)
	c.CursorP50Ms, _, c.CursorMaxMs = percentiles(curLat)
	c.Same = slices.Equal(offPage, curPage)
	g.rep.Compare = append(g.rep.Compare, c)
	return nil
}

// runIDs 执行 n 次查询，返回每次的耗时和最后一次取到的订单号（第一列）。
func runIDs(ctx context.Context, c call, n int) ([]time.Duration, []int64, error) {
	var (
		lat []time.Duration
		ids []int64
	)
	for range n {
		start := time.Now()
		var err error
		if ids, err = queryIDs(ctx, c); err != nil {
			return nil, nil, err
		}
		lat = append(lat, time.Since(start))
	}
	return lat, ids, nil
}

// queryIDs 执行一次查询，读完全部行，返回第一列（订单号）。
func queryIDs(ctx context.Context, c call) ([]int64, error) {
	rows, err := c.db.QueryContext(ctx, c.query, c.args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}
	var id int64
	dst := make([]any, len(cols))
	dst[0] = &id
	for i := 1; i < len(dst); i++ {
		dst[i] = new(sql.RawBytes)
	}
	var ids []int64
	for rows.Next() {
		if err := rows.Scan(dst...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
	}
	return ids, nil
}

// scatter 并行地在每个 slot 上查最近的 limit 个订单（before 非 0 时只要订单号小于它的），返回各 slot 的结果和取回的总行数。
// 全站最近订单沿主键倒序扫，订单号的高位是时间，所以按订单号倒序就是按创建时间倒序。
func (g *gen) scatter(ctx context.Context, before int64, limit int) ([][]listRow, int64, error) {
	lists := make([][]listRow, len(g.slots))
	errs := make([]error, len(g.slots))
	var wg sync.WaitGroup
	for slot, si := range g.slots {
		wg.Go(func() {
			q := "SELECT order_id, user_id, status, total_amount, created_at FROM " + sharding.Table("orders", slot) //nolint:gosec // 表名只来自 sharding.Table
			args := []any{}
			if before > 0 {
				q += " WHERE order_id < ?"
				args = append(args, before)
			}
			q += " ORDER BY order_id DESC LIMIT ?"
			args = append(args, limit)
			rows, err := si.db.QueryContext(ctx, q, args...)
			if err != nil {
				errs[slot] = fmt.Errorf("slot %d: %w", slot, err)
				return
			}
			defer rows.Close()
			l := make([]listRow, 0, limit)
			for rows.Next() {
				var r listRow
				if err := rows.Scan(&r.OrderID, &r.UserID, &r.Status, &r.TotalAmount, &r.CreatedAt); err != nil {
					errs[slot] = fmt.Errorf("slot %d: %w", slot, err)
					return
				}
				l = append(l, r)
			}
			if err := rows.Err(); err != nil {
				errs[slot] = fmt.Errorf("slot %d: %w", slot, err)
				return
			}
			lists[slot] = l
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, 0, err
	}
	var n int64
	for _, l := range lists {
		n += int64(len(l))
	}
	return lists, n, nil
}
