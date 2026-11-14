package seedreport

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// ArchiveReport 是冷库一节：三张冷表的大小、分区、和热库的每行字节对比、冷库查询的延迟和执行计划。
type ArchiveReport struct {
	Tables      []ArchiveTable  `json:"tables"`
	Partitions  []PartitionStat `json:"partitions"`
	Compression []Compression   `json:"compression"`
	Queries     []QueryStat     `json:"queries"`
	Notes       []string        `json:"notes,omitempty"`
}

// ArchiveTable 是一张冷表：行数是 COUNT(*) 的精确值，大小是各分区之和。
type ArchiveTable struct {
	Table      string `json:"table"`
	Rows       int64  `json:"rows"`
	DataBytes  int64  `json:"data_bytes"`
	IndexBytes int64  `json:"index_bytes"`
	// BytesPerRow 是 (data+index)/rows；表是空的时为 0。
	BytesPerRow float64 `json:"bytes_per_row"`
}

// PartitionStat 是冷表一个分区的统计。EstRows 取自 information_schema.partitions 的 table_rows，是估计值。
type PartitionStat struct {
	Table      string `json:"table"`
	Partition  string `json:"partition"`
	EstRows    int64  `json:"est_rows"`
	DataBytes  int64  `json:"data_bytes"`
	IndexBytes int64  `json:"index_bytes"`
}

// Compression 是同一张表在热库（各 slot 汇总）和冷库里每行字节数的对比。
// 热库被归档掏空以后，页里全是空洞，热库的每行字节没有意义，所以同时给出热库的行数，由读报告的人判断。
// 任何一边的行数是 0 时，该边的每行字节和 Ratio 记 0（不可比）。
type Compression struct {
	Table           string  `json:"table"`
	HotRows         int64   `json:"hot_rows"`
	HotBytesPerRow  float64 `json:"hot_bytes_per_row"`
	ColdRows        int64   `json:"cold_rows"`
	ColdBytesPerRow float64 `json:"cold_bytes_per_row"`
	// Ratio 是热库每行字节 / 冷库每行字节。
	Ratio float64 `json:"ratio"`
}

// 冷表名，与 services/order/migrations_archive 一致。
const (
	coldOrders   = "orders_archive"
	coldItems    = "order_items_archive"
	coldPayments = "payment_results_archive"
)

// coldUserCursors 是带游标的用户列表最多取多少个用户（每个用户要算一次游标）。
const coldUserCursors = 50

func perRow(bytes, rows int64) float64 {
	if rows <= 0 {
		return 0
	}
	return float64(bytes) / float64(rows)
}

func (g *gen) archive(ctx context.Context) error {
	ar := &ArchiveReport{}
	g.rep.Archive = ar
	db := g.cfg.ArchiveDB

	for _, t := range []string{coldOrders, coldItems, coldPayments} {
		st, parts, err := g.archiveTableStat(ctx, db, t)
		if err != nil {
			return err
		}
		ar.Tables = append(ar.Tables, st)
		ar.Partitions = append(ar.Partitions, parts...)
	}
	if !g.cfg.Analyze {
		ar.Notes = append(ar.Notes, "sizes come from InnoDB persistent statistics and may be stale; run with -analyze to refresh them (ANALYZE LOCAL TABLE, not logged to the binlog)")
	}
	ar.Notes = append(ar.Notes, "partition rows are information_schema.partitions.table_rows, an estimate; the table rows above are exact COUNT(*)")

	if err := g.compression(ctx, ar); err != nil {
		return err
	}

	orders := ar.Tables[0]
	if orders.Rows == 0 {
		ar.Notes = append(ar.Notes, "no orders in the cold db: cold queries skipped")
		return nil
	}
	classes, err := g.coldClasses(ctx, db, orders.Rows, ar)
	if err != nil {
		return err
	}
	ar.Queries, err = g.runClasses(ctx, classes)
	return err
}

// archiveTableStat 读一张冷表的精确行数，以及按分区汇总的大小。
func (g *gen) archiveTableStat(ctx context.Context, db *sql.DB, table string) (ArchiveTable, []PartitionStat, error) {
	t := ArchiveTable{Table: table}
	conn, err := db.Conn(ctx)
	if err != nil {
		return t, nil, fmt.Errorf("conn: %w", err)
	}
	defer conn.Close()
	if g.cfg.Analyze {
		if _, err := conn.ExecContext(ctx, "ANALYZE LOCAL TABLE "+table); err != nil { //nolint:gosec // 表名是常量
			return t, nil, fmt.Errorf("analyze %s: %w", table, err)
		}
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		return t, nil, fmt.Errorf("set stats expiry: %w", err)
	}
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&t.Rows); err != nil { //nolint:gosec // 表名是常量
		return t, nil, fmt.Errorf("count %s: %w", table, err)
	}
	rows, err := conn.QueryContext(ctx, `SELECT partition_name, COALESCE(table_rows, 0), COALESCE(data_length, 0), COALESCE(index_length, 0)
		FROM information_schema.partitions WHERE table_schema = DATABASE() AND table_name = ?
		ORDER BY partition_ordinal_position`, table)
	if err != nil {
		return t, nil, fmt.Errorf("partitions of %s: %w", table, err)
	}
	defer rows.Close()
	var parts []PartitionStat
	for rows.Next() {
		p := PartitionStat{Table: table}
		var name sql.NullString // 没分区的表这一列是 NULL
		if err := rows.Scan(&name, &p.EstRows, &p.DataBytes, &p.IndexBytes); err != nil {
			return t, nil, fmt.Errorf("scan partitions of %s: %w", table, err)
		}
		p.Partition = name.String
		t.DataBytes += p.DataBytes
		t.IndexBytes += p.IndexBytes
		parts = append(parts, p)
	}
	if err := rows.Err(); err != nil {
		return t, nil, fmt.Errorf("partitions of %s: %w", table, err)
	}
	if len(parts) == 0 {
		return t, nil, fmt.Errorf("table %s not found in the archive db", table)
	}
	t.BytesPerRow = perRow(t.DataBytes+t.IndexBytes, t.Rows)
	return t, parts, nil
}

// compression 算热库（各 slot 汇总）和冷库每行字节的对比。orders、order_items 的热库统计在 tableStats 里已经读过，
// payment_results 这里单独读。
func (g *gen) compression(ctx context.Context, ar *ArchiveReport) error {
	type sum struct{ rows, bytes int64 }
	hot := map[string]*sum{"orders": {}, "order_items": {}, "payment_results": {}}
	for _, t := range g.rep.OrderTables {
		kind, _, _ := strings.Cut(t.Table, "_")
		if strings.HasPrefix(t.Table, "order_items_") {
			kind = "order_items"
		}
		if s := hot[kind]; s != nil {
			s.rows += t.Rows
			s.bytes += t.DataBytes + t.IndexBytes
		}
	}
	for i, db := range g.cfg.OrderDBs {
		for _, slot := range g.or.SlotsOf(i) {
			t, err := g.tableStat(ctx, db, i, slot, sharding.Table("payment_results", slot))
			if err != nil {
				return err
			}
			hot["payment_results"].rows += t.Rows
			hot["payment_results"].bytes += t.DataBytes + t.IndexBytes
		}
	}
	for i, kind := range []string{"orders", "order_items", "payment_results"} {
		cold := ar.Tables[i]
		c := Compression{Table: kind, HotRows: hot[kind].rows, ColdRows: cold.Rows}
		c.HotBytesPerRow = perRow(hot[kind].bytes, hot[kind].rows)
		c.ColdBytesPerRow = cold.BytesPerRow
		if c.HotBytesPerRow > 0 && c.ColdBytesPerRow > 0 {
			c.Ratio = c.HotBytesPerRow / c.ColdBytesPerRow
		}
		ar.Compression = append(ar.Compression, c)
	}
	ar.Notes = append(ar.Notes, "hot bytes per row is meaningless once the hot dbs have been archived (the pages are full of holes): compare it only when the hot row counts are comparable to the cold ones")
	return nil
}

// coldClasses 给出冷库的三类查询：按订单号点查、用户列表第一页、用户列表带游标的一页。
// 取样方式与热库一样：在订单号范围里取一个随机值，取不小于它的第一个订单。
func (g *gen) coldClasses(ctx context.Context, db *sql.DB, total int64, ar *ArchiveReport) ([]class, error) {
	var lo, hi int64
	if err := db.QueryRowContext(ctx, "SELECT MIN(order_id), MAX(order_id) FROM "+coldOrders).Scan(&lo, &hi); err != nil {
		return nil, fmt.Errorf("min/max of %s: %w", coldOrders, err)
	}
	var ss []sample
	for range g.cfg.Runs {
		from := lo + g.r.Int64N(hi-lo+1)
		var s sample
		if err := db.QueryRowContext(ctx, "SELECT order_id, user_id FROM "+coldOrders+" WHERE order_id >= ? ORDER BY order_id LIMIT 1", from).
			Scan(&s.orderID, &s.userID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, fmt.Errorf("sample %s: %w", coldOrders, err)
		}
		ss = append(ss, s)
	}
	if len(ss) == 0 {
		ar.Notes = append(ar.Notes, fmt.Sprintf("could not sample any order of the %d in the cold db: cold queries skipped", total))
		return nil, nil
	}
	ps := g.cfg.PageSize
	classes := []class{
		{"cold: order by id", func(i int) (call, error) {
			return call{db, getOrderSQLOn(coldOrders), []any{ss[i%len(ss)].orderID}}, nil
		}},
		{"cold: user list, first page", func(int) (call, error) {
			q, args := userListSQLOn(coldOrders, ss[g.r.IntN(len(ss))].userID, 0, 0, ps)
			return call{db, q, args}, nil
		}},
	}

	// 带游标的一页：游标取用户在冷库里订单号的中位数。每个用户要算一次，最多取 coldUserCursors 个至少有两个订单的用户。
	type cursor struct{ user, before int64 }
	var cursors []cursor
	seen := map[int64]bool{}
	for _, s := range ss {
		if len(cursors) == coldUserCursors {
			break
		}
		if seen[s.userID] {
			continue
		}
		seen[s.userID] = true
		var n int64
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+coldOrders+" FORCE INDEX (idx_orders_user) WHERE user_id = ?", s.userID).Scan(&n); err != nil {
			return nil, fmt.Errorf("count cold orders of user %d: %w", s.userID, err)
		}
		if n < 2 {
			continue
		}
		var id int64
		if err := db.QueryRowContext(ctx, "SELECT order_id FROM "+coldOrders+" FORCE INDEX (idx_orders_user) WHERE user_id = ? ORDER BY order_id DESC LIMIT 1 OFFSET ?",
			s.userID, n/2).Scan(&id); err != nil {
			return nil, fmt.Errorf("median cursor of user %d: %w", s.userID, err)
		}
		cursors = append(cursors, cursor{s.userID, id})
	}
	if len(cursors) == 0 {
		ar.Notes = append(ar.Notes, "no sampled user has two or more cold orders: cold cursor page skipped")
		return classes, nil
	}
	classes = append(classes, class{"cold: user list, cursor page (median)", func(int) (call, error) {
		c := cursors[g.r.IntN(len(cursors))]
		q, args := userListSQLOn(coldOrders, c.user, 0, c.before, ps)
		return call{db, q, args}, nil
	}})
	return classes, nil
}
