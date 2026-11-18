package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// CheckStockPending 是“库存增量都已应用”这条检查的编号。
const CheckStockPending = 22

// pendingExamples 是第 22 条最多逐个 SKU 列出的个数（会让数变负的排在最前）。
const pendingExamples = 20

// errNoStockPending 表示库存库里没有 stock_pending 表：连的是 00005 迁移之前的旧库。
// 这不是“没有未应用的增量”，不能当作空表放行。
var errNoStockPending = errors.New("table stock_pending does not exist (inventory db without migration 00005, " +
	"an old version?)")

const errNoSuchTable = 1146 // ER_NO_SUCH_TABLE

// wrapPendingErr 把“表不存在”换成 errNoStockPending，其余原样返回。
func wrapPendingErr(err error) error {
	if me, ok := errors.AsType[*mysql.MySQLError](err); ok && me.Number == errNoSuchTable {
		return fmt.Errorf("%w: %w", errNoStockPending, err)
	}
	return err
}

// PendingSKU 是 stock_pending 里一个 SKU 没有应用的增量（设计 1d 第 3 节，ADR-0018）。
type PendingSKU struct {
	SKUID int64
	Rows  int64
	// DAvailable、DReserved、DSold 是各行三个增量的合计。
	DAvailable, DReserved, DSold int64
	// Oldest 是最老一行的 created_at（库的时钟，已格式化），OldestAge 是它到库的 NOW(6) 的时长。
	Oldest    string
	OldestAge time.Duration
}

// pendingStockSQL 一条语句读出每个 SKU 的两组数：sku_stock 的原始值，和 stock_pending 里同一个 SKU 的增量合计。
// 第二段兜住“有增量但 sku_stock 里没有这个 SKU”的情况（这时 in_stock 为 0）。
const pendingStockSQL = `
WITH p AS (
	SELECT sku_id, COUNT(*) AS n, SUM(d_available) AS da, SUM(d_reserved) AS dr, SUM(d_sold) AS ds,
	       DATE_FORMAT(MIN(created_at), '%Y-%m-%d %H:%i:%s.%f') AS oldest,
	       TIMESTAMPDIFF(MICROSECOND, MIN(created_at), NOW(6)) AS age_us
	FROM stock_pending GROUP BY sku_id
)
SELECT s.sku_id, 1, s.available, s.reserved, s.sold,
       COALESCE(p.n, 0), COALESCE(p.da, 0), COALESCE(p.dr, 0), COALESCE(p.ds, 0), COALESCE(p.oldest, ''), COALESCE(p.age_us, 0)
FROM sku_stock s LEFT JOIN p ON p.sku_id = s.sku_id
UNION ALL
SELECT p.sku_id, 0, 0, 0, 0, p.n, p.da, p.dr, p.ds, p.oldest, p.age_us
FROM p WHERE NOT EXISTS (SELECT 1 FROM sku_stock s WHERE s.sku_id = p.sku_id)`

// collectStock 读 sku_stock 和 stock_pending：Snapshot.RawStock 是表里的值，Snapshot.Stock 是真实的数
// （原始值加上没有应用的增量），Snapshot.Pending 是各 SKU 没有应用的增量。
func collectStock(ctx context.Context, db *sql.DB, s *Snapshot) error {
	return scanStock(ctx, db, pendingStockSQL, nil, s)
}

// scanStock 执行 pendingStockSQL 或它只取部分 SKU 的版本（列相同），填 Stock、RawStock、Pending。
func scanStock(ctx context.Context, db *sql.DB, query string, args []any, s *Snapshot) error {
	s.Stock = map[int64]SKUStock{}
	s.RawStock = map[int64]SKUStock{}
	s.Pending = map[int64]PendingSKU{}
	return wrapPendingErr(queryArgs(ctx, db, query, args, func(r *sql.Rows) error {
		var (
			raw       SKUStock
			inStock   int
			p         PendingSKU
			ageMicros int64
		)
		if err := r.Scan(&raw.SKUID, &inStock, &raw.Available, &raw.Reserved, &raw.Sold,
			&p.Rows, &p.DAvailable, &p.DReserved, &p.DSold, &p.Oldest, &ageMicros); err != nil {
			return err
		}
		p.SKUID, p.OldestAge = raw.SKUID, time.Duration(ageMicros)*time.Microsecond
		if p.Rows > 0 {
			s.Pending[p.SKUID] = p
		}
		if inStock == 0 {
			return nil
		}
		s.RawStock[raw.SKUID] = raw
		s.Stock[raw.SKUID] = SKUStock{
			SKUID:     raw.SKUID,
			Available: raw.Available + p.DAvailable,
			Reserved:  raw.Reserved + p.DReserved,
			Sold:      raw.Sold + p.DSold,
		}
		return nil
	}))
}

// stockPending 是第 22 条：对账时 stock_pending 必须为空。
//
// 静止之后增量应该都被合并进 sku_stock 了。不空而且不再减少，说明某个 SKU 卡住：合并会让某个数变负，
// 即闸门放行的数量超过了 MySQL 里有的（超卖），或者账不平。这是异步扣减之后发现超卖的地方。
func (c *checker) stockPending() {
	if len(c.s.Pending) == 0 {
		return
	}
	type row struct {
		p   PendingSKU
		neg []string // 应用之后变负的数
		raw SKUStock
		has bool
	}
	var rows []row
	var total, negative int64
	for _, sku := range sortedKeys(c.s.Pending) {
		p := c.s.Pending[sku]
		raw, has := c.s.RawStock[sku]
		r := row{p: p, raw: raw, has: has}
		if has {
			for _, n := range []struct {
				name       string
				cur, delta int64
			}{{"available", raw.Available, p.DAvailable}, {"reserved", raw.Reserved, p.DReserved}, {"sold", raw.Sold, p.DSold}} {
				if n.cur+n.delta < 0 {
					r.neg = append(r.neg, fmt.Sprintf("%s %d%+d = %d", n.name, n.cur, n.delta, n.cur+n.delta))
				}
			}
		}
		total += p.Rows
		if len(r.neg) > 0 {
			negative++
		}
		rows = append(rows, r)
	}
	// 会变负的排在前面，其余按 SKU 升序（sortedKeys 已排序，稳定排序保持）。
	slices.SortStableFunc(rows, func(a, b row) int {
		switch {
		case len(a.neg) > 0 && len(b.neg) == 0:
			return -1
		case len(a.neg) == 0 && len(b.neg) > 0:
			return 1
		}
		return 0
	})
	head := fmt.Sprintf("%d stock_pending rows for %d skus were not applied to sku_stock", total, len(rows))
	if negative > 0 {
		head += fmt.Sprintf("; %d skus would go NEGATIVE once applied: OVERSOLD (the gate let through more than mysql has)", negative)
	}
	c.fail(CheckStockPending, "stock_pending", "%s", head)
	for _, r := range rows[:min(len(rows), pendingExamples)] {
		detail := fmt.Sprintf("%d rows, sum d_available %d d_reserved %d d_sold %d, oldest written %s (%s ago); ",
			r.p.Rows, r.p.DAvailable, r.p.DReserved, r.p.DSold, r.p.Oldest, r.p.OldestAge.Round(time.Millisecond))
		switch {
		case !r.has:
			detail += "the sku is not in sku_stock"
		case len(r.neg) > 0:
			detail += fmt.Sprintf("sku_stock now available %d reserved %d sold %d; OVERSOLD, applying them makes %s",
				r.raw.Available, r.raw.Reserved, r.raw.Sold, strings.Join(r.neg, ", "))
		default:
			detail += fmt.Sprintf("sku_stock now available %d reserved %d sold %d; applying them would not make a number negative",
				r.raw.Available, r.raw.Reserved, r.raw.Sold)
		}
		c.fail(CheckStockPending, skuSubj(r.p.SKUID), "%s", detail)
	}
	if len(rows) > pendingExamples {
		c.note("check 22: %d more skus with unapplied stock_pending rows not listed", len(rows)-pendingExamples)
	}
}

// pendingProbe 是静止条件里 stock_pending 的当前值。
type pendingProbe struct {
	Rows      int64
	OldestAge time.Duration
	SKUs      []int64 // 至多 pendingProbeSKUs 个，升序
}

const pendingProbeSKUs = 10

func probePending(ctx context.Context, db *sql.DB) (pendingProbe, error) {
	var p pendingProbe
	var ageMicros int64
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*), COALESCE(TIMESTAMPDIFF(MICROSECOND, MIN(created_at), NOW(6)), 0) FROM stock_pending",
	).Scan(&p.Rows, &ageMicros); err != nil {
		return p, fmt.Errorf("count stock_pending: %w", wrapPendingErr(err))
	}
	p.OldestAge = time.Duration(ageMicros) * time.Microsecond
	if p.Rows == 0 {
		return p, nil
	}
	err := queryEach(ctx, db, fmt.Sprintf("SELECT DISTINCT sku_id FROM stock_pending ORDER BY sku_id LIMIT %d", pendingProbeSKUs),
		func(r *sql.Rows) error {
			var sku int64
			if err := r.Scan(&sku); err != nil {
				return err
			}
			p.SKUs = append(p.SKUs, sku)
			return nil
		})
	return p, err
}
