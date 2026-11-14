package recon

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// Repairer 按订单库修复商家索引（设计文档 7.8）：只修复查后确认的差异，逐个订单，
// 以订单库此刻的映像为准，带“订单库的版本号不低于索引里的”这个条件。
type Repairer struct {
	// primaryOf 把订单库的从库映射到它的主库：修复前的重读要读主库，从库可能落后。
	primaryOf map[*DB]*DB
	index     []*DB
	router    *sharding.Router
	now       func() time.Time
}

// NewRepairer 返回修复商家索引用的 Repairer。
func NewRepairer(topo Topology) (*Repairer, error) {
	if len(topo.MerchantIndex) != sharding.MerchantIndexDBs {
		return nil, fmt.Errorf("recon: repair needs %d merchant index databases, got %d", sharding.MerchantIndexDBs, len(topo.MerchantIndex))
	}
	router, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil {
		return nil, fmt.Errorf("recon: merchant index shard router: %w", err)
	}
	r := &Repairer{primaryOf: map[*DB]*DB{}, index: topo.MerchantIndex, router: router, now: time.Now}
	for i, rep := range topo.OrderReplicas {
		if i < len(topo.OrderPrimaries) {
			r.primaryOf[rep] = topo.OrderPrimaries[i]
		}
	}
	return r, nil
}

// RepairReport 是一次修复的结果。
type RepairReport struct {
	// Orders 是要修的订单数（确认的差异涉及的订单号去重）。
	Orders int `json:"orders"`
	// Upserted 是实际改动了索引行的写入次数，Deleted 是删掉的索引行数。
	Upserted int   `json:"upserted"`
	Deleted  int64 `json:"deleted"`
	// Skipped 是没有修的订单数（原因在 Notes 里）。
	Skipped int `json:"skipped"`
	// Fixed 是修完重读已经一致的订单数，Remaining 是仍然不一致的差异条数。
	Fixed     int      `json:"fixed"`
	Remaining int      `json:"remaining"`
	Notes     []string `json:"notes,omitempty"`
	// RemainingFindings 是修完重读仍然不一致的差异。
	RemainingFindings []Finding `json:"-"`
}

// orderImage 是订单库里一个订单在索引里应有的映像。
type orderImage struct {
	orderID, merchantID, userID int64
	status                      int64
	totalAmount                 int64
	currency                    string
	createdAt                   time.Time
	paidAt                      sql.NullTime
	version                     int64
	table                       string
}

func (o *orderImage) sameContent(p *orderImage) bool {
	return o.merchantID == p.merchantID && o.userID == p.userID && o.status == p.status &&
		o.totalAmount == p.totalAmount && o.currency == p.currency && o.createdAt.Equal(p.createdAt) &&
		o.paidAt.Valid == p.paidAt.Valid && (!o.paidAt.Valid || o.paidAt.Time.Equal(p.paidAt.Time))
}

// 修复用的语句，{T} 换成物理表名（sharding.Table 生成）。
//
// 写入语句和 orderindex 平时的写法一样（store.go），区别只有一处：条件是 new.src_version >= 原版本，
// 不是 >。版本相同而内容不同正是要修的情况（索引行被人改了，版本号没变）；版本更低的映像仍然被挡住，
// 所以修复和 CDC 的消费者并发写同一行时不会把索引写回旧值。
// ON DUPLICATE KEY UPDATE 的赋值从左到右执行，src_version 必须最后赋值，原因见 store.go。
const (
	repairUpsertTemplate = `INSERT INTO {T} (merchant_id, order_id, user_id, status, total_amount, currency,
    created_at, paid_at, src_version, synced_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
AS new
ON DUPLICATE KEY UPDATE
    user_id      = IF(new.src_version >= {T}.src_version, new.user_id,      {T}.user_id),
    status       = IF(new.src_version >= {T}.src_version, new.status,       {T}.status),
    total_amount = IF(new.src_version >= {T}.src_version, new.total_amount, {T}.total_amount),
    currency     = IF(new.src_version >= {T}.src_version, new.currency,     {T}.currency),
    created_at   = IF(new.src_version >= {T}.src_version, new.created_at,   {T}.created_at),
    paid_at      = IF(new.src_version >= {T}.src_version, new.paid_at,      {T}.paid_at),
    synced_at    = IF(new.src_version >= {T}.src_version, new.synced_at,    {T}.synced_at),
    src_version  = IF(new.src_version >= {T}.src_version, new.src_version,  {T}.src_version)`
	// 索引表的主键是 (merchant_id, order_id)，没有以订单号开头的索引。按订单号删是带锁的全表扫描：
	// REPEATABLE READ 下它会给扫过的每一行加锁，挡住 CDC 消费者对整张表的写入。所以分两步：
	// 先用不加锁的读找出这些订单号在索引里现有的行（repairFindTemplate，全表扫描但不加锁，一批订单一次），
	// 再按主键逐行删（repairDeleteTemplate）。
	repairFindTemplate   = `SELECT order_id, merchant_id FROM {T} WHERE order_id IN (%s)`
	repairDeleteTemplate = `DELETE FROM {T} WHERE merchant_id = ? AND order_id = ?`

	// repairSelect 是重读订单行的列。只用于 index 这一对：左侧的表是 orders_N 和 orders_archive，没有别名。
	repairSelect = `SELECT order_id, merchant_id, user_id, status, total_amount, currency, created_at, paid_at, version FROM `
)

func onTable(tmpl string, table string) string { return strings.ReplaceAll(tmpl, "{T}", table) }

// repair 逐个订单修复 fs 涉及的订单，然后重读这些订单，报告修好了多少。
func (r *Repairer) repair(ctx context.Context, c Comparison, left, right *sideSource, fs []Finding) (*RepairReport, error) {
	keys := findingKeys(fs)
	rep := &RepairReport{Orders: len(keys)}
	existing, err := r.indexRows(ctx, keys)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		// 订单行在 indexRows 之后读：merchant_id 写入后不变，订单号早于水位线的订单不会再被创建，
		// 所以“索引里现有哪些行该删”不会因为这中间订单行变了而判断错；要写进去的映像则越新越好。
		img, err := r.readOrder(ctx, c, k)
		if err != nil {
			rep.Skipped++
			rep.Notes = append(rep.Notes, fmt.Sprintf("order %d skipped: %v", k, err))
			continue
		}
		if err := r.apply(ctx, k, img, existing[k], rep); err != nil {
			return nil, err
		}
	}
	after, err := readAndClassify(ctx, left, right, func(s *sideSource, ctx context.Context) ([]row, error) {
		return s.readKeys(ctx, keys)
	})
	if err != nil {
		return nil, fmt.Errorf("recon: reread after repair: %w", err)
	}
	rep.RemainingFindings = after
	rep.Remaining = len(after)
	rep.Fixed = len(keys) - len(findingKeys(after))
	return rep, nil
}

// readOrder 按订单号在左侧各表里重读订单行（读主库），没有时返回 nil。
//
// 按 Side.Tables 的顺序读，热库在前、冷库在后。归档是先写冷库再删热库，所以先读热库、后读冷库时
// 一个正在被搬的订单至少能在一边读到，不会被误判成“订单库里没有”而删掉索引行；反过来的顺序会。
func (r *Repairer) readOrder(ctx context.Context, c Comparison, orderID int64) (*orderImage, error) {
	var found []*orderImage
	for _, t := range c.Left.Tables {
		db := t.DB
		if p, ok := r.primaryOf[db]; ok {
			db = p
		}
		imgs, err := readOrderFrom(ctx, db, t, orderID)
		if err != nil {
			return nil, err
		}
		found = append(found, imgs...)
	}
	if len(found) == 0 {
		return nil, nil //nolint:nilnil // 没有这个订单是正常结果
	}
	// 同一个订单在左侧有不止一份：归档搬到一半（热冷两边相同）时取版本最高的；
	// 版本相同而内容不同时不知道哪份是对的，不修。
	best := found[0]
	for _, o := range found[1:] {
		switch {
		case o.version > best.version:
			best = o
		case o.version == best.version && !o.sameContent(best):
			return nil, fmt.Errorf("two copies with version %d differ (%s, %s)", o.version, best.table, o.table)
		}
	}
	return best, nil
}

func readOrderFrom(ctx context.Context, db *DB, t Table, orderID int64) ([]*orderImage, error) {
	q := repairSelect + t.From + " WHERE order_id = ?" //nolint:gosec // 表名和条件来自 pairs.go 的常量，值走占位符
	if t.Where != "" {
		q += " AND (" + t.Where + ")"
	}
	rows, err := db.Conn.QueryContext(ctx, q, orderID)
	if err != nil {
		return nil, fmt.Errorf("recon: read order %d from %s: %w", orderID, t.Label, err)
	}
	defer rows.Close() //nolint:errcheck // 只读；错误由 rows.Err 报告
	var out []*orderImage
	for rows.Next() {
		o := &orderImage{table: t.Label}
		var merchant sql.NullInt64
		if err := rows.Scan(&o.orderID, &merchant, &o.userID, &o.status, &o.totalAmount, &o.currency,
			&o.createdAt, &o.paidAt, &o.version); err != nil {
			return nil, fmt.Errorf("recon: scan order %d from %s: %w", orderID, t.Label, err)
		}
		if !merchant.Valid {
			continue
		}
		o.merchantID = merchant.Int64
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("recon: read order %d from %s: %w", orderID, t.Label, err)
	}
	return out, nil
}

// indexKey 是索引里现有的一行的主键和它所在的 slot。
type indexKey struct {
	slot       int
	merchantID int64
}

// indexRows 找出 keys 这些订单号在索引各表里现有的行。不加锁的读，每张表每批订单号一次全表扫描。
func (r *Repairer) indexRows(ctx context.Context, keys []int64) (map[int64][]indexKey, error) {
	out := map[int64][]indexKey{}
	for slot := range r.router.Slots() {
		table := sharding.Table("merchant_orders", slot)
		for chunk := range slices.Chunk(keys, maxListArgs) {
			args := make([]any, len(chunk))
			for i, k := range chunk {
				args[i] = k
			}
			q := fmt.Sprintf(onTable(repairFindTemplate, table), placeholders(len(chunk))) //nolint:gosec // 表名由 sharding.Table 生成，值走占位符
			err := func() error {
				rows, err := r.index[r.router.DB(slot)].Conn.QueryContext(ctx, q, args...)
				if err != nil {
					return err
				}
				defer rows.Close() //nolint:errcheck // 只读；错误由 rows.Err 报告
				for rows.Next() {
					var orderID int64
					ir := indexKey{slot: slot}
					if err := rows.Scan(&orderID, &ir.merchantID); err != nil {
						return err
					}
					out[orderID] = append(out[orderID], ir)
				}
				return rows.Err()
			}()
			if err != nil {
				return nil, fmt.Errorf("recon: find index rows on %s: %w", table, err)
			}
		}
	}
	return out, nil
}

// apply 把一个订单的索引行改成 img（nil 表示订单库里没有它，索引里也不该有）。existing 是索引里这个订单现有的行。
//
// merchant_id 写入后不变，所以同一个订单号、商家不同的行一定是错的，在哪个 slot 都删掉。
func (r *Repairer) apply(ctx context.Context, orderID int64, img *orderImage, existing []indexKey, rep *RepairReport) error {
	exec := func(slot int, tmpl string, args ...any) (int64, error) {
		table := sharding.Table("merchant_orders", slot)
		res, err := r.index[r.router.DB(slot)].Conn.ExecContext(ctx, onTable(tmpl, table), args...) //nolint:gosec // 表名由 sharding.Table 生成，值走占位符
		if err != nil {
			return 0, fmt.Errorf("recon: repair order %d on %s: %w", orderID, table, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("recon: repair order %d on %s: %w", orderID, table, err)
		}
		return n, nil
	}
	if img != nil {
		n, err := exec(r.router.ForMerchant(img.merchantID), repairUpsertTemplate, img.merchantID, img.orderID, img.userID, img.status,
			img.totalAmount, img.currency, img.createdAt, img.paidAt, img.version, r.now().UTC())
		if err != nil {
			return err
		}
		if n > 0 {
			rep.Upserted++
		}
	}
	for _, ir := range existing {
		if img != nil && ir.merchantID == img.merchantID {
			continue
		}
		n, err := exec(ir.slot, repairDeleteTemplate, ir.merchantID, orderID)
		if err != nil {
			return err
		}
		rep.Deleted += n
	}
	return nil
}
