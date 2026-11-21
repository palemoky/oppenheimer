package verify

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/palemoky/oppenheimer/pkg/sharding"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
)

// Scope 把对账限定在“这一轮压测”的数据上，用于库里混着大量别的数据的环境（例如集群上带着造数和别的流量）。
//
// 范围由两样东西决定：
//   - SKUs：这一轮压测专用的 SKU，别的流量不会买它们；
//   - 账本：账本里出现过的订单号（OrderIDs）和流程（Flows，即 (user_id, 幂等键)）。
//
// 范围内的订单 = OrderIDs ∪ 预占明细里买了这些 SKU 的订单（经 idx_reservation_item_sku）
// ∪ 订单库里 (user_id, 幂等键) 属于 Flows 的订单（经 uk_user_idem）。最后一项让“重试用尽、没拿到订单号”
// 和“被回收后用同一个幂等键重新提交”的订单也在范围里，第 15 条的两个计数才与不限定范围时一致。
type Scope struct {
	SKUs     []int64
	OrderIDs []int64
	Flows    []IdemRef
}

// NewScope 由 SKU 和账本算出范围。结果去重并排序，与输入的顺序无关。
func NewScope(skus []int64, entries []ledger.Entry) Scope {
	ids := map[int64]bool{}
	flows := map[IdemRef]bool{}
	for _, e := range entries {
		if id, ok := parseOrderID(e.OrderID); ok {
			ids[id] = true
		}
		if e.IdempotencyKey != "" && e.UserID > 0 {
			flows[IdemRef{UserID: e.UserID, Key: e.IdempotencyKey}] = true
		}
	}
	sc := Scope{
		SKUs:     slices.Compact(slices.Sorted(slices.Values(skus))),
		OrderIDs: slices.Sorted(maps.Keys(ids)),
		Flows: slices.SortedFunc(maps.Keys(flows), func(a, b IdemRef) int {
			return cmp.Or(cmp.Compare(a.UserID, b.UserID), cmp.Compare(a.Key, b.Key))
		}),
	}
	return sc
}

// Empty 报告范围是否为空。没有 SKU 就没有范围，按不限定范围采集。
func (sc Scope) Empty() bool { return len(sc.SKUs) == 0 }

// scopeBatch 是点查时一批 IN (...) 的个数：远小于 max_allowed_packet 和占位符上限（65535），
// 又能把往返次数压到订单数的千分之几。
const scopeBatch = 500

// chunks 把 s 切成每段至多 n 个的连续片段。n 小于 1 时按 1 算。
func chunks[T any](s []T, n int) iter.Seq[[]T] {
	return slices.Chunk(s, max(n, 1))
}

// placeholders 返回 n 个用逗号分隔的占位符，n 至少为 1。
func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func int64Args(ids []int64) []any {
	out := make([]any, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}

// CollectScoped 只读范围内的数据。范围为空时与 Collect 相同。
//
// 与 Collect 的差别只在“读什么”，Snapshot 的含义不变，Check 照常执行：
//   - 库存、stock_pending、闸门的库存和已购数只读范围内的 SKU；
//   - 订单、明细、预占、支付单、闸门的占用只读范围内的订单（按主键分批点查，不扫表）；
//   - 商家索引、冷库、死信 topic、库存服务日志不读，Snapshot 里对应的字段保持零值，第 18–21 条跳过
//     （死信和日志不按订单或 SKU 分，读了也没法只算这一轮的）；
//   - outbox 的未发布行数照旧是全库的 COUNT。
func CollectScoped(ctx context.Context, src Sources, initial map[int64]int64, sc Scope) (*Snapshot, error) {
	if sc.Empty() {
		return Collect(ctx, src, initial)
	}
	router, err := sharding.New(sharding.OrderLayout(len(src.OrderDBs)))
	if err != nil {
		return nil, fmt.Errorf("build order shard router: %w", err)
	}
	s := &Snapshot{Initial: initial, OutboxPending: map[string]int64{}}
	ids := map[int64]bool{}
	for _, id := range sc.OrderIDs {
		ids[id] = true
	}

	steps := []struct {
		name string
		fn   func() error
	}{
		{"inventory_db stock", func() error { return collectStockScoped(ctx, src.InventoryDB, sc.SKUs, s) }},
		{"inventory_db reservations by sku", func() error { return reservedOrdersOfSKUs(ctx, src.InventoryDB, sc.SKUs, ids) }},
		{"order_db by idempotency key", func() error { return ordersOfFlows(ctx, src.OrderDBs, router, sc.Flows, ids) }},
		{"order_db", func() error { return collectOrdersScoped(ctx, src.OrderDBs, router, slices.Sorted(maps.Keys(ids)), s) }},
		{"inventory_db reservations", func() error {
			return collectReservationsScoped(ctx, src.InventoryDB, slices.Sorted(maps.Keys(ids)), s)
		}},
		{"payment_db", func() error { return collectPaymentsScoped(ctx, src.PaymentDB, slices.Sorted(maps.Keys(ids)), s) }},
		{"stock gate", func() error { return collectGateScoped(ctx, src, sc.SKUs, slices.Sorted(maps.Keys(ids)), s) }},
	}
	for _, st := range steps {
		if err := st.fn(); err != nil {
			return nil, fmt.Errorf("collect %s: %w", st.name, err)
		}
	}
	return s, nil
}

// pendingStockScopedSQL 是 pendingStockSQL 只取给定 SKU 的版本，两处 %s 都是同样个数的占位符。
// sku_stock 按主键、stock_pending 按 idx_stock_pending_sku 取。
const pendingStockScopedSQL = `
WITH p AS (
	SELECT sku_id, COUNT(*) AS n, SUM(d_available) AS da, SUM(d_reserved) AS dr, SUM(d_sold) AS ds,
	       DATE_FORMAT(MIN(created_at), '%%Y-%%m-%%d %%H:%%i:%%s.%%f') AS oldest,
	       TIMESTAMPDIFF(MICROSECOND, MIN(created_at), NOW(6)) AS age_us
	FROM stock_pending WHERE sku_id IN (%s) GROUP BY sku_id
)
SELECT s.sku_id, 1, s.available, s.reserved, s.sold,
       COALESCE(p.n, 0), COALESCE(p.da, 0), COALESCE(p.dr, 0), COALESCE(p.ds, 0), COALESCE(p.oldest, ''), COALESCE(p.age_us, 0)
FROM sku_stock s LEFT JOIN p ON p.sku_id = s.sku_id WHERE s.sku_id IN (%s)
UNION ALL
SELECT p.sku_id, 0, 0, 0, 0, p.n, p.da, p.dr, p.ds, p.oldest, p.age_us
FROM p WHERE NOT EXISTS (SELECT 1 FROM sku_stock s WHERE s.sku_id = p.sku_id)`

// collectStockScoped 是只读给定 SKU 的 collectStock。SKU 是这一轮压测的几个，不分批。
func collectStockScoped(ctx context.Context, db *sql.DB, skus []int64, s *Snapshot) error {
	ph := placeholders(len(skus))
	args := append(int64Args(skus), int64Args(skus)...)
	return scanStock(ctx, db, fmt.Sprintf(pendingStockScopedSQL, ph, ph), args, s)
}

// reservedOrdersOfSKUs 把预占明细里买过这些 SKU 的订单号加进 ids。
func reservedOrdersOfSKUs(ctx context.Context, db *sql.DB, skus []int64, ids map[int64]bool) error {
	for batch := range chunks(skus, scopeBatch) {
		q := "SELECT DISTINCT order_id FROM reservation_item WHERE sku_id IN (" + placeholders(len(batch)) + ")"
		if err := queryArgs(ctx, db, q, int64Args(batch), func(r *sql.Rows) error {
			var id int64
			if err := r.Scan(&id); err != nil {
				return err
			}
			ids[id] = true
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// ordersOfFlows 按 (user_id, 幂等键) 找订单号加进 ids，走 uk_user_idem。流程按用户的 slot 分组。
func ordersOfFlows(ctx context.Context, dbs []*sql.DB, router *sharding.Router, flows []IdemRef, ids map[int64]bool) error {
	bySlot := map[int][]IdemRef{}
	for _, f := range flows {
		slot := router.ForUser(f.UserID)
		bySlot[slot] = append(bySlot[slot], f)
	}
	for _, slot := range slices.Sorted(maps.Keys(bySlot)) {
		for batch := range chunks(bySlot[slot], scopeBatch) {
			args := make([]any, 0, 2*len(batch))
			for _, f := range batch {
				args = append(args, f.UserID, f.Key)
			}
			q := "SELECT order_id FROM " + sharding.Table("orders", slot) + " WHERE (user_id, idempotency_key) IN (" +
				strings.TrimSuffix(strings.Repeat("(?,?),", len(batch)), ",") + ")" + notSeededAnd("")
			if err := queryArgs(ctx, dbs[router.DB(slot)], q, args, func(r *sql.Rows) error {
				var id int64
				if err := r.Scan(&id); err != nil {
					return err
				}
				ids[id] = true
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// notSeededAnd 是 notSeeded 接在已有 WHERE 条件后面的版本。
func notSeededAnd(prefix string) string {
	return fmt.Sprintf(" AND %suser_id < %d", prefix, seededUserBase)
}

// collectOrdersScoped 按主键点查订单和明细：订单号按 slot（订单号里的分片基因）分组，每组分批 IN。
// 和 collectOrders 一样排除造数的订单，并汇总每个订单库的 outbox 未发布行数。
func collectOrdersScoped(ctx context.Context, dbs []*sql.DB, router *sharding.Router, ids []int64, s *Snapshot) error {
	s.Orders = map[int64]Order{}
	s.OrdersByIdem = map[IdemRef]int64{}
	bySlot := map[int][]int64{}
	for _, id := range ids {
		slot := router.ForOrder(id)
		bySlot[slot] = append(bySlot[slot], id)
	}
	for _, slot := range slices.Sorted(maps.Keys(bySlot)) {
		conn := dbs[router.DB(slot)]
		orders, items := sharding.Table("orders", slot), sharding.Table("order_items", slot)
		for batch := range chunks(bySlot[slot], scopeBatch) {
			ph, args := placeholders(len(batch)), int64Args(batch)
			if err := queryArgs(ctx, conn, `SELECT order_id, user_id, status, total_amount, currency,
		idempotency_key, COALESCE(payment_id, 0), COALESCE(fail_reason, '') FROM `+orders+
				" WHERE order_id IN ("+ph+")"+notSeededAnd(""), args, func(r *sql.Rows) error {
				o := Order{Items: map[int64]int64{}}
				if err := r.Scan(&o.OrderID, &o.UserID, &o.Status, &o.TotalAmount, &o.Currency,
					&o.IdempotencyKey, &o.PaymentID, &o.FailReason); err != nil {
					return err
				}
				s.Orders[o.OrderID] = o
				s.OrdersByIdem[IdemRef{UserID: o.UserID, Key: o.IdempotencyKey}] = o.OrderID
				return nil
			}); err != nil {
				return err
			}
			// 明细的主键是 (order_id, sku_id)，按前缀点查。造数订单上面没有读进来，这里跳过它们的明细。
			if err := queryArgs(ctx, conn, "SELECT order_id, sku_id, quantity FROM "+items+" WHERE order_id IN ("+ph+")", args,
				func(r *sql.Rows) error {
					var orderID, sku, qty int64
					if err := r.Scan(&orderID, &sku, &qty); err != nil {
						return err
					}
					if o, ok := s.Orders[orderID]; ok {
						o.Items[sku] += qty
					}
					return nil
				}); err != nil {
				return err
			}
		}
	}
	var pending int64
	for i, conn := range dbs {
		n, err := countPendingOutbox(ctx, conn)
		if err != nil {
			return fmt.Errorf("order_db_%d: %w", i, err)
		}
		pending += n
	}
	s.OutboxPending["order"] = pending
	return nil
}

// collectReservationsScoped 按订单号分批读预占头记录和明细（明细的主键以 order_id 开头）。
// 明细读的是订单的全部 SKU，不只范围内的：第 5 条要拿它和订单明细整体比较。
func collectReservationsScoped(ctx context.Context, db *sql.DB, ids []int64, s *Snapshot) error {
	s.Reservations = map[int64]Reservation{}
	for batch := range chunks(ids, scopeBatch) {
		ph, args := placeholders(len(batch)), int64Args(batch)
		if err := queryArgs(ctx, db, "SELECT order_id, status FROM reservation WHERE order_id IN ("+ph+")", args,
			func(r *sql.Rows) error {
				res := Reservation{Items: map[int64]int64{}}
				if err := r.Scan(&res.OrderID, &res.Status); err != nil {
					return err
				}
				s.Reservations[res.OrderID] = res
				return nil
			}); err != nil {
			return err
		}
		if err := queryArgs(ctx, db, "SELECT order_id, sku_id, quantity FROM reservation_item WHERE order_id IN ("+ph+")", args,
			func(r *sql.Rows) error {
				var orderID, sku, qty int64
				if err := r.Scan(&orderID, &sku, &qty); err != nil {
					return err
				}
				res, ok := s.Reservations[orderID]
				if !ok {
					// 与 collectInventory 相同：明细没有头记录，当作状态未知的预占。
					res = Reservation{OrderID: orderID, Items: map[int64]int64{}}
					s.Reservations[orderID] = res
				}
				res.Items[sku] += qty
				return nil
			}); err != nil {
			return err
		}
	}
	return nil
}

// collectPaymentsScoped 按 order_id 分批读支付单（uk_payments_order），outbox 照旧数全库。
func collectPaymentsScoped(ctx context.Context, db *sql.DB, ids []int64, s *Snapshot) error {
	for batch := range chunks(ids, scopeBatch) {
		q := "SELECT payment_id, order_id, user_id, status, amount, currency, notified FROM payments WHERE order_id IN (" +
			placeholders(len(batch)) + ")"
		if err := queryArgs(ctx, db, q, int64Args(batch), func(r *sql.Rows) error {
			var p Payment
			if err := r.Scan(&p.PaymentID, &p.OrderID, &p.UserID, &p.Status, &p.Amount, &p.Currency, &p.Notified); err != nil {
				return err
			}
			s.Payments = append(s.Payments, p)
			return nil
		}); err != nil {
			return err
		}
	}
	// 不限定范围时是表的扫描顺序（主键 payment_id）；Check 不依赖顺序，这里排一下让结果稳定。
	slices.SortFunc(s.Payments, func(a, b Payment) int { return cmp.Compare(a.PaymentID, b.PaymentID) })
	n, err := countPendingOutbox(ctx, db)
	if err != nil {
		return err
	}
	s.OutboxPending["payment"] = n
	return nil
}

// collectGateScoped 读范围内 SKU 的闸门库存和已购数，以及范围内订单的占用。
//
// 占用不 SCAN：对每个订单号查 gate:hold:<id> 是否存在、在 gate:holds 和 gate:claimed 里有没有分数，
// 分批放进一个 pipeline。键名与 pkg/stockgate 的脚本一致（collectGate 也是直接读这些键）。
func collectGateScoped(ctx context.Context, src Sources, skus, ids []int64, s *Snapshot) error {
	gate := stockgate.New(src.Redis, src.RedisPrefix)
	s.Gate = map[int64]GateStock{}
	s.Bought = map[int64]map[int64]int64{}
	for _, sku := range skus {
		if err := readGateSKU(ctx, src, gate, sku, s); err != nil {
			return err
		}
	}

	s.Holds = map[int64]bool{}
	s.Unclaimed, s.Claimed = []int64{}, []int64{}
	p := src.RedisPrefix
	for batch := range chunks(ids, scopeBatch) {
		type probe struct {
			hold               *redis.IntCmd
			unclaimed, claimed *redis.FloatCmd
		}
		probes := make([]probe, len(batch))
		pipe := src.Redis.Pipeline()
		for i, id := range batch {
			m := strconv.FormatInt(id, 10)
			probes[i] = probe{
				hold:      pipe.Exists(ctx, p+"gate:hold:"+m),
				unclaimed: pipe.ZScore(ctx, p+"gate:holds", m),
				claimed:   pipe.ZScore(ctx, p+"gate:claimed", m),
			}
		}
		// ZSCORE 对不存在的成员返回 redis.Nil，Exec 会把它当作错误返回，逐条判断。
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("read holds: %w", err)
		}
		for i, id := range batch {
			n, err := probes[i].hold.Result()
			if err != nil {
				return fmt.Errorf("read hold of order %d: %w", id, err)
			}
			if n > 0 {
				s.Holds[id] = true
			}
			for _, z := range []struct {
				cmd *redis.FloatCmd
				dst *[]int64
				key string
			}{{probes[i].unclaimed, &s.Unclaimed, "gate:holds"}, {probes[i].claimed, &s.Claimed, "gate:claimed"}} {
				switch _, err := z.cmd.Result(); {
				case errors.Is(err, redis.Nil):
				case err != nil:
					return fmt.Errorf("read %s member %d: %w", z.key, id, err)
				default:
					*z.dst = append(*z.dst, id)
				}
			}
		}
	}
	return nil
}

// readGateSKU 读一个 SKU 的闸门库存和已购数；SKU 没有装载进闸门时什么也不加（第 11、12 条对它跳过）。
func readGateSKU(ctx context.Context, src Sources, gate *stockgate.Gate, sku int64, s *Snapshot) error {
	st, ok, err := gate.Stock(ctx, sku)
	if err != nil {
		return fmt.Errorf("read gate sku %d: %w", sku, err)
	}
	if !ok {
		return nil
	}
	s.Gate[sku] = GateStock{Avail: st.Avail, Held: st.Held, Limit: st.Limit}

	m, err := src.Redis.HGetAll(ctx, src.RedisPrefix+"gate:bought:"+strconv.FormatInt(sku, 10)).Result()
	if err != nil {
		return fmt.Errorf("read bought of sku %d: %w", sku, err)
	}
	bought := make(map[int64]int64, len(m))
	for u, q := range m {
		uid, err1 := strconv.ParseInt(u, 10, 64)
		qty, err2 := strconv.ParseInt(q, 10, 64)
		if err := errors.Join(err1, err2); err != nil {
			return fmt.Errorf("bad bought entry %q=%q of sku %d: %w", u, q, sku, err)
		}
		bought[uid] = qty
	}
	s.Bought[sku] = bought
	return nil
}
