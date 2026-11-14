package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
)

// OrderSummary 是用户订单列表里的一行：只有列表页要显示的列，不含明细。
type OrderSummary struct {
	OrderID     int64
	Status      orderv1.OrderStatus
	TotalAmount int64
	Currency    string
	ExpireAt    time.Time
	PaidAt      time.Time // 零值表示未支付
	CreatedAt   time.Time
}

// UserOrdersQuery 是 ListUserOrders 的条件。
type UserOrdersQuery struct {
	UserID int64
	// Status 不为 ORDER_STATUS_UNSPECIFIED 时只返回这个状态的订单。
	Status orderv1.OrderStatus
	// BeforeOrderID 大于 0 时只返回订单号小于它的订单（游标）。
	BeforeOrderID int64
	// Limit 是最多返回的行数，必须大于 0。
	Limit int
}

// ListUserOrders 按订单号从大到小（即创建时间从新到旧）返回用户的订单，只查用户所在的 slot。
// more 表示在返回的这些之后还有满足条件的订单。src 为 Replica 时读从库，结果可能是旧的，只能用于展示。
//
// 查询多取一行来判断有没有下一页，省掉一次 COUNT。走 idx_orders_user (user_id, order_id)：
// user_id 等值、order_id 范围，索引本身就是要的顺序，不排序，读到 limit+1 行就停。
// 按状态筛选不另建索引，在这个索引范围里回表过滤：一个用户的订单不多，回表的行数有上限（同一个用户的全部订单）。
//
// 用 FORCE INDEX 固定索引：带状态筛选时优化器可能改选 idx_orders_status_expire (status, expire_at)，
// 那样要扫描整张表里这个状态的所有订单再排序，一个状态的订单量随全站订单增长，而不是随这个用户。
// 索引被改名或删掉时这条语句直接报错，而不是悄悄变成慢查询。
func (s *Store) ListUserOrders(ctx context.Context, q UserOrdersQuery, src Source) (orders []OrderSummary, more bool, err error) {
	if err := q.validate(); err != nil {
		return nil, false, err
	}
	slot := s.router.ForUser(q.UserID)
	return listUserOrders(ctx, s.sourceOf(slot, src), s.tables[slot].orders, q)
}

func (q UserOrdersQuery) validate() error {
	if q.UserID <= 0 {
		return fmt.Errorf("store: list user orders: user_id must be positive, got %d", q.UserID)
	}
	if q.Limit <= 0 {
		return fmt.Errorf("store: list user orders: limit must be positive, got %d", q.Limit)
	}
	return nil
}

// listUserOrders 在 table 上执行用户订单列表的查询。热表和冷库的表列名、索引名相同，共用这一份。
func listUserOrders(ctx context.Context, db queryer, table string, q UserOrdersQuery) (orders []OrderSummary, more bool, err error) {
	query, args := userOrdersSQL(table, q)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list orders of user %d: %w", q.UserID, err)
	}
	defer rows.Close()
	orders = make([]OrderSummary, 0, min(q.Limit+1, 128))
	for rows.Next() {
		var (
			o      OrderSummary
			status int32
			paidAt sql.NullTime
		)
		if err := rows.Scan(&o.OrderID, &status, &o.TotalAmount, &o.Currency, &o.ExpireAt, &paidAt, &o.CreatedAt); err != nil {
			return nil, false, fmt.Errorf("scan order summary: %w", err)
		}
		o.Status = orderv1.OrderStatus(status)
		o.PaidAt = paidAt.Time
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate orders of user %d: %w", q.UserID, err)
	}
	if len(orders) > q.Limit {
		return orders[:q.Limit], true, nil
	}
	return orders, false, nil
}

// userOrdersSQL 生成 ListUserOrders 的语句。table 只来自 sharding.Table。单独拆出来给测试做 EXPLAIN。
func userOrdersSQL(table string, q UserOrdersQuery) (string, []any) {
	query := "SELECT order_id, status, total_amount, currency, expire_at, paid_at, created_at FROM " + table +
		" FORCE INDEX (idx_orders_user) WHERE user_id = ?"
	args := []any{q.UserID}
	if q.Status != orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED {
		query += " AND status = ?"
		args = append(args, int32(q.Status))
	}
	if q.BeforeOrderID > 0 {
		query += " AND order_id < ?"
		args = append(args, q.BeforeOrderID)
	}
	query += " ORDER BY order_id DESC LIMIT ?"
	args = append(args, q.Limit+1)
	return query, args
}
