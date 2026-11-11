// Package store 封装 order 服务对 order_db 的全部 SQL 访问。
//
// 每个方法对应一条或一组语句，不做业务判断。状态转换都以前置状态为条件，
// 返回值里的 bool 表示这次调用是否真的完成了转换（受影响行数为 1），
// 由调用方据此决定下一步，而不是先查后改。
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	eventv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/event/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
	"github.com/palemoky/Oppenheimer/pkg/outbox"
)

var (
	// ErrNotFound 表示要找的记录不存在。
	ErrNotFound = errors.New("store: not found")
	// ErrDuplicate 表示插入违反了唯一约束。
	ErrDuplicate = errors.New("store: duplicate key")
)

// Order 是 orders 表的一行及其商品明细。
type Order struct {
	OrderID        int64
	UserID         int64
	Status         orderv1.OrderStatus
	TotalAmount    int64
	Currency       string
	IdempotencyKey string
	RequestHash    []byte
	PaymentID      int64  // 0 表示未支付
	FailReason     string // 空表示没有
	ExpireAt       time.Time
	PaidAt         time.Time // 零值表示未支付
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Items          []Item
}

// Item 是 order_items 表的一行。
type Item struct {
	SKUID     int64
	Quantity  int32
	UnitPrice int64
}

// PaymentResult 是 payment_results 表的一行。
type PaymentResult struct {
	PaymentID   int64
	OrderID     int64
	Succeeded   bool
	Amount      int64
	Disposition eventv1.PaymentDisposition
}

// OrderRef 是后台扫描返回的最小信息。user_id 写进事件里，下单后不会变。
type OrderRef struct {
	OrderID int64
	UserID  int64
}

// Store 访问 order_db。
type Store struct {
	db *sql.DB
}

// New 返回使用 db 的 Store。
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

const orderColumns = `order_id, user_id, status, total_amount, currency, idempotency_key, request_hash,
	payment_id, fail_reason, expire_at, paid_at, created_at, updated_at`

// queryer 让读订单的代码在事务内外共用。
type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// InsertOrder 在一个事务里插入订单和明细。
// (user_id, idempotency_key) 或 order_id 冲突时返回 ErrDuplicate。
func (s *Store) InsertOrder(ctx context.Context, o *Order) error {
	err := mysqlx.InTx(ctx, s.db, func(tx *sql.Tx) error {
		// created_at、updated_at 显式写入，使所有时间都来自服务注入的时钟，
		// 后台任务按时限扫描时比较的是同一个时间源。
		_, err := tx.ExecContext(ctx, `INSERT INTO orders
			(order_id, user_id, status, total_amount, currency, idempotency_key, request_hash,
			 expire_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			o.OrderID, o.UserID, int32(o.Status), o.TotalAmount, o.Currency, o.IdempotencyKey, o.RequestHash,
			o.ExpireAt, o.CreatedAt, o.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert order: %w", err)
		}
		if len(o.Items) == 0 {
			return nil
		}
		var b strings.Builder
		b.WriteString("INSERT INTO order_items (order_id, sku_id, quantity, unit_price, created_at) VALUES ")
		args := make([]any, 0, len(o.Items)*5)
		for i, it := range o.Items {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("(?, ?, ?, ?, ?)")
			args = append(args, o.OrderID, it.SKUID, it.Quantity, it.UnitPrice, o.CreatedAt)
		}
		if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
			return fmt.Errorf("insert order items: %w", err)
		}
		return nil
	})
	if mysqlx.IsDuplicateKey(err) {
		return fmt.Errorf("%w: %w", ErrDuplicate, err)
	}
	return err
}

// GetOrder 按 order_id 读订单及明细。
func (s *Store) GetOrder(ctx context.Context, orderID int64) (*Order, error) {
	return getOrder(ctx, s.db, "WHERE order_id = ?", orderID)
}

// GetOrderByIdempotencyKey 按 (user_id, idempotency_key) 读订单及明细。
func (s *Store) GetOrderByIdempotencyKey(ctx context.Context, userID int64, key string) (*Order, error) {
	return getOrder(ctx, s.db, "WHERE user_id = ? AND idempotency_key = ?", userID, key)
}

func getOrder(ctx context.Context, q queryer, where string, args ...any) (*Order, error) {
	o, err := scanOrder(q.QueryRowContext(ctx, "SELECT "+orderColumns+" FROM orders "+where, args...))
	if err != nil {
		return nil, err
	}
	if o.Items, err = listItems(ctx, q, o.OrderID); err != nil {
		return nil, err
	}
	return o, nil
}

func scanOrder(row *sql.Row) (*Order, error) {
	var (
		o          Order
		status     int32
		paymentID  sql.NullInt64
		failReason sql.NullString
		paidAt     sql.NullTime
	)
	err := row.Scan(&o.OrderID, &o.UserID, &status, &o.TotalAmount, &o.Currency, &o.IdempotencyKey, &o.RequestHash,
		&paymentID, &failReason, &o.ExpireAt, &paidAt, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan order: %w", err)
	}
	o.Status = orderv1.OrderStatus(status)
	o.PaymentID = paymentID.Int64
	o.FailReason = failReason.String
	o.PaidAt = paidAt.Time
	return &o, nil
}

func listItems(ctx context.Context, q queryer, orderID int64) ([]Item, error) {
	rows, err := q.QueryContext(ctx,
		"SELECT sku_id, quantity, unit_price FROM order_items WHERE order_id = ? ORDER BY sku_id", orderID)
	if err != nil {
		return nil, fmt.Errorf("query order items: %w", err)
	}
	defer rows.Close()
	var items []Item
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.SKUID, &it.Quantity, &it.UnitPrice); err != nil {
			return nil, fmt.Errorf("scan order item: %w", err)
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate order items: %w", err)
	}
	return items, nil
}

// MarkReserved 把 CREATING 的订单改为 PENDING_PAYMENT，写入总金额、币种和各商品单价。
// 订单已不在 CREATING 时返回 false，什么都不改。
func (s *Store) MarkReserved(ctx context.Context, orderID, total int64, currency string, items []Item, now time.Time) (bool, error) {
	var done bool
	err := mysqlx.InTx(ctx, s.db, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE orders SET status = ?, total_amount = ?, currency = ?, updated_at = ?
			WHERE order_id = ? AND status = ?`,
			int32(orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT), total, currency, now,
			orderID, int32(orderv1.OrderStatus_ORDER_STATUS_CREATING))
		if err != nil {
			return fmt.Errorf("mark reserved: %w", err)
		}
		if done, err = affectedOne(res); err != nil || !done {
			return err
		}
		for _, it := range items {
			if _, err := tx.ExecContext(ctx, "UPDATE order_items SET unit_price = ? WHERE order_id = ? AND sku_id = ?",
				it.UnitPrice, orderID, it.SKUID); err != nil {
				return fmt.Errorf("update unit price: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return done, nil
}

// ListExpired 返回截止时间早于 now 的 PENDING_PAYMENT 订单。
func (s *Store) ListExpired(ctx context.Context, now time.Time, limit int) ([]OrderRef, error) {
	return s.listRefs(ctx, "SELECT order_id, user_id FROM orders WHERE status = ? AND expire_at < ? ORDER BY expire_at LIMIT ?",
		int32(orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT), now, limit)
}

// ListStaleCreating 返回落库时间（updated_at，CREATING 的行落库后不再更新）早于 before 仍在 CREATING 的订单。
func (s *Store) ListStaleCreating(ctx context.Context, before time.Time, limit int) ([]OrderRef, error) {
	return s.listRefs(ctx, "SELECT order_id, user_id FROM orders WHERE status = ? AND updated_at < ? ORDER BY updated_at LIMIT ?",
		int32(orderv1.OrderStatus_ORDER_STATUS_CREATING), before, limit)
}

func (s *Store) listRefs(ctx context.Context, query string, args ...any) ([]OrderRef, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	defer rows.Close()
	var refs []OrderRef
	for rows.Next() {
		var r OrderRef
		if err := rows.Scan(&r.OrderID, &r.UserID); err != nil {
			return nil, fmt.Errorf("scan order ref: %w", err)
		}
		refs = append(refs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate order refs: %w", err)
	}
	return refs, nil
}

func affectedOne(res sql.Result) (bool, error) {
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}
	return n == 1, nil
}

// InTx 在一个事务里执行 fn，事务内的操作通过 Tx 完成。
func (s *Store) InTx(ctx context.Context, fn func(tx *Tx) error) error {
	return mysqlx.InTx(ctx, s.db, func(tx *sql.Tx) error {
		return fn(&Tx{tx: tx})
	})
}

// Tx 是事务内可用的操作。
type Tx struct {
	tx *sql.Tx
}

// LockOrder 读订单并加行锁，使同一订单上的其他加锁读和条件更新排队等待本事务结束。
func (t *Tx) LockOrder(ctx context.Context, orderID int64) (*Order, error) {
	return scanOrder(t.tx.QueryRowContext(ctx, "SELECT "+orderColumns+" FROM orders WHERE order_id = ? FOR UPDATE", orderID))
}

// GetPaymentResult 读已处理过的支付结果。
func (t *Tx) GetPaymentResult(ctx context.Context, paymentID int64) (*PaymentResult, error) {
	var (
		r           PaymentResult
		disposition int32
	)
	err := t.tx.QueryRowContext(ctx,
		"SELECT payment_id, order_id, succeeded, amount, disposition FROM payment_results WHERE payment_id = ?",
		paymentID).Scan(&r.PaymentID, &r.OrderID, &r.Succeeded, &r.Amount, &disposition)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get payment result: %w", err)
	}
	r.Disposition = eventv1.PaymentDisposition(disposition)
	return &r, nil
}

// InsertPaymentResult 记录一笔支付结果及给出的答复。payment_id 已存在时返回 ErrDuplicate。
func (t *Tx) InsertPaymentResult(ctx context.Context, r *PaymentResult, now time.Time) error {
	_, err := t.tx.ExecContext(ctx, `INSERT INTO payment_results
		(payment_id, order_id, succeeded, amount, disposition, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		r.PaymentID, r.OrderID, r.Succeeded, r.Amount, int32(r.Disposition), now)
	if mysqlx.IsDuplicateKey(err) {
		return fmt.Errorf("insert payment result: %w: %w", ErrDuplicate, err)
	}
	if err != nil {
		return fmt.Errorf("insert payment result: %w", err)
	}
	return nil
}

// MarkPaid 把 PENDING_PAYMENT 的订单改为 PAID。
func (t *Tx) MarkPaid(ctx context.Context, orderID, paymentID int64, now time.Time) (bool, error) {
	res, err := t.tx.ExecContext(ctx, `UPDATE orders SET status = ?, payment_id = ?, paid_at = ?, updated_at = ?
		WHERE order_id = ? AND status = ?`,
		int32(orderv1.OrderStatus_ORDER_STATUS_PAID), paymentID, now, now,
		orderID, int32(orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT))
	if err != nil {
		return false, fmt.Errorf("mark paid: %w", err)
	}
	return affectedOne(res)
}

// MarkCreateFailed 把 CREATING 的订单改为 FAILED。
func (t *Tx) MarkCreateFailed(ctx context.Context, orderID int64, reason string, now time.Time) (bool, error) {
	return t.exec(ctx, `UPDATE orders SET status = ?, fail_reason = ?, updated_at = ?
		WHERE order_id = ? AND status = ?`,
		int32(orderv1.OrderStatus_ORDER_STATUS_FAILED), reason, now,
		orderID, int32(orderv1.OrderStatus_ORDER_STATUS_CREATING))
}

// Cancel 把属于 userID 的 PENDING_PAYMENT 订单改为 CANCELLED。
func (t *Tx) Cancel(ctx context.Context, orderID, userID int64, now time.Time) (bool, error) {
	return t.exec(ctx, `UPDATE orders SET status = ?, updated_at = ?
		WHERE order_id = ? AND user_id = ? AND status = ?`,
		int32(orderv1.OrderStatus_ORDER_STATUS_CANCELLED), now,
		orderID, userID, int32(orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT))
}

// CloseExpired 把截止时间早于 now 的 PENDING_PAYMENT 订单改为 CLOSED。
func (t *Tx) CloseExpired(ctx context.Context, orderID int64, now time.Time) (bool, error) {
	return t.exec(ctx, `UPDATE orders SET status = ?, updated_at = ?
		WHERE order_id = ? AND status = ? AND expire_at < ?`,
		int32(orderv1.OrderStatus_ORDER_STATUS_CLOSED), now,
		orderID, int32(orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT), now)
}

// AddOutbox 在本事务里写入待发布的消息，与本事务的状态变更一起提交或回滚。
func (t *Tx) AddOutbox(ctx context.Context, msgs ...outbox.Message) error {
	return outbox.Add(ctx, t.tx, msgs...)
}

func (t *Tx) exec(ctx context.Context, query string, args ...any) (bool, error) {
	res, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("exec: %w", err)
	}
	return affectedOne(res)
}
