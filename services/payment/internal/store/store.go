// Package store 封装 payments 表的 SQL 访问。
//
// 每个方法对应一条语句，不做业务判断。所有状态转换都是带前置状态的条件更新，
// 返回值 bool 表示这次调用是否真的完成了转换：并发的多个调用者里只有一个会得到 true。
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
)

var (
	// ErrNotFound 表示支付单不存在。
	ErrNotFound = errors.New("payment not found")
	// ErrDuplicate 表示同一订单已有支付单（order_id 唯一键冲突）。
	ErrDuplicate = errors.New("payment already exists for order")
)

// Payment 是 payments 表的一行。Status、Outcome 的数值与 proto 枚举一致，直接用枚举类型承载。
type Payment struct {
	PaymentID int64
	OrderID   int64
	UserID    int64
	Status    paymentv1.PaymentStatus
	Amount    int64
	Currency  string
	Outcome   paymentv1.SimulatedOutcome
	// Notified 表示订单服务已经答复了这笔支付的结果（order.events 的 payment_result_handled）。
	Notified  bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Store 访问 payments 表。
type Store struct {
	db *sql.DB
}

// New 返回使用 db 的 Store。
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

const columns = "payment_id, order_id, user_id, status, amount, currency, outcome, notified, created_at, updated_at"

type scanner interface {
	Scan(dest ...any) error
}

func scanPayment(row scanner) (Payment, error) {
	var (
		p               Payment
		status, outcome int32
	)
	err := row.Scan(&p.PaymentID, &p.OrderID, &p.UserID, &status, &p.Amount, &p.Currency,
		&outcome, &p.Notified, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return Payment{}, err
	}
	p.Status = paymentv1.PaymentStatus(status)
	p.Outcome = paymentv1.SimulatedOutcome(outcome)
	p.CreatedAt = p.CreatedAt.UTC()
	p.UpdatedAt = p.UpdatedAt.UTC()
	return p, nil
}

// GetByID 按支付单号查询支付单，不存在时返回 ErrNotFound。
func (s *Store) GetByID(ctx context.Context, paymentID int64) (Payment, error) {
	row := s.db.QueryRowContext(ctx, "SELECT "+columns+" FROM payments WHERE payment_id = ?", paymentID)
	p, err := scanPayment(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("get payment %d: %w", paymentID, err)
	}
	return p, nil
}

// GetByOrderID 按订单号查询支付单，不存在时返回 ErrNotFound。
func (s *Store) GetByOrderID(ctx context.Context, orderID int64) (Payment, error) {
	row := s.db.QueryRowContext(ctx, "SELECT "+columns+" FROM payments WHERE order_id = ?", orderID)
	p, err := scanPayment(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("get payment by order %d: %w", orderID, err)
	}
	return p, nil
}

// Insert 插入一笔支付单。created_at 与 updated_at 取 p.CreatedAt，
// 不用数据库的默认值，这样“当前时间”只有一个来源（调用方的时钟），测试才能控制它。
// 同一订单已有支付单时返回 ErrDuplicate。
func (s *Store) Insert(ctx context.Context, p Payment) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO payments (payment_id, order_id, user_id, status, amount, currency, outcome, notified, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`,
		p.PaymentID, p.OrderID, p.UserID, int32(p.Status), p.Amount, p.Currency, int32(p.Outcome), p.CreatedAt, p.CreatedAt)
	if mysqlx.IsDuplicateKey(err) {
		return ErrDuplicate
	}
	if err != nil {
		return fmt.Errorf("insert payment for order %d: %w", p.OrderID, err)
	}
	return nil
}

// ListPendingCreatedBefore 返回创建时间不晚于 before 的 PENDING 支付单，最多 limit 条，先创建的在前。
func (s *Store) ListPendingCreatedBefore(ctx context.Context, before time.Time, limit int) ([]Payment, error) {
	return s.list(ctx,
		"SELECT "+columns+` FROM payments
		 WHERE status = ? AND created_at <= ?
		 ORDER BY created_at LIMIT ?`,
		int32(paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING), before, limit)
}

func (s *Store) list(ctx context.Context, query string, args ...any) ([]Payment, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list payments: %w", err)
	}
	defer rows.Close()

	var out []Payment
	for rows.Next() {
		p, err := scanPayment(rows)
		if err != nil {
			return nil, fmt.Errorf("scan payment: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list payments: %w", err)
	}
	return out, nil
}

// InTx 在一个事务里执行 fn，供需要把状态变更和 outbox 写在一起的调用方使用。
func (s *Store) InTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return mysqlx.InTx(ctx, s.db, fn)
}

// CompletePending 在事务 tx 里把 PENDING 的支付单改为 to（SUCCEEDED 或 FAILED）。
//
// 只接受 *sql.Tx：出结果必须和 PaymentEvent 写进同一个事务（见 processor），
// 从类型上避免有人在事务外改了状态却忘了写事件。
func (s *Store) CompletePending(ctx context.Context, tx *sql.Tx, paymentID int64, to paymentv1.PaymentStatus, now time.Time) (bool, error) {
	return exec(ctx, tx, "complete payment",
		"UPDATE payments SET status = ?, updated_at = ? WHERE payment_id = ? AND status = ?",
		int32(to), now, paymentID, int32(paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING))
}

// MarkNotified 在状态仍为 status 且尚未标记时，把支付单标记为订单服务已答复。
//
// 以 status 为前置条件：答复针对的是读到的那个状态。若期间状态变了（例如被退款），这次标记不生效。
func (s *Store) MarkNotified(ctx context.Context, paymentID int64, status paymentv1.PaymentStatus, now time.Time) (bool, error) {
	return exec(ctx, s.db, "mark payment notified",
		"UPDATE payments SET notified = 1, updated_at = ? WHERE payment_id = ? AND status = ? AND notified = 0",
		now, paymentID, int32(status))
}

// RefundAndMarkNotified 把 SUCCEEDED 且尚未标记的支付单改为 REFUNDED 并标记已答复。
// 两件事在同一条语句里完成，不存在“退了款却没标记”而被再次处理的中间状态。
//
// 前置条件带 notified = 0：订单服务对同一笔支付的第一次答复为准。
// 已经被答复“接受”的支付（订单已付清）不能再因为一条矛盾的答复被退款。
func (s *Store) RefundAndMarkNotified(ctx context.Context, paymentID int64, now time.Time) (bool, error) {
	return exec(ctx, s.db, "refund payment",
		"UPDATE payments SET status = ?, notified = 1, updated_at = ? WHERE payment_id = ? AND status = ? AND notified = 0",
		int32(paymentv1.PaymentStatus_PAYMENT_STATUS_REFUNDED), now, paymentID,
		int32(paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED))
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func exec(ctx context.Context, db execer, op, query string, args ...any) (bool, error) {
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("%s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%s: %w", op, err)
	}
	return n == 1, nil
}
