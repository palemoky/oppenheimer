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
	PaymentID      int64
	OrderID        int64
	UserID         int64
	Status         paymentv1.PaymentStatus
	Amount         int64
	Currency       string
	Outcome        paymentv1.SimulatedOutcome
	Notified       bool
	NotifyAttempts int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Store 访问 payments 表。
type Store struct {
	db *sql.DB
}

// New 返回使用 db 的 Store。
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

const columns = "payment_id, order_id, user_id, status, amount, currency, outcome, notified, notify_attempts, created_at, updated_at"

type scanner interface {
	Scan(dest ...any) error
}

func scanPayment(row scanner) (Payment, error) {
	var (
		p               Payment
		status, outcome int32
	)
	err := row.Scan(&p.PaymentID, &p.OrderID, &p.UserID, &status, &p.Amount, &p.Currency,
		&outcome, &p.Notified, &p.NotifyAttempts, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return Payment{}, err
	}
	p.Status = paymentv1.PaymentStatus(status)
	p.Outcome = paymentv1.SimulatedOutcome(outcome)
	p.CreatedAt = p.CreatedAt.UTC()
	p.UpdatedAt = p.UpdatedAt.UTC()
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
		`INSERT INTO payments (payment_id, order_id, user_id, status, amount, currency, outcome, notified, notify_attempts, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)`,
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
		 WHERE status = ? AND notified = 0 AND created_at <= ?
		 ORDER BY created_at LIMIT ?`,
		int32(paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING), before, limit)
}

// ListUnnotifiedDue 返回已出结果（SUCCEEDED 或 FAILED）、尚未回调成功、且已到重试时间的支付单，最多 limit 条。
//
// 从未尝试过的（notify_attempts = 0）立即到期；尝试过 n 次的，在 updated_at 之后
// min(base × 2^n, maxDelay) 到期。退避在 SQL 里算，是为了让 LIMIT 只截取真正到期的行：
// 若在 Go 里过滤，一批里可能全是未到期的行，把到期的挤出去。
// 移位次数封顶，避免 notify_attempts 很大时溢出；封顶后的值远大于 maxDelay，不影响结果。
func (s *Store) ListUnnotifiedDue(ctx context.Context, now time.Time, base, maxDelay time.Duration, limit int) ([]Payment, error) {
	return s.list(ctx,
		"SELECT "+columns+` FROM payments
		 WHERE status IN (?, ?) AND notified = 0
		   AND (notify_attempts = 0
		        OR TIMESTAMPADD(MICROSECOND, LEAST(? << LEAST(notify_attempts, 32), ?), updated_at) <= ?)
		 ORDER BY updated_at LIMIT ?`,
		int32(paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED), int32(paymentv1.PaymentStatus_PAYMENT_STATUS_FAILED),
		base.Microseconds(), maxDelay.Microseconds(), now, limit)
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

// CompletePending 把 PENDING 的支付单改为 to（SUCCEEDED 或 FAILED）。
func (s *Store) CompletePending(ctx context.Context, paymentID int64, to paymentv1.PaymentStatus, now time.Time) (bool, error) {
	return s.exec(ctx, "complete payment",
		"UPDATE payments SET status = ?, updated_at = ? WHERE payment_id = ? AND status = ?",
		int32(to), now, paymentID, int32(paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING))
}

// MarkNotified 在状态仍为 status 且尚未标记时，把支付单标记为订单服务已确认收到。
func (s *Store) MarkNotified(ctx context.Context, paymentID int64, status paymentv1.PaymentStatus, now time.Time) (bool, error) {
	return s.exec(ctx, "mark payment notified",
		"UPDATE payments SET notified = 1, updated_at = ? WHERE payment_id = ? AND status = ? AND notified = 0",
		now, paymentID, int32(status))
}

// RefundAndMarkNotified 把 SUCCEEDED 且尚未标记的支付单改为 REFUNDED 并标记已回调。
// 两件事在同一条语句里完成，不存在“退了款却没标记”而被再次处理的中间状态。
func (s *Store) RefundAndMarkNotified(ctx context.Context, paymentID int64, now time.Time) (bool, error) {
	return s.exec(ctx, "refund payment",
		"UPDATE payments SET status = ?, notified = 1, updated_at = ? WHERE payment_id = ? AND status = ? AND notified = 0",
		int32(paymentv1.PaymentStatus_PAYMENT_STATUS_REFUNDED), now, paymentID,
		int32(paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED))
}

// IncrNotifyAttempts 记录一次失败的回调。以读到的 attempts 为前置条件，
// 两个处理者同时回调失败时只记一次，退避不会被重复放大。
func (s *Store) IncrNotifyAttempts(ctx context.Context, paymentID int64, status paymentv1.PaymentStatus, attempts int, now time.Time) (bool, error) {
	return s.exec(ctx, "increment notify attempts",
		`UPDATE payments SET notify_attempts = notify_attempts + 1, updated_at = ?
		 WHERE payment_id = ? AND status = ? AND notified = 0 AND notify_attempts = ?`,
		now, paymentID, int32(status), attempts)
}

func (s *Store) exec(ctx context.Context, op, query string, args ...any) (bool, error) {
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("%s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%s: %w", op, err)
	}
	return n == 1, nil
}
