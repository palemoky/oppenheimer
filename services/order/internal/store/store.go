// Package store 封装 order 服务对订单库的全部 SQL 访问。
//
// 订单数据按 user_id 分成若干逻辑分片（slot），分布在多个库上（ADR-0012）。每个方法按订单号
// （sharding.Router.ForOrder）或用户号（ForUser）定位到一个 slot，只读写这个 slot 的表，
// 不做跨分片查询；后台扫描显式带 slot 参数，由调用方逐个 slot 扫。拼进 SQL 的表名只由
// sharding.Table 生成，不拼接任何外部输入。
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
	"github.com/palemoky/Oppenheimer/pkg/sharding"
)

var (
	// ErrNotFound 表示要找的记录不存在。
	ErrNotFound = errors.New("store: not found")
	// ErrDuplicate 表示插入违反了唯一约束。
	ErrDuplicate = errors.New("store: duplicate key")
	// ErrWrongShard 表示请求违反了分片约束：订单号的分片基因与 user_id 不在同一个 slot，
	// 或者在一个 slot 的事务里操作另一个 slot 的订单。正常路径不会出现，出现说明调用方有缺陷。
	ErrWrongShard = errors.New("store: wrong shard")
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

// Store 访问分片后的订单库。并发安全。
type Store struct {
	dbs []*sql.DB
	// replicas[i] 是 dbs[i] 的从库，为空表示没有配置从库。
	replicas []*sql.DB
	router   *sharding.Router
	tables   []tables // 下标是 slot
}

// Option 是 New 的可选项。
type Option func(*Store)

// WithReplicas 给每个库配一个异步复制的从库，replicas[i] 是 dbs[i] 的从库。
// 个数必须等于 router.DBs()，元素不能为 nil（在 New 里检查）。
//
// 从库可能落后，读它会读到旧数据。所以只有显式带“从库”字样的方法（GetOrderFromReplica、
// 带 Replica 参数的扫描）会读它，其余方法一律读写主库，行为与没有从库时相同。
func WithReplicas(replicas []*sql.DB) Option {
	return func(s *Store) { s.replicas = replicas }
}

// Source 指定一次只读查询读主库还是从库。
type Source int

const (
	// Primary 读主库。
	Primary Source = iota
	// Replica 读从库；没有配置从库时读主库。
	Replica
)

// tables 是一个 slot 上的物理表名，全部由 sharding.Table 生成。
type tables struct {
	orders, items, payments string
}

// New 返回使用 dbs 的 Store，dbs[i] 是第 i 个库。库的个数必须等于 router.DBs()。
func New(dbs []*sql.DB, router *sharding.Router, opts ...Option) (*Store, error) {
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
	ts := make([]tables, router.Slots())
	for slot := range ts {
		ts[slot] = tables{
			orders:   sharding.Table("orders", slot),
			items:    sharding.Table("order_items", slot),
			payments: sharding.Table("payment_results", slot),
		}
	}
	st := &Store{dbs: dbs, router: router, tables: ts}
	for _, o := range opts {
		o(st)
	}
	if st.replicas != nil {
		if len(st.replicas) != router.DBs() {
			return nil, fmt.Errorf("store: got %d replicas, sharding layout has %d databases", len(st.replicas), router.DBs())
		}
		for i, db := range st.replicas {
			if db == nil {
				return nil, fmt.Errorf("store: replica %d is nil", i)
			}
		}
	}
	return st, nil
}

// HasReplicas 报告是否配置了从库。
func (s *Store) HasReplicas() bool { return s.replicas != nil }

// Router 返回 Store 使用的分片布局。
func (s *Store) Router() *sharding.Router { return s.router }

// DBOfOrder 返回订单所在的库号。
func (s *Store) DBOfOrder(orderID int64) int { return s.router.DB(s.router.ForOrder(orderID)) }

func (s *Store) dbOf(slot int) *sql.DB { return s.dbs[s.router.DB(slot)] }

// replicaOf 返回 slot 所在库的从库，没有配置从库时返回主库。
func (s *Store) replicaOf(slot int) *sql.DB {
	if s.replicas == nil {
		return s.dbOf(slot)
	}
	return s.replicas[s.router.DB(slot)]
}

func (s *Store) sourceOf(slot int, src Source) *sql.DB {
	if src == Replica {
		return s.replicaOf(slot)
	}
	return s.dbOf(slot)
}

func (s *Store) checkSlot(slot int) error {
	if slot < 0 || slot >= s.router.Slots() {
		return fmt.Errorf("%w: slot %d out of range [0, %d)", ErrWrongShard, slot, s.router.Slots())
	}
	return nil
}

const orderColumns = `order_id, user_id, status, total_amount, currency, idempotency_key, request_hash,
	payment_id, fail_reason, expire_at, paid_at, created_at, updated_at`

// queryer 让读订单的代码在事务内外共用。
type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// InsertOrder 在一个事务里插入订单和明细，写进订单号所在的 slot。
// (user_id, idempotency_key) 或 order_id 冲突时返回 ErrDuplicate。
//
// 订单号的分片基因与 user_id 不在同一个 slot 时返回 ErrWrongShard、什么都不写：这样的订单写进去之后，
// 按用户（幂等键）和按订单号会找到两个不同的分片，(user_id, idempotency_key) 的唯一约束也就管不住了。
func (s *Store) InsertOrder(ctx context.Context, o *Order) error {
	slot := s.router.ForOrder(o.OrderID)
	if us := s.router.ForUser(o.UserID); us != slot {
		return fmt.Errorf("%w: order %d is in slot %d but user %d is in slot %d",
			ErrWrongShard, o.OrderID, slot, o.UserID, us)
	}
	tb := s.tables[slot]
	err := mysqlx.InTx(ctx, s.dbOf(slot), func(tx *sql.Tx) error {
		// created_at、updated_at 显式写入，使所有时间都来自服务注入的时钟，
		// 后台任务按时限扫描时比较的是同一个时间源。
		//nolint:gosec // 表名只来自 sharding.Table，不含外部输入
		_, err := tx.ExecContext(ctx, `INSERT INTO `+tb.orders+`
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
		b.WriteString("INSERT INTO " + tb.items + " (order_id, sku_id, quantity, unit_price, created_at) VALUES ")
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

// GetOrder 按 order_id 读订单及明细。只查订单号所在的 slot：那里没有就是不存在。
func (s *Store) GetOrder(ctx context.Context, orderID int64) (*Order, error) {
	slot := s.router.ForOrder(orderID)
	return getOrder(ctx, s.dbOf(slot), s.tables[slot], "WHERE order_id = ?", orderID)
}

// GetOrderFromReplica 与 GetOrder 相同，但读订单所在库的从库；没有配置从库时读主库。
//
// 结果可能是旧的，从库还没复制到的订单返回 ErrNotFound。只能用于展示，
// 不能作为任何写入或决定的前提；“订单不存在”必须由主库确认。
func (s *Store) GetOrderFromReplica(ctx context.Context, orderID int64) (*Order, error) {
	slot := s.router.ForOrder(orderID)
	return getOrder(ctx, s.replicaOf(slot), s.tables[slot], "WHERE order_id = ?", orderID)
}

// GetOrderByIdempotencyKey 按 (user_id, idempotency_key) 读订单及明细，只查用户所在的 slot。
func (s *Store) GetOrderByIdempotencyKey(ctx context.Context, userID int64, key string) (*Order, error) {
	slot := s.router.ForUser(userID)
	return getOrder(ctx, s.dbOf(slot), s.tables[slot], "WHERE user_id = ? AND idempotency_key = ?", userID, key)
}

func getOrder(ctx context.Context, q queryer, tb tables, where string, args ...any) (*Order, error) {
	o, err := scanOrder(q.QueryRowContext(ctx, "SELECT "+orderColumns+" FROM "+tb.orders+" "+where, args...))
	if err != nil {
		return nil, err
	}
	if o.Items, err = listItems(ctx, q, tb, o.OrderID); err != nil {
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

func listItems(ctx context.Context, q queryer, tb tables, orderID int64) ([]Item, error) {
	rows, err := q.QueryContext(ctx,
		"SELECT sku_id, quantity, unit_price FROM "+tb.items+" WHERE order_id = ? ORDER BY sku_id", orderID)
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
	slot := s.router.ForOrder(orderID)
	tb := s.tables[slot]
	var done bool
	err := mysqlx.InTx(ctx, s.dbOf(slot), func(tx *sql.Tx) error {
		//nolint:gosec // 表名只来自 sharding.Table，不含外部输入
		res, err := tx.ExecContext(ctx, `UPDATE `+tb.orders+` SET status = ?, total_amount = ?, currency = ?, updated_at = ?
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
			if _, err := tx.ExecContext(ctx, "UPDATE "+tb.items+" SET unit_price = ? WHERE order_id = ? AND sku_id = ?", //nolint:gosec // 表名只来自 sharding.Table
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

// ListExpired 返回 slot 上截止时间早于 now 的 PENDING_PAYMENT 订单。
//
// 后台扫描是明确豁免的全分片扫描：调用方逐个 slot 调用，不合并排序。
// src 为 Replica 时读从库，结果可能是旧的，只能当作候选。
func (s *Store) ListExpired(ctx context.Context, slot int, now time.Time, limit int, src Source) ([]OrderRef, error) {
	if err := s.checkSlot(slot); err != nil {
		return nil, err
	}
	return s.listRefs(ctx, slot, src,
		"SELECT order_id, user_id FROM "+s.tables[slot].orders+" WHERE status = ? AND expire_at < ? ORDER BY expire_at LIMIT ?",
		int32(orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT), now, limit)
}

// ListStaleCreating 返回 slot 上落库时间（updated_at，CREATING 的行落库后不再更新）早于 before 仍在 CREATING 的订单。
// src 的含义同 ListExpired。
func (s *Store) ListStaleCreating(ctx context.Context, slot int, before time.Time, limit int, src Source) ([]OrderRef, error) {
	if err := s.checkSlot(slot); err != nil {
		return nil, err
	}
	return s.listRefs(ctx, slot, src,
		"SELECT order_id, user_id FROM "+s.tables[slot].orders+" WHERE status = ? AND updated_at < ? ORDER BY updated_at LIMIT ?",
		int32(orderv1.OrderStatus_ORDER_STATUS_CREATING), before, limit)
}

func (s *Store) listRefs(ctx context.Context, slot int, src Source, query string, args ...any) ([]OrderRef, error) {
	rows, err := s.sourceOf(slot, src).QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list orders in slot %d: %w", slot, err)
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

// InTx 在 orderID 所在 slot 的库上开一个事务执行 fn，事务内的操作通过 Tx 完成。
//
// 事务绑定在这一个 slot 上：Tx 的方法只操作这个 slot 的表，传入别的 slot 的订单号时返回
// ErrWrongShard，而不是悄悄去查别的表。这是“不允许跨分片事务”在代码里的体现。
func (s *Store) InTx(ctx context.Context, orderID int64, fn func(tx *Tx) error) error {
	slot := s.router.ForOrder(orderID)
	return mysqlx.InTx(ctx, s.dbOf(slot), func(tx *sql.Tx) error {
		return fn(&Tx{tx: tx, slot: slot, tables: s.tables[slot], router: s.router})
	})
}

// Tx 是绑定在一个 slot 上的事务内可用的操作。
type Tx struct {
	tx     *sql.Tx
	slot   int
	tables tables
	router *sharding.Router
}

// Slot 返回事务所在的 slot。
func (t *Tx) Slot() int { return t.slot }

// own 检查 orderID 属于本事务的 slot。
func (t *Tx) own(orderID int64) error {
	if s := t.router.ForOrder(orderID); s != t.slot {
		return fmt.Errorf("%w: order %d is in slot %d, transaction is on slot %d", ErrWrongShard, orderID, s, t.slot)
	}
	return nil
}

// LockOrder 读订单并加行锁，使同一订单上的其他加锁读和条件更新排队等待本事务结束。
func (t *Tx) LockOrder(ctx context.Context, orderID int64) (*Order, error) {
	if err := t.own(orderID); err != nil {
		return nil, err
	}
	return scanOrder(t.tx.QueryRowContext(ctx,
		"SELECT "+orderColumns+" FROM "+t.tables.orders+" WHERE order_id = ? FOR UPDATE", orderID))
}

// GetPaymentResult 读本 slot 上已处理过的支付结果。
//
// payment_results 跟着订单分片，只能查到属于本 slot 的订单的记录。同一笔支付总是带着同一个订单号到来、
// 被路由到同一个 slot，所以去重不受影响；但“同一 payment_id 被记在另一个订单上”这种矛盾，
// 只在两个订单恰好同一个 slot 时才查得出来。
func (t *Tx) GetPaymentResult(ctx context.Context, paymentID int64) (*PaymentResult, error) {
	var (
		r           PaymentResult
		disposition int32
	)
	err := t.tx.QueryRowContext(ctx,
		"SELECT payment_id, order_id, succeeded, amount, disposition FROM "+t.tables.payments+" WHERE payment_id = ?",
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
// r.OrderID 必须属于本事务的 slot。
func (t *Tx) InsertPaymentResult(ctx context.Context, r *PaymentResult, now time.Time) error {
	if err := t.own(r.OrderID); err != nil {
		return err
	}
	//nolint:gosec // 表名只来自 sharding.Table，不含外部输入
	_, err := t.tx.ExecContext(ctx, `INSERT INTO `+t.tables.payments+`
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
	if err := t.own(orderID); err != nil {
		return false, err
	}
	//nolint:gosec // 表名只来自 sharding.Table，不含外部输入
	res, err := t.tx.ExecContext(ctx, `UPDATE `+t.tables.orders+` SET status = ?, payment_id = ?, paid_at = ?, updated_at = ?
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
	if err := t.own(orderID); err != nil {
		return false, err
	}
	return t.exec(ctx, `UPDATE `+t.tables.orders+` SET status = ?, fail_reason = ?, updated_at = ?
		WHERE order_id = ? AND status = ?`,
		int32(orderv1.OrderStatus_ORDER_STATUS_FAILED), reason, now,
		orderID, int32(orderv1.OrderStatus_ORDER_STATUS_CREATING))
}

// Cancel 把属于 userID 的 PENDING_PAYMENT 订单改为 CANCELLED。
func (t *Tx) Cancel(ctx context.Context, orderID, userID int64, now time.Time) (bool, error) {
	if err := t.own(orderID); err != nil {
		return false, err
	}
	return t.exec(ctx, `UPDATE `+t.tables.orders+` SET status = ?, updated_at = ?
		WHERE order_id = ? AND user_id = ? AND status = ?`,
		int32(orderv1.OrderStatus_ORDER_STATUS_CANCELLED), now,
		orderID, userID, int32(orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT))
}

// CloseExpired 把截止时间早于 now 的 PENDING_PAYMENT 订单改为 CLOSED。
func (t *Tx) CloseExpired(ctx context.Context, orderID int64, now time.Time) (bool, error) {
	if err := t.own(orderID); err != nil {
		return false, err
	}
	return t.exec(ctx, `UPDATE `+t.tables.orders+` SET status = ?, updated_at = ?
		WHERE order_id = ? AND status = ? AND expire_at < ?`,
		int32(orderv1.OrderStatus_ORDER_STATUS_CLOSED), now,
		orderID, int32(orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT), now)
}

// AddOutbox 在本事务里写入待发布的消息，与本事务的状态变更一起提交或回滚。
// outbox 每个库一张，消息写进事务所在库的 outbox，由这个库的中继发出。
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
