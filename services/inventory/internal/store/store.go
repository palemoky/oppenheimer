// Package store 是 inventory 服务的 SQL 访问层。
//
// 每个方法对应一条或一组语句，只负责把结果翻译成 Go 值，不做业务判断；
// 状态机、加锁顺序和错误码都由 service 层决定。
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
)

// Status 是 reservation.status 的取值。
type Status int8

// 取值与迁移文件中的注释一致，写入数据库后不能再改。
const (
	StatusReserved  Status = 1
	StatusConfirmed Status = 2
	StatusReleased  Status = 3
)

func (s Status) String() string {
	switch s {
	case StatusReserved:
		return "RESERVED"
	case StatusConfirmed:
		return "CONFIRMED"
	case StatusReleased:
		return "RELEASED"
	default:
		return fmt.Sprintf("Status(%d)", int8(s))
	}
}

// Price 是一个 SKU 的单价，金额单位为分。
type Price struct {
	Amount   int64
	Currency string
}

// Item 是预占中的一项。
type Item struct {
	SKUID     int64
	Quantity  int32
	UnitPrice Price
}

// Stock 是 sku_stock 的一行。
type Stock struct {
	SKUID     int64
	Available int64
	Reserved  int64
	Sold      int64
	UnitPrice Price
}

// Store 持有连接池，提供事务入口和事务外的只读查询。
type Store struct {
	db *sql.DB
}

// New 返回使用 db 的 Store。
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// InTx 在一个事务里执行 fn。
func (s *Store) InTx(ctx context.Context, fn func(*Tx) error) error {
	return mysqlx.InTx(ctx, s.db, func(tx *sql.Tx) error {
		return fn(&Tx{tx: tx})
	})
}

// GetStock 读取一个 SKU 的库存。不存在时 found 为 false。
func (s *Store) GetStock(ctx context.Context, skuID int64) (stock Stock, found bool, err error) {
	err = s.db.QueryRowContext(ctx,
		`SELECT sku_id, available, reserved, sold, unit_price, currency FROM sku_stock WHERE sku_id = ?`, skuID,
	).Scan(&stock.SKUID, &stock.Available, &stock.Reserved, &stock.Sold, &stock.UnitPrice.Amount, &stock.UnitPrice.Currency)
	if errors.Is(err, sql.ErrNoRows) {
		return Stock{}, false, nil
	}
	if err != nil {
		return Stock{}, false, fmt.Errorf("select sku_stock %d: %w", skuID, err)
	}
	return stock, true, nil
}

// Tx 是事务内的操作集合。
type Tx struct {
	tx *sql.Tx
}

// InsertReservation 插入订单的预占头记录。订单已有记录时 inserted 为 false。
//
// 唯一键冲突被翻译成返回值而不是错误，因为调用方要靠它分支，
// 而“先插入、靠唯一键互斥”正是这里的并发控制手段。
func (t *Tx) InsertReservation(ctx context.Context, orderID int64, status Status) (inserted bool, err error) {
	_, err = t.tx.ExecContext(ctx, `INSERT INTO reservation (order_id, status) VALUES (?, ?)`, orderID, status)
	if mysqlx.IsDuplicateKey(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("insert reservation %d: %w", orderID, err)
	}
	return true, nil
}

// InsertReservationOrLock 插入订单的预占头记录；记录已存在时不修改它，但对它加排他锁。
// 订单已有记录时 inserted 为 false。
//
// 普通 INSERT 撞上唯一键时只给已有记录加共享锁，调用方随后 SELECT ... FOR UPDATE
// 要升级为排他锁；两个并发事务都持有共享锁再都申请排他锁就会死锁（并发的 Release、
// 或 Release 与 Confirm 同时到达都是这种情况）。ON DUPLICATE KEY UPDATE 在冲突时
// 直接加排他锁，并发的请求在这里排队，而不是互相死锁。
func (t *Tx) InsertReservationOrLock(ctx context.Context, orderID int64, status Status) (inserted bool, err error) {
	res, err := t.tx.ExecContext(ctx,
		`INSERT INTO reservation (order_id, status) VALUES (?, ?) ON DUPLICATE KEY UPDATE status = status`,
		orderID, status)
	if err != nil {
		return false, fmt.Errorf("insert reservation %d: %w", orderID, err)
	}
	// 受影响行数：新插入为 1；已存在且未修改为 0（mysqlx 固定了按实际修改的行数计）。
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}
	return n == 1, nil
}

// ReservationStatusForShare 以共享锁读取预占状态。
//
// 插入撞上唯一键冲突时，InnoDB 已经在该记录上加了共享锁；这里继续用共享锁，
// 多个并发的重复请求可以同时持有，不会因为都要升级为排他锁而互相死锁。
func (t *Tx) ReservationStatusForShare(ctx context.Context, orderID int64) (Status, bool, error) {
	return t.reservationStatus(ctx, orderID, "FOR SHARE")
}

// ReservationStatusForUpdate 以排他锁读取预占状态，用于随后要修改状态的场景。
func (t *Tx) ReservationStatusForUpdate(ctx context.Context, orderID int64) (Status, bool, error) {
	return t.reservationStatus(ctx, orderID, "FOR UPDATE")
}

func (t *Tx) reservationStatus(ctx context.Context, orderID int64, lockClause string) (Status, bool, error) {
	var status Status
	// lockClause 只来自上面两个固定字面量，不含外部输入。
	err := t.tx.QueryRowContext(ctx,
		`SELECT status FROM reservation WHERE order_id = ? `+lockClause, orderID, //nolint:gosec // 见上
	).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("select reservation %d: %w", orderID, err)
	}
	return status, true, nil
}

// UpdateReservationStatus 把预占状态从 from 改为 to，返回是否由本次调用完成。
func (t *Tx) UpdateReservationStatus(ctx context.Context, orderID int64, from, to Status) (bool, error) {
	res, err := t.tx.ExecContext(ctx,
		`UPDATE reservation SET status = ? WHERE order_id = ? AND status = ?`, to, orderID, from)
	if err != nil {
		return false, fmt.Errorf("update reservation %d: %w", orderID, err)
	}
	return affectedOne(res)
}

// ReservationItems 按 sku_id 升序读取订单的预占明细。
//
// 用一致性读而不是加锁读：明细与头记录在同一个事务里写入、之后不再修改，
// 调用方已经锁住了头记录，而对明细加锁读会在主键上加间隙锁，徒增与其他订单插入的冲突。
// 调用方之前没有做过一致性读，快照在这里才建立，能看到已提交的明细。
func (t *Tx) ReservationItems(ctx context.Context, orderID int64) ([]Item, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT sku_id, quantity, unit_price, currency FROM reservation_item WHERE order_id = ? ORDER BY sku_id`, orderID)
	if err != nil {
		return nil, fmt.Errorf("select reservation_item %d: %w", orderID, err)
	}
	defer rows.Close()

	var items []Item
	for rows.Next() {
		var it Item
		if err := rows.Scan(&it.SKUID, &it.Quantity, &it.UnitPrice.Amount, &it.UnitPrice.Currency); err != nil {
			return nil, fmt.Errorf("scan reservation_item %d: %w", orderID, err)
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reservation_item %d: %w", orderID, err)
	}
	return items, nil
}

// InsertReservationItems 写入预占明细。
func (t *Tx) InsertReservationItems(ctx context.Context, orderID int64, items []Item) error {
	if len(items) == 0 {
		return nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?, ?),", len(items)), ",")
	args := make([]any, 0, len(items)*5)
	for _, it := range items {
		args = append(args, orderID, it.SKUID, it.Quantity, it.UnitPrice.Amount, it.UnitPrice.Currency)
	}
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO reservation_item (order_id, sku_id, quantity, unit_price, currency) VALUES `+placeholders, //nolint:gosec // 只拼接占位符
		args...)
	if err != nil {
		return fmt.Errorf("insert reservation_item %d: %w", orderID, err)
	}
	return nil
}

// ReserveStock 从可售转入预占。可售不足或 SKU 不存在时返回 false。
//
// 判断与扣减在同一条语句里，由行锁保证原子，这是防超卖的核心。
func (t *Tx) ReserveStock(ctx context.Context, skuID int64, qty int32) (bool, error) {
	res, err := t.tx.ExecContext(ctx,
		`UPDATE sku_stock SET available = available - ?, reserved = reserved + ? WHERE sku_id = ? AND available >= ?`,
		qty, qty, skuID, qty)
	if err != nil {
		return false, fmt.Errorf("reserve sku_stock %d: %w", skuID, err)
	}
	return affectedOne(res)
}

// ConfirmStock 从预占转入已售。预占量不足或 SKU 不存在时返回 false。
func (t *Tx) ConfirmStock(ctx context.Context, skuID int64, qty int32) (bool, error) {
	res, err := t.tx.ExecContext(ctx,
		`UPDATE sku_stock SET reserved = reserved - ?, sold = sold + ? WHERE sku_id = ? AND reserved >= ?`,
		qty, qty, skuID, qty)
	if err != nil {
		return false, fmt.Errorf("confirm sku_stock %d: %w", skuID, err)
	}
	return affectedOne(res)
}

// ReleaseStock 从预占退回可售。预占量不足或 SKU 不存在时返回 false。
func (t *Tx) ReleaseStock(ctx context.Context, skuID int64, qty int32) (bool, error) {
	res, err := t.tx.ExecContext(ctx,
		`UPDATE sku_stock SET reserved = reserved - ?, available = available + ? WHERE sku_id = ? AND reserved >= ?`,
		qty, qty, skuID, qty)
	if err != nil {
		return false, fmt.Errorf("release sku_stock %d: %w", skuID, err)
	}
	return affectedOne(res)
}

// SKUExists 判断 SKU 是否存在。
func (t *Tx) SKUExists(ctx context.Context, skuID int64) (bool, error) {
	var one int
	err := t.tx.QueryRowContext(ctx, `SELECT 1 FROM sku_stock WHERE sku_id = ?`, skuID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("select sku_stock %d: %w", skuID, err)
	}
	return true, nil
}

// SKUPrices 读取一组 SKU 的单价。不存在的 SKU 不出现在结果里。
func (t *Tx) SKUPrices(ctx context.Context, skuIDs []int64) (map[int64]Price, error) {
	if len(skuIDs) == 0 {
		return map[int64]Price{}, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(skuIDs)), ",")
	args := make([]any, len(skuIDs))
	for i, id := range skuIDs {
		args[i] = id
	}
	rows, err := t.tx.QueryContext(ctx,
		`SELECT sku_id, unit_price, currency FROM sku_stock WHERE sku_id IN (`+placeholders+`)`, //nolint:gosec // 只拼接占位符
		args...)
	if err != nil {
		return nil, fmt.Errorf("select sku prices: %w", err)
	}
	defer rows.Close()

	prices := make(map[int64]Price, len(skuIDs))
	for rows.Next() {
		var (
			id int64
			p  Price
		)
		if err := rows.Scan(&id, &p.Amount, &p.Currency); err != nil {
			return nil, fmt.Errorf("scan sku price: %w", err)
		}
		prices[id] = p
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sku prices: %w", err)
	}
	return prices, nil
}

// affectedOne 报告语句是否恰好修改了一行。
// mysqlx.Open 固定了“受影响行数 = 实际修改的行数”，条件更新据此判断是否生效。
func affectedOne(res sql.Result) (bool, error) {
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}
	return n == 1, nil
}
