package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
)

// 冷库（归档库）的表名，见迁移 migrations_archive。
const (
	ColdOrders   = "orders_archive"
	ColdItems    = "order_items_archive"
	ColdPayments = "payment_results_archive"
)

// coldTables 让冷库复用读热表的代码：三张表的列名与热表相同。
var coldTables = tables{orders: ColdOrders, items: ColdItems, payments: ColdPayments}

// Cold 访问冷库：已归档订单的只读查询。并发安全。
//
// 冷库里的订单都是终态，写进去之后不再改（设计文档 1c 第 6.3 节的不变量 2）。在线服务只读它；
// 唯一的写入方是归档任务。冷库不按用户分片，一个库里一套表。
//
// 读的顺序由调用方保证：必须先查热库、没有再查冷库。数据只从热流向冷，而且写冷在删热之前，
// 所以“热库没有”之后去冷库查不会漏；反过来先冷后热，会在“还没写冷”时查冷、“已经删热”后查热，两头落空。
type Cold struct {
	db *sql.DB
}

// NewCold 返回 Cold。db 连到冷库，表结构由 order.ArchiveMigrations 建好。
func NewCold(db *sql.DB) *Cold { return &Cold{db: db} }

// DB 返回冷库的连接池，给归档任务写入用。
func (c *Cold) DB() *sql.DB { return c.db }

// ErrArchiverRunning 表示已经有一个归档任务持有冷库上的互斥锁。
var ErrArchiverRunning = errors.New("store: another archiver is running")

// LockArchiver 在冷库上取归档任务的互斥锁，拿不到时立即返回 ErrArchiverRunning，不等待。
// 用完调用 release。
//
// 用的是 MySQL 的 GET_LOCK：锁属于会话，所以这里占住一个连接直到 release；进程崩溃时连接断开，
// 锁自动释放，不会留下要人工清理的状态。锁名带库名，同一个实例上的多套环境（e2e_、bench_）互不影响。
// 这把锁只是为了避免两个任务互相造成误报，归档的正确性不依赖它：连接中途断了、锁丢了，
// 最坏也只是多出一个任务同时在跑。
func (c *Cold) LockArchiver(ctx context.Context) (release func(), err error) {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("archiver lock: %w", err)
	}
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(CONCAT('order-archiver:', DATABASE()), 0)").Scan(&got); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("archiver lock: %w", err)
	}
	if got.Int64 != 1 {
		_ = conn.Close()
		return nil, ErrArchiverRunning
	}
	return func() {
		// 进程即将退出，ctx 多半已经取消；用独立的短时限释放。失败也无妨，连接关闭时锁随之释放。
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(rctx, "DO RELEASE_LOCK(CONCAT('order-archiver:', DATABASE()))")
		_ = conn.Close()
	}, nil
}

// GetOrder 按 order_id 读已归档的订单及明细。没有时返回 ErrNotFound。
func (c *Cold) GetOrder(ctx context.Context, orderID int64) (*Order, error) {
	return getOrder(ctx, c.db, coldTables, "WHERE order_id = ?", orderID)
}

// GetPaymentResult 读已归档订单 orderID 上 paymentID 的处理记录。没有时返回 ErrNotFound。
// 带上 order_id 是因为冷库的主键是 (order_id, payment_id)，这样能裁剪到一个分区。
func (c *Cold) GetPaymentResult(ctx context.Context, orderID, paymentID int64) (*PaymentResult, error) {
	var (
		r           PaymentResult
		disposition int32
	)
	err := c.db.QueryRowContext(ctx,
		"SELECT payment_id, order_id, succeeded, amount, disposition FROM "+ColdPayments+" WHERE order_id = ? AND payment_id = ?",
		orderID, paymentID).Scan(&r.PaymentID, &r.OrderID, &r.Succeeded, &r.Amount, &disposition)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get archived payment result: %w", err)
	}
	r.Disposition = eventv1.PaymentDisposition(disposition)
	return &r, nil
}

// ListUserOrders 与 Store.ListUserOrders 的语义相同，查的是冷库。
// 带游标（BeforeOrderID）时，游标之后的年份分区会被裁剪掉。
func (c *Cold) ListUserOrders(ctx context.Context, q UserOrdersQuery) (orders []OrderSummary, more bool, err error) {
	if err := q.validate(); err != nil {
		return nil, false, err
	}
	return listUserOrders(ctx, c.db, ColdOrders, q)
}
