package store

// 本文件是归档任务（设计文档 1c 第 6.3 节）用到的读写：一个订单在三张表里的全部内容（bundle）、
// 它的摘要，热库上的列出、读取、校验后删除，冷库上的写入和读回。

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"maps"
	"slices"
	"strings"
	"time"

	eventv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/event/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
)

// MaxArchiveBatch 是归档一批订单的上限。一批订单号会进 IN 列表，订单行会进多行 INSERT，
// 占位符的个数随批大小增长，而 MySQL 一条预处理语句最多 65535 个占位符：orders 一行 15 列，
// 2000 个订单是 30000 个，留足了余量。
const MaxArchiveBatch = 2000

// insertChunkRows 是多行 INSERT 一条语句最多写的行数。明细和支付结果的行数不受批大小约束
// （一个订单可以有很多行），所以按行数再切一次。15 列 × 1000 行 = 15000 个占位符。
const insertChunkRows = 1000

// deleteRetries 是校验后删除遇到死锁或等锁超时时最多做几次。
const deleteRetries = 3

// ErrNotTerminal 表示要写进冷库的订单不是终态。冷库里只能有终态订单（设计文档 6.3 的不变量 3）。
var ErrNotTerminal = errors.New("store: order is not in a terminal status")

// terminalStatuses 是终态。终态订单的 orders 行不会再变：更新 orders 的语句都以非终态为前置条件。
var terminalStatuses = []orderv1.OrderStatus{
	orderv1.OrderStatus_ORDER_STATUS_PAID,
	orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
	orderv1.OrderStatus_ORDER_STATUS_CLOSED,
	orderv1.OrderStatus_ORDER_STATUS_FAILED,
}

// IsTerminal 报告 st 是否为终态。
func IsTerminal(st orderv1.OrderStatus) bool { return slices.Contains(terminalStatuses, st) }

// terminalIn 是 "status IN (?, ?, ?, ?)" 和它的参数，在列出和计数的语句里共用。
func terminalIn() (string, []any) {
	args := make([]any, len(terminalStatuses))
	for i, st := range terminalStatuses {
		args[i] = int32(st)
	}
	return "status IN (" + placeholders(len(args)) + ")", args
}

// OrderRow 是 orders 表的一行，全部 15 列。和 Order 不同，可空的列保留 NULL 和零值的区别：
// 归档要原样搬运，摘要也要能区分两者。
type OrderRow struct {
	OrderID        int64
	UserID         int64
	MerchantID     sql.Null[int64]
	Version        int64
	Status         orderv1.OrderStatus
	TotalAmount    int64
	Currency       string
	IdempotencyKey string
	RequestHash    []byte
	PaymentID      sql.Null[int64]
	FailReason     sql.Null[string]
	ExpireAt       time.Time
	PaidAt         sql.Null[time.Time]
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ItemRow 是 order_items 表的一行，全部 5 列。
type ItemRow struct {
	OrderID   int64
	SKUID     int64
	Quantity  int32
	UnitPrice int64
	CreatedAt time.Time
}

// PaymentRow 是 payment_results 表的一行，全部 6 列。
type PaymentRow struct {
	PaymentID   int64
	OrderID     int64
	Succeeded   bool
	Amount      int64
	Disposition eventv1.PaymentDisposition
	CreatedAt   time.Time
}

// Bundle 是一个订单在三张表里的全部内容。读出来的 Items 按 sku_id、Payments 按 payment_id 升序。
type Bundle struct {
	Order    OrderRow
	Items    []ItemRow
	Payments []PaymentRow
}

// Digest 是 Bundle 的 SHA-256 摘要。
type Digest [sha256.Size]byte

// String 返回十六进制的摘要，用于日志。
func (d Digest) String() string { return hex.EncodeToString(d[:]) }

// digestDomain 写在摘要输入的最前面。编码方式改变时改这个值，新旧摘要就不会碰巧相等。
const digestDomain = "oppenheimer/order-bundle/v1"

// Digest 返回 b 的摘要，覆盖三张表的全部列（冷库的 archived_at 不是订单的内容，不算）。
//
// 同一个订单从热库读、从冷库读，摘要必须相同；任何一列不同，摘要就不同。为此规范化是严格的：
//   - 整数一律编码成 8 字节大端，布尔是 1 字节；
//   - 字符串和字节串前面带 8 字节长度，所以相邻字段之间没有歧义（"CN"+"Yk" 和 "CNY"+"k" 不同），
//     末尾的 0x00 也算数；
//   - 时间换成 UTC 的 Unix 微秒（列是 DATETIME(6)），与 time.Time 的时区和单调时钟读数无关；
//   - 可空的列先写 1 字节的“是否非 NULL”，非 NULL 才写值，所以 NULL 和 0、""、零时间都不同；
//   - 明细和支付结果先写行数，再按主键排好序逐行写，与读出的顺序无关。
func (b *Bundle) Digest() Digest {
	e := encoder{h: sha256.New()}
	e.str(digestDomain)

	o := &b.Order
	e.int(o.OrderID)
	e.int(o.UserID)
	e.nullInt(o.MerchantID)
	e.int(o.Version)
	e.int(int64(o.Status))
	e.int(o.TotalAmount)
	e.str(o.Currency)
	e.str(o.IdempotencyKey)
	e.bytes(o.RequestHash)
	e.nullInt(o.PaymentID)
	e.present(o.FailReason.Valid)
	if o.FailReason.Valid {
		e.str(o.FailReason.V)
	}
	e.time(o.ExpireAt)
	e.present(o.PaidAt.Valid)
	if o.PaidAt.Valid {
		e.time(o.PaidAt.V)
	}
	e.time(o.CreatedAt)
	e.time(o.UpdatedAt)

	items := slices.SortedFunc(slices.Values(b.Items), func(x, y ItemRow) int { return cmp.Compare(x.SKUID, y.SKUID) })
	e.int(int64(len(items)))
	for _, it := range items {
		e.int(it.OrderID)
		e.int(it.SKUID)
		e.int(int64(it.Quantity))
		e.int(it.UnitPrice)
		e.time(it.CreatedAt)
	}

	pays := slices.SortedFunc(slices.Values(b.Payments), func(x, y PaymentRow) int { return cmp.Compare(x.PaymentID, y.PaymentID) })
	e.int(int64(len(pays)))
	for _, p := range pays {
		e.int(p.PaymentID)
		e.int(p.OrderID)
		e.bool(p.Succeeded)
		e.int(p.Amount)
		e.int(int64(p.Disposition))
		e.time(p.CreatedAt)
	}

	var d Digest
	e.h.Sum(d[:0])
	return d
}

// encoder 把字段按无歧义的方式写进哈希。hash.Hash 的 Write 不会返回错误。
type encoder struct {
	h   hash.Hash
	buf [8]byte
}

func (e *encoder) int(v int64) {
	binary.BigEndian.PutUint64(e.buf[:], uint64(v)) //nolint:gosec // 按位重新解释，负数也是一一对应的
	e.h.Write(e.buf[:])
}

func (e *encoder) bool(v bool) {
	var b byte
	if v {
		b = 1
	}
	e.h.Write([]byte{b})
}

func (e *encoder) present(valid bool) { e.bool(valid) }

func (e *encoder) bytes(b []byte) {
	e.int(int64(len(b)))
	e.h.Write(b)
}

func (e *encoder) str(s string) {
	e.int(int64(len(s)))
	e.h.Write([]byte(s))
}

func (e *encoder) time(t time.Time) { e.int(t.UTC().UnixMicro()) }

func (e *encoder) nullInt(v sql.Null[int64]) {
	e.present(v.Valid)
	if v.Valid {
		e.int(v.V)
	}
}

// 三张表的列。热表和冷表的列名相同，冷表多出的 archived_at 不读也不写（由默认值填上）。
var (
	bundleOrderCols = []string{
		"order_id", "user_id", "merchant_id", "version", "status", "total_amount", "currency", "idempotency_key",
		"request_hash", "payment_id", "fail_reason", "expire_at", "paid_at", "created_at", "updated_at",
	}
	bundleItemCols    = []string{"order_id", "sku_id", "quantity", "unit_price", "created_at"}
	bundlePaymentCols = []string{"payment_id", "order_id", "succeeded", "amount", "disposition", "created_at"}
)

func orderArgs(o *OrderRow) []any {
	return []any{
		o.OrderID, o.UserID, o.MerchantID, o.Version, int32(o.Status), o.TotalAmount, o.Currency, o.IdempotencyKey,
		o.RequestHash, o.PaymentID, o.FailReason, o.ExpireAt, o.PaidAt, o.CreatedAt, o.UpdatedAt,
	}
}

func itemArgs(it *ItemRow) []any {
	return []any{it.OrderID, it.SKUID, it.Quantity, it.UnitPrice, it.CreatedAt}
}

func paymentArgs(p *PaymentRow) []any {
	return []any{p.PaymentID, p.OrderID, p.Succeeded, p.Amount, int32(p.Disposition), p.CreatedAt}
}

// placeholders 返回 n 个用逗号分隔的 "?"。
func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// int64Args 把订单号转成语句参数。
func int64Args(ids []int64) []any {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return args
}

// checkBatch 检查一批订单号非空、不超过上限、没有重复，并且都属于 slot。
func (s *Store) checkBatch(slot int, ids []int64) error {
	if err := checkBatchSize(ids); err != nil {
		return err
	}
	if err := s.checkSlot(slot); err != nil {
		return err
	}
	for _, id := range ids {
		if got := s.router.ForOrder(id); got != slot {
			return fmt.Errorf("%w: order %d is in slot %d, not %d", ErrWrongShard, id, got, slot)
		}
	}
	return nil
}

func checkBatchSize(ids []int64) error {
	if len(ids) == 0 || len(ids) > MaxArchiveBatch {
		return fmt.Errorf("store: batch of %d orders, want 1..%d", len(ids), MaxArchiveBatch)
	}
	sorted := slices.Sorted(slices.Values(ids))
	if len(slices.Compact(sorted)) != len(ids) {
		return errors.New("store: batch has duplicate order ids")
	}
	return nil
}

// ListArchivable 在 slot 的主库上返回 after < order_id < below 的终态订单号，升序，最多 limit 个。
//
// 只看订单号和状态，不看时间列：订单号的高位是受理时间（设计文档 6.2），按主键范围扫描。
// 固定走主键：条件里有 status，优化器可能改选 (status, expire_at) 再排序，那样每一批都要把
// 这个状态的全部订单读出来排一遍。归档把表删空的过程中统计信息一直在变，不能指望它每次都选对。
// 读主库是因为结果马上要用来搬数据，从库落后会漏掉刚变成终态的订单（漏了也只是这次不搬）。
func (s *Store) ListArchivable(ctx context.Context, slot int, after, below int64, limit int) ([]int64, error) {
	if err := s.checkSlot(slot); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > MaxArchiveBatch {
		return nil, fmt.Errorf("store: list limit %d, want 1..%d", limit, MaxArchiveBatch)
	}
	in, args := terminalIn()
	args = append([]any{after, below}, append(args, limit)...)
	//nolint:gosec // 表名只来自 sharding.Table，不含外部输入
	rows, err := s.dbOf(slot).QueryContext(ctx,
		"SELECT order_id FROM "+s.tables[slot].orders+" FORCE INDEX (PRIMARY) WHERE order_id > ? AND order_id < ? AND "+in+
			" ORDER BY order_id LIMIT ?", args...)
	if err != nil {
		return nil, fmt.Errorf("list archivable orders in slot %d: %w", slot, err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan archivable order id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate archivable orders: %w", err)
	}
	return ids, nil
}

// CountBelow 在 slot 的主库上数 order_id < below 的订单：terminal 是终态的（可归档），
// nonTerminal 是其余的（早于截止点却还没完结，留在热库，报告给人看）。扫描 below 以下的整段主键。
func (s *Store) CountBelow(ctx context.Context, slot int, below int64) (terminal, nonTerminal int64, err error) {
	if err := s.checkSlot(slot); err != nil {
		return 0, 0, err
	}
	in, args := terminalIn()
	args = append(args, below)
	var total int64
	//nolint:gosec // 表名只来自 sharding.Table，不含外部输入
	err = s.dbOf(slot).QueryRowContext(ctx,
		"SELECT COUNT(*), COALESCE(SUM("+in+"), 0) FROM "+s.tables[slot].orders+" WHERE order_id < ?",
		args...).Scan(&total, &terminal)
	if err != nil {
		return 0, 0, fmt.Errorf("count orders below %d in slot %d: %w", below, slot, err)
	}
	return terminal, total - terminal, nil
}

// ReadBundles 在 slot 的主库上读这批订单的 bundle，按订单号升序。不存在的订单不在结果里。
//
// 普通的快照读，不加锁：放在一个只读事务里，三条语句看到同一个快照。读到的内容随后可能变化
// （支付结果还会增加），由 DeleteArchived 在锁里重读来兜住，这里不需要更强的保证。
func (s *Store) ReadBundles(ctx context.Context, slot int, ids []int64) ([]Bundle, error) {
	if err := s.checkBatch(slot, ids); err != nil {
		return nil, err
	}
	tx, err := s.dbOf(slot).BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin read bundles: %w", err)
	}
	// 只读事务，回滚和提交没有区别。
	defer func() { _ = tx.Rollback() }()
	return readBundles(ctx, tx, s.tables[slot], ids, false)
}

// readBundles 读 ids 的 bundle。lock 为 true 时订单行用 FOR UPDATE 读，必须在事务里调用。
// 订单行最先读：加锁时先一次拿齐这批订单的行锁，再碰子表。
func readBundles(ctx context.Context, q queryer, tb tables, ids []int64, lock bool) ([]Bundle, error) {
	in := placeholders(len(ids))
	args := int64Args(ids)

	orderSQL := "SELECT " + strings.Join(bundleOrderCols, ", ") + " FROM " + tb.orders +
		" WHERE order_id IN (" + in + ") ORDER BY order_id"
	if lock {
		orderSQL += " FOR UPDATE"
	}
	bundles, err := queryRows(ctx, q, orderSQL, args, func(rows *sql.Rows) (Bundle, error) {
		var (
			b      Bundle
			o      = &b.Order
			status int32
		)
		err := rows.Scan(&o.OrderID, &o.UserID, &o.MerchantID, &o.Version, &status, &o.TotalAmount, &o.Currency,
			&o.IdempotencyKey, &o.RequestHash, &o.PaymentID, &o.FailReason, &o.ExpireAt, &o.PaidAt, &o.CreatedAt, &o.UpdatedAt)
		o.Status = orderv1.OrderStatus(status)
		return b, err
	})
	if err != nil {
		return nil, fmt.Errorf("read orders from %s: %w", tb.orders, err)
	}
	if len(bundles) == 0 {
		return nil, nil
	}
	index := make(map[int64]int, len(bundles))
	for i := range bundles {
		index[bundles[i].Order.OrderID] = i
	}

	//nolint:gosec // 表名只来自 sharding.Table 或冷库的常量，不含外部输入
	items, err := queryRows(ctx, q, "SELECT "+strings.Join(bundleItemCols, ", ")+" FROM "+tb.items+
		" WHERE order_id IN ("+in+") ORDER BY order_id, sku_id", args, func(rows *sql.Rows) (ItemRow, error) {
		var it ItemRow
		err := rows.Scan(&it.OrderID, &it.SKUID, &it.Quantity, &it.UnitPrice, &it.CreatedAt)
		return it, err
	})
	if err != nil {
		return nil, fmt.Errorf("read items from %s: %w", tb.items, err)
	}
	for _, it := range items {
		// 订单行不在结果里的子表行（不加锁时订单恰好在两条语句之间被删）不属于任何 bundle。
		if i, ok := index[it.OrderID]; ok {
			bundles[i].Items = append(bundles[i].Items, it)
		}
	}

	//nolint:gosec // 表名只来自 sharding.Table 或冷库的常量，不含外部输入
	pays, err := queryRows(ctx, q, "SELECT "+strings.Join(bundlePaymentCols, ", ")+" FROM "+tb.payments+
		" WHERE order_id IN ("+in+") ORDER BY order_id, payment_id", args, func(rows *sql.Rows) (PaymentRow, error) {
		var (
			p           PaymentRow
			disposition int32
		)
		err := rows.Scan(&p.PaymentID, &p.OrderID, &p.Succeeded, &p.Amount, &disposition, &p.CreatedAt)
		p.Disposition = eventv1.PaymentDisposition(disposition)
		return p, err
	})
	if err != nil {
		return nil, fmt.Errorf("read payment results from %s: %w", tb.payments, err)
	}
	for _, p := range pays {
		if i, ok := index[p.OrderID]; ok {
			bundles[i].Payments = append(bundles[i].Payments, p)
		}
	}
	return bundles, nil
}

// queryRows 执行查询，用 scan 把每一行转成 T。
func queryRows[T any](ctx context.Context, q queryer, query string, args []any, scan func(*sql.Rows) (T, error)) ([]T, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return out, nil
}

// DeleteResult 是 DeleteArchived 的结果，三组订单号互不相交，都是升序。
type DeleteResult struct {
	// Deleted 的三张表的行已经删除。
	Deleted []int64
	// Mismatched 在锁里重算的摘要与期望不同（或者不是终态，不应发生），没有删。
	Mismatched []int64
	// Gone 在热库里已经没有了，通常是另一个归档任务刚删掉。
	Gone []int64
}

// DeleteArchived 在 slot 的主库上删除已经写进冷库的订单：want 是订单号到冷库里那份内容的摘要。
// 只有在订单行锁里重算的摘要等于 want、并且是终态的订单才删，其余的跳过。一个事务，要么这批的
// 结果全部生效，要么都不生效。
//
// 为什么要在锁里重读，而不是比一下订单行的版本号：终态订单的 orders 行不会再变，但它的
// payment_results 还可能增加——订单取消之后才到的支付成功结果会插入一条“要求退款”的记录，
// 而且不改 orders 行，版本号不变。只比订单行的话，这条记录会在写冷之后、删热之前插进热库，
// 然后被一起删掉，冷库里却没有它：钱的处理记录丢了。
//
// 订单行锁就是这个订单三张表的互斥锁：处理支付结果的事务第一步就是对订单行 SELECT ... FOR UPDATE
// （Tx.LockOrder），明细只在更新订单行的事务里改。所以拿到这批订单行锁之后读到的三张表，
// 在提交之前不会再变；它等于 want，而 want 等于冷库的内容，删的那一刻冷热相同。
//
// 加锁顺序：一条语句按主键顺序一次拿齐这批订单行锁，再碰子表。处理支付结果的事务只锁一个订单行，
// 等在这批里的订单上时它手里什么都没有，所以两者之间不会死锁。READ COMMITTED 是为了删子表时
// 不加间隙锁，免得挡住相邻订单的支付结果插入；也让锁里的普通读看到最新提交的内容。
//
// 死锁和等锁超时时整个事务重做，最多 deleteRetries 次。
func (s *Store) DeleteArchived(ctx context.Context, slot int, want map[int64]Digest) (DeleteResult, error) {
	ids := slices.Sorted(maps.Keys(want))
	if err := s.checkBatch(slot, ids); err != nil {
		return DeleteResult{}, err
	}
	var (
		res DeleteResult
		err error
	)
	for attempt := range deleteRetries {
		res, err = s.deleteArchivedOnce(ctx, slot, ids, want)
		retryable := mysqlx.IsDeadlock(err) || mysqlx.IsLockWaitTimeout(err)
		if !retryable || attempt == deleteRetries-1 {
			break
		}
		select {
		case <-ctx.Done():
			return DeleteResult{}, fmt.Errorf("retry delete archived orders: %w (after %w)", ctx.Err(), err)
		case <-time.After(time.Duration(attempt+1) * 50 * time.Millisecond):
		}
	}
	if err != nil {
		return DeleteResult{}, fmt.Errorf("delete archived orders in slot %d: %w", slot, err)
	}
	return res, nil
}

func (s *Store) deleteArchivedOnce(ctx context.Context, slot int, ids []int64, want map[int64]Digest) (res DeleteResult, err error) {
	tb := s.tables[slot]
	tx, err := s.dbOf(slot).BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return DeleteResult{}, fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				err = errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
			}
		}
	}()

	locked, err := readBundles(ctx, tx, tb, ids, true)
	if err != nil {
		return DeleteResult{}, err
	}
	var (
		del               []int64
		wantItems, wantPs int64
	)
	found := make(map[int64]bool, len(locked))
	for i := range locked {
		b := &locked[i]
		id := b.Order.OrderID
		found[id] = true
		if !IsTerminal(b.Order.Status) || b.Digest() != want[id] {
			res.Mismatched = append(res.Mismatched, id)
			continue
		}
		del = append(del, id)
		wantItems += int64(len(b.Items))
		wantPs += int64(len(b.Payments))
	}
	for _, id := range ids {
		if !found[id] {
			res.Gone = append(res.Gone, id)
		}
	}

	if len(del) > 0 {
		in := placeholders(len(del))
		args := int64Args(del)
		// 先子表后订单行。每条语句删掉的行数必须正好是锁里读到的行数：我们持有这些订单的行锁，
		// 子表不会再变，对不上说明有绕过订单行锁写子表的路径，宁可回滚报错也不删。
		for _, step := range []struct {
			table string
			want  int64
		}{
			{tb.items, wantItems},
			{tb.payments, wantPs},
			{tb.orders, int64(len(del))},
		} {
			r, err := tx.ExecContext(ctx, "DELETE FROM "+step.table+" WHERE order_id IN ("+in+")", args...) //nolint:gosec // 表名只来自 sharding.Table
			if err != nil {
				return DeleteResult{}, fmt.Errorf("delete from %s: %w", step.table, err)
			}
			n, err := r.RowsAffected()
			if err != nil {
				return DeleteResult{}, fmt.Errorf("rows affected on %s: %w", step.table, err)
			}
			if n != step.want {
				return DeleteResult{}, fmt.Errorf("delete from %s removed %d rows, read %d under the order locks", step.table, n, step.want)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return DeleteResult{}, fmt.Errorf("commit: %w", err)
	}
	committed = true
	res.Deleted = del
	return res, nil
}

// UpsertBundles 在一个事务里把 bundles 写进冷库。三张表都是整行覆盖的 upsert
// （archived_at 保留第一次写入的时间），重复执行结果相同：归档在“写冷之后、删热之前”中断时，
// 重跑会再写一遍同样的内容。
//
// 只做加法：冷库里多出的明细或支付结果行不会被删掉。按设计这不会发生（热库里的这两张表只增不改），
// 万一发生，归档任务读回冷库比对摘要时会发现并停下。
func (c *Cold) UpsertBundles(ctx context.Context, bundles []Bundle) error {
	if len(bundles) == 0 {
		return nil
	}
	if len(bundles) > MaxArchiveBatch {
		return fmt.Errorf("store: batch of %d orders exceeds %d", len(bundles), MaxArchiveBatch)
	}
	var (
		orders = make([][]any, 0, len(bundles))
		items  [][]any
		pays   [][]any
	)
	for i := range bundles {
		b := &bundles[i]
		if !IsTerminal(b.Order.Status) {
			return fmt.Errorf("%w: order %d has status %s", ErrNotTerminal, b.Order.OrderID, b.Order.Status)
		}
		orders = append(orders, orderArgs(&b.Order))
		for j := range b.Items {
			items = append(items, itemArgs(&b.Items[j]))
		}
		for j := range b.Payments {
			pays = append(pays, paymentArgs(&b.Payments[j]))
		}
	}
	return mysqlx.InTx(ctx, c.db, func(tx *sql.Tx) error {
		for _, t := range []struct {
			table string
			cols  []string
			key   int // 前 key 列是主键，不在 UPDATE 里
			rows  [][]any
		}{
			{ColdOrders, bundleOrderCols, 1, orders},
			{ColdItems, bundleItemCols, 2, items},
			// 冷表的主键是 (order_id, payment_id)，热表是 payment_id；两列都不更新。
			{ColdPayments, bundlePaymentCols, 2, pays},
		} {
			if err := upsertRows(ctx, tx, t.table, t.cols, t.key, t.rows); err != nil {
				return err
			}
		}
		return nil
	})
}

// upsertRows 用多行的 INSERT ... AS new ON DUPLICATE KEY UPDATE 写入 rows，每条语句最多 insertChunkRows 行。
// cols 的前 key 列是主键列，其余列全部用新值覆盖。
func upsertRows(ctx context.Context, tx *sql.Tx, table string, cols []string, key int, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	sets := make([]string, 0, len(cols)-key)
	for _, c := range cols[key:] {
		sets = append(sets, c+" = new."+c)
	}
	head := "INSERT INTO " + table + " (" + strings.Join(cols, ", ") + ") VALUES "
	tail := " AS new ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", ")
	tuple := "(" + placeholders(len(cols)) + ")"
	for chunk := range slices.Chunk(rows, insertChunkRows) {
		var b strings.Builder
		b.WriteString(head)
		args := make([]any, 0, len(chunk)*len(cols))
		for i, r := range chunk {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(tuple)
			args = append(args, r...)
		}
		b.WriteString(tail)
		if _, err := tx.ExecContext(ctx, b.String(), args...); err != nil {
			return fmt.Errorf("upsert into %s: %w", table, err)
		}
	}
	return nil
}

// ReadBundles 从冷库读这批订单的 bundle，按订单号升序，不存在的订单不在结果里。
// 归档任务写冷之后用它读回来比对摘要。
func (c *Cold) ReadBundles(ctx context.Context, ids []int64) ([]Bundle, error) {
	if err := checkBatchSize(ids); err != nil {
		return nil, err
	}
	return readBundles(ctx, c.db, coldTables, ids, false)
}

// HotTables 返回 slot 上的三张热表名：orders、order_items、payment_results。
func (s *Store) HotTables(slot int) ([]string, error) {
	if err := s.checkSlot(slot); err != nil {
		return nil, err
	}
	tb := s.tables[slot]
	return []string{tb.orders, tb.items, tb.payments}, nil
}

// checkHotTable 确认 table 是 slot 上的一张热表，拼进 DDL 的表名只能来自 HotTables。
func (s *Store) checkHotTable(slot int, table string) error {
	names, err := s.HotTables(slot)
	if err != nil {
		return err
	}
	if !slices.Contains(names, table) {
		return fmt.Errorf("store: %q is not a table of slot %d", table, slot)
	}
	return nil
}

// TableBytes 返回 slot 上 table 的 data_length + index_length。先 ANALYZE TABLE：
// information_schema.tables 里的统计值有缓存（information_schema_stats_expiry），刚重建的表会读到旧值。
// 数值是 InnoDB 的估计值，用来看量级。
func (s *Store) TableBytes(ctx context.Context, slot int, table string) (int64, error) {
	if err := s.checkHotTable(slot, table); err != nil {
		return 0, err
	}
	db := s.dbOf(slot)
	if err := adminStatement(ctx, db, "ANALYZE TABLE "+table); err != nil {
		return 0, err
	}
	var n int64
	if err := db.QueryRowContext(ctx,
		"SELECT data_length + index_length FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		table).Scan(&n); err != nil {
		return 0, fmt.Errorf("size of %s: %w", table, err)
	}
	return n, nil
}

// OptimizeTable 在 slot 的主库上重建 table，回收归档删除后留下的空间（设计文档 6.8）。
// InnoDB 上等价于 ALTER TABLE ... FORCE：在线 DDL，开始和结束各要一次短暂的元数据锁，
// 有长事务时会被堵住，并堵住它后面的语句；重建会经复制在从库上再做一遍。
func (s *Store) OptimizeTable(ctx context.Context, slot int, table string) error {
	if err := s.checkHotTable(slot, table); err != nil {
		return err
	}
	return adminStatement(ctx, s.dbOf(slot), "OPTIMIZE TABLE "+table)
}

// adminStatement 执行 ANALYZE / OPTIMIZE 这类返回结果集（Table, Op, Msg_type, Msg_text）的语句。
// 这类语句的失败常常不是语句错误，而是结果集里 Msg_type 为 error 的一行，所以要读完结果集逐行检查。
func adminStatement(ctx context.Context, db *sql.DB, stmt string) error {
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("%s: %w", stmt, err)
	}
	defer rows.Close()
	var msgs []string
	for rows.Next() {
		var table, op, typ, text string
		if err := rows.Scan(&table, &op, &typ, &text); err != nil {
			return fmt.Errorf("%s: scan result: %w", stmt, err)
		}
		if strings.EqualFold(typ, "error") {
			msgs = append(msgs, table+": "+text)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%s: %w", stmt, err)
	}
	if len(msgs) > 0 {
		return fmt.Errorf("%s: %s", stmt, strings.Join(msgs, "; "))
	}
	return nil
}
