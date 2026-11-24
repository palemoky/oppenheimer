//go:build faults

package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"time"

	"github.com/palemoky/oppenheimer/pkg/fault"
)

var replicaScanLimit = fault.Switch("f06")

const (
	// defaultReplicaScanLimit 是调用方没有给时限时，从库上一次扫描在服务端最多执行的时间。
	defaultReplicaScanLimit = 5 * time.Second
	// sessionRestoreTimeout 是连接交还之前还原会话变量的时限。和调用方的 ctx 无关：调用方的时限可能刚好到了，
	// 还原仍然要做完，否则这条连接只能丢掉。
	sessionRestoreTimeout = time.Second
)

// listRefs 在 slot 上执行 query，返回 (order_id, user_id) 的列表。src 的含义同 ListExpired。
func (s *Store) listRefs(ctx context.Context, slot int, src Source, query string, args ...any) ([]OrderRef, error) {
	if src == Replica && s.replicas != nil && replicaScanLimit.On() {
		return listReplicaRefs(ctx, s.replicaOf(slot), slot, query, args...)
	}
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

// listReplicaRefs 是 listRefs 在从库上的版本：除了 ctx 的时限，在服务端也给这次扫描设一个执行时限。
//
// 只靠 ctx 不够：ctx 到期时驱动关掉的是客户端这一头的连接，从库上的那条 SELECT 还会一直跑完，
// 一个慢的从库上会越积越多。max_execution_time 让从库自己在时限到了的时候停下（报错 3024）。
// 语句是调用方拼好传进来的，这里不去改写它加优化器提示，所以用会话变量：SET 和 SELECT 必须在同一条连接上，
// 先从池子里拿出一条连接固定下来。
//
// 连接交还之前要把会话变量还原：同一个池子里还有探测用的 WAIT_FOR_EXECUTED_GTID_SET（也是 SELECT），
// 带着一个几毫秒的时限回到池子里的连接会让探测莫名其妙地失败。
func listReplicaRefs(ctx context.Context, db *sql.DB, slot int, query string, args ...any) ([]OrderRef, error) {
	limit := defaultReplicaScanLimit
	if dl, ok := ctx.Deadline(); ok {
		limit = time.Until(dl)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("list orders in slot %d: %w", slot, err)
	}
	ms := strconv.FormatInt(max(limit.Milliseconds(), 1), 10)
	if _, err := conn.ExecContext(ctx, "SET SESSION max_execution_time = "+ms); err != nil {
		handBackScanConn(ctx, conn)
		return nil, fmt.Errorf("list orders in slot %d: set execution limit: %w", slot, err)
	}
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		handBackScanConn(ctx, conn)
		return nil, fmt.Errorf("list orders in slot %d: %w", slot, err)
	}
	if !rows.Next() {
		// 没有候选，这是大多数轮次的情况（延迟消息是关单的主路径，扫表只兜底）。
		// Next 返回 false 时结果集已经自己关了，Err 区分“没有行”和“读的时候出错”。
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate order refs: %w", err)
		}
		return nil, nil
	}
	defer handBackScanConn(ctx, conn)
	defer rows.Close()
	var refs []OrderRef
	for ok := true; ok; ok = rows.Next() {
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

// handBackScanConn 还原会话的执行时限并把连接交还池子。还原失败的连接不再交还（告诉 database/sql 它坏了，
// 由池子关掉它），不让带着时限的会话被别的查询拿到。
func handBackScanConn(ctx context.Context, conn *sql.Conn) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionRestoreTimeout)
	defer cancel()
	if _, err := conn.ExecContext(rctx, "SET SESSION max_execution_time = DEFAULT"); err != nil {
		// Raw 的回调返回 ErrBadConn 时 database/sql 把这条连接标成坏的，Close 时不回池而是关掉。
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	// 连接已经交还或者已经丢掉，Close 的错误（ErrConnDone）没有可做的补救。
	_ = conn.Close()
}
