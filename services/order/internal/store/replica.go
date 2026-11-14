package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// Position 是某一时刻主库的复制位置。
type Position struct {
	// GTID 是主库已执行的 GTID 集合（@@global.gtid_executed），已去掉空白字符。
	GTID string
	// At 是读出 GTID 时主库的时间，Unix 微秒。同一个主库上，At 较大的那次读到的集合包含 At 较小的那次的，
	// 用来比较两个位置的先后，不用去解析和比较 GTID 集合。
	At int64
}

// ErrNoReplica 表示调用了需要从库的方法，但没有配置从库。
var ErrNoReplica = errors.New("store: no replica configured")

// waitReplicaSlack 是 WaitReplica 在服务端时限之外多给的余量：服务端超时后返回 1 还需要一次网络往返，
// 不给余量的话 ctx 可能抢在结果之前到期，把“超时”报成错误。
const waitReplicaSlack = time.Second

// PrimaryPosition 在订单所在库的主库上读出当前的 GTID 集合和主库时间。
//
// 在写入提交之后调用：提交返回时本次事务的 GTID 已在 gtid_executed 里，读到的集合包含这次写入
// （也可能多包含几个同时提交的事务，从库等它们也只是多等一点）。两个值在同一条语句里读，
// 保证 At 对应的就是这个集合。
func (s *Store) PrimaryPosition(ctx context.Context, orderID int64) (Position, error) {
	p, err := s.PrimaryPositionOfDB(ctx, s.DBOfOrder(orderID))
	if err != nil {
		return Position{}, fmt.Errorf("order %d: %w", orderID, err)
	}
	return p, nil
}

// PrimaryPositionOfDB 和 PrimaryPosition 相同，按库号定位。给从库的健康探测用（设计文档 1d 的 6.7）。
func (s *Store) PrimaryPositionOfDB(ctx context.Context, db int) (Position, error) {
	if db < 0 || db >= len(s.dbs) {
		return Position{}, fmt.Errorf("store: db %d out of range [0, %d)", db, len(s.dbs))
	}
	var p Position
	err := s.dbs[db].QueryRowContext(ctx,
		"SELECT @@global.gtid_executed, CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000 AS UNSIGNED)").Scan(&p.GTID, &p.At)
	if err != nil {
		return Position{}, fmt.Errorf("read primary position of db %d: %w", db, err)
	}
	// 集合有多个来源（例如主从切换过）时，MySQL 在逗号后面加换行。
	p.GTID = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, p.GTID)
	return p, nil
}

// WaitReplica 在订单所在库的从库上等它执行完 gtid 集合，最多等 timeout。
// 返回 true 表示已追上：之后从库上的任何连接都能读到这个集合里的写入，不要求等待和读用同一条连接。
//
// 在 MySQL 9.7.4 上实测 WAIT_FOR_EXECUTED_GTID_SET(set, timeout) 的行为：
//   - timeout 接受小数秒：0.1 约 100 毫秒、0.35 约 350 毫秒后返回，0.001 也有效。
//   - 已执行的集合立即返回 0；永远等不到的集合（事务号远超当前值、或从没见过的 UUID）在超时后返回 1，不报错。
//   - timeout 为 0 表示不限时、一直等下去；负数报错 1210；格式错误的集合报错 1772。
//
// 所以这里拒绝 timeout <= 0：传 0 会让一个落后的从库把请求永远挂住。另外给查询加了一个比 timeout
// 稍长的 ctx 时限，防止从库连上了却不响应。ctx 先到期时驱动关闭这条连接（连接池不会再用它），
// 服务端的等待最多持续到 timeout 后自行结束，不会遗留。
func (s *Store) WaitReplica(ctx context.Context, orderID int64, gtid string, timeout time.Duration) (bool, error) {
	caught, err := s.WaitReplicaOfDB(ctx, s.DBOfOrder(orderID), gtid, timeout)
	if err != nil {
		return false, fmt.Errorf("order %d: %w", orderID, err)
	}
	return caught, nil
}

// WaitReplicaOfDB 和 WaitReplica 相同，按库号定位。给从库的健康探测用（设计文档 1d 的 6.7）。
func (s *Store) WaitReplicaOfDB(ctx context.Context, db int, gtid string, timeout time.Duration) (bool, error) {
	if s.replicas == nil {
		return false, ErrNoReplica
	}
	if db < 0 || db >= len(s.replicas) {
		return false, fmt.Errorf("store: db %d out of range [0, %d)", db, len(s.replicas))
	}
	if timeout <= 0 {
		return false, fmt.Errorf("store: wait replica timeout must be positive, got %s", timeout)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout+waitReplicaSlack)
	defer cancel()
	var r sql.NullInt64
	if err := s.replicas[db].QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, ?)",
		gtid, timeout.Seconds()).Scan(&r); err != nil {
		return false, fmt.Errorf("wait replica of db %d: %w", db, err)
	}
	switch {
	case !r.Valid:
		return false, fmt.Errorf("wait replica of db %d: got NULL", db)
	case r.Int64 == 0:
		return true, nil
	case r.Int64 == 1:
		return false, nil
	default:
		return false, fmt.Errorf("wait replica of db %d: unexpected result %d", db, r.Int64)
	}
}
