package idgen

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Leased 是 Open 的 workerID 参数的特殊值：不固定节点号，从数据库里租。
const Leased = -1

// 启动时租节点号的时限。正常是几条语句；要等上一任的上界时最多等一个租期。
const openTimeout = DefaultLeaseTTL + 15*time.Second

// Open 按配置得到一个 Generator，是各服务装配时的入口。
//
//   - workerID 在 [0, MaxWorkerID]：固定用这个节点号，不碰数据库，返回的 Lease 是 nil。
//     只适合一个服务只有一个实例的环境（本机、CI），由部署的人保证不重复。
//   - workerID 是 Leased：从 db 里租（db 必须是主库，表不存在时自动建）。返回的 Lease 要交给 app.Run，
//     排在所有会发号的组件之前；没有交出去就退出时调用它的 Stop。
func Open(ctx context.Context, workerID int64, db *sql.DB, logger *slog.Logger) (*Generator, *Lease, error) {
	if workerID != Leased {
		g, err := New(workerID)
		return g, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()
	store, err := NewSQLLeaseStore(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	l, err := AcquireLease(ctx, LeaseConfig{Store: store, Logger: logger})
	if err != nil {
		return nil, nil, err
	}
	return l.Generator(), l, nil
}

// LeaseSchema 是租约表的建表语句。一行一个节点号，一共 MaxWorkerID+1 行，行由 NewSQLLeaseStore 补齐。
//
//   - expires_at 用库的时钟（NOW(3)）写、用库的时钟比，不和任何一台应用机器的时钟比较。
//   - last_ms 是这个节点号的上界：历任持有者可能用到的最大毫秒时间（相对 Epoch），见 lease.go 开头。
//   - owner 为空表示已经让出。
const LeaseSchema = `CREATE TABLE IF NOT EXISTS idgen_worker (
  worker_id  TINYINT UNSIGNED NOT NULL,
  owner      VARCHAR(128)     NOT NULL DEFAULT '',
  expires_at DATETIME(3)      NOT NULL,
  last_ms    BIGINT           NOT NULL DEFAULT 0,
  PRIMARY KEY (worker_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`

// 一次取几个候选的节点号去抢。多个进程同时启动时看到的候选相同，第一个被别人抢走就试下一个。
const leaseCandidates = 8

// SQLLeaseStore 把租约放在 MySQL 的一张表里（LeaseSchema）。必须连主库。
//
// 每个方法都是单条的条件更新，靠主键上的行锁互斥，不开事务、不用加锁读。
type SQLLeaseStore struct {
	db *sql.DB
}

// NewSQLLeaseStore 建表（不存在时）并补齐每个节点号的行，可以重复执行、可以多个进程同时执行。
//
// 表由这里建而不放进各服务的迁移文件：order 的迁移是按分片渲染、每个订单库都跑一遍的，
// 而这张表只应该存在于一个库里（一个服务一张）。
func NewSQLLeaseStore(ctx context.Context, db *sql.DB) (*SQLLeaseStore, error) {
	if _, err := db.ExecContext(ctx, LeaseSchema); err != nil {
		return nil, fmt.Errorf("idgen: create lease table: %w", err)
	}
	// 新行的 expires_at 是一个很早的时间：没人持有，而且按“过期最久的优先”排在最前面。
	var b strings.Builder
	b.WriteString("INSERT IGNORE INTO idgen_worker (worker_id, expires_at) VALUES ")
	for i := range MaxWorkerID + 1 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d, '2000-01-01 00:00:00.000')", i)
	}
	if _, err := db.ExecContext(ctx, b.String()); err != nil {
		return nil, fmt.Errorf("idgen: seed lease table: %w", err)
	}
	return &SQLLeaseStore{db: db}, nil
}

// Acquire 实现 LeaseStore。
//
// 先不加锁地读出过期最久的几个候选，再逐个用条件更新去抢（WHERE 里重新判断过期，影响一行才算抢到）。
// 不用 SELECT ... FOR UPDATE SKIP LOCKED：这张表按 expires_at 排序要扫全表，加锁读会把扫到的行都锁住，
// 同时启动的另一个进程跳过所有被锁的行之后会以为没有可租的号。
func (s *SQLLeaseStore) Acquire(ctx context.Context, owner string, ttl time.Duration) (int64, int64, error) {
	if owner == "" {
		return 0, 0, errors.New("idgen: lease owner must not be empty")
	}
	// 候选被抢光时重新取一批；连续几批都抢不到说明启动的进程比空闲的号还多。
	for range 3 {
		ids, err := s.candidates(ctx)
		if err != nil {
			return 0, 0, err
		}
		if len(ids) == 0 {
			return 0, 0, ErrNoWorkerID
		}
		for _, id := range ids {
			res, err := s.db.ExecContext(ctx,
				`UPDATE idgen_worker SET owner = ?, expires_at = TIMESTAMPADD(MICROSECOND, ?, NOW(3))
				  WHERE worker_id = ? AND expires_at < NOW(3)`, owner, ttl.Microseconds(), id)
			if err != nil {
				return 0, 0, fmt.Errorf("idgen: claim worker id %d: %w", id, err)
			}
			if n, err := res.RowsAffected(); err != nil {
				return 0, 0, fmt.Errorf("idgen: claim worker id %d: %w", id, err)
			} else if n == 0 {
				continue // 被别人抢先了
			}
			// 抢到之后才读上界：上一任这时已经不是持有者，改不了它。带上 owner 是防这段时间里租约又过期被人租走。
			var prior int64
			err = s.db.QueryRowContext(ctx,
				`SELECT last_ms FROM idgen_worker WHERE worker_id = ? AND owner = ?`, id, owner).Scan(&prior)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				return 0, 0, fmt.Errorf("idgen: read bound of worker id %d: %w", id, err)
			}
			return id, prior, nil
		}
	}
	return 0, 0, fmt.Errorf("%w: lost every race for a candidate", ErrNoWorkerID)
}

func (s *SQLLeaseStore) candidates(ctx context.Context) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT worker_id FROM idgen_worker WHERE expires_at < NOW(3) ORDER BY expires_at, worker_id LIMIT ?`, leaseCandidates)
	if err != nil {
		return nil, fmt.Errorf("idgen: list free worker ids: %w", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("idgen: list free worker ids: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("idgen: list free worker ids: %w", err)
	}
	return ids, nil
}

// Extend 实现 LeaseStore。条件只有“我还是持有者”，不要求没过期：过期了但没被别人租走的，续上就是。
func (s *SQLLeaseStore) Extend(ctx context.Context, workerID int64, owner string, ttl time.Duration, boundMs int64) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE idgen_worker SET expires_at = TIMESTAMPADD(MICROSECOND, ?, NOW(3)), last_ms = GREATEST(last_ms, ?)
		  WHERE worker_id = ? AND owner = ?`, ttl.Microseconds(), boundMs, workerID, owner)
	if err != nil {
		return false, fmt.Errorf("idgen: extend worker id %d: %w", workerID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("idgen: extend worker id %d: %w", workerID, err)
	}
	if n > 0 {
		return true, nil
	}
	// 影响 0 行有两种可能：不是持有者；或者是持有者但这次更新没有改变任何值（同一毫秒里重复续租，
	// 驱动默认报告的是“改变的行数”而不是“匹配的行数”）。后一种不能当成丢了租约，查一次分清楚。
	var one int
	err = s.db.QueryRowContext(ctx,
		`SELECT 1 FROM idgen_worker WHERE worker_id = ? AND owner = ? AND last_ms >= ?`, workerID, owner, boundMs).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("idgen: verify worker id %d: %w", workerID, err)
	}
	return true, nil
}

// Release 实现 LeaseStore。expires_at 设成现在：一毫秒之后就满足“已过期”，而且在空闲的号里排在最后，
// 下一个启动的进程会先用闲置更久的号。
func (s *SQLLeaseStore) Release(ctx context.Context, workerID int64, owner string, boundMs int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE idgen_worker SET owner = '', expires_at = NOW(3), last_ms = ? WHERE worker_id = ? AND owner = ?`,
		boundMs, workerID, owner)
	if err != nil {
		return fmt.Errorf("idgen: release worker id %d: %w", workerID, err)
	}
	return nil
}
