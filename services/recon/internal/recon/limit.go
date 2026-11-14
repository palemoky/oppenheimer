package recon

import (
	"context"
	"sync"
)

// DefaultDBConcurrency 是同一个库上同时在跑的对账语句的默认上限。
// 对账的语句都是大范围的扫描，同一个库上开太多会挤占业务的 IO 和缓冲池。
const DefaultDBConcurrency = 2

// Limiter 限制每个库上同时在跑的语句数。按 *DB 区分：同一个连接池（例如订单库的同一个从库）
// 被几张表、几次比对共用时，共用同一个上限。零值不可用，用 NewLimiter。
type Limiter struct {
	n    int
	mu   sync.Mutex
	sems map[*DB]chan struct{}
}

// NewLimiter 返回每个库最多 n 条语句同时执行的 Limiter，n 不大于 0 时取 DefaultDBConcurrency。
func NewLimiter(n int) *Limiter {
	if n <= 0 {
		n = DefaultDBConcurrency
	}
	return &Limiter{n: n, sems: map[*DB]chan struct{}{}}
}

func (l *Limiter) sem(db *DB) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.sems[db]
	if !ok {
		s = make(chan struct{}, l.n)
		l.sems[db] = s
	}
	return s
}

// acquire 占一个 db 上的名额，返回释放函数。ctx 结束时返回错误。
func (l *Limiter) acquire(ctx context.Context, db *DB) (func(), error) {
	s := l.sem(db)
	select {
	case s <- struct{}{}:
		return func() { <-s }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// group 是一组并发的任务：第一个错误取消其余的，Wait 返回它。和 errgroup 一样，
// 这里自己写是为了不给这个模块加依赖。
type group struct {
	wg     sync.WaitGroup
	mu     sync.Mutex
	err    error
	cancel context.CancelFunc
}

func newGroup(ctx context.Context) (*group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &group{cancel: cancel}, ctx
}

func (g *group) Go(f func() error) {
	g.wg.Go(func() {
		if err := f(); err != nil {
			g.mu.Lock()
			if g.err == nil {
				g.err = err
				g.cancel()
			}
			g.mu.Unlock()
		}
	})
}

func (g *group) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}
