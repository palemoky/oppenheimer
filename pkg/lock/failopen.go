package lock

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// failOpenWarnEvery 是锁持续不可用期间重复告警的最小间隔。
const failOpenWarnEvery = 30 * time.Second

// Allower 是 FailOpen 包装的对象，与 worker.Guard 的方法集相同。
// 在这里重复声明是为了不让 lock 依赖 worker。
type Allower interface {
	Allow(ctx context.Context) (bool, error)
}

// FailOpenGuard 见 FailOpen。
type FailOpenGuard struct {
	inner  Allower
	logger *slog.Logger
	now    func() time.Time

	mu       sync.Mutex
	degraded bool
	lastWarn time.Time
}

// FailOpen 包装一个 Guard：它无法判断（返回错误）时放行，而不是跳过这一轮。
//
// 只能用在“互斥只是为了省事”的任务上：outbox 中继、sweeper、孤儿占用回收、支付处理器。
// 它们的正确性靠条件更新和原子脚本保证，多个副本同时运行只是浪费。这类任务如果在锁
// 不可用时停下，Redis 一挂事件链路就跟着停，等于凭空多了一个单点。
//
// 不能用在正确性依赖互斥的任务上（闸门对账）：那里宁可不跑，也不能跑两份。
//
// 放行期间的行为是“每个副本都跑”，不是“只有原来的 leader 跑”：非 leader 的加锁请求
// 同样会失败，同样被放行。
func FailOpen(inner Allower, logger *slog.Logger) *FailOpenGuard {
	if logger == nil {
		logger = slog.Default()
	}
	return &FailOpenGuard{inner: inner, logger: logger, now: time.Now}
}

// Allow 在内层 Guard 出错时返回 true。调用方的 ctx 已结束时不放行：那是进程在退出，
// 不是锁不可用。
func (f *FailOpenGuard) Allow(ctx context.Context) (bool, error) {
	ok, err := f.inner.Allow(ctx)
	if err == nil {
		f.recovered()
		return ok, nil
	}
	if ctx.Err() != nil {
		return false, err
	}
	f.warn(err)
	return true, nil
}

func (f *FailOpenGuard) warn(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	if f.degraded && now.Sub(f.lastWarn) < failOpenWarnEvery {
		return
	}
	f.degraded = true
	f.lastWarn = now
	f.logger.Warn("lock unavailable, running without mutual exclusion", "error", err)
}

func (f *FailOpenGuard) recovered() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.degraded {
		return
	}
	f.degraded = false
	f.logger.Info("lock available again")
}
