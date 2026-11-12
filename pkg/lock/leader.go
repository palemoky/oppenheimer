package lock

import (
	"context"
	"errors"
	"sync"
)

// Leader 让多个实例中只有一个在运行某个周期任务。它满足 worker.Guard 接口。
//
// 它只适合“重复执行无害、只是想避免浪费”的任务：持有者暂停超过 TTL 时，两个实例可能
// 在同一轮里都认为自己是 leader。
type Leader struct {
	locker *Locker
	name   string

	mu   sync.Mutex
	lock *Lock // 当前持有的锁；未持有时为 nil
}

// Leader 返回一个按 name 竞争的 Leader。
func (l *Locker) Leader(name string) *Leader {
	return &Leader{locker: l, name: name}
}

// Allow 的语义：当前实例持有锁返回 true；没持有则尝试加锁，成功返回 true，被别人持有返回 false。
// 持有期间由看门狗续期；锁丢失后下一次 Allow 会重新竞争。
//
// 返回 true 只说明“刚才确认过”自己持有锁，不保证这一轮运行期间一直持有。
func (ld *Leader) Allow(ctx context.Context) (bool, error) {
	ld.mu.Lock()
	defer ld.mu.Unlock()

	if ld.lock != nil {
		if ld.lock.Context().Err() == nil {
			return true, nil
		}
		// 看门狗已经判定丢锁。Release 停掉（已退出的）看门狗并收尾；
		// 结果多半是 ErrLost，不影响重新竞争。
		_ = ld.lock.Release(ctx)
		ld.lock = nil
	}

	k, err := ld.locker.TryAcquire(ctx, ld.name)
	if errors.Is(err, ErrNotAcquired) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ld.lock = k
	return true, nil
}

// Token 返回当前持有的 fencing token，未持有时为 0。
func (ld *Leader) Token() int64 {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	if ld.lock == nil || ld.lock.Context().Err() != nil {
		return 0
	}
	return ld.lock.Token()
}

// Close 释放锁。锁已经丢失时返回 nil：对 leader 来说，退出前丢锁是正常情况，不是错误。
// Close 之后再调用 Allow 会重新竞争。
func (ld *Leader) Close(ctx context.Context) error {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	if ld.lock == nil {
		return nil
	}
	err := ld.lock.Release(ctx)
	ld.lock = nil
	if errors.Is(err, ErrLost) {
		return nil
	}
	return err
}
