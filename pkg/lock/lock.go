// Package lock 是基于单个 Redis 实例的分布式锁，带看门狗续期和 fencing token。
//
// 它能保证的：在 Redis 不丢数据、各方时钟大致正常的前提下，同一时刻 Redis 里只有一个
// 持有者；解锁和续期只作用于自己的锁，不会误删别人的锁；持有者崩溃后锁最多 TTL 就自动释放；
// 每次加锁成功都拿到一个全局单调递增的 fencing token。
//
// 它不能保证的：
//   - 只针对单个 Redis 实例。主从切换时，主节点上刚写入的锁可能还没复制到从节点就宕机了，
//     从节点升主后锁不存在，第二个持有者就能加锁成功。
//   - 持有者暂停（GC、虚拟机迁移、进程被调度走、网络卡住）超过 TTL 时，锁会在它不知情的
//     情况下过期并被别人拿走。它醒来后仍会继续做“持锁期间”的工作，于是两个持有者同时存在。
//     看门狗和 Lock.Context 只能缩短这个窗口，无法消除它。
//
// 所以这把锁只适合两类场景：重复执行无害、加锁只是为了避免浪费（例如只让一个实例跑周期任务）；
// 或者受保护的资源会检查 fencing token，拒绝比已见过的更小的 token。需要严格互斥又无法
// 检查 token 的场景，不要依赖它。
package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrNotAcquired 表示锁被别人持有。
var ErrNotAcquired = errors.New("lock: not acquired")

// ErrLost 表示操作时发现锁已不属于自己（过期后被别人拿走，或已过期）。
var ErrLost = errors.New("lock: lost")

// acquireScript 加锁并取 fencing token。
//
// SET NX 和 INCR 必须在同一个脚本里原子执行。如果分成两次调用：A 加锁成功后还没来得及
// INCR 就卡住，锁过期，B 加锁并 INCR 拿到 1，A 醒来再 INCR 拿到 2。后加锁的 B 反而拿到了
// 更小的 token，资源侧会把 B 当成过时的持有者拒绝，把 A 当成最新的放行，fencing 的意义就
// 完全颠倒了。Redis 单线程执行脚本，脚本内的两条命令之间不会插入别的命令，
// 所以“加锁顺序”和“token 顺序”一致。
//
// 计数器 key 不设过期：一旦过期重置，新的 token 会从 1 重新开始，比旧持有者的 token 还小。
// 返回 0 表示锁被占用（token 从 1 开始，不会与之冲突）。
var acquireScript = redis.NewScript(`
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
	return redis.call('INCR', KEYS[2])
end
return 0
`)

// releaseScript 只在值仍是自己的随机值时才删除。
//
// 不能直接 DEL：持有者以为自己还持有锁的时候，锁可能已经过期并被别人拿走了。
// 这时直接 DEL 删掉的是别人的锁，又会有第三个人进来，互斥被彻底打破。
// GET 和 DEL 必须在一个脚本里：分两次调用的话，GET 之后、DEL 之前锁仍可能过期易主。
var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`)

// renewScript 只在值仍是自己的随机值时才延长过期时间，理由同 releaseScript。
var renewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`)

// Options 是 Locker 的配置。
type Options struct {
	// TTL 是锁的过期时间，必填且至少 1 毫秒（Redis 的 PX 以毫秒为单位）。
	// 持有者崩溃后，锁最多这么久就会自动释放。看门狗每 TTL/3 续期一次。
	TTL time.Duration
	// KeyPrefix 是所有 key 的前缀。
	KeyPrefix string
	// Logger 为空时用 slog.Default()。
	Logger *slog.Logger

	// 以下只在测试里用。

	// now 是本地单调时钟，用来判断“距上次续期成功是否已超过 TTL”。为空时用 time.Now。
	now func() time.Time
	// noWatchdog 为 true 时加锁后不启动看门狗，用来模拟持有者崩溃或卡顿。
	noWatchdog bool
}

// Locker 创建锁。可并发使用。
type Locker struct {
	client redis.Scripter
	ttl    time.Duration
	prefix string
	logger *slog.Logger
	now    func() time.Time

	noWatchdog bool
	// watchdogs 是正在运行的看门狗数量，测试用它确认没有泄漏 goroutine。
	watchdogs atomic.Int64
}

// New 创建 Locker。client 只需要能执行脚本，*redis.Client、redis.UniversalClient 都满足。
func New(client redis.Scripter, opts Options) (*Locker, error) {
	if client == nil {
		return nil, errors.New("lock: nil client")
	}
	if opts.TTL < time.Millisecond {
		return nil, fmt.Errorf("lock: ttl must be at least 1ms, got %v", opts.TTL)
	}
	l := &Locker{
		client:     client,
		ttl:        opts.TTL,
		prefix:     opts.KeyPrefix,
		logger:     opts.Logger,
		now:        opts.now,
		noWatchdog: opts.noWatchdog,
	}
	if l.logger == nil {
		l.logger = slog.Default()
	}
	if l.now == nil {
		// time.Now 带单调时钟读数，Sub 用的是单调时钟，不受系统时间被调整的影响。
		l.now = time.Now
	}
	return l, nil
}

// 锁和计数器用不同的命名空间，避免名为 "x:fence" 的锁与锁 "x" 的计数器撞 key。
func (l *Locker) lockKey(name string) string  { return l.prefix + "lock:" + name }
func (l *Locker) fenceKey(name string) string { return l.prefix + "lockfence:" + name }

// TryAcquire 尝试加锁，不等待。被别人持有时返回 ErrNotAcquired。
// 成功后启动看门狗自动续期，直到 Release 或锁丢失。
//
// 加锁请求超时或出错时，服务端可能已经执行了 SET，此时锁在 Redis 里但调用方不知道，
// 它会在 TTL 后自动过期。
func (l *Locker) TryAcquire(ctx context.Context, name string) (*Lock, error) {
	if name == "" {
		return nil, errors.New("lock: empty name")
	}
	value, err := randomValue()
	if err != nil {
		return nil, err
	}

	// 超过 TTL 才返回的加锁结果没有意义：即使成功，锁也可能已经过期了。
	callCtx, cancel := context.WithTimeout(ctx, l.ttl)
	defer cancel()
	// 记下发出请求“之前”的时间。Redis 的过期时间从它执行 SET 时算起，这一刻一定晚于
	// 发请求的时刻，所以用发请求前的时间作为起点是保守的：本地判断的过期只会早于真实过期。
	sent := l.now()
	token, err := acquireScript.Run(callCtx, l.client,
		[]string{l.lockKey(name), l.fenceKey(name)},
		value, l.ttl.Milliseconds()).Int64()
	if err != nil {
		return nil, fmt.Errorf("lock: acquire %s: %w", name, err)
	}
	if token == 0 {
		return nil, ErrNotAcquired
	}

	// 锁的生命周期独立于调用方的 ctx：调用方加锁用的 ctx 结束后锁仍然有效，直到 Release。
	// WithoutCancel 保留 ctx 里的值（例如 trace），只去掉取消信号。
	lockCtx, lockCancel := context.WithCancelCause(context.WithoutCancel(ctx))
	k := &Lock{
		locker: l,
		name:   name,
		key:    l.lockKey(name),
		value:  value,
		token:  token,
		ctx:    lockCtx,
		cancel: lockCancel,
		done:   make(chan struct{}),
	}
	if l.noWatchdog {
		close(k.done)
		k.stopWatchdog = func() {}
		return k, nil
	}
	// 看门狗的 ctx 由锁的 ctx 派生：锁丢失或释放时它跟着停；但它也可以单独停掉
	// （测试里用来模拟持有者卡顿），此时锁的 ctx 不受影响，持有者仍以为自己持有锁。
	wdCtx, wdCancel := context.WithCancel(lockCtx)
	k.stopWatchdog = func() {
		wdCancel()
		<-k.done
	}
	l.watchdogs.Add(1)
	go k.watch(wdCtx, sent)
	return k, nil
}

// Acquire 加锁，被占用时每隔 retryInterval 重试，直到成功或 ctx 结束。
// ctx 结束时返回的错误同时匹配 ErrNotAcquired 和 ctx 的错误。
func (l *Locker) Acquire(ctx context.Context, name string, retryInterval time.Duration) (*Lock, error) {
	if retryInterval <= 0 {
		return nil, fmt.Errorf("lock: retry interval must be positive, got %v", retryInterval)
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("lock: acquire %s: %w: %w", name, ErrNotAcquired, ctx.Err())
		case <-timer.C:
		}
		k, err := l.TryAcquire(ctx, name)
		if err == nil {
			return k, nil
		}
		if ctx.Err() != nil {
			// ctx 恰好在加锁请求途中结束。统一按“没拿到”返回，调用方不用区分两种超时。
			// 服务端可能已经执行了 SET，那把无主的锁会在 TTL 后过期。
			return nil, fmt.Errorf("lock: acquire %s: %w: %w", name, ErrNotAcquired, ctx.Err())
		}
		if !errors.Is(err, ErrNotAcquired) {
			return nil, err
		}
		timer.Reset(retryInterval)
	}
}

// Lock 是一把已持有的锁。
type Lock struct {
	locker *Locker
	name   string
	key    string
	value  string // 本次加锁的随机值，用来识别“这把锁还是不是我的”
	token  int64

	ctx    context.Context
	cancel context.CancelCauseFunc

	stopWatchdog func() // 停掉看门狗并等它退出；可重复调用
	done         chan struct{}

	mu       sync.Mutex
	released bool
}

// Token 返回 fencing token：全局单调递增，每次加锁成功都会得到一个更大的值。
// 持锁期间访问受保护的资源时带上它，资源侧拒绝比已见过的更小的 token。
func (k *Lock) Token() int64 { return k.token }

// Context 返回一个在锁丢失或释放时被取消的 context。
// 持锁期间的工作应当使用它（或由它派生），这样锁丢了能尽快停下来。
// 取消原因（context.Cause）在锁丢失时是 ErrLost，在 Release 时是 context.Canceled。
//
// 它被取消只说明“已经发现”锁丢了。持有者暂停期间锁可能早就过期了，
// 所以它没被取消并不能证明锁仍属于自己。
func (k *Lock) Context() context.Context { return k.ctx }

// Release 释放锁并停止看门狗。只有当锁仍属于自己时才删除。
// 锁已丢失时返回 ErrLost（此时没有删除任何东西）。可以重复调用，第二次起返回 nil。
//
// Redis 调用出错时返回该错误，锁会在 TTL 后自动过期；之后再调用 Release 也返回 nil，不会重试。
func (k *Lock) Release(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.released {
		return nil
	}
	k.released = true

	k.stopWatchdog()
	// 先让持锁期间的工作停下来，再删锁。反过来的话，删锁之后、工作停下之前，
	// 别人已经能拿到锁，两边会同时工作一小段时间。
	k.cancel(context.Canceled)

	callCtx, cancel := context.WithTimeout(ctx, k.locker.ttl)
	defer cancel()
	n, err := releaseScript.Run(callCtx, k.locker.client, []string{k.key}, k.value).Int64()
	if err != nil {
		return fmt.Errorf("lock: release %s: %w", k.name, err)
	}
	if n == 0 {
		return ErrLost
	}
	return nil
}

// watch 是看门狗：每 TTL/3 续期一次，直到 ctx 被取消或发现锁丢失。
// lastOK 是上一次确认锁仍属于自己的时刻（加锁或续期请求发出之前）。
func (k *Lock) watch(ctx context.Context, lastOK time.Time) {
	l := k.locker
	// 先减计数再 close(done)：Release 等到 done 之后，计数一定已经归零。
	defer close(k.done)
	defer l.watchdogs.Add(-1)

	// 间隔取 TTL/3：一次续期失败后还有两次机会，网络抖一下不至于丢锁。
	interval := max(l.ttl/3, time.Millisecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		sent := l.now()
		ok, err := k.renew(ctx, interval)
		switch {
		case err == nil && ok:
			lastOK = sent
		case err == nil:
			// Redis 明确说锁不是我的了：过期后被别人拿走，或者已经过期不存在。
			l.logger.WarnContext(ctx, "lock lost", "lock_name", k.name, "fencing_token", k.token, "reason", "not_owner")
			k.cancel(ErrLost)
			return
		case ctx.Err() != nil:
			// Release 或测试停掉了看门狗，调用被取消，不算丢锁。
			return
		default:
			// 网络问题不能立刻判定丢锁：续期可能其实已经成功，只是响应丢了。
			// 但从上一次确认算起超过 TTL 后，即使 Redis 没出问题，锁也已经过期了，
			// 必须判定丢失，让持有者停下来。
			//
			// 这个判断只是尽力而为：本地单调时钟和 Redis 的时钟不是同一个，两者走速可能不同；
			// 持有者整个进程暂停（GC、虚拟机迁移）时，这段代码根本没机会运行，锁会在它不知情的
			// 情况下过期。所以“Context 没被取消”不能证明锁还在，受保护的资源仍需要检查 fencing token。
			if elapsed := l.now().Sub(lastOK); elapsed >= l.ttl {
				l.logger.WarnContext(ctx, "lock lost", "lock_name", k.name, "fencing_token", k.token,
					"reason", "renew_failed_past_ttl", "since_last_renew", elapsed, "error", err)
				k.cancel(ErrLost)
				return
			}
			l.logger.WarnContext(ctx, "lock renew failed", "lock_name", k.name, "fencing_token", k.token, "error", err)
		}
	}
}

// renew 续期一次。返回 false 表示锁已不属于自己。
// 每次调用的超时不超过续期间隔，避免一次卡住的调用拖过多个续期周期。
func (k *Lock) renew(ctx context.Context, timeout time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	n, err := renewScript.Run(ctx, k.locker.client, []string{k.key}, k.value, k.locker.ttl.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("lock: renew %s: %w", k.name, err)
	}
	return n == 1, nil
}

// randomValue 生成锁的值。它必须在所有持有者之间不可能重复，否则解锁和续期的校验会把
// 别人的锁当成自己的。128 位随机数的碰撞概率可以忽略。
func randomValue() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("lock: random value: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
