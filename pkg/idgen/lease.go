package idgen

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// 节点号的租约（ADR-0028，设计文档 2c 第 5 节）。
//
// 一个服务跑多个副本时，每个进程启动时从共享的存储（LeaseStore，生产环境是本服务的 MySQL）里租一个
// 节点号，定期续租，退出时让出。要保证的性质：**同一个节点号发出的号不重复**，包括滚动发布时新旧
// 进程同时存在、持有者卡住很久之后醒来、两台机器的时钟不一致的时候。
//
// 这条性质不靠“同一时刻只有一个持有者”来保证（那要比较不同机器上的时钟），而是靠给每一任持有者划一段
// 互不重叠的毫秒区间：
//
//   - 存储里每个节点号有一个上界 last_ms：历任持有者可能用到的最大毫秒时间。
//   - 持有者每次续租把上界抬到“自己的时钟加一个租期”。只有存储确认写入了，它才用到那么远；
//     发号时毫秒时间超过已确认的上界就拒绝（ErrLeaseExpired）。
//   - 下一任在租到的同时读到上一任留下的上界，只发比它大的毫秒。上一任这时已经不是持有者，
//     续租会失败，再也抬不动上界。
//
// 所以上一任发的号都在上界以内，下一任发的都在上界以外，中间由存储的一次条件更新隔开。租期和续租间隔
// 只影响可用性（存储不可用多久之后停止发号、崩溃的进程占着的号多久能被别人租走），不影响唯一性。
//
// 仍然依赖的一点：存储自己不丢已经确认的写入。MySQL 的主库故障切换（异步复制）可能丢掉最后一次续租，
// 新主库上的上界就比持有者手里的小。Guard 是为这个留的余量：下一任从“上界加 Guard”之后才开始发号，
// 而丢了续租的持有者最迟在下一次续租时发现自己不再是持有者。

// ErrLeaseExpired 表示节点号的租约没有及时续上（存储不可用，或者本机时钟向前跳了），暂时不能发号。
// 续租成功后自动恢复。调用方应当把它当成依赖不可用（降级），不是内部错误。
var ErrLeaseExpired = errors.New("idgen: worker id lease expired")

// ErrLeaseLost 表示节点号已经被别的进程租走。不可恢复：进程应当退出，重启后重新租。
var ErrLeaseLost = errors.New("idgen: worker id lease lost")

// ErrNoWorkerID 表示所有节点号都有人持有。
var ErrNoWorkerID = errors.New("idgen: no free worker id")

// LeaseStore 存放节点号的租约。实现必须保证三个方法各自是原子的，并且“过期与否”只用存储自己的时钟判断。
type LeaseStore interface {
	// Acquire 把一个没人持有或已经过期的节点号租给 owner，返回节点号和上一任留下的上界（毫秒，相对 Epoch）。
	// 没有可租的返回 ErrNoWorkerID。
	Acquire(ctx context.Context, owner string, ttl time.Duration) (workerID, priorBoundMs int64, err error)
	// Extend 续租，并把上界抬到 boundMs（已有的更大就保持）。held 为 false 表示 owner 已经不是持有者，
	// 这时什么都没有改。
	Extend(ctx context.Context, workerID int64, owner string, ttl time.Duration, boundMs int64) (held bool, err error)
	// Release 让出节点号，把上界改成 boundMs（可以比登记的小：持有者实际只用到这里）。
	// owner 已经不是持有者时什么都不做。
	Release(ctx context.Context, workerID int64, owner string, boundMs int64) error
}

const (
	// DefaultLeaseTTL 是租期。存储不可用超过“租期减续租间隔”之后发号会停；崩溃的进程占着的号过这么久才能被别人租走。
	DefaultLeaseTTL = 30 * time.Second
	// 租不到合适的号时最多换几个。
	maxAcquireAttempts = 5
	// 续租失败（存储出错）后多久重试，不等一个完整的间隔。
	renewRetryInterval = time.Second
)

// LeaseConfig 是 AcquireLease 的配置。
type LeaseConfig struct {
	Store LeaseStore
	// Owner 标识这个进程，要全局唯一。默认是主机名（Pod 名）加一段随机串：同一个 Pod 里重启的容器也是新的持有者。
	Owner string
	// TTL 是租期，默认 DefaultLeaseTTL。
	TTL time.Duration
	// RenewInterval 是续租间隔，默认 TTL 的三分之一。
	RenewInterval time.Duration
	// Guard 是接手时在上一任的上界之外再让出的时间，默认等于 RenewInterval（见文件开头的说明）。
	Guard  time.Duration
	Logger *slog.Logger
	// Now 是本机的时钟，默认 time.Now。测试用它模拟时钟不一致的机器。
	Now func() time.Time
}

// Lease 是一个租来的节点号和用它发号的 Generator。它实现 app.Component：Start 负责续租，Stop 让出。
//
// Lease 要排在所有会发号的组件之前（app 按相反顺序停止，它最后停）：Stop 之后 Generator 不再发号。
type Lease struct {
	cfg      LeaseConfig
	workerID int64
	priorMs  int64 // 上一任留下的上界，让出时登记的上界不能比它小
	gen      *Generator

	stopOnce sync.Once
	stop     chan struct{}
	started  chan struct{} // Start 被调用后关闭
	startMu  sync.Once
	done     chan struct{} // Start 返回后关闭
}

// AcquireLease 租一个节点号，等到可以安全发号（本机时钟超过上一任的上界加 Guard）后返回。
//
// ctx 约束整个过程。返回之后调用方要把 Lease 交给 app.Run（或者自己调用 Start），否则一个租期之后发号会停。
// 没有交给 app.Run 就退出时调用 Stop 让出，免得这个号白占一个租期。
func AcquireLease(ctx context.Context, cfg LeaseConfig) (*Lease, error) {
	if cfg.Store == nil {
		return nil, errors.New("idgen: lease store is required")
	}
	cfg.TTL = cmp.Or(cfg.TTL, DefaultLeaseTTL)
	cfg.RenewInterval = cmp.Or(cfg.RenewInterval, cfg.TTL/3)
	cfg.Guard = cmp.Or(cfg.Guard, cfg.RenewInterval)
	if cfg.TTL < 3*time.Millisecond || cfg.RenewInterval <= 0 || cfg.RenewInterval >= cfg.TTL || cfg.Guard < 0 {
		return nil, fmt.Errorf("idgen: invalid lease timing: ttl %v, renew interval %v, guard %v", cfg.TTL, cfg.RenewInterval, cfg.Guard)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Owner == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "unknown"
		}
		cfg.Owner = host + "/" + rand.Text()[:8]
	}

	for range maxAcquireAttempts {
		l, retry, err := acquireOnce(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if !retry {
			return l, nil
		}
	}
	return nil, fmt.Errorf("idgen: could not settle on a worker id after %d attempts", maxAcquireAttempts)
}

// acquireOnce 租一个号并登记第一段上界。retry 为真表示这个号不合适（已经让出），换一个再试。
func acquireOnce(ctx context.Context, cfg LeaseConfig) (l *Lease, retry bool, err error) {
	id, prior, err := cfg.Store.Acquire(ctx, cfg.Owner, cfg.TTL)
	if err != nil {
		return nil, false, fmt.Errorf("idgen: acquire worker id: %w", err)
	}
	if id < 0 || id > MaxWorkerID {
		return nil, false, fmt.Errorf("idgen: store returned worker id %d out of range [0, %d]", id, MaxWorkerID)
	}
	floor := prior + cfg.Guard.Milliseconds()

	// 等本机时钟超过 floor。正常情况下不用等：让出的号、崩溃后过期的号，上界都在一个租期之内，
	// 而存储按“过期最久的优先”给号。要等超过一个租期，说明上一任的时钟比本机快了很多（或者本机慢了很多），
	// 不在这个号上耗着，放回去换一个。上界原样放回：它仍然约束着以后的持有者。
	if wait := time.Duration(floor-nowMs(cfg.Now)+1) * time.Millisecond; wait > 0 {
		if wait > cfg.TTL {
			cfg.Logger.WarnContext(ctx, "worker id is bounded far in the future, trying another",
				"worker_id", id, "wait", wait.String())
			if err := cfg.Store.Release(ctx, id, cfg.Owner, prior); err != nil {
				return nil, false, fmt.Errorf("idgen: release worker id %d: %w", id, err)
			}
			return nil, true, nil
		}
		cfg.Logger.InfoContext(ctx, "waiting for the clock to pass the previous holder's bound",
			"worker_id", id, "wait", wait.String())
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			_ = cfg.Store.Release(context.WithoutCancel(ctx), id, cfg.Owner, prior)
			return nil, false, fmt.Errorf("idgen: waiting to use worker id %d: %w", id, ctx.Err())
		}
	}

	// 登记第一段上界。等待期间租约可能已经过期并被别人租走，那就换一个。
	bound := nowMs(cfg.Now) + cfg.TTL.Milliseconds()
	held, err := cfg.Store.Extend(ctx, id, cfg.Owner, cfg.TTL, bound)
	if err != nil {
		_ = cfg.Store.Release(context.WithoutCancel(ctx), id, cfg.Owner, prior)
		return nil, false, fmt.Errorf("idgen: register bound for worker id %d: %w", id, err)
	}
	if !held {
		return nil, true, nil
	}

	gen, err := New(id, WithClock(cfg.Now, time.Sleep))
	if err != nil {
		return nil, false, err
	}
	gen.leased = true
	gen.floorMs = floor
	gen.boundMs.Store(bound)
	cfg.Logger.InfoContext(ctx, "worker id leased", "worker_id", id, "owner", cfg.Owner, "ttl", cfg.TTL.String())
	return &Lease{
		cfg: cfg, workerID: id, priorMs: prior, gen: gen,
		stop: make(chan struct{}), started: make(chan struct{}), done: make(chan struct{}),
	}, false, nil
}

func nowMs(now func() time.Time) int64 { return now().Sub(Epoch).Milliseconds() }

// Generator 返回用这个节点号发号的 Generator。
func (l *Lease) Generator() *Generator { return l.gen }

// WorkerID 返回租到的节点号。
func (l *Lease) WorkerID() int64 { return l.workerID }

// Name 返回组件名。
func (l *Lease) Name() string { return "idgen-lease" }

// Start 按 RenewInterval 续租，直到 Stop 或 ctx 取消（返回 nil），或者发现节点号被别人租走（返回 ErrLeaseLost，
// 此后 Generator 不再发号；app 会因为组件失败而停止整个进程，重启后重新租）。
//
// 续租因为存储不可用而失败时只记日志并很快重试：已经确认的上界用完之前照常发号，用完之后 Next 返回
// ErrLeaseExpired，存储恢复后的第一次续租让它恢复。只能调用一次。
func (l *Lease) Start(ctx context.Context) error {
	called := false
	l.startMu.Do(func() { called = true; close(l.started) })
	if !called {
		return errors.New("idgen: lease started twice")
	}
	defer close(l.done)

	t := time.NewTimer(l.cfg.RenewInterval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return nil
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		next := l.cfg.RenewInterval
		if err := l.renew(ctx); err != nil {
			if errors.Is(err, ErrLeaseLost) {
				l.gen.boundMs.Store(-1)
				l.cfg.Logger.ErrorContext(ctx, "worker id lease lost, id generation stopped", "worker_id", l.workerID, "owner", l.cfg.Owner)
				return err
			}
			l.cfg.Logger.WarnContext(ctx, "worker id lease renewal failed", "worker_id", l.workerID, "error", err)
			next = min(next, renewRetryInterval)
		}
		t.Reset(next)
	}
}

// renew 续租一次。上界先按发送之前的本机时钟算好，存储确认之后才在本地生效：
// 本地的上界任何时候都不超过存储里已经写入的。结果不明（超时）时本地不动，这是安全的一侧。
func (l *Lease) renew(ctx context.Context) error {
	bound := nowMs(l.cfg.Now) + l.cfg.TTL.Milliseconds()
	ctx, cancel := context.WithTimeout(ctx, l.cfg.RenewInterval)
	defer cancel()
	held, err := l.cfg.Store.Extend(ctx, l.workerID, l.cfg.Owner, l.cfg.TTL, bound)
	if err != nil {
		return err
	}
	if !held {
		return ErrLeaseLost
	}
	// 只升不降：本机时钟回拨时新算的上界可能更小，而存储里保留的是较大的那个。已经结束的（-1）不恢复。
	for {
		cur := l.gen.boundMs.Load()
		if cur < 0 || bound <= cur || l.gen.boundMs.CompareAndSwap(cur, bound) {
			return nil
		}
	}
}

// Stop 停止续租、停止发号并让出节点号。可以重复调用，也可以在没有 Start 的情况下调用。
//
// 让出时把上界改成实际发过的最大毫秒时间，下一任不用白等到登记的上界。
func (l *Lease) Stop(ctx context.Context) error {
	var err error
	l.stopOnce.Do(func() {
		close(l.stop)
		select {
		case <-l.started:
			// 等续租循环退出：让出之后不能再有一次续租把上界抬回去（虽然那次续租会因为不是持有者而失败）。
			select {
			case <-l.done:
			case <-ctx.Done():
			}
		default:
		}
		// 先关掉发号，再读最后用到的毫秒：lastIssuedMs 要拿 Generator 的锁，正在进行的 Next 会先完成，
		// 之后的 Next 看到的是 -1。
		l.gen.boundMs.Store(-1)
		bound := max(l.gen.lastIssuedMs(), l.priorMs)
		if rerr := l.cfg.Store.Release(ctx, l.workerID, l.cfg.Owner, bound); rerr != nil {
			err = fmt.Errorf("idgen: release worker id %d: %w", l.workerID, rerr)
			return
		}
		l.cfg.Logger.InfoContext(ctx, "worker id released", "worker_id", l.workerID, "owner", l.cfg.Owner)
	})
	return err
}
