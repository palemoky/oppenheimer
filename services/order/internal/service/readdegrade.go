package service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/palemoky/oppenheimer/pkg/breaker"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/retry"
	"github.com/palemoky/oppenheimer/pkg/worker"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 本文件是读路径的降级（设计文档 1d 的 6.7、6.8）：从库的健康标记和探测、读己之写的标记经过闸门的熔断器、
// 冷库的熔断器和后台迁移。受理、落库这一侧的降级在 degrade.go。

// 读路径的降级开关。
const (
	// SwitchReplicaRead 关掉之后展示读只读主库。
	SwitchReplicaRead = "replica_read"
	// SwitchArchiveRead 关掉之后不查冷库，表现和冷库的熔断器打开时相同。
	SwitchArchiveRead = "archive_read"
)

// errArchiveUnavailable 是冷库不可用（还没迁移完、熔断器打开、开关关掉、这一跳出错或超时）时对外的错误。
// 降级错误（FAILED_PRECONDITION），不用 UNAVAILABLE 的理由见 errx.Degraded：冷库坏了不能让调用方的边车把整个 order 摘掉。
var errArchiveUnavailable = errx.Degraded(errx.ReasonArchiveUnavailable,
	"archived orders are temporarily unavailable, retry later")

// isArchiveUnavailable 判断 err 是不是 errArchiveUnavailable（按原因码）。
func isArchiveUnavailable(err error) bool { return errx.Reason(err) == errx.ReasonArchiveUnavailable }

// ---------- 从库的健康标记 ----------

// 从库探测的默认参数（设计文档 1d 的 6.7）。
const (
	DefaultReplicaProbeInterval = time.Second
	// DefaultReplicaMaxLag 是从库允许的最大延迟。ReadWindow 下它必须小于 pin 的时长（cmd/order 检查）：
	// 延迟超过 pin 的时长时，pin 到期之后的读会读到自己刚写的数据的旧版本。
	DefaultReplicaMaxLag = time.Second
)

// replicaRecoverPasses 是从不健康恢复成健康需要的连续通过次数，避免延迟在阈值附近时来回切换。
const replicaRecoverPasses = 2

// replicaProbeSlack 是一次探测在最大延迟之外多给的时间：读主库位置一次往返，等待超时之后返回结果再一次往返。
const replicaProbeSlack = time.Second

// replicaHealth 是一个订单库的从库的健康标记。并发安全：探测和请求路径都会改它。
//
// 初始是不健康：启动时还不知道从库的状况（可能连不上，设计文档 1d 的 6.10），按“任何不确定都读主库”处理，
// 探测连续通过 replicaRecoverPasses 次之后才用从库。
type replicaHealth struct {
	// healthy 给请求路径读，不加锁。
	healthy atomic.Bool

	mu sync.Mutex
	// passes 是不健康期间连续通过的探测次数。
	passes int
	// reason 是最近一次被标记为不健康的原因，给状态展示。
	reason string
}

func newReplicaHealth() *replicaHealth {
	return &replicaHealth{reason: "not probed yet"}
}

// pass 记一次通过的探测，返回这一次是否让它从不健康恢复成了健康。
func (h *replicaHealth) pass() (recovered bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.healthy.Load() {
		return false
	}
	h.passes++
	if h.passes < replicaRecoverPasses {
		return false
	}
	h.passes = 0
	h.reason = ""
	h.healthy.Store(true)
	return true
}

// fail 把它标记为不健康，返回这一次是否让它从健康变成了不健康。reason 总会更新（状态展示看最新的原因）。
func (h *replicaHealth) fail(reason string) (changed bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.passes = 0
	h.reason = reason
	return h.healthy.Swap(false)
}

// describe 返回给状态展示的一段说明。
func (h *replicaHealth) describe() (healthy bool, detail string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.healthy.Load() {
		return true, "healthy"
	}
	return false, fmt.Sprintf("unhealthy(%s,passes=%d/%d)", h.reason, h.passes, replicaRecoverPasses)
}

// replicaHealthy 报告第 db 个库的从库现在能不能读。没有配置从库时返回 false。
func (s *Service) replicaHealthy(db int) bool {
	return db >= 0 && db < len(s.replicas) && s.replicas[db].healthy.Load()
}

// markReplicaUnhealthy 立刻把第 db 个库的从库标记为不健康，不等下一次探测（请求路径上读从库出错时调用）。
// 只在从健康变成不健康时记一条 WARN：从库坏着的时候并发的请求会同时失败，逐条记会刷屏。
func (s *Service) markReplicaUnhealthy(ctx context.Context, db int, err error) {
	if db < 0 || db >= len(s.replicas) {
		return
	}
	if s.replicas[db].fail("read failed: " + err.Error()) {
		s.logger.WarnContext(ctx, "replica marked unhealthy by a failed read, reading primary", "db", db, "error", err)
	}
}

// marksDistrustWindow 是闸门的熔断器恢复之后不信任读己之写标记的时长（设计文档 1d 的 6.4、6.7）。
//
// 熔断器打开期间 pin 和 stamp 都被跳过，那段时间里的写入没有标记。恢复之后，一个没有标记的订单
// 分不清是“很久没写过”还是“故障期间写过”，要等那些写入本来该有的标记全部到期，标记才重新可信：
// ReadWindow 只用 pin，等 pin 的时长；ReadGTID 还用位置标记，等两者中较长的一个。
func (s *Service) marksDistrustWindow() time.Duration {
	if s.readMode == ReadGTID {
		return max(s.pinTTL, s.gtidTTL)
	}
	return s.pinTTL
}

// replicaStatus 是从库读的降级状态，给 GET /degrade 用。
func (s *Service) replicaStatus() degrade.Status {
	if len(s.replicas) == 0 {
		return degrade.Status{Detail: "no replicas configured, display reads use the primary"}
	}
	var (
		parts    []string
		degraded bool
	)
	parts = append(parts, "read_mode="+s.readMode.String())
	for i, h := range s.replicas {
		ok, detail := h.describe()
		degraded = degraded || !ok
		parts = append(parts, "db_"+strconv.Itoa(i)+"="+detail)
	}
	if s.readMode != ReadPrimary && s.gateRecoveredWithin(s.marksDistrustWindow()) {
		degraded = true
		parts = append(parts, "marks=distrusted(stock gate recovered recently)")
	}
	return degrade.Status{Degraded: degraded, Detail: strings.Join(parts, " ")}
}

// ---------- 从库的探测 ----------

// replicaProbeTarget 是探测用到的两个查询。*store.Store 满足它；测试里替换成能模拟“落后”的实现。
type replicaProbeTarget interface {
	PrimaryPositionOfDB(ctx context.Context, db int) (store.Position, error)
	WaitReplicaOfDB(ctx context.Context, db int, gtid string, timeout time.Duration) (bool, error)
}

// ReplicaProber 定期探测每个订单库的从库，维护它们的健康标记（设计文档 1d 的 6.7）。实现 app.Component。
//
// 每一轮对每个库：在主库上读当前的 GTID 集合，在从库上 WAIT_FOR_EXECUTED_GTID_SET(这个集合, 最大延迟)。
// 在最大延迟内追上了是通过；没追上、出错、超时是不通过。量的是“主库刚才的位置，从库多久能追上”，
// 就是读者关心的那个量；不用 Seconds_Behind_Source，理由见设计文档。
//
// 每个库一个独立的循环：一个从库挂住时，它那个库的探测最多等 maxLag + replicaProbeSlack，
// 不会拖住别的库的探测。
type ReplicaProber struct {
	svc    *Service
	target replicaProbeTarget
	maxLag time.Duration
	loops  []*worker.Loop
	// probed[db] 表示第 db 个库已经有过一次探测结果，用来让启动后第一次的不通过也记一条日志。
	probed []atomic.Bool
}

// ReplicaProber 返回本服务从库的探测器。没有配置从库时返回错误（cmd/order 只在有从库时创建它）。
// interval、maxLag 不大于 0 时用默认值。
func (s *Service) ReplicaProber(interval, maxLag time.Duration) (*ReplicaProber, error) {
	if len(s.replicas) == 0 {
		return nil, errors.New("service: replica prober needs replicas in the store")
	}
	if interval <= 0 {
		interval = DefaultReplicaProbeInterval
	}
	if maxLag <= 0 {
		maxLag = DefaultReplicaMaxLag
	}
	p := &ReplicaProber{svc: s, target: s.store, maxLag: maxLag, probed: make([]atomic.Bool, len(s.replicas))}
	for db := range s.replicas {
		loop, err := worker.New(worker.Config{
			Name:       "replica-prober-db-" + strconv.Itoa(db),
			Interval:   interval,
			Logger:     s.logger,
			RunOnStart: true,
		}, func(ctx context.Context) error {
			p.ProbeOnce(ctx, db)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("new replica prober: %w", err)
		}
		p.loops = append(p.loops, loop)
	}
	return p, nil
}

// Name 实现 app.Component。
func (p *ReplicaProber) Name() string { return "order-replica-prober" }

// Start 实现 app.Component：每个库一个循环，直到 Stop 或 ctx 取消。
func (p *ReplicaProber) Start(ctx context.Context) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, l := range p.loops {
		wg.Go(func() {
			if err := l.Start(ctx); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Stop 实现 app.Component：停下所有循环，等正在进行的探测结束（每次最多 maxLag + replicaProbeSlack）。
func (p *ReplicaProber) Stop(ctx context.Context) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for _, l := range p.loops {
		wg.Go(func() {
			if err := l.Stop(ctx); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// ProbeOnce 探测第 db 个库的从库一次并更新它的健康标记。
func (p *ReplicaProber) ProbeOnce(ctx context.Context, db int) {
	s := p.svc
	pctx, cancel := context.WithTimeout(ctx, p.maxLag+replicaProbeSlack)
	defer cancel()
	var caught bool
	pos, err := p.target.PrimaryPositionOfDB(pctx, db)
	if err == nil {
		caught, err = p.target.WaitReplicaOfDB(pctx, db, pos.GTID, p.maxLag)
	}
	if ctx.Err() != nil {
		// 进程在停止，这一次说明不了从库的状况。
		return
	}
	first := !p.probed[db].Swap(true)
	h := s.replicas[db]
	switch {
	case err != nil:
		// 包括读主库位置失败：那时同样无法证明从库是新的，按“不确定就读主库”处理。
		if h.fail("probe failed: "+err.Error()) || first {
			s.logger.WarnContext(ctx, "replica probe failed, reading primary", "db", db, "error", err)
		}
	case !caught:
		if h.fail("lag over "+p.maxLag.String()) || first {
			s.logger.WarnContext(ctx, "replica lags behind, reading primary", "db", db, "max_lag", p.maxLag.String())
		}
	default:
		if h.pass() {
			s.logger.InfoContext(ctx, "replica healthy, reading replica again", "db", db)
		}
	}
}

// ---------- 读己之写的标记 ----------

// markCall 是对读己之写标记的一次调用（和闸门是同一个 Redis 实例），经过闸门的熔断器，时限 markTimeout。
//
// 熔断器打开时不访问 Redis，立刻返回一个 isBreakerOpen 为真的错误；调用方按“读不到标记”处理（读主库）
// 或跳过（pin、stamp），并且不记日志：打开和恢复各有一条日志（breakerChanged），逐个请求记会刷屏。
// 标记的失败同样喂给闸门的熔断器：两者在同一个 Redis 上，标记读写失败就是闸门的 Redis 有问题。
func (s *Service) markCall(ctx context.Context, fn func(context.Context) error) error {
	done, err := s.gateBreaker.Allow()
	if err != nil {
		return fmt.Errorf("read marks: %w", err)
	}
	mctx, cancel := context.WithTimeout(ctx, markTimeout)
	err = fn(mctx)
	cancel()
	done(depOutcome(ctx, err))
	return err
}

// isServerAnswer 判断 err 是不是 MySQL 明确回答的错误（例如标记里的 GTID 集合格式不对）。
// 这类错误说明从库是通的，不是从库的问题；其余错误（连不上、挂住、这一跳超时）才是。
func isServerAnswer(err error) bool {
	_, ok := errors.AsType[*mysql.MySQLError](err)
	return ok
}

// ---------- 冷库 ----------

// archiveUsable 报告冷库现在能不能查（展示读）：配置了、迁移完了、开关没关。熔断器另外判断。
func (s *Service) archiveUsable() bool {
	return s.cold != nil && s.coldReady.Load() && !s.swArchive.Off()
}

// coldRead 是展示读（GetOrder、lookup、ListUserOrders）对冷库的一次读：先看迁移和开关，再过熔断器，
// 再占冷库的名额，最多 coldTimeout。
//
// 返回值：
//   - errArchiveUnavailable：迁移没完成、开关关掉、熔断器没有放行。冷库没有被访问。
//   - errReadBusy：名额满了。不是冷库的失败，不计入熔断器（Ignored）。
//   - store.ErrNotFound：冷库里没有。是业务结果，算成功。
//   - 其余错误原样返回，由调用方用 archiveUnavailable 区分“冷库坏了”和“调用方取消、超时”。
//
// 熔断器在占名额之前问：打开时不该占名额，更不该排队等名额。
func (s *Service) coldRead(ctx context.Context, fn func(context.Context) error) error {
	if !s.archiveUsable() {
		return errArchiveUnavailable
	}
	done, err := s.coldBreaker.Allow()
	if err != nil {
		return errArchiveUnavailable
	}
	release, err := acquire(s.limits.cold)
	if err != nil {
		done(breaker.Ignored)
		return err
	}
	defer release()
	err = hop(ctx, coldTimeout, fn)
	if errors.Is(err, store.ErrNotFound) {
		done(breaker.Success)
		return err
	}
	done(depOutcome(ctx, err))
	return err
}

// archiveUnavailable 判断 coldRead 返回的错误是不是“冷库不可用”，是的话这一次要按降级处理。
// ctx 是判断“调用方取消、超时”用的 ctx（和 depOutcome 一样，看 ctx 的状态，不看错误的类型）：
//   - errArchiveUnavailable：是。
//   - 名额满（errReadBusy）、ctx 已经结束：不是，调用方按原来的方式映射。
//   - 其余（冷库这一跳超时或出错而 ctx 还有效）：是，记一条 WARN。熔断器打开之后冷库不再被访问，
//     这条日志也就不再出现，不会刷屏。
func (s *Service) archiveUnavailable(ctx context.Context, op string, err error, attrs ...any) bool {
	switch {
	case isArchiveUnavailable(err):
		return true
	case errors.Is(err, errReadBusy), ctx.Err() != nil:
		return false
	}
	s.logger.WarnContext(ctx, "archive read failed, degrading", append(attrs, "op", op, "error", err)...)
	return true
}

// coldDecisionRead 是支付结果路径对冷库的一次读（archivedReply）。和 coldRead 不同：
//   - 不看开关和迁移状态：开关管的是展示读；没迁移完时照常去查，查不到表就是一次失败，消费者重试。
//   - 熔断器没有放行时返回一个包装了 breaker.ErrOpen 的普通错误，不是 errArchiveUnavailable，
//     也绝不是 store.ErrNotFound：调用方只把 ErrNotFound 当作“冷库里没有”，其余错误一律返回，消费者重试。
//   - 不占展示读的名额：冷库的连接池给它留了位置（DefaultArchiveMaxConcurrency）。
//
// 加 coldTimeout 是为了让冷库挂住时这次处理在 1 秒内失败，而不是在热库的事务里等满事件的时限（10 秒），
// 并且让熔断器数得到这次失败。
func (s *Service) coldDecisionRead(ctx context.Context, fn func(context.Context) error) error {
	done, err := s.coldBreaker.Allow()
	if err != nil {
		return fmt.Errorf("archive: %w", err)
	}
	err = hop(ctx, coldTimeout, fn)
	if errors.Is(err, store.ErrNotFound) {
		done(breaker.Success)
		return err
	}
	done(depOutcome(ctx, err))
	return err
}

// archiveStatus 是冷库读的降级状态，给 GET /degrade 用。
func (s *Service) archiveStatus() degrade.Status {
	if s.cold == nil {
		return degrade.Status{Detail: "no archive configured"}
	}
	st := s.coldBreaker.Stats()
	ready := s.coldReady.Load()
	return degrade.Status{
		Degraded: !ready || st.State != breaker.Closed,
		Detail: fmt.Sprintf("migrated=%t breaker=%s(opens=%d,rejected=%d)",
			ready, st.State, st.Opens, st.Rejected),
	}
}

// ---------- 冷库的后台迁移 ----------

// 冷库迁移的重试间隔：从 1 秒开始翻倍，最多 30 秒，带抖动。
var archiveMigrateBackoff = retry.Policy{Initial: time.Second, Max: 30 * time.Second}

// archiveMigrateTimeout 是一次迁移的时限。迁移要先拿 MySQL 的迁移锁（mysqlx 里最多等 60 秒）。
const archiveMigrateTimeout = 2 * time.Minute

// ArchiveMigrator 在后台给冷库建表，失败了隔一段时间再试，直到成功（设计文档 1d 的 6.8、6.10）。实现 app.Component。
//
// 冷库是可选的依赖，启动时连不上不该让服务起不来。成功之前冷库的展示读立刻返回 errArchiveUnavailable，
// 不访问冷库；成功之后才打开。成功之后 Start 返回，组件结束。
type ArchiveMigrator struct {
	svc     *Service
	migrate func(context.Context) error

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
	started  atomic.Bool
}

// ArchiveMigrator 返回冷库的迁移组件。没有冷库或者不需要迁移（Deps.ColdMigrate 为空）时返回 nil。
func (s *Service) ArchiveMigrator() *ArchiveMigrator {
	if s.cold == nil || s.coldMigrate == nil {
		return nil
	}
	return &ArchiveMigrator{svc: s, migrate: s.coldMigrate, stop: make(chan struct{}), done: make(chan struct{})}
}

// Name 实现 app.Component。
func (m *ArchiveMigrator) Name() string { return "order-archive-migrator" }

// Start 实现 app.Component：重试迁移直到成功、Stop 或 ctx 取消。
func (m *ArchiveMigrator) Start(ctx context.Context) error {
	if m.started.Swap(true) {
		return errors.New("archive migrator: already started")
	}
	defer close(m.done)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Stop 时打断正在进行的迁移：它可能正连着一个不响应的地址。
	go func() {
		select {
		case <-m.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	s := m.svc
	for attempt := 1; ; attempt++ {
		err := m.RunOnce(ctx)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		wait := retry.Backoff(archiveMigrateBackoff, attempt)
		s.logger.WarnContext(ctx, "archive migration failed, archived orders unavailable until it succeeds",
			"attempt", attempt, "retry_in", wait.String(), "error", err)
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil
		}
	}
}

// RunOnce 做一次迁移，成功时把冷库标记为可用。
func (m *ArchiveMigrator) RunOnce(ctx context.Context) error {
	mctx, cancel := context.WithTimeout(ctx, archiveMigrateTimeout)
	defer cancel()
	if err := m.migrate(mctx); err != nil {
		return fmt.Errorf("migrate archive: %w", err)
	}
	m.svc.coldReady.Store(true)
	m.svc.logger.InfoContext(ctx, "archive migrated, archived orders available")
	return nil
}

// Stop 实现 app.Component。
func (m *ArchiveMigrator) Stop(ctx context.Context) error {
	m.stopOnce.Do(func() { close(m.stop) })
	if !m.started.Load() {
		return nil
	}
	select {
	case <-m.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("archive migrator: stop: %w", ctx.Err())
	}
}
