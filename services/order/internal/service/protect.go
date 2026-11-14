package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/palemoky/oppenheimer/pkg/bulkhead"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
)

// 本文件是 order 进程内部的服务保护（设计文档 1d 的 5.6、5.7、5.9）：按跳的时限、展示读对数据库连接的并发上限、
// 调用方取消和超时时的错误映射。服务之间的并发上限、摘除在 Envoy 边车上，这里只做代理看不到的部分。

// 按跳的时限（设计文档 1d 的 5.6）。每一跳给下一跳的时限比自己剩下的短，下一跳超时之后自己还有时间
// 返回明确的错误或者走降级；都用 context.WithTimeout 取“这个数”和“上游剩下的时间”中更早的一个。
const (
	// gateTimeout 是对库存闸门（Redis）一次调用的时限。闸门的命令是几十微秒的 Lua 脚本，正常往返在 1 毫秒以内；
	// 300 毫秒容得下一次 TCP 重传（已建好的连接约 200 毫秒，见 redisx.DefaultPoolSize 的注释），
	// 再长就说明 Redis 卡住了，与其拖住请求不如快速失败：受理返回 UNAVAILABLE 让客户端带幂等键重试，
	// 查询退回只读订单行。
	gateTimeout = 300 * time.Millisecond
	// coldTimeout 是读冷库一次的时限。1c 留下的：GetOrder 查冷库原来没有自己的时限，冷库卡住时旧订单的查询
	// 一直等到调用方的截止时间。冷库只承接旧订单，配合 archiveMaxConcurrency 的上限，
	// 冷库卡住时最多占住几个协程、各等 1 秒，不影响热订单。
	coldTimeout = time.Second
	// replicaTimeout 是展示读读从库一次的时限。从库超时或出错时退回主库，所以它必须给主库留出时间：
	// 网关给下游的总预算是 3 秒，从库最多用 500 毫秒，剩下的足够再读一次主库。
	replicaTimeout = 500 * time.Millisecond
)

// errHopTimeout 标记“这一跳自己设的时限到了”（而调用方的 ctx 还有效）。internal 据此答 UNAVAILABLE：
// 是下游慢，不是调用方的问题，也不是本服务的内部错误。
var errHopTimeout = errors.New("hop timeout")

// hopContext 返回一个最多 d 的子 ctx，到期的原因（context.Cause）是 errHopTimeout。
// 调用方的截止时间更早时子 ctx 跟着调用方到期，原因是调用方的，hopErr 不会把它标成这一跳的超时。
func hopContext(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(ctx, d, errHopTimeout)
}

// hopErr 在 hctx 因为这一跳自己的时限到期时给 err 加上 errHopTimeout。
//
// 不能只看 err 是不是 context.DeadlineExceeded：驱动在超时后可能返回别的错误（Redis 客户端返回的是网络层的
// i/o timeout，MySQL 驱动在连接被关掉时可能返回 invalid connection），按 ctx 的状态判断才可靠。
func hopErr(hctx context.Context, err error) error {
	if err == nil || errors.Is(err, errHopTimeout) {
		return err
	}
	if errors.Is(context.Cause(hctx), errHopTimeout) {
		return fmt.Errorf("%w: %w", errHopTimeout, err)
	}
	return err
}

// hop 在最多 d 的子 ctx 里执行 fn，这一跳的时限到期导致的错误带上 errHopTimeout。
func hop(ctx context.Context, d time.Duration, fn func(context.Context) error) error {
	hctx, cancel := hopContext(ctx, d)
	defer cancel()
	return hopErr(hctx, fn(hctx))
}

// gateCall 是对库存闸门的一次调用，统一加 gateTimeout。所有闸门调用都经过它，不在调用点各写一遍。
func (s *Service) gateCall(ctx context.Context, fn func(context.Context) error) error {
	return s.gateCallWithin(ctx, gateTimeout, fn)
}

// gateCallWithin 是时限为 d 的一次闸门调用，经过闸门的熔断器（设计文档 1d 的 6.4）。
//
// 熔断器打开时不访问 Redis，立刻返回一个 isBreakerOpen 为真的错误：Redis 坏着的时候，
// 受理、查询、落库不用每次都等满时限才知道。闸门的方法把业务结果（库存不足、占用不存在）放在返回值里，
// 返回错误就是 Redis 或脚本出了问题，所以除了调用方自己取消之外的错误都算失败。
func (s *Service) gateCallWithin(ctx context.Context, d time.Duration, fn func(context.Context) error) error {
	done, err := s.gateBreaker.Allow()
	if err != nil {
		return fmt.Errorf("stock gate: %w", err)
	}
	err = hop(ctx, d, fn)
	done(depOutcome(ctx, err))
	return err
}

// gateGet、gateHold 是最常用的两个闸门调用的简写。
func (s *Service) gateGet(ctx context.Context, orderID int64) (hold stockgate.HoldInfo, held bool, err error) {
	err = s.gateCall(ctx, func(ctx context.Context) error {
		var err error
		hold, held, err = s.gate.Get(ctx, orderID)
		return err
	})
	return hold, held, err
}

func (s *Service) gateHold(ctx context.Context, req stockgate.HoldRequest) (res stockgate.HoldResult, err error) {
	err = s.gateCall(ctx, func(ctx context.Context) error {
		var err error
		res, err = s.gate.Hold(ctx, req)
		return err
	})
	return res, err
}

// ---------- 调用方取消、超时的错误映射 ----------

// ctxStatusError 是“调用方取消 / 调用方的截止时间到了”对外的 gRPC 状态，同时 Unwrap 到对应的 context 错误。
//
// 为什么要能 Unwrap：查询缓存用 singleflight 合并同一个订单的回源，发起回源的调用被取消时，
// 搭便车的调用会按 errors.Is(err, context.Canceled) 认出“这是别人的取消”而自己重新回源（pkg/cache 的 Get）。
// 只返回一个普通的 status 错误的话，它认不出来，会把别人的取消当成自己的结果。
type ctxStatusError struct {
	st    *status.Status
	cause error
}

func (e *ctxStatusError) Error() string              { return e.st.Err().Error() }
func (e *ctxStatusError) GRPCStatus() *status.Status { return e.st }
func (e *ctxStatusError) Unwrap() error              { return e.cause }

// errDependencyTimeout 是这一跳自己设的时限到期时对外的错误：下游慢，稍后重试。
var errDependencyTimeout = status.Error(codes.Unavailable, "a dependency timed out, retry later")

// contextError 判断 err 是不是由超时或取消造成的，是就返回对外的错误并按级别记日志，否则返回 nil。
//
// 判断的依据和先后：
//  1. 请求自己的 ctx 被取消（调用方断开、上游放弃）：CANCELED，只记 DEBUG。调用方已经不在了，
//     这不是故障，记成 ERROR 会在压测和客户端频繁取消时淹没真正的告警。
//  2. 请求自己的 ctx 到了截止时间：DEADLINE_EXCEEDED，记 WARN。截止时间是调用方给的（或者本服务对整个请求
//     设的总时限），慢的是这条链路整体，值得注意但不是本服务的内部错误。
//  3. 请求的 ctx 还有效，而错误是超时：这一跳自己设的更短的时限到了（读冷库、读闸门……）。下游慢，
//     答 UNAVAILABLE 让调用方稍后重试，记 WARN。
//
// 先看 ctx 再看 err：ctx 已经结束时，不管下游返回的是什么错误，根因都是 ctx 结束。
func (s *Service) contextError(ctx context.Context, op string, err error, attrs ...any) error {
	ctxErr := ctx.Err()
	switch {
	case errors.Is(ctxErr, context.Canceled):
		s.logger.DebugContext(ctx, "request canceled", append(attrs, "op", op, "error", err)...)
		return &ctxStatusError{st: status.New(codes.Canceled, "request canceled"), cause: ctxErr}
	case errors.Is(ctxErr, context.DeadlineExceeded):
		s.logger.WarnContext(ctx, "request deadline exceeded", append(attrs, "op", op, "error", err)...)
		return &ctxStatusError{st: status.New(codes.DeadlineExceeded, "deadline exceeded"), cause: ctxErr}
	case errors.Is(err, errHopTimeout), errors.Is(err, context.DeadlineExceeded):
		s.logger.WarnContext(ctx, "dependency timed out", append(attrs, "op", op, "error", err)...)
		return errDependencyTimeout
	}
	return nil
}

// ---------- 展示读的并发上限 ----------

// ReasonReadBusy 表示订单查询的并发已满（RESOURCE_EXHAUSTED，网关映射为 HTTP 429）。什么都没有发生，稍后重试。
//
// 只在 order 里产生，网关和 payment 原样透传，不按它分支，所以没有放进 pkg/errx。
const ReasonReadBusy = "READ_BUSY"

// errReadBusy 是展示读拿不到名额时的错误。是有意的拒绝、不是故障：不记 ERROR，也不逐条记日志
// （满的时候每秒成百上千条），由 ReadLimitReporter 周期性汇总。
var errReadBusy = errx.New(codes.ResourceExhausted, ReasonReadBusy, "too many concurrent order queries, retry later")

// 并发上限的默认值（设计文档 1d 的 5.7），cmd/order 的环境变量没设时用它们。
const (
	// DefaultReadMaxConcurrency 是每个订单库上展示读的并发上限。订单库连接池默认 25，给落库、支付结果、
	// 取消、关单这些写路径留 9 个：查询再多也只能占满 16 个，写路径总有连接可用。
	DefaultReadMaxConcurrency = 16
	// DefaultGateFailureSpan 是闸门的熔断器打开之前连续失败至少要持续的时间。
	//
	// 闸门的调用方并发很高（每秒上千次受理加落库），一次停顿会让在途的十几个调用同时超时。压测里量到的停顿
	// 是 300 多毫秒一次（设计文档 1d 的 8.18），1 秒足够把它和“Redis 真的不可用”分开，又远小于 5 秒的冷却。
	DefaultGateFailureSpan = time.Second
	// DefaultReadGateMaxConcurrency 是展示读同时访问库存闸门的上限。闸门的连接池（REDIS_POOL_SIZE）默认 128，
	// 查询最多占其中 16 个，其余留给受理、落库和回收。一次读占用不到 1 毫秒，16 个名额每秒能过一万多次查询；
	// 占不到名额说明查询的量已经远超这个数。
	DefaultReadGateMaxConcurrency = 16
	// DefaultArchiveMaxConcurrency 是冷库读的并发上限。冷库连接池是 10，留 2 个给支付结果在冷库上的复查。
	DefaultArchiveMaxConcurrency = 8
)

// readLimits 是展示读的名额：每个订单库一个（下标是库号），冷库一个。nil 的 *Bulkhead 不限制。
type readLimits struct {
	dbs  []*bulkhead.Bulkhead
	cold *bulkhead.Bulkhead
	// gate 是展示读同时读闸门占用的名额，见 fetchHold。
	gate *bulkhead.Bulkhead
}

// newReadLimits 按配置建名额。perDB、cold 为 0 表示不限制。
func newReadLimits(dbs, perDB, cold int) (readLimits, error) {
	var l readLimits
	if perDB < 0 || cold < 0 {
		return l, fmt.Errorf("service: read concurrency limits must not be negative, got %d and %d", perDB, cold)
	}
	if perDB > 0 {
		l.dbs = make([]*bulkhead.Bulkhead, dbs)
		for i := range l.dbs {
			b, err := bulkhead.New(perDB)
			if err != nil {
				return l, fmt.Errorf("service: read limit of db %d: %w", i, err)
			}
			l.dbs[i] = b
		}
	}
	if cold > 0 {
		b, err := bulkhead.New(cold)
		if err != nil {
			return l, fmt.Errorf("service: archive read limit: %w", err)
		}
		l.cold = b
	}
	return l, nil
}

// db 返回第 db 个订单库的名额，没有配置上限时返回 nil（不限制）。
func (l readLimits) db(db int) *bulkhead.Bulkhead {
	if db < 0 || db >= len(l.dbs) {
		return nil
	}
	return l.dbs[db]
}

// acquire 占 b 的一个名额，满了返回 errReadBusy。
func acquire(b *bulkhead.Bulkhead) (release func(), err error) {
	release, err = b.Acquire()
	if errors.Is(err, bulkhead.ErrFull) {
		return nil, errReadBusy
	}
	if err != nil {
		return nil, fmt.Errorf("acquire read slot: %w", err)
	}
	return release, nil
}

// ---------- 拒绝次数的汇总日志 ----------

// DefaultReadLimitReportInterval 是汇总日志的间隔。
const DefaultReadLimitReportInterval = 10 * time.Second

// ReadLimitReporter 每隔一段时间，如果这段时间里有查询因为并发上限被拒绝，记一条 WARN（按库分开）。
// 这段时间没有拒绝时不输出。实现 app.Component。
type ReadLimitReporter struct {
	limits   readLimits
	logger   *slog.Logger
	interval time.Duration

	// mu 保护 last：Flush 可能被 Start 的循环和停止时并发调用。
	mu   sync.Mutex
	last []int64 // 上一次汇总时各名额的累计拒绝数，最后一个是冷库

	stopOnce sync.Once
	stop     chan struct{}
}

// ReadLimitReporter 返回本服务名额的汇总器。interval 不大于 0 时用 DefaultReadLimitReportInterval。
func (s *Service) ReadLimitReporter(interval time.Duration) *ReadLimitReporter {
	if interval <= 0 {
		interval = DefaultReadLimitReportInterval
	}
	return &ReadLimitReporter{
		limits: s.limits, logger: s.logger, interval: interval,
		last: make([]int64, len(s.limits.dbs)+1), stop: make(chan struct{}),
	}
}

// Name 实现 app.Component。
func (r *ReadLimitReporter) Name() string { return "order-read-limit-reporter" }

// Start 实现 app.Component：定期汇总，直到 Stop 或 ctx 取消。停止前把最后一段输出掉。
func (r *ReadLimitReporter) Start(ctx context.Context) error {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			r.Flush(ctx)
		case <-r.stop:
			r.Flush(ctx)
			return nil
		case <-ctx.Done():
			r.Flush(ctx)
			return nil
		}
	}
}

// Stop 实现 app.Component。
func (r *ReadLimitReporter) Stop(context.Context) error {
	r.stopOnce.Do(func() { close(r.stop) })
	return nil
}

// Flush 输出上次汇总以来的拒绝次数；没有拒绝时不输出。
func (r *ReadLimitReporter) Flush(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur := make([]int64, 0, len(r.last))
	for _, b := range r.limits.dbs {
		cur = append(cur, b.Rejected())
	}
	cur = append(cur, r.limits.cold.Rejected())

	attrs := []any{"interval", r.interval.String()}
	var total int64
	for i, n := range cur {
		d := n - r.last[i]
		if d == 0 {
			continue
		}
		total += d
		name := "archive"
		if i < len(r.limits.dbs) {
			name = "db_" + strconv.Itoa(i)
		}
		attrs = append(attrs, "rejected_"+name, d)
	}
	r.last = cur
	if total == 0 {
		return
	}
	r.logger.WarnContext(ctx, "order queries rejected by the concurrency limit", append(attrs, "rejected", total)...)
}
