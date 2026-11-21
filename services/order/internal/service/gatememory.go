package service

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/worker"
)

// 本文件是受理端对闸门内存的检查（ADR-0034）。
//
// 闸门的 Redis 不能淘汰任何键（占用被淘汰就是库存凭空变多），所以它写满之后所有写入都失败：
// 不只是新的受理，还有落库的认领、回收任务的归还、分布式锁。3b 第 2 轮里它被写满过一次，
// 5672 个已经受理的订单卡住，不会自己恢复。闸门里会随受理增长的只有受理写下的东西（占用和幂等键），
// 所以在水位到达上限之前先停止受理，剩下的内存留给“把已经受理的订单处理完”的那些写入。
//
// 为什么在进程里采样，而不是放进 Hold 的脚本：脚本在受理的热路径上，每单执行一次；内存的数只有 INFO 给，
// 在脚本里每单生成并解析一段 INFO 不值得。水位离上限留了一成半，而一秒里受理写入的量只有它的几百分之一，
// 一秒前的数足够用。代价是每个实例各采各的，重复的幂等键重放在水位以上也会被拒绝（它本来不写任何东西）。

const (
	// DefaultGateMemoryWatermarkPercent 是停止受理的水位：闸门已用内存占上限的百分比。
	// 留出的一成半（768mb 时约 115 MB）给认领、回收、锁这些不随受理累积的写入，
	// 也盖住采样间隔里还会受理进来的订单（每单几百字节）。
	DefaultGateMemoryWatermarkPercent = 85
	// DefaultGateMemoryInterval 是采样间隔。
	DefaultGateMemoryInterval = time.Second
	// gateMemoryMaxAge 是一次采样的有效期。采不到新的数（闸门不可用、采样的循环停了）超过这么久，
	// 就当作不知道、照常受理：闸门不可用由它的熔断器管，这里不能因为一个旧数永远关着。
	gateMemoryMaxAge = 10 * time.Second
	// gateMemoryProbeTimeout 是一次采样的时限。
	gateMemoryProbeTimeout = 500 * time.Millisecond
)

// errGateFull 是闸门内存到水位时受理的错误。什么都没有发生，稍后用同一个幂等键重试。
// 原因码沿用 STOCK_GATE_UNAVAILABLE（对客户端是同一件事：闸门现在收不了新订单），
// 指标里用单独的结局 gate_memory_full 和闸门故障分开。
var errGateFull = errx.Degraded(errx.ReasonStockGateUnavailable,
	"stock gate has no room for new orders, retry later with the same idempotency key")

// gateMemorySample 是一次采样的结果。
type gateMemorySample struct {
	used, limit int64
	// at 是采到它的时刻（Service 的时钟）。
	at time.Time
}

// full 报告这次采样是否到了水位。没有设上限（limit 为 0）时永远不到。
func (m *gateMemorySample) full(percent int) bool {
	return m.limit > 0 && m.used >= m.limit/100*int64(percent)
}

// gateMemoryFull 报告现在是否应当因为闸门内存而拒绝受理。没有采样、采样过期、没开这项检查时都是 false。
func (s *Service) gateMemoryFull() bool {
	if s.gateMemPercent <= 0 {
		return false
	}
	m := s.gateMem.Load()
	if m == nil || s.clock().Sub(m.at) > gateMemoryMaxAge {
		return false
	}
	return m.full(s.gateMemPercent)
}

// GateMemoryWatcher 周期性地读闸门的内存，存到 Service 上给受理用。实现 app.Component。
// 每个实例都要跑（各自的受理各自判断），不抢 leader。
type GateMemoryWatcher struct {
	svc    *Service
	logger *slog.Logger
	loop   *worker.Loop

	// 下面两个字段只在 RunOnce 里读写，RunOnce 由 loop 串行调用。只用来在状态变化时记一条日志。
	full, failing bool
}

// NewGateMemoryWatcher 返回采样的循环。interval 不大于 0 时用 DefaultGateMemoryInterval。
// Service 没开这项检查（Config.GateMemoryWatermarkPercent 为 0）时它照样采样，只是没有人看。
func NewGateMemoryWatcher(svc *Service, interval time.Duration) (*GateMemoryWatcher, error) {
	w := &GateMemoryWatcher{svc: svc, logger: svc.logger.With("component", "gate-memory")}
	loop, err := worker.New(worker.Config{
		Name:       "gate-memory",
		Interval:   cmp.Or(max(interval, 0), DefaultGateMemoryInterval),
		Logger:     w.logger,
		RunOnStart: true,
	}, func(ctx context.Context) error {
		w.RunOnce(ctx)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("new gate memory watcher: %w", err)
	}
	w.loop = loop
	return w, nil
}

// Name 实现 app.Component。
func (w *GateMemoryWatcher) Name() string { return w.loop.Name() }

// Start 实现 app.Component。
func (w *GateMemoryWatcher) Start(ctx context.Context) error { return w.loop.Start(ctx) }

// Stop 实现 app.Component。
func (w *GateMemoryWatcher) Stop(ctx context.Context) error { return w.loop.Stop(ctx) }

// RunOnce 采样一次。失败时保留上一次的结果，由 gateMemoryMaxAge 让它过期。
//
// 不经过闸门的熔断器：一秒一次的只读调用说明不了闸门对受理是否可用，不该由它打开或关上熔断器；
// 熔断器打开时这里照常去读，读不到就是下面的失败分支。
func (w *GateMemoryWatcher) RunOnce(ctx context.Context) {
	s := w.svc
	pctx, cancel := context.WithTimeout(ctx, gateMemoryProbeTimeout)
	used, limit, err := s.gateMemProbe(pctx)
	cancel()
	if err != nil {
		if ctx.Err() == nil && !w.failing {
			w.failing = true
			w.logger.WarnContext(ctx, "read stock gate memory failed, keeping the last sample until it expires", "error", err)
		}
		return
	}
	w.failing = false
	m := &gateMemorySample{used: used, limit: limit, at: s.clock()}
	s.gateMem.Store(m)
	if s.gateMemPercent <= 0 {
		return
	}
	full := m.full(s.gateMemPercent)
	switch {
	case full && !w.full:
		w.logger.WarnContext(ctx, "stock gate memory reached the watermark, rejecting new orders",
			"used_bytes", used, "limit_bytes", limit, "watermark_percent", s.gateMemPercent)
	case !full && w.full:
		w.logger.InfoContext(ctx, "stock gate memory is below the watermark again, accepting new orders",
			"used_bytes", used, "limit_bytes", limit, "watermark_percent", s.gateMemPercent)
	}
	w.full = full
}
