// Package worker 提供周期任务组件 Loop，实现 app.Component。
//
// 典型用途是定时扫描（超时订单关闭、本地消息表投递等）。
// 配合 Guard 可以做到“只在持有分布式锁的实例上运行”。
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// guardTimeout 是一次 Guard 调用的时限。
const guardTimeout = time.Second

// Guard 决定这一轮是否应该运行。用于“只在持有分布式锁时运行”。
// 返回 false 表示跳过这一轮（不算错误）；返回 error 表示无法判断，也跳过并记 warn 日志。
type Guard interface {
	Allow(ctx context.Context) (bool, error)
}

// Config 是 Loop 的配置。
type Config struct {
	// Name 是组件名，必填。
	Name string
	// Interval 是两轮之间的间隔（上一轮结束到下一轮开始），必填且 > 0。
	// 从结束时刻算起，是为了保证慢任务不会让两轮首尾相接地连续运行。
	Interval time.Duration
	// Logger 为 nil 时用 slog.Default()。
	Logger *slog.Logger
	// Guard 可选。
	Guard Guard
	// RunOnStart 为 true 时 Start 后立即跑第一轮，否则先等一个 Interval。
	RunOnStart bool
}

// Loop 按固定间隔调用 fn，同一时刻最多只有一轮在运行。
type Loop struct {
	cfg    Config
	fn     func(ctx context.Context) error
	logger *slog.Logger

	kick     chan struct{} // 容量 1：多次 Kick 在下一轮开始前自然合并
	stopCh   chan struct{}
	stopOnce sync.Once

	mu      sync.Mutex
	started bool
	done    chan struct{} // Start 返回时关闭
}

// New 校验配置并创建 Loop。
func New(cfg Config, fn func(ctx context.Context) error) (*Loop, error) {
	if cfg.Name == "" {
		return nil, errors.New("worker: name is required")
	}
	if cfg.Interval <= 0 {
		return nil, fmt.Errorf("worker %s: interval must be > 0, got %v", cfg.Name, cfg.Interval)
	}
	if fn == nil {
		return nil, fmt.Errorf("worker %s: fn is required", cfg.Name)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Loop{
		cfg:    cfg,
		fn:     fn,
		logger: logger,
		kick:   make(chan struct{}, 1),
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}, nil
}

// Name 返回组件名。
func (l *Loop) Name() string { return l.cfg.Name }

// Kick 让下一轮立即开始，不必等 Interval。
// 不阻塞；多次调用在下一轮开始前合并成一次；一轮正在运行时调用，会在它结束后立即再跑一轮。
// Start 之前调用的 Kick 会被丢弃，Stop 之后调用无害。
func (l *Loop) Kick() {
	select {
	case l.kick <- struct{}{}:
	default: // 已有待处理的 Kick，合并
	}
}

// Start 阻塞运行直到被 Stop 停止或 ctx 取消，两种情况都返回 nil。
// ctx 取消表示必须立刻放弃：不再启动新的一轮，且 fn 收到的就是这个 ctx。
// Loop 只能 Start 一次。
func (l *Loop) Start(ctx context.Context) error {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return fmt.Errorf("worker %s: already started", l.cfg.Name)
	}
	l.started = true
	l.mu.Unlock()
	defer close(l.done)

	// 丢弃 Start 之前积压的 Kick，否则 RunOnStart=false 时会被意外提前触发。
	select {
	case <-l.kick:
	default:
	}

	first := l.cfg.RunOnStart
	for {
		if !first {
			l.wait(ctx)
		}
		first = false
		// wait 返回和 stop/cancel 可能同时就绪，这里统一优先判断退出。
		if l.halted(ctx) {
			return nil
		}
		l.round(ctx)
	}
}

// halted 报告是否应该停止启动新的一轮。
func (l *Loop) halted(ctx context.Context) bool {
	select {
	case <-l.stopCh:
		return true
	default:
	}
	return ctx.Err() != nil
}

// wait 等到下一轮该开始的时刻：Interval 到期、被 Kick、被 Stop 或 ctx 取消。
func (l *Loop) wait(ctx context.Context) {
	t := time.NewTimer(l.cfg.Interval)
	defer t.Stop()
	select {
	case <-t.C:
	case <-l.kick:
	case <-l.stopCh:
	case <-ctx.Done():
	}
}

// round 执行一轮：先问 Guard，再调用 fn。fn 的错误和 panic 都不会让循环退出。
func (l *Loop) round(ctx context.Context) {
	if g := l.cfg.Guard; g != nil {
		// Guard 通常是分布式锁（一次 Redis 调用）。给它单独的时限：它卡住时这一轮跳过，
		// 而不是让周期任务跟着卡住、连停止都要等它（设计文档 1d 的 5.6）。
		gctx, cancel := context.WithTimeout(ctx, guardTimeout)
		ok, err := g.Allow(gctx)
		cancel()
		if err != nil {
			l.logger.LogAttrs(ctx, slog.LevelWarn, "worker guard failed, skip round",
				slog.String("component", l.cfg.Name), slog.Any("error", err))
			return
		}
		if !ok {
			return
		}
	}
	if err := l.safeRun(ctx); err != nil {
		l.logger.LogAttrs(ctx, slog.LevelError, "worker round failed",
			slog.String("component", l.cfg.Name), slog.Any("error", err))
	}
}

// safeRun 调用 fn 并把 panic 转成日志，避免一个坏任务带崩整个进程。
func (l *Loop) safeRun(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			l.logger.LogAttrs(ctx, slog.LevelError, "worker round panicked",
				slog.String("component", l.cfg.Name),
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())))
			err = nil // 已经记过日志，不重复记 error
		}
	}()
	return l.fn(ctx)
}

// Stop 不再启动新的一轮，并等待正在运行的一轮结束（即 Start 返回）。
// ctx 到期而那一轮还没结束时返回 ctx 的错误，Start 仍会在那一轮结束后返回。
// 可重复调用；在 Start 之前调用时，之后的 Start 立即返回 nil。
func (l *Loop) Stop(ctx context.Context) error {
	l.stopOnce.Do(func() { close(l.stopCh) })

	l.mu.Lock()
	started := l.started
	l.mu.Unlock()
	if !started {
		return nil
	}

	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("worker %s: stop: %w", l.cfg.Name, ctx.Err())
	}
}
