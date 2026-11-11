// Package app 管理服务进程的生命周期：并发启动各组件，收到退出信号或任一组件失败时，
// 按启动的相反顺序优雅停止。
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"
)

// DefaultStopTimeout 是优雅停止的总时限。
// 需要小于 Kubernetes 的 terminationGracePeriodSeconds（默认 30 秒），否则会被强杀。
const DefaultStopTimeout = 20 * time.Second

// ErrStopTimeout 表示在时限内有组件的 Start 没有返回。
var ErrStopTimeout = errors.New("app: components did not exit before stop timeout")

// Component 是一个有生命周期的长期运行单元，例如 HTTP 服务、gRPC 服务、消息消费者。
type Component interface {
	// Name 用于日志和错误信息。
	Name() string
	// Start 阻塞运行直到组件停止。被 Stop 正常停止时返回 nil。
	//
	// ctx 不会因为收到退出信号而取消：优雅停止由 Stop 发起，在途工作可以继续用 ctx 做完。
	// ctx 只在停止时限用尽后才取消，表示必须立刻放弃。
	Start(ctx context.Context) error
	// Stop 让 Start 返回，并在 ctx 到期前尽量处理完在途工作。
	Stop(ctx context.Context) error
}

// ComponentError 表示某个组件的 Start 异常退出。
type ComponentError struct {
	Component string
	Err       error
}

func (e *ComponentError) Error() string {
	return fmt.Sprintf("component %s: %v", e.Component, e.Err)
}

func (e *ComponentError) Unwrap() error { return e.Err }

// Run 启动所有组件并阻塞，直到收到 SIGINT、SIGTERM、ctx 被取消或任一组件失败，
// 然后按相反顺序停止组件。因信号或 ctx 取消而正常退出时返回 nil。
func Run(ctx context.Context, logger *slog.Logger, components ...Component) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, logger, DefaultStopTimeout, components)
}

func run(ctx context.Context, logger *slog.Logger, stopTimeout time.Duration, components []Component) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	// 组件拿到的 context 与退出信号脱钩。若直接把 ctx 传给 Start，收到 SIGTERM 的瞬间
	// 所有在途的数据库和 RPC 调用都会被取消，Stop 承诺的“处理完在途工作”就成了空话。
	runCtx, abort := context.WithCancel(context.WithoutCancel(ctx))
	defer abort()

	var wg sync.WaitGroup
	for _, c := range components {
		wg.Go(func() {
			logger.Info("component starting", "component", c.Name())
			if err := c.Start(runCtx); err != nil {
				// 只有第一个失败会成为 cause，后续调用无效。
				cancel(&ComponentError{Component: c.Name(), Err: err})
				return
			}
			logger.Info("component exited", "component", c.Name())
		})
	}

	<-ctx.Done()

	var errs []error
	if cerr, ok := errors.AsType[*ComponentError](context.Cause(ctx)); ok {
		logger.Error("component failed, shutting down", "component", cerr.Component, "error", cerr.Err)
		errs = append(errs, cerr)
	} else {
		logger.Info("shutdown requested")
	}

	// ctx 已经取消，停止阶段需要一个不受它影响、有独立时限的 context。
	stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(ctx), stopTimeout)
	defer stopCancel()

	// 相反顺序：先停入口（不再接新请求），再停它依赖的下游组件。
	for _, c := range slices.Backward(components) {
		logger.Info("component stopping", "component", c.Name())
		if err := c.Stop(stopCtx); err != nil {
			errs = append(errs, fmt.Errorf("stop component %s: %w", c.Name(), err))
		}
	}

	exited := make(chan struct{})
	go func() {
		wg.Wait()
		close(exited)
	}()

	select {
	case <-exited:
	case <-stopCtx.Done():
		// 时限用尽：取消组件的 context，让还在运行的工作尽快放弃。
		abort()
		errs = append(errs, ErrStopTimeout)
	}

	return errors.Join(errs...)
}
