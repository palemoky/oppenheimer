// Package app 管理服务进程的生命周期：并发启动各组件，收到退出信号或任一组件失败时，
// 按启动的相反顺序优雅停止。
package app

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
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

// Drainer 是可选的接口：组件实现它，就会在停止流程一开始（摘流的等待之前）被调用一次。
//
// 用来对外宣布“我要走了”，同时继续正常服务：比如 gRPC 服务端把健康检查置为不可用。
// Drain 不能阻塞，也不能停止接收请求，那是 Stop 的事。
type Drainer interface {
	Drain()
}

// Readiness 是进程的就绪状态，供就绪探测（/readyz）读取。零值是“还没就绪”。
//
// 就绪和存活是两回事（设计文档 1d 的 6.11）：存活回答“进程是不是还能响应”，不能就重启它；
// 就绪回答“现在能不能给它发请求”，不能就把它从负载均衡里摘掉，但不重启。
// 进程启动中、正在停止时都是活着但未就绪。
//
// 依赖降级不影响就绪：Redis 这样的共享依赖坏了，所有实例会同时变成未就绪，负载均衡把它们全部摘掉，
// 本来还能提供的功能也跟着没了。
type Readiness struct {
	state atomic.Int32
}

const (
	stateStarting int32 = iota
	stateReady
	stateDraining
)

// Ready 报告现在能不能接请求。在 nil 上调用返回 true（没有接就绪状态的服务按一直就绪处理）。
func (r *Readiness) Ready() bool { return r == nil || r.state.Load() == stateReady }

func (r *Readiness) set(v int32) {
	if r != nil {
		r.state.Store(v)
	}
}

// Options 是 RunWith 的可选项，零值就是 Run 的行为。
type Options struct {
	// Readiness 不为 nil 时由 RunWith 维护：组件都启动之后置为就绪，开始停止时置为未就绪。
	Readiness *Readiness
	// DrainDelay 是摘流的等待：收到退出信号后先变成未就绪并继续正常服务这么久，再开始停止组件。
	//
	// K8s 里“给进程发 SIGTERM”和“把这个实例从服务的端点里摘掉”同时发生，后者要经过端点控制器和各节点上的
	// 代理才生效，通常要几秒。收到信号就立刻停止接请求的话，这几秒里发过来的请求会被拒绝。
	// 本机每个服务只有一个实例，没有别处可去，取 0。它加上 StopTimeout 要小于 terminationGracePeriodSeconds。
	// 因为组件失败而退出时不等待：进程已经不正常了，尽快停。
	DrainDelay time.Duration
	// StopTimeout 是停止各组件的总时限，默认 DefaultStopTimeout。不含 DrainDelay。
	StopTimeout time.Duration
}

// DrainDelayEnv 是存放 Options.DrainDelay 的环境变量名。
const DrainDelayEnv = "DRAIN_DELAY"

// DrainDelayFromEnv 读 DRAIN_DELAY。没设是 0（不等待）。
func DrainDelayFromEnv() (time.Duration, error) {
	v := os.Getenv(DrainDelayEnv)
	if v == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("invalid %s %q, want a non-negative duration", DrainDelayEnv, v)
	}
	return d, nil
}

// ErrForced 表示停止过程中又收到一次退出信号，没有等组件停完就退出了。
var ErrForced = errors.New("app: second signal received, exiting without waiting for components")

// Run 启动所有组件并阻塞，直到收到 SIGINT、SIGTERM、ctx 被取消或任一组件失败，
// 然后按相反顺序停止组件。因信号或 ctx 取消而正常退出时返回 nil。
func Run(ctx context.Context, logger *slog.Logger, components ...Component) error {
	return RunWith(ctx, logger, Options{}, components...)
}

// RunWith 和 Run 相同，另外按 opts 维护就绪状态、在停止前等待摘流。
//
// 停止过程中再收到一次 SIGINT 或 SIGTERM 就立刻放弃：取消组件的 context，返回 ErrForced。
func RunWith(ctx context.Context, logger *slog.Logger, opts Options, components ...Component) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 第二次信号。先登记自己的通道再等第一次信号：NotifyContext 在第一次信号之后仍然登记着，
	// 同一个信号会同时送到两边，所以这里要把第一次的那个跳过去。
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	force := make(chan struct{})
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		seen := 0
		for {
			select {
			case <-sigs:
				if seen++; seen == 2 {
					close(force)
					return
				}
			case <-finished:
				return
			}
		}
	}()
	return run(ctx, logger, opts, force, components)
}

func run(ctx context.Context, logger *slog.Logger, opts Options, force <-chan struct{}, components []Component) error {
	stopTimeout := cmp.Or(opts.StopTimeout, DefaultStopTimeout)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	// 组件拿到的 context 与退出信号脱钩。若直接把 ctx 传给 Start，收到 SIGTERM 的瞬间
	// 所有在途的数据库和 RPC 调用都会被取消，Stop 承诺的“处理完在途工作”就成了空话。
	runCtx, abort := context.WithCancel(context.WithoutCancel(ctx))
	defer abort()

	var wg sync.WaitGroup
	for _, c := range components {
		wg.Go(func() {
			logger.InfoContext(ctx, "component starting", "component", c.Name())
			if err := c.Start(runCtx); err != nil {
				// 只有第一个失败会成为 cause，后续调用无效。
				cancel(&ComponentError{Component: c.Name(), Err: err})
				return
			}
			logger.InfoContext(ctx, "component exited", "component", c.Name())
		})
	}
	// 监听的端口在构造组件时就已经绑定（ListenHTTP、grpcx.Listen），这时发来的请求已经能排进队列，
	// 所以不必等每个 Start 真的跑起来再宣布就绪。
	opts.Readiness.set(stateReady)

	<-ctx.Done()

	var errs []error
	failed := false
	if cerr, ok := errors.AsType[*ComponentError](context.Cause(ctx)); ok {
		logger.ErrorContext(ctx, "component failed, shutting down", "component", cerr.Component, "error", cerr.Err)
		errs = append(errs, cerr)
		failed = true
	} else {
		logger.InfoContext(ctx, "shutdown requested")
	}

	// 摘流：先对外宣布不再就绪，同时继续正常服务，等负载均衡把流量挪走。
	opts.Readiness.set(stateDraining)
	for _, c := range components {
		if d, ok := c.(Drainer); ok {
			d.Drain()
		}
	}
	forced := false
	if opts.DrainDelay > 0 && !failed {
		logger.InfoContext(ctx, "draining before stop", "drain_delay", opts.DrainDelay.String())
		t := time.NewTimer(opts.DrainDelay)
		select {
		case <-t.C:
		case <-force:
			t.Stop()
			forced = true
		}
	}

	// ctx 已经取消，停止阶段需要一个不受它影响、有独立时限的 context。
	stopCtx, stopCancel := context.WithTimeoutCause(context.WithoutCancel(ctx), stopTimeout, ErrStopTimeout)
	defer stopCancel()
	stopCtx, forceCancel := context.WithCancelCause(stopCtx)
	defer forceCancel(nil)
	if forced {
		forceCancel(ErrForced)
	}
	stopping := make(chan struct{})
	defer close(stopping)
	go func() {
		select {
		case <-force:
			forceCancel(ErrForced)
		case <-stopping:
		}
	}()

	// 相反顺序：先停入口（不再接新请求），再停它依赖的下游组件。
	for _, c := range slices.Backward(components) {
		logger.InfoContext(ctx, "component stopping", "component", c.Name())
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
		// 时限用尽或者被第二次信号打断：取消组件的 context，让还在运行的工作尽快放弃。
		abort()
		errs = append(errs, context.Cause(stopCtx))
	}

	return errors.Join(errs...)
}
