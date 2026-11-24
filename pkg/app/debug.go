package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"time"
)

// DebugAddrEnv 是调试端口（pprof）监听地址的环境变量名。不设就不开调试端口。
//
// 调试端口单独开，不挂在 HTTP_ADDR 的探测端口上：
//   - gateway 的 HTTP 端口是对外的 API 端口；
//   - 其他服务的 HTTP 端口经 ClusterIP Service 暴露给整个集群，任何 Pod 都能连。
//     /debug/pprof/profile、/debug/pprof/trace 会让进程按请求的时长采样，cmdline 会泄露启动参数，
//     这些不该对集群里的任何人开放。
//
// 集群里设成回环地址（例如 127.0.0.1:6060），只能经 kubectl port-forward 访问（要有 pods/portforward 权限）：
//
//	kubectl -n <ns> port-forward deploy/<服务> 6060:6060
//	go tool pprof http://127.0.0.1:6060/debug/pprof/heap
//
// 代码里不给默认值：本机同时跑几个服务时会抢同一个端口。
const DebugAddrEnv = "DEBUG_ADDR"

// mutex、block 剖析的采样（设计 4a 的 3.4）。不设或设成 0 就不采样（和 Go 的默认一样），
// 只在调试端口开着（DEBUG_ADDR 设了）时生效：没有端口就取不走剖析，采了也白采。
//
//   - DEBUG_MUTEX_FRACTION：整数 n，平均每 n 次锁争用记 1 次（runtime.SetMutexProfileFraction）。
//   - DEBUG_BLOCK_RATE：Go 的时长（例如 1ms），平均每阻塞这么久记 1 次阻塞事件（runtime.SetBlockProfileRate
//     的参数是纳秒，这里写成时长，免得把 1 毫秒写成 1、变成每次阻塞都记）。
//
// 两者都是进程级的全局设置，进程活着就一直采。开销要实测（3b 的压测对比吞吐和 p99）再定值。
const (
	DebugMutexFractionEnv = "DEBUG_MUTEX_FRACTION"
	DebugBlockRateEnv     = "DEBUG_BLOCK_RATE"
)

// debugSampling 读 mutex、block 采样的两个环境变量。值不合法时返回错误：写错了就悄悄不采样，
// 要用的时候才发现剖析是空的。
func debugSampling() (mutexFraction int, blockRate time.Duration, err error) {
	if v := os.Getenv(DebugMutexFractionEnv); v != "" {
		mutexFraction, err = strconv.Atoi(v)
		if err != nil || mutexFraction < 0 {
			return 0, 0, fmt.Errorf("%s=%q: want a non-negative integer (sample 1 in n contention events)", DebugMutexFractionEnv, v)
		}
	}
	if v := os.Getenv(DebugBlockRateEnv); v != "" {
		blockRate, err = time.ParseDuration(v)
		if err != nil || blockRate < 0 {
			return 0, 0, fmt.Errorf("%s=%q: want a non-negative duration such as 1ms", DebugBlockRateEnv, v)
		}
	}
	return mutexFraction, blockRate, nil
}

// DebugMux 返回只挂 pprof 的 mux，路径和 net/http/pprof 往 DefaultServeMux 上注册的一致（只接受 GET）：
//
//	/debug/pprof/          索引；/debug/pprof/<名字> 取 runtime/pprof 的各个剖析
//	                       （heap、goroutine、allocs、block、mutex、threadcreate）
//	/debug/pprof/cmdline   启动参数
//	/debug/pprof/profile   CPU 剖析，?seconds=N
//	/debug/pprof/symbol    符号查询
//	/debug/pprof/trace     执行跟踪，?seconds=N
//
// 处理函数显式挂在自己的 mux 上。导入 net/http/pprof 时它的 init 仍会往 DefaultServeMux 上注册一份，
// 所以任何端口都不能用 DefaultServeMux 服务（ListenHTTP 拒绝 nil handler）。
//
// block 和 mutex 剖析默认不采样，取到的是空的。要采样时用环境变量 DEBUG_MUTEX_FRACTION、DEBUG_BLOCK_RATE
// 在启动时打开（见 ListenDebug），不提供运行中开关的接口：采样有开销，也会改变被观察的程序，
// 打开它是部署时的决定，不能由来抓剖析的一方（人或诊断 Agent）顺手打开。
func DebugMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return mux
}

// ListenDebug 按 DEBUG_ADDR 绑定调试端口。没设时返回 nil, nil，调用方不把它加进组件。
//
// 返回的服务器和其他 HTTP 服务器一样交给 Run 管理生命周期，区别是 Stop 直接断开连接，不等在途请求：
// 在途的多半是 30 秒的 CPU 剖析或执行跟踪，等它们会吃掉整个停止时限；断开后客户端拿不到这一份剖析，
// 但不影响任何业务。调用方应把它排在组件列表的最前面，让它最后停，停止过程卡住时还能取 goroutine。
func ListenDebug(ctx context.Context, logger *slog.Logger) (*HTTPServer, error) {
	addr := os.Getenv(DebugAddrEnv)
	if addr == "" {
		return nil, nil
	}
	mutexFraction, blockRate, err := debugSampling()
	if err != nil {
		return nil, err
	}
	s, err := ListenHTTP(ctx, "debug", addr, DebugMux())
	if err != nil {
		return nil, err
	}
	s.closeOnStop = true
	// 端口绑上之后再打开采样：绑不上时进程会退出，没必要先改全局设置。
	if mutexFraction > 0 {
		runtime.SetMutexProfileFraction(mutexFraction)
	}
	if blockRate > 0 {
		runtime.SetBlockProfileRate(int(blockRate.Nanoseconds()))
	}
	logger.InfoContext(ctx, "debug server listening", "debug_addr", s.Addr().String(),
		"mutex_profile_fraction", mutexFraction, "block_profile_rate", blockRate.String())
	return s, nil
}

// WithDebug 把 debug（可以是 nil，即没开调试端口）排在 components 的最前面：Run 按相反顺序停止，
// 调试端口最后停，其他组件停止时卡住了还能取剖析。
func WithDebug(debug *HTTPServer, components ...Component) []Component {
	if debug == nil {
		return components
	}
	return append([]Component{debug}, components...)
}
