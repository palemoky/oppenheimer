package main

import (
	"net/http"

	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/fault"
)

// defaultAdminAddr 是内部管理端口的默认地址。只在本机回环上监听：上面的 /degrade 没有鉴权，
// 能连上它的人就能关掉接口，所以不能对外。其他服务的内部端口不经过 gateway，本来就不对外；
// gateway 的 HTTP 端口是对外的，只能另开一个。
const defaultAdminAddr = "127.0.0.1:8089"

// publicMux 是对外端口的路由：API 加两个探测。
//
// 探测很频繁，挂在 mux 上，不经过 API 的鉴权、限流和请求日志。/readyz 也要在这里：
// 负载均衡的就绪探测打的是对外的端口，内部端口只在本机监听，它够不着。
// 其余路径全部交给 API，/degrade 在这里落到 API 的 404（TestPublicMuxHidesDegrade 钉住）。
func publicMux(api http.Handler, readiness *app.Readiness) *http.ServeMux {
	probes := app.AdminMux(serviceName, readiness)
	mux := http.NewServeMux()
	mux.Handle("/healthz", probes)
	mux.Handle("/readyz", probes)
	mux.Handle("/", api)
	return mux
}

// adminMux 是内部管理端口的路由：两个探测和降级开关（设计文档 1d 的 6.9）。
// fault.Register 在平时的构建里什么都不挂（pkg/fault 的包注释）。
func adminMux(readiness *app.Readiness, switches *degrade.Set) *http.ServeMux {
	mux := app.AdminMux(serviceName, readiness)
	switches.Register(mux)
	fault.Register(mux)
	return mux
}
