package app

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// HTTPServer 把 http.Server 适配成 Component。
type HTTPServer struct {
	name string
	ln   net.Listener
	srv  *http.Server
}

// ListenHTTP 立即绑定 addr 并返回尚未开始服务的 HTTPServer。
// 在这里而不是在 Start 里监听，端口冲突等错误可以在启动其他组件之前暴露。
func ListenHTTP(ctx context.Context, name, addr string, handler http.Handler) (*HTTPServer, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s on %s: %w", name, addr, err)
	}
	return &HTTPServer{
		name: name,
		ln:   ln,
		srv: &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}, nil
}

// Name 实现 Component。
func (s *HTTPServer) Name() string { return s.name }

// Addr 返回实际监听的地址。addr 用了端口 0 时可以由此得到系统分配的端口。
func (s *HTTPServer) Addr() net.Addr { return s.ln.Addr() }

// Start 实现 Component。
func (s *HTTPServer) Start(context.Context) error {
	if err := s.srv.Serve(s.ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Stop 实现 Component：停止接收新连接，等待在途请求处理完或 ctx 到期。
func (s *HTTPServer) Stop(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)
	// Start 从未被调用时 Shutdown 不知道这个 listener，需要自己关。
	// 已被 Shutdown 关过时这里会返回 net.ErrClosed，可以忽略。
	if cerr := s.ln.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
		err = errors.Join(err, cerr)
	}
	return err
}

type healthResponse struct {
	Service string `json:"service"`
	Status  string `json:"status"`
}

// HealthHandler 返回只提供存活探测（和一个永远就绪的就绪探测）的 handler。
// 需要真实的就绪状态或者要往同一个端口上挂别的内部接口时用 AdminMux。
func HealthHandler(service string) http.Handler { return AdminMux(service, nil) }

// AdminMux 返回服务内部端口用的 mux，带两个探测：
//
//	GET /healthz  存活：进程能响应就是 200。
//	GET /readyz   就绪：r.Ready() 为真是 200，否则 503。r 为 nil 时永远是 200。
//
// 调用方可以接着往上挂别的内部接口（比如 degrade.Set.Register）。这个端口不经过 gateway。
func AdminMux(service string, r *Readiness) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeHealth(w, http.StatusOK, healthResponse{Service: service, Status: "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !r.Ready() {
			writeHealth(w, http.StatusServiceUnavailable, healthResponse{Service: service, Status: "not ready"})
			return
		}
		writeHealth(w, http.StatusOK, healthResponse{Service: service, Status: "ready"})
	})
	return mux
}

func writeHealth(w http.ResponseWriter, code int, v healthResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// 写响应失败说明客户端已断开，没有可做的补救。
	_ = json.MarshalWrite(w, v)
}
