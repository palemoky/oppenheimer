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

// HealthHandler 返回只提供 GET /healthz 的 handler，用于存活探测。
func HealthHandler(service string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// 写响应失败说明客户端已断开，没有可做的补救。
		_ = json.MarshalWrite(w, healthResponse{Service: service, Status: "ok"})
	})
	return mux
}
