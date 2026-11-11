// Package grpcx 提供各服务统一的 gRPC 服务端与客户端配置。
package grpcx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// DefaultCallTimeout 是客户端调用没有自带截止时间时使用的超时。
// 没有超时的调用在下游卡住时会一直占着 goroutine 和连接，是级联故障的常见起点。
const DefaultCallTimeout = 3 * time.Second

// Server 把 grpc.Server 适配成 app.Component。
type Server struct {
	name   string
	ln     net.Listener
	srv    *grpc.Server
	health *health.Server
}

// Listen 立即绑定 addr，创建带统一拦截器的 gRPC 服务端，并调用 register 注册业务服务。
// 同时注册标准的健康检查服务，供 Kubernetes 的 gRPC 探针使用。
func Listen(ctx context.Context, logger *slog.Logger, name, addr string, register func(*grpc.Server)) (*Server, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s on %s: %w", name, addr, err)
	}

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		// 顺序：日志在最外层，能记录到 recover 转换后的错误。
		loggingInterceptor(logger),
		recoveryInterceptor(logger),
	))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	register(srv)

	return &Server{name: name, ln: ln, srv: srv, health: hs}, nil
}

// Name 实现 app.Component。
func (s *Server) Name() string { return s.name }

// Addr 返回实际监听的地址。
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Start 实现 app.Component。
func (s *Server) Start(context.Context) error {
	if err := s.srv.Serve(s.ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// Stop 实现 app.Component：先把健康状态置为不可用，再等待在途调用结束；ctx 到期则强制关闭。
func (s *Server) Stop(ctx context.Context) error {
	s.health.Shutdown()

	done := make(chan struct{})
	go func() {
		s.srv.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.srv.Stop()
		<-done
		return fmt.Errorf("grpc server %s: graceful stop interrupted: %w", s.name, context.Cause(ctx))
	}
}

func recoveryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorContext(ctx, "panic in grpc handler",
					"method", info.FullMethod, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
				// 不把 panic 内容返回给调用方，避免泄露内部细节。
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

func loggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == healthpb.Health_Check_FullMethodName {
			return handler(ctx, req)
		}

		start := time.Now()
		resp, err := handler(ctx, req)

		code := status.Code(err)
		level := slog.LevelInfo
		if isServerFault(code) {
			level = slog.LevelError
		}
		attrs := []any{"method", info.FullMethod, "code", code.String(), "duration_ms", time.Since(start).Milliseconds()}
		if err != nil {
			attrs = append(attrs, "error", err.Error())
		}
		logger.Log(ctx, level, "grpc request", attrs...)
		return resp, err
	}
}

// isServerFault 区分“服务端出了问题”和“调用方的请求被正常拒绝”。
// 库存不足、订单不存在这类业务结果不是故障，不应该打成 error 触发告警。
func isServerFault(code codes.Code) bool {
	switch code {
	case codes.Unknown, codes.DeadlineExceeded, codes.Unimplemented, codes.Internal, codes.Unavailable, codes.DataLoss:
		return true
	default:
		return false
	}
}

// Dial 创建到 target 的客户端连接。连接是惰性建立的，Dial 本身不会因为对端不可达而失败。
//
// 1a 阶段服务间走明文。没有自带截止时间的调用会被加上 DefaultCallTimeout。
func Dial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	base := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(timeoutInterceptor(DefaultCallTimeout)),
	}
	conn, err := grpc.NewClient(target, append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("grpcx: dial %s: %w", target, err)
	}
	return conn, nil
}

func timeoutInterceptor(timeout time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
