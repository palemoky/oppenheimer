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
		sanitizeInterceptor(logger),
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

// Drain 实现 app.Drainer：把健康检查置为不可用，在途和新来的调用照常处理。
// 停止流程一开始就调用，让看健康检查的负载均衡在摘流的等待期间把流量挪走。
func (s *Server) Drain() { s.health.Shutdown() }

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

// sanitizeInterceptor 保证返回给调用方的都是 gRPC 状态错误。
//
// handler 直接返回普通 error（例如包装过的数据库错误）时，gRPC 会把它作为 Unknown
// 连同完整消息发给调用方，SQL 和内部细节就泄露出去了。这里统一兜底：
// 细节只进日志，对外是不含内部信息的 Internal。
func sanitizeInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err == nil {
			return resp, nil
		}
		if _, ok := status.FromError(err); ok {
			return resp, err
		}
		switch {
		case errors.Is(err, context.Canceled):
			return nil, status.Error(codes.Canceled, "request canceled")
		case errors.Is(err, context.DeadlineExceeded):
			return nil, status.Error(codes.DeadlineExceeded, "deadline exceeded")
		}
		logger.ErrorContext(ctx, "unhandled error in grpc handler", "method", info.FullMethod, "error", err.Error())
		return nil, status.Error(codes.Internal, "internal error")
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
//
// 要给这条连接上的调用单独设一个更短的时限（逐跳收紧），加上 WithHopTimeout。
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

// SidecarMargin 是进程里的单跳时限比 Envoy 边车出站路由的 timeout 多出来的那一截。
//
// 一跳的预算是边车路由上的那个数（比如 3 秒）；进程里的时限取“预算加这一截”，只做兜底。
// 两个数不能相等，更不能让进程里的更短：进程里的计时器先启动，相等时总是调用方先放手，边车看到的是
// “调用方断开了”，而不是“上游超时了”。实测只有边车自己的路由超时到期才计入异常检测（设计文档 1d 的 5.3），
// 调用方先放手的话，一个卡住的下游永远不会被摘除，每个请求都要等满时限。让边车先超时，它才能数到
// 连续 5 次并把下游摘掉。兜底的用处是边车不在（直连）或者边车自己卡住时，调用方仍然会放手。
const SidecarMargin = 500 * time.Millisecond

// WithHopTimeout 给这条连接上的每次调用设一个时限：这一跳最多等 d，同时不超过调用方 ctx 里剩下的时间。
//
// 用来逐跳收紧时限（设计文档 1d 的 5.6）。总时限只在入口设一次，经 ctx 往下传；如果每一跳都把剩下的时间
// 原样交给下一跳，下游卡住时这条链上的每一层会在同一刻超时，谁都来不及做任何处理。给下一跳的时限比自己的短，
// 上一跳在它超时之后还有时间返回明确的错误或者走降级。
//
// 和 timeoutInterceptor 的区别：那个只在调用方没带截止时间时才加一个兜底，这个不管带没带都生效，取两者中更早的。
// 同一跳在 Envoy 边车的出站路由上也有时限（deploy/envoy/）。d 应当等于边车的那个数加上 SidecarMargin：
// 边车先超时，慢下游才会被摘除；进程里的保证边车不在或出问题时调用方仍然按时放手。
func WithHopTimeout(d time.Duration) grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// context.WithTimeout 本来就取父 ctx 的截止时间和新时限中更早的一个。
		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		return invoker(ctx, method, req, reply, cc, opts...)
	})
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
