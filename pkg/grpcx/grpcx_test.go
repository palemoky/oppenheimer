package grpcx

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// probe 借用标准健康检查的接口形状做一个可控的测试服务：
// 按请求里的 service 名决定行为，省去为测试单独生成 proto。
type probe struct {
	healthpb.UnimplementedHealthServer
	sawDeadline chan bool
}

func (p *probe) List(ctx context.Context, _ *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	_, ok := ctx.Deadline()
	p.sawDeadline <- ok
	return &healthpb.HealthListResponse{}, nil
}

func (p *probe) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	switch req.GetService() {
	case "panic":
		panic("secret internal detail")
	case "slow":
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

// probeDesc 把 probe 注册到一个不与内置健康服务冲突的服务名下。
var probeDesc = func() grpc.ServiceDesc {
	desc := healthpb.Health_ServiceDesc
	desc.ServiceName = "test.Probe"
	return desc
}()

const (
	probeCheck = "/test.Probe/Check"
	probeList  = "/test.Probe/List"
)

func startServer(t *testing.T) (*grpc.ClientConn, *probe, *Server) {
	t.Helper()
	p := &probe{sawDeadline: make(chan bool, 1)}
	logger := slog.New(slog.DiscardHandler)

	srv, err := Listen(t.Context(), logger, "grpc", "127.0.0.1:0", func(s *grpc.Server) {
		s.RegisterService(&probeDesc, p)
	})
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Start(context.Background()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
		if err := <-serveErr; err != nil {
			t.Errorf("Start() = %v, want nil after Stop", err)
		}
	})

	conn, err := Dial(srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, p, srv
}

func check(ctx context.Context, conn *grpc.ClientConn, service string) error {
	return conn.Invoke(ctx, probeCheck, &healthpb.HealthCheckRequest{Service: service}, &healthpb.HealthCheckResponse{})
}

func TestBuiltInHealthServiceIsServing(t *testing.T) {
	conn, _, _ := startServer(t)

	resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("health status = %v, want SERVING", resp.GetStatus())
	}
}

func TestPanicBecomesInternalWithoutLeakingDetails(t *testing.T) {
	conn, _, _ := startServer(t)

	err := check(t.Context(), conn, "panic")

	st := status.Convert(err)
	if st.Code() != codes.Internal {
		t.Fatalf("code = %v, want Internal (err: %v)", st.Code(), err)
	}
	if st.Message() != "internal error" {
		t.Errorf("message = %q, leaks panic details", st.Message())
	}
	// 服务端在 panic 之后仍然可用。
	if err := check(t.Context(), conn, "ok"); err != nil {
		t.Errorf("call after panic: %v", err)
	}
}

func TestClientAddsDefaultTimeoutOnlyWhenMissing(t *testing.T) {
	conn, p, _ := startServer(t)

	// 调用方没有设置截止时间：客户端拦截器补上，服务端能看到。
	if err := conn.Invoke(context.Background(), probeList, &healthpb.HealthListRequest{}, &healthpb.HealthListResponse{}); err != nil {
		t.Fatal(err)
	}
	if !<-p.sawDeadline {
		t.Error("server saw no deadline; default timeout was not applied")
	}

	// 调用方自带更短的截止时间：以调用方为准，而不是被默认值覆盖。
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := check(ctx, conn, "slow")
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("slow call = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > DefaultCallTimeout/2 {
		t.Errorf("slow call took %v; caller deadline was overridden by the default", elapsed)
	}
}

func TestStopForcesShutdownWhenContextExpires(t *testing.T) {
	conn, _, srv := startServer(t)

	// 一个卡住的在途调用会让 GracefulStop 一直等下去。
	callErr := make(chan error, 1)
	callCtx, cancelCall := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelCall()
	go func() { callErr <- check(callCtx, conn, "slow") }()
	time.Sleep(100 * time.Millisecond) // 等调用到达服务端

	stopCtx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := srv.Stop(stopCtx); err == nil {
		t.Error("Stop() = nil, want an error reporting the forced shutdown")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Stop took %v, did not honor its context", elapsed)
	}

	if err := <-callErr; status.Code(err) == codes.OK {
		t.Error("in-flight call succeeded after forced shutdown")
	}
}
