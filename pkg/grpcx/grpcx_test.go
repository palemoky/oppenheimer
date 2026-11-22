package grpcx

import (
	"context"
	"errors"
	"fmt"
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
	case "raw":
		return nil, fmt.Errorf("query: %w", errors.New("Error 1064: SELECT secret FROM t"))
	case "canceled":
		return nil, fmt.Errorf("query: %w", context.Canceled)
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

func TestPlainErrorsAreSanitized(t *testing.T) {
	conn, _, _ := startServer(t)

	st := status.Convert(check(t.Context(), conn, "raw"))
	if st.Code() != codes.Internal || st.Message() != "internal error" {
		t.Errorf("plain error reached the client as %v %q, want Internal \"internal error\"", st.Code(), st.Message())
	}

	if code := status.Code(check(t.Context(), conn, "canceled")); code != codes.Canceled {
		t.Errorf("wrapped context.Canceled = %v, want Canceled", code)
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

// 逐跳收紧：这一跳的时限比调用方剩下的时间短时以它为准；调用方剩下的更短时以调用方为准。
func TestHopTimeoutIsTheEarlierOfHopAndCaller(t *testing.T) {
	_, _, srv := startServer(t)
	conn, err := Dial(srv.Addr().String(), WithHopTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// 调用方给了 5 秒，这一跳只等 100 毫秒：调用方在下游超时之后还剩很多时间可以处理。
	caller, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err = check(caller, conn, "slow")
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("slow call = %v, want DeadlineExceeded", err)
	}
	if el := time.Since(start); el < 80*time.Millisecond || el > time.Second {
		t.Errorf("slow call returned after %v, want about the 100ms hop timeout", el)
	}
	if caller.Err() != nil {
		t.Error("the caller's own context expired with the hop")
	}

	// 调用方只剩 30 毫秒：不会因为这一跳的时限是 100 毫秒而多等。
	short, cancelShort := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancelShort()
	start = time.Now()
	if err := check(short, conn, "slow"); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("slow call = %v, want DeadlineExceeded", err)
	}
	if el := time.Since(start); el > 90*time.Millisecond {
		t.Errorf("slow call took %v with 30ms left on the caller, want it to stop with the caller", el)
	}

	// 调用方没带截止时间时同样生效。
	start = time.Now()
	if err := check(context.Background(), conn, "slow"); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("slow call = %v, want DeadlineExceeded", err)
	}
	if el := time.Since(start); el > time.Second {
		t.Errorf("slow call without a caller deadline took %v, want about 100ms", el)
	}
	// 正常的调用不受影响。
	if err := check(t.Context(), conn, "ok"); err != nil {
		t.Fatalf("fast call: %v", err)
	}
}

// 按方法分时限：列出的方法用自己的时限（可以比默认的长），没列出的用默认的。
func TestHopTimeoutsPerMethod(t *testing.T) {
	_, _, srv := startServer(t)
	slowCall := func(t *testing.T, perMethod map[string]time.Duration) time.Duration {
		t.Helper()
		conn, err := Dial(srv.Addr().String(), WithHopTimeouts(100*time.Millisecond, perMethod))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		caller, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		start := time.Now()
		if err := check(caller, conn, "slow"); status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("slow call = %v, want DeadlineExceeded", err)
		}
		return time.Since(start)
	}

	if el := slowCall(t, map[string]time.Duration{probeCheck: 500 * time.Millisecond}); el < 450*time.Millisecond || el > 2*time.Second {
		t.Errorf("listed method returned after %v, want about its own 500ms", el)
	}
	// 只列了别的方法：这个方法仍然用默认的 100 毫秒。
	if el := slowCall(t, map[string]time.Duration{probeList: 5 * time.Second}); el < 80*time.Millisecond || el > 400*time.Millisecond {
		t.Errorf("unlisted method returned after %v, want about the default 100ms", el)
	}
}
