package httpapi_test

import (
	"context"
	"encoding/json/v2"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/ratelimit"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi"
)

// logRecord 是记下来的一条日志：级别、消息和 status 属性（访问日志才有）。
type logRecord struct {
	level  slog.Level
	msg    string
	status int64
}

// recordingHandler 记下每条日志的级别，用来断言“没有 ERROR”。并发安全。
type recordingHandler struct {
	mu      sync.Mutex
	records []logRecord
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler            { return h }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{level: r.Level, msg: r.Message}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "status" {
			rec.status = a.Value.Int64()
		}
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, rec)
	return nil
}

func (h *recordingHandler) snapshot() []logRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]logRecord(nil), h.records...)
}

// hasLevel 报告有没有这个级别或更高的日志。
func (h *recordingHandler) hasLevel(l slog.Level) bool {
	for _, r := range h.snapshot() {
		if r.level >= l {
			return true
		}
	}
	return false
}

// accessLogStatus 返回访问日志（"http request"）里记的状态码。
func (h *recordingHandler) accessLogStatus(t *testing.T) int64 {
	t.Helper()
	for _, r := range h.snapshot() {
		if r.msg == "http request" {
			return r.status
		}
	}
	t.Fatal("no access log")
	return 0
}

// newClosedEnv 建一个用 recordingHandler 记日志的 handler。getStock 的假下游由调用方给。
func newClosedEnv(t *testing.T, callTimeout time.Duration, rl *httpapi.RateLimit,
	getStock func(context.Context, *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error),
) (http.Handler, *recordingHandler) {
	t.Helper()
	e := newEnv(t)
	e.inventory.getStock = getStock
	logs := &recordingHandler{}
	h := httpapi.New(httpapi.Deps{Order: e.order, Payment: e.payment, Inventory: e.inventory, OrderIndex: e.orderIndex}, httpapi.Config{
		Logger:      slog.New(logs),
		CallTimeout: callTimeout,
		RateLimit:   rl,
	})
	return h, logs
}

// blockUntilDone 是一个阻塞到 ctx 结束、然后像 gRPC 客户端一样返回 ctx 对应状态码的假下游。entered 在它开始等时关闭。
func blockUntilDone(entered chan struct{}) func(context.Context, *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
	return func(ctx context.Context, _ *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
		close(entered)
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// serve 在 ctx 下发一个 GET 请求，返回状态码和错误码（响应体不是错误结构时错误码为空）。
// 不用 t：TestClientClosedRequest 在另一个 goroutine 里调它，那里不能 t.Fatal。
func serve(ctx context.Context, h http.Handler, path string) (int, string) {
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	req.Header.Set("X-User-Id", "7")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		return rec.Code, ""
	}
	return rec.Code, body.Error.Code
}

// TestClientClosedRequest：客户端在下游调用途中断开（请求的 ctx 被取消）时是 499 CLIENT_CLOSED_REQUEST，
// 不记 ERROR，访问日志照常记一条 499。
func TestClientClosedRequest(t *testing.T) {
	entered := make(chan struct{})
	h, logs := newClosedEnv(t, time.Minute, nil, blockUntilDone(entered))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type res struct {
		code    int
		errCode string
	}
	done := make(chan res, 1)
	go func() {
		c, ec := serve(ctx, h, "/api/v1/skus/1/stock")
		done <- res{c, ec}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("downstream was not called")
	}
	cancel()
	var r res
	select {
	case r = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not finish after the client went away")
	}

	if r.code != 499 || r.errCode != "CLIENT_CLOSED_REQUEST" {
		t.Errorf("got %d %s, want 499 CLIENT_CLOSED_REQUEST", r.code, r.errCode)
	}
	if logs.hasLevel(slog.LevelWarn) {
		t.Errorf("a client going away was logged at WARN or above: %+v", logs.snapshot())
	}
	if got := logs.accessLogStatus(t); got != 499 {
		t.Errorf("access log status = %d, want 499", got)
	}
}

// TestGatewayDeadlineIsStill504：gateway 自己的总时限到期（ctx 的错误是 DeadlineExceeded，不是 Canceled）
// 是服务端超时，仍然是 504。
func TestGatewayDeadlineIsStill504(t *testing.T) {
	entered := make(chan struct{})
	h, logs := newClosedEnv(t, 20*time.Millisecond, nil, blockUntilDone(entered))
	code, errCode := serve(t.Context(), h, "/api/v1/skus/1/stock")
	if code != http.StatusGatewayTimeout || errCode != "DEADLINE_EXCEEDED" {
		t.Errorf("got %d %s, want 504 DEADLINE_EXCEEDED", code, errCode)
	}
	if got := logs.accessLogStatus(t); got != http.StatusGatewayTimeout {
		t.Errorf("access log status = %d, want 504", got)
	}
}

// TestDownstreamCanceledWithLiveClient：下游返回了 Canceled，但请求的 ctx 没有被取消（客户端还在等），
// 是下游自己的问题，保持原来的映射：500 CANCELLED，记 ERROR。
func TestDownstreamCanceledWithLiveClient(t *testing.T) {
	h, logs := newClosedEnv(t, time.Minute, nil, func(context.Context, *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
		return nil, status.Error(codes.Canceled, "downstream cancelled its own call")
	})
	code, errCode := serve(t.Context(), h, "/api/v1/skus/1/stock")
	if code != http.StatusInternalServerError || errCode != "CANCELLED" {
		t.Errorf("got %d %s, want 500 CANCELLED", code, errCode)
	}
	if !logs.hasLevel(slog.LevelError) {
		t.Error("a downstream failure with a live client should be logged at ERROR")
	}
}

// TestRateLimiterClientClosedRequest：限流器因为调用方取消而出错时，同样是 499，不放行、不记 ERROR。
func TestRateLimiterClientClosedRequest(t *testing.T) {
	h, logs := newClosedEnv(t, time.Minute, &httpapi.RateLimit{
		Shared: failingLimiter{err: context.Canceled},
		User:   map[string]ratelimit.Limit{"getStock": {Rate: 1, Burst: 1}},
	}, func(context.Context, *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
		t.Error("downstream called for a request the client already abandoned")
		return &inventoryv1.GetStockResponse{}, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	code, errCode := serve(ctx, h, "/api/v1/skus/1/stock")
	if code != 499 || errCode != "CLIENT_CLOSED_REQUEST" {
		t.Errorf("got %d %s, want 499 CLIENT_CLOSED_REQUEST", code, errCode)
	}
	if logs.hasLevel(slog.LevelWarn) {
		t.Errorf("logged at WARN or above: %+v", logs.snapshot())
	}
}
