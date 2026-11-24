//go:build faults

package httpapi_test

import (
	"context"
	"net/http"
	"runtime"
	"testing"
	"time"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/fault"
)

// 这个文件只在带 faults 标签的构建里编译。它钉住的是故障点 f01 的行为（答案在 chaos/scenarios/ 里，这里不写）：
// 开关关着时和平时的构建一样；开着时，下游快的请求不受影响，下游慢的请求表现出预期的症状。
func TestStockVariant(t *testing.T) {
	get := func(h http.Handler) int {
		code, _ := serve(t.Context(), h, "/api/v1/skus/7/stock")
		return code
	}
	delay := func(d time.Duration) func(context.Context, *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
		return func(ctx context.Context, _ *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
			select {
			case <-time.After(d):
				return &inventoryv1.GetStockResponse{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	slow, _ := newClosedEnv(t, time.Minute, nil, delay(200*time.Millisecond))
	fast, _ := newClosedEnv(t, time.Minute, nil, delay(0))

	// 关着：慢的下游也等到结果。
	if code := get(slow); code != http.StatusOK {
		t.Fatalf("switch off, slow downstream: status = %d, want 200", code)
	}

	if err := fault.Set("f01", true, time.Minute); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fault.Set("f01", false, 0) })

	if code := get(fast); code != http.StatusOK {
		t.Errorf("switch on, fast downstream: status = %d, want 200", code)
	}
	const n = 20
	before := runtime.NumGoroutine()
	for range n {
		if code := get(slow); code != http.StatusGatewayTimeout {
			t.Fatalf("switch on, slow downstream: status = %d, want 504", code)
		}
	}
	// 等的时间远长于假下游的 200 毫秒：这时还多出来的 goroutine 不是“还没跑完”的。
	time.Sleep(600 * time.Millisecond)
	if grown := runtime.NumGoroutine() - before; grown < n {
		t.Errorf("goroutines grew by %d after %d slow requests, want at least %d", grown, n, n)
	}
}
