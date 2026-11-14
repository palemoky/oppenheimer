package httpapi_test

import (
	"encoding/json/v2"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	orderindexv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/orderindex/v1"
	"github.com/palemoky/oppenheimer/pkg/ratelimit"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi"
)

// newHotEnv 和 newRateLimitedEnv 一样，另外让商家订单列表的假下游成功并计数。
func newHotEnv(t *testing.T, rl *httpapi.RateLimit) (e *env, stockCalls, merchantCalls *atomic.Int64) {
	t.Helper()
	e, stockCalls = newRateLimitedEnv(t, rl)
	merchantCalls = &atomic.Int64{}
	e.orderIndex.list = func(*orderindexv1.ListMerchantOrdersRequest) (*orderindexv1.ListMerchantOrdersResponse, error) {
		merchantCalls.Add(1)
		return &orderindexv1.ListMerchantOrdersResponse{}, nil
	}
	return e, stockCalls, merchantCalls
}

func stock(t *testing.T, h http.Handler, sku, user string) result {
	t.Helper()
	return send(t, h, http.MethodGet, "/api/v1/skus/"+sku+"/stock", user, "")
}

func merchantOrders(t *testing.T, h http.Handler, merchant, user string) result {
	t.Helper()
	return send(t, h, http.MethodGet, "/api/v1/merchants/"+merchant+"/orders", user, "")
}

// wide 是很宽的按用户限额：测试里不会碰到它，但按用户的中间件是挂着的。
var wide = ratelimit.Limit{Rate: slow, Burst: 1000}

// TestHotLimit 对两个有热点参数的接口：同一个参数值超额后 429 hot；另一个值不受影响；不同用户合起来计数
// （这是它和按用户限流的区别）；时钟前进后恢复；被拒绝的请求不调用下游；汇总日志里按 hot.<operationId> 计。
func TestHotLimit(t *testing.T) {
	for _, tt := range []struct {
		id    string
		call  func(t *testing.T, h http.Handler, value, user string) result
		calls func(stock, merchant *atomic.Int64) int64
	}{
		{"getStock", stock, func(s, _ *atomic.Int64) int64 { return s.Load() }},
		{"listMerchantOrders", merchantOrders, func(_, m *atomic.Int64) int64 { return m.Load() }},
	} {
		t.Run(tt.id, func(t *testing.T) {
			clock := newFakeClock()
			stats := &httpapi.RateLimitStats{}
			// 每秒 0.5 个、桶 3：用完之后 2 秒才有下一个令牌，Retry-After 应当是 2。
			e, stockCalls, merchantCalls := newHotEnv(t, &httpapi.RateLimit{
				Shared:    newLocal(newFakeClock()),
				User:      map[string]ratelimit.Limit{tt.id: wide},
				Hot:       newLocal(clock),
				HotLimits: map[string]ratelimit.Limit{tt.id: {Rate: 0.5, Burst: 3}},
				Stats:     stats,
			})
			calls := func() int64 { return tt.calls(stockCalls, merchantCalls) }

			// 三个不同的用户查同一个值，合起来用完了它的桶。每个用户自己的额度都还很宽。
			for u := range 3 {
				wantStatus(t, tt.call(t, e.h, "1001", strconv.Itoa(u+1)), http.StatusOK)
			}
			for u := range 5 {
				wantRateLimited(t, tt.call(t, e.h, "1001", strconv.Itoa(u+10)), "hot", 2)
			}
			if got := calls(); got != 3 {
				t.Errorf("downstream calls = %d, want 3 (rejected requests must not reach it)", got)
			}

			// 另一个值不受影响。
			wantStatus(t, tt.call(t, e.h, "1002", "1"), http.StatusOK)

			// 过了 Retry-After 之后恢复一个令牌。
			clock.Advance(2 * time.Second)
			wantStatus(t, tt.call(t, e.h, "1001", "99"), http.StatusOK)
			wantRateLimited(t, tt.call(t, e.h, "1001", "99"), "hot", 2)
			if got := calls(); got != 5 {
				t.Errorf("downstream calls = %d, want 5", got)
			}

			// 汇总日志里按 hot.<operationId> 计。
			var buf strings.Builder
			httpapi.NewRateLimitReporter(stats, nil, slog.New(slog.NewJSONHandler(&buf, nil)), time.Hour).Flush(t.Context())
			var line struct {
				Denied map[string]int64 `json:"denied"`
			}
			if err := json.Unmarshal([]byte(buf.String()), &line); err != nil {
				t.Fatalf("summary: %v: %q", err, buf.String())
			}
			if line.Denied["hot."+tt.id] != 6 || len(line.Denied) != 1 {
				t.Errorf("summary denied = %v, want hot.%s: 6", line.Denied, tt.id)
			}
		})
	}
}

// TestHotLimitIsCheckedAfterUser：一个请求同时超过按用户的和热点的限制时，报的是 user，热点的桶根本没有被问到。
func TestHotLimitIsCheckedAfterUser(t *testing.T) {
	hot := &countingLimiter{Limiter: newLocal(newFakeClock())}
	e, stockCalls, _ := newHotEnv(t, &httpapi.RateLimit{
		Shared:    newLocal(newFakeClock()),
		User:      map[string]ratelimit.Limit{"getStock": {Rate: slow, Burst: 1}},
		Hot:       hot,
		HotLimits: map[string]ratelimit.Limit{"getStock": {Rate: slow, Burst: 2}},
	})
	wantStatus(t, stock(t, e.h, "5", "7"), http.StatusOK)
	wantStatus(t, stock(t, e.h, "5", "8"), http.StatusOK)
	// 用户 7 的桶和 SKU 5 的桶都空了。
	wantRateLimited(t, stock(t, e.h, "5", "7"), "user", 0)
	if hot.calls != 2 {
		t.Errorf("hot limiter called %d times, want 2 (a request denied per user must not reach it)", hot.calls)
	}
	// 换一个还有额度的用户，报的才是 hot。
	wantRateLimited(t, stock(t, e.h, "5", "9"), "hot", 0)
	if stockCalls.Load() != 2 {
		t.Errorf("downstream calls = %d, want 2", stockCalls.Load())
	}
}

// TestHotLimitIgnoresInvalidValues：参数值不是正整数时不限流，handler 照原来的方式返回 400，限流器里也不为它建桶。
// 同一个 ID 换个写法（前导零）落在同一个桶里，绕不过限额。
func TestHotLimitIgnoresInvalidValues(t *testing.T) {
	hot := newLocal(newFakeClock())
	e, stockCalls, merchantCalls := newHotEnv(t, &httpapi.RateLimit{
		Hot: hot,
		HotLimits: map[string]ratelimit.Limit{
			"getStock":           {Rate: slow, Burst: 1},
			"listMerchantOrders": {Rate: slow, Burst: 1},
		},
	})
	for _, bad := range []string{"abc", "0", "-1", "1.5", "99999999999999999999"} {
		for range 3 {
			for _, r := range []result{stock(t, e.h, bad, "7"), merchantOrders(t, e.h, bad, "7")} {
				wantStatus(t, r, http.StatusBadRequest)
				if errCode(t, r) != "INVALID_ARGUMENT" {
					t.Errorf("%s: code = %s, want INVALID_ARGUMENT", bad, errCode(t, r))
				}
			}
		}
	}
	if n := hot.Len(); n != 0 {
		t.Errorf("hot limiter has %d buckets after invalid values, want 0", n)
	}
	if stockCalls.Load() != 0 || merchantCalls.Load() != 0 {
		t.Errorf("downstream calls = %d, %d; want 0", stockCalls.Load(), merchantCalls.Load())
	}

	wantStatus(t, stock(t, e.h, "7", "1"), http.StatusOK)
	wantRateLimited(t, stock(t, e.h, "007", "2"), "hot", 0)
	if n := hot.Len(); n != 1 {
		t.Errorf("hot limiter has %d buckets, want 1 (7 and 007 are the same SKU)", n)
	}
}

// TestHotLimitPerItem：个别参数值单独的阈值生效（包括单独设成不限），其他值用接口的默认值；
// 接口的默认值不限时，只限单独列出来的值。
func TestHotLimitPerItem(t *testing.T) {
	e, _, _ := newHotEnv(t, &httpapi.RateLimit{
		Hot:       newLocal(newFakeClock()),
		HotLimits: map[string]ratelimit.Limit{"getStock": {Rate: slow, Burst: 1}},
		HotItems: map[httpapi.HotItem]ratelimit.Limit{
			{Operation: "getStock", Value: 1001}:          {Rate: slow, Burst: 3},
			{Operation: "getStock", Value: 1002}:          {}, // 不限
			{Operation: "listMerchantOrders", Value: 300}: {Rate: slow, Burst: 2},
		},
	})
	for range 3 {
		wantStatus(t, stock(t, e.h, "1001", "7"), http.StatusOK)
	}
	wantRateLimited(t, stock(t, e.h, "1001", "7"), "hot", 0)

	wantStatus(t, stock(t, e.h, "5", "7"), http.StatusOK)
	wantRateLimited(t, stock(t, e.h, "5", "7"), "hot", 0)

	for range 10 {
		wantStatus(t, stock(t, e.h, "1002", "7"), http.StatusOK)
	}

	// listMerchantOrders 没有默认值（不限），只有商家 300 单独设了。
	for range 2 {
		wantStatus(t, merchantOrders(t, e.h, "300", "7"), http.StatusOK)
	}
	wantRateLimited(t, merchantOrders(t, e.h, "300", "7"), "hot", 0)
	for range 10 {
		wantStatus(t, merchantOrders(t, e.h, "301", "7"), http.StatusOK)
	}
}

// TestHotLimitOff：给了限流器但所有值都不限时，不限。
func TestHotLimitOff(t *testing.T) {
	hot := &countingLimiter{Limiter: newLocal(newFakeClock())}
	e, _, _ := newHotEnv(t, &httpapi.RateLimit{
		Hot:       hot,
		HotLimits: map[string]ratelimit.Limit{"getStock": {}},
		HotItems:  map[httpapi.HotItem]ratelimit.Limit{{Operation: "getStock", Value: 1}: {}},
	})
	for range 50 {
		wantStatus(t, stock(t, e.h, "1", "7"), http.StatusOK)
	}
	if hot.calls != 0 {
		t.Errorf("hot limiter called %d times, want 0", hot.calls)
	}
}
