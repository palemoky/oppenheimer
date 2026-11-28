//go:build faults

package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/fault"
)

// 这个文件只在带 faults 标签的构建里编译。它钉住的是故障点 f02 的行为（答案在 chaos/scenarios/ 里，这里不写）：
// 开关关着时和平时的构建一样，每次都问下游；开着时，终态的订单只问一次，记下的条目不会被清掉，堆随读过的订单数增长。
func TestOrderDetailVariant(t *testing.T) {
	const owner = 7
	// 订单号从别的用例不用的区间取，每跑一次（-count）换一段：开关开着时记下的条目留在进程里。
	base := 1<<40 + variantRuns.Add(1)<<30

	e := newEnv(t)
	e.order.get = func(in *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
		if in.GetUserId() != owner {
			return nil, status.Error(codes.NotFound, "order not found")
		}
		st := orderv1.OrderStatus_ORDER_STATUS_PAID
		if in.GetOrderId()%2 == 1 {
			st = orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT
		}
		return &orderv1.GetOrderResponse{Order: realisticOrder(in.GetOrderId(), owner, st)}, nil
	}
	get := func(user, id int64) (int, string) {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/orders/"+strconv.FormatInt(id, 10), nil)
		req.Header.Set("X-User-Id", strconv.FormatInt(user, 10))
		rec := httptest.NewRecorder()
		e.h.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	// readTwice 读同一张订单两次，返回下游被调了几次；两次的响应必须一样。
	readTwice := func(user, id int64) int {
		t.Helper()
		before := e.order.calls
		c1, b1 := get(user, id)
		c2, b2 := get(user, id)
		if c1 != c2 || b1 != b2 {
			t.Fatalf("order %d read twice gave %d %s, then %d %s", id, c1, b1, c2, b2)
		}
		return e.order.calls - before
	}

	// 关着：终态的订单也每次都问下游。
	if n := readTwice(owner, base); n != 2 {
		t.Fatalf("switch off, settled order: downstream called %d times for 2 reads, want 2", n)
	}

	if err := fault.Set("f02", true, time.Minute); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fault.Set("f02", false, 0) })

	// 开着：没到终态的照常每次问；终态的第二次不问；别的用户读同一个订单号照样问，并且是 404。
	if n := readTwice(owner, base+1); n != 2 {
		t.Errorf("switch on, pending order: downstream called %d times, want 2", n)
	}
	if n := readTwice(owner, base+2); n != 1 {
		t.Errorf("switch on, settled order: downstream called %d times, want 1", n)
	}
	if code, _ := get(owner+1, base+2); code != http.StatusNotFound {
		t.Errorf("switch on, someone else's settled order: status = %d, want 404", code)
	}

	// 症状：每一张读过的终态订单留下一份，之后也不释放。
	// 测试环境的链路导出器把每个 span 都留在内存里，所以同样的读法先在开关关着时量一遍，两次的差才是记下的条目。
	const n = 20000
	heap := func() int64 {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return int64(ms.HeapAlloc)
	}
	readMany := func(from int64) int64 {
		t.Helper()
		before := heap()
		for i := range int64(n) {
			if code, _ := get(owner, from+2*i); code != http.StatusOK {
				t.Fatalf("status = %d, want 200", code)
			}
		}
		return heap() - before
	}
	if err := fault.Set("f02", false, 0); err != nil {
		t.Fatal(err)
	}
	control := readMany(base + 1000)
	if err := fault.Set("f02", true, time.Minute); err != nil {
		t.Fatal(err)
	}
	kept := readMany(base+1000+2*n) - control
	t.Logf("reading %d settled orders kept %d bytes more than with the switch off (%d bytes each)", n, kept, kept/n)
	if kept < n*300 {
		t.Errorf("reading %d settled orders kept %d bytes, want at least %d", n, kept, n*300)
	}

	// 关掉以后回到每次都问；已经记下的不会自己消失，只有重启才回来。
	if err := fault.Set("f02", false, 0); err != nil {
		t.Fatal(err)
	}
	if n := readTwice(owner, base+2); n != 2 {
		t.Errorf("switch off again, settled order: downstream called %d times, want 2", n)
	}
}

var variantRuns atomic.Int64

// realisticOrder 是一张和线上大小差不多的订单：一个商品，三个时间。
func realisticOrder(id, user int64, st orderv1.OrderStatus) *orderv1.Order {
	at := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	o := &orderv1.Order{
		OrderId:    id,
		UserId:     user,
		Status:     st,
		Total:      &commonv1.Money{Amount: 1999, Currency: "CNY"},
		Items:      []*orderv1.OrderItem{{SkuId: 42, Quantity: 1, UnitPrice: &commonv1.Money{Amount: 1999, Currency: "CNY"}}},
		CreateTime: timestamppb.New(at),
		ExpireTime: timestamppb.New(at.Add(15 * time.Minute)),
	}
	if st == orderv1.OrderStatus_ORDER_STATUS_PAID {
		o.PayTime = timestamppb.New(at.Add(time.Minute))
	}
	return o
}
