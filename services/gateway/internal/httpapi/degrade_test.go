package httpapi_test

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	orderindexv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/orderindex/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi"
)

// 降级响应的 Retry-After（秒），和 httpapi 里的 degradedRetryAfter 一致。
const wantDegradedRetryAfter = "5"

var degradedReasons = []string{
	errx.ReasonStockGateUnavailable,
	errx.ReasonOrderLandingUnavailable,
	errx.ReasonInventoryUnavailable,
	errx.ReasonOrderQueueUnavailable,
	errx.ReasonOrderUnavailable,
	errx.ReasonArchiveUnavailable,
	errx.ReasonFeatureDisabled,
}

func errMessage(t *testing.T, r result) string {
	t.Helper()
	e, ok := r.body["error"].(map[string]any)
	if !ok {
		t.Fatalf("no error object in %s", r.raw)
	}
	return e["message"].(string)
}

// 下游的降级错误（FAILED_PRECONDITION 加降级原因码）是 503 加原因码和 Retry-After，不是 409；
// 不记 ERROR 日志。普通的 FAILED_PRECONDITION 仍然是 409，没有 Retry-After。
func TestDegradedErrorMapping(t *testing.T) {
	for _, reason := range degradedReasons {
		t.Run(reason, func(t *testing.T) {
			e := newEnv(t)
			msg := "dependency down, retry later"
			err := errx.Degraded(reason, "%s", msg)
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("errx.Degraded code = %v, the test assumes FAILED_PRECONDITION", status.Code(err))
			}
			e.order.get = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) { return nil, err }
			r := do(t, e.h, "GET", "/api/v1/orders/1", "7", "")
			if r.status != http.StatusServiceUnavailable || errCode(t, r) != reason {
				t.Fatalf("status = %d code = %s, want 503 %s; body %s", r.status, errCode(t, r), reason, r.raw)
			}
			if got := r.header.Get("Retry-After"); got != wantDegradedRetryAfter {
				t.Errorf("Retry-After = %q, want %q", got, wantDegradedRetryAfter)
			}
			if got := errMessage(t, r); got != msg {
				t.Errorf("message = %q, want the downstream message %q", got, msg)
			}
			if strings.Contains(e.logs.String(), `"level":"ERROR"`) {
				t.Errorf("degraded response logged an error: %s", e.logs.String())
			}
		})
	}

	for _, tt := range []struct {
		name string
		err  error
		code string
	}{
		{"insufficient stock", errx.New(codes.FailedPrecondition, errx.ReasonInsufficientStock, "no stock"), errx.ReasonInsufficientStock},
		{"plain failed precondition", status.Error(codes.FailedPrecondition, "x"), "FAILED_PRECONDITION"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.order.get = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) { return nil, tt.err }
			r := do(t, e.h, "GET", "/api/v1/orders/1", "7", "")
			if r.status != http.StatusConflict || errCode(t, r) != tt.code {
				t.Fatalf("status = %d code = %s, want 409 %s; body %s", r.status, errCode(t, r), tt.code, r.raw)
			}
			if got := r.header.Get("Retry-After"); got != "" {
				t.Errorf("Retry-After = %q, want none", got)
			}
		})
	}
}

// 降级错误在各个接口上的映射相同：下单（受理时依赖不可用）和支付（payment 问不到订单）各走一遍。
func TestDegradedErrorOnWrites(t *testing.T) {
	e := newEnv(t)
	e.order.create = func(*orderv1.SubmitOrderRequest) (*orderv1.SubmitOrderResponse, error) {
		return nil, errx.Degraded(errx.ReasonStockGateUnavailable, "stock gate unavailable")
	}
	e.payment.create = func(*paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
		return nil, errx.Degraded(errx.ReasonOrderUnavailable, "order unavailable")
	}
	for _, tt := range []struct {
		method, path, body string
		hdr                []string
		code               string
	}{
		{"POST", "/api/v1/orders", `{"items":[{"sku_id":"1","quantity":1}]}`, []string{"Idempotency-Key", "k"}, errx.ReasonStockGateUnavailable},
		{"POST", "/api/v1/orders/1/pay", ``, nil, errx.ReasonOrderUnavailable},
	} {
		r := do(t, e.h, tt.method, tt.path, "7", tt.body, tt.hdr...)
		if r.status != http.StatusServiceUnavailable || errCode(t, r) != tt.code || r.header.Get("Retry-After") != wantDegradedRetryAfter {
			t.Errorf("%s %s: status = %d Retry-After = %q, body %s", tt.method, tt.path, r.status, r.header.Get("Retry-After"), r.raw)
		}
	}
}

// switchEnv 是带降级开关的 env，并记录每个下游被调用的次数。
type switchEnv struct {
	*env
	set                                  *degrade.Set
	stockCalls, merchantCalls, userCalls int
}

func newSwitchEnv(t *testing.T) *switchEnv {
	t.Helper()
	e := &switchEnv{env: newEnv(t), set: degrade.New("gateway", slog.New(slog.DiscardHandler))}
	e.h = httpapi.New(httpapi.Deps{Order: e.order, Payment: e.payment, Inventory: e.inventory, OrderIndex: e.orderIndex}, httpapi.Config{
		Logger:      slog.New(slog.NewJSONHandler(e.logs, nil)),
		CallTimeout: time.Second,
		Degrade:     e.set,
	})
	e.inventory.getStock = func(context.Context, *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
		e.stockCalls++
		return &inventoryv1.GetStockResponse{SkuId: 1, Available: 3}, nil
	}
	e.orderIndex.list = func(*orderindexv1.ListMerchantOrdersRequest) (*orderindexv1.ListMerchantOrdersResponse, error) {
		e.merchantCalls++
		return &orderindexv1.ListMerchantOrdersResponse{}, nil
	}
	e.order.list = func(*orderv1.ListUserOrdersRequest) (*orderv1.ListUserOrdersResponse, error) {
		e.userCalls++
		return &orderv1.ListUserOrdersResponse{}, nil
	}
	return e
}

func (e *switchEnv) setMode(t *testing.T, name, mode string) {
	t.Helper()
	if err := e.set.SetMode(name, mode); err != nil {
		t.Fatal(err)
	}
}

func TestFeatureSwitches(t *testing.T) {
	for _, tt := range []struct {
		switchName string
		path       string
		calls      func(*switchEnv) int
	}{
		{httpapi.SwitchMerchantOrders, "/api/v1/merchants/3/orders", func(e *switchEnv) int { return e.merchantCalls }},
		{httpapi.SwitchUserOrders, "/api/v1/orders", func(e *switchEnv) int { return e.userCalls }},
		{httpapi.SwitchStockQuery, "/api/v1/skus/1/stock", func(e *switchEnv) int { return e.stockCalls }},
	} {
		t.Run(tt.switchName, func(t *testing.T) {
			e := newSwitchEnv(t)
			if r := do(t, e.h, "GET", tt.path, "7", ""); r.status != http.StatusOK || tt.calls(e) != 1 {
				t.Fatalf("switch on: status = %d, downstream calls = %d; body %s", r.status, tt.calls(e), r.raw)
			}

			e.setMode(t, tt.switchName, degrade.ModeOff)
			r := do(t, e.h, "GET", tt.path, "7", "")
			if r.status != http.StatusServiceUnavailable || errCode(t, r) != errx.ReasonFeatureDisabled {
				t.Fatalf("switch off: status = %d, body %s", r.status, r.raw)
			}
			if got := r.header.Get("Retry-After"); got != wantDegradedRetryAfter {
				t.Errorf("Retry-After = %q, want %q", got, wantDegradedRetryAfter)
			}
			if tt.calls(e) != 1 {
				t.Errorf("downstream called %d times, want no call while switched off", tt.calls(e)-1)
			}
			// 开关在身份校验之前：没带 X-User-Id 的请求也是 503，不是 401。
			if r := do(t, e.h, "GET", tt.path, "", ""); r.status != http.StatusServiceUnavailable {
				t.Errorf("switch off without user: status = %d, want 503", r.status)
			}

			e.setMode(t, tt.switchName, degrade.ModeAuto)
			if r := do(t, e.h, "GET", tt.path, "7", ""); r.status != http.StatusOK || tt.calls(e) != 2 {
				t.Fatalf("switch back on: status = %d, downstream calls = %d; body %s", r.status, tt.calls(e), r.raw)
			}
		})
	}
}

// 三个开关全关时，核心接口（下单、查单个订单、取消、支付、查支付）照常，别的开关管的接口也只受自己的开关影响。
func TestFeatureSwitchesSpareCoreRoutes(t *testing.T) {
	e := newSwitchEnv(t)
	for _, name := range []string{httpapi.SwitchMerchantOrders, httpapi.SwitchUserOrders, httpapi.SwitchStockQuery} {
		e.setMode(t, name, degrade.ModeOff)
	}
	e.order.create = func(*orderv1.SubmitOrderRequest) (*orderv1.SubmitOrderResponse, error) {
		return &orderv1.SubmitOrderResponse{Order: sampleOrder()}, nil
	}
	e.order.get = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
		return &orderv1.GetOrderResponse{Order: sampleOrder()}, nil
	}
	e.order.cancel = func(*orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
		return &orderv1.CancelOrderResponse{Order: sampleOrder()}, nil
	}
	e.payment.create = func(*paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
		return &paymentv1.CreatePaymentResponse{Payment: &paymentv1.Payment{PaymentId: 1, OrderId: 1}}, nil
	}
	e.payment.get = func(*paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
		return &paymentv1.GetPaymentResponse{Payment: &paymentv1.Payment{PaymentId: 1, OrderId: 1}}, nil
	}
	for _, tt := range []struct {
		method, path, body string
		hdr                []string
		want               int
	}{
		{"POST", "/api/v1/orders", `{"items":[{"sku_id":"1","quantity":1}]}`, []string{"Idempotency-Key", "k"}, http.StatusAccepted},
		{"GET", "/api/v1/orders/1", ``, nil, http.StatusOK},
		{"POST", "/api/v1/orders/1/cancel", ``, nil, http.StatusOK},
		{"POST", "/api/v1/orders/1/pay", ``, nil, http.StatusAccepted},
		{"GET", "/api/v1/orders/1/payment", ``, nil, http.StatusOK},
	} {
		if r := do(t, e.h, tt.method, tt.path, "7", tt.body, tt.hdr...); r.status != tt.want {
			t.Errorf("%s %s with all switches off: status = %d, want %d; body %s", tt.method, tt.path, r.status, tt.want, r.raw)
		}
	}

	// 只关一个：另外两个接口不受影响。
	e.setMode(t, httpapi.SwitchUserOrders, degrade.ModeAuto)
	e.setMode(t, httpapi.SwitchStockQuery, degrade.ModeAuto)
	if r := do(t, e.h, "GET", "/api/v1/orders", "7", ""); r.status != http.StatusOK {
		t.Errorf("user orders with only merchant_orders off: status = %d", r.status)
	}
	if r := do(t, e.h, "GET", "/api/v1/skus/1/stock", "7", ""); r.status != http.StatusOK {
		t.Errorf("stock query with only merchant_orders off: status = %d", r.status)
	}
}
