package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/services/gateway/internal/httpapi"
)

// 超过 2^53 的 ID，用来验证字符串往返不丢精度。
const bigID int64 = 9007199254741994

// 假客户端：嵌入接口，只实现用到的方法，并记录最后一次请求。
type fakeOrder struct {
	orderv1.OrderServiceClient
	create     func(*orderv1.SubmitOrderRequest) (*orderv1.SubmitOrderResponse, error)
	get        func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error)
	cancel     func(*orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error)
	lastCreate *orderv1.SubmitOrderRequest
	lastGet    *orderv1.GetOrderRequest
	lastCancel *orderv1.CancelOrderRequest
	calls      int
}

func (f *fakeOrder) SubmitOrder(_ context.Context, in *orderv1.SubmitOrderRequest, _ ...grpc.CallOption) (*orderv1.SubmitOrderResponse, error) {
	f.calls++
	f.lastCreate = in
	return f.create(in)
}

func (f *fakeOrder) GetOrder(_ context.Context, in *orderv1.GetOrderRequest, _ ...grpc.CallOption) (*orderv1.GetOrderResponse, error) {
	f.calls++
	f.lastGet = in
	return f.get(in)
}

func (f *fakeOrder) CancelOrder(_ context.Context, in *orderv1.CancelOrderRequest, _ ...grpc.CallOption) (*orderv1.CancelOrderResponse, error) {
	f.calls++
	f.lastCancel = in
	return f.cancel(in)
}

type fakePayment struct {
	paymentv1.PaymentServiceClient
	create     func(*paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error)
	get        func(*paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error)
	lastCreate *paymentv1.CreatePaymentRequest
	lastGet    *paymentv1.GetPaymentRequest
}

func (f *fakePayment) CreatePayment(_ context.Context, in *paymentv1.CreatePaymentRequest, _ ...grpc.CallOption) (*paymentv1.CreatePaymentResponse, error) {
	f.lastCreate = in
	return f.create(in)
}

func (f *fakePayment) GetPayment(_ context.Context, in *paymentv1.GetPaymentRequest, _ ...grpc.CallOption) (*paymentv1.GetPaymentResponse, error) {
	f.lastGet = in
	return f.get(in)
}

type fakeInventory struct {
	inventoryv1.InventoryServiceClient
	getStock func(ctx context.Context, in *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error)
}

func (f *fakeInventory) GetStock(ctx context.Context, in *inventoryv1.GetStockRequest, _ ...grpc.CallOption) (*inventoryv1.GetStockResponse, error) {
	return f.getStock(ctx, in)
}

type env struct {
	h         http.Handler
	order     *fakeOrder
	payment   *fakePayment
	inventory *fakeInventory
	logs      *bytes.Buffer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{
		order:     &fakeOrder{},
		payment:   &fakePayment{},
		inventory: &fakeInventory{},
		logs:      &bytes.Buffer{},
	}
	e.h = httpapi.New(httpapi.Deps{Order: e.order, Payment: e.payment, Inventory: e.inventory}, httpapi.Config{
		Logger:      slog.New(slog.NewJSONHandler(e.logs, nil)),
		CallTimeout: time.Second,
	})
	return e
}

func sampleOrder() *orderv1.Order {
	return &orderv1.Order{
		OrderId:    bigID,
		UserId:     7,
		Status:     orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT,
		Total:      &commonv1.Money{Amount: 1999, Currency: "CNY"},
		Items:      []*orderv1.OrderItem{{SkuId: 42, Quantity: 2, UnitPrice: &commonv1.Money{Amount: 999, Currency: "CNY"}}},
		CreateTime: timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
		ExpireTime: timestamppb.New(time.Date(2026, 1, 2, 3, 19, 5, 0, time.UTC)),
	}
}

type result struct {
	status int
	header http.Header
	body   map[string]any
	raw    string
}

// do 发请求并把响应解析成通用 map；user 为空表示不带 X-User-Id。
func do(t *testing.T, h http.Handler, method, path, user, body string, hdr ...string) result {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
	if user != "" {
		req.Header.Set("X-User-Id", user)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	res := result{status: rec.Code, header: rec.Header(), raw: rec.Body.String()}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q, body %s", ct, res.raw)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res.body); err != nil {
		t.Fatalf("response is not a JSON object: %v: %s", err, res.raw)
	}
	return res
}

func errCode(t *testing.T, r result) string {
	t.Helper()
	e, ok := r.body["error"].(map[string]any)
	if !ok {
		t.Fatalf("no error object in %s", r.raw)
	}
	return e["code"].(string)
}

func TestCreateOrder(t *testing.T) {
	e := newEnv(t)
	e.order.create = func(*orderv1.SubmitOrderRequest) (*orderv1.SubmitOrderResponse, error) {
		return &orderv1.SubmitOrderResponse{Order: sampleOrder()}, nil
	}

	// sku_id 分别用字符串和数字，都应被接受。
	r := do(t, e.h, "POST", "/api/v1/orders", "7",
		`{"items":[{"sku_id":"42","quantity":2},{"sku_id":43,"quantity":1}]}`, "Idempotency-Key", "k1")
	// 下单是异步受理：202，并用 Location 告诉客户端去哪里轮询。
	if r.status != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", r.status, r.raw)
	}
	if loc := r.header.Get("Location"); loc != "/api/v1/orders/9007199254741994" {
		t.Errorf("Location = %q", loc)
	}
	in := e.order.lastCreate
	if in.GetUserId() != 7 || in.GetIdempotencyKey() != "k1" || len(in.GetItems()) != 2 ||
		in.GetItems()[0].GetSkuId() != 42 || in.GetItems()[1].GetSkuId() != 43 || in.GetItems()[0].GetQuantity() != 2 {
		t.Errorf("unexpected downstream request: %v", in)
	}

	// 整体比较响应形状：ID 是字符串，金额是整数分，枚举小写，没有 pay_time。
	want := map[string]any{
		"order_id": "9007199254741994",
		"user_id":  "7",
		"status":   "pending_payment",
		"total":    map[string]any{"amount": 1999.0, "currency": "CNY"},
		"items": []any{map[string]any{
			"sku_id": "42", "quantity": 2.0, "unit_price": map[string]any{"amount": 999.0, "currency": "CNY"},
		}},
		"create_time": "2026-01-02T03:04:05Z",
		"expire_time": "2026-01-02T03:19:05Z",
	}
	got, _ := json.Marshal(r.body)
	wantJSON, _ := json.Marshal(want)
	if !jsonEqual(t, got, wantJSON) {
		t.Errorf("body = %s, want %s", got, wantJSON)
	}
}

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatal(err)
	}
	ab, _ := json.Marshal(x, json.Deterministic(true))
	bb, _ := json.Marshal(y, json.Deterministic(true))
	return bytes.Equal(ab, bb)
}

func TestCreateOrderInvalidInput(t *testing.T) {
	longKey := strings.Repeat("k", 65)
	tests := []struct {
		name string
		body string
		hdr  []string
	}{
		{"missing idempotency key", `{"items":[{"sku_id":"1","quantity":1}]}`, nil},
		{"key too long", `{"items":[{"sku_id":"1","quantity":1}]}`, []string{"Idempotency-Key", longKey}},
		{"empty body", ``, []string{"Idempotency-Key", "k"}},
		{"malformed json", `{"items":`, []string{"Idempotency-Key", "k"}},
		{"unknown field", `{"items":[{"sku_id":"1","quantity":1}],"extra":1}`, []string{"Idempotency-Key", "k"}},
		{"unknown nested field", `{"items":[{"sku_id":"1","quantity":1,"price":1}]}`, []string{"Idempotency-Key", "k"}},
		{"trailing data", `{"items":[{"sku_id":"1","quantity":1}]} {}`, []string{"Idempotency-Key", "k"}},
		{"no items", `{"items":[]}`, []string{"Idempotency-Key", "k"}},
		{"zero quantity", `{"items":[{"sku_id":"1","quantity":0}]}`, []string{"Idempotency-Key", "k"}},
		{"negative quantity", `{"items":[{"sku_id":"1","quantity":-1}]}`, []string{"Idempotency-Key", "k"}},
		{"non-numeric sku", `{"items":[{"sku_id":"abc","quantity":1}]}`, []string{"Idempotency-Key", "k"}},
		{"fractional sku", `{"items":[{"sku_id":1.5,"quantity":1}]}`, []string{"Idempotency-Key", "k"}},
		{"zero sku", `{"items":[{"sku_id":"0","quantity":1}]}`, []string{"Idempotency-Key", "k"}},
		{"quantity overflow", `{"items":[{"sku_id":"1","quantity":99999999999}]}`, []string{"Idempotency-Key", "k"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			r := do(t, e.h, "POST", "/api/v1/orders", "7", tt.body, tt.hdr...)
			if r.status != http.StatusBadRequest || errCode(t, r) != "INVALID_ARGUMENT" {
				t.Errorf("status = %d, body %s", r.status, r.raw)
			}
			if e.order.calls != 0 {
				t.Error("downstream must not be called for invalid input")
			}
		})
	}
}

func TestBodyTooLarge(t *testing.T) {
	e := newEnv(t)
	body := `{"items":[{"sku_id":"1","quantity":1}],"pad":"` + strings.Repeat("x", 2<<20) + `"}`
	r := do(t, e.h, "POST", "/api/v1/orders", "7", body, "Idempotency-Key", "k")
	if r.status != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, body %.200s", r.status, r.raw)
	}
	if e.order.calls != 0 {
		t.Error("downstream must not be called")
	}
}

func TestUserIDHeader(t *testing.T) {
	routes := []struct{ method, path string }{
		{"POST", "/api/v1/orders"},
		{"GET", "/api/v1/orders/1"},
		{"POST", "/api/v1/orders/1/cancel"},
		{"POST", "/api/v1/orders/1/pay"},
		{"GET", "/api/v1/orders/1/payment"},
		{"GET", "/api/v1/skus/1/stock"},
	}
	for _, rt := range routes {
		for _, user := range []string{"", "abc", "0", "-3", "1.5"} {
			t.Run(rt.method+" "+rt.path+" user="+user, func(t *testing.T) {
				e := newEnv(t)
				r := do(t, e.h, rt.method, rt.path, user, `{}`, "Idempotency-Key", "k")
				if r.status != http.StatusUnauthorized {
					t.Errorf("status = %d, body %s", r.status, r.raw)
				}
			})
		}
	}
}

func TestGetOrder(t *testing.T) {
	e := newEnv(t)
	paid := sampleOrder()
	paid.Status = orderv1.OrderStatus_ORDER_STATUS_PAID
	paid.PayTime = timestamppb.New(time.Date(2026, 1, 2, 3, 5, 0, 0, time.UTC))
	e.order.get = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
		return &orderv1.GetOrderResponse{Order: paid}, nil
	}

	r := do(t, e.h, "GET", "/api/v1/orders/9007199254741994", "7", "")
	if r.status != http.StatusOK {
		t.Fatalf("status = %d, body %s", r.status, r.raw)
	}
	if e.order.lastGet.GetOrderId() != 9007199254741994 || e.order.lastGet.GetUserId() != 7 {
		t.Errorf("unexpected downstream request: %v", e.order.lastGet)
	}
	if r.body["order_id"] != "9007199254741994" || r.body["status"] != "paid" || r.body["pay_time"] != "2026-01-02T03:05:00Z" {
		t.Errorf("body = %s", r.raw)
	}
	// 原始文本里 ID 必须带引号，不能只靠解析后的类型判断。
	if !strings.Contains(r.raw, `"order_id":"9007199254741994"`) {
		t.Errorf("order_id is not a JSON string: %s", r.raw)
	}
}

func TestCancelOrder(t *testing.T) {
	e := newEnv(t)
	cancelled := sampleOrder()
	cancelled.Status = orderv1.OrderStatus_ORDER_STATUS_CANCELLED
	e.order.cancel = func(*orderv1.CancelOrderRequest) (*orderv1.CancelOrderResponse, error) {
		return &orderv1.CancelOrderResponse{Order: cancelled}, nil
	}
	r := do(t, e.h, "POST", "/api/v1/orders/5/cancel", "7", "")
	if r.status != http.StatusOK || r.body["status"] != "cancelled" {
		t.Errorf("status = %d, body %s", r.status, r.raw)
	}
	if e.order.lastCancel.GetOrderId() != 5 || e.order.lastCancel.GetUserId() != 7 {
		t.Errorf("unexpected downstream request: %v", e.order.lastCancel)
	}
}

func TestPayOrder(t *testing.T) {
	pay := &paymentv1.Payment{
		PaymentId:  bigID + 1,
		OrderId:    bigID,
		UserId:     7,
		Status:     paymentv1.PaymentStatus_PAYMENT_STATUS_PENDING,
		Amount:     &commonv1.Money{Amount: 1999, Currency: "CNY"},
		CreateTime: timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
	}
	tests := []struct {
		name string
		body string
		want paymentv1.SimulatedOutcome
	}{
		{"no body", ``, paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_UNSPECIFIED},
		{"empty object", `{}`, paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_UNSPECIFIED},
		{"success", `{"outcome":"success"}`, paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_SUCCESS},
		{"failure", `{"outcome":"failure"}`, paymentv1.SimulatedOutcome_SIMULATED_OUTCOME_FAILURE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.payment.create = func(*paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
				return &paymentv1.CreatePaymentResponse{Payment: pay}, nil
			}
			r := do(t, e.h, "POST", "/api/v1/orders/5/pay", "7", tt.body)
			if r.status != http.StatusAccepted {
				t.Fatalf("status = %d, body %s", r.status, r.raw)
			}
			if got := e.payment.lastCreate; got.GetOrderId() != 5 || got.GetUserId() != 7 || got.GetOutcome() != tt.want {
				t.Errorf("unexpected downstream request: %v", got)
			}
			if !strings.Contains(r.raw, `"payment_id":"9007199254741995"`) || !strings.Contains(r.raw, `"order_id":"9007199254741994"`) {
				t.Errorf("ids are not JSON strings: %s", r.raw)
			}
			if r.body["status"] != "pending" || r.body["notified"] != false {
				t.Errorf("body = %s", r.raw)
			}
		})
	}
}

func TestPayOrderInvalidInput(t *testing.T) {
	for name, body := range map[string]string{
		"bad outcome":   `{"outcome":"maybe"}`,
		"unknown field": `{"outcome":"success","amount":1}`,
		"malformed":     `{`,
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			r := do(t, e.h, "POST", "/api/v1/orders/5/pay", "7", body)
			if r.status != http.StatusBadRequest || errCode(t, r) != "INVALID_ARGUMENT" {
				t.Errorf("status = %d, body %s", r.status, r.raw)
			}
		})
	}
}

func TestGetPayment(t *testing.T) {
	e := newEnv(t)
	e.payment.get = func(*paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
		return &paymentv1.GetPaymentResponse{Payment: &paymentv1.Payment{
			PaymentId: 9, OrderId: 5, UserId: 7,
			Status: paymentv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED,
			Amount: &commonv1.Money{Amount: 100, Currency: "CNY"}, Notified: true,
		}}, nil
	}
	r := do(t, e.h, "GET", "/api/v1/orders/5/payment", "7", "")
	if r.status != http.StatusOK || r.body["status"] != "succeeded" || r.body["notified"] != true || r.body["payment_id"] != "9" {
		t.Errorf("status = %d, body %s", r.status, r.raw)
	}
	if e.payment.lastGet.GetOrderId() != 5 || e.payment.lastGet.GetUserId() != 7 {
		t.Errorf("unexpected downstream request: %v", e.payment.lastGet)
	}
}

func TestGetStock(t *testing.T) {
	e := newEnv(t)
	e.inventory.getStock = func(_ context.Context, in *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
		return &inventoryv1.GetStockResponse{
			SkuId: in.GetSkuId(), Available: 10, Reserved: 2, Sold: 3,
			UnitPrice: &commonv1.Money{Amount: 999, Currency: "CNY"},
		}, nil
	}
	r := do(t, e.h, "GET", "/api/v1/skus/9007199254741994/stock", "7", "")
	if r.status != http.StatusOK {
		t.Fatalf("status = %d, body %s", r.status, r.raw)
	}
	want := `{"sku_id":"9007199254741994","available":10,"reserved":2,"sold":3,"unit_price":{"amount":999,"currency":"CNY"}}`
	if !jsonEqual(t, []byte(r.raw), []byte(want)) || !strings.Contains(r.raw, `"sku_id":"9007199254741994"`) {
		t.Errorf("body = %s", r.raw)
	}
}

func TestInvalidPathIDs(t *testing.T) {
	for _, p := range []string{"abc", "0", "-1", "1.5", "99999999999999999999"} {
		for _, rt := range []struct{ method, path string }{
			{"GET", "/api/v1/orders/" + p},
			{"POST", "/api/v1/orders/" + p + "/cancel"},
			{"POST", "/api/v1/orders/" + p + "/pay"},
			{"GET", "/api/v1/orders/" + p + "/payment"},
			{"GET", "/api/v1/skus/" + p + "/stock"},
		} {
			t.Run(rt.method+" "+rt.path, func(t *testing.T) {
				e := newEnv(t)
				r := do(t, e.h, rt.method, rt.path, "7", "")
				if r.status != http.StatusBadRequest || errCode(t, r) != "INVALID_ARGUMENT" {
					t.Errorf("status = %d, body %s", r.status, r.raw)
				}
			})
		}
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"invalid argument", errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument, "bad"), 400, "INVALID_ARGUMENT"},
		{"sku not found", errx.New(codes.NotFound, errx.ReasonSKUNotFound, "no sku"), 404, "SKU_NOT_FOUND"},
		{"order not found", errx.New(codes.NotFound, errx.ReasonOrderNotFound, "no order"), 404, "ORDER_NOT_FOUND"},
		{"key reused", errx.New(codes.AlreadyExists, errx.ReasonIdempotencyKeyReused, "reused"), 409, "IDEMPOTENCY_KEY_REUSED"},
		{"not cancellable", errx.New(codes.FailedPrecondition, errx.ReasonOrderNotCancellable, "no"), 409, "ORDER_NOT_CANCELLABLE"},
		{"insufficient stock", errx.New(codes.FailedPrecondition, errx.ReasonInsufficientStock, "no stock"), 409, "INSUFFICIENT_STOCK"},
		{"unavailable", errx.New(codes.Unavailable, "", "down"), 503, "UNAVAILABLE"},
		{"deadline", status.Error(codes.DeadlineExceeded, "slow"), 504, "DEADLINE_EXCEEDED"},
		{"plain failed precondition", status.Error(codes.FailedPrecondition, "x"), 409, "FAILED_PRECONDITION"},
		{"plain already exists", status.Error(codes.AlreadyExists, "x"), 409, "ALREADY_EXISTS"},
		{"internal", status.Error(codes.Internal, "sql: secret table"), 500, "INTERNAL"},
		{"unknown non-grpc error", io.ErrUnexpectedEOF, 500, "UNKNOWN"},
		{"permission denied", status.Error(codes.PermissionDenied, "secret detail"), 500, "PERMISSION_DENIED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.order.get = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) { return nil, tt.err }
			r := do(t, e.h, "GET", "/api/v1/orders/1", "7", "")
			if r.status != tt.wantStatus || errCode(t, r) != tt.wantCode {
				t.Errorf("status = %d code = %s, want %d %s; body %s", r.status, errCode(t, r), tt.wantStatus, tt.wantCode, r.raw)
			}
			if tt.wantStatus == 500 {
				for _, leak := range []string{"secret", "sql:", "unexpected EOF"} {
					if strings.Contains(r.raw, leak) {
						t.Errorf("500 response leaks internal message: %s", r.raw)
					}
				}
			}
		})
	}
}

// 下游返回空数据违反契约，应当是 500 而不是 nil 指针 panic 或空 200。
func TestEmptyDownstreamResponse(t *testing.T) {
	e := newEnv(t)
	e.order.get = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
		return &orderv1.GetOrderResponse{}, nil
	}
	r := do(t, e.h, "GET", "/api/v1/orders/1", "7", "")
	if r.status != http.StatusInternalServerError {
		t.Errorf("status = %d, body %s", r.status, r.raw)
	}
}

func TestDownstreamDeadline(t *testing.T) {
	e := newEnv(t)
	e.inventory.getStock = func(ctx context.Context, _ *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
		dl, ok := ctx.Deadline()
		if !ok || time.Until(dl) > time.Second {
			t.Errorf("downstream context deadline = %v, %v; want within configured timeout", dl, ok)
		}
		return nil, status.FromContextError(context.DeadlineExceeded).Err()
	}
	r := do(t, e.h, "GET", "/api/v1/skus/1/stock", "7", "")
	if r.status != http.StatusGatewayTimeout {
		t.Errorf("status = %d, body %s", r.status, r.raw)
	}
}

func TestUnknownRouteAndMethod(t *testing.T) {
	e := newEnv(t)
	if r := do(t, e.h, "GET", "/nope", "7", ""); r.status != http.StatusNotFound || errCode(t, r) != "NOT_FOUND" {
		t.Errorf("unknown route: status = %d, body %s", r.status, r.raw)
	}
	if r := do(t, e.h, "DELETE", "/api/v1/orders/1", "7", ""); r.status != http.StatusMethodNotAllowed {
		t.Errorf("wrong method: status = %d, body %s", r.status, r.raw)
	}
}

func TestPanicRecovered(t *testing.T) {
	e := newEnv(t)
	e.order.get = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) { panic("boom") }
	r := do(t, e.h, "GET", "/api/v1/orders/1", "7", "")
	if r.status != http.StatusInternalServerError || strings.Contains(r.raw, "boom") {
		t.Errorf("status = %d, body %s", r.status, r.raw)
	}
}

func TestRequestLog(t *testing.T) {
	e := newEnv(t)
	e.order.get = func(*orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
		return nil, errx.New(codes.NotFound, errx.ReasonOrderNotFound, "no order")
	}
	do(t, e.h, "GET", "/api/v1/orders/12345", "7", "")

	var rec struct {
		Msg      string `json:"msg"`
		Method   string `json:"method"`
		Route    string `json:"route"`
		Status   int    `json:"status"`
		Duration int64  `json:"duration"`
	}
	// 日志里可能还有其他条目，找到请求日志那条。
	for line := range bytes.Lines(e.logs.Bytes()) {
		r2 := rec
		if err := json.Unmarshal(line, &r2); err == nil && r2.Msg == "http request" {
			rec = r2
		}
	}
	if rec.Method != "GET" || rec.Route != "/api/v1/orders/:order_id" || rec.Status != 404 {
		t.Errorf("request log = %+v; logs: %s", rec, e.logs)
	}
	if strings.Contains(e.logs.String(), "12345") {
		t.Errorf("log should use the route template, not the concrete path: %s", e.logs)
	}
}
