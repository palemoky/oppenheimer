package service

import (
	"database/sql"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/idgen"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/services/order"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

// 测试用商品。
const (
	skuA       = 1 // 单价 100
	skuB       = 2 // 单价 250
	skuScarce  = 3 // 只有 1 件
	skuHuge    = 4 // 单价接近 int64 上限的一半
	skuMissing = 99

	priceA = 100
	priceB = 250
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type env struct {
	db      *sql.DB
	store   *store.Store
	inv     *fakeInventory
	clock   *fakeClock
	svc     *Service
	sweeper *Sweeper
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testdb.New(t, order.Migrations, order.MigrationsDir)
	inv := newFakeInventory()
	inv.addSKU(skuA, priceA, 1000)
	inv.addSKU(skuB, priceB, 1000)
	inv.addSKU(skuScarce, 10, 1)
	inv.addSKU(skuHuge, 1<<62, 1000)

	ids, err := idgen.New(0)
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{t: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
	logger := slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug}))
	st := store.New(db)
	svc := New(st, inv, ids, logger, Config{Clock: clock.Now})
	return &env{
		db:      db,
		store:   st,
		inv:     inv,
		clock:   clock,
		svc:     svc,
		sweeper: NewSweeper(svc, SweeperConfig{}),
	}
}

func items(pairs ...int64) []*orderv1.CreateOrderItem {
	var out []*orderv1.CreateOrderItem
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &orderv1.CreateOrderItem{SkuId: pairs[i], Quantity: int32(pairs[i+1])})
	}
	return out
}

func createReq(userID int64, key string, it []*orderv1.CreateOrderItem) *orderv1.CreateOrderRequest {
	return &orderv1.CreateOrderRequest{UserId: userID, IdempotencyKey: key, Items: it}
}

// mustPending 下一个 skuA×2 的订单并确认它进入待支付。
func (e *env) mustPending(t *testing.T, userID int64, key string) *orderv1.Order {
	t.Helper()
	resp, err := e.svc.CreateOrder(t.Context(), createReq(userID, key, items(skuA, 2)))
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if got := resp.GetOrder().GetStatus(); got != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
		t.Fatalf("status = %s, want PENDING_PAYMENT", got)
	}
	return resp.GetOrder()
}

func (e *env) load(t *testing.T, orderID int64) *store.Order {
	t.Helper()
	o, err := e.store.GetOrder(t.Context(), orderID)
	if err != nil {
		t.Fatalf("load order %d: %v", orderID, err)
	}
	return o
}

func (e *env) loadByKey(t *testing.T, userID int64, key string) *store.Order {
	t.Helper()
	o, err := e.store.GetOrderByIdempotencyKey(t.Context(), userID, key)
	if err != nil {
		t.Fatalf("load order by key: %v", err)
	}
	return o
}

func (e *env) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.db.QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *env) pay(t *testing.T, orderID, paymentID, amount int64, succeeded bool) (orderv1.PaymentDisposition, error) {
	t.Helper()
	resp, err := e.svc.HandlePaymentResult(t.Context(), &orderv1.HandlePaymentResultRequest{
		OrderId:   orderID,
		PaymentId: paymentID,
		Succeeded: succeeded,
		Amount:    &commonv1.Money{Amount: amount, Currency: "CNY"},
	})
	return resp.GetDisposition(), err
}

func wantErr(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	if errx.Code(err) != code || errx.Reason(err) != reason {
		t.Fatalf("err = %v (code %s, reason %q), want code %s reason %q",
			err, errx.Code(err), errx.Reason(err), code, reason)
	}
}

func wantStatus(t *testing.T, o *store.Order, st orderv1.OrderStatus, settled bool) {
	t.Helper()
	if o.Status != st || o.StockSettled != settled {
		t.Fatalf("order %d: status=%s settled=%v, want status=%s settled=%v",
			o.OrderID, o.Status, o.StockSettled, st, settled)
	}
}

func wantStock(t *testing.T, inv *fakeInventory, sku, available, reserved, sold int64) {
	t.Helper()
	s := inv.stock(sku)
	if s.available != available || s.reserved != reserved || s.sold != sold {
		t.Fatalf("sku %d stock = %d/%d/%d, want %d/%d/%d (available/reserved/sold)",
			sku, s.available, s.reserved, s.sold, available, reserved, sold)
	}
}
