package service

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/testdb"
	"github.com/palemoky/Oppenheimer/services/inventory"
	"github.com/palemoky/Oppenheimer/services/inventory/internal/store"
)

type env struct {
	db  *sql.DB
	svc *Service
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testdb.New(t, inventory.Migrations, inventory.MigrationsDir)
	return &env{db: db, svc: New(store.New(db), slog.New(slog.NewTextHandler(t.Output(), nil)))}
}

// noRetry 关掉死锁重试。并发测试用它证明场景本身不会死锁，而不是靠重试掩盖。
func (e *env) noRetry() { e.svc.txRetry.MaxAttempts = 1 }

// seed 入库一个 SKU，并在测试结束时断言它的三个量都不为负、总和等于入库量。
// 每个测试都经由它准备数据，因此每个测试都会检查恒等式。
func (e *env) seed(t *testing.T, skuID, price, total int64) {
	t.Helper()
	testdb.MustExec(t, e.db,
		`INSERT INTO sku_stock (sku_id, name, unit_price, available) VALUES (?, ?, ?, ?)`,
		skuID, "sku", price, total)
	t.Cleanup(func() {
		// Cleanup 运行前 t.Context() 已被取消，这里只需要它携带的值。
		s := e.stock(context.WithoutCancel(t.Context()), t, skuID)
		if s.GetAvailable() < 0 || s.GetReserved() < 0 || s.GetSold() < 0 {
			t.Errorf("sku %d has negative stock: %v", skuID, s)
		}
		if sum := s.GetAvailable() + s.GetReserved() + s.GetSold(); sum != total {
			t.Errorf("sku %d: available+reserved+sold = %d, want %d", skuID, sum, total)
		}
	})
}

func (e *env) stock(ctx context.Context, t *testing.T, skuID int64) *inventoryv1.GetStockResponse {
	t.Helper()
	resp, err := e.svc.GetStock(ctx, &inventoryv1.GetStockRequest{SkuId: skuID})
	if err != nil {
		t.Fatalf("GetStock(%d): %v", skuID, err)
	}
	return resp
}

func (e *env) assertStock(t *testing.T, skuID, available, reserved, sold int64) {
	t.Helper()
	s := e.stock(t.Context(), t, skuID)
	if s.GetAvailable() != available || s.GetReserved() != reserved || s.GetSold() != sold {
		t.Errorf("sku %d = (available %d, reserved %d, sold %d), want (%d, %d, %d)",
			skuID, s.GetAvailable(), s.GetReserved(), s.GetSold(), available, reserved, sold)
	}
}

func (e *env) reserve(ctx context.Context, orderID int64, items ...int64) (*inventoryv1.ReserveResponse, error) {
	req := &inventoryv1.ReserveRequest{OrderId: orderID}
	for i := 0; i+1 < len(items); i += 2 {
		req.Items = append(req.Items, &inventoryv1.ReserveItem{SkuId: items[i], Quantity: int32(items[i+1])})
	}
	return e.svc.Reserve(ctx, req)
}

func (e *env) confirm(ctx context.Context, orderID int64) error {
	_, err := e.svc.Confirm(ctx, &inventoryv1.ConfirmRequest{OrderId: orderID})
	return err
}

func (e *env) release(ctx context.Context, orderID int64) error {
	_, err := e.svc.Release(ctx, &inventoryv1.ReleaseRequest{OrderId: orderID})
	return err
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertErr(t *testing.T, err error, code codes.Code, reason string) {
	t.Helper()
	if status.Code(err) != code || !errx.Is(err, reason) {
		t.Fatalf("error = %v, want code %s reason %s", err, code, reason)
	}
}

func TestReserveConcurrentFlashSale(t *testing.T) {
	e := newEnv(t)
	const stock, buyers = 10, 60
	e.seed(t, 1, 100, stock)

	errs := make([]error, buyers)
	var wg sync.WaitGroup
	for i := range buyers {
		wg.Go(func() {
			_, errs[i] = e.reserve(t.Context(), int64(1000+i), 1, 1)
		})
	}
	wg.Wait()

	succeeded := 0
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errx.Is(err, errx.ReasonInsufficientStock):
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if succeeded != stock {
		t.Errorf("succeeded = %d, want %d", succeeded, stock)
	}
	e.assertStock(t, 1, 0, stock, 0)
}

func TestReserveReverseOrderNoDeadlock(t *testing.T) {
	e := newEnv(t)
	e.noRetry()
	const orders = 40
	e.seed(t, 1, 100, orders)
	e.seed(t, 2, 200, orders)

	errs := make([]error, orders)
	var wg sync.WaitGroup
	for i := range orders {
		wg.Go(func() {
			if i%2 == 0 {
				_, errs[i] = e.reserve(t.Context(), int64(1000+i), 1, 1, 2, 1)
			} else {
				_, errs[i] = e.reserve(t.Context(), int64(1000+i), 2, 1, 1, 1)
			}
		})
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("order %d: %v", 1000+i, err)
		}
	}
	e.assertStock(t, 1, 0, orders, 0)
	e.assertStock(t, 2, 0, orders, 0)
}

func TestReserveAllOrNothing(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	e.seed(t, 2, 200, 1)

	_, err := e.reserve(t.Context(), 1, 1, 5, 2, 2)
	assertErr(t, err, codes.FailedPrecondition, errx.ReasonInsufficientStock)
	e.assertStock(t, 1, 10, 0, 0)
	e.assertStock(t, 2, 1, 0, 0)

	_, err = e.reserve(t.Context(), 1, 1, 5, 999, 1)
	assertErr(t, err, codes.NotFound, errx.ReasonSKUNotFound)
	e.assertStock(t, 1, 10, 0, 0)

	// 头记录也随失败回滚了，同一订单之后仍可正常预占。
	resp, err := e.reserve(t.Context(), 1, 1, 5, 2, 1)
	mustOK(t, err)
	if len(resp.GetItems()) != 2 {
		t.Fatalf("items = %v, want 2 items", resp.GetItems())
	}
	e.assertStock(t, 1, 5, 5, 0)
	e.assertStock(t, 2, 0, 1, 0)
}

func TestReserveConcurrentDuplicate(t *testing.T) {
	e := newEnv(t)
	e.noRetry()
	e.seed(t, 1, 100, 100)
	e.seed(t, 2, 200, 100)
	const calls = 20

	resps := make([]*inventoryv1.ReserveResponse, calls)
	errs := make([]error, calls)
	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() {
			resps[i], errs[i] = e.reserve(t.Context(), 42, 1, 3, 2, 4)
		})
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if !proto.Equal(resps[i], resps[0]) {
			t.Errorf("call %d response = %v, want %v", i, resps[i], resps[0])
		}
	}
	e.assertStock(t, 1, 97, 3, 0)
	e.assertStock(t, 2, 96, 4, 0)
}

func TestReserveIdempotent(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	e.seed(t, 2, 250, 10)

	first, err := e.reserve(t.Context(), 7, 2, 1, 1, 2)
	mustOK(t, err)
	want := &inventoryv1.ReserveResponse{Items: []*inventoryv1.ReservedItem{
		{SkuId: 1, Quantity: 2, UnitPrice: toProtoMoney(store.Price{Amount: 100, Currency: "CNY"})},
		{SkuId: 2, Quantity: 1, UnitPrice: toProtoMoney(store.Price{Amount: 250, Currency: "CNY"})},
	}}
	if !proto.Equal(first, want) {
		t.Fatalf("Reserve = %v, want %v", first, want)
	}

	// 项的顺序不同但内容相同，仍是同一个请求。
	again, err := e.reserve(t.Context(), 7, 1, 2, 2, 1)
	mustOK(t, err)
	if !proto.Equal(again, first) {
		t.Errorf("repeated Reserve = %v, want %v", again, first)
	}
	e.assertStock(t, 1, 8, 2, 0)
	e.assertStock(t, 2, 9, 1, 0)

	mismatches := map[string][]int64{
		"different quantity": {1, 3, 2, 1},
		"missing item":       {1, 2},
		"extra item":         {1, 2, 2, 1, 3, 1},
		"different sku":      {1, 2, 3, 1},
	}
	for name, items := range mismatches {
		t.Run(name, func(t *testing.T) {
			_, err := e.reserve(t.Context(), 7, items...)
			assertErr(t, err, codes.AlreadyExists, errx.ReasonReservationMismatch)
		})
	}
	e.assertStock(t, 1, 8, 2, 0)
	e.assertStock(t, 2, 9, 1, 0)

	// 确认之后重复 Reserve 仍返回原结果。
	mustOK(t, e.confirm(t.Context(), 7))
	again, err = e.reserve(t.Context(), 7, 1, 2, 2, 1)
	mustOK(t, err)
	if !proto.Equal(again, first) {
		t.Errorf("Reserve after Confirm = %v, want %v", again, first)
	}
	e.assertStock(t, 1, 8, 0, 2)
	e.assertStock(t, 2, 9, 0, 1)
}

func TestConfirmIdempotent(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	_, err := e.reserve(t.Context(), 7, 1, 3)
	mustOK(t, err)
	for range 3 {
		mustOK(t, e.confirm(t.Context(), 7))
	}
	e.assertStock(t, 1, 7, 0, 3)

	assertErr(t, e.release(t.Context(), 7), codes.FailedPrecondition, errx.ReasonReservationConfirmed)
	e.assertStock(t, 1, 7, 0, 3)
}

func TestConfirmErrors(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	assertErr(t, e.confirm(t.Context(), 7), codes.NotFound, errx.ReasonReservationNotFound)

	_, err := e.reserve(t.Context(), 8, 1, 3)
	mustOK(t, err)
	mustOK(t, e.release(t.Context(), 8))
	assertErr(t, e.confirm(t.Context(), 8), codes.FailedPrecondition, errx.ReasonReservationReleased)
	e.assertStock(t, 1, 10, 0, 0)
}

func TestReleaseIdempotent(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	_, err := e.reserve(t.Context(), 7, 1, 3)
	mustOK(t, err)
	for range 3 {
		mustOK(t, e.release(t.Context(), 7))
	}
	e.assertStock(t, 1, 10, 0, 0)

	_, err = e.reserve(t.Context(), 7, 1, 3)
	assertErr(t, err, codes.FailedPrecondition, errx.ReasonReservationReleased)
	e.assertStock(t, 1, 10, 0, 0)
}

func TestReleaseConcurrentDuplicate(t *testing.T) {
	e := newEnv(t)
	e.noRetry()
	e.seed(t, 1, 100, 10)
	_, err := e.reserve(t.Context(), 7, 1, 3)
	mustOK(t, err)

	const calls = 10
	errs := make([]error, calls)
	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() { errs[i] = e.release(t.Context(), 7) })
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("call %d: %v", i, err)
		}
	}
	e.assertStock(t, 1, 10, 0, 0)
}

func TestReleaseBeforeReserve(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	// 空回滚：没有预占也成功，并留下标记。
	mustOK(t, e.release(t.Context(), 7))
	mustOK(t, e.release(t.Context(), 7))

	// 迟到的 Reserve 被标记挡住，不会悬挂一份没人释放的库存。
	_, err := e.reserve(t.Context(), 7, 1, 3)
	assertErr(t, err, codes.FailedPrecondition, errx.ReasonReservationReleased)
	e.assertStock(t, 1, 10, 0, 0)

	assertErr(t, e.confirm(t.Context(), 7), codes.FailedPrecondition, errx.ReasonReservationReleased)
}

func TestReserveRaceWithRelease(t *testing.T) {
	e := newEnv(t)
	e.noRetry()
	const orders = 30
	e.seed(t, 1, 100, orders)

	reserveErrs := make([]error, orders)
	releaseErrs := make([]error, orders)
	var wg sync.WaitGroup
	for i := range orders {
		orderID := int64(1000 + i)
		wg.Go(func() { _, reserveErrs[i] = e.reserve(t.Context(), orderID, 1, 1) })
		wg.Go(func() { releaseErrs[i] = e.release(t.Context(), orderID) })
	}
	wg.Wait()

	for i := range orders {
		if releaseErrs[i] != nil {
			t.Errorf("order %d Release: %v", 1000+i, releaseErrs[i])
		}
		// Reserve 要么先到并随后被释放，要么被空回滚的标记挡住。
		if err := reserveErrs[i]; err != nil && !errx.Is(err, errx.ReasonReservationReleased) {
			t.Errorf("order %d Reserve: %v", 1000+i, err)
		}
	}
	// 无论谁先到，最终都不能有悬挂的预占。
	e.assertStock(t, 1, orders, 0, 0)
}

func TestConfirmRaceWithRelease(t *testing.T) {
	e := newEnv(t)
	e.noRetry()
	const orders = 30
	e.seed(t, 1, 100, orders)
	for i := range orders {
		_, err := e.reserve(t.Context(), int64(1000+i), 1, 1)
		mustOK(t, err)
	}

	confirmErrs := make([]error, orders)
	releaseErrs := make([]error, orders)
	var wg sync.WaitGroup
	for i := range orders {
		orderID := int64(1000 + i)
		wg.Go(func() { confirmErrs[i] = e.confirm(t.Context(), orderID) })
		wg.Go(func() { releaseErrs[i] = e.release(t.Context(), orderID) })
	}
	wg.Wait()

	var confirmed int64
	for i := range orders {
		cErr, rErr := confirmErrs[i], releaseErrs[i]
		switch {
		case cErr == nil && errx.Is(rErr, errx.ReasonReservationConfirmed):
			confirmed++
		case rErr == nil && errx.Is(cErr, errx.ReasonReservationReleased):
		default:
			t.Errorf("order %d: Confirm = %v, Release = %v, want exactly one to succeed", 1000+i, cErr, rErr)
		}
	}
	e.assertStock(t, 1, orders-confirmed, 0, confirmed)
}

func TestValidation(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	reserves := map[string]*inventoryv1.ReserveRequest{
		"zero order id":  {OrderId: 0, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: 1}}},
		"no items":       {OrderId: 1},
		"zero quantity":  {OrderId: 1, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: 0}}},
		"negative qty":   {OrderId: 1, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: -1}}},
		"duplicate skus": {OrderId: 1, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: 1}, {SkuId: 1, Quantity: 2}}},
	}
	for name, req := range reserves {
		t.Run("reserve "+name, func(t *testing.T) {
			_, err := e.svc.Reserve(t.Context(), req)
			assertErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
		})
	}
	assertErr(t, e.confirm(t.Context(), 0), codes.InvalidArgument, errx.ReasonInvalidArgument)
	assertErr(t, e.release(t.Context(), -1), codes.InvalidArgument, errx.ReasonInvalidArgument)
	e.assertStock(t, 1, 10, 0, 0)
}

func TestGetStock(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 199, 10)

	got := e.stock(t.Context(), t, 1)
	want := &inventoryv1.GetStockResponse{
		SkuId: 1, Available: 10,
		UnitPrice: toProtoMoney(store.Price{Amount: 199, Currency: "CNY"}),
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetStock = %v, want %v", got, want)
	}

	_, err := e.svc.GetStock(t.Context(), &inventoryv1.GetStockRequest{SkuId: 2})
	assertErr(t, err, codes.NotFound, errx.ReasonSKUNotFound)
}

func TestReserveConcurrentDuplicateRejected(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 2)
	const calls = 10

	// 每个插入者都因库存不足回滚，排队的重复请求随之去插入。
	// 这是唯一要靠死锁重试兜底的场景，结果仍必须全部是业务错误。
	errs := make([]error, calls)
	var wg sync.WaitGroup
	for i := range calls {
		wg.Go(func() { _, errs[i] = e.reserve(t.Context(), 42, 1, 3) })
	}
	wg.Wait()

	for i, err := range errs {
		if !errx.Is(err, errx.ReasonInsufficientStock) {
			t.Errorf("call %d: %v, want INSUFFICIENT_STOCK", i, err)
		}
	}
	e.assertStock(t, 1, 2, 0, 0)
}
