package service

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
)

func TestCreateOrderValidation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	cases := map[string]*orderv1.CreateOrderRequest{
		"zero user":     createReq(0, "k", items(skuA, 1)),
		"empty key":     createReq(1, "", items(skuA, 1)),
		"long key":      createReq(1, strings.Repeat("k", 65), items(skuA, 1)),
		"no items":      createReq(1, "k", nil),
		"zero quantity": createReq(1, "k", items(skuA, 0)),
		"negative qty":  createReq(1, "k", items(skuA, -1)),
		"zero sku":      createReq(1, "k", items(0, 1)),
		"duplicate sku": createReq(1, "k", items(skuA, 1, skuB, 1, skuA, 2)),
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := e.svc.CreateOrder(t.Context(), req)
			wantErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
		})
	}
	if n := e.count(t, "SELECT COUNT(*) FROM orders"); n != 0 {
		t.Errorf("orders = %d after invalid requests, want 0", n)
	}
	if _, err := e.svc.CreateOrder(t.Context(), createReq(1, strings.Repeat("k", 64), items(skuA, 1))); err != nil {
		t.Errorf("64-char key rejected: %v", err)
	}
}

func TestCreateOrderSuccess(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp, err := e.svc.CreateOrder(t.Context(), createReq(7, "k1", items(skuB, 3, skuA, 2)))
	if err != nil {
		t.Fatal(err)
	}
	o := resp.GetOrder()
	if o.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
		t.Fatalf("status = %s", o.GetStatus())
	}
	if want := int64(2*priceA + 3*priceB); o.GetTotal().GetAmount() != want || o.GetTotal().GetCurrency() != "CNY" {
		t.Errorf("total = %v, want %d CNY", o.GetTotal(), want)
	}
	if got, want := o.GetExpireTime().AsTime(), e.clock.Now().Add(DefaultPaymentTimeout); !got.Equal(want) {
		t.Errorf("expire = %s, want %s", got, want)
	}
	if len(o.GetItems()) != 2 || o.GetItems()[0].GetUnitPrice().GetAmount() != priceA || o.GetItems()[1].GetUnitPrice().GetAmount() != priceB {
		t.Errorf("items = %v", o.GetItems())
	}
	stored := e.load(t, o.GetOrderId())
	wantStatus(t, stored, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT, false)
	if stored.Items[0].UnitPrice != priceA || stored.Items[1].UnitPrice != priceB {
		t.Errorf("stored unit prices = %+v", stored.Items)
	}
	wantStock(t, e.inv, skuA, 998, 2, 0)
	wantStock(t, e.inv, skuB, 997, 3, 0)

	got, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: o.GetOrderId(), UserId: 7})
	if err != nil || got.GetOrder().GetTotal().GetAmount() != o.GetTotal().GetAmount() {
		t.Errorf("GetOrder = %v, %v", got, err)
	}
}

func TestCreateOrderIdempotent(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	first, err := e.svc.CreateOrder(t.Context(), createReq(1, "k", items(skuA, 2, skuB, 1)))
	if err != nil {
		t.Fatal(err)
	}
	// 商品顺序不同但内容相同，仍是同一个请求。
	second, err := e.svc.CreateOrder(t.Context(), createReq(1, "k", items(skuB, 1, skuA, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if first.GetOrder().GetOrderId() != second.GetOrder().GetOrderId() {
		t.Fatalf("order ids differ: %d vs %d", first.GetOrder().GetOrderId(), second.GetOrder().GetOrderId())
	}
	if n := e.inv.appliedCount("Reserve"); n != 1 {
		t.Errorf("reserve applied %d times, want 1", n)
	}
	// 已是待支付的订单直接作答，不再调用 Reserve。
	if n := e.inv.callCount("Reserve"); n != 1 {
		t.Errorf("reserve called %d times, want 1", n)
	}
	wantStock(t, e.inv, skuA, 998, 2, 0)

	// 同一幂等键属于另一个用户时互不相干。
	other, err := e.svc.CreateOrder(t.Context(), createReq(2, "k", items(skuA, 1)))
	if err != nil || other.GetOrder().GetOrderId() == first.GetOrder().GetOrderId() {
		t.Errorf("other user's order = %v, %v", other, err)
	}
}

func TestCreateOrderIdempotencyKeyReused(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, err := e.svc.CreateOrder(t.Context(), createReq(1, "k", items(skuA, 2))); err != nil {
		t.Fatal(err)
	}
	for _, it := range [][]*orderv1.CreateOrderItem{items(skuA, 3), items(skuB, 2), items(skuA, 2, skuB, 1)} {
		_, err := e.svc.CreateOrder(t.Context(), createReq(1, "k", it))
		wantErr(t, err, codes.AlreadyExists, errx.ReasonIdempotencyKeyReused)
	}
	if n := e.count(t, "SELECT COUNT(*) FROM orders"); n != 1 {
		t.Errorf("orders = %d, want 1", n)
	}
	if n := e.inv.callCount("Reserve"); n != 1 {
		t.Errorf("reserve called %d times, want 1", n)
	}
}

func TestCreateOrderConcurrentSameKey(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const n = 16
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		ids   = map[int64]int{}
		start = make(chan struct{})
	)
	for range n {
		wg.Go(func() {
			<-start
			resp, err := e.svc.CreateOrder(t.Context(), createReq(1, "same", items(skuA, 2)))
			if err != nil {
				t.Errorf("CreateOrder: %v", err)
				return
			}
			mu.Lock()
			ids[resp.GetOrder().GetOrderId()]++
			mu.Unlock()
		})
	}
	close(start)
	wg.Wait()
	if len(ids) != 1 {
		t.Fatalf("got %d distinct order ids, want 1: %v", len(ids), ids)
	}
	if c := e.count(t, "SELECT COUNT(*) FROM orders"); c != 1 {
		t.Errorf("orders = %d, want 1", c)
	}
	if c := e.count(t, "SELECT COUNT(*) FROM order_items"); c != 1 {
		t.Errorf("order_items = %d, want 1", c)
	}
	if c := e.inv.appliedCount("Reserve"); c != 1 {
		t.Errorf("reserve applied %d times, want 1", c)
	}
	wantStock(t, e.inv, skuA, 998, 2, 0)
}

func TestCreateOrderRejected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		items  []*orderv1.CreateOrderItem
		inject error
		code   codes.Code
		reason string
	}{
		{"sku not found", items(skuA, 1, skuMissing, 1), nil, codes.NotFound, errx.ReasonSKUNotFound},
		{"insufficient stock", items(skuScarce, 2), nil, codes.FailedPrecondition, errx.ReasonInsufficientStock},
		{
			"reservation released", items(skuA, 1),
			errx.New(codes.FailedPrecondition, errx.ReasonReservationReleased, "released"),
			codes.FailedPrecondition, errx.ReasonReservationReleased,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			if tc.inject != nil {
				e.inv.inject("Reserve", fault{err: tc.inject})
			}
			req := createReq(1, "k", tc.items)
			_, err := e.svc.CreateOrder(t.Context(), req)
			wantErr(t, err, tc.code, tc.reason)

			o := e.loadByKey(t, 1, "k")
			wantStatus(t, o, orderv1.OrderStatus_ORDER_STATUS_FAILED, true)
			if o.FailReason != tc.reason {
				t.Errorf("fail_reason = %q, want %q", o.FailReason, tc.reason)
			}

			// 重复请求得到同样的错误，且不再调用库存。
			for range 3 {
				_, err := e.svc.CreateOrder(t.Context(), req)
				wantErr(t, err, tc.code, tc.reason)
			}
			if n := e.inv.callCount("Reserve"); n != 1 {
				t.Errorf("reserve called %d times, want 1", n)
			}
			// “已释放”的拒绝无需再调 Release；另外两种拒绝在线上路径里已经 Release 过一次并结清。
			// 无论哪种，后台任务都不会再去释放。
			wantReleases := 1
			if tc.reason == errx.ReasonReservationReleased {
				wantReleases = 0
			}
			e.clock.Advance(time.Hour)
			e.sweeper.RunOnce(t.Context())
			if n := e.inv.callCount("Release"); n != wantReleases {
				t.Errorf("release called %d times, want %d", n, wantReleases)
			}
		})
	}
}

func TestCreateOrderReservationMismatchKeepsCreating(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.inv.inject("Reserve", fault{err: errx.New(codes.AlreadyExists, errx.ReasonReservationMismatch, "mismatch")})
	_, err := e.svc.CreateOrder(t.Context(), createReq(1, "k", items(skuA, 1)))
	if errx.Code(err) != codes.Internal {
		t.Fatalf("err = %v, want Internal", err)
	}
	wantStatus(t, e.loadByKey(t, 1, "k"), orderv1.OrderStatus_ORDER_STATUS_CREATING, false)
}

func TestCreateOrderReserveTimeoutThenRetry(t *testing.T) {
	t.Parallel()
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%v", applied), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			if applied {
				e.inv.inject("Reserve", fault{errAfterApply: errTimeout})
			} else {
				e.inv.inject("Reserve", fault{err: errTimeout})
			}
			req := createReq(1, "k", items(skuA, 2))
			_, err := e.svc.CreateOrder(t.Context(), req)
			if errx.Code(err) != codes.Unavailable {
				t.Fatalf("err = %v, want Unavailable", err)
			}
			wantStatus(t, e.loadByKey(t, 1, "k"), orderv1.OrderStatus_ORDER_STATUS_CREATING, false)

			resp, err := e.svc.CreateOrder(t.Context(), req)
			if err != nil {
				t.Fatalf("retry: %v", err)
			}
			if resp.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT {
				t.Fatalf("status = %s", resp.GetOrder().GetStatus())
			}
			if n := e.inv.appliedCount("Reserve"); n != 1 {
				t.Errorf("reserve applied %d times, want 1", n)
			}
			wantStock(t, e.inv, skuA, 998, 2, 0)
		})
	}
}

func TestCreateOrderReserveTimeoutWithoutRetry(t *testing.T) {
	t.Parallel()
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%v", applied), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			if applied {
				e.inv.inject("Reserve", fault{errAfterApply: errTimeout})
			} else {
				e.inv.inject("Reserve", fault{err: errTimeout})
			}
			req := createReq(1, "k", items(skuA, 2))
			if _, err := e.svc.CreateOrder(t.Context(), req); errx.Code(err) != codes.Unavailable {
				t.Fatalf("err = %v, want Unavailable", err)
			}
			id := e.loadByKey(t, 1, "k").OrderID

			// 未到创建时限不动它。
			e.clock.Advance(DefaultCreateTimeout - time.Second)
			e.sweeper.RunOnce(t.Context())
			wantStatus(t, e.load(t, id), orderv1.OrderStatus_ORDER_STATUS_CREATING, false)

			e.clock.Advance(2 * time.Second)
			e.sweeper.RunOnce(t.Context())
			o := e.load(t, id)
			wantStatus(t, o, orderv1.OrderStatus_ORDER_STATUS_FAILED, true)
			if o.FailReason != ReasonCreateTimeout {
				t.Errorf("fail_reason = %q", o.FailReason)
			}
			if n := e.inv.callCount("Release"); n != 1 {
				t.Errorf("release called %d times, want 1", n)
			}
			if e.inv.state(id) != resReleased {
				t.Errorf("reservation state = %d, want released", e.inv.state(id))
			}
			wantStock(t, e.inv, skuA, 1000, 0, 0)

			// 之后用同一幂等键重试，得到创建超时的错误，不会再预占。
			_, err := e.svc.CreateOrder(t.Context(), req)
			wantErr(t, err, codes.Aborted, ReasonCreateTimeout)
			if n := e.inv.callCount("Reserve"); n != 1 {
				t.Errorf("reserve called %d times, want 1", n)
			}
		})
	}
}

// 在线路径的 Reserve 还在路上时，后台任务按超时把订单置为失败并释放；
// 迟到的 Reserve 被空回滚标记挡住，库存不会泄漏。
func TestCreateOrderLateReserveAfterTimeout(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.inv.inject("Reserve", fault{before: func(int64) {
		e.clock.Advance(DefaultCreateTimeout + time.Second)
		e.sweeper.RunOnce(t.Context())
	}})
	_, err := e.svc.CreateOrder(t.Context(), createReq(1, "k", items(skuA, 2)))
	wantErr(t, err, codes.Aborted, ReasonCreateTimeout)
	wantStatus(t, e.loadByKey(t, 1, "k"), orderv1.OrderStatus_ORDER_STATUS_FAILED, true)
	wantStock(t, e.inv, skuA, 1000, 0, 0)
}

// 同一订单的两次推进并发：另一次因库存不足把订单置为 FAILED（未结清），
// 还没来得及 Release 就崩溃了；随后本次的 Reserve 成功。
// 这份预占必须由后台任务释放，不能泄漏。
func TestCreateOrderReserveSucceededOnFailedOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.inv.inject("Reserve", fault{before: func(orderID int64) {
		if _, err := e.store.MarkCreateFailed(t.Context(), orderID, errx.ReasonInsufficientStock, false, e.clock.Now()); err != nil {
			t.Error(err)
		}
	}})
	_, err := e.svc.CreateOrder(t.Context(), createReq(1, "k", items(skuA, 2)))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonInsufficientStock)
	o := e.loadByKey(t, 1, "k")
	wantStatus(t, o, orderv1.OrderStatus_ORDER_STATUS_FAILED, false)
	wantStock(t, e.inv, skuA, 998, 2, 0)

	e.clock.Advance(DefaultSettleGrace + time.Second)
	e.sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, o.OrderID), orderv1.OrderStatus_ORDER_STATUS_FAILED, true)
	wantStock(t, e.inv, skuA, 1000, 0, 0)
}

func TestCreateOrderInvalidReserveResult(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	// 单价 2^62 × 2 溢出 int64。
	_, err := e.svc.CreateOrder(t.Context(), createReq(1, "overflow", items(skuHuge, 2)))
	if errx.Code(err) != codes.Internal {
		t.Fatalf("overflow err = %v, want Internal", err)
	}
	wantStatus(t, e.loadByKey(t, 1, "overflow"), orderv1.OrderStatus_ORDER_STATUS_CREATING, false)

	// 求和溢出：2^62 + 2^62。
	e.inv.addSKU(5, 1<<62, 10)
	_, err = e.svc.CreateOrder(t.Context(), createReq(1, "sum", items(skuHuge, 1, 5, 1)))
	if errx.Code(err) != codes.Internal {
		t.Fatalf("sum overflow err = %v, want Internal", err)
	}

	// 同一订单内币种不一致。
	e.inv.setCurrency(skuB, "USD")
	_, err = e.svc.CreateOrder(t.Context(), createReq(1, "currency", items(skuA, 1, skuB, 1)))
	if errx.Code(err) != codes.Internal {
		t.Fatalf("currency err = %v, want Internal", err)
	}
	wantStatus(t, e.loadByKey(t, 1, "currency"), orderv1.OrderStatus_ORDER_STATUS_CREATING, false)

	// 这些订单最终由后台任务置为失败并释放库存。
	e.clock.Advance(DefaultCreateTimeout + time.Second)
	e.sweeper.RunOnce(t.Context())
	for _, key := range []string{"overflow", "sum", "currency"} {
		wantStatus(t, e.loadByKey(t, 1, key), orderv1.OrderStatus_ORDER_STATUS_FAILED, true)
	}
	wantStock(t, e.inv, skuHuge, 1000, 0, 0)
	wantStock(t, e.inv, skuB, 1000, 0, 0)
}

func TestAmountArithmetic(t *testing.T) {
	t.Parallel()
	if v, ok := mulAmount(math.MaxInt64/3, 3); !ok || v != math.MaxInt64/3*3 {
		t.Errorf("mulAmount max = %d, %v", v, ok)
	}
	if _, ok := mulAmount(math.MaxInt64/3+1, 3); ok {
		t.Error("mulAmount did not detect overflow")
	}
	if v, ok := mulAmount(0, 5); !ok || v != 0 {
		t.Errorf("mulAmount zero = %d, %v", v, ok)
	}
	if _, ok := addAmount(math.MaxInt64, 1); ok {
		t.Error("addAmount did not detect overflow")
	}
	if v, ok := addAmount(math.MaxInt64-1, 1); !ok || v != math.MaxInt64 {
		t.Errorf("addAmount max = %d, %v", v, ok)
	}
}

func TestGetOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "k")

	got, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: o.GetOrderId(), UserId: 1})
	if err != nil || got.GetOrder().GetOrderId() != o.GetOrderId() {
		t.Fatalf("GetOrder = %v, %v", got, err)
	}
	if got.GetOrder().GetPayTime() != nil {
		t.Error("unpaid order has pay_time")
	}
	_, err = e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: o.GetOrderId(), UserId: 2})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	_, err = e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: 12345, UserId: 1})
	wantErr(t, err, codes.NotFound, errx.ReasonOrderNotFound)
	_, err = e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: 0, UserId: 1})
	wantErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
}
