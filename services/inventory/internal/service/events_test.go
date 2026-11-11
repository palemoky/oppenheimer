package service

import (
	"context"
	"sync"
	"testing"

	"github.com/palemoky/Oppenheimer/api/eventx"
	eventv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/event/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/kafkax"
)

func paidEvent(orderID int64) *eventv1.OrderEvent {
	return &eventv1.OrderEvent{OrderId: orderID, Body: &eventv1.OrderEvent_Paid{Paid: &eventv1.OrderPaid{PaymentId: orderID * 10}}}
}

func cancelledEvent(orderID int64) *eventv1.OrderEvent {
	return &eventv1.OrderEvent{OrderId: orderID, Body: &eventv1.OrderEvent_Cancelled{Cancelled: &eventv1.OrderCancelled{}}}
}

func closedEvent(orderID int64) *eventv1.OrderEvent {
	return &eventv1.OrderEvent{OrderId: orderID, Body: &eventv1.OrderEvent_Closed{Closed: &eventv1.OrderClosed{}}}
}

func failedEvent(orderID int64) *eventv1.OrderEvent {
	return &eventv1.OrderEvent{OrderId: orderID, Body: &eventv1.OrderEvent_Failed{Failed: &eventv1.OrderFailed{Reason: "CREATE_TIMEOUT"}}}
}

// reservationCount 返回 reservation 表的行数，用来确认被跳过的事件没有留下任何记录。
func (e *env) reservationCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM reservation`).Scan(&n); err != nil {
		t.Fatalf("count reservation: %v", err)
	}
	return n
}

func TestHandleOrderEventTwice(t *testing.T) {
	cases := []struct {
		name string
		ev   func(int64) *eventv1.OrderEvent
		// 处理之后 SKU 1 的 (available, reserved, sold)，入库 10、预占 3。
		available, sold int64
	}{
		{"paid", paidEvent, 7, 3},
		{"cancelled", cancelledEvent, 10, 0},
		{"closed", closedEvent, 10, 0},
		{"failed", failedEvent, 10, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.seed(t, 1, 100, 10)
			_, err := e.reserve(t.Context(), 7, 1, 3)
			mustOK(t, err)

			mustOK(t, e.svc.HandleOrderEvent(t.Context(), c.ev(7)))
			e.assertStock(t, 1, c.available, 0, c.sold)
			// 至少一次投递：同一事件再来一次，结果与库存都不变。
			mustOK(t, e.svc.HandleOrderEvent(t.Context(), c.ev(7)))
			e.assertStock(t, 1, c.available, 0, c.sold)
		})
	}
}

func TestHandleOrderEventFailedBeforeReserve(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	// 下单时 Reserve 超时，订单判定失败；failed 事件先于仍在路上的 Reserve 到达。
	// 不依赖消息与在线调用的先后：空回滚标记挡住之后的 Reserve。
	mustOK(t, e.svc.HandleOrderEvent(t.Context(), failedEvent(7)))
	mustOK(t, e.svc.HandleOrderEvent(t.Context(), failedEvent(7)))

	_, err := e.reserve(t.Context(), 7, 1, 3)
	if !errx.Is(err, errx.ReasonReservationReleased) {
		t.Fatalf("late Reserve = %v, want RESERVATION_RELEASED", err)
	}
	e.assertStock(t, 1, 10, 0, 0)
}

func TestHandleOrderEventImpossible(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(t *testing.T, e *env)
		ev     *eventv1.OrderEvent
		reason string
		// 处理之后 SKU 1 的 (available, sold)，期间库存不能被改动。
		available, sold int64
	}{
		{
			name: "paid but released",
			setup: func(t *testing.T, e *env) {
				_, err := e.reserve(t.Context(), 7, 1, 3)
				mustOK(t, err)
				mustOK(t, e.release(t.Context(), 7))
			},
			ev: paidEvent(7), reason: errx.ReasonReservationReleased, available: 10,
		},
		{
			name:  "paid but not found",
			setup: func(*testing.T, *env) {},
			ev:    paidEvent(7), reason: errx.ReasonReservationNotFound, available: 10,
		},
		{
			name: "cancelled but confirmed",
			setup: func(t *testing.T, e *env) {
				_, err := e.reserve(t.Context(), 7, 1, 3)
				mustOK(t, err)
				mustOK(t, e.confirm(t.Context(), 7))
			},
			ev: cancelledEvent(7), reason: errx.ReasonReservationConfirmed, available: 7, sold: 3,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t)
			e.seed(t, 1, 100, 10)
			c.setup(t, e)

			// 处理两次都应进死信，不会因为第一次失败而改了什么、第二次变成成功。
			for range 2 {
				err := e.svc.HandleOrderEvent(t.Context(), c.ev)
				if !kafkax.IsPermanent(err) {
					t.Fatalf("HandleOrderEvent = %v, want permanent", err)
				}
				if !errx.Is(err, c.reason) {
					t.Fatalf("HandleOrderEvent = %v, want reason %s", err, c.reason)
				}
			}
			e.assertStock(t, 1, c.available, 0, c.sold)
		})
	}
}

func TestHandleOrderEventSkipped(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)

	events := map[string]*eventv1.OrderEvent{
		"payment result handled": {OrderId: 7, Body: &eventv1.OrderEvent_PaymentResultHandled{
			PaymentResultHandled: &eventv1.PaymentResultHandled{PaymentId: 1, Disposition: eventv1.PaymentDisposition_PAYMENT_DISPOSITION_ACCEPTED},
		}},
		// 发布方新增了本服务还不认识的事件时，解码出来就是空的 oneof。
		"unknown body": {OrderId: 7},
	}
	for name, ev := range events {
		t.Run(name, func(t *testing.T) {
			mustOK(t, e.svc.HandleOrderEvent(t.Context(), ev))
		})
	}
	// 跳过就是什么都不做：尤其不能像 Release 那样留下空回滚标记。
	if n := e.reservationCount(t); n != 0 {
		t.Errorf("reservation rows = %d, want 0", n)
	}
	e.assertStock(t, 1, 10, 0, 0)
}

func TestHandleOrderEventWithoutOrderID(t *testing.T) {
	e := newEnv(t)
	for _, ev := range []*eventv1.OrderEvent{paidEvent(0), failedEvent(-1)} {
		if err := e.svc.HandleOrderEvent(t.Context(), ev); !kafkax.IsPermanent(err) {
			t.Errorf("HandleOrderEvent(%v) = %v, want permanent", ev, err)
		}
	}
	if n := e.reservationCount(t); n != 0 {
		t.Errorf("reservation rows = %d, want 0", n)
	}
}

// TestHandleOrderEventConcurrentWithOnline 让事件消费与在线 gRPC 路径同时作用在同一批订单上：
// 事件重复投递、与 Reserve 赛跑、与排障用的 gRPC Confirm/Release 撞在一起，
// 最终库存不能超卖（seed 的清理检查恒等式和非负），也不能有悬挂的预占。
func TestHandleOrderEventConcurrentWithOnline(t *testing.T) {
	e := newEnv(t)
	const orders, stock = 80, 50
	e.seed(t, 1, 100, stock)

	type result struct {
		reserveErr error
		// confirmed 表示这笔订单最终应当已售。
		confirmed bool
		errs      []error
	}
	results := make([]result, orders)

	var wg sync.WaitGroup
	for i := range orders {
		wg.Go(func() {
			orderID := int64(1000 + i)
			r := &results[i]
			var mu sync.Mutex
			record := func(err error) {
				mu.Lock()
				defer mu.Unlock()
				r.errs = append(r.errs, err)
			}
			// 每个订单内部再并发：重复的事件之间、事件与 gRPC 调用之间都没有先后。
			var inner sync.WaitGroup
			dup := func(n int, f func() error) {
				for range n {
					inner.Go(func() { record(f()) })
				}
			}

			switch i % 4 {
			case 0:
				// 正常下单：Reserve 成功才有 paid，否则订单失败。
				_, r.reserveErr = e.reserve(t.Context(), orderID, 1, 1)
				if r.reserveErr == nil {
					r.confirmed = true
					dup(2, func() error { return e.svc.HandleOrderEvent(t.Context(), paidEvent(orderID)) })
					dup(1, func() error { return e.confirm(t.Context(), orderID) })
				} else {
					dup(2, func() error { return e.svc.HandleOrderEvent(t.Context(), failedEvent(orderID)) })
				}
			case 1:
				// Reserve 超时：failed 事件与仍在路上的 Reserve 赛跑。
				inner.Go(func() { _, r.reserveErr = e.reserve(t.Context(), orderID, 1, 1) })
				dup(2, func() error { return e.svc.HandleOrderEvent(t.Context(), failedEvent(orderID)) })
				dup(1, func() error { return e.release(t.Context(), orderID) })
			default:
				// 用户取消或超时关闭：释放事件与 gRPC Release 撞在一起。
				_, r.reserveErr = e.reserve(t.Context(), orderID, 1, 1)
				ev := cancelledEvent
				if i%4 == 3 {
					ev = closedEvent
				}
				dup(2, func() error { return e.svc.HandleOrderEvent(t.Context(), ev(orderID)) })
				dup(1, func() error { return e.release(t.Context(), orderID) })
			}
			inner.Wait()
		})
	}
	wg.Wait()

	var sold int64
	for i, r := range results {
		orderID := 1000 + i
		for _, err := range r.errs {
			if err != nil {
				t.Errorf("order %d: %v", orderID, err)
			}
		}
		if err := r.reserveErr; err != nil &&
			!errx.Is(err, errx.ReasonInsufficientStock) && !errx.Is(err, errx.ReasonReservationReleased) {
			t.Errorf("order %d Reserve: %v", orderID, err)
		}
		if r.confirmed {
			sold++
		}
	}
	if sold == 0 {
		t.Fatal("no order was confirmed, the test did not exercise the paid path")
	}
	// 所有订单都已进入终态，预占必须清零；已售恰好等于 paid 的订单数。
	e.assertStock(t, 1, stock-sold, 0, sold)
}

func TestOrderEventHandler(t *testing.T) {
	e := newEnv(t)
	e.seed(t, 1, 100, 10)
	_, err := e.reserve(t.Context(), 7, 1, 3)
	mustOK(t, err)
	h := e.svc.OrderEventHandler(0)

	t.Run("undecodable", func(t *testing.T) {
		err := h(t.Context(), kafkax.Message{Value: []byte{0xff, 0xff, 0xff}})
		if !kafkax.IsPermanent(err) {
			t.Fatalf("handler = %v, want permanent", err)
		}
	})

	t.Run("canceled context is retried", func(t *testing.T) {
		_, value, err := eventx.MarshalOrderEvent(paidEvent(7))
		mustOK(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err = h(ctx, kafkax.Message{Value: value})
		if err == nil || kafkax.IsPermanent(err) {
			t.Fatalf("handler = %v, want retryable error", err)
		}
		e.assertStock(t, 1, 7, 3, 0)
	})

	t.Run("decoded and handled", func(t *testing.T) {
		key, value, err := eventx.MarshalOrderEvent(paidEvent(7))
		mustOK(t, err)
		mustOK(t, h(t.Context(), kafkax.Message{Key: key, Value: value}))
		e.assertStock(t, 1, 7, 0, 3)
	})
}
