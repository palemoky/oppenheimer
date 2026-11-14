package service

import (
	"slices"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
)

func listReq(userID int64, size int32, before int64) *orderv1.ListUserOrdersRequest {
	return &orderv1.ListUserOrdersRequest{UserId: userID, PageSize: size, BeforeOrderId: before}
}

// listIDs 用 svc 从头翻到底，返回依次拿到的订单号。
func listIDs(t *testing.T, svc *Service, req *orderv1.ListUserOrdersRequest) []int64 {
	t.Helper()
	var ids []int64
	for range 100 {
		resp, err := svc.ListUserOrders(t.Context(), req)
		if err != nil {
			t.Fatalf("ListUserOrders: %v", err)
		}
		for _, o := range resp.GetOrders() {
			ids = append(ids, o.GetOrderId())
		}
		next := resp.GetNextBeforeOrderId()
		if next == 0 {
			return ids
		}
		if last := resp.GetOrders()[len(resp.GetOrders())-1].GetOrderId(); next != last {
			t.Fatalf("next_before_order_id = %d, want last order %d", next, last)
		}
		req.BeforeOrderId = next
	}
	t.Fatal("paging did not terminate")
	return nil
}

func TestListUserOrdersValidation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for name, req := range map[string]*orderv1.ListUserOrdersRequest{
		"zero user":       listReq(0, 10, 0),
		"negative user":   listReq(-1, 10, 0),
		"page too large":  listReq(1, MaxListPageSize+1, 0),
		"negative page":   listReq(1, -1, 0),
		"negative before": listReq(1, 10, -1),
		"unknown status":  {UserId: 1, Status: orderv1.OrderStatus(99)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := e.svc.ListUserOrders(t.Context(), req)
			wantErr(t, err, codes.InvalidArgument, errx.ReasonInvalidArgument)
		})
	}
	// 边界值合法；没有订单时返回空列表、没有下一页。
	for _, req := range []*orderv1.ListUserOrdersRequest{listReq(1, 0, 0), listReq(1, 1, 0), listReq(1, MaxListPageSize, 1)} {
		resp, err := e.svc.ListUserOrders(t.Context(), req)
		if err != nil || len(resp.GetOrders()) != 0 || resp.GetNextBeforeOrderId() != 0 {
			t.Errorf("%v: resp %v, err %v", req, resp, err)
		}
	}
}

// 下单、落库、取消后，列表与 GetOrder 的说法一致；翻页不重不漏；状态筛选；同 slot 的别人的订单不出现；
// 已受理没落库的订单不在列表里。
func TestListUserOrders(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const user, neighbor = 4, 4 + 8 // 同一个 slot
	var landed []int64
	for i := range 5 {
		landed = append(landed, e.mustPending(t, user, "k"+string(rune('a'+i))).GetOrderId())
	}
	e.mustPending(t, neighbor, "n")
	queued := e.submit(t, user, "queued", items(skuA, 1)) // 受理了，还没落库
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(landed[1], user)); err != nil {
		t.Fatal(err)
	}

	want := slices.Clone(landed)
	slices.Reverse(want)
	for _, size := range []int32{0, 1, 2, 5} {
		if got := listIDs(t, e.svc, listReq(user, size, 0)); !slices.Equal(got, want) {
			t.Errorf("page size %d: got %v, want %v", size, got, want)
		}
	}
	if got := listIDs(t, e.svc, listReq(user, 2, 0)); slices.Contains(got, queued.GetOrderId()) {
		t.Error("an order still in the queue must not be listed")
	}

	resp, err := e.svc.ListUserOrders(t.Context(), &orderv1.ListUserOrdersRequest{
		UserId: user, Status: orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetOrders()) != 1 || resp.GetNextBeforeOrderId() != 0 {
		t.Fatalf("cancelled: %v", resp)
	}
	got := resp.GetOrders()[0]
	full, err := e.svc.GetOrder(t.Context(), &orderv1.GetOrderRequest{OrderId: landed[1], UserId: user})
	if err != nil {
		t.Fatal(err)
	}
	o := full.GetOrder()
	if got.GetOrderId() != o.GetOrderId() || got.GetStatus() != o.GetStatus() ||
		got.GetTotal().GetAmount() != o.GetTotal().GetAmount() || got.GetTotal().GetCurrency() != o.GetTotal().GetCurrency() ||
		!got.GetCreateTime().AsTime().Equal(o.GetCreateTime().AsTime()) ||
		!got.GetExpireTime().AsTime().Equal(o.GetExpireTime().AsTime()) || got.GetPayTime() != nil {
		t.Errorf("summary %v does not match order %v", got, o)
	}
	if got.GetTotal().GetAmount() != 2*priceA {
		t.Errorf("total = %v", got.GetTotal())
	}
}

// 读己之写：即使读模式允许读从库、从库完全落后，刚落库的订单也立刻出现在列表里（列表始终读主库）。
func TestListUserOrdersSeesOwnWritesWithStaleReplica(t *testing.T) {
	t.Parallel()
	for _, mode := range []ReadMode{ReadWindow, ReadGTID} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			e := newEnvWith(t, envOptions{replica: emptyReplica, readMode: mode})
			o := e.mustPending(t, 1, "k")
			// pin 和 GTID 标记都没了也一样：列表不依赖按订单的标记。
			e.dropMark(t, "pin", o.GetOrderId())
			e.dropMark(t, "gtid", o.GetOrderId())
			if got := listIDs(t, e.svc, listReq(1, 0, 0)); !slices.Equal(got, []int64{o.GetOrderId()}) {
				t.Errorf("got %v, want the order just placed", got)
			}
		})
	}
}

// 列表与落库、取消并发进行（-race）：每一页内订单号严格递减，翻页不重复。
func TestListUserOrdersConcurrentWithWrites(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const user = 9
	first := e.mustPending(t, user, "seed")
	// 读在子 goroutine 里，写留在测试 goroutine 里（写的 helper 失败时调用 t.Fatal）。
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 5 {
				req := listReq(user, 2, 0)
				var seen []int64
				for {
					resp, err := e.svc.ListUserOrders(t.Context(), req)
					if err != nil {
						t.Error(err)
						return
					}
					for _, o := range resp.GetOrders() {
						if n := len(seen); n > 0 && o.GetOrderId() >= seen[n-1] {
							t.Errorf("order ids not strictly decreasing: %v then %d", seen, o.GetOrderId())
							return
						}
						seen = append(seen, o.GetOrderId())
					}
					if resp.GetNextBeforeOrderId() == 0 {
						break
					}
					req.BeforeOrderId = resp.GetNextBeforeOrderId()
				}
				if !slices.Contains(seen, first.GetOrderId()) {
					t.Errorf("seed order missing from %v", seen)
				}
			}
		})
	}
	for i := range 4 {
		e.mustPending(t, user, "w"+string(rune('a'+i)))
	}
	if _, err := e.svc.CancelOrder(t.Context(), cancelReq(first.GetOrderId(), user)); err != nil {
		t.Error(err)
	}
	wg.Wait()
	if got := listIDs(t, e.svc, listReq(user, 0, 0)); len(got) != 5 {
		t.Errorf("got %d orders, want 5", len(got))
	}
}
