package service

import (
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	orderindexv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/orderindex/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/storetest"
)

var created = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func newService(t *testing.T) (*Service, *store.Store) {
	t.Helper()
	st, _ := storetest.New(t)
	svc, err := New(st)
	if err != nil {
		t.Fatal(err)
	}
	return svc, st
}

func put(t *testing.T, st *store.Store, merchant, order int64, status orderv1.OrderStatus) {
	t.Helper()
	r := store.Row{
		MerchantID: merchant, OrderID: order, UserID: order + 1, Status: int32(status),
		TotalAmount: order * 100, Currency: "CNY", CreatedAt: created.Add(time.Duration(order) * time.Millisecond), Version: 1,
	}
	if status == orderv1.OrderStatus_ORDER_STATUS_PAID {
		r.PaidAt = r.CreatedAt.Add(time.Minute)
	}
	if _, err := st.UpsertRows(t.Context(), []store.Row{r}, time.Now()); err != nil {
		t.Fatal(err)
	}
}

// listAll 按 pageSize 翻完所有页，返回订单号。
func listAll(t *testing.T, svc *Service, merchant int64, status orderv1.OrderStatus, pageSize int32) []int64 {
	t.Helper()
	var (
		ids    []int64
		before int64
	)
	for page := 0; ; page++ {
		if page > 1000 {
			t.Fatal("too many pages")
		}
		resp, err := svc.ListMerchantOrders(t.Context(), &orderindexv1.ListMerchantOrdersRequest{
			MerchantId: merchant, Status: status, PageSize: pageSize, BeforeOrderId: before,
		})
		if err != nil {
			t.Fatal(err)
		}
		if pageSize > 0 && len(resp.GetOrders()) > int(pageSize) {
			t.Fatalf("page has %d orders, page size %d", len(resp.GetOrders()), pageSize)
		}
		for _, o := range resp.GetOrders() {
			ids = append(ids, o.GetOrderId())
		}
		if resp.GetNextBeforeOrderId() == 0 {
			return ids
		}
		if len(resp.GetOrders()) == 0 {
			t.Fatal("empty page with a next cursor")
		}
		before = resp.GetNextBeforeOrderId()
	}
}

func TestListPagination(t *testing.T) {
	t.Parallel()
	svc, st := newService(t)
	statuses := []orderv1.OrderStatus{
		orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT,
		orderv1.OrderStatus_ORDER_STATUS_PAID,
		orderv1.OrderStatus_ORDER_STATUS_CLOSED,
	}
	const merchant = 6
	var all []int64
	byStatus := map[orderv1.OrderStatus][]int64{}
	for i := range int64(47) {
		id := 1000 + i*7
		s := statuses[i%3]
		put(t, st, merchant, id, s)
		all = append(all, id)
		byStatus[s] = append(byStatus[s], id)
	}
	// 同一个 slot（6 & 3 == 10 & 3）、同一张表上的另一个商家，不能混进来。
	put(t, st, 10, 999999, orderv1.OrderStatus_ORDER_STATUS_PAID)
	desc := func(s []int64) []int64 { s = slices.Clone(s); slices.Sort(s); slices.Reverse(s); return s }

	for _, size := range []int32{1, 5, 10, 47, 100, 0} {
		if got := listAll(t, svc, merchant, 0, size); !slices.Equal(got, desc(all)) {
			t.Errorf("page size %d: got %v, want %v", size, got, desc(all))
		}
	}
	for s, ids := range byStatus {
		if got := listAll(t, svc, merchant, s, 4); !slices.Equal(got, desc(ids)) {
			t.Errorf("status %v: got %v, want %v", s, got, desc(ids))
		}
	}
	if got := listAll(t, svc, merchant, orderv1.OrderStatus_ORDER_STATUS_CANCELLED, 4); len(got) != 0 {
		t.Errorf("cancelled: got %v, want none", got)
	}

	// 默认页大小 20，下一页游标是本页最后一条；恰好翻完时游标为 0。
	resp, err := svc.ListMerchantOrders(t.Context(), &orderindexv1.ListMerchantOrdersRequest{MerchantId: merchant})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetOrders()) != DefaultPageSize || resp.GetNextBeforeOrderId() != resp.GetOrders()[DefaultPageSize-1].GetOrderId() {
		t.Errorf("default page: %d orders, next %d", len(resp.GetOrders()), resp.GetNextBeforeOrderId())
	}
	resp, err = svc.ListMerchantOrders(t.Context(), &orderindexv1.ListMerchantOrdersRequest{MerchantId: merchant, PageSize: 47})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetOrders()) != 47 || resp.GetNextBeforeOrderId() != 0 {
		t.Errorf("exact page: %d orders, next %d", len(resp.GetOrders()), resp.GetNextBeforeOrderId())
	}

	// 字段转换。
	o := resp.GetOrders()[0]
	paid := byStatus[orderv1.OrderStatus_ORDER_STATUS_PAID]
	if o.GetOrderId() != desc(all)[0] || o.GetUserId() != o.GetOrderId()+1 ||
		o.GetTotal().GetAmount() != o.GetOrderId()*100 || o.GetTotal().GetCurrency() != "CNY" ||
		!o.GetCreateTime().AsTime().Equal(created.Add(time.Duration(o.GetOrderId())*time.Millisecond)) {
		t.Errorf("first order = %v", o)
	}
	for _, o := range resp.GetOrders() {
		if hasPay := o.GetPayTime() != nil; hasPay != slices.Contains(paid, o.GetOrderId()) {
			t.Errorf("order %d: pay_time = %v, status %v", o.GetOrderId(), o.GetPayTime(), o.GetStatus())
		}
	}
}

// 不同商家落在不同 slot 和库上，互不可见；没有订单的商家返回空列表。
func TestListIsolationAndEmpty(t *testing.T) {
	t.Parallel()
	svc, st := newService(t)
	// 商家 1..4 的 slot 分别是 1、2、3、0，库是 1、0、1、0。
	for m := range int64(4) {
		for i := range int64(3) {
			put(t, st, m+1, (m+1)*100+i, orderv1.OrderStatus_ORDER_STATUS_PAID)
		}
	}
	for m := range int64(4) {
		got := listAll(t, svc, m+1, 0, 2)
		want := []int64{(m+1)*100 + 2, (m+1)*100 + 1, (m+1)*100 + 0}
		if !slices.Equal(got, want) {
			t.Errorf("merchant %d: got %v, want %v", m+1, got, want)
		}
	}
	resp, err := svc.ListMerchantOrders(t.Context(), &orderindexv1.ListMerchantOrdersRequest{MerchantId: 77})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetOrders()) != 0 || resp.GetNextBeforeOrderId() != 0 {
		t.Errorf("empty merchant: %v", resp)
	}
}

func TestListValidation(t *testing.T) {
	t.Parallel()
	svc, _ := newService(t)
	for name, req := range map[string]*orderindexv1.ListMerchantOrdersRequest{
		"zero merchant":     {MerchantId: 0},
		"negative merchant": {MerchantId: -1},
		"page size 101":     {MerchantId: 1, PageSize: 101},
		"negative size":     {MerchantId: 1, PageSize: -1},
		"negative cursor":   {MerchantId: 1, BeforeOrderId: -5},
		"unknown status":    {MerchantId: 1, Status: orderv1.OrderStatus(42)},
	} {
		_, err := svc.ListMerchantOrders(t.Context(), req)
		if errx.Code(err) != codes.InvalidArgument || !errx.Is(err, errx.ReasonInvalidArgument) {
			t.Errorf("%s: err = %v, want INVALID_ARGUMENT", name, err)
		}
	}
	for _, size := range []int32{1, 100} {
		if _, err := svc.ListMerchantOrders(t.Context(), &orderindexv1.ListMerchantOrdersRequest{MerchantId: 1, PageSize: size}); err != nil {
			t.Errorf("page size %d: %v", size, err)
		}
	}
}
