package service

import (
	"context"
	"slices"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/Oppenheimer/pkg/errx"
	"github.com/palemoky/Oppenheimer/pkg/stockgate"
)

// errTimeout 模拟调用超时：调用方无法知道下游是否执行了。
var errTimeout = status.Error(codes.DeadlineExceeded, "fake: deadline exceeded")

// fault 描述对下一次调用注入的行为。字段可以组合。
type fault struct {
	// before 在执行之前调用，用于在调用“进行中”制造并发事件。
	before func(orderID int64)
	// err 非空时不执行，直接返回它（超时或业务错误）。
	err error
	// errAfterApply 非空时照常执行，然后返回它，模拟“实际成功但返回超时”。
	errAfterApply error
}

type resState int

const (
	resReserved resState = iota + 1
	resConfirmed
	resReleased
)

type fakeSKU struct {
	price                     int64
	currency                  string
	available, reserved, sold int64
	limit                     int64
}

type fakeReservation struct {
	items []*inventoryv1.ReserveItem
	state resState
}

// fakeInventory 是按 inventory.proto 契约实现的内存版库存服务，
// 并能对每个方法按调用顺序注入故障。WarmUp 把数量装载进真实的闸门（Redis）。
type fakeInventory struct {
	mu     sync.Mutex
	gate   *stockgate.Gate
	skus   map[int64]*fakeSKU
	res    map[int64]*fakeReservation
	faults map[string][]fault
	calls  map[string]int
	// 真正改变了库存的次数，用来断言“副作用只发生一次”。
	applied map[string]int
}

func newFakeInventory(gate *stockgate.Gate) *fakeInventory {
	return &fakeInventory{
		gate:    gate,
		skus:    map[int64]*fakeSKU{},
		res:     map[int64]*fakeReservation{},
		faults:  map[string][]fault{},
		calls:   map[string]int{},
		applied: map[string]int{},
	}
}

func (f *fakeInventory) addSKU(id, price, available int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.skus[id] = &fakeSKU{price: price, currency: "CNY", available: available}
}

func (f *fakeInventory) setLimit(id, limit int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.skus[id].limit = limit
}

// setAvailable 直接改 MySQL 一侧的可售数量，用来制造“闸门比 MySQL 大”。
func (f *fakeInventory) setAvailable(id, available int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.skus[id].available = available
}

func (f *fakeInventory) setCurrency(id int64, currency string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.skus[id].currency = currency
}

// inject 为 method 的后续调用依次排上故障。
func (f *fakeInventory) inject(method string, fs ...fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults[method] = append(f.faults[method], fs...)
}

func (f *fakeInventory) stock(id int64) fakeSKU {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *f.skus[id]
}

func (f *fakeInventory) state(orderID int64) resState {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.res[orderID]; ok {
		return r.state
	}
	return 0
}

func (f *fakeInventory) callCount(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[method]
}

func (f *fakeInventory) appliedCount(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied[method]
}

// begin 记一次调用并取出排在最前面的故障。
func (f *fakeInventory) begin(method string) fault {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[method]++
	q := f.faults[method]
	if len(q) == 0 {
		return fault{}
	}
	f.faults[method] = q[1:]
	return q[0]
}

func run[T any](f *fakeInventory, method string, orderID int64, apply func() (T, error)) (T, error) {
	ft := f.begin(method)
	if ft.before != nil {
		ft.before(orderID)
	}
	if ft.err != nil {
		var zero T
		return zero, ft.err
	}
	f.mu.Lock()
	resp, err := apply()
	f.mu.Unlock()
	if err == nil && ft.errAfterApply != nil {
		var zero T
		return zero, ft.errAfterApply
	}
	return resp, err
}

func (f *fakeInventory) Reserve(_ context.Context, in *inventoryv1.ReserveRequest, _ ...grpc.CallOption) (*inventoryv1.ReserveResponse, error) {
	return run(f, "Reserve", in.GetOrderId(), func() (*inventoryv1.ReserveResponse, error) {
		if r, ok := f.res[in.GetOrderId()]; ok {
			if r.state == resReleased {
				return nil, errx.New(codes.FailedPrecondition, errx.ReasonReservationReleased, "released")
			}
			if !slices.EqualFunc(r.items, in.GetItems(), func(a, b *inventoryv1.ReserveItem) bool {
				return a.GetSkuId() == b.GetSkuId() && a.GetQuantity() == b.GetQuantity()
			}) {
				return nil, errx.New(codes.AlreadyExists, errx.ReasonReservationMismatch, "mismatch")
			}
			return f.reserved(in.GetItems()), nil
		}
		for _, it := range in.GetItems() {
			sku, ok := f.skus[it.GetSkuId()]
			if !ok {
				return nil, errx.New(codes.NotFound, errx.ReasonSKUNotFound, "sku %d not found", it.GetSkuId())
			}
			if sku.available < int64(it.GetQuantity()) {
				return nil, errx.New(codes.FailedPrecondition, errx.ReasonInsufficientStock, "sku %d", it.GetSkuId())
			}
		}
		for _, it := range in.GetItems() {
			sku := f.skus[it.GetSkuId()]
			sku.available -= int64(it.GetQuantity())
			sku.reserved += int64(it.GetQuantity())
		}
		f.res[in.GetOrderId()] = &fakeReservation{items: in.GetItems(), state: resReserved}
		f.applied["Reserve"]++
		return f.reserved(in.GetItems()), nil
	})
}

func (f *fakeInventory) reserved(items []*inventoryv1.ReserveItem) *inventoryv1.ReserveResponse {
	out := make([]*inventoryv1.ReservedItem, 0, len(items))
	for _, it := range items {
		sku := f.skus[it.GetSkuId()]
		out = append(out, &inventoryv1.ReservedItem{
			SkuId:     it.GetSkuId(),
			Quantity:  it.GetQuantity(),
			UnitPrice: &commonv1.Money{Amount: sku.price, Currency: sku.currency},
		})
	}
	return &inventoryv1.ReserveResponse{Items: out}
}

func (f *fakeInventory) Confirm(_ context.Context, in *inventoryv1.ConfirmRequest, _ ...grpc.CallOption) (*inventoryv1.ConfirmResponse, error) {
	return run(f, "Confirm", in.GetOrderId(), func() (*inventoryv1.ConfirmResponse, error) {
		r, ok := f.res[in.GetOrderId()]
		switch {
		case !ok:
			return nil, errx.New(codes.NotFound, errx.ReasonReservationNotFound, "no reservation")
		case r.state == resReleased:
			return nil, errx.New(codes.FailedPrecondition, errx.ReasonReservationReleased, "released")
		case r.state == resConfirmed:
			return &inventoryv1.ConfirmResponse{}, nil
		}
		for _, it := range r.items {
			sku := f.skus[it.GetSkuId()]
			sku.reserved -= int64(it.GetQuantity())
			sku.sold += int64(it.GetQuantity())
		}
		r.state = resConfirmed
		f.applied["Confirm"]++
		return &inventoryv1.ConfirmResponse{}, nil
	})
}

func (f *fakeInventory) Release(_ context.Context, in *inventoryv1.ReleaseRequest, _ ...grpc.CallOption) (*inventoryv1.ReleaseResponse, error) {
	return run(f, "Release", in.GetOrderId(), func() (*inventoryv1.ReleaseResponse, error) {
		r, ok := f.res[in.GetOrderId()]
		switch {
		case !ok:
			// 空回滚：留下已释放标记，挡住之后迟到的 Reserve。
			f.res[in.GetOrderId()] = &fakeReservation{state: resReleased}
			return &inventoryv1.ReleaseResponse{}, nil
		case r.state == resReleased:
			return &inventoryv1.ReleaseResponse{}, nil
		case r.state == resConfirmed:
			return nil, errx.New(codes.FailedPrecondition, errx.ReasonReservationConfirmed, "confirmed")
		}
		for _, it := range r.items {
			sku := f.skus[it.GetSkuId()]
			sku.reserved -= int64(it.GetQuantity())
			sku.available += int64(it.GetQuantity())
		}
		r.state = resReleased
		f.applied["Release"]++
		return &inventoryv1.ReleaseResponse{}, nil
	})
}

func (f *fakeInventory) GetStock(_ context.Context, in *inventoryv1.GetStockRequest, _ ...grpc.CallOption) (*inventoryv1.GetStockResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sku, ok := f.skus[in.GetSkuId()]
	if !ok {
		return nil, errx.New(codes.NotFound, errx.ReasonSKUNotFound, "sku not found")
	}
	return &inventoryv1.GetStockResponse{
		SkuId: in.GetSkuId(), Available: sku.available, Reserved: sku.reserved, Sold: sku.sold,
		UnitPrice: &commonv1.Money{Amount: sku.price, Currency: sku.currency},
	}, nil
}

// WarmUp 按契约把 SKU 装载进闸门：avail 取当前可售数量，已装载时只更新限购。
func (f *fakeInventory) WarmUp(ctx context.Context, in *inventoryv1.WarmUpRequest, _ ...grpc.CallOption) (*inventoryv1.WarmUpResponse, error) {
	ft := f.begin("WarmUp")
	if ft.err != nil {
		return nil, ft.err
	}
	type load struct{ sku, avail, limit int64 }
	var (
		loads []load
		resp  = &inventoryv1.WarmUpResponse{}
	)
	f.mu.Lock()
	for _, id := range in.GetSkuIds() {
		sku, ok := f.skus[id]
		if !ok {
			resp.Missing = append(resp.Missing, id)
			continue
		}
		loads = append(loads, load{id, sku.available, sku.limit})
	}
	f.mu.Unlock()
	for _, l := range loads {
		if _, err := f.gate.Load(ctx, l.sku, l.avail, l.limit); err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
	}
	return resp, nil
}

var _ inventoryv1.InventoryServiceClient = (*fakeInventory)(nil)
