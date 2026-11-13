// Package service 实现 OrderIndexService：商家维度的订单列表（设计文档 1c 4.9）。
//
// 数据来自索引表，是订单库的最终一致副本，只用于展示（设计文档 1c 4.10）。
package service

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	orderindexv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/orderindex/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

// 每页条数的默认值和上限，见 proto 的注释。
const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// Service 实现 orderindexv1.OrderIndexServiceServer。
type Service struct {
	orderindexv1.UnimplementedOrderIndexServiceServer

	store *store.Store
}

// New 返回读 st 的 Service。
func New(st *store.Store) (*Service, error) {
	if st == nil {
		return nil, errors.New("service: nil store")
	}
	return &Service{store: st}, nil
}

// ListMerchantOrders 按订单号倒序列出一个商家的订单。多查一条来判断有没有下一页。
func (s *Service) ListMerchantOrders(ctx context.Context, req *orderindexv1.ListMerchantOrdersRequest) (
	*orderindexv1.ListMerchantOrdersResponse, error,
) {
	if req.GetMerchantId() <= 0 {
		return nil, errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument, "merchant_id must be positive")
	}
	size := int(req.GetPageSize())
	switch {
	case size == 0:
		size = DefaultPageSize
	case size < 1 || size > MaxPageSize:
		return nil, errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument,
			"page_size must be in [1, %d], got %d", MaxPageSize, size)
	}
	if req.GetBeforeOrderId() < 0 {
		return nil, errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument, "before_order_id must not be negative")
	}
	status := req.GetStatus()
	if _, known := orderv1.OrderStatus_name[int32(status)]; !known {
		return nil, errx.New(codes.InvalidArgument, errx.ReasonInvalidArgument, "unknown status %d", int32(status))
	}

	rows, err := s.store.List(ctx, store.ListQuery{
		MerchantID:    req.GetMerchantId(),
		Status:        int32(status),
		BeforeOrderID: req.GetBeforeOrderId(),
		Limit:         size + 1,
	})
	if err != nil {
		return nil, fmt.Errorf("list merchant orders: %w", err)
	}
	resp := &orderindexv1.ListMerchantOrdersResponse{}
	if len(rows) > size {
		rows = rows[:size]
		resp.NextBeforeOrderId = rows[size-1].OrderID
	}
	resp.Orders = make([]*orderindexv1.MerchantOrder, len(rows))
	for i, r := range rows {
		resp.Orders[i] = toProto(r)
	}
	return resp, nil
}

func toProto(r store.Row) *orderindexv1.MerchantOrder {
	o := &orderindexv1.MerchantOrder{
		OrderId:    r.OrderID,
		UserId:     r.UserID,
		Status:     orderv1.OrderStatus(r.Status),
		Total:      &commonv1.Money{Amount: r.TotalAmount, Currency: r.Currency},
		CreateTime: timestamppb.New(r.CreatedAt),
	}
	if !r.PaidAt.IsZero() {
		o.PayTime = timestamppb.New(r.PaidAt)
	}
	return o
}
