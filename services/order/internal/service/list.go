package service

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 用户订单列表的分页参数。
const (
	DefaultListPageSize = 20
	MaxListPageSize     = 100
)

// listTimeout 是一次列表查询的时限。调用方给的 deadline 更短时以调用方为准。
const listTimeout = 3 * time.Second

// ListUserOrders 实现 OrderServiceServer：按订单号倒序（创建时间从新到旧）列出用户的订单，游标分页。
//
// 只查用户所在的一个分片。已受理还没落库的订单只在闸门的占用里，不在列表里（proto 注释写明了），
// 这里不去合并占用：占用按订单号存，按用户找它们要么扫描、要么再维护一份按用户的索引，不值得。
//
// 读路径：始终读主库，与读模式无关。读己之写的标记（readmark）按订单打，列表是“这个用户的一组订单”，
// 要读从库就得证明这个用户最近写过的每一个订单都已复制到从库，而读的一方事先不知道是哪些订单
// （刚落库的新订单正是从库上没有、也就无从查起标记的那一行）。按订单的标记套不到集合查询上，
// 硬套的结果就是“下单后看列表看不到刚下的单”。按“任何不确定都退回主库”的原则（见 useReplica），
// 这里没有能证明新鲜的依据，所以读主库。这条查询走 idx_orders_user、只读一个分片的一小段索引，
// 主库多承担它的代价不大。以后要分流，需要在每个写订单的地方再按用户打一份 pin 和 GTID 标记
// （用户和订单在同一个库，按订单读出的主库位置对用户同样有效）。
func (s *Service) ListUserOrders(ctx context.Context, req *orderv1.ListUserOrdersRequest) (*orderv1.ListUserOrdersResponse, error) {
	q, err := validateListUserOrders(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, listTimeout)
	defer cancel()
	rows, more, err := s.store.ListUserOrders(ctx, q, store.Primary)
	if err != nil {
		return nil, s.internal(ctx, "list user orders", err, "user_id", req.GetUserId())
	}
	resp := &orderv1.ListUserOrdersResponse{Orders: make([]*orderv1.OrderSummary, 0, len(rows))}
	for _, r := range rows {
		resp.Orders = append(resp.Orders, toSummaryProto(r))
	}
	if more {
		resp.NextBeforeOrderId = rows[len(rows)-1].OrderID
	}
	return resp, nil
}

func validateListUserOrders(req *orderv1.ListUserOrdersRequest) (store.UserOrdersQuery, error) {
	if req.GetUserId() <= 0 {
		return store.UserOrdersQuery{}, invalidArgument("user_id must be positive")
	}
	size := int(req.GetPageSize())
	switch {
	case size == 0:
		size = DefaultListPageSize
	case size < 1 || size > MaxListPageSize:
		return store.UserOrdersQuery{}, invalidArgument("page_size must be 1 to %d, or 0 for the default", MaxListPageSize)
	}
	if req.GetBeforeOrderId() < 0 {
		return store.UserOrdersQuery{}, invalidArgument("before_order_id must not be negative")
	}
	// 不认识的枚举值不能当作“不筛选”，否则客户端拼错状态会拿到全部订单。
	if _, known := orderv1.OrderStatus_name[int32(req.GetStatus())]; !known {
		return store.UserOrdersQuery{}, invalidArgument("unknown status %d", int32(req.GetStatus()))
	}
	return store.UserOrdersQuery{
		UserID:        req.GetUserId(),
		Status:        req.GetStatus(),
		BeforeOrderID: req.GetBeforeOrderId(),
		Limit:         size,
	}, nil
}

func toSummaryProto(o store.OrderSummary) *orderv1.OrderSummary {
	p := &orderv1.OrderSummary{
		OrderId:    o.OrderID,
		Status:     o.Status,
		Total:      &commonv1.Money{Amount: o.TotalAmount, Currency: o.Currency},
		CreateTime: timestamppb.New(o.CreatedAt),
		ExpireTime: timestamppb.New(o.ExpireAt),
	}
	if !o.PaidAt.IsZero() {
		p.PayTime = timestamppb.New(o.PaidAt)
	}
	return p
}
