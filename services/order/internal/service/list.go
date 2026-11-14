package service

import (
	"cmp"
	"context"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/services/order/internal/archive"
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
//
// 已归档的订单（设计文档 1c 第 6.6 节）：配置了冷库时，结果是热库和冷库的并集按订单号倒序的精确分页，
// 游标仍然是订单号。先查热库，必要时（见 mayNeedCold）再用同样的条件查冷库，两边归并（见 mergeNewestFirst）。
// 先热后冷，理由同 loadCold：数据只从热流向冷、写冷在删热之前，一个订单在两次查询之间被归档，
// 先查热时它还在热库，后查冷时它已经在冷库，至少一边看得到；反过来两边都可能看不到。
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
	if s.cold != nil && s.mayNeedCold(rows, more) {
		cold, moreCold, err := s.cold.ListUserOrders(ctx, q)
		if err != nil {
			// 不悄悄只返回热库的部分：那样这一页会漏掉已归档的订单，而且 more 可能是错的，翻页就此结束。
			return nil, s.internal(ctx, "list archived user orders", err, "user_id", req.GetUserId())
		}
		rows, more = mergeNewestFirst(rows, more, cold, moreCold, q.Limit)
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

// mayNeedCold 判断热库这一页（hot、moreHot）之外，是否还需要查冷库才能得出精确的一页。
//
// 热库返回了满页（moreHot 为真，说明后面肯定还有），而且这一页最后一条的订单号新于 now - MinHotAge 时不需要：
// 冷库里的订单号都早于 now - MinHotAge（不变量 3；now 只会往后走，这条只会更宽松），
// 比这一页的每一条都小，挤不进这一页；more 也已经确定为真。活跃用户的头几页因此只走热库。
//
// 条件里必须有 moreHot：热库刚好用完一页（或不满一页）时，后面有没有下一页要看冷库里还有没有，
// 不查冷库就会在冷库还有订单时答 more=false，翻页提前结束。
func (s *Service) mayNeedCold(hot []store.OrderSummary, moreHot bool) bool {
	return !moreHot || archive.MaybeCold(hot[len(hot)-1].OrderID, s.now())
}

// mergeNewestFirst 把热库和冷库各自按同样的条件取出的一页（各至多 limit 条，按订单号倒序）归并成
// 两库并集的一页：按订单号倒序、去重、取前 limit 条，并算出并集里这一页之后是否还有。
//
// 为什么这一页是对的：记 H、C 为两库里满足条件的全部订单，U = H ∪ C。U 的前 limit 条里任何一条 x，
// 如果来自 H，则 H 里比 x 大的都在 U 里也比 x 大，不到 limit 条，所以 x 在 H 的前 limit 条里；C 同理。
// 所以 U 的前 limit 条都在 hot ∪ cold 里，hot ∪ cold 的前 limit 条就是 U 的前 limit 条。
//
// 为什么 more 是对的（more 为真当且仅当 |U| > limit）：
//   - moreHot 为真说明 |H| > limit，于是 |U| ≥ |H| > limit；moreCold 同理；
//   - 两者都为假时 hot = H、cold = C，归并去重的结果就是整个 U，|U| > limit 当且仅当归并结果多于 limit 条。
//
// 一个订单可能在“已写冷、未删热”的瞬间两边都有，内容相同（设计文档 1c 第 6.3 节不变量 1），只留一条。
// 热库那条排在前面、稳定排序保持这个先后、CompactFunc 留每段相同元素的第一个，所以留下的是热库那条。
// 两边各至多 MaxListPageSize 条，拼起来整体排序就够了，不值得手写双指针归并。
func mergeNewestFirst(hot []store.OrderSummary, moreHot bool, cold []store.OrderSummary, moreCold bool, limit int) ([]store.OrderSummary, bool) {
	all := slices.Concat(hot, cold)
	slices.SortStableFunc(all, func(a, b store.OrderSummary) int { return cmp.Compare(b.OrderID, a.OrderID) })
	all = slices.CompactFunc(all, func(a, b store.OrderSummary) bool { return a.OrderID == b.OrderID })
	more := moreHot || moreCold || len(all) > limit
	return all[:min(len(all), limit)], more
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
