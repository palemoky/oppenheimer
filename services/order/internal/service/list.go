package service

import (
	"cmp"
	"context"
	"errors"
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
// 游标仍然是订单号。冷库不可用时不整页失败，返回精确的前一段并设 ArchiveUnavailable（见 listHotOnly）。先查热库，必要时（见 mayNeedCold）再用同样的条件查冷库，两边归并（见 mergeNewestFirst）。
// 先热后冷，理由同 loadCold：数据只从热流向冷、写冷在删热之前，一个订单在两次查询之间被归档，
// 先查热时它还在热库，后查冷时它已经在冷库，至少一边看得到；反过来两边都可能看不到。
func (s *Service) ListUserOrders(ctx context.Context, req *orderv1.ListUserOrdersRequest) (*orderv1.ListUserOrdersResponse, error) {
	q, err := validateListUserOrders(req)
	if err != nil {
		return nil, err
	}
	// lctx 是本服务给这次列表的总时限；internal 用请求自己的 ctx，以便区分“调用方超时”和“这里的时限到了”。
	lctx, cancel := hopContext(ctx, listTimeout)
	defer cancel()
	rows, more, err := s.listHot(lctx, q)
	if err != nil {
		return nil, s.listError(ctx, "list user orders", hopErr(lctx, err), req.GetUserId())
	}
	if s.cold != nil && s.mayNeedCold(rows, more) {
		const op = "list archived user orders"
		cold, moreCold, err := s.listCold(lctx, q)
		switch {
		case err == nil:
			rows, more = mergeNewestFirst(rows, more, cold, moreCold, q.Limit)
		case s.archiveUnavailable(lctx, op, err, "user_id", req.GetUserId()):
			// 冷库不可用：返回精确的前一段，并明说不完整（设计文档 1d 的 6.8）。
			return s.listHotOnly(rows)
		default:
			// 调用方取消、超时，本服务给列表的总时限到了，名额满：按原来的方式返回错误。
			// 不悄悄只返回热库的部分：那样这一页会漏掉已归档的订单，而且 more 可能是错的，翻页就此结束。
			return nil, s.listError(ctx, op, hopErr(lctx, err), req.GetUserId())
		}
	}
	resp := newListResponse(rows)
	if more {
		resp.NextBeforeOrderId = rows[len(rows)-1].OrderID
	}
	return resp, nil
}

func newListResponse(rows []store.OrderSummary) *orderv1.ListUserOrdersResponse {
	resp := &orderv1.ListUserOrdersResponse{Orders: make([]*orderv1.OrderSummary, 0, len(rows))}
	for _, r := range rows {
		resp.Orders = append(resp.Orders, toSummaryProto(r))
	}
	return resp
}

// listHotOnly 是冷库不可用时的列表：热库这一页里订单号晚于冷热分界的那一段（!archive.MaybeCold），
// 带上 ArchiveUnavailable 和指向这一段最后一条的游标。这一段为空时返回 errArchiveUnavailable。
//
// 为什么这一段是精确的（和冷库正常时那一页的前 n 条完全相同）：沿用 mergeNewestFirst 的记号，
// H、C 是两库里满足条件（用户、状态、游标之前）的全部订单，U = H ∪ C，hot 是热库按订单号倒序的前 limit 条。
// 记 P 为 hot 里 !MaybeCold 的那些行，m 为 P 里最小的订单号。
//  1. P 是 hot 的一个前缀：MaybeCold 只看订单号里的时间，订单号越小时间越早（时间在订单号的高位），
//     所以 MaybeCold 随订单号单调，倒序排列的 hot 里一旦出现 MaybeCold 的行，后面的都是。
//  2. C 里没有大于 m 的订单：冷库里的订单号都早于 now - MinHotAge（设计文档 1c 第 6.3 节不变量 3；
//     now 只会往后走，这条只会更宽松），对它们 MaybeCold 为真；而 m 的 MaybeCold 为假，由单调性，
//     任何大于 m 的订单号 MaybeCold 也为假，不可能在 C 里。
//  3. 所以 U 里大于等于 m 的订单都在 H 里。H 里大于等于 m 的订单排在 m 前面，m 在 hot（H 的前 limit 条）里，
//     它们也都在 hot 里，并且由单调性都是 !MaybeCold，也就都在 P 里。反过来 P ⊆ H ⊆ U。
//     于是 P 恰好是 U 里大于等于 m 的全部订单，也就是 U 按倒序的前 |P| 条：和冷库正常时那一页的前 |P| 条相同，
//     不多（没有不满足条件的）、不少（没有漏掉冷库里的）、顺序一样。
//
// 为什么游标 m 可以接着用：下一次请求取的是 U 里小于 m 的订单，和正常翻页的语义完全一样；
// 由 3，大于等于 m 的已经全部返回过了，接着取不会漏也不会重。那时冷库恢复了就是正常的一页（热冷归并），
// 还没恢复就再返回下一段（通常为空，因为 P 已经到了冷热分界，于是得到 ARCHIVE_UNAVAILABLE）。
// 游标总是给出，即使 U 里其实已经没有更早的订单：没有冷库就不知道“后面还有没有”，不能答 more=false。
//
// 这一段为空（游标已经翻过冷热分界，或者 hot 的第一条就可能在冷库）时没有任何能确定的东西，返回降级错误。
func (s *Service) listHotOnly(hot []store.OrderSummary) (*orderv1.ListUserOrdersResponse, error) {
	now := s.now()
	n := len(hot)
	if i := slices.IndexFunc(hot, func(r store.OrderSummary) bool { return archive.MaybeCold(r.OrderID, now) }); i >= 0 {
		n = i
	}
	if n == 0 {
		return nil, errArchiveUnavailable
	}
	resp := newListResponse(hot[:n])
	resp.ArchiveUnavailable = true
	resp.NextBeforeOrderId = hot[n-1].OrderID
	return resp, nil
}

// listHot 读用户所在库的主库，先占那个库的展示读名额（与 GetOrder 共用，设计文档 1d 的 5.7）。
func (s *Service) listHot(ctx context.Context, q store.UserOrdersQuery) ([]store.OrderSummary, bool, error) {
	router := s.store.Router()
	release, err := acquire(s.limits.db(router.DB(router.ForUser(q.UserID))))
	if err != nil {
		return nil, false, err
	}
	defer release()
	return s.store.ListUserOrders(ctx, q, store.Primary)
}

// listCold 读冷库，经过 coldRead（迁移、开关、熔断器、名额，最多 coldTimeout，与 loadCold 相同）。
func (s *Service) listCold(ctx context.Context, q store.UserOrdersQuery) (rows []store.OrderSummary, more bool, err error) {
	err = s.coldRead(ctx, func(ctx context.Context) error {
		var err error
		rows, more, err = s.cold.ListUserOrders(ctx, q)
		return err
	})
	return rows, more, err
}

// listError 把列表的错误转成对外的错误：名额满原样返回（RESOURCE_EXHAUSTED），其余交给 internal。
func (s *Service) listError(ctx context.Context, op string, err error, userID int64) error {
	if errors.Is(err, errReadBusy) {
		return errReadBusy
	}
	return s.internal(ctx, op, err, "user_id", userID)
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
