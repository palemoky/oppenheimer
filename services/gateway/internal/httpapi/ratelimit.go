package httpapi

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/status"

	"github.com/palemoky/oppenheimer/pkg/ratelimit"
)

// 网关限流（设计文档 1d 第 4 节和 5.8，ADR-0019）。四个维度：
//
//   - 按客户端地址（ip）：进程内的令牌桶，所有接口共用一个桶。在识别用户之前判断，挡没有身份的洪水，
//     被它拒绝的请求不做任何别的事。它不依赖 Redis：打垮 Redis 不应当等于关掉第一道闸。
//   - 按用户加接口（user）和按接口（route）：一次 Allow 判断两个桶，全有或全无，
//     请求被接口的总量拒绝时不消耗用户自己的配额。平时在 Redis 里（所有实例共用），Redis 不可用时
//     由 ratelimit.Fallback 退回进程内。
//   - 热点参数（hot）：只对 hotParams 表里的接口，按路径里某个参数的值（SKU、商家）一个值一个桶，不分用户。
//     进程内，见 limitHot 和 RateLimit.Hot。
//
// 中间件的顺序（从外到内）：traceEntry（链路，见 telemetry.go）→ requestLog → recoverPanic → checkFeature（功能开关，见 degrade.go）→ limitIP → requireUser → limitUserAndRoute
// → limitHot → withDeadline → handler。被任何一维拒绝的请求都不调用下游。

// codeRateLimited 是被限流时的原因码。下单的背压是 429 BACKLOGGED，两者要分开：
// BACKLOGGED 是系统忙，所有人都该退避；RATE_LIMITED 是这个调用方自己太快了。
const codeRateLimited = "RATE_LIMITED"

// operation 是规范里的一个接口。
type operation struct {
	method string
	// path 是 gin 风格的路由模板（:order_id），也就是 c.FullPath() 返回的形式。
	path string
	// id 是规范里的 operationId，限流的配置和键都用它标识接口，不用路径字符串。
	id string
}

// operations 是“方法 + 路由模板 → operationId”的表。
//
// 生成的代码没有告诉中间件当前是哪个 operation：oapi-codegen 的 gin 服务端只有一组对所有接口都一样的
// 中间件，也不在 context 里放标识。所以手写这张表，用方法加 c.FullPath() 查。
// TestOperationsMatchSpec 保证它和规范里的 operation、生成代码注册的路由一一对应，规范加了接口而这里没跟上时测试失败。
// 用数组而不是切片，len(operations) 是常量，统计的计数器可以是定长数组。
var operations = [...]operation{
	{http.MethodGet, "/api/v1/orders", "listOrders"},
	{http.MethodPost, "/api/v1/orders", "createOrder"},
	{http.MethodGet, "/api/v1/orders/:order_id", "getOrder"},
	{http.MethodPost, "/api/v1/orders/:order_id/cancel", "cancelOrder"},
	{http.MethodPost, "/api/v1/orders/:order_id/pay", "payOrder"},
	{http.MethodGet, "/api/v1/orders/:order_id/payment", "getPayment"},
	{http.MethodGet, "/api/v1/skus/:sku_id/stock", "getStock"},
	{http.MethodGet, "/api/v1/merchants/:merchant_id/orders", "listMerchantOrders"},
}

// hotParam 是一个接口的热点参数：用路径里哪个参数的值分桶，以及每个值默认的桶。
type hotParam struct {
	// name 是规范里路径模板中的参数名（{sku_id}），也就是 gin 的 c.Param 用的名字。
	// TestHotParamsMatchSpec 保证它确实出现在这个接口的路径模板里。
	name string
	def  ratelimit.Limit
}

// hotParams 是“operationId → 热点参数”的表（设计文档 1d 5.8 的表），默认值只在这里写一份。
// 不在表里的接口没有热点限流，RATE_LIMIT_HOT 里写它们是配置错误。
//
// 下单不按 SKU 做热点限流：库存闸门已经按 SKU 原子地放行，卖完了的请求在闸门就返回了。
var hotParams = map[string]hotParam{
	// 商品页的展示读，大促时所有人都在刷同一个 SKU。
	"getStock": {name: "sku_id", def: ratelimit.Limit{Rate: 500, Burst: 1000}},
	// 一个大商家的订单列表是范围查询，比查库存贵得多，阈值低一个数量级。
	"listMerchantOrders": {name: "merchant_id", def: ratelimit.Limit{Rate: 50, Burst: 100}},
}

// defaultUserLimits 是按用户的默认限额（设计文档 1d 4.2 的表），默认值只在这里写一份。
// TestEveryOperationHasDefaultUserLimit 保证规范里的每个接口都有一项。
var defaultUserLimits = map[string]ratelimit.Limit{
	// 人不会每秒下 5 单；桶 10 容得下连点和超时后的重试。支付、取消同理。
	"createOrder": {Rate: 5, Burst: 10},
	"payOrder":    {Rate: 5, Burst: 10},
	"cancelOrder": {Rate: 5, Burst: 10},
	// 轮询：客户端每 100 毫秒查一次是 10 次/秒，留一倍。
	"getOrder":   {Rate: 20, Burst: 40},
	"getPayment": {Rate: 20, Burst: 40},
	// 翻页。
	"listOrders":         {Rate: 10, Burst: 20},
	"listMerchantOrders": {Rate: 10, Burst: 20},
	// 商品页的展示读，有缓存。
	"getStock": {Rate: 50, Burst: 100},
}

// OperationIDs 返回规范里所有接口的 operationId，按字母序。
func OperationIDs() []string {
	ids := make([]string, 0, len(operations))
	for _, op := range operations {
		ids = append(ids, op.id)
	}
	slices.Sort(ids)
	return ids
}

// DefaultUserLimits 返回按用户的默认限额（operationId → 限额）的副本。
func DefaultUserLimits() map[string]ratelimit.Limit { return maps.Clone(defaultUserLimits) }

// HotOperationIDs 返回有热点参数的接口的 operationId，按字母序。
func HotOperationIDs() []string { return slices.Sorted(maps.Keys(hotParams)) }

// DefaultHotLimits 返回热点参数每个值的默认限额（operationId → 限额）。
func DefaultHotLimits() map[string]ratelimit.Limit {
	out := make(map[string]ratelimit.Limit, len(hotParams))
	for id, hp := range hotParams {
		out[id] = hp.def
	}
	return out
}

// ParseOperationLimits 解析按接口的限额配置："operationId=速率/桶,..."，例如 "createOrder=5/10,getOrder=off"。
// 结果包含每一个接口：配置里写到的用配置的值（off 表示不限），没写到的用 base 里的（base 里也没有就是不限）。
// 整个字符串是 "off" 时所有接口都不限；空字符串时就是 base。
// 出现规范里没有的 operationId、同一个接口写了两次、格式错误时返回错误，指出是哪一项。
func ParseOperationLimits(s string, base map[string]ratelimit.Limit) (map[string]ratelimit.Limit, error) {
	return parseLimits(s, OperationIDs(), base)
}

// ParseHotLimits 解析热点参数的限额配置（RATE_LIMIT_HOT），格式和规则同 ParseOperationLimits，限额指的是这个接口下
// 每个参数值的桶。结果只包含 hotParams 里的接口；写了规范里有、但没有热点参数的接口也是错误
// （这一维对它没有意义，写了多半是写错了接口）。
func ParseHotLimits(s string, base map[string]ratelimit.Limit) (map[string]ratelimit.Limit, error) {
	return parseLimits(s, HotOperationIDs(), base)
}

// parseLimits 是 ParseOperationLimits 和 ParseHotLimits 的实现，ids 是允许出现的 operationId。
func parseLimits(s string, ids []string, base map[string]ratelimit.Limit) (map[string]ratelimit.Limit, error) {
	out := make(map[string]ratelimit.Limit, len(ids))
	s = strings.TrimSpace(s)
	if s == "off" {
		for _, id := range ids {
			out[id] = ratelimit.Limit{}
		}
		return out, nil
	}
	for _, id := range ids {
		out[id] = base[id]
	}
	seen := make(map[string]bool)
	for item := range strings.SplitSeq(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		id, value, ok := strings.Cut(item, "=")
		id = strings.TrimSpace(id)
		if !ok || id == "" {
			return nil, fmt.Errorf("%q: want operationId=rate/burst or operationId=off", item)
		}
		if _, known := out[id]; !known {
			return nil, unknownOperation(item, id, ids)
		}
		if seen[id] {
			return nil, fmt.Errorf("%q: operationId %q given more than once", item, id)
		}
		seen[id] = true
		l, err := ratelimit.Parse(value)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", item, err)
		}
		out[id] = l
	}
	return out, nil
}

// unknownOperation 是配置里出现了不允许的 operationId 时的错误。区分“规范里根本没有”和“有，但这一维不适用”，
// 后者多半是把配置写到了错误的变量里。
func unknownOperation(item, id string, allowed []string) error {
	known := strings.Join(allowed, ", ")
	if slices.ContainsFunc(operations[:], func(op operation) bool { return op.id == id }) {
		return fmt.Errorf("%q: operation %q does not take this limit (allowed: %s)", item, id, known)
	}
	return fmt.Errorf("%q: unknown operationId %q (known: %s)", item, id, known)
}

// HotItem 标识热点限流里的一个参数值：某个接口下的某个 SKU 或商家。
type HotItem struct {
	Operation string
	Value     int64
}

// ParseHotItems 解析个别参数值的限额（RATE_LIMIT_HOT_ITEMS）："operationId:参数值=速率/桶,..."，
// 例如 "getStock:1001=5000/10000"（某个大促的 SKU）。值也可以是 off（这个值不限）。
// operationId 必须有热点参数，参数值必须是正整数（和 handler 接受的 ID 一样）；同一项写两次、格式错误都返回错误，
// 指出是哪一项。
func ParseHotItems(s string) (map[HotItem]ratelimit.Limit, error) {
	out := make(map[HotItem]ratelimit.Limit)
	ids := HotOperationIDs()
	for item := range strings.SplitSeq(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		target, value, hasValue := strings.Cut(item, "=")
		id, raw, hasParam := strings.Cut(target, ":")
		id, raw = strings.TrimSpace(id), strings.TrimSpace(raw)
		if !hasValue || !hasParam || id == "" || raw == "" {
			return nil, fmt.Errorf("%q: want operationId:value=rate/burst or operationId:value=off", item)
		}
		hp, hot := hotParams[id]
		if !hot {
			return nil, unknownOperation(item, id, ids)
		}
		n, valid := parseID(raw)
		if !valid {
			return nil, fmt.Errorf("%q: %s %q is not a positive integer", item, hp.name, raw)
		}
		key := HotItem{Operation: id, Value: n}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("%q: %s:%d given more than once", item, id, n)
		}
		l, err := ratelimit.Parse(value)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", item, err)
		}
		out[key] = l
	}
	return out, nil
}

// RateLimit 是限流的配置。Config.RateLimit 为 nil 时不限流。
type RateLimit struct {
	// IP 是按客户端地址的限流器（应当是进程内的），IPLimit 是每个地址的桶。
	// IP 为 nil 或 IPLimit 不限时不按地址限流，中间件也不挂。
	IP      ratelimit.Limiter
	IPLimit ratelimit.Limit
	// TrustedProxies 是受信任的代理，决定 X-Forwarded-For 能不能信，见 clientAddr。
	TrustedProxies []netip.Prefix

	// Shared 是按用户和按接口的限流器（通常是 Fallback{Primary: Redis, Secondary: Local}）。
	// 为 nil 或者所有接口的 User、Route 都不限时，这两维不限，中间件也不挂。
	Shared ratelimit.Limiter
	// User 和 Route 以 operationId 为键：每个用户对这个接口的桶、这个接口所有用户合计的桶。
	// 没有的项不限。键应当来自 ParseOperationLimits（它校验 operationId），这里不认识的键不起作用。
	User, Route map[string]ratelimit.Limit

	// Hot 是热点参数的限流器，应当是进程内的、单独的一个实例（不和按 IP 的共用：两者的键空间不同，
	// 一边的键多了会把另一边的桶挤出去）。为 nil、或者 HotLimits 和 HotItems 都不限时，这一维不限，中间件也不挂。
	//
	// 状态放在进程里而不是 Redis（设计文档 1d 5.8）：热点保护要的是“别让一个 SKU 把这个实例占满”，
	// 每个实例挡住打到自己身上的那一份就够了，不需要跨实例精确。而且被打爆的恰恰是热点值，
	// 放进 Redis 等于把所有实例对同一个热点的请求再汇聚到同一个 Redis 键上，制造出一个新的热点，
	// 每个请求还多一次往返。代价是总量是 实例数 × 单实例的阈值，阈值按单实例来配。
	Hot ratelimit.Limiter
	// HotLimits 以 operationId 为键，是这个接口下每个参数值默认的桶，应当来自 ParseHotLimits。
	// 不在 hotParams 里的接口不起作用。
	HotLimits map[string]ratelimit.Limit
	// HotItems 是个别参数值单独的桶，覆盖 HotLimits 里这个接口的默认值，应当来自 ParseHotItems。
	HotItems map[HotItem]ratelimit.Limit

	// Stats 收集被拒绝的次数，给 RateLimitReporter 汇总。为 nil 时内部建一个（没有人读）。
	Stats *RateLimitStats
}

// dimension 是限流的维度。
type dimension int

const (
	dimIP dimension = iota
	dimUser
	dimRoute
	dimHot
	numDimensions
)

// dimensionNames 是对外的维度名，出现在 429 的错误信息和汇总日志里。客户端和测试依赖它们，不要改。
var dimensionNames = [numDimensions]string{"ip", "user", "route", "hot"}

// rateLimitedMessages 是 429 的错误信息，预先拼好，拒绝时不必每次格式化。
var rateLimitedMessages = [numDimensions]string{
	"rate limit exceeded: ip",
	"rate limit exceeded: user",
	"rate limit exceeded: route",
	"rate limit exceeded: hot",
}

// routeKey 是查 operation 的键。用结构体而不是拼接的字符串，查表时不分配内存。
type routeKey struct{ method, path string }

// opLimits 是一个接口的限流参数和预先拼好的键。
type opLimits struct {
	// index 是它在 operations 里的下标，用来定位统计的计数器。
	index int
	// userKeySuffix 是 ":<operationId>"，用户的桶的键是 "u:<用户号>:<operationId>"。
	userKeySuffix string
	// routeKey 是接口的桶的键 "r:<operationId>"。
	routeKey    string
	user, route ratelimit.Limit
	// hot 为 nil 表示这个接口没有热点限流（没有热点参数，或者配置成全部不限）。
	hot *hotLimits
}

// hotLimits 是一个接口的热点限流参数。
type hotLimits struct {
	// param 是路径参数的名字。
	param string
	// keyPrefix 是 "h:<operationId>:"，桶的键是它加上规范化的参数值。
	keyPrefix string
	// def 是每个参数值默认的桶；items 是个别参数值单独的桶，按解析后的整数查（查表不分配内存）。
	def   ratelimit.Limit
	items map[int64]ratelimit.Limit
}

// rateLimiter 是 server 里限流用到的状态。
type rateLimiter struct {
	ip      ratelimit.Limiter
	ipLimit ratelimit.Limit
	trusted []netip.Prefix
	shared  ratelimit.Limiter
	hot     ratelimit.Limiter
	ops     map[routeKey]*opLimits
	stats   *RateLimitStats
}

// newRateLimiter 按配置建好查表用的结构；返回 nil 表示不限流。
// 三个返回的布尔值说明要不要挂按 IP 的、按用户和接口的、热点参数的中间件。
func newRateLimiter(cfg *RateLimit) (rl *rateLimiter, limitIP, limitShared, limitHot bool) {
	if cfg == nil {
		return nil, false, false, false
	}
	rl = &rateLimiter{
		ip:      cfg.IP,
		ipLimit: cfg.IPLimit,
		trusted: slices.Clone(cfg.TrustedProxies),
		shared:  cfg.Shared,
		hot:     cfg.Hot,
		ops:     make(map[routeKey]*opLimits, len(operations)),
		stats:   cmp.Or(cfg.Stats, &RateLimitStats{}),
	}
	for i, op := range operations {
		ol := &opLimits{
			index:         i,
			userKeySuffix: ":" + op.id,
			routeKey:      "r:" + op.id,
			user:          cfg.User[op.id],
			route:         cfg.Route[op.id],
		}
		ol.hot = newHotLimits(op.id, cfg)
		rl.ops[routeKey{op.method, op.path}] = ol
		if ol.user.Enabled() || ol.route.Enabled() {
			limitShared = true
		}
		if ol.hot != nil {
			limitHot = true
		}
	}
	limitIP = rl.ip != nil && rl.ipLimit.Enabled()
	limitShared = limitShared && rl.shared != nil
	limitHot = limitHot && rl.hot != nil
	return rl, limitIP, limitShared, limitHot
}

// newHotLimits 返回接口 id 的热点限流参数；没有热点参数、或者默认值和所有单独设的值都不限时返回 nil。
func newHotLimits(id string, cfg *RateLimit) *hotLimits {
	hp, ok := hotParams[id]
	if !ok {
		return nil
	}
	h := &hotLimits{param: hp.name, keyPrefix: "h:" + id + ":", def: cfg.HotLimits[id]}
	enabled := h.def.Enabled()
	for item, l := range cfg.HotItems {
		if item.Operation != id {
			continue
		}
		if h.items == nil {
			h.items = make(map[int64]ratelimit.Limit)
		}
		// 单独设成 off 的也要记下：它覆盖默认值，表示这个值不限。
		h.items[item.Value] = l
		enabled = enabled || l.Enabled()
	}
	if !enabled {
		return nil
	}
	return h
}

// op 返回当前请求的接口；不在表里时返回 nil（TestOperationsMatchSpec 保证 API 分组里的路由都在表里）。
func (rl *rateLimiter) op(c *gin.Context) *opLimits {
	return rl.ops[routeKey{c.Request.Method, c.FullPath()}]
}

// limitIP 按客户端地址限流。在 requireUser 之前：没有身份的洪水在这里就被拒绝，不再解析用户、不碰 Redis。
func (s *server) limitIP(c *gin.Context) {
	rl := s.rl
	d, err := rl.ip.Allow(c.Request.Context(), ratelimit.Request{Key: clientKey(c.Request, rl.trusted), Limit: rl.ipLimit})
	if err != nil {
		s.rateLimiterFailed(c, dimIP, err)
		return
	}
	if !d.Allowed {
		// 只在拒绝时才查是哪个接口（给统计用），放行的路径上少做一次查表。
		s.rejectRateLimited(c, rl.op(c), dimIP, d.RetryAfter)
		return
	}
	recordDecision(c.Request.Context(), dimIP, true)
}

// limitUserAndRoute 按用户加接口、按接口限流：一次 Allow 判断两个桶，用户的在前、接口的在后。
// 两个桶全有或全无，被接口的总量拒绝时不扣用户的令牌。某一维不限时照样传，限流器会忽略零值的 Limit。
func (s *server) limitUserAndRoute(c *gin.Context) {
	rl := s.rl
	op := rl.op(c)
	if op == nil || (!op.user.Enabled() && !op.route.Enabled()) {
		return
	}
	userKey := "u:" + strconv.FormatInt(userID(c), 10) + op.userKeySuffix
	d, err := rl.shared.Allow(c.Request.Context(),
		ratelimit.Request{Key: userKey, Limit: op.user},
		ratelimit.Request{Key: op.routeKey, Limit: op.route},
	)
	if err != nil {
		s.rateLimiterFailed(c, dimUser, err)
		return
	}
	if !d.Allowed {
		dim := dimUser
		if d.Denied == 1 {
			dim = dimRoute
		}
		// 被拒绝时只记拒绝的那一维：两个桶全有或全无，另一维什么也没有扣，不算它放行了。
		s.rejectRateLimited(c, op, dim, d.RetryAfter)
		return
	}
	// 放行时两维各记一次（不限的那一维不是判定，不记）。
	ctx := c.Request.Context()
	if op.user.Enabled() {
		recordDecision(ctx, dimUser, true)
	}
	if op.route.Enabled() {
		recordDecision(ctx, dimRoute, true)
	}
}

// limitHot 按热点参数限流：这个接口下同一个参数值（SKU、商家）的所有请求共用一个桶，不分用户。
// 在 limitUserAndRoute 之后：一个用户自己刷得太快应当报 user，被按用户拒绝的请求也不消耗这个值的公共额度。
// 在 withDeadline 之前：进程内的判断没有网络往返，但也没有理由占用下游调用的预算。
//
// 参数值不是正整数时不限流、直接放过，由 handler 照原来的方式返回 400：垃圾值不该在限流器里占桶
// （换着花样写垃圾值就能不断建新桶，把真正的热点挤出去）。桶的键用解析后的整数重新格式化，
// 不用原始字符串：parseID 接受 "007"、"+7"，按原始字符串分桶的话换个写法就能绕过同一个 SKU 的限额。
func (s *server) limitHot(c *gin.Context) {
	rl := s.rl
	op := rl.op(c)
	if op == nil || op.hot == nil {
		return
	}
	h := op.hot
	n, ok := parseID(c.Param(h.param))
	if !ok {
		return
	}
	limit := h.def
	if l, set := h.items[n]; set {
		limit = l
	}
	if !limit.Enabled() {
		return
	}
	// 键在栈上的缓冲区里拼好，只在转成 string 时分配一次（限流器要把它存成 map 的键）。
	var buf [64]byte
	key := string(strconv.AppendInt(append(buf[:0], h.keyPrefix...), n, 10))
	d, err := rl.hot.Allow(c.Request.Context(), ratelimit.Request{Key: key, Limit: limit})
	if err != nil {
		s.rateLimiterFailed(c, dimHot, err)
		return
	}
	if !d.Allowed {
		s.rejectRateLimited(c, op, dimHot, d.RetryAfter)
		return
	}
	recordDecision(c.Request.Context(), dimHot, true)
}

// rejectRateLimited 写出 429 RATE_LIMITED 并中止请求。不记日志：洪水来时每个请求一条日志会成为第二个问题，
// 只计数，由 RateLimitReporter 周期性地汇总成一条。requestLog 照常记这个请求的访问日志。
// 同时记指标（gateway.ratelimit.decisions）和 span 上的原因：汇总日志按接口分，指标不按接口分（规则和结果就够了）。
func (s *server) rejectRateLimited(c *gin.Context, op *opLimits, dim dimension, retryAfter time.Duration) {
	if op != nil {
		s.rl.stats.denied[op.index][dim].Add(1)
	}
	ctx := c.Request.Context()
	recordDecision(ctx, dim, false)
	setRateLimited(ctx, dim)
	c.Header("Retry-After", strconv.FormatInt(retryAfterSeconds(retryAfter), 10))
	writeError(c, http.StatusTooManyRequests, codeRateLimited, rateLimitedMessages[dim])
	c.Abort()
}

// retryAfterSeconds 把限流器给的等待时间换成 Retry-After 的秒数：向上取整（取整到下面会让客户端来早了
// 再被拒一次），至少 1（0 会被理解成“立刻重试”）。
func retryAfterSeconds(d time.Duration) int64 {
	return max(int64((d+time.Second-1)/time.Second), 1)
}

// rateLimiterFailed 处理限流器自己出错的情况。
//
// Fallback 已经把 Redis 的失败退回了进程内的限流器，走到这里只有两种可能：调用方取消了请求，
// 或者进程内的限流器也出错了（只会是参数不合法这类代码缺陷）。前者按下游调用被取消一样结束请求
// （客户端断开时是 499，见 writeGRPCError）；后者放行：限流是保护措施，不能因为它自己的问题拒绝正常的请求。
// dim 只用于日志。
func (s *server) rateLimiterFailed(c *gin.Context, dim dimension, err error) {
	ctx := c.Request.Context()
	if ctxErr := ctx.Err(); ctxErr != nil {
		s.writeGRPCError(c, status.FromContextError(ctxErr).Err())
		c.Abort()
		return
	}
	s.logger.WarnContext(ctx, "rate limiter failed, letting the request through",
		"dimension", dimensionNames[dim], "route", c.FullPath(), "error", err)
}

// RateLimitStats 是被限流的次数，按接口和维度分开计。并发安全，零值可用。
type RateLimitStats struct {
	denied [len(operations)][numDimensions]atomic.Int64
}

// DefaultRateLimitReportInterval 是限流汇总日志的间隔。
const DefaultRateLimitReportInterval = 10 * time.Second

// RateLimitReporter 每隔一段时间把被限流的次数汇总成一行日志（按“维度.operationId”分开），
// 带上这段时间里 Fallback 退回进程内限流器的次数。这段时间什么都没有时不输出。实现 app.Component。
type RateLimitReporter struct {
	stats     *RateLimitStats
	fallbacks func() int64
	log       *slog.Logger
	interval  time.Duration

	// mu 保护 lastFallbacks：Flush 可能被 Start 的循环和停止时并发调用。
	mu            sync.Mutex
	lastFallbacks int64

	stopOnce sync.Once
	stop     chan struct{}
}

// NewRateLimitReporter 返回 RateLimitReporter。fallbacks 返回 Fallback 退回的累计次数（Fallback.Fallbacks），
// 不用 Redis 时传 nil。interval 不大于 0 时用 DefaultRateLimitReportInterval。
func NewRateLimitReporter(stats *RateLimitStats, fallbacks func() int64, logger *slog.Logger, interval time.Duration) *RateLimitReporter {
	if interval <= 0 {
		interval = DefaultRateLimitReportInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	r := &RateLimitReporter{stats: stats, fallbacks: fallbacks, log: logger, interval: interval, stop: make(chan struct{})}
	if fallbacks != nil {
		// 从创建时开始算，之前的退回（比如启动时 Redis 不通）不算进第一段。
		r.lastFallbacks = fallbacks()
	}
	return r
}

// Name 实现 app.Component。
func (r *RateLimitReporter) Name() string { return "gateway-rate-limit-reporter" }

// Start 实现 app.Component：定期输出，直到 Stop 或 ctx 取消。停止前把最后一段输出掉。
func (r *RateLimitReporter) Start(ctx context.Context) error {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			r.Flush(ctx)
		case <-r.stop:
			r.Flush(ctx)
			return nil
		case <-ctx.Done():
			r.Flush(ctx)
			return nil
		}
	}
}

// Stop 实现 app.Component。
func (r *RateLimitReporter) Stop(context.Context) error {
	r.stopOnce.Do(func() { close(r.stop) })
	return nil
}

// Flush 输出并清零当前一段的统计；没有被限流、也没有退回时不输出。
func (r *RateLimitReporter) Flush(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var denied []slog.Attr
	var total int64
	for i := range r.stats.denied {
		for d := range numDimensions {
			if n := r.stats.denied[i][d].Swap(0); n > 0 {
				denied = append(denied, slog.Int64(dimensionNames[d]+"."+operations[i].id, n))
				total += n
			}
		}
	}
	var fallbacks int64
	if r.fallbacks != nil {
		now := r.fallbacks()
		fallbacks, r.lastFallbacks = now-r.lastFallbacks, now
	}
	if total == 0 && fallbacks == 0 {
		return
	}
	attrs := []slog.Attr{
		slog.String("interval", r.interval.String()),
		slog.Int64("denied_total", total),
		// denied 里每一项是 "维度.operationId": 次数，维度是 ip、user、route、hot。
		slog.GroupAttrs("denied", denied...),
	}
	if r.fallbacks != nil {
		// 这段时间里按用户和接口的判断有多少次没有用 Redis，而是退回了进程内（限额变宽）。
		attrs = append(attrs, slog.Int64("fallbacks", fallbacks))
	}
	r.log.LogAttrs(ctx, slog.LevelInfo, "rate limit summary", attrs...)
}
