package httpapi

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/breaker"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 降级（设计文档 1d 第 6 节）在 gateway 这一层有两件事：
//
//   - 下游返回的降级错误（errx.Degraded）对客户端是 503 加原因码和 Retry-After，见 writeGRPCError。
//   - 非核心接口的开关（6.9）：关掉之后直接返回 503 FEATURE_DISABLED，不调用下游。

// degradedRetryAfter 是降级响应的 Retry-After（5 秒）。取下游各服务进程里熔断器的冷却时间（breaker.DefaultCooldown），
// 也和 Envoy 边车摘除的基础时长一样：熔断器打开期间再来也是同样的结果，早于冷却结束重试没有意义；
// 冷却结束时熔断器放行试探的请求，客户端这时再来正好赶上。
// 人工关掉的开关不知道什么时候打开，用同一个数，让客户端按固定的节奏退避，而不是立刻重试。
// 它只是建议：下游的熔断器各自可以配不同的冷却时间，gateway 看不到。
const degradedRetryAfter = breaker.DefaultCooldown

// 非核心接口的开关的名字（设计文档 1d 的 6.9）。下单、查单个订单、取消、支付是核心接口，没有开关：
// 关掉它们就是停服，不是降级。下单的开关（admission）在 order 服务里，因为它要在受理之前判断。
const (
	SwitchMerchantOrders = "merchant_orders"
	SwitchUserOrders     = "user_orders"
	SwitchStockQuery     = "stock_query"
)

// feature 是一个可以被关掉的接口。
type feature struct {
	switchName string
	desc       string
}

// features 是“operationId → 开关”的表。TestFeaturesMatchSpec 保证这里的 operationId 都在规范里。
var features = map[string]feature{
	"listMerchantOrders": {SwitchMerchantOrders, "merchant order list returns 503 FEATURE_DISABLED"},
	"listOrders":         {SwitchUserOrders, "user order list returns 503 FEATURE_DISABLED"},
	"getStock":           {SwitchStockQuery, "stock query returns 503 FEATURE_DISABLED"},
}

// registerFeatures 把 features 里的开关登记到 set 上，返回“路由 → 开关”的表，供 checkFeature 查。
func registerFeatures(set *degrade.Set) map[routeKey]*degrade.Switch {
	byRoute := make(map[routeKey]*degrade.Switch, len(features))
	for _, op := range operations {
		f, ok := features[op.id]
		if !ok {
			continue
		}
		byRoute[routeKey{op.method, op.path}] = set.Switch(f.switchName, f.desc)
	}
	return byRoute
}

// checkFeature 拒绝被关掉的接口的请求，不调用下游。
//
// 排在 API 分组的最前面，在所有限流之前：判断只是读一个原子变量，比任何一维限流都便宜。
// 关掉一个功能通常就是因为它在给系统添负担，被关掉的请求不该再去碰限流的 Redis，也不该消耗用户和接口的配额
// （否则功能恢复的那一刻，用户的配额已经被这段时间里注定失败的请求用掉了）。
// 不记日志：开关关着时每个请求都会走到这里，requestLog 照常记访问日志。
func (s *server) checkFeature(c *gin.Context) {
	if s.features[routeKey{c.Request.Method, c.FullPath()}].Off() {
		writeDegraded(c, errx.ReasonFeatureDisabled, "this feature is temporarily switched off, retry later")
		c.Abort()
	}
}

// writeDegraded 写出降级的 503：原因码、给人看的消息和 Retry-After。
//
// 降级响应只从这里写出，所以在这里计数（gateway.degraded.responses）并把原因码记在 span 上。
// reason 只会是降级原因码（errx.IsDegraded 判断过的，或 FEATURE_DISABLED），是有限的几个，可以做标签。
// 在 gin context 里设 degradedKey：traceEntry 据此不把这个 503 标成 Error（降级不是 gateway 的故障，设计文档 1d 8.4）。
func writeDegraded(c *gin.Context, reason, message string) {
	ctx := c.Request.Context()
	c.Set(degradedKey, true)
	degradedResponses.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
	otelx.SetReason(ctx, reason)
	c.Header("Retry-After", strconv.FormatInt(retryAfterSeconds(degradedRetryAfter), 10))
	writeError(c, http.StatusServiceUnavailable, reason, message)
}
