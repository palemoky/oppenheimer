// Package httpapi 实现 gateway 的 HTTP 接口：把请求转成对下游服务的 gRPC 调用。
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	orderindexv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/orderindex/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/grpcx"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi/oas"
)

const (
	// DefaultCallTimeout 是单个请求内所有下游调用的总时限。
	DefaultCallTimeout = 5 * time.Second
	// DownstreamHopBudget 是 gateway 给任一下游的单跳预算（设计文档 1d 5.6）。总时限 5 秒，给下游 3 秒：
	// 下游超时之后 gateway 还来得及返回明确的错误，而不是和下游在同一刻一起超时。
	// 这个数写在 deploy/envoy/gateway.yaml 四条出站路由的 timeout 上（TestHopTimeoutMatchesEnvoy 核对），由边车执行：
	// 只有边车自己的路由超时到期才计入异常检测，慢下游才会被摘除。
	DownstreamHopBudget = 3 * time.Second
	// DownstreamHopTimeout 是进程里的单跳时限，由 grpcx.WithHopTimeout 加在每条下游连接上。
	// 比预算多 grpcx.SidecarMargin：让边车先超时（理由见那个常量），进程里的只在边车不在或卡住时兜底。
	// 所以正常情况下，下游超时时客户端看到的是边车返回的 503（Unavailable），不是 504。
	DownstreamHopTimeout = DownstreamHopBudget + grpcx.SidecarMargin
	// maxBodyBytes 限制请求体大小，防止客户端用超大 body 占用内存。
	maxBodyBytes = 1 << 20
)

// Deps 是 gateway 依赖的下游客户端。用接口注入，测试时换成假对象。
type Deps struct {
	Order      orderv1.OrderServiceClient
	Payment    paymentv1.PaymentServiceClient
	Inventory  inventoryv1.InventoryServiceClient
	OrderIndex orderindexv1.OrderIndexServiceClient
}

// Config 是 HTTP 层的可调参数，零值取默认。
type Config struct {
	Logger      *slog.Logger
	CallTimeout time.Duration
	// RateLimit 为 nil 时不限流。
	RateLimit *RateLimit
	// Degrade 不为 nil 时，New 把非核心接口的开关（merchant_orders、user_orders、stock_query）登记到它上面；
	// 调用方在 New 之后再 Apply 初始值、把它挂到内部端口上。为 nil 时没有开关，接口一直可用。
	Degrade *degrade.Set
}

// server 必须实现规范生成的接口。规范新增或修改了接口而这里没跟上时，编译失败。
var _ oas.ServerInterface = (*server)(nil)

type server struct {
	deps   Deps
	logger *slog.Logger
	// callTimeout 在中间件里转成 context 截止时间，handler 不必各自处理。
	callTimeout time.Duration
	// rl 为 nil 时不限流。
	rl *rateLimiter
	// features 是可以被关掉的接口的开关，按路由查；没有开关的接口查到 nil（nil 开关永远是打开的）。
	features map[routeKey]*degrade.Switch
}

// New 构造路由。返回 http.Handler 而不是 *gin.Engine，不让 gin 泄漏到装配层。
func New(deps Deps, cfg Config) http.Handler {
	// release 模式下 gin 不往标准输出打印路由表等调试信息。
	gin.SetMode(gin.ReleaseMode)

	s := &server{
		deps:        deps,
		logger:      cfg.Logger,
		callTimeout: cfg.CallTimeout,
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	if s.callTimeout <= 0 {
		s.callTimeout = DefaultCallTimeout
	}

	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.NoRoute(func(c *gin.Context) {
		writeError(c, http.StatusNotFound, "NOT_FOUND", "route not found")
	})
	r.NoMethod(func(c *gin.Context) {
		writeError(c, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	})
	// 日志在最外层，才能记录到 recover 之后写出的 500。
	r.Use(s.requestLog, s.recoverPanic)

	// 路由由规范生成的代码注册，路径和方法以 api/openapi/gateway.yaml 为准。
	// 规范里的路径已经带 /api/v1 前缀，所以分组的前缀为空，分组只用来挂中间件。
	// 限流挂在这个分组上，只作用于规范里的接口：没匹配到路由的 404 不经过它，
	// 健康检查挂在外层的 mux 上（cmd/gateway），也不经过它。
	// 顺序：功能开关最先（只读一个原子变量，被关掉的请求不消耗任何配额，理由见 checkFeature）；
	// 按 IP 在识别用户之前（最便宜，挡没有身份的洪水）；按用户和接口在识别用户之后、设置下游时限之前
	// （限流器有自己的时限，不占用下游调用的预算）；热点参数在按用户之后（先按用户限，再看这个值是不是被打爆了）。
	rl, limitIP, limitShared, limitHot := newRateLimiter(cfg.RateLimit)
	s.rl = rl
	var mw []gin.HandlerFunc
	if cfg.Degrade != nil {
		s.features = registerFeatures(cfg.Degrade)
		mw = append(mw, s.checkFeature)
	}
	if limitIP {
		mw = append(mw, s.limitIP)
	}
	mw = append(mw, s.requireUser)
	if limitShared {
		mw = append(mw, s.limitUserAndRoute)
	}
	if limitHot {
		mw = append(mw, s.limitHot)
	}
	mw = append(mw, s.withDeadline)
	api := r.Group("", mw...)
	oas.RegisterHandlersWithOptions(api, s, oas.GinServerOptions{ErrorHandler: bindError})
	return r
}

// requestLog 记录方法、路径模板、状态码和耗时。
// 用路由模板而不是实际路径，避免 ID 造成日志维度爆炸。
func (s *server) requestLog(c *gin.Context) {
	start := time.Now()
	c.Next()

	route := c.FullPath()
	if route == "" {
		route = "unmatched"
	}
	s.logger.LogAttrs(c.Request.Context(), slog.LevelInfo, "http request",
		slog.String("method", c.Request.Method),
		slog.String("route", route),
		slog.Int("status", c.Writer.Status()),
		slog.Duration("duration", time.Since(start)),
	)
}

func (s *server) recoverPanic(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.ErrorContext(c.Request.Context(), "handler panicked", "panic", r)
			if !c.Writer.Written() {
				writeError(c, http.StatusInternalServerError, "INTERNAL", "internal error")
			}
			c.Abort()
		}
	}()
	c.Next()
}

func (s *server) withDeadline(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), s.callTimeout)
	defer cancel()
	c.Request = c.Request.WithContext(ctx)
	c.Next()
}
