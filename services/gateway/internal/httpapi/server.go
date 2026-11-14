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
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi/oas"
)

const (
	// DefaultCallTimeout 是单个请求内所有下游调用的总时限。
	DefaultCallTimeout = 5 * time.Second
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
	// 顺序：按 IP 在识别用户之前（最便宜，挡没有身份的洪水）；按用户和接口在识别用户之后、设置下游时限之前
	// （限流器有自己的时限，不占用下游调用的预算）。
	rl, limitIP, limitShared := newRateLimiter(cfg.RateLimit)
	s.rl = rl
	var mw []gin.HandlerFunc
	if limitIP {
		mw = append(mw, s.limitIP)
	}
	mw = append(mw, s.requireUser)
	if limitShared {
		mw = append(mw, s.limitUserAndRoute)
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
