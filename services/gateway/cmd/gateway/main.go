// Command gateway 是 gateway 服务的入口：对外 HTTP，对内调用 order、payment、inventory、orderindex 四个服务。
package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	orderindexv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/orderindex/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/grpcx"
	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi"
)

const serviceName = "gateway"

func main() {
	logger := logx.New(serviceName, os.Stdout, logx.LevelFromEnv())
	slog.SetDefault(logger)

	if err := run(context.Background(), logger); err != nil {
		logger.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	// 链路和指标（设计文档 1d 第 8 节）。最先装配、最后关闭：其他组件停止时产生的 span 也要发出去。
	// 没设 OTEL_EXPORTER_OTLP_ENDPOINT 时不导出，只让日志带上 trace id。
	shutdownTelemetry, err := otelx.Setup(ctx, serviceName, logger)
	if err != nil {
		return err
	}
	defer otelx.Shutdown(ctx, shutdownTelemetry, logger)

	addr := cmp.Or(os.Getenv("HTTP_ADDR"), ":8080")
	adminAddr := cmp.Or(os.Getenv("ADMIN_ADDR"), defaultAdminAddr)
	orderAddr := cmp.Or(os.Getenv("ORDER_ADDR"), "127.0.0.1:9081")
	inventoryAddr := cmp.Or(os.Getenv("INVENTORY_ADDR"), "127.0.0.1:9082")
	paymentAddr := cmp.Or(os.Getenv("PAYMENT_ADDR"), "127.0.0.1:9083")
	orderIndexAddr := cmp.Or(os.Getenv("ORDERINDEX_ADDR"), "127.0.0.1:9084")
	// 限流的配置先读：写错了就不启动，不必等连下游。
	rlCfg, err := loadRateLimitConfig(os.LookupEnv)
	if err != nil {
		return err
	}
	drainDelay, err := app.DrainDelayFromEnv()
	if err != nil {
		return err
	}

	// Dial 是惰性的，下游暂时不可达不会阻止 gateway 启动。
	// 每条下游连接都加单跳时限（设计文档 1d 5.6），和 Envoy 边车出站路由的 timeout 是同一个数。
	hop := grpcx.WithHopTimeout(httpapi.DownstreamHopTimeout)
	orderConn, err := grpcx.Dial(orderAddr, hop)
	if err != nil {
		return fmt.Errorf("dial order: %w", err)
	}
	defer orderConn.Close()
	inventoryConn, err := grpcx.Dial(inventoryAddr, hop)
	if err != nil {
		return fmt.Errorf("dial inventory: %w", err)
	}
	defer inventoryConn.Close()
	paymentConn, err := grpcx.Dial(paymentAddr, hop)
	if err != nil {
		return fmt.Errorf("dial payment: %w", err)
	}
	defer paymentConn.Close()
	orderIndexConn, err := grpcx.Dial(orderIndexAddr, hop)
	if err != nil {
		return fmt.Errorf("dial orderindex: %w", err)
	}
	defer orderIndexConn.Close()

	// 限流的 Redis 连不上不阻止启动，见 newRateLimitRedis。defer 逆序执行：它在所有组件停止之后关闭。
	rateLimit, rlReporter, closeRateLimit := newRateLimit(ctx, logger, rlCfg)
	defer closeRateLimit()

	// 降级开关（设计文档 1d 的 6.9）：非核心接口的开关由 httpapi.New 登记，登记完之后再读 DEGRADE 里的初始值。
	switches := degrade.New(serviceName, logger)
	api := httpapi.New(httpapi.Deps{
		Order:      orderv1.NewOrderServiceClient(orderConn),
		Payment:    paymentv1.NewPaymentServiceClient(paymentConn),
		Inventory:  inventoryv1.NewInventoryServiceClient(inventoryConn),
		OrderIndex: orderindexv1.NewOrderIndexServiceClient(orderIndexConn),
	}, httpapi.Config{Logger: logger, RateLimit: rateLimit, Degrade: switches})
	// 名字写错就拒绝启动：被悄悄忽略的话，人以为关了其实没关。
	if err := switches.Apply(os.Getenv(degrade.Env)); err != nil {
		return fmt.Errorf("apply %s: %w", degrade.Env, err)
	}

	readiness := &app.Readiness{}
	httpServer, err := app.ListenHTTP(ctx, "http", addr, publicMux(api, readiness))
	if err != nil {
		return err
	}
	adminServer, err := app.ListenHTTP(ctx, "admin", adminAddr, adminMux(readiness, switches))
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, "listening", "addr", httpServer.Addr().String(), "admin_addr", adminServer.Addr().String())

	// 停止顺序与列出的顺序相反：
	//  0. 收到退出信号后先摘流（设计文档 1d 的 6.11）：/readyz 变成 503，继续正常服务 DRAIN_DELAY 这么久，
	//     让负载均衡把流量挪走，然后才开始下面的停止。本机默认 0。
	//  1. 对外的 HTTP：不再接新请求，等在途的请求做完。
	//  2. 限流的汇总日志：在 HTTP 停下之后才停，把停止前最后一段的拒绝次数也记下来。
	//  3. 内部端口最后停：停止过程中探测和 /degrade 一直可用。
	// 下游连接和限流的 Redis 由 defer 在 RunWith 返回后关闭，此时已没有组件在用。
	components := []app.Component{adminServer}
	if rlReporter != nil {
		components = append(components, rlReporter)
	}
	components = append(components, httpServer)
	return app.RunWith(ctx, logger, app.Options{Readiness: readiness, DrainDelay: drainDelay}, components...)
}
