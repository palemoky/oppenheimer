// Command gateway 是 gateway 服务的入口：对外 HTTP，对内调用 order、payment、inventory、orderindex 四个服务。
package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	orderindexv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/orderindex/v1"
	paymentv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/grpcx"
	"github.com/palemoky/oppenheimer/pkg/logx"
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
	addr := cmp.Or(os.Getenv("HTTP_ADDR"), ":8080")
	orderAddr := cmp.Or(os.Getenv("ORDER_ADDR"), "127.0.0.1:9081")
	inventoryAddr := cmp.Or(os.Getenv("INVENTORY_ADDR"), "127.0.0.1:9082")
	paymentAddr := cmp.Or(os.Getenv("PAYMENT_ADDR"), "127.0.0.1:9083")
	orderIndexAddr := cmp.Or(os.Getenv("ORDERINDEX_ADDR"), "127.0.0.1:9084")
	// 限流的配置先读：写错了就不启动，不必等连下游。
	rlCfg, err := loadRateLimitConfig(os.LookupEnv)
	if err != nil {
		return err
	}

	// Dial 是惰性的，下游暂时不可达不会阻止 gateway 启动。
	orderConn, err := grpcx.Dial(orderAddr)
	if err != nil {
		return fmt.Errorf("dial order: %w", err)
	}
	defer orderConn.Close()
	inventoryConn, err := grpcx.Dial(inventoryAddr)
	if err != nil {
		return fmt.Errorf("dial inventory: %w", err)
	}
	defer inventoryConn.Close()
	paymentConn, err := grpcx.Dial(paymentAddr)
	if err != nil {
		return fmt.Errorf("dial payment: %w", err)
	}
	defer paymentConn.Close()
	orderIndexConn, err := grpcx.Dial(orderIndexAddr)
	if err != nil {
		return fmt.Errorf("dial orderindex: %w", err)
	}
	defer orderIndexConn.Close()

	// 限流的 Redis 连不上不阻止启动，见 newRateLimitRedis。defer 逆序执行：它在所有组件停止之后关闭。
	rateLimit, rlReporter, closeRateLimit := newRateLimit(ctx, logger, rlCfg)
	defer closeRateLimit()

	api := httpapi.New(httpapi.Deps{
		Order:      orderv1.NewOrderServiceClient(orderConn),
		Payment:    paymentv1.NewPaymentServiceClient(paymentConn),
		Inventory:  inventoryv1.NewInventoryServiceClient(inventoryConn),
		OrderIndex: orderindexv1.NewOrderIndexServiceClient(orderIndexConn),
	}, httpapi.Config{Logger: logger, RateLimit: rateLimit})

	// 健康检查探针很频繁，单独挂在 mux 上，不经过 API 的鉴权和请求日志。
	mux := http.NewServeMux()
	mux.Handle("/healthz", app.HealthHandler(serviceName))
	mux.Handle("/", api)

	httpServer, err := app.ListenHTTP(ctx, "http", addr, mux)
	if err != nil {
		return err
	}
	logger.Info("listening", "addr", httpServer.Addr().String())

	components := []app.Component{}
	if rlReporter != nil {
		// 排在 HTTP 服务前面：停止时逆序，HTTP 服务先停，汇总日志最后输出停止前的那一段。
		components = append(components, rlReporter)
	}
	components = append(components, httpServer)
	return app.Run(ctx, logger, components...)
}
