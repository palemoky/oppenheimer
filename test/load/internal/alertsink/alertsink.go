// Package alertsink 是 Alertmanager 的 webhook 接收端：把每条告警打成一行日志、按状态和级别计数，
// 并记下最近一次收到 Watchdog 的时间。设计见 3c 的 3.1 节。
package alertsink

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// MaxBodyBytes 是一次 webhook 请求体的上限。
const MaxBodyBytes = 1 << 20

// ShutdownTimeout 是停止时等在途请求做完的最长时间。
const ShutdownTimeout = 10 * time.Second

// webhook 是 Alertmanager webhook（version "4"）里用到的字段，其余字段忽略。
type webhook struct {
	Receiver string  `json:"receiver"`
	Status   string  `json:"status"`
	GroupKey string  `json:"groupKey"`
	Alerts   []alert `json:"alerts"`
}

type alert struct {
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    string            `json:"startsAt"`
	EndsAt      string            `json:"endsAt"`
	Fingerprint string            `json:"fingerprint"`
}

// Handler 处理 webhook 和健康检查。
type Handler struct {
	logger *slog.Logger
	now    func() time.Time
}

// NewHandler 返回处理器。now 为 nil 时用 time.Now，测试里注入固定的时钟。
func NewHandler(logger *slog.Logger, now func() time.Time) *Handler {
	if now == nil {
		now = time.Now
	}
	return &Handler{logger: logger, now: now}
}

// Mux 返回挂好路由的 ServeMux。
func (h *Handler) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /alerts", h.alerts)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func (h *Handler) alerts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var wh webhook
	if err := json.UnmarshalRead(http.MaxBytesReader(w, r.Body, MaxBodyBytes), &wh); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		h.logger.WarnContext(ctx, "invalid webhook body", "error", err)
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	for _, a := range wh.Alerts {
		h.record(ctx, &wh, &a)
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) record(ctx context.Context, wh *webhook, a *alert) {
	name, severity := a.Labels["alertname"], a.Labels["severity"]
	alertsTotal.Add(ctx, 1, alertOptions(a.Status, severity))
	if name == "Watchdog" && a.Status == "firing" {
		watchdogLastReceived.Record(ctx, h.now().Unix())
	}
	h.logger.InfoContext(ctx, "alert received",
		"alertname", name,
		"status", a.Status,
		"severity", severity,
		"fingerprint", a.Fingerprint,
		"starts_at", a.StartsAt,
		"ends_at", a.EndsAt,
		"group_key", wh.GroupKey,
		"receiver", wh.Receiver,
		"labels", a.Labels,
		"summary", a.Annotations["summary"],
	)
}

// Config 是 Run 的参数。
type Config struct {
	Listen string
	Logger *slog.Logger
	Now    func() time.Time
}

// Run 监听并服务，直到 ctx 结束，然后优雅退出。
func Run(ctx context.Context, cfg Config) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Listen, err)
	}
	return Serve(ctx, ln, cfg)
}

// Serve 在已有的 listener 上服务，测试用。
func Serve(ctx context.Context, ln net.Listener, cfg Config) error {
	srv := &http.Server{
		Handler:           NewHandler(cfg.Logger, cfg.Now).Mux(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	cfg.Logger.InfoContext(ctx, "alertsink listening", "addr", ln.Addr().String())

	select {
	case err := <-errc:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}
	// ctx 已经结束，关闭要用新的 ctx。
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
