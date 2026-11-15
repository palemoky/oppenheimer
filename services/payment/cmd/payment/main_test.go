package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/palemoky/oppenheimer/api/eventx"
	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/grpcx"
	"github.com/palemoky/oppenheimer/pkg/testdb"
	"github.com/palemoky/oppenheimer/pkg/testkafka"
)

// repoRoot 从测试的工作目录（包目录）往上找 go.work 所在的仓库根目录。
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.work not found above the test directory")
		}
		dir = parent
	}
}

// envoyRouteTimeout 读 Envoy 配置里名为 listener 的监听的第一条路由的 timeout。
func envoyRouteTimeout(t *testing.T, file, listener string) time.Duration {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "deploy", "envoy", file))
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		StaticResources struct {
			Listeners []struct {
				Name         string `yaml:"name"`
				FilterChains []struct {
					Filters []struct {
						TypedConfig struct {
							RouteConfig struct {
								VirtualHosts []struct {
									Routes []struct {
										Route struct {
											Timeout string `yaml:"timeout"`
										} `yaml:"route"`
									} `yaml:"routes"`
								} `yaml:"virtual_hosts"`
							} `yaml:"route_config"`
						} `yaml:"typed_config"`
					} `yaml:"filters"`
				} `yaml:"filter_chains"`
			} `yaml:"listeners"`
		} `yaml:"static_resources"`
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	for _, l := range cfg.StaticResources.Listeners {
		if l.Name != listener {
			continue
		}
		if len(l.FilterChains) == 0 || len(l.FilterChains[0].Filters) == 0 ||
			len(l.FilterChains[0].Filters[0].TypedConfig.RouteConfig.VirtualHosts) == 0 ||
			len(l.FilterChains[0].Filters[0].TypedConfig.RouteConfig.VirtualHosts[0].Routes) == 0 {
			t.Fatalf("%s: listener %s has no route", file, listener)
		}
		raw := l.FilterChains[0].Filters[0].TypedConfig.RouteConfig.VirtualHosts[0].Routes[0].Route.Timeout
		d, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("%s: listener %s route timeout %q: %v", file, listener, raw, err)
		}
		return d
	}
	t.Fatalf("%s: no listener named %s", file, listener)
	return 0
}

// payment 调 order 的按跳时限和 Envoy 上 to_order 出站路由的 timeout 是同一个数（设计文档 1d 的 5.6）。
func TestOrderHopTimeoutMatchesEnvoy(t *testing.T) {
	t.Parallel()
	if got := envoyRouteTimeout(t, "payment.yaml", "to_order"); got != orderHopBudget {
		t.Fatalf("envoy to_order route timeout = %s, orderHopBudget = %s; keep them equal", got, orderHopBudget)
	}
	// 进程里的时限必须比边车的长：边车先超时，慢下游才会被摘除。
	if orderHopTimeout != orderHopBudget+grpcx.SidecarMargin || grpcx.SidecarMargin <= 0 {
		t.Fatalf("orderHopTimeout = %s, want orderHopBudget %s plus a positive grpcx.SidecarMargin %s",
			orderHopTimeout, orderHopBudget, grpcx.SidecarMargin)
	}
}

// recorder 是记下每条日志消息的 slog.Handler，测试用它等 run 走到某一步、取出监听地址。
type recorder struct {
	mu       sync.Mutex
	msgs     []string
	httpAddr string
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, rec.Message)
	if rec.Message == "listening" {
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == "http_addr" {
				r.httpAddr = a.Value.String()
			}
			return true
		})
	}
	return nil
}

func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }

func (r *recorder) logged(msg string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.msgs, msg)
}

func (r *recorder) addr() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.httpAddr
}

// withDB 把 DSN 里的库名换成 name。DSN 的格式是 user:pass@tcp(host)/db?params，库名在最后一个 / 和 ? 之间。
func withDB(t *testing.T, dsn, name string) string {
	t.Helper()
	i := strings.LastIndex(dsn, "/")
	if i < 0 {
		t.Fatalf("%s has no database part: %q", testdb.DSNEnv, dsn)
	}
	rest := dsn[i+1:]
	params := ""
	if j := strings.Index(rest, "?"); j >= 0 {
		params = rest[j:]
	}
	return dsn[:i+1] + name + params
}

// TestRunStartsWithRedisDown：Redis 连不上时 payment 照样启动（设计文档 1d 的 6.10）。它只为周期任务的
// leader 锁连 Redis，锁是 FailOpen 的；以前启动时 PING 失败就退出（1b 留下的问题）。
// 进程级的测试：用真的 MySQL 和 Kafka（各自隔离的库和 topic）、一个不通的 Redis 地址跑 run，
// 等它就绪，检查内部端口，然后取消 ctx 让它正常退出。
func TestRunStartsWithRedisDown(t *testing.T) {
	_, dbName := testdb.Empty(t)
	brokers := testkafka.New(t)
	// topic 预先建好（带唯一前缀），测试结束时由 testkafka 删掉；run 里的 EnsureTopics 对已存在的不做修改。
	prefix := "test-" + strings.ToLower(rand.Text()) + "."
	for _, name := range []string{eventx.PaymentEvents, eventx.OrderEvents} {
		testkafka.TopicNamed(t, brokers, prefix+name, eventx.Partitions)
		testkafka.TopicNamed(t, brokers, prefix+name+eventx.DLQSuffix, eventx.Partitions)
	}

	t.Setenv("MYSQL_DSN", withDB(t, os.Getenv(testdb.DSNEnv), dbName))
	t.Setenv("KAFKA_BROKERS", strings.Join(brokers, ","))
	t.Setenv("KAFKA_PREFIX", prefix)
	// 端口 1 上没有服务，连接立刻被拒绝。
	t.Setenv("REDIS_ADDR", "127.0.0.1:1")
	t.Setenv("REDIS_PREFIX", prefix)
	t.Setenv("ORDER_ADDR", "127.0.0.1:1")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("GRPC_ADDR", "127.0.0.1:0")
	t.Setenv(degrade.Env, "")
	t.Setenv(app.DrainDelayEnv, "")

	rec := &recorder{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, slog.New(rec)) }()

	// 等就绪：所有组件都启动之后 /readyz 才是 200。
	deadline := time.After(30 * time.Second)
	for {
		if addr := rec.addr(); addr != "" && get(t, addr, "/readyz") == http.StatusOK {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("run exited before becoming ready: %v", err)
		case <-deadline:
			t.Fatal("payment did not become ready within 30s with redis down")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !rec.logged("redis is not reachable at startup, starting degraded") {
		t.Error("no warning about redis being unreachable at startup")
	}
	// 内部端口挂着降级开关的接口；payment 没有开关，列表是空的。
	if code, body := getBody(t, rec.addr(), "/degrade"); code != http.StatusOK || !strings.Contains(body, `"switches":[]`) {
		t.Errorf("GET /degrade = %d %s, want 200 with an empty switch list", code, body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run = %v, want nil after the context is canceled", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not return after the context was canceled")
	}
}

func get(t *testing.T, addr, path string) int {
	t.Helper()
	code, _ := getBody(t, addr, path)
	return code
}

func getBody(t *testing.T, addr, path string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("GET %s timed out", path)
		}
		return 0, err.Error()
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return resp.StatusCode, string(body)
}

// meshRoute 是 Istio VirtualService 里 spec.http 的一条路由（只取这里要核对的字段）。
type meshRoute struct {
	Name  string `yaml:"name"`
	Match []struct {
		SourceLabels map[string]string `yaml:"sourceLabels"`
	} `yaml:"match"`
	Timeout string `yaml:"timeout"`
	Retries *struct {
		Attempts *int `yaml:"attempts"`
	} `yaml:"retries"`
}

// meshRoutes 读 deploy/cluster/mesh/<callee>.yaml（多文档），返回其中唯一一个 VirtualService 的路由列表。
func meshRoutes(t *testing.T, path string) []meshRoute {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read mesh config: %v", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	var routes []meshRoute
	found := 0
	for {
		var doc struct {
			Kind string `yaml:"kind"`
			Spec struct {
				HTTP []meshRoute `yaml:"http"`
			} `yaml:"spec"`
		}
		if err := dec.Decode(&doc); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if doc.Kind == "VirtualService" {
			found++
			routes = doc.Spec.HTTP
		}
	}
	if found != 1 {
		t.Fatalf("%s: found %d VirtualService documents, want exactly 1", path, found)
	}
	return routes
}

// checkMeshRoute 断言一条路由有 timeout（可选地等于 want）并且显式写了 retries.attempts: 0。
func checkMeshRoute(t *testing.T, path string, r meshRoute, want time.Duration) {
	t.Helper()
	d, err := time.ParseDuration(r.Timeout)
	switch {
	case r.Timeout == "":
		t.Errorf("%s: route %q has no timeout; without it a slow downstream is never ejected by the sidecar", path, r.Name)
	case err != nil:
		t.Errorf("%s: route %q timeout %q: %v", path, r.Name, r.Timeout, err)
	case want != 0 && d != want:
		t.Errorf("%s: route %q timeout = %v, hop budget in Go = %v; they must match", path, r.Name, d, want)
	}
	if r.Retries == nil || r.Retries.Attempts == nil {
		t.Errorf("%s: route %q has no retries.attempts; Istio retries unavailable errors twice by default, "+
			"and a synchronous call chain must not retry (a non-idempotent write would run more than once)", path, r.Name)
	} else if *r.Retries.Attempts != 0 {
		t.Errorf("%s: route %q retries.attempts = %d, want 0; a synchronous call chain must not retry "+
			"(a non-idempotent write would run more than once)", path, r.Name, *r.Retries.Attempts)
	}
}

// meshEdgeRoute 找出 sourceLabels 的 app.kubernetes.io/name 等于 caller 的那条路由。
func meshEdgeRoute(t *testing.T, path string, routes []meshRoute, caller string) (meshRoute, bool) {
	t.Helper()
	for _, r := range routes {
		for _, m := range r.Match {
			if m.SourceLabels["app.kubernetes.io/name"] == caller {
				return r, true
			}
		}
	}
	t.Errorf("%s: no route matches sourceLabels app.kubernetes.io/name=%s; %s would fall through to the catch-all route "+
		"(or have no timeout at all without a VirtualService), so a slow downstream is never ejected", path, caller, caller)
	return meshRoute{}, false
}

// 集群上 order 的 VirtualService 里，payment 这个调用方有自己的路由：timeout 等于 orderHopBudget，且不重试。
func TestOrderHopTimeoutMatchesMesh(t *testing.T) {
	t.Parallel()
	path := filepath.Join(repoRoot(t), "deploy", "cluster", "mesh", "order.yaml")
	routes := meshRoutes(t, path)
	if r, ok := meshEdgeRoute(t, path, routes, "payment"); ok {
		checkMeshRoute(t, path, r, orderHopBudget)
	}
}
