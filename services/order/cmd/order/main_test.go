package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/palemoky/oppenheimer/services/order/internal/service"

	"github.com/palemoky/oppenheimer/pkg/grpcx"
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

// order 调 inventory 的按跳时限和 Envoy 上 to_inventory 出站路由的 timeout 是同一个数（设计文档 1d 的 5.6）。
func TestInventoryHopTimeoutMatchesEnvoy(t *testing.T) {
	t.Parallel()
	if got := envoyRouteTimeout(t, "order.yaml", "to_inventory"); got != inventoryHopBudget {
		t.Fatalf("envoy to_inventory route timeout = %s, inventoryHopBudget = %s; keep them equal", got, inventoryHopBudget)
	}
	// 进程里的时限必须比边车的长：边车先超时，慢下游才会被摘除。
	if inventoryHopTimeout != inventoryHopBudget+grpcx.SidecarMargin || grpcx.SidecarMargin <= 0 {
		t.Fatalf("inventoryHopTimeout = %s, want inventoryHopBudget %s plus a positive grpcx.SidecarMargin %s",
			inventoryHopTimeout, inventoryHopBudget, grpcx.SidecarMargin)
	}
}

// READ_MAX_CONCURRENCY：默认值、合法值，小于 1 或不小于连接池上限时拒绝启动。
func TestReadMaxConcurrencyConfig(t *testing.T) {
	for _, c := range []struct {
		name, readMax, maxOpen string
		want                   int // 0 表示应当拒绝
	}{
		{"default", "", "", service.DefaultReadMaxConcurrency},
		{"explicit", "10", "", 10},
		{"one below the pool", "24", "25", 24},
		{"smaller pool", "3", "4", 3},
		{"zero", "0", "", 0},
		{"negative", "-1", "", 0},
		{"equal to the pool", "25", "25", 0},
		{"above the pool", "40", "25", 0},
		{"default above a small pool", "", "8", 0},
		{"not a number", "many", "", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("MYSQL_DSN", "")
			t.Setenv("READ_MAX_CONCURRENCY", c.readMax)
			t.Setenv("MYSQL_MAX_OPEN_CONNS", c.maxOpen)
			cfg, err := loadConfig()
			if c.want == 0 {
				if err == nil {
					t.Fatalf("READ_MAX_CONCURRENCY=%q MYSQL_MAX_OPEN_CONNS=%q accepted (%d), want error",
						c.readMax, c.maxOpen, cfg.readMaxConcurrency)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.readMaxConcurrency != c.want {
				t.Fatalf("readMaxConcurrency = %d, want %d", cfg.readMaxConcurrency, c.want)
			}
		})
	}
}

// ARCHIVE_MAX_CONCURRENCY：默认值、合法值，小于 1 或超过冷库连接池时拒绝启动。
func TestArchiveMaxConcurrencyConfig(t *testing.T) {
	for _, c := range []struct {
		value string
		want  int // 0 表示应当拒绝
	}{
		{"", service.DefaultArchiveMaxConcurrency},
		{"1", 1},
		{strconv.Itoa(archiveMaxOpenConns), archiveMaxOpenConns},
		{"0", 0},
		{strconv.Itoa(archiveMaxOpenConns + 1), 0},
	} {
		t.Run("value="+c.value, func(t *testing.T) {
			t.Setenv("MYSQL_DSN", "")
			t.Setenv("ARCHIVE_MAX_CONCURRENCY", c.value)
			cfg, err := loadConfig()
			if c.want == 0 {
				if err == nil {
					t.Fatalf("ARCHIVE_MAX_CONCURRENCY=%q accepted, want error", c.value)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.archiveMaxConcurrency != c.want {
				t.Fatalf("archiveMaxConcurrency = %d, want %d", cfg.archiveMaxConcurrency, c.want)
			}
		})
	}
}

// ORDER_READ_MAX_LAG：window 模式下必须小于 ORDER_READ_PIN_TTL，否则拒绝启动；其余模式不受这条限制。
func TestReadMaxLagConfig(t *testing.T) {
	for _, c := range []struct {
		name, mode, maxLag, pinTTL string
		want                       time.Duration // 0 表示应当拒绝
	}{
		{"default", "", "", "", service.DefaultReplicaMaxLag},
		{"window default", "window", "", "", service.DefaultReplicaMaxLag},
		{"window below pin", "window", "1500ms", "3s", 1500 * time.Millisecond},
		// 最大延迟加一个探测间隔（1 秒）不能达到 pin 的时长。
		{"window plus probe interval reaches pin", "window", "2s", "3s", 0},
		{"window equal to pin", "window", "3s", "3s", 0},
		{"window above pin", "window", "5s", "3s", 0},
		{"window default lag with a short pin", "window", "", "500ms", 0},
		{"gtid above pin", "gtid", "5s", "3s", 5 * time.Second},
		{"not positive", "", "0s", "", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("MYSQL_DSN", "")
			t.Setenv("ORDER_READ_MODE", c.mode)
			t.Setenv("MYSQL_REPLICA_DSNS", "root:x@tcp(127.0.0.1:1)/a,root:x@tcp(127.0.0.1:1)/b")
			t.Setenv("ORDER_READ_MAX_LAG", c.maxLag)
			t.Setenv("ORDER_READ_PIN_TTL", c.pinTTL)
			cfg, err := loadConfig()
			if c.want == 0 {
				if err == nil {
					t.Fatalf("ORDER_READ_MAX_LAG=%q ORDER_READ_PIN_TTL=%q accepted in mode %q, want error", c.maxLag, c.pinTTL, c.mode)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.readMaxLag != c.want {
				t.Fatalf("readMaxLag = %s, want %s", cfg.readMaxLag, c.want)
			}
		})
	}
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

// 集群上 inventory 的 VirtualService 里，order 这个调用方有自己的路由：timeout 等于 inventoryHopBudget，且不重试。
func TestInventoryHopTimeoutMatchesMesh(t *testing.T) {
	t.Parallel()
	path := filepath.Join(repoRoot(t), "deploy", "cluster", "mesh", "inventory.yaml")
	routes := meshRoutes(t, path)
	if r, ok := meshEdgeRoute(t, path, routes, "order"); ok {
		checkMeshRoute(t, path, r, inventoryHopBudget)
	}
}
