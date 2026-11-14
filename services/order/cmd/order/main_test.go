package main

import (
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
