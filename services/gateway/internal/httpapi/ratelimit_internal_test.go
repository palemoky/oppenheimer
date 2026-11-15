package httpapi

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.yaml.in/yaml/v3"

	"github.com/palemoky/oppenheimer/pkg/ratelimit"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi/oas"

	"github.com/palemoky/oppenheimer/pkg/grpcx"
)

const (
	specPath         = "../../../../api/openapi/gateway.yaml"
	envoyGatewayPath = "../../../../deploy/envoy/gateway.yaml"
)

// specOperation 是规范里的一个 operation。
type specOperation struct {
	method, path, id string
	// responses 是状态码 → 引用的 components.responses（内联定义的为空）。
	responses map[string]string
}

// loadSpec 从规范文件读出所有 operation 和 components.responses。operation 的清单以规范为准，测试里不手抄。
func loadSpec(t *testing.T) (ops []specOperation, responses map[string]yaml.Node) {
	t.Helper()
	raw, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read spec: %v", err)
	}
	var doc struct {
		Paths      map[string]map[string]yaml.Node `yaml:"paths"`
		Components struct {
			Responses map[string]yaml.Node `yaml:"responses"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse spec: %v", err)
	}
	methods := map[string]string{
		"get": http.MethodGet, "post": http.MethodPost, "put": http.MethodPut,
		"patch": http.MethodPatch, "delete": http.MethodDelete, "head": http.MethodHead, "options": http.MethodOptions,
	}
	for path, item := range doc.Paths {
		for key, node := range item {
			method, ok := methods[key]
			if !ok {
				continue // parameters、summary 等不是 operation
			}
			var op struct {
				OperationID string `yaml:"operationId"`
				Responses   map[string]struct {
					Ref string `yaml:"$ref"`
				} `yaml:"responses"`
			}
			if err := node.Decode(&op); err != nil {
				t.Fatalf("decode %s %s: %v", key, path, err)
			}
			so := specOperation{method: method, path: path, id: op.OperationID, responses: map[string]string{}}
			for code, r := range op.Responses {
				so.responses[code] = strings.TrimPrefix(r.Ref, "#/components/responses/")
			}
			ops = append(ops, so)
		}
	}
	if len(ops) == 0 {
		t.Fatal("no operations found in spec")
	}
	return ops, doc.Components.Responses
}

var pathParam = regexp.MustCompile(`\{([^}]+)\}`)

// ginPath 把规范的路径模板（{order_id}）换成 gin 的写法（:order_id）。
func ginPath(p string) string { return pathParam.ReplaceAllString(p, ":$1") }

// TestOperationsMatchSpec 保证 operations 表和规范、生成代码注册的路由一一对应：
// 规范新增了接口而表没跟上时，中间件认不出它，限流对它不起作用，这里失败。
func TestOperationsMatchSpec(t *testing.T) {
	specOps, _ := loadSpec(t)

	table := map[routeKey]string{}
	for _, op := range operations {
		k := routeKey{op.method, op.path}
		if _, dup := table[k]; dup {
			t.Errorf("duplicate route %v in operations", k)
		}
		table[k] = op.id
	}
	for _, so := range specOps {
		k := routeKey{so.method, ginPath(so.path)}
		if got, ok := table[k]; !ok {
			t.Errorf("spec operation %s (%s %s) is missing from operations", so.id, so.method, so.path)
		} else if got != so.id {
			t.Errorf("operations maps %v to %q, spec says %q", k, got, so.id)
		}
	}
	if len(table) != len(specOps) {
		t.Errorf("operations has %d entries, spec has %d operations", len(table), len(specOps))
	}

	// 生成代码实际注册的路由（c.FullPath() 返回的就是这些模板）也都要在表里。
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	oas.RegisterHandlers(r, nil)
	routes := r.Routes()
	if len(routes) != len(table) {
		t.Errorf("generated code registers %d routes, operations has %d", len(routes), len(table))
	}
	for _, rt := range routes {
		if _, ok := table[routeKey{rt.Method, rt.Path}]; !ok {
			t.Errorf("generated route %s %s is missing from operations", rt.Method, rt.Path)
		}
	}
}

// TestHotParamsMatchSpec 保证 hotParams 表里的接口都在规范里，参数名确实出现在这个接口的路径模板中
// （规范把 {sku_id} 改了名而表没跟上时，c.Param 拿到空字符串，热点限流悄悄失效），默认值合法。
func TestHotParamsMatchSpec(t *testing.T) {
	specOps, _ := loadSpec(t)
	byID := map[string]specOperation{}
	for _, so := range specOps {
		byID[so.id] = so
	}
	if len(hotParams) == 0 {
		t.Fatal("hotParams is empty")
	}
	for id, hp := range hotParams {
		so, ok := byID[id]
		if !ok {
			t.Errorf("hot operation %s is not in the spec", id)
			continue
		}
		if !strings.Contains(so.path, "{"+hp.name+"}") {
			t.Errorf("hot operation %s: parameter %q is not in its path template %s", id, hp.name, so.path)
		}
		// 中间件按 operations 表里的 gin 模板认路由，c.Param 用的是那里的名字，也核对一遍。
		for _, op := range operations {
			if op.id == id && !strings.Contains(op.path, ":"+hp.name) {
				t.Errorf("hot operation %s: parameter %q is not in the gin route %s", id, hp.name, op.path)
			}
		}
		if !hp.def.Enabled() || hp.def.Validate() != nil {
			t.Errorf("default hot limit of %s is %+v, want a valid enabled limit", id, hp.def)
		}
	}
}

// TestHopTimeoutMatchesEnvoy 核对三个数的关系：Envoy 边车每条出站路由的 timeout 等于 DownstreamHopBudget；
// 进程里的 DownstreamHopTimeout 比它长 grpcx.SidecarMargin（边车必须先超时，慢下游才会被摘除）；
// 两者都严格短于 gateway 的总时限。改了一处忘了别处时失败。
func TestHopTimeoutMatchesEnvoy(t *testing.T) {
	raw, err := os.ReadFile(envoyGatewayPath)
	if err != nil {
		t.Fatalf("read envoy config: %v", err)
	}
	var doc struct {
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
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse envoy config: %v", err)
	}
	if DownstreamHopTimeout != DownstreamHopBudget+grpcx.SidecarMargin || grpcx.SidecarMargin <= 0 {
		t.Errorf("DownstreamHopTimeout %v must be DownstreamHopBudget %v plus a positive grpcx.SidecarMargin %v: "+
			"the sidecar has to time out first or a slow downstream is never ejected", DownstreamHopTimeout, DownstreamHopBudget, grpcx.SidecarMargin)
	}
	if DownstreamHopTimeout >= DefaultCallTimeout {
		t.Errorf("DownstreamHopTimeout %v must be shorter than DefaultCallTimeout %v", DownstreamHopTimeout, DefaultCallTimeout)
	}
	// gateway 的边车只有出站监听，四个下游各一个。
	want := map[string]bool{"to_order": true, "to_inventory": true, "to_payment": true, "to_orderindex": true}
	for _, l := range doc.StaticResources.Listeners {
		if !want[l.Name] {
			t.Errorf("unexpected listener %q; update this test if the gateway got a new downstream", l.Name)
			continue
		}
		delete(want, l.Name)
		if len(l.FilterChains) == 0 || len(l.FilterChains[0].Filters) == 0 ||
			len(l.FilterChains[0].Filters[0].TypedConfig.RouteConfig.VirtualHosts) == 0 ||
			len(l.FilterChains[0].Filters[0].TypedConfig.RouteConfig.VirtualHosts[0].Routes) == 0 {
			t.Errorf("listener %s: no route found", l.Name)
			continue
		}
		raw := l.FilterChains[0].Filters[0].TypedConfig.RouteConfig.VirtualHosts[0].Routes[0].Route.Timeout
		got, err := time.ParseDuration(raw)
		if err != nil {
			t.Errorf("listener %s: route timeout %q: %v", l.Name, raw, err)
			continue
		}
		if got != DownstreamHopBudget {
			t.Errorf("listener %s: route timeout = %v, DownstreamHopBudget = %v; they must match", l.Name, got, DownstreamHopBudget)
		}
	}
	for name := range want {
		t.Errorf("outbound listener %s is missing from the envoy config", name)
	}
}

// TestEveryOperationHasDefaultUserLimit：新增接口忘了配按用户的默认限额时失败。
func TestEveryOperationHasDefaultUserLimit(t *testing.T) {
	specOps, _ := loadSpec(t)
	ids := map[string]bool{}
	for _, so := range specOps {
		ids[so.id] = true
		l, ok := defaultUserLimits[so.id]
		if !ok {
			t.Errorf("operation %s has no default per-user limit", so.id)
			continue
		}
		if !l.Enabled() || l.Validate() != nil {
			t.Errorf("default per-user limit of %s is %+v, want a valid enabled limit", so.id, l)
		}
	}
	for id := range defaultUserLimits {
		if !ids[id] {
			t.Errorf("default per-user limit for %s, which is not in the spec", id)
		}
	}
}

// TestEveryOperationDocuments429 保证规范里每个接口都声明了 429，并且 TooManyRequests 带 Retry-After 头。
func TestEveryOperationDocuments429(t *testing.T) {
	specOps, responses := loadSpec(t)
	for _, so := range specOps {
		if got := so.responses["429"]; got != "TooManyRequests" {
			t.Errorf("%s: 429 response = %q, want a reference to TooManyRequests", so.id, got)
		}
	}
	node, ok := responses["TooManyRequests"]
	if !ok {
		t.Fatal("components.responses.TooManyRequests is missing")
	}
	var resp struct {
		Description string `yaml:"description"`
		Headers     map[string]struct {
			Ref string `yaml:"$ref"`
		} `yaml:"headers"`
	}
	if err := node.Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Headers["Retry-After"].Ref == "" {
		t.Error("TooManyRequests has no Retry-After header")
	}
	for _, code := range []string{codeRateLimited, "BACKLOGGED"} {
		if !strings.Contains(resp.Description, code) {
			t.Errorf("TooManyRequests does not describe %s", code)
		}
	}
	for _, dim := range dimensionNames {
		if !strings.Contains(resp.Description, "`"+dim+"`") {
			t.Errorf("TooManyRequests does not describe dimension %s", dim)
		}
	}
}

func TestParseOperationLimits(t *testing.T) {
	base := DefaultUserLimits()
	t.Run("empty keeps base", func(t *testing.T) {
		got, err := ParseOperationLimits("", base)
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range operations {
			if got[op.id] != base[op.id] {
				t.Errorf("%s = %v, want %v", op.id, got[op.id], base[op.id])
			}
		}
	})
	t.Run("override and off", func(t *testing.T) {
		got, err := ParseOperationLimits(" createOrder = 1/2 , getOrder=off,", base)
		if err != nil {
			t.Fatal(err)
		}
		if got["createOrder"] != (ratelimit.Limit{Rate: 1, Burst: 2}) {
			t.Errorf("createOrder = %v", got["createOrder"])
		}
		if got["getOrder"].Enabled() {
			t.Errorf("getOrder = %v, want off", got["getOrder"])
		}
		if got["payOrder"] != base["payOrder"] {
			t.Errorf("payOrder = %v, want default", got["payOrder"])
		}
	})
	t.Run("all off", func(t *testing.T) {
		got, err := ParseOperationLimits("off", base)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(operations) {
			t.Fatalf("len = %d", len(got))
		}
		for id, l := range got {
			if l.Enabled() {
				t.Errorf("%s = %v, want off", id, l)
			}
		}
	})
	t.Run("nil base", func(t *testing.T) {
		got, err := ParseOperationLimits("getStock=100/200", nil)
		if err != nil {
			t.Fatal(err)
		}
		for id, l := range got {
			if l.Enabled() != (id == "getStock") {
				t.Errorf("%s = %v", id, l)
			}
		}
	})
	for _, bad := range []string{
		"nope=1/2",                        // 规范里没有
		"createOrder",                     // 没有 =
		"=1/2",                            // 没有名字
		"createOrder=fast",                // 值的格式
		"createOrder=0/1",                 // 零（要写 off）
		"createOrder=1/2,createOrder=off", // 重复
		"/api/v1/orders=1/2",              // 用路径而不是 operationId
	} {
		if _, err := ParseOperationLimits(bad, base); err == nil {
			t.Errorf("ParseOperationLimits(%q) succeeded, want error", bad)
		} else if !strings.Contains(err.Error(), "\"") {
			t.Errorf("error %q does not name the bad item", err)
		}
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	for _, tt := range []struct {
		in   time.Duration
		want int64
	}{
		{0, 1},
		{time.Microsecond, 1},
		{time.Second, 1},
		{time.Second + time.Nanosecond, 2},
		{2500 * time.Millisecond, 3},
	} {
		if got := retryAfterSeconds(tt.in); got != tt.want {
			t.Errorf("retryAfterSeconds(%v) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestClientAddr(t *testing.T) {
	mustProxies := func(s string) []netip.Prefix {
		ps, err := ParseTrustedProxies(s)
		if err != nil {
			t.Fatal(err)
		}
		return ps
	}
	lb := mustProxies("10.0.0.0/8, 192.0.2.10, 2001:db8:ffff::/48")

	tests := []struct {
		name    string
		remote  string
		xff     []string
		trusted []netip.Prefix
		want    string
	}{
		{"no proxies, no header", "203.0.113.5:4321", nil, nil, "203.0.113.5"},
		{"no proxies, forged header ignored", "203.0.113.5:4321", []string{"1.1.1.1"}, nil, "203.0.113.5"},
		{"not from proxy, header ignored", "203.0.113.5:4321", []string{"1.1.1.1"}, lb, "203.0.113.5"},
		{"one proxy", "10.0.0.1:80", []string{"198.51.100.7"}, lb, "198.51.100.7"},
		{"one proxy, forged left part ignored", "10.0.0.1:80", []string{"6.6.6.6, 198.51.100.7"}, lb, "198.51.100.7"},
		{"two proxies", "10.0.0.1:80", []string{"6.6.6.6, 198.51.100.7, 10.1.2.3"}, lb, "198.51.100.7"},
		{"rightmost is proxy (single IP entry)", "10.0.0.1:80", []string{"198.51.100.7,192.0.2.10"}, lb, "198.51.100.7"},
		{"all trusted", "10.0.0.1:80", []string{"10.0.0.2, 10.0.0.3"}, lb, "10.0.0.1"},
		{"header missing", "10.0.0.1:80", nil, lb, "10.0.0.1"},
		{"empty header", "10.0.0.1:80", []string{""}, lb, "10.0.0.1"},
		{"garbage", "10.0.0.1:80", []string{"not-an-ip"}, lb, "10.0.0.1"},
		{"garbage behind a proxy", "10.0.0.1:80", []string{"unknown, 10.0.0.2"}, lb, "10.0.0.1"},
		{"garbage left of the client is never read", "10.0.0.1:80", []string{"<script>, 198.51.100.7"}, lb, "198.51.100.7"},
		{"entry with port", "10.0.0.1:80", []string{"198.51.100.7:5555"}, lb, "198.51.100.7"},
		{"multiple headers are concatenated in order", "10.0.0.1:80", []string{"6.6.6.6, 198.51.100.7", "10.0.0.9"}, lb, "198.51.100.7"},
		{"multiple headers, client in last", "10.0.0.1:80", []string{"6.6.6.6", "198.51.100.7, 10.0.0.9"}, lb, "198.51.100.7"},
		{"ipv6 remote", "[2001:db8::1]:443", nil, nil, "2001:db8::1"},
		{"ipv6 remote with zone", "[fe80::1%en0]:443", nil, nil, "fe80::1"},
		{"ipv6 proxy and client", "[2001:db8:ffff::1]:443", []string{"[2001:db8:1::5]:1234, 2001:db8:ffff::2"}, lb, "2001:db8:1::5"},
		{"ipv6 bracketed without port", "10.0.0.1:80", []string{"[2001:db8:1::5]"}, lb, "2001:db8:1::5"},
		{"ipv4-mapped remote", "[::ffff:203.0.113.5]:80", nil, nil, "203.0.113.5"},
		{"ipv4-mapped remote matches ipv4 proxy", "[::ffff:10.0.0.1]:80", []string{"198.51.100.7"}, lb, "198.51.100.7"},
		{"ipv4-mapped entry", "10.0.0.1:80", []string{"::ffff:198.51.100.7"}, lb, "198.51.100.7"},
		{"remote without port", "203.0.113.5", nil, nil, "203.0.113.5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := clientAddr(tt.remote, tt.xff, tt.trusted)
			if !ok {
				t.Fatalf("clientAddr(%q) not ok", tt.remote)
			}
			if got.String() != tt.want {
				t.Errorf("clientAddr = %s, want %s", got, tt.want)
			}
		})
	}

	if _, ok := clientAddr("@", nil, nil); ok {
		t.Error("unparsable remote address should not be ok")
	}
}

func TestClientKey(t *testing.T) {
	tests := []struct {
		remote, want string
	}{
		{"203.0.113.5:1", "203.0.113.5"},
		// IPv4 映射的 IPv6 和 IPv4 是同一个键。
		{"[::ffff:203.0.113.5]:1", "203.0.113.5"},
		// IPv6 按 /64：同一个 /64 里的地址共用一个桶，不同的 /64 不共用。
		{"[2001:db8:1:2::1]:1", "2001:db8:1:2::/64"},
		{"[2001:db8:1:2:ffff:ffff:ffff:ffff]:1", "2001:db8:1:2::/64"},
		{"[2001:db8:1:3::1]:1", "2001:db8:1:3::/64"},
		// 解析不了的远端地址原样当键，仍然受限。
		{"@", "@"},
	}
	for _, tt := range tests {
		r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
		if err != nil {
			t.Fatal(err)
		}
		r.RemoteAddr = tt.remote
		if got := clientKey(r, nil); got != tt.want {
			t.Errorf("clientKey(%q) = %q, want %q", tt.remote, got, tt.want)
		}
	}
}

func TestParseTrustedProxies(t *testing.T) {
	got, err := ParseTrustedProxies(" 10.1.2.3/8 ,192.0.2.1, ::ffff:192.0.2.2, 2001:db8::/32, fe80::1%en0,")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10.0.0.0/8", "192.0.2.1/32", "192.0.2.2/32", "2001:db8::/32", "fe80::1/128"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i].String() != want[i] {
			t.Errorf("[%d] = %s, want %s", i, got[i], want[i])
		}
	}
	if got, err := ParseTrustedProxies(""); err != nil || len(got) != 0 {
		t.Errorf("empty: %v, %v", got, err)
	}
	for _, bad := range []string{"10.0.0.0/33", "not-an-ip", "::ffff:10.0.0.0/104", "10.0.0.1:80"} {
		if _, err := ParseTrustedProxies(bad); err == nil {
			t.Errorf("ParseTrustedProxies(%q) succeeded, want error", bad)
		}
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

// TestHopTimeoutMatchesMesh：gateway 到四个下游的 VirtualService 路由都存在，timeout 等于 DownstreamHopBudget、不重试；
// 并且每个文件里的每条路由都有 timeout 和 retries.attempts: 0，没有 match 的兜底路由在最后
// （Istio 取第一条匹配的，兜底放前面会吃掉后面所有的路由）。
func TestHopTimeoutMatchesMesh(t *testing.T) {
	for _, callee := range []string{"order", "inventory", "payment", "orderindex"} {
		t.Run(callee, func(t *testing.T) {
			path := "../../../../deploy/cluster/mesh/" + callee + ".yaml"
			routes := meshRoutes(t, path)
			if len(routes) == 0 {
				t.Fatalf("%s: VirtualService has no http routes", path)
			}
			if r, ok := meshEdgeRoute(t, path, routes, "gateway"); ok {
				checkMeshRoute(t, path, r, DownstreamHopBudget)
			}
			for _, r := range routes {
				checkMeshRoute(t, path, r, 0)
			}
			for i, r := range routes {
				if last := i == len(routes)-1; last == (len(r.Match) > 0) {
					t.Errorf("%s: route %q (index %d of %d): only the last route may have no match; "+
						"Istio takes the first matching route, so a catch-all before the others swallows them", path, r.Name, i, len(routes))
				}
			}
		})
	}
}
