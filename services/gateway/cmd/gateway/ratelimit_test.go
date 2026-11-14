package main

import (
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/ratelimit"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi"
)

// envMap 把一张表变成 os.LookupEnv 的形状。
func envMap(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadRateLimitConfigDefaults(t *testing.T) {
	cfg, err := loadRateLimitConfig(envMap(nil))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.enabled {
		t.Error("rate limit should be on by default")
	}
	if cfg.ip != (ratelimit.Limit{Rate: 200, Burst: 400}) {
		t.Errorf("ip = %v", cfg.ip)
	}
	defaults := httpapi.DefaultUserLimits()
	for _, id := range httpapi.OperationIDs() {
		if cfg.user[id] != defaults[id] || !cfg.user[id].Enabled() {
			t.Errorf("user[%s] = %v, want default %v", id, cfg.user[id], defaults[id])
		}
		if l, ok := cfg.route[id]; !ok || l.Enabled() {
			t.Errorf("route[%s] = %v, %v; want present and off", id, l, ok)
		}
	}
	// 设计 4.2 的表里的几项，防止默认值被悄悄改掉。
	for id, want := range map[string]ratelimit.Limit{
		"createOrder": {Rate: 5, Burst: 10}, "getOrder": {Rate: 20, Burst: 40},
		"listMerchantOrders": {Rate: 10, Burst: 20}, "getStock": {Rate: 50, Burst: 100},
	} {
		if cfg.user[id] != want {
			t.Errorf("default user[%s] = %v, want %v", id, cfg.user[id], want)
		}
	}
	// 热点参数（设计 5.8 的表）：只有这两个接口，没有单独设的值。
	wantHot := map[string]ratelimit.Limit{"getStock": {Rate: 500, Burst: 1000}, "listMerchantOrders": {Rate: 50, Burst: 100}}
	if len(cfg.hot) != len(wantHot) {
		t.Errorf("hot = %v, want %v", cfg.hot, wantHot)
	}
	for id, want := range wantHot {
		if cfg.hot[id] != want {
			t.Errorf("default hot[%s] = %v, want %v", id, cfg.hot[id], want)
		}
	}
	if len(cfg.hotItems) != 0 {
		t.Errorf("hot items = %v, want none", cfg.hotItems)
	}
	if cfg.redisAddr != "127.0.0.1:6380" || cfg.redisPrefix != "" {
		t.Errorf("redis = %q prefix %q", cfg.redisAddr, cfg.redisPrefix)
	}
	if cfg.redisTimeout != 20*time.Millisecond || cfg.redisCooldown != time.Second {
		t.Errorf("timeout %v cooldown %v", cfg.redisTimeout, cfg.redisCooldown)
	}
	if len(cfg.trustedProxies) != 0 {
		t.Errorf("trusted proxies = %v", cfg.trustedProxies)
	}
}

func TestLoadRateLimitConfigOverrides(t *testing.T) {
	cfg, err := loadRateLimitConfig(envMap(map[string]string{
		"RATE_LIMIT":                "on",
		"RATE_LIMIT_IP":             "off",
		"RATE_LIMIT_USER":           "createOrder=1/2,getOrder=off",
		"RATE_LIMIT_ROUTE":          "createOrder=3000/6000",
		"RATE_LIMIT_HOT":            "getStock=100/200",
		"RATE_LIMIT_HOT_ITEMS":      " getStock:1001=5000/10000, listMerchantOrders:7=off ,",
		"RATE_LIMIT_REDIS_TIMEOUT":  "5ms",
		"RATE_LIMIT_REDIS_COOLDOWN": "3s",
		"RATE_LIMIT_REDIS_PREFIX":   "custom:",
		"REDIS_PREFIX":              "ignored:",
		"TRUSTED_PROXIES":           "10.0.0.0/8,192.0.2.1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ip.Enabled() {
		t.Errorf("ip = %v, want off", cfg.ip)
	}
	if cfg.user["createOrder"] != (ratelimit.Limit{Rate: 1, Burst: 2}) || cfg.user["getOrder"].Enabled() {
		t.Errorf("user = %v", cfg.user)
	}
	if cfg.user["payOrder"] != httpapi.DefaultUserLimits()["payOrder"] {
		t.Errorf("payOrder should keep its default, got %v", cfg.user["payOrder"])
	}
	if cfg.route["createOrder"] != (ratelimit.Limit{Rate: 3000, Burst: 6000}) || cfg.route["getOrder"].Enabled() {
		t.Errorf("route = %v", cfg.route)
	}
	if cfg.hot["getStock"] != (ratelimit.Limit{Rate: 100, Burst: 200}) {
		t.Errorf("hot[getStock] = %v", cfg.hot["getStock"])
	}
	if cfg.hot["listMerchantOrders"] != httpapi.DefaultHotLimits()["listMerchantOrders"] {
		t.Errorf("hot[listMerchantOrders] should keep its default, got %v", cfg.hot["listMerchantOrders"])
	}
	wantItems := map[httpapi.HotItem]ratelimit.Limit{
		{Operation: "getStock", Value: 1001}:        {Rate: 5000, Burst: 10000},
		{Operation: "listMerchantOrders", Value: 7}: {},
	}
	if len(cfg.hotItems) != len(wantItems) {
		t.Errorf("hot items = %v, want %v", cfg.hotItems, wantItems)
	}
	for k, want := range wantItems {
		if got, ok := cfg.hotItems[k]; !ok || got != want {
			t.Errorf("hot item %v = %v, %v; want %v", k, got, ok, want)
		}
	}
	if got := formatHotItems(cfg.hotItems); got != "getStock:1001=5000/10000,listMerchantOrders:7=off" {
		t.Errorf("formatHotItems = %q", got)
	}
	if cfg.redisTimeout != 5*time.Millisecond || cfg.redisCooldown != 3*time.Second || cfg.redisPrefix != "custom:" {
		t.Errorf("redis settings = %v %v %q", cfg.redisTimeout, cfg.redisCooldown, cfg.redisPrefix)
	}
	if len(cfg.trustedProxies) != 2 {
		t.Errorf("trusted proxies = %v", cfg.trustedProxies)
	}
}

func TestLoadRateLimitConfigOff(t *testing.T) {
	cfg, err := loadRateLimitConfig(envMap(map[string]string{"RATE_LIMIT": "off"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.enabled {
		t.Error("RATE_LIMIT=off should disable")
	}
	rl, rep, closeFn := newRateLimit(t.Context(), slog.New(slog.DiscardHandler), cfg)
	defer closeFn()
	if rl != nil || rep != nil {
		t.Errorf("RATE_LIMIT=off created limiters: %v %v", rl, rep)
	}

	// RATE_LIMIT=off 时热点这一维也不生效（上面已经没有任何限流器）；单独把热点整个关掉：
	cfg, err = loadRateLimitConfig(envMap(map[string]string{"RATE_LIMIT_HOT": "off", "RATE_LIMIT_REDIS_ADDR": ""}))
	if err != nil {
		t.Fatal(err)
	}
	for id, l := range cfg.hot {
		if l.Enabled() {
			t.Errorf("hot[%s] = %v, want off", id, l)
		}
	}
	rl, _, closeHot := newRateLimit(t.Context(), slog.New(slog.DiscardHandler), cfg)
	defer closeHot()
	if rl.Hot != nil {
		t.Errorf("hot limiter = %T, want nil when RATE_LIMIT_HOT=off", rl.Hot)
	}

	// 一维整个关掉。
	cfg, err = loadRateLimitConfig(envMap(map[string]string{"RATE_LIMIT_USER": "off"}))
	if err != nil {
		t.Fatal(err)
	}
	for id, l := range cfg.user {
		if l.Enabled() {
			t.Errorf("user[%s] = %v, want off", id, l)
		}
	}
}

func TestLoadRateLimitConfigRedisAddr(t *testing.T) {
	tests := []struct {
		name      string
		env       map[string]string
		addr, pfx string
	}{
		{"unset uses the cache instance", nil, "127.0.0.1:6380", ""},
		{"unset follows CACHE_REDIS_ADDR", map[string]string{"CACHE_REDIS_ADDR": "cache:6379"}, "cache:6379", ""},
		{"set", map[string]string{"RATE_LIMIT_REDIS_ADDR": "rl:6379", "CACHE_REDIS_ADDR": "cache:6379"}, "rl:6379", ""},
		{"set to empty means no Redis", map[string]string{"RATE_LIMIT_REDIS_ADDR": "", "CACHE_REDIS_ADDR": "cache:6379"}, "", ""},
		{"prefix defaults to REDIS_PREFIX", map[string]string{"REDIS_PREFIX": "e2e-1:"}, "127.0.0.1:6380", "e2e-1:"},
		{"prefix set to empty", map[string]string{"REDIS_PREFIX": "e2e-1:", "RATE_LIMIT_REDIS_PREFIX": ""}, "127.0.0.1:6380", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := loadRateLimitConfig(envMap(tt.env))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.redisAddr != tt.addr || cfg.redisPrefix != tt.pfx {
				t.Errorf("addr %q prefix %q, want %q %q", cfg.redisAddr, cfg.redisPrefix, tt.addr, tt.pfx)
			}
		})
	}
}

func TestLoadRateLimitConfigErrors(t *testing.T) {
	tests := []struct {
		env  map[string]string
		want string // 错误信息里要指出的配置项
	}{
		{map[string]string{"RATE_LIMIT": "yes"}, "RATE_LIMIT"},
		{map[string]string{"RATE_LIMIT_IP": "200"}, "RATE_LIMIT_IP"},
		{map[string]string{"RATE_LIMIT_IP": "0/0"}, "RATE_LIMIT_IP"},
		{map[string]string{"RATE_LIMIT_USER": "createOrdr=5/10"}, "createOrdr"},
		{map[string]string{"RATE_LIMIT_USER": "createOrder=5"}, "RATE_LIMIT_USER"},
		{map[string]string{"RATE_LIMIT_ROUTE": "nope=1/1"}, "RATE_LIMIT_ROUTE"},
		{map[string]string{"RATE_LIMIT_ROUTE": "getOrder=-1/1"}, "getOrder"},
		{map[string]string{"RATE_LIMIT_REDIS_TIMEOUT": "20"}, "RATE_LIMIT_REDIS_TIMEOUT"},
		{map[string]string{"RATE_LIMIT_REDIS_COOLDOWN": "-1s"}, "RATE_LIMIT_REDIS_COOLDOWN"},
		{map[string]string{"TRUSTED_PROXIES": "10.0.0.0/40"}, "TRUSTED_PROXIES"},
		// 关掉限流时写错的配置照样报出来。
		{map[string]string{"RATE_LIMIT": "off", "RATE_LIMIT_USER": "bogus=1/1"}, "bogus"},
		// 热点参数：规范里没有的、规范里有但没有热点参数的、格式错误、重复。
		{map[string]string{"RATE_LIMIT_HOT": "getStok=1/1"}, "unknown operationId"},
		{map[string]string{"RATE_LIMIT_HOT": "getOrder=1/1"}, "does not take this limit"},
		{map[string]string{"RATE_LIMIT_HOT": "getStock=1"}, "RATE_LIMIT_HOT"},
		{map[string]string{"RATE_LIMIT_HOT": "getStock"}, "RATE_LIMIT_HOT"},
		{map[string]string{"RATE_LIMIT_HOT": "getStock=0/1"}, "getStock"},
		{map[string]string{"RATE_LIMIT_HOT": "getStock=1/2,getStock=off"}, "more than once"},
		{map[string]string{"RATE_LIMIT": "off", "RATE_LIMIT_HOT": "createOrder=1/1"}, "createOrder"},
		// 个别参数值：未知的、没有热点参数的 operationId，不是正整数的参数值，格式错误，重复。
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": "getStok:1=1/1"}, "unknown operationId"},
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": "getOrder:1=1/1"}, "does not take this limit"},
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": "getStock:abc=1/1"}, "not a positive integer"},
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": "getStock:0=1/1"}, "not a positive integer"},
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": "getStock:-5=1/1"}, "not a positive integer"},
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": "getStock=1/1"}, "RATE_LIMIT_HOT_ITEMS"},
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": "getStock:1"}, "RATE_LIMIT_HOT_ITEMS"},
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": "getStock:=1/1"}, "RATE_LIMIT_HOT_ITEMS"},
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": ":1=1/1"}, "RATE_LIMIT_HOT_ITEMS"},
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": "getStock:1=fast"}, "getStock:1=fast"},
		{map[string]string{"RATE_LIMIT_HOT_ITEMS": "getStock:1=1/1,getStock:01=2/2"}, "more than once"},
	}
	for _, tt := range tests {
		_, err := loadRateLimitConfig(envMap(tt.env))
		if err == nil {
			t.Errorf("%v: want error", tt.env)
			continue
		}
		if !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%v: error %q does not mention %s", tt.env, err, tt.want)
		}
	}
}

func TestNewRateLimitWithoutRedis(t *testing.T) {
	cfg, err := loadRateLimitConfig(envMap(map[string]string{"RATE_LIMIT_REDIS_ADDR": ""}))
	if err != nil {
		t.Fatal(err)
	}
	rl, rep, closeFn := newRateLimit(t.Context(), slog.New(slog.DiscardHandler), cfg)
	defer closeFn()
	if rl == nil || rep == nil {
		t.Fatal("rate limit should be on")
	}
	if _, ok := rl.Shared.(*ratelimit.Local); !ok {
		t.Errorf("shared limiter = %T, want the local limiter when RATE_LIMIT_REDIS_ADDR is empty", rl.Shared)
	}
	if _, ok := rl.IP.(*ratelimit.Local); !ok {
		t.Errorf("ip limiter = %T, want local", rl.IP)
	}
	// 热点默认开着，用单独的一个进程内限流器，不和按 IP 的共用。
	if hot, ok := rl.Hot.(*ratelimit.Local); !ok || hot == rl.IP {
		t.Errorf("hot limiter = %T (same as ip: %v), want its own local limiter", rl.Hot, rl.Hot == rl.IP)
	}
	if rl.HotLimits["getStock"] != cfg.hot["getStock"] {
		t.Errorf("hot limits not passed on: %v", rl.HotLimits)
	}

	// 按用户和按接口都关掉时不建限流器（也就不会连 Redis）。
	cfg, err = loadRateLimitConfig(envMap(map[string]string{"RATE_LIMIT_USER": "off"}))
	if err != nil {
		t.Fatal(err)
	}
	rl, _, closeFn2 := newRateLimit(t.Context(), slog.New(slog.DiscardHandler), cfg)
	defer closeFn2()
	if rl.Shared != nil {
		t.Errorf("shared limiter = %T, want nil when both dimensions are off", rl.Shared)
	}
}

// hangingServer 接受连接但从不回应，模拟卡住的 Redis：比连接被拒绝更难处理，
// 客户端如果不看 ctx 的截止时间，就会一直等到自己的读超时。
func hangingServer(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

// TestNewRateLimitRedisHangs：Redis 卡住时网关照常启动，每次判断最多等 RATE_LIMIT_REDIS_TIMEOUT 左右就退回进程内，
// 限额仍然生效。这验证的是 newRateLimitRedis 的设置（ContextTimeoutEnabled、不重试），用的是生产的装配代码。
func TestNewRateLimitRedisHangs(t *testing.T) {
	const timeout = 30 * time.Millisecond
	cfg, err := loadRateLimitConfig(envMap(map[string]string{
		"RATE_LIMIT_REDIS_ADDR":    hangingServer(t),
		"RATE_LIMIT_REDIS_TIMEOUT": timeout.String(),
		// 冷却时间设得很短，让每次判断都真的去问一次卡住的 Redis。
		"RATE_LIMIT_REDIS_COOLDOWN": "1ns",
		"RATE_LIMIT_USER":           "getOrder=0.001/2",
	}))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	rl, _, closeFn := newRateLimit(t.Context(), slog.New(slog.DiscardHandler), cfg)
	defer closeFn()
	if d := time.Since(start); d > rateLimitRedisPingTimeout+time.Second {
		t.Errorf("startup took %v with a hanging Redis", d)
	}
	fb, ok := rl.Shared.(*ratelimit.Fallback)
	if !ok {
		t.Fatalf("shared limiter = %T, want *ratelimit.Fallback", rl.Shared)
	}

	req := ratelimit.Request{Key: "u:7:getOrder", Limit: cfg.user["getOrder"]}
	var allowed int
	for range 4 {
		start := time.Now()
		d, err := fb.Allow(t.Context(), req)
		if err != nil {
			t.Fatal(err)
		}
		if el := time.Since(start); el > timeout+250*time.Millisecond {
			t.Errorf("Allow took %v with a hanging Redis, want about %v", el, timeout)
		}
		if d.Allowed {
			allowed++
		}
	}
	if allowed != 2 {
		t.Errorf("allowed %d of 4, want 2 (the local limiter still enforces the burst)", allowed)
	}
	if fb.Fallbacks() != 4 {
		t.Errorf("fallbacks = %d, want 4", fb.Fallbacks())
	}
}
