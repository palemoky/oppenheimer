package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/palemoky/oppenheimer/pkg/ratelimit"
	"github.com/palemoky/oppenheimer/pkg/redisx"
	"github.com/palemoky/oppenheimer/services/gateway/internal/httpapi"
)

// 限流配置的默认值（设计文档 1d 4.2、4.11）。按用户的默认限额在 httpapi.DefaultUserLimits，只写一份。
const (
	defaultIPLimit = "200/400"
	// defaultRateLimitRedisAddr 是本机的缓存实例（compose 里的 redis-cache）。限流的状态丢了只是宽松一次，
	// 正适合不持久化、允许淘汰的缓存实例；也不该让每个请求都去库存闸门所在的实例多一次往返（设计 4.7）。
	defaultRateLimitRedisAddr = "127.0.0.1:6380"
	// rateLimitRedisPingTimeout 是启动时探一次 Redis 的时限。只用来记日志，连不上照样启动。
	rateLimitRedisPingTimeout = time.Second
)

// rateLimitConfig 是限流的配置，由 loadRateLimitConfig 从环境变量读出。
type rateLimitConfig struct {
	// enabled 为 false（RATE_LIMIT=off）时不创建任何限流器、不连 Redis。
	enabled bool
	ip      ratelimit.Limit
	// user 和 route 含规范里的每一个 operationId。
	user, route map[string]ratelimit.Limit
	// hot 含每一个有热点参数的 operationId（httpapi.HotOperationIDs）；hotItems 是个别参数值单独的限额。
	hot      map[string]ratelimit.Limit
	hotItems map[httpapi.HotItem]ratelimit.Limit
	// redisAddr 为空表示不用 Redis，按用户和按接口的也在进程内。
	redisAddr      string
	redisPrefix    string
	redisTimeout   time.Duration
	redisCooldown  time.Duration
	trustedProxies []netip.Prefix
}

// loadRateLimitConfig 读限流的配置。lookup 是 os.LookupEnv 的形状，测试时换成一张表。
//
//   - RATE_LIMIT：on（默认）或 off。
//   - RATE_LIMIT_IP：每个客户端地址的 速率/桶，或 off。默认 200/400。
//   - RATE_LIMIT_USER：按接口覆盖每个用户的限额，"operationId=速率/桶,..."，值可以是 off；
//     整个变量是 off 表示这一维全部不限。没写到的接口用 httpapi.DefaultUserLimits。
//   - RATE_LIMIT_ROUTE：每个接口所有用户合计的限额，格式同上，默认全部不限。
//   - RATE_LIMIT_HOT：热点参数限流，这个接口下每个参数值（SKU、商家）一个桶，格式同上。只能写有热点参数的接口
//     （getStock、listMerchantOrders），没写到的用 httpapi.DefaultHotLimits（每个 SKU 500/1000、每个商家 50/100）。
//   - RATE_LIMIT_HOT_ITEMS：个别参数值单独的限额，"operationId:参数值=速率/桶,..."，例如 getStock:1001=5000/10000。
//     覆盖这个接口的默认值（包括 RATE_LIMIT_HOT 里设成 off 的：那样只限单独列出来的值）。默认没有。
//   - RATE_LIMIT_REDIS_ADDR：按用户和按接口的桶放在哪个 Redis。没设时用 CACHE_REDIS_ADDR（和 inventory 的查询缓存
//     是同一个实例），它也没设时用 127.0.0.1:6380；设成空字符串表示不用 Redis，全部在进程内。
//   - RATE_LIMIT_REDIS_PREFIX：键前缀。没设时用 REDIS_PREFIX（e2e 和压测靠它隔离每一轮的键）。
//   - RATE_LIMIT_REDIS_TIMEOUT（默认 20ms）、RATE_LIMIT_REDIS_COOLDOWN（默认 1s）：见 ratelimit.Fallback。
//   - TRUSTED_PROXIES：逗号分隔的 CIDR 或 IP，默认空，见 httpapi.ParseTrustedProxies。
//
// RATE_LIMIT=off 时其余变量照样校验：写错了的配置在关掉限流时也应当被发现，而不是等打开那天才启动失败。
func loadRateLimitConfig(lookup func(string) (string, bool)) (rateLimitConfig, error) {
	get := func(name string) string {
		v, _ := lookup(name)
		return strings.TrimSpace(v)
	}
	var cfg rateLimitConfig
	switch v := get("RATE_LIMIT"); v {
	case "", "on":
		cfg.enabled = true
	case "off":
	default:
		return rateLimitConfig{}, fmt.Errorf("invalid RATE_LIMIT %q: want on or off", v)
	}

	var err error
	if cfg.ip, err = ratelimit.Parse(cmp.Or(get("RATE_LIMIT_IP"), defaultIPLimit)); err != nil {
		return rateLimitConfig{}, fmt.Errorf("invalid RATE_LIMIT_IP: %w", err)
	}
	if cfg.user, err = httpapi.ParseOperationLimits(get("RATE_LIMIT_USER"), httpapi.DefaultUserLimits()); err != nil {
		return rateLimitConfig{}, fmt.Errorf("invalid RATE_LIMIT_USER: %w", err)
	}
	if cfg.route, err = httpapi.ParseOperationLimits(get("RATE_LIMIT_ROUTE"), nil); err != nil {
		return rateLimitConfig{}, fmt.Errorf("invalid RATE_LIMIT_ROUTE: %w", err)
	}
	if cfg.hot, err = httpapi.ParseHotLimits(get("RATE_LIMIT_HOT"), httpapi.DefaultHotLimits()); err != nil {
		return rateLimitConfig{}, fmt.Errorf("invalid RATE_LIMIT_HOT: %w", err)
	}
	if cfg.hotItems, err = httpapi.ParseHotItems(get("RATE_LIMIT_HOT_ITEMS")); err != nil {
		return rateLimitConfig{}, fmt.Errorf("invalid RATE_LIMIT_HOT_ITEMS: %w", err)
	}

	// “没设”和“设成空”含义不同：前者用默认的缓存实例，后者表示不用 Redis。所以用 lookup 的第二个返回值区分。
	if v, ok := lookup("RATE_LIMIT_REDIS_ADDR"); ok {
		cfg.redisAddr = strings.TrimSpace(v)
	} else {
		cfg.redisAddr = cmp.Or(get("CACHE_REDIS_ADDR"), defaultRateLimitRedisAddr)
	}
	if cfg.redisAddr != "" {
		if _, err := redisx.ParseAddr(cfg.redisAddr); err != nil {
			return rateLimitConfig{}, fmt.Errorf("invalid rate limit redis address: %w", err)
		}
	}
	if v, ok := lookup("RATE_LIMIT_REDIS_PREFIX"); ok {
		cfg.redisPrefix = strings.TrimSpace(v)
	} else {
		cfg.redisPrefix = get("REDIS_PREFIX")
	}
	if cfg.redisTimeout, err = positiveDuration(get, "RATE_LIMIT_REDIS_TIMEOUT", ratelimit.DefaultPrimaryTimeout); err != nil {
		return rateLimitConfig{}, err
	}
	if cfg.redisCooldown, err = positiveDuration(get, "RATE_LIMIT_REDIS_COOLDOWN", ratelimit.DefaultCooldown); err != nil {
		return rateLimitConfig{}, err
	}
	if cfg.trustedProxies, err = httpapi.ParseTrustedProxies(get("TRUSTED_PROXIES")); err != nil {
		return rateLimitConfig{}, fmt.Errorf("invalid TRUSTED_PROXIES: %w", err)
	}
	return cfg, nil
}

// positiveDuration 读一个正的时长，没设时用 def。
func positiveDuration(get func(string) string, name string, def time.Duration) (time.Duration, error) {
	v := get(name)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid %s %q: want a positive duration such as 20ms", name, v)
	}
	return d, nil
}

// newRateLimitRedis 创建限流用的 Redis 客户端。
//
// 不用 redisx.Open：它在启动时 PING，连不上就返回错误。限流的 Redis 是可以退回的依赖（Fallback 会退回进程内），
// 它暂时不可用不应当让网关起不来。go-redis 的客户端本来就是惰性连接、断了自动重连的，所以直接 NewClient，
// 启动时另外探一次只为了记日志（newRateLimit）。另外几项和 redisx 不同的设置也是这里不复用它的原因：
//
//   - ContextTimeoutEnabled：go-redis 默认不看 ctx 的截止时间，只按 ReadTimeout（1 秒）等。Fallback 给 Redis 的
//     时限（默认 20 毫秒）是通过 ctx 传的，不打开它的话 Redis 卡住时每个请求要等满 1 秒才退回。
//   - MaxRetries = -1：不重试。限流脚本扣令牌，不是幂等的，超时后重试可能扣两次；而且 20 毫秒里也没有重试的余地，
//     失败了交给 Fallback 退回更合适。
//   - DialerRetries = 1：同理，建连失败不在时限里反复重试。
//
// 连接池的大小和预先建满沿用 redisx 的理由（突发流量下不现建连接，见 redisx.DefaultPoolSize）；go-redis 在后台补齐空闲连接，
// Redis 不可用时它按自己的节奏重试，不阻塞请求。
//
// 链路和指标和 redisx 里的一样（设计文档 1d 的 8.4）：每条命令一个 CLIENT span，不记命令内容（键里有用户号），
// 不给建连单独出 span；连接池的统计进指标。埋点失败只返回错误，客户端照样可用，由调用方决定怎么处理。
//
// 客户端本身由 redisx.New 创建（它不访问网络）：地址可以是单节点、Sentinel 或 Cluster 的写法（redisx.ParseAddr），
// 集群上限流的桶放在 Redis Cluster 里，那时每个桶单独调用一次脚本（pkg/ratelimit 的 Redis）。
func newRateLimitRedis(addr string) (redis.UniversalClient, error) {
	rdb, err := redisx.New(redisx.Config{
		Addr:          addr,
		MaxRetries:    -1,
		DialerRetries: 1,
		DialTimeout:   time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("rate limit redis: %w", err)
	}
	return rdb, nil
}

// newRateLimit 按配置创建限流器。返回的 rl 为 nil 表示不限流；reporter 为 nil 表示没有汇总日志要输出。
// closeFn 关闭 Redis 客户端（没有时什么也不做），调用方在所有组件停止之后调用。
func newRateLimit(ctx context.Context, logger *slog.Logger, cfg rateLimitConfig) (rl *httpapi.RateLimit, reporter *httpapi.RateLimitReporter, closeFn func()) {
	closeFn = func() {}
	if !cfg.enabled {
		logger.InfoContext(ctx, "rate limit disabled")
		return nil, nil, closeFn
	}

	rl = &httpapi.RateLimit{
		IPLimit:        cfg.ip,
		TrustedProxies: cfg.trustedProxies,
		User:           cfg.user,
		Route:          cfg.route,
		Stats:          &httpapi.RateLimitStats{},
	}
	if cfg.ip.Enabled() {
		// 按 IP 永远在进程内：它是第一道闸，不能依赖 Redis（设计 4.4）。
		rl.IP = ratelimit.NewLocal(ratelimit.LocalConfig{})
	}
	if anyEnabled(cfg.hot) || anyEnabled(cfg.hotItems) {
		// 热点参数也在进程内（为什么不放 Redis 见 httpapi.RateLimit.Hot），但单独一个实例，不和按 IP 的共用：
		// 客户端地址的数量没有上限，共用的话地址多起来会把热点的桶挤出去（被挤掉的桶下次是满的，等于放宽）。
		// 键的数量是 SKU 数加商家数，容量和按 IP 的一样用默认上限。
		rl.Hot = ratelimit.NewLocal(ratelimit.LocalConfig{MaxKeys: ratelimit.DefaultMaxKeys})
		rl.HotLimits = cfg.hot
		rl.HotItems = cfg.hotItems
	}

	var fallbacks func() int64
	useRedis := false
	// 两维都全部不限时不建限流器，也不连 Redis。
	if anyEnabled(cfg.user) || anyEnabled(cfg.route) {
		local := ratelimit.NewLocal(ratelimit.LocalConfig{})
		rl.Shared = local
		var rdb redis.UniversalClient
		if cfg.redisAddr != "" {
			var err error
			// 地址的写法在读配置时已经校验过，这里出错只可能是埋点没装上。那样就不用 Redis，
			// 按用户和按接口的限流退回进程内的令牌桶，照常启动。
			if rdb, err = newRateLimitRedis(cfg.redisAddr); err != nil {
				logger.WarnContext(ctx, "rate limit redis client was not created; per-user and per-route limits use the local limiter", "error", err)
				rdb = nil
			}
		}
		if rdb != nil {
			pctx, cancel := context.WithTimeout(ctx, rateLimitRedisPingTimeout)
			if err := rdb.Ping(pctx).Err(); err != nil {
				logger.WarnContext(ctx, "rate limit redis is unreachable; starting anyway, per-user and per-route limits use the local limiter until it is back",
					"addr", cfg.redisAddr, "error", err)
			}
			cancel()
			fb := ratelimit.NewFallback(ratelimit.FallbackConfig{
				Primary:   ratelimit.NewRedis(rdb, cfg.redisPrefix),
				Secondary: local,
				Timeout:   cfg.redisTimeout,
				Cooldown:  cfg.redisCooldown,
				Logger:    logger,
			})
			rl.Shared = fb
			fallbacks = fb.Fallbacks
			useRedis = true
			unregister, err := httpapi.RegisterFallbackMetric(fb.Fallbacks)
			if err != nil {
				logger.WarnContext(ctx, "register rate limit fallback metric", "error", err)
				unregister = func() {}
			}
			closeFn = func() {
				unregister()
				if err := rdb.Close(); err != nil {
					logger.WarnContext(ctx, "close rate limit redis", "error", err)
				}
			}
		}
	}

	attrs := []any{
		"ip", cfg.ip.String(),
		"user", formatLimits(cfg.user),
		"route", formatLimits(cfg.route),
		"hot", formatLimits(cfg.hot),
		"hot_items", formatHotItems(cfg.hotItems),
		"trusted_proxies", formatPrefixes(cfg.trustedProxies),
		"redis", useRedis,
	}
	if useRedis {
		attrs = append(attrs,
			"redis_addr", cfg.redisAddr,
			"redis_prefix", cfg.redisPrefix,
			"redis_timeout", cfg.redisTimeout.String(),
			"redis_cooldown", cfg.redisCooldown.String(),
		)
	}
	logger.InfoContext(ctx, "rate limit config", attrs...)

	reporter = httpapi.NewRateLimitReporter(rl.Stats, fallbacks, logger, httpapi.DefaultRateLimitReportInterval)
	return rl, reporter, closeFn
}

func anyEnabled[K comparable](m map[K]ratelimit.Limit) bool {
	for _, l := range m {
		if l.Enabled() {
			return true
		}
	}
	return false
}

// formatLimits 把按接口的限额写成 "operationId=速率/桶,..."（按 operationId 排序），和配置的格式一致。
func formatLimits(m map[string]ratelimit.Limit) string {
	parts := make([]string, 0, len(m))
	for _, id := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, id+"="+m[id].String())
	}
	return strings.Join(parts, ",")
}

// formatHotItems 把个别参数值的限额写成 "operationId:参数值=速率/桶,..."（排好序），和配置的格式一致。
func formatHotItems(m map[httpapi.HotItem]ratelimit.Limit) string {
	items := slices.SortedFunc(maps.Keys(m), func(a, b httpapi.HotItem) int {
		return cmp.Or(cmp.Compare(a.Operation, b.Operation), cmp.Compare(a.Value, b.Value))
	})
	parts := make([]string, 0, len(items))
	for _, it := range items {
		parts = append(parts, it.Operation+":"+strconv.FormatInt(it.Value, 10)+"="+m[it].String())
	}
	return strings.Join(parts, ",")
}

func formatPrefixes(ps []netip.Prefix) string {
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		parts = append(parts, p.String())
	}
	return strings.Join(parts, ",")
}
