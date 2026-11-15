// Package redisx 封装 Redis 客户端的创建。
//
// 所有使用 Redis 的代码都要接受一个 key 前缀：不同环境（本机、端到端测试、单元测试）
// 共用同一个 Redis 实例，靠前缀隔离。
//
// 三种部署方式用同一个地址字符串区分（见 ParseAddr），返回的都是 redis.UniversalClient：
//
//	127.0.0.1:6379                          单节点（本机、CI）
//	sentinel://gate@s0:26379,s1:26379       主从加 Sentinel：向 Sentinel 问当前的主库，切换后自己重连
//	cluster://c0:6379,c1:6379               Redis Cluster：按键的槽把命令发给对应的节点
//
// 换部署方式不是只换地址就行的，要看使用方怎么访问键（设计文档 2b 的 1、4 节）：
// 一个 Lua 脚本或事务里的键必须在同一个节点上。库存闸门（pkg/stockgate）的脚本一次改很多个
// 名字不同的键，而且键名是在脚本里拼的，只能用单节点或 Sentinel；查询缓存一次只碰一个键，
// 三种都可以；限流（pkg/ratelimit）对 Cluster 有单独的处理。
package redisx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

// Config 是连接参数。零值的超时取默认值。
type Config struct {
	// Addr 是地址字符串，三种写法见 ParseAddr。
	Addr     string
	Password string
	DB       int
	// DialTimeout 默认 2 秒；ReadTimeout 和 WriteTimeout 默认 1 秒。
	// Redis 在下单主链路上，超时要短：宁可快速失败走降级，也不要拖住请求。
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// PoolSize 是连接数上限，默认 DefaultPoolSize。
	PoolSize int
	// MinIdleConns 是始终保持的空闲连接数，默认等于 PoolSize（启动时建满，见 Open）。
	MinIdleConns int
	// MaxRetries 是一条命令失败后客户端自己重试的次数。0 用 go-redis 的默认值（3 次），-1 不重试。
	// 非幂等的操作要先想清楚被执行两次的后果（设计文档 1d 的 4.16）。
	MaxRetries int
	// DialerRetries 是建连接失败后重试的次数，0 用 go-redis 的默认值。
	DialerRetries int
}

// Mode 是 Redis 的部署方式。
type Mode int

const (
	// Single 是单个节点。
	Single Mode = iota
	// Sentinel 是一主多从，由 Sentinel 监视和切换。
	Sentinel
	// Cluster 是 Redis Cluster。
	Cluster
)

func (m Mode) String() string {
	switch m {
	case Sentinel:
		return "sentinel"
	case Cluster:
		return "cluster"
	default:
		return "single"
	}
}

// Target 是解析后的地址。
type Target struct {
	Mode Mode
	// Addrs：Single 时是唯一的节点；Sentinel 时是各个 Sentinel 的地址（不是 Redis 的）；
	// Cluster 时是用来发现拓扑的种子节点，不必列全。
	Addrs []string
	// Master 是 Sentinel 里登记的主库名字，只有 Sentinel 模式有。
	Master string
}

// ParseAddr 解析地址字符串。三种写法：
//
//	host:port
//	sentinel://<主库名>@host:port[,host:port...]
//	cluster://host:port[,host:port...]
//
// 用一个字符串而不是几个配置项，是为了每个 Redis 仍然只对应一个环境变量（REDIS_ADDR、
// CACHE_REDIS_ADDR、RATE_LIMIT_REDIS_ADDR）：本机写主机加端口，集群上换成带模式的写法，服务的配置不用变。
func ParseAddr(s string) (Target, error) {
	scheme, rest, ok := strings.Cut(s, "://")
	if !ok {
		if s == "" {
			return Target{}, errors.New("redisx: empty address")
		}
		if strings.ContainsAny(s, ",@") {
			return Target{}, fmt.Errorf("redisx: %q: several addresses need a sentinel:// or cluster:// prefix", s)
		}
		return Target{Mode: Single, Addrs: []string{s}}, nil
	}
	var t Target
	switch scheme {
	case "sentinel":
		t.Mode = Sentinel
		master, addrs, ok := strings.Cut(rest, "@")
		if !ok || master == "" {
			return Target{}, fmt.Errorf("redisx: %q: want the form sentinel://<master>@host:port", s)
		}
		t.Master, rest = master, addrs
	case "cluster":
		t.Mode = Cluster
	default:
		return Target{}, fmt.Errorf("redisx: %q: unknown scheme %q (want sentinel or cluster)", s, scheme)
	}
	for a := range strings.SplitSeq(rest, ",") {
		if a = strings.TrimSpace(a); a != "" {
			t.Addrs = append(t.Addrs, a)
		}
	}
	if len(t.Addrs) == 0 {
		return Target{}, fmt.Errorf("redisx: %q: no addresses", s)
	}
	return t, nil
}

// DefaultPoolSize 是默认的连接池大小。
//
// 比 go-redis 的默认值（10 × GOMAXPROCS）小，并且启动时就建满，原因是突发流量：
// 秒杀开始的那一刻几百个请求同时到达，连接池是空的，客户端会同时新建几十上百个连接。
// 新连接还没有往返时间的样本，第一个包丢了要等初始重传超时（1 秒）才重发，正好撞上
// 1 秒的读超时，于是一批请求在同一时刻失败（端到端测试的偶发 503 就是这样）。
// 已经建好的连接重传只要两百毫秒左右，不会超时。
// 固定大小、预先建满之后，突发的请求排队等现成的连接，而不是各自去新建。
// 一次 Lua 调用只要几十微秒，32 个连接每秒能处理十几万次调用，排队的代价可以忽略。
const DefaultPoolSize = 32

// warmUpTimeout 是 Open 等连接池建满的上限。建不满不算错误，只是失去了预热的效果。
const warmUpTimeout = 2 * time.Second

// Open 创建客户端并用 PING 确认可达。调用方负责 Close。
//
// Redis 不可达时返回错误。只有“没有 Redis 就什么都做不了”的程序（测试、工具）该用它；
// 服务用 OpenLazy，见那里的说明。
func Open(ctx context.Context, cfg Config) (redis.UniversalClient, error) {
	client, minIdle, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisx: ping %s: %w", cfg.Addr, err)
	}
	waitWarm(ctx, client, minIdle)
	return client, nil
}

// OpenLazy 创建客户端，Redis 此刻不可达也照样返回它。调用方负责 Close。
//
// 服务启动时用它（设计文档 1d 的 6.10）。Redis 在运行中坏了，各个使用方都有降级的办法（受理拒绝、
// 缓存回源、锁放行）；启动时坏着却让进程起不来的话，Redis 故障期间重启或发布的实例就回不来了，
// 连那些不依赖 Redis 的功能也跟着没了。运行中能降级的东西，启动时坏着也应该能起来。
//
// 可达时和 Open 一样等连接池建满。不可达时记一条 WARN 就返回：go-redis 在每次命令时自己建连接，
// Redis 恢复之后客户端不用重建。只有配置本身不对（地址为空）才返回错误。
func OpenLazy(ctx context.Context, cfg Config, logger *slog.Logger) (redis.UniversalClient, error) {
	client, minIdle, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		if logger == nil {
			logger = slog.Default()
		}
		logger.WarnContext(ctx, "redis is not reachable at startup, starting degraded", "addr", cfg.Addr, "error", err)
		return client, nil
	}
	waitWarm(ctx, client, minIdle)
	return client, nil
}

// New 按 cfg 创建客户端，不访问网络，不等连接池建满。调用方负责 Close。
//
// 给“连不上也要立刻能用、自己另有退路”的使用方（gateway 的限流）。其余的用 Open 或 OpenLazy。
func New(cfg Config) (redis.UniversalClient, error) {
	client, _, err := newClient(cfg)
	return client, err
}

// newClient 按 cfg 创建客户端，不访问网络。返回的 minIdle 是要预先建好的连接数。
func newClient(cfg Config) (client redis.UniversalClient, minIdle int, err error) {
	target, err := ParseAddr(cfg.Addr)
	if err != nil {
		return nil, 0, err
	}
	// 三种模式共用一份选项，再转成各自的类型：超时、连接池、重试的设置在三种模式下含义相同。
	// Sentinel 和 Cluster 模式下 PoolSize、MinIdleConns 是每个节点各自的上限，不是合计。
	opts := &redis.UniversalOptions{
		Addrs:         target.Addrs,
		MasterName:    target.Master,
		Password:      cfg.Password,
		DB:            cfg.DB,
		DialTimeout:   cmp(cfg.DialTimeout, 2*time.Second),
		ReadTimeout:   cmp(cfg.ReadTimeout, time.Second),
		WriteTimeout:  cmp(cfg.WriteTimeout, time.Second),
		PoolSize:      cfg.PoolSize,
		MinIdleConns:  cfg.MinIdleConns,
		MaxRetries:    cfg.MaxRetries,
		DialerRetries: cfg.DialerRetries,
		// 让命令遵守调用方 ctx 的截止时间和取消。go-redis 默认不看 ctx：不开的话，调用方给一次 Redis 调用
		// 设的时限（比如“这次查询最多等 50 毫秒”）不起作用，真正生效的只有上面的 ReadTimeout（1 秒），
		// 调用方取消了命令也照样等到底。1d 做网关限流时发现（设计文档 1d 的 4.16）。
		ContextTimeoutEnabled: true,
	}
	if opts.PoolSize <= 0 {
		opts.PoolSize = DefaultPoolSize
	}
	if opts.MinIdleConns <= 0 {
		opts.MinIdleConns = opts.PoolSize
	}
	opts.MinIdleConns = min(opts.MinIdleConns, opts.PoolSize)
	// 不用 redis.NewUniversalClient：它按“有没有主库名、地址有几个”猜模式，只给一个种子节点的 Cluster
	// 会被当成单节点。这里的模式是地址里写明的。
	switch target.Mode {
	case Sentinel:
		// 返回的是指向当前主库的 *redis.Client。它订阅 Sentinel 的切换通知，主库换了之后新建的连接
		// 连到新的主库；切换期间在途的命令会失败，由调用方按“Redis 不可用”处理。
		client = redis.NewFailoverClient(opts.Failover())
	case Cluster:
		client = redis.NewClusterClient(opts.Cluster())
	default:
		client = redis.NewClient(opts.Simple())
	}
	// 链路和指标（设计文档 1d 的 8.4）：每条命令一个 CLIENT span，名字是命令名；连接池的统计进指标。
	// 不记命令的内容：key 里有用户号和幂等键，Lua 脚本很长。
	// 没有父 span 的命令（后台循环发的）由采样器丢掉，见 otelx 的 dropRootClient。
	if err := redisotel.InstrumentTracing(client, redisotel.WithDBStatement(false), redisotel.WithDialFilter(true),
		// 不记调用位置：它要每条命令做一次 runtime.Caller，而位置总是落在我们自己的封装里，没有信息量。
		redisotel.WithCallerEnabled(false)); err != nil {
		return nil, 0, fmt.Errorf("redisx: instrument tracing: %w", err)
	}
	if err := redisotel.InstrumentMetrics(client); err != nil {
		return nil, 0, fmt.Errorf("redisx: instrument metrics: %w", err)
	}
	return client, opts.MinIdleConns, nil
}

func cmp(v, def time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return def
}

// waitWarm 等 go-redis 在后台建好 n 个空闲连接（它异步补齐 MinIdleConns），最多 warmUpTimeout。
// 服务在健康检查通过之前就把连接建好，第一波流量用的都是现成的连接。
//
// Cluster 模式下 PoolStats 是各节点的合计，节点的连接池在第一次用到那个节点时才建，
// 所以这里等到的只是“合计有 n 个”，不保证每个节点都建满。
func waitWarm(ctx context.Context, client redis.UniversalClient, n int) {
	deadline := time.NewTimer(warmUpTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for int(client.PoolStats().IdleConns) < n {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-tick.C:
		}
	}
}
