// Package redisx 封装 Redis 客户端的创建。
//
// 所有使用 Redis 的代码都要接受一个 key 前缀：不同环境（本机、端到端测试、单元测试）
// 共用同一个 Redis 实例，靠前缀隔离。
package redisx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config 是连接参数。零值的超时取默认值。
type Config struct {
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
func Open(ctx context.Context, cfg Config) (*redis.Client, error) {
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
func OpenLazy(ctx context.Context, cfg Config, logger *slog.Logger) (*redis.Client, error) {
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

// newClient 按 cfg 创建客户端，不访问网络。返回的 minIdle 是要预先建好的连接数。
func newClient(cfg Config) (client *redis.Client, minIdle int, err error) {
	if cfg.Addr == "" {
		return nil, 0, errors.New("redisx: empty address")
	}
	opts := &redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cmp(cfg.DialTimeout, 2*time.Second),
		ReadTimeout:  cmp(cfg.ReadTimeout, time.Second),
		WriteTimeout: cmp(cfg.WriteTimeout, time.Second),
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
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
	return redis.NewClient(opts), opts.MinIdleConns, nil
}

func cmp(v, def time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return def
}

// waitWarm 等 go-redis 在后台建好 n 个空闲连接（它异步补齐 MinIdleConns），最多 warmUpTimeout。
// 服务在健康检查通过之前就把连接建好，第一波流量用的都是现成的连接。
func waitWarm(ctx context.Context, client *redis.Client, n int) {
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
