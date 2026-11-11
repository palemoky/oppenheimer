// Package redisx 封装 Redis 客户端的创建。
//
// 所有使用 Redis 的代码都要接受一个 key 前缀：不同环境（本机、端到端测试、单元测试）
// 共用同一个 Redis 实例，靠前缀隔离。
package redisx

import (
	"context"
	"fmt"
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
	PoolSize     int
}

// Open 创建客户端并用 PING 确认可达。调用方负责 Close。
func Open(ctx context.Context, cfg Config) (*redis.Client, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("redisx: empty address")
	}
	opts := &redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cmp(cfg.DialTimeout, 2*time.Second),
		ReadTimeout:  cmp(cfg.ReadTimeout, time.Second),
		WriteTimeout: cmp(cfg.WriteTimeout, time.Second),
		PoolSize:     cfg.PoolSize,
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisx: ping %s: %w", cfg.Addr, err)
	}
	return client, nil
}

func cmp(v, def time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return def
}
