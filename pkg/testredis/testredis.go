// Package testredis 给测试提供隔离的 Redis key 空间。
package testredis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/palemoky/oppenheimer/pkg/redisx"
)

// AddrEnv 是指定测试用 Redis 地址的环境变量，例如 127.0.0.1:6379。
const AddrEnv = "OPPENHEIMER_TEST_REDIS_ADDR"

// New 返回一个客户端和本次测试专用的 key 前缀（形如 "t:3f9a1c:"）。
// 被测代码的所有 key 都必须带这个前缀；测试结束时删除该前缀下的全部 key。
//
// 未设置 AddrEnv 时跳过测试；但在 CI 里（CI 环境变量非空）视为失败，
// 避免集成测试因为配置遗漏而悄悄不跑。
func New(t testing.TB) (client *redis.Client, prefix string) {
	t.Helper()
	addr := os.Getenv(AddrEnv)
	if addr == "" {
		if os.Getenv("CI") != "" {
			t.Fatalf("%s is not set in CI", AddrEnv)
		}
		t.Skipf("%s is not set; run `make up` and `make test`", AddrEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := redisx.Open(ctx, redisx.Config{Addr: addr})
	if err != nil {
		t.Fatalf("open redis: %v", err)
	}

	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("random prefix: %v", err)
	}
	prefix = "t:" + hex.EncodeToString(b[:]) + ":"

	t.Cleanup(func() {
		// 测试的 ctx 此时已取消，清理用独立的 ctx。
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		iter := client.Scan(ctx, 0, prefix+"*", 500).Iterator()
		for iter.Next(ctx) {
			if err := client.Del(ctx, iter.Val()).Err(); err != nil {
				t.Logf("cleanup redis key %s: %v", iter.Val(), err)
			}
		}
		if err := iter.Err(); err != nil {
			t.Logf("cleanup redis scan: %v", err)
		}
		_ = client.Close()
	})
	return client, prefix
}
