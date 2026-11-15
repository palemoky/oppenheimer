package ratelimit_test

// Redis Cluster 下的限流器：每个桶单独调用一次脚本（见 Redis 的类型注释）。
//
// Cluster 环境是可选的（make redis-modes-up，profile redis-modes）：OPPENHEIMER_TEST_REDIS_CLUSTER_ADDR
// 指向的地址连不上就跳过，即使在 CI 里也跳过，因为 CI 暂时不起这个 profile。这和单节点测试
// （testredis.New：CI 里没配就失败）不同：单节点是所有集成测试的基础，漏配要暴露；这套只有这里用。

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/palemoky/oppenheimer/pkg/ratelimit"
	"github.com/palemoky/oppenheimer/pkg/redisx"
)

func openCluster(t *testing.T) (client redis.UniversalClient, prefix string) {
	t.Helper()
	const env = "OPPENHEIMER_TEST_REDIS_CLUSTER_ADDR"
	addr := os.Getenv(env)
	if addr == "" {
		t.Skipf("%s is not set; run `make redis-modes-up`", env)
	}
	target, err := redisx.ParseAddr(addr)
	if err != nil {
		t.Fatalf("%s: %v", env, err)
	}
	probe := redis.NewClient(&redis.Options{Addr: target.Addrs[0], DialTimeout: time.Second, ReadTimeout: time.Second, MaxRetries: -1})
	pctx, cancel := context.WithTimeout(t.Context(), time.Second)
	err = probe.Ping(pctx).Err()
	cancel()
	_ = probe.Close()
	if err != nil {
		t.Skipf("%s unreachable (%v); run `make redis-modes-up`", env, err)
	}
	octx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client, err = redisx.Open(octx, redisx.Config{Addr: addr})
	if err != nil {
		t.Fatalf("open cluster: %v", err)
	}
	cc, ok := client.(*redis.ClusterClient)
	if !ok {
		t.Fatalf("client is %T, want *redis.ClusterClient", client)
	}

	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	prefix = "t:" + hex.EncodeToString(b[:]) + ":"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// SCAN 只看一个节点：对每个主节点各做一次。
		err := cc.ForEachMaster(ctx, func(ctx context.Context, n *redis.Client) error {
			iter := n.Scan(ctx, 0, prefix+"*", 500).Iterator()
			for iter.Next(ctx) {
				if err := n.Del(ctx, iter.Val()).Err(); err != nil {
					return err
				}
			}
			return iter.Err()
		})
		if err != nil {
			t.Logf("cleanup: %v", err)
		}
		_ = client.Close()
	})
	return client, prefix
}

func TestClusterRedisSecondBucketDenies(t *testing.T) {
	t.Parallel()
	rdb, prefix := openCluster(t)
	l := ratelimit.NewRedis(rdb, prefix)
	ctx := t.Context()
	// 补充很慢（每 1000 秒一个），测试期间可以当作不补。
	slow := func(burst int) ratelimit.Limit { return ratelimit.Limit{Rate: 0.001, Burst: burst} }
	reqs := []ratelimit.Request{{Key: "big", Limit: slow(100)}, {Key: "small", Limit: slow(1)}}

	d, err := l.Allow(ctx, reqs...)
	if err != nil || !d.Allowed {
		t.Fatalf("first Allow = %+v, %v; want allowed", d, err)
	}
	d, err = l.Allow(ctx, reqs...)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || d.Denied != 1 || d.RetryAfter <= 0 {
		t.Fatalf("second Allow = %+v; want denied by request 1 with RetryAfter > 0", d)
	}
}

// 钉住“白扣”：Cluster 下前面的桶先扣，后面的桶拒绝时前面扣掉的不退。单节点和主从下是全有或全无，
// 被拒绝的请求一个令牌都不扣。这是有意接受的代价（ADR-0019 的补充）：限流被拒绝时略微偏严，不会偏松。
func TestClusterRedisEarlierBucketsStayCharged(t *testing.T) {
	t.Parallel()
	rdb, prefix := openCluster(t)
	l := ratelimit.NewRedis(rdb, prefix)
	ctx := t.Context()
	slow := func(burst int) ratelimit.Limit { return ratelimit.Limit{Rate: 0.001, Burst: burst} }
	first, second := slow(2), slow(1)

	for i, wantAllowed := range []bool{true, false} {
		d, err := l.Allow(ctx, ratelimit.Request{Key: "a", Limit: first}, ratelimit.Request{Key: "b", Limit: second})
		if err != nil || d.Allowed != wantAllowed {
			t.Fatalf("call %d = %+v, %v; want allowed=%v", i, d, err, wantAllowed)
		}
	}
	// 两次调用都扣了桶 a（容量 2），第二次虽然被 b 拒绝。只查 a：已经空了。
	d, err := l.Allow(ctx, ratelimit.Request{Key: "a", Limit: first})
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || d.Denied != 0 || d.RetryAfter <= 0 {
		t.Fatalf("third Allow on bucket a = %+v; want denied (the denied second call still charged it)", d)
	}
}

// 单个桶的调用在 Cluster 下和单节点一样：放行到耗尽，然后拒绝。
func TestClusterRedisSingleBucket(t *testing.T) {
	t.Parallel()
	rdb, prefix := openCluster(t)
	l := ratelimit.NewRedis(rdb, prefix)
	ctx := t.Context()
	req := ratelimit.Request{Key: "solo", Limit: ratelimit.Limit{Rate: 0.001, Burst: 5}}

	for i := range 5 {
		d, err := l.Allow(ctx, req)
		if err != nil || !d.Allowed || d.Denied != -1 {
			t.Fatalf("call %d = %+v, %v; want allowed", i, d, err)
		}
	}
	d, err := l.Allow(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || d.Denied != 0 || d.RetryAfter <= 0 {
		t.Fatalf("call 6 = %+v; want denied with RetryAfter > 0", d)
	}
}
