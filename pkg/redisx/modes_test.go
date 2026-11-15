package redisx

// Sentinel 和 Cluster 的集成测试。
//
// 这两套环境是可选的（make redis-modes-up 启动，profile redis-modes），环境变量指向的地址连不上就跳过，
// 即使在 CI 里也跳过：CI 暂时不起这个 profile。这和单节点测试不同（CI 里没配就失败），因为单节点是
// 所有集成测试的基础，漏配要暴露；这两套只有这里的测试用，漏起不应当拖垮 CI。
// 以后 CI 起了这个 profile，应当改成和单节点一样的规矩。
//
// Sentinel 的测试都不并行：故障切换会改变整套 Sentinel 的主库，并行的其他测试会撞上切换。

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	sentinelAddrEnv = "OPPENHEIMER_TEST_REDIS_SENTINEL_ADDR"
	clusterAddrEnv  = "OPPENHEIMER_TEST_REDIS_CLUSTER_ADDR"
)

// openOptional 读环境变量里的地址，先 PING（1 秒超时），连不上就跳过；通了就用 Open 打开。
func openOptional(t *testing.T, env string) (redis.UniversalClient, Target, string) {
	t.Helper()
	addr := os.Getenv(env)
	if addr == "" {
		t.Skipf("%s is not set; run `make redis-modes-up`", env)
	}
	target, err := ParseAddr(addr)
	if err != nil {
		t.Fatalf("%s: %v", env, err)
	}
	probe := redis.NewClient(&redis.Options{Addr: target.Addrs[0], DialTimeout: time.Second, ReadTimeout: time.Second, MaxRetries: -1})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err = probe.Ping(ctx).Err()
	_ = probe.Close()
	if err != nil {
		t.Skipf("%s unreachable (%v); run `make redis-modes-up`", env, err)
	}
	ctx, cancel = context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	client, err := Open(ctx, Config{Addr: addr})
	if err != nil {
		t.Fatalf("Open(%s): %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, target, addr
}

func randPrefix(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "t:" + hex.EncodeToString(b[:]) + ":"
}

// cleanupKeys 在测试结束时删掉这些键。Cluster 下逐个 DEL（不同的槽不能一起删）。
func cleanupKeys(t *testing.T, c redis.UniversalClient, keys func() []string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, k := range keys() {
			if err := c.Del(ctx, k).Err(); err != nil {
				t.Logf("cleanup %s: %v", k, err)
			}
		}
	})
}

// twoKeyScript 读两个键，是“脚本访问多个不同名字的键”的最小例子。
var twoKeyScript = redis.NewScript(`
redis.call('SET', KEYS[1], '1')
redis.call('SET', KEYS[2], '2')
return redis.call('GET', KEYS[1]) .. redis.call('GET', KEYS[2])
`)

func TestSentinelBasic(t *testing.T) {
	c, target, _ := openOptional(t, sentinelAddrEnv)
	if target.Mode != Sentinel || target.Master != "gate" {
		t.Fatalf("target = %+v, want sentinel mode with master gate", target)
	}
	p := randPrefix(t)
	k1, k2 := p+"a", p+"b"
	cleanupKeys(t, c, func() []string { return []string{k1, k2} })
	ctx := t.Context()

	if err := c.Set(ctx, k1, "v", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Get(ctx, k1).Result(); err != nil || got != "v" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	// 主从下只有一个写入节点，名字不同的键之间没有槽的限制。
	got, err := twoKeyScript.Run(ctx, c, []string{k1, k2}).Text()
	if err != nil || got != "12" {
		t.Fatalf("multi-key script = %q, %v; want 12", got, err)
	}
}

// 故障切换：用同一个客户端，切换前写入的键在切换后还在，切换期间的写入在时限内重试能成功。
func TestSentinelFailover(t *testing.T) {
	c, target, _ := openOptional(t, sentinelAddrEnv)
	ctx := t.Context()
	p := randPrefix(t)
	before := p + "before"
	cleanupKeys(t, c, func() []string { return []string{before, p + "during", p + "after"} })

	sentinel := redis.NewClient(&redis.Options{Addr: target.Addrs[0], DialTimeout: time.Second, ReadTimeout: 3 * time.Second, MaxRetries: -1})
	t.Cleanup(func() { _ = sentinel.Close() })
	master := func() string {
		t.Helper()
		a, err := sentinel.Do(ctx, "SENTINEL", "get-master-addr-by-name", target.Master).StringSlice()
		if err != nil || len(a) != 2 {
			t.Fatalf("get-master-addr-by-name: %v %v", a, err)
		}
		return a[0] + ":" + a[1]
	}

	if err := c.Set(ctx, before, "x", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	// 等从库收到，否则切换会丢掉这个键：复制是异步的，这里测的不是丢写。
	// 上一次切换（连跑多次时）刚结束，旧主库要一会儿才作为从库连回来，所以重试。WAIT 的时限要小于客户端的读超时。
	for i := 0; ; i++ {
		n, err := c.Do(ctx, "WAIT", 1, 500).Int()
		if err == nil && n >= 1 {
			break
		}
		if i >= 40 {
			t.Fatalf("WAIT = %d, %v; want a replica to have the write", n, err)
		}
	}
	old := master()

	if err := sentinel.Do(ctx, "SENTINEL", "FAILOVER", target.Master).Err(); err != nil {
		t.Fatalf("SENTINEL FAILOVER: %v", err)
	}

	// FAILOVER 是异步的：命令返回时切换才刚开始。等 Sentinel 报告的主库变了，期间一直用同一个客户端写，
	// 记下失败的次数；主库变了之后写入必须在时限内成功。
	start := time.Now()
	deadline := start.Add(15 * time.Second)
	var failures int
	write := func() error {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return c.Set(cctx, p+"during", "y", time.Minute).Err()
	}
	for master() == old {
		if time.Now().After(deadline) {
			t.Fatalf("sentinel still reports master %s %s after FAILOVER", old, time.Since(start))
		}
		if err := write(); err != nil {
			failures++
		}
		time.Sleep(50 * time.Millisecond)
	}
	switched := time.Since(start)
	for {
		lastErr := write()
		if lastErr == nil {
			break
		}
		failures++
		if time.Now().After(deadline) {
			t.Fatalf("write still failing %s after FAILOVER (%d failures), last error: %v", time.Since(start), failures, lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("sentinel switched master after %s; writes succeeded again after %s; %d failed attempts", switched, time.Since(start), failures)

	if got, err := c.Get(ctx, before).Result(); err != nil || got != "x" {
		t.Errorf("key written before failover: %q, %v", got, err)
	}
	if got, err := c.Get(ctx, p+"during").Result(); err != nil || got != "y" {
		t.Errorf("key written during failover: %q, %v", got, err)
	}
}

func TestClusterBasic(t *testing.T) {
	c, target, _ := openOptional(t, clusterAddrEnv)
	if target.Mode != Cluster {
		t.Fatalf("target = %+v, want cluster mode", target)
	}
	t.Parallel()
	p := randPrefix(t)
	const n = 200
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("%sk%d", p, i)
	}
	cleanupKeys(t, c, func() []string { return keys })
	ctx := t.Context()

	for i, k := range keys {
		if err := c.Set(ctx, k, i, time.Minute).Err(); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}
	for i, k := range keys {
		if got, err := c.Get(ctx, k).Result(); err != nil || got != strconv.Itoa(i) {
			t.Fatalf("Get %s = %q, %v; want %d", k, got, err, i)
		}
	}
	// 键确实分散在不同的槽：200 个键不可能都在一个槽里。
	slots := map[int64]bool{}
	for _, k := range keys {
		s, err := c.ClusterKeySlot(ctx, k).Result()
		if err != nil {
			t.Fatal(err)
		}
		slots[s] = true
	}
	if len(slots) < 50 {
		t.Errorf("only %d distinct slots for %d keys", len(slots), n)
	}
}

// 钉住“为什么库存闸门不能上 Cluster”：脚本访问的键不在同一个槽就被 Redis 拒绝。
func TestClusterScriptCrossSlot(t *testing.T) {
	c, _, _ := openOptional(t, clusterAddrEnv)
	t.Parallel()
	p := randPrefix(t)
	ctx := t.Context()

	// 找两个确实不同槽的键，不靠运气。
	a, b := p+"a", p+"b"
	sa, _ := c.ClusterKeySlot(ctx, a).Result()
	sb, _ := c.ClusterKeySlot(ctx, b).Result()
	if sa == sb {
		b = p + "c"
	}
	_, err := twoKeyScript.Run(ctx, c, []string{a, b}).Result()
	if err == nil {
		t.Fatalf("script over keys in different slots succeeded, want a CROSSSLOT error")
	}
	if !strings.Contains(err.Error(), "CROSSSLOT") {
		t.Errorf("error = %v, want CROSSSLOT", err)
	}

	// 相同的 hash tag 落在同一个槽，脚本可以执行。
	t1, t2 := "{"+p+"t}:a", "{"+p+"t}:b"
	cleanupKeys(t, c, func() []string { return []string{t1, t2} })
	got, err := twoKeyScript.Run(ctx, c, []string{t1, t2}).Text()
	if err != nil || got != "12" {
		t.Fatalf("script over hash-tagged keys = %q, %v; want 12", got, err)
	}
}
