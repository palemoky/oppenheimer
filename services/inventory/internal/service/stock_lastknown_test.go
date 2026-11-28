//go:build faults

package service

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	inventoryv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/inventory/v1"
	"github.com/palemoky/oppenheimer/pkg/cache"
	"github.com/palemoky/oppenheimer/pkg/fault"
	"github.com/palemoky/oppenheimer/pkg/testredis"
)

// slowRedis 让每条命令多花 d，模拟一个慢一点的缓存 Redis。
type slowRedis struct{ d time.Duration }

func (h slowRedis) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}

func (h slowRedis) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		time.Sleep(h.d)
		return next(ctx, cmd)
	}
}

func (h slowRedis) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		time.Sleep(h.d)
		return next(ctx, cmds)
	}
}

// 这个文件只在带 faults 标签的构建里编译。它钉住的是故障点 f03 的行为（答案在 chaos/scenarios/ 里，这里不写）：
// 开关关着时和平时的构建一样，并发的查询互不等待；开着时，并发的查询一个接一个地过，Reserve 不受影响。
func TestStockLastKnownVariant(t *testing.T) {
	const (
		n   = 8
		rtt = 15 * time.Millisecond
	)
	e := newEnv(t)
	rdb, prefix := testredis.New(t)
	rdb.AddHook(slowRedis{d: rtt})
	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	c, err := cache.New(cache.Config{Client: rdb, KeyPrefix: prefix, Logger: logger}, cache.ProtoCodec[*inventoryv1.GetStockResponse]{})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	svc := New(e.store, e.gate, logger, WithStockCache(c, time.Minute))
	for i := range n {
		e.seed(t, int64(i+1), 100, 10)
	}

	// burst 同时查 n 个不同的 SKU，返回全部查完用的时间。缓存热了之后每次查询正好是一次 Redis 读。
	burst := func() time.Duration {
		start := time.Now()
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				resp, err := svc.GetStock(t.Context(), &inventoryv1.GetStockRequest{SkuId: int64(i + 1)})
				if err != nil || resp.GetAvailable() != 10 {
					t.Errorf("GetStock(%d) = %v, %v; want available 10", i+1, resp, err)
				}
			})
		}
		wg.Wait()
		return time.Since(start)
	}
	burst() // 把缓存填热

	// 关着：n 个查询并行，总耗时接近一次查询。
	if d := burst(); d > n*rtt/2 {
		t.Errorf("switch off: %d concurrent lookups took %s, want under %s", n, d, n*rtt/2)
	}

	// 先量一次不排队的 Reserve 要多久。它写的是真的 MySQL，慢的机器上（CI，带 -race）一次就有几十毫秒：
	// 下面比的是“开着开关时多出来多少”，不是绝对值。原来直接拿 Reserve 的耗时和查询总耗时的一半比，
	// CI 上一次 79 毫秒的 Reserve 就把它判成了“跟着排队”（2026-11-27）。
	start := time.Now()
	if _, err := svc.Reserve(t.Context(), &inventoryv1.ReserveRequest{
		OrderId: 2, Items: []*inventoryv1.ReserveItem{{SkuId: 2, Quantity: 1}},
	}); err != nil {
		t.Fatalf("baseline Reserve: %v", err)
	}
	base := time.Since(start)

	if err := fault.Set("f03", true, time.Minute); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fault.Set("f03", false, 0) })

	// 开着：查询排队，总耗时至少是 n 次 Redis 读之和。同时下一笔 Reserve，它不该跟着排队。
	done := make(chan time.Duration, 1)
	go func() { done <- burst() }()
	time.Sleep(rtt) // 让查询先排起队
	start = time.Now()
	if _, err := svc.Reserve(t.Context(), &inventoryv1.ReserveRequest{
		OrderId: 1, Items: []*inventoryv1.ReserveItem{{SkuId: 1, Quantity: 1}},
	}); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	reserve := time.Since(start)
	d := <-done
	if want := n * rtt * 9 / 10; d < want {
		t.Errorf("switch on: %d concurrent lookups took %s, want at least %s (one after another)", n, d, want)
	}
	// 跟着排队的话 Reserve 要等到查询都过完，比平时至少多出 (n-1)*rtt；不排队时多出来的只是 MySQL 自己的波动。
	if reserve-base > d/2 {
		t.Errorf("switch on: Reserve took %s (%s without the switch) while lookups took %s, want Reserve not to wait for them", reserve, base, d)
	}
}
