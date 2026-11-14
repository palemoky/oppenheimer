package cache

import (
	"context"
	"crypto/rand"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/palemoky/oppenheimer/pkg/breaker"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/testredis"
)

// 全局的 MeterProvider 是进程级的，在 TestMain 里设一次。各用例用自己独有的标签值（熔断器、开关、缓存的名字）
// 区分自己的数据点，不假设别的用例没有记过指标。
var reader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	os.Exit(m.Run())
}

// metricInt 返回名为 name、标签恰好是 attrs 的 int64 数据点（计数器的累计值或仪表的最新值）。
func metricInt(t testing.TB, name string, attrs ...attribute.KeyValue) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	want := attribute.NewSet(attrs...)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			var points []metricdata.DataPoint[int64]
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				points = d.DataPoints
			case metricdata.Gauge[int64]:
				points = d.DataPoints
			default:
				t.Fatalf("metric %s has unexpected data type %T", name, m.Data)
			}
			for _, dp := range points {
				if dp.Attributes.Equals(&want) {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

func cacheRequests(t *testing.T, name, result string) int64 {
	t.Helper()
	v, _ := metricInt(t, "cache.requests", attribute.String("cache", name), attribute.String("result", result))
	return v
}

func uniqueName() string { return "t-" + rand.Text() }

func TestRequestsCountHitsAndMisses(t *testing.T) {
	t.Parallel()
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	name := uniqueName()
	c := newCache(t, client, prefix, func(cfg *Config) { cfg.Name = name })
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	mustGet(ctx, t, c, "k", l) // 未命中，回源
	if got := cacheRequests(t, name, "miss"); got != 1 {
		t.Fatalf("miss = %d after the first Get, want 1", got)
	}
	if got := cacheRequests(t, name, "hit"); got != 0 {
		t.Fatalf("hit = %d after the first Get, want 0", got)
	}

	mustGet(ctx, t, c, "k", l) // 命中
	if got := cacheRequests(t, name, "hit"); got != 1 {
		t.Fatalf("hit = %d after the second Get, want 1", got)
	}
	if got := cacheRequests(t, name, "miss"); got != 1 {
		t.Fatalf("miss = %d after the second Get, want it unchanged at 1", got)
	}
	for _, r := range []string{"bypass", "error"} {
		if got := cacheRequests(t, name, r); got != 0 {
			t.Errorf("%s = %d, want 0", r, got)
		}
	}
}

// 空值也缓存，读到它算命中。
func TestRequestsCountCachedNullAsHit(t *testing.T) {
	t.Parallel()
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	name := uniqueName()
	c := newCache(t, client, prefix, func(cfg *Config) { cfg.Name = name })
	l := &loader{res: Loaded[string]{Found: false, TTL: time.Minute}}

	mustGet(ctx, t, c, "k", l)
	mustGet(ctx, t, c, "k", l)
	if h, m := cacheRequests(t, name, "hit"), cacheRequests(t, name, "miss"); h != 1 || m != 1 {
		t.Errorf("hit, miss = %d, %d; want 1, 1", h, m)
	}
}

func TestRequestsDefaultName(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	c := newCache(t, client, prefix)
	before := cacheRequests(t, "default", "miss")
	mustGet(ctx, t, c, "k", &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}})
	if got := cacheRequests(t, "default", "miss"); got <= before {
		t.Errorf("miss under the default name = %d, was %d, want it to increase", got, before)
	}
}

func TestRequestsCountBypassWhenSwitchedOff(t *testing.T) {
	t.Parallel()
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	name := uniqueName()
	set := degrade.New("test", slog.New(slog.DiscardHandler))
	sw := set.Switch("cache", "")
	c := newCache(t, client, prefix, func(cfg *Config) { cfg.Name = name; cfg.Switch = sw })
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	if err := set.SetMode("cache", degrade.ModeOff); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		mustGet(ctx, t, c, "k", l)
	}
	if got := cacheRequests(t, name, "bypass"); got != 3 {
		t.Errorf("bypass = %d, want 3", got)
	}
	if h, m := cacheRequests(t, name, "hit"), cacheRequests(t, name, "miss"); h != 0 || m != 0 {
		t.Errorf("hit, miss = %d, %d while switched off, want 0, 0", h, m)
	}
}

// Redis 读失败：记 error；熔断器打开之后不再读 Redis，记 bypass。
func TestRequestsCountErrorsThenBypassWhenBreakerOpens(t *testing.T) {
	t.Parallel()
	dead := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond, MaxRetries: -1, DialerRetries: 1,
	})
	t.Cleanup(func() { _ = dead.Close() })
	ctx := testCtx(t)
	name := uniqueName()
	const failures = 2
	br := breaker.New(breaker.Config{Name: name, Failures: failures, Cooldown: time.Hour})
	c := newCache(t, dead, "t:dead:", func(cfg *Config) { cfg.Name = name; cfg.Breaker = br })
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}}

	for range failures {
		mustGet(ctx, t, c, "k", l)
	}
	if got := cacheRequests(t, name, "error"); got != failures {
		t.Fatalf("error = %d, want %d", got, failures)
	}
	if got := cacheRequests(t, name, "bypass"); got != 0 {
		t.Fatalf("bypass = %d before the breaker opened, want 0", got)
	}
	for range 3 {
		mustGet(ctx, t, c, "k", l)
	}
	if got := cacheRequests(t, name, "bypass"); got != 3 {
		t.Errorf("bypass = %d after the breaker opened, want 3", got)
	}
	if got := cacheRequests(t, name, "error"); got != failures {
		t.Errorf("error = %d, want it to stay at %d once Redis is not called", got, failures)
	}
}

func TestRequestsCountCoalesced(t *testing.T) {
	t.Parallel()
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	name := uniqueName()
	c := newCache(t, client, prefix, func(cfg *Config) { cfg.Name = name })
	l := &loader{res: Loaded[string]{Value: "v", Found: true, TTL: time.Minute}, delay: 200 * time.Millisecond}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 20 {
		wg.Go(func() {
			<-start
			if _, _, err := c.Get(ctx, "hot", l.load); err != nil {
				t.Errorf("Get: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()

	if got := cacheRequests(t, name, "coalesced"); got == 0 {
		t.Error("coalesced = 0, want concurrent Gets on one cold key to be counted as coalesced")
	}
	if n := l.calls.Load(); n != 1 {
		t.Errorf("loaded %d times, want 1", n)
	}
}
