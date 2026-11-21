package redisx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 这里的用例各自建一份 commandMetrics，接自己的 ManualReader，按“命令名|错误类别”数出精确的次数，
// 不受同一个进程里其他用例往全局直方图里记的东西影响。

func newTestMetrics(t testing.TB) (*commandMetrics, *sdkmetric.ManualReader) {
	t.Helper()
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r), sdkmetric.WithView(otelx.DurationView()))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return newCommandMetrics(mp.Meter("test")), r
}

// counts 返回 db.client.operation.duration 的记录次数，键是 "命令名|error.type"（成功时 error.type 为空）。
// 顺带检查每个数据点只有允许的三个标签、db.system.name 是 redis、单位是秒。
func counts(t *testing.T, r *sdkmetric.ManualReader) map[string]uint64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]uint64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "db.client.operation.duration" {
				continue
			}
			if m.Unit != "s" {
				t.Errorf("unit = %q, want s", m.Unit)
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("data is %T, want a float64 histogram", m.Data)
			}
			for _, dp := range h.DataPoints {
				for _, kv := range dp.Attributes.ToSlice() {
					switch kv.Key {
					case "db.system.name":
						if kv.Value.AsString() != "redis" {
							t.Errorf("db.system.name = %q", kv.Value.AsString())
						}
					case "db.operation.name", "error.type":
					default:
						t.Errorf("unexpected attribute %s = %q", kv.Key, kv.Value.String())
					}
				}
				op, _ := dp.Attributes.Value("db.operation.name")
				et, _ := dp.Attributes.Value("error.type")
				out[op.AsString()+"|"+et.AsString()] += dp.Count
			}
		}
	}
	return out
}

func checkCounts(t *testing.T, r *sdkmetric.ManualReader, want map[string]uint64) {
	t.Helper()
	if got := counts(t, r); !maps.Equal(got, want) {
		t.Errorf("recorded\n  %v\nwant\n  %v", got, want)
	}
}

// singleAddr 是单节点 Redis 的地址。和 testredis 用同一个环境变量（这里引用不了 testredis：它依赖 redisx）。
func singleAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("OPPENHEIMER_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("OPPENHEIMER_TEST_REDIS_ADDR is not set in CI")
		}
		t.Skip("OPPENHEIMER_TEST_REDIS_ADDR is not set; run `make up` and `make test`")
	}
	return addr
}

func uniqueScript(t *testing.T) *redis.Script {
	t.Helper()
	var b [8]byte
	_, _ = rand.Read(b[:])
	// 注释让脚本的 SHA 每次不同，第一次 EVALSHA 一定是 NOSCRIPT。
	return redis.NewScript("-- " + hex.EncodeToString(b[:]) + "\nreturn ARGV[1]")
}

// runCommands 发一组命令，返回预期记下的次数。命令的结果本身也检查，确认 hook 没有改变它们。
func runCommands(t *testing.T, c redis.UniversalClient) map[string]uint64 {
	t.Helper()
	ctx := t.Context()
	p := randPrefix(t)
	k, missing := p+"k", p+"missing"
	t.Cleanup(func() { _ = c.Del(context.Background(), k).Err() })

	if err := c.Set(ctx, k, "v", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Get(ctx, k).Result(); err != nil || v != "v" {
		t.Fatalf("Get = %q, %v", v, err)
	}
	if err := c.Get(ctx, missing).Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("Get(missing) = %v, want redis.Nil", err)
	}
	// 管道里有一条 redis.Nil：整个管道记一次 pipeline，不按里面的命令记。
	pipe := c.Pipeline()
	pipe.Get(ctx, k)
	pipe.Get(ctx, missing)
	if _, err := pipe.Exec(ctx); !errors.Is(err, redis.Nil) {
		t.Fatalf("pipeline Exec = %v, want redis.Nil from the second GET", err)
	}
	// 第一次运行：EVALSHA 收到 NOSCRIPT，库自己改发 EVAL。两条各记一次，都不算错误。
	if v, err := uniqueScript(t).Run(ctx, c, []string{k}, "x").Text(); err != nil || v != "x" {
		t.Fatalf("script = %q, %v", v, err)
	}
	return map[string]uint64{
		"set|": 1, "get|": 2, "pipeline|": 1, "evalsha|": 1, "eval|": 1,
	}
}

// 单节点：成功、redis.Nil、管道、事务、脚本、Redis 返回的错误、超时、取消。
func TestCommandMetricsSingle(t *testing.T) {
	client, _, err := newClient(Config{Addr: singleAddr(t), PoolSize: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	m, r := newTestMetrics(t)
	addCommandMetrics(client, m)

	want := runCommands(t, client)

	ctx := t.Context()
	p := randPrefix(t)
	k := p + "s"
	t.Cleanup(func() { _ = client.Del(context.Background(), k).Err() })
	if _, err := client.TxPipelined(ctx, func(tx redis.Pipeliner) error {
		tx.Set(ctx, k, "not-a-number", time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want["multi|"]++
	if err := client.Incr(ctx, k).Err(); err == nil || !strings.Contains(err.Error(), "not an integer") {
		t.Fatalf("Incr on a string = %v, want a Redis error", err)
	}
	want["incr|redis_error"]++

	// 阻塞的命令等不到数据，在调用方的截止时间到的时候返回。
	short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := client.BLPop(short, 5*time.Second, p+"empty").Err(); err == nil {
		t.Fatal("BLPop on an empty list returned without error")
	}
	want["blpop|timeout"]++

	canceled, cancel2 := context.WithCancel(ctx)
	cancel2()
	if err := client.Get(canceled, k).Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get with a canceled ctx = %v, want context.Canceled", err)
	}
	want["get|canceled"]++

	checkCounts(t, r, want)
}

// 连不上的地址：连接错误。不需要 Redis。
func TestCommandMetricsConnectionError(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // 端口关掉，连接被拒绝

	client, _, err := newClient(Config{Addr: addr, PoolSize: 1, MaxRetries: -1, DialTimeout: 200 * time.Millisecond, DialerRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	m, r := newTestMetrics(t)
	addCommandMetrics(client, m)

	if err := client.Get(t.Context(), "k").Err(); err == nil {
		t.Fatal("Get against a closed port succeeded")
	}
	checkCounts(t, r, map[string]uint64{"get|connection": 1})
}

// Cluster 模式下 hook 加在 ClusterClient 上，节点的 Client 不继承它：一条命令只记一次。
// 用单节点的 Redis 冒充一个只有一个节点的集群（ClusterSlots 手工给出拓扑），不需要真的集群环境。
// 同时在节点那一层挂一个计数的 hook，确认命令确实经过了节点的 Client（不是绕过去了才只记一次）。
func TestCommandMetricsClusterRecordsOnce(t *testing.T) {
	addr := singleAddr(t)
	c := redis.NewClusterClient(&redis.ClusterOptions{
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: addr}}}}, nil
		},
		PoolSize:              4,
		ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { _ = c.Close() })
	var nodeCmds, nodePipes atomic.Int64
	c.OnNewNode(func(n *redis.Client) { n.AddHook(countingHook{&nodeCmds, &nodePipes}) })
	if err := instrument(c); err != nil {
		t.Fatal(err)
	}
	m, r := newTestMetrics(t)
	addCommandMetrics(c, m)

	want := runCommands(t, c)
	checkCounts(t, r, want)
	// set、get×2、evalsha、eval 经过节点的 ProcessHook；管道经过节点的 ProcessPipelineHook。
	if n := nodeCmds.Load(); n != 5 {
		t.Errorf("node-level ProcessHook saw %d commands, want 5", n)
	}
	if n := nodePipes.Load(); n < 1 {
		t.Errorf("node-level ProcessPipelineHook saw %d pipelines, want at least 1", n)
	}
}

type countingHook struct{ cmds, pipes *atomic.Int64 }

func (countingHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h countingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		// 建连接时的握手命令也经过 hook；ClusterClient 还会直接在节点上发一次 COMMAND 取命令的元信息
		// （不经过 ClusterClient 的 hook，所以指标里也没有它）。这里只数业务命令。
		if name := cmd.Name(); !isHandshake(name) && name != "command" {
			h.cmds.Add(1)
		}
		return next(ctx, cmd)
	}
}

func (h countingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !isHandshake(cmds[0].Name()) {
			h.pipes.Add(1)
		}
		return next(ctx, cmds)
	}
}

// 真的 Cluster 和 Sentinel（make redis-modes-up；没起就跳过）：同一组命令记的次数和单节点一样。
// Cluster 下键分散在不同的槽，管道会拆到几个节点上发，仍然只记一次 pipeline。
func TestCommandMetricsRealCluster(t *testing.T) {
	c, _, _ := openOptional(t, clusterAddrEnv)
	m, r := newTestMetrics(t)
	addCommandMetrics(c, m)
	want := runCommands(t, c)
	ctx := t.Context()
	p := randPrefix(t)
	keys := make([]string, 20)
	pipe := c.Pipeline()
	for i := range keys {
		keys[i] = fmt.Sprintf("%sk%d", p, i)
		pipe.Set(ctx, keys[i], "v", time.Minute)
	}
	cleanupKeys(t, c, func() []string { return keys })
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	want["pipeline|"]++
	checkCounts(t, r, want)
}

func TestCommandMetricsRealSentinel(t *testing.T) {
	c, _, _ := openOptional(t, sentinelAddrEnv)
	m, r := newTestMetrics(t)
	addCommandMetrics(c, m)
	checkCounts(t, r, runCommands(t, c))
}

// hook 不改变结果：错误原样返回，下一层只调用一次。
func TestMetricsHookPassesThrough(t *testing.T) {
	m, r := newTestMetrics(t)
	h := metricsHook{m}
	boom := errors.New("boom")
	var calls int
	process := h.ProcessHook(func(context.Context, redis.Cmder) error {
		calls++
		return boom
	})
	cmd := redis.NewStringCmd(t.Context(), "get", "k")
	if err := process(t.Context(), cmd); !errors.Is(err, boom) || calls != 1 {
		t.Errorf("ProcessHook returned %v after %d calls, want boom after 1", err, calls)
	}
	calls = 0
	pipeline := h.ProcessPipelineHook(func(context.Context, []redis.Cmder) error {
		calls++
		return redis.Nil
	})
	if err := pipeline(t.Context(), []redis.Cmder{cmd}); !errors.Is(err, redis.Nil) || calls != 1 {
		t.Errorf("ProcessPipelineHook returned %v after %d calls, want redis.Nil after 1", err, calls)
	}
	// 握手命令照常执行，但不记。
	calls = 0
	if err := process(t.Context(), redis.NewStatusCmd(t.Context(), "client", "setinfo", "lib-name", "x")); !errors.Is(err, boom) || calls != 1 {
		t.Errorf("handshake command returned %v after %d calls, want boom after 1", err, calls)
	}
	calls = 0
	if err := pipeline(t.Context(), []redis.Cmder{redis.NewStatusCmd(t.Context(), "select", 1)}); !errors.Is(err, redis.Nil) || calls != 1 {
		t.Errorf("handshake pipeline returned %v after %d calls, want redis.Nil after 1", err, calls)
	}
	checkCounts(t, r, map[string]uint64{"get|_OTHER": 1, "pipeline|": 1})
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want errClass
	}{
		{nil, errNone},
		{redis.Nil, errNone},
		{fmt.Errorf("wrapped: %w", redis.Nil), errNone},
		{redis.ErrNoScript, errNone},
		{redis.ErrPoolTimeout, errPoolTimeout},
		{context.Canceled, errCanceled},
		{context.DeadlineExceeded, errTimeout},
		{fmt.Errorf("read: %w", os.ErrDeadlineExceeded), errTimeout},
		{&net.OpError{Op: "read", Net: "tcp", Err: timeoutErr{}}, errTimeout},
		{&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, errConnection},
		{redis.ErrClosed, errConnection},
		{io.EOF, errConnection},
		{redis.ErrCrossSlot, errRedis},
		{errors.New("something else"), errOther},
	} {
		if got := classify(tc.err); got != tc.want {
			t.Errorf("classify(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

// 命令名的种数有上限，并发地遇到新名字时也守得住（-race 下跑）。
func TestOperationNameLimitConcurrent(t *testing.T) {
	m, r := newTestMetrics(t)
	process := metricsHook{m}.ProcessHook(func(context.Context, redis.Cmder) error { return nil })
	const workers, names = 16, 200
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range names {
				// 各个 goroutine 从不同的位置开始，让新名字的插入互相交错。
				name := fmt.Sprintf("cmd%d", (i+w*13)%names)
				_ = process(t.Context(), redis.NewCmd(t.Context(), name))
			}
		})
	}
	wg.Wait()
	_ = process(t.Context(), redis.NewCmd(t.Context(), strings.Repeat("x", maxOperationNameLen+1)))

	got := counts(t, r)
	var total uint64
	for _, n := range got {
		total += n
	}
	if total != workers*names+1 {
		t.Errorf("recorded %d commands, want %d", total, workers*names+1)
	}
	if len(got) != maxOperationNames+1 {
		t.Errorf("%d distinct label sets, want %d names plus _OTHER", len(got), maxOperationNames)
	}
	if got["_OTHER|"] == 0 {
		t.Error("nothing went to _OTHER")
	}
	// 已经登记的名字之后仍然记在自己名下。
	if n := len(*m.ops.Load()); n != maxOperationNames+2 {
		t.Errorf("%d cached names, want %d plus the two pipeline names", n, maxOperationNames)
	}
}

func benchHook(b *testing.B, err error) {
	m, _ := newTestMetrics(b)
	process := metricsHook{m}.ProcessHook(func(context.Context, redis.Cmder) error { return err })
	ctx := context.Background()
	cmd := redis.NewStringCmd(ctx, "get", "k")
	_ = process(ctx, cmd) // 第一次登记命令名
	b.ReportAllocs()
	for b.Loop() {
		_ = process(ctx, cmd)
	}
}

// 稳定状态下（命令名已经登记过）hook 本身每次调用的耗时和分配。下一层是空函数，不碰网络。
func BenchmarkProcessHook(b *testing.B)    { benchHook(b, nil) }
func BenchmarkProcessHookNil(b *testing.B) { benchHook(b, redis.Nil) }
func BenchmarkProcessHookError(b *testing.B) {
	benchHook(b, &net.OpError{Op: "read", Net: "tcp", Err: timeoutErr{}})
}

func BenchmarkProcessPipelineHook(b *testing.B) {
	m, _ := newTestMetrics(b)
	pipeline := metricsHook{m}.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return nil })
	ctx := context.Background()
	cmds := []redis.Cmder{redis.NewStringCmd(ctx, "get", "a"), redis.NewStringCmd(ctx, "get", "b")}
	b.ReportAllocs()
	for b.Loop() {
		_ = pipeline(ctx, cmds)
	}
}
