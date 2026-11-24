package pprof

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	rpprof "runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// httpForwarder 顶替端口转发：直接对 httptest 起的调试端口（app.DebugMux，和服务里的是同一个）发请求，
// 记下每次请求的命名空间、Pod、端口和路径。
type httpForwarder struct {
	base string
	mu   sync.Mutex
	reqs []string
}

func (f *httpForwarder) Get(ctx context.Context, ns, pod string, port int, path string, limit int64) ([]byte, error) {
	f.mu.Lock()
	f.reqs = append(f.reqs, fmt.Sprintf("%s/%s:%d%s", ns, pod, port, path))
	f.mu.Unlock()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.base+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	return readBody(resp, limit)
}

// fwdFunc 是可以自己决定返回什么的转发器（测上限时用）。
type fwdFunc func(ctx context.Context, pod, path string) ([]byte, error)

func (f fwdFunc) Get(ctx context.Context, _, pod string, _ int, path string, _ int64) ([]byte, error) {
	return f(ctx, pod, path)
}

func runningPod(name string) *corev1.Pod {
	return &corev1.Pod{
		Name: name, Namespace: DefaultNamespace,
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func pods() *fake.Clientset {
	pending := runningPod("order-pending")
	pending.Status.Phase = corev1.PodPending
	other := runningPod("redis-0")
	other.Namespace = "redis"
	return fake.NewClientset(runningPod("order-1"), runningPod("order-2"), pending, other)
}

// debugServer 起一个和服务里一样的调试端口，返回顶替端口转发的 Forwarder。
func debugServer(t *testing.T) *httpForwarder {
	t.Helper()
	srv := httptest.NewServer(app.DebugMux())
	t.Cleanup(srv.Close)
	return &httpForwarder{base: srv.URL}
}

func newTool(fwd Forwarder, now func() time.Time) *capturer {
	return New(Config{Pods: pods(), Forwarder: fwd, Now: now}).(*capturer)
}

// inRun 返回一个模拟循环里第 step 步的 ctx。
func inRun(ctx context.Context, scope *tool.Scope, step string) context.Context {
	return tool.WithCall(ctx, scope, step)
}

func mustCapture(ctx context.Context, t *testing.T, c *capturer, input string) tool.Result {
	t.Helper()
	res, err := c.Call(ctx, jsontext.Value(input))
	if err != nil {
		t.Fatalf("Call(%s): %v", input, err)
	}
	return res
}

func wantContains(t *testing.T, text string, subs ...string) {
	t.Helper()
	for _, s := range subs {
		if !strings.Contains(text, s) {
			t.Errorf("result does not contain %q:\n%s", s, text)
		}
	}
}

// 下面几个 *ForTest 函数是测试里制造剖析内容的地方，名字独特，摘要里找得到。

//go:noinline
func parkedForTest(ch <-chan struct{}, ready *sync.WaitGroup) {
	ready.Done()
	<-ch
}

//go:noinline
func leakedForTest(ch <-chan struct{}, ready *sync.WaitGroup) {
	ready.Done()
	<-ch
}

func startGoroutines(t *testing.T, n int, fn func(<-chan struct{}, *sync.WaitGroup)) {
	t.Helper()
	ch := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(n)
	for range n {
		go fn(ch, &ready)
	}
	ready.Wait()
	t.Cleanup(func() { close(ch) })
	// 等它们真的停在 chan receive 上：Done 之后到 park 之间还有几条指令。
	time.Sleep(20 * time.Millisecond)
}

func TestGoroutineSummaryAndBase(t *testing.T) {
	fwd := debugServer(t)
	c := newTool(fwd, nil)
	scope := tool.NewScope("run-1")

	startGoroutines(t, 50, parkedForTest)
	first := mustCapture(inRun(t.Context(), scope, "s1"), t, c, `{"pod":"order-1","type":"goroutine"}`)
	t.Log(first.Text) // go test -v 看模型拿到的是什么
	wantContains(t, first.Text,
		"goroutine profile of pod oppenheimer/order-1",
		"raw/s1.pb.gz",
		"50 goroutines",
		"state: chan receive",
		"parkedForTest",
	)
	if first.RawExt != ".pb.gz" || len(first.Raw) == 0 {
		t.Fatalf("raw = %d bytes, ext %q; want the profile as .pb.gz", len(first.Raw), first.RawExt)
	}
	if _, err := profile.ParseData(first.Raw); err != nil {
		t.Fatalf("raw is not a profile: %v", err)
	}
	// 运行时帧（gopark、chanrecv）不占摘要的帧数，最里面给的是业务的帧。
	_, block, _ := strings.Cut(first.Text, "50 goroutines")
	block, _, _ = strings.Cut(block, "#2")
	if strings.Contains(block, "runtime.gopark") {
		t.Errorf("runtime frames not dropped:\n%s", block)
	}

	startGoroutines(t, 40, leakedForTest)
	second := mustCapture(inRun(t.Context(), scope, "s2"), t, c, `{"pod":"order-1","type":"goroutine","base":"s1"}`)
	t.Log(second.Text)
	wantContains(t, second.Text, "Compared with s1", "+40 (0 -> 40), state: chan receive", "leakedForTest")
	// 增长最多的是新的那一组，原来的 50 个没变，不该出现在增长里。
	if strings.Contains(second.Text, "parkedForTest") {
		t.Errorf("unchanged stack listed as growth:\n%s", second.Text)
	}

	if got := fwd.reqs; len(got) != 2 || got[0] != "oppenheimer/order-1:6060/debug/pprof/goroutine" {
		t.Errorf("requests = %q", got)
	}
}

var retained [][]byte

//go:noinline
func retainForTest(n int) {
	for range n {
		retained = append(retained, make([]byte, 1<<20))
	}
}

func TestHeapSummaryAndBase(t *testing.T) {
	c := newTool(debugServer(t), nil)
	scope := tool.NewScope("")
	t.Cleanup(func() { retained = nil })

	runtime.GC()
	first := mustCapture(inRun(t.Context(), scope, "s1"), t, c, `{"pod":"order-1","type":"heap"}`)
	wantContains(t, first.Text, "heap profile (inuse_space)", "Top ", "flat%", "cum%", "Top 3 call stacks", "capture again later with base")

	retainForTest(32)
	runtime.GC()
	runtime.GC()
	second := mustCapture(inRun(t.Context(), scope, "s2"), t, c, `{"pod":"order-1","type":"heap","base":"s1"}`)
	t.Log(second.Text)
	wantContains(t, second.Text, "Compared with s1", "functions by growth", "retainForTest", "MiB")
	// 增长最多的就是它：第一行数据里。
	lines := strings.Split(second.Text, "\n")
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "growth") && i+1 < len(lines) {
			if !strings.Contains(lines[i+1], "retainForTest") {
				t.Errorf("top growth is %q, want retainForTest", lines[i+1])
			}
			break
		}
	}

	// base 必须是同一种剖析（heap 的不同样本类型也算不同）。
	if _, err := c.Call(inRun(t.Context(), scope, "s3"), jsontext.Value(`{"pod":"order-1","type":"heap","heap_sample":"alloc_space","base":"s1"}`)); err == nil {
		t.Error("base of another heap sample type accepted")
	}
}

//go:noinline
func spinForTest(stop *atomic.Bool) int {
	n := 0
	for !stop.Load() {
		for i := range 1000 {
			n += i * i
		}
	}
	return n
}

func TestCPUSummary(t *testing.T) {
	c := newTool(debugServer(t), nil)
	var stop atomic.Bool
	done := make(chan int)
	go func() { done <- spinForTest(&stop) }()
	defer func() { stop.Store(true); <-done }()

	res := mustCapture(inRun(t.Context(), tool.NewScope(""), "s1"), t, c, `{"pod":"order-1","type":"cpu","seconds":1}`)
	t.Log(res.Text)
	wantContains(t, res.Text, "cpu profile (1s)", "cores on average", "Top ", "spinForTest")
}

func TestMutexEmptyIsExplained(t *testing.T) {
	if runtime.SetMutexProfileFraction(-1) != 0 {
		t.Skip("mutex sampling is on in this process")
	}
	c := newTool(debugServer(t), nil)
	res := mustCapture(t.Context(), t, c, `{"pod":"order-1","type":"mutex"}`)
	wantContains(t, res.Text, "The profile is empty", "DEBUG_MUTEX_FRACTION", "NOT evidence")
}

func TestBadArguments(t *testing.T) {
	c := newTool(debugServer(t), nil)
	scope := tool.NewScope("")
	mustCapture(inRun(t.Context(), scope, "s1"), t, c, `{"pod":"order-1","type":"goroutine"}`)
	for _, tt := range []struct{ input, want string }{
		{`{"type":"goroutine"}`, "pod is required"},
		{`{"pod":"order-1","type":"trace"}`, "type must be one of"},
		{`{"pod":"order-1","type":"cpu","seconds":31}`, "between 1 and 30"},
		{`{"pod":"order-1","type":"heap","seconds":5}`, "only applies to type cpu"},
		{`{"pod":"order-1","type":"cpu","heap_sample":"inuse_space"}`, "only applies to type heap"},
		{`{"pod":"redis/redis-0","type":"goroutine"}`, "only pods in namespace oppenheimer"},
		{`{"pod":"redis-0","type":"goroutine"}`, "not found in namespace oppenheimer"},
		{`{"pod":"Order_1","type":"goroutine"}`, "not a valid pod name"},
		{`{"pod":"order-pending","type":"goroutine"}`, "is Pending"},
		{`{"pod":"order-1","type":"goroutine","base":"s9"}`, "not a successful pprof_capture of this run (those are s1)"},
		{`{"pod":"order-1","type":"heap","base":"s1"}`, "base must be a capture of the same type"},
		{`{"pod":"order-1","type":"goroutine","path":"/debug/pprof/cmdline"}`, "invalid arguments"},
	} {
		_, err := c.Call(inRun(t.Context(), scope, "s2"), jsontext.Value(tt.input))
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("Call(%s) error = %v, want it to contain %q", tt.input, err, tt.want)
		}
	}
	// namespace/pod 的写法在允许的命名空间里是可以的。
	mustCapture(inRun(t.Context(), scope, "s3"), t, c, `{"pod":"oppenheimer/order-1","type":"goroutine"}`)
}

// cannedProfile 是一个手写的、只有一个样本的剖析，测上限时让转发器立刻返回。
func cannedProfile(t *testing.T, sample, unit string) []byte {
	t.Helper()
	fn := &profile.Function{ID: 1, Name: "main.work", Filename: "main.go"}
	loc := &profile.Location{ID: 1, Line: []profile.Line{{Function: fn, Line: 10}}}
	p := &profile.Profile{
		SampleType:    []*profile.ValueType{{Type: sample, Unit: unit}},
		Sample:        []*profile.Sample{{Location: []*profile.Location{loc}, Value: []int64{1000}}},
		Location:      []*profile.Location{loc},
		Function:      []*profile.Function{fn},
		DurationNanos: int64(time.Second),
	}
	var buf bytes.Buffer
	if err := p.Write(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func cannedForwarder(t *testing.T) fwdFunc {
	cpu := cannedProfile(t, "cpu", "nanoseconds")
	g := cannedProfile(t, "goroutine", "count")
	return func(_ context.Context, _, path string) ([]byte, error) {
		if strings.HasPrefix(path, "/debug/pprof/profile") {
			return cpu, nil
		}
		return g, nil
	}
}

func TestRunLimit(t *testing.T) {
	var failNext atomic.Bool
	canned := cannedForwarder(t)
	c := newTool(fwdFunc(func(ctx context.Context, pod, path string) ([]byte, error) {
		if failNext.Swap(false) {
			return nil, errors.New("connection reset")
		}
		return canned(ctx, pod, path)
	}), nil)
	scope := tool.NewScope("")

	// 失败的那次不占名额。
	failNext.Store(true)
	if _, err := c.Call(inRun(t.Context(), scope, "s0"), jsontext.Value(`{"pod":"order-1","type":"goroutine"}`)); err == nil {
		t.Fatal("failing forwarder did not fail")
	}
	// 同一轮里并发的调用也不能超过名额。
	var wg sync.WaitGroup
	var ok, limited atomic.Int32
	for i := range maxPerRun + 4 {
		wg.Go(func() {
			res, err := c.Call(inRun(t.Context(), scope, fmt.Sprintf("s%d", i+1)), jsontext.Value(`{"pod":"order-1","type":"goroutine"}`))
			switch {
			case err != nil:
				t.Errorf("call %d: %v", i, err)
			case strings.HasPrefix(res.Text, "Not captured: this run already took 8 profiles"):
				limited.Add(1)
			default:
				ok.Add(1)
			}
		})
	}
	wg.Wait()
	if ok.Load() != maxPerRun || limited.Load() != 4 {
		t.Errorf("ok = %d, limited = %d; want %d and 4", ok.Load(), limited.Load(), maxPerRun)
	}
	// 另一次运行有自己的名额。
	res := mustCapture(inRun(t.Context(), tool.NewScope(""), "s1"), t, c, `{"pod":"order-1","type":"goroutine"}`)
	if strings.HasPrefix(res.Text, "Not captured") {
		t.Errorf("a new run is limited: %s", res.Text)
	}
}

func TestCPUOnePerPodAtATime(t *testing.T) {
	canned := cannedForwarder(t)
	started := make(chan struct{})
	release := make(chan struct{})
	c := newTool(fwdFunc(func(ctx context.Context, pod, path string) ([]byte, error) {
		if pod == "order-1" && strings.HasPrefix(path, "/debug/pprof/profile") {
			close(started)
			<-release
		}
		return canned(ctx, pod, path)
	}), nil)

	first := make(chan error, 1)
	go func() {
		_, err := c.Call(inRun(t.Context(), tool.NewScope("a"), "s1"), jsontext.Value(`{"pod":"order-1","type":"cpu"}`))
		first <- err
	}()
	<-started

	// 另一次运行（也是同一个进程）对同一个 Pod：被挡住，不报错。
	res := mustCapture(inRun(t.Context(), tool.NewScope("b"), "s1"), t, c, `{"pod":"order-1","type":"cpu"}`)
	wantContains(t, res.Text, "Not captured: a CPU profile of this pod is already running")
	// 别的 Pod、同一个 Pod 的别的剖析不受影响。
	if res := mustCapture(inRun(t.Context(), tool.NewScope("b"), "s2"), t, c, `{"pod":"order-2","type":"cpu"}`); strings.HasPrefix(res.Text, "Not captured") {
		t.Errorf("other pod limited: %s", res.Text)
	}
	if res := mustCapture(inRun(t.Context(), tool.NewScope("b"), "s3"), t, c, `{"pod":"order-1","type":"goroutine"}`); strings.HasPrefix(res.Text, "Not captured") {
		t.Errorf("goroutine profile limited: %s", res.Text)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestCPUGapPerPod(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 11, 24, 3, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}
	c := newTool(cannedForwarder(t), clock)
	scope := tool.NewScope("")
	cpu := `{"pod":"order-1","type":"cpu","seconds":5}`

	mustCapture(inRun(t.Context(), scope, "s1"), t, c, cpu)
	advance(30 * time.Second)
	res := mustCapture(inRun(t.Context(), scope, "s2"), t, c, cpu)
	wantContains(t, res.Text, "Not captured: the last CPU profile of this pod ended 30s ago", "about 30s more")
	advance(31 * time.Second)
	res = mustCapture(inRun(t.Context(), scope, "s3"), t, c, cpu)
	if strings.HasPrefix(res.Text, "Not captured") {
		t.Errorf("CPU profile 61s after the last one limited: %s", res.Text)
	}
	// 被挡住的那次不占这次运行的名额：s1、s3 两次。
	if got := scope.Value(runStateKey{}, newRunState).(*runState).ids(); len(got) != 2 {
		t.Errorf("captures = %v, want s1 and s3", got)
	}
}

// 另一方（手工的 go tool pprof）正在剖析同一个进程时，net/http/pprof 返回 500 "already in use"：
// 当成上限说明，不当成错误。
func TestCPUAlreadyInUseElsewhere(t *testing.T) {
	c := newTool(debugServer(t), nil)
	if err := rpprof.StartCPUProfile(&bytes.Buffer{}); err != nil {
		t.Skipf("cannot start a CPU profile: %v", err)
	}
	defer rpprof.StopCPUProfile()
	res := mustCapture(inRun(t.Context(), tool.NewScope(""), "s1"), t, c, `{"pod":"order-1","type":"cpu","seconds":1}`)
	wantContains(t, res.Text, "Not captured: another CPU profile of this pod is already running")
}

func TestTimeoutCoversLongestCPUProfile(t *testing.T) {
	c := newTool(cannedForwarder(t), nil)
	if got := tool.TimeoutOf(c, 15*time.Second); got <= maxCPUSeconds*time.Second {
		t.Errorf("timeout = %v, want more than the longest CPU profile (%ds)", got, maxCPUSeconds)
	}
}

func TestReadBodyLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing" {
			http.Error(w, "Unknown profile", http.StatusNotFound)
			return
		}
		_, _ = w.Write(make([]byte, 2048))
	}))
	defer srv.Close()
	fwd := &httpForwarder{base: srv.URL}
	if _, err := fwd.Get(t.Context(), "", "", 0, "/big", 1024); !errors.Is(err, errTooLarge) {
		t.Errorf("over-limit body error = %v, want errTooLarge", err)
	}
	_, err := fwd.Get(t.Context(), "", "", 0, "/missing", 1024)
	if se, ok := errors.AsType[*StatusError](err); !ok || se.Code != http.StatusNotFound || se.Body != "Unknown profile" {
		t.Errorf("non-200 error = %v, want StatusError 404", err)
	}
}
