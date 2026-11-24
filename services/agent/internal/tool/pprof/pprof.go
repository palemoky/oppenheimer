// Package pprof 是诊断 Agent 取被诊断服务剖析的工具 pprof_capture（设计 4a 的 3.4）。
//
// 边界：
//   - 只连配置的命名空间（默认 oppenheimer）里 Pod 回环上的调试端口（pkg/app 的 DEBUG_ADDR），
//     经 Kubernetes 的端口转发；只发固定路径的 GET（kinds 里那五个），不开采样、不改被诊断的进程。
//   - CPU 剖析有开销，不算纯只读：同一个 Pod 同时只有一个，两次之间至少隔 cpuGap；一次运行最多 maxPerRun 次。
//     超了返回一句说明，不报错。
//   - 给模型的是摘要（summary.go），原始剖析（.pb.gz）落在运行记录的 raw/ 里，人拿去 go tool pprof。
package pprof

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/pprof/profile"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// Name 是工具名。
const Name = "pprof_capture"

const (
	// DefaultNamespace 是默认允许的命名空间：六个服务所在的那个。
	DefaultNamespace = "oppenheimer"
	// DefaultPort 是调试端口（deploy/charts/service 的 DEBUG_ADDR）。端口来自 ConfigMap 的 envFrom，
	// Agent 读不了 ConfigMap，所以不从 Pod 上发现，按配置来。
	DefaultPort = 6060

	defaultCPUSeconds = 10
	maxCPUSeconds     = 30
	// cpuGap 是同一个 Pod 两次 CPU 剖析之间的最短间隔，从上一次结束算起。
	cpuGap = 60 * time.Second
	// maxPerRun 是一次运行最多抓几次（各种类型合计）。
	maxPerRun = 8
	// maxProfileBytes 是一份剖析的上限。Agent 的内存上限是 256Mi，解析会把它放大几倍。
	maxProfileBytes = 16 << 20
	// overhead 是剖析本身之外（端口转发、传输）的时间预算；非 CPU 的剖析只有这一段。
	overhead = 15 * time.Second
	// podGetTimeout 是确认 Pod 在跑的那一次 apiserver 调用的时限。
	podGetTimeout = 5 * time.Second
)

// kindSpec 是一种剖析：调试端口上的固定路径、默认看哪个样本类型。
type kindSpec struct {
	path   string
	sample string
}

// kinds 是允许的剖析。路径是常量，模型给的参数只决定选哪一项和 CPU 的秒数。
var kinds = map[string]kindSpec{
	"cpu":       {path: "/debug/pprof/profile", sample: "cpu"},
	"goroutine": {path: "/debug/pprof/goroutine", sample: "goroutine"},
	"heap":      {path: "/debug/pprof/heap", sample: "inuse_space"},
	"mutex":     {path: "/debug/pprof/mutex", sample: "delay"},
	"block":     {path: "/debug/pprof/block", sample: "delay"},
}

var kindNames = []string{"cpu", "goroutine", "heap", "mutex", "block"}

var heapSamples = []string{"inuse_space", "inuse_objects", "alloc_space", "alloc_objects"}

// 抓取的结果，指标的标签。
const (
	resultOK      = "ok"
	resultError   = "error"
	resultLimited = "limited"
)

// 标签只有类型（5 个）和结果（3 个）。Pod 名进日志，不进标签。
var (
	meter    = otelx.Meter("github.com/palemoky/oppenheimer/services/agent/internal/tool/pprof")
	captures = otelx.Counter(meter, "agent.pprof.captures", "pprof_capture calls by profile type and result (ok, error, limited).")
)

// Config 是工具的依赖。
type Config struct {
	// Pods 用来在转发前确认 Pod 存在、在跑。
	Pods kubernetes.Interface
	// Forwarder 连到 Pod 回环上的调试端口。
	Forwarder Forwarder
	// Namespace 是唯一允许的命名空间，空的用 DefaultNamespace。
	Namespace string
	// Port 是 Pod 回环上的调试端口，0 用 DefaultPort。
	Port int
	// Logger 记每次抓取，nil 时不记。
	Logger *slog.Logger
	// Now 为 nil 时用 time.Now。
	Now func() time.Time
}

// New 返回 pprof_capture 工具。可以并发调用。
func New(cfg Config) tool.Tool {
	if cfg.Namespace == "" {
		cfg.Namespace = DefaultNamespace
	}
	if cfg.Port == 0 {
		cfg.Port = DefaultPort
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &capturer{
		cfg:      cfg,
		cpu:      &cpuLimiter{now: cfg.Now, pods: map[string]*cpuState{}},
		fallback: tool.NewScope(""),
	}
}

type capturer struct {
	cfg Config
	// cpu 是按 Pod 的 CPU 剖析的并发和间隔，进程级（跨运行）：被诊断的 Pod 不管是哪次运行来抓的。
	cpu *cpuLimiter
	// fallback 是不在一次运行里（agent tool 手工调用）时用的共享状态，进程级。
	fallback  *tool.Scope
	manualSeq atomic.Int64
}

func (c *capturer) Def() llm.ToolDef {
	return llm.ToolDef{
		Name: Name,
		Description: fmt.Sprintf("Capture a pprof profile from one Go service pod in namespace %s (through the pod's loopback debug port) and return a summary: "+
			"for cpu, heap, mutex and block the top functions by flat and cum with percentages and the top call stacks; for goroutine the goroutines grouped by call stack with counts and inferred wait state. "+
			"Pass base (the step id of an earlier capture of the same type in this run) to get what grew between the two instead. "+
			"cpu samples for `seconds` and has a real cost on the pod: one at a time per pod, at least %ds apart per pod. "+
			"mutex and block are only populated when the service samples them (off by default); they and heap alloc_* are cumulative since process start, so compare two captures with base to see the recent window. "+
			"At most %d captures per run. The raw profile is kept in the run record for go tool pprof.",
			c.cfg.Namespace, int(cpuGap.Seconds()), maxPerRun),
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"pod":  map[string]any{"type": "string", "description": "Pod name (not a deployment name; list pods with kube_get). Namespace is fixed to " + c.cfg.Namespace + "."},
				"type": map[string]any{"type": "string", "enum": kindNames, "description": "Which profile."},
				"seconds": map[string]any{
					"type": "integer", "minimum": 1, "maximum": maxCPUSeconds,
					"description": fmt.Sprintf("cpu only: how long to sample, default %d, max %d.", defaultCPUSeconds, maxCPUSeconds),
				},
				"base": map[string]any{"type": "string", "description": "Step id of an earlier pprof_capture of the same type in this run (for example s4); the result shows the growth since then."},
				"heap_sample": map[string]any{
					"type": "string", "enum": heapSamples,
					"description": "heap only: which value to look at, default inuse_space (bytes in use at the last GC). alloc_* count every allocation since process start.",
				},
			},
			"required":             []string{"pod", "type"},
			"additionalProperties": false,
		},
	}
}

// Timeout 实现 tool.Timeouter：最长的 CPU 剖析加上转发和传输的时间。
func (c *capturer) Timeout() time.Duration { return maxCPUSeconds*time.Second + overhead }

type input struct {
	Pod        string `json:"pod"`
	Type       string `json:"type"`
	Seconds    int    `json:"seconds,omitzero"`
	Base       string `json:"base,omitzero"`
	HeapSample string `json:"heap_sample,omitzero"`
}

// request 是校验过的参数。
type request struct {
	pod     string
	kind    string
	spec    kindSpec
	sample  string
	seconds int
	base    string
}

func (c *capturer) parse(raw jsontext.Value) (request, error) {
	var in input
	if err := tool.Decode(raw, &in); err != nil {
		return request{}, err
	}
	r := request{pod: strings.TrimSpace(in.Pod), kind: in.Type, base: strings.TrimSpace(in.Base)}
	if ns, name, ok := strings.Cut(r.pod, "/"); ok {
		if ns != c.cfg.Namespace {
			return request{}, fmt.Errorf("only pods in namespace %s can be profiled", c.cfg.Namespace)
		}
		r.pod = name
	}
	if r.pod == "" {
		return request{}, errors.New("pod is required: give a pod name in namespace " + c.cfg.Namespace + " (list them with kube_get)")
	}
	if errs := validation.IsDNS1123Subdomain(r.pod); len(errs) > 0 {
		return request{}, fmt.Errorf("pod %q is not a valid pod name", r.pod)
	}
	spec, ok := kinds[r.kind]
	if !ok {
		return request{}, fmt.Errorf("type must be one of %s", strings.Join(kindNames, ", "))
	}
	r.spec, r.sample = spec, spec.sample
	switch {
	case r.kind == "cpu":
		r.seconds = in.Seconds
		if r.seconds == 0 {
			r.seconds = defaultCPUSeconds
		}
		if r.seconds < 1 || r.seconds > maxCPUSeconds {
			return request{}, fmt.Errorf("seconds must be between 1 and %d", maxCPUSeconds)
		}
	case in.Seconds != 0:
		return request{}, errors.New("seconds only applies to type cpu")
	}
	switch {
	case r.kind == "heap" && in.HeapSample != "":
		if !slices.Contains(heapSamples, in.HeapSample) {
			return request{}, fmt.Errorf("heap_sample must be one of %s", strings.Join(heapSamples, ", "))
		}
		r.sample = in.HeapSample
	case in.HeapSample != "":
		return request{}, errors.New("heap_sample only applies to type heap")
	}
	return r, nil
}

// path 是调试端口上的请求路径：常量路径，CPU 的加上秒数。
func (r request) path() string {
	if r.kind == "cpu" {
		return fmt.Sprintf("%s?seconds=%d", r.spec.path, r.seconds)
	}
	return r.spec.path
}

func (c *capturer) Call(ctx context.Context, raw jsontext.Value) (res tool.Result, err error) {
	req, err := c.parse(raw)
	if err != nil {
		return tool.Result{}, err
	}
	scope, step := tool.CallFrom(ctx)
	if scope == nil {
		scope = c.fallback
	}
	if step == "" {
		step = fmt.Sprintf("c%d", c.manualSeq.Add(1))
	}
	rs := scope.Value(runStateKey{}, newRunState).(*runState)
	began := c.cfg.Now()

	result := resultError
	var size int
	defer func() {
		if err == nil && result == resultError {
			result = resultOK
		}
		captures.Add(ctx, 1, metric.WithAttributes(attribute.String("type", req.kind), attribute.String("result", result)))
		attrs := []any{
			"run", scope.RunID, "step", step, "namespace", c.cfg.Namespace, "pod", req.pod, "type", req.kind,
			"result", result, "bytes", size, "duration_ms", c.cfg.Now().Sub(began).Milliseconds(),
		}
		if req.kind == "cpu" {
			attrs = append(attrs, "seconds", req.seconds)
		}
		if req.base != "" {
			attrs = append(attrs, "base", req.base)
		}
		switch {
		case err != nil:
			c.cfg.Logger.WarnContext(ctx, "pprof capture failed", append(attrs, "error", err)...)
		case result == resultLimited:
			c.cfg.Logger.InfoContext(ctx, "pprof capture refused by limit", append(attrs, "reason", res.Text)...)
		default:
			c.cfg.Logger.InfoContext(ctx, "pprof capture", attrs...)
		}
	}()
	limited := func(msg string) (tool.Result, error) {
		result = resultLimited
		return tool.Result{Text: msg}, nil
	}

	var base *capture
	if req.base != "" {
		base, err = rs.lookup(req.base)
		if err != nil {
			return tool.Result{}, err
		}
		if base.kind != req.kind || base.sample != req.sample {
			return tool.Result{}, fmt.Errorf("base %s is a %s profile; base must be a capture of the same type", req.base, base.describe())
		}
	}

	// 一次运行的名额先占上，失败或者被别的上限挡住时退回。
	if !rs.reserve() {
		return limited(fmt.Sprintf("Not captured: this run already took %d profiles, the per-run limit. Work with the captures you have (%s).",
			maxPerRun, strings.Join(rs.ids(), ", ")))
	}
	kept := false
	defer func() {
		if !kept {
			rs.refund()
		}
	}()

	if req.kind == "cpu" {
		release, why := c.cpu.acquire(c.cfg.Namespace + "/" + req.pod)
		if release == nil {
			return limited("Not captured: " + why)
		}
		defer release()
	}

	if err := c.checkPod(ctx, req.pod); err != nil {
		return tool.Result{}, err
	}

	budget := overhead
	if req.kind == "cpu" {
		budget += time.Duration(req.seconds) * time.Second
	}
	fetchCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	data, err := c.cfg.Forwarder.Get(fetchCtx, c.cfg.Namespace, req.pod, c.cfg.Port, req.path(), maxProfileBytes)
	if err != nil {
		if se, ok := errors.AsType[*StatusError](err); ok && req.kind == "cpu" && strings.Contains(se.Body, "already in use") {
			// 有人（另一个 Agent 进程、手工的 go tool pprof）正在对这个进程做 CPU 剖析：一个进程同时只能有一个。
			return limited("Not captured: another CPU profile of this pod is already running (started outside this agent). Try again later or profile another replica.")
		}
		return tool.Result{}, c.fetchError(err, req)
	}
	size = len(data)
	p, err := profile.ParseData(data)
	if err != nil {
		return tool.Result{}, fmt.Errorf("the debug port of pod %s returned something that is not a pprof profile: %w", req.pod, err)
	}

	cur := &capture{id: step, pod: req.pod, kind: req.kind, sample: req.sample, seconds: req.seconds, data: data, at: began}
	text, err := c.summarize(req, cur, p, base)
	if err != nil {
		return tool.Result{}, err
	}
	rs.store(cur)
	kept = true
	return tool.Result{Text: text, Raw: data, RawExt: ".pb.gz"}, nil
}

// checkPod 确认 Pod 在允许的命名空间里存在、在跑。不在跑的 Pod 转发会失败，错误信息远不如这里清楚。
func (c *capturer) checkPod(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, podGetTimeout)
	defer cancel()
	pod, err := c.cfg.Pods.CoreV1().Pods(c.cfg.Namespace).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return fmt.Errorf("pod %q not found in namespace %s; list pods with kube_get", name, c.cfg.Namespace)
	case apierrors.IsForbidden(err):
		return fmt.Errorf("the agent is not permitted to read pods in namespace %s", c.cfg.Namespace)
	case err != nil:
		return fmt.Errorf("reading pod %q: %w", name, err)
	case pod.Status.Phase != corev1.PodRunning:
		return fmt.Errorf("pod %q is %s; only a running pod can be profiled", name, pod.Status.Phase)
	}
	return nil
}

// fetchError 把取剖析的错误翻译成写给模型看的话。超时保留 context.DeadlineExceeded（循环据此记成 timeout）。
func (c *capturer) fetchError(err error, req request) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("profiling pod %s timed out: %w", req.pod, err)
	}
	if se, ok := errors.AsType[*StatusError](err); ok {
		return fmt.Errorf("the debug port of pod %s answered HTTP %d (%s)", req.pod, se.Code, se.Body)
	}
	if errors.Is(err, errTooLarge) {
		return fmt.Errorf("the %s profile of pod %s is larger than %d MiB and was not read", req.kind, req.pod, maxProfileBytes>>20)
	}
	if strings.Contains(err.Error(), "connection refused") {
		return fmt.Errorf("nothing listens on port %d inside pod %s: its debug port (DEBUG_ADDR) is probably off, or it is not one of the Go services", c.cfg.Port, req.pod)
	}
	return fmt.Errorf("could not reach the debug port of pod %s: %w", req.pod, err)
}

func (c *capturer) summarize(req request, cur *capture, p *profile.Profile, base *capture) (string, error) {
	idx, err := sampleIndex(p, req.sample)
	if err != nil {
		return "", err
	}
	unit := unitOf(p, idx)
	st := aggregate(p, idx, 1)

	var b strings.Builder
	fmt.Fprintf(&b, "pprof %s of pod %s/%s, captured %s", cur.describe(), c.cfg.Namespace, req.pod, cur.at.UTC().Format(time.RFC3339))
	b.WriteString(". Raw profile: raw/" + cur.id + ".pb.gz in this run's record.\n")

	if base != nil {
		bp, err := profile.ParseData(base.data)
		if err != nil {
			return "", fmt.Errorf("re-reading base %s: %w", base.id, err)
		}
		bidx, err := sampleIndex(bp, req.sample)
		if err != nil {
			return "", err
		}
		scale := 1.0
		if req.kind == "cpu" && bp.DurationNanos > 0 && p.DurationNanos > 0 && bp.DurationNanos != p.DurationNanos {
			scale = float64(p.DurationNanos) / float64(bp.DurationNanos)
		}
		bst := aggregate(bp, bidx, scale)
		fmt.Fprintf(&b, "Compared with %s (pod %s, captured %s earlier).", base.id, base.pod, cur.at.Sub(base.at).Round(time.Second))
		if base.pod != req.pod {
			b.WriteString(" Different pods: the difference is between two processes, not growth over time.")
		}
		if scale != 1 {
			fmt.Fprintf(&b, " The base was sampled for %ds; its values are scaled to %ds.", base.seconds, req.seconds)
		}
		b.WriteString("\n")
		if req.kind == "goroutine" {
			writeGoroutineDiff(&b, st, bst)
		} else {
			writeDiff(&b, st, bst, unit, req.sample)
		}
		return b.String(), nil
	}

	switch req.kind {
	case "goroutine":
		writeGoroutines(&b, st)
		return b.String(), nil
	case "cpu":
		fmt.Fprintf(&b, "Total cpu %s", format(st.total, unit))
		if p.DurationNanos > 0 {
			fmt.Fprintf(&b, " over %s (%.2f cores on average)", time.Duration(p.DurationNanos).Round(time.Millisecond), float64(st.total)/float64(p.DurationNanos))
		}
		b.WriteString(".\n")
	case "heap":
		fmt.Fprintf(&b, "Total %s %s. ", req.sample, format(st.total, unit))
		if strings.HasPrefix(req.sample, "inuse") {
			b.WriteString("In-use values are as of the most recent GC and are sampled estimates. One snapshot's biggest entries are often normal caches; capture again later with base to see what grows.\n")
		} else {
			b.WriteString("alloc_* values are cumulative since the process started, not recent; compare two captures with base for a window.\n")
		}
	case "mutex", "block":
		if st.total == 0 {
			env := "DEBUG_MUTEX_FRACTION"
			if req.kind == "block" {
				env = "DEBUG_BLOCK_RATE"
			}
			fmt.Fprintf(&b, "The profile is empty. Most likely the service does not sample %s events (%s unset, which is the default); "+
				"it can also mean nothing was contended since sampling began. An empty profile is NOT evidence that there is no contention.\n", req.kind, env)
			return b.String(), nil
		}
		fmt.Fprintf(&b, "Total %s %s. Values are cumulative since the process started sampling, not recent; capture again later with base to see the recent window.\n",
			req.sample, format(st.total, unit))
	}
	if st.total == 0 {
		b.WriteString("The profile has no samples.\n")
		return b.String(), nil
	}
	writeFlat(&b, st, unit, req.sample)
	return b.String(), nil
}

// capture 是这次运行里成功的一次抓取。data 留着给后面带 base 的调用（压缩的原始剖析，比解析后的小得多）。
type capture struct {
	id      string
	pod     string
	kind    string
	sample  string
	seconds int
	data    []byte
	at      time.Time
}

func (c *capture) describe() string {
	switch c.kind {
	case "cpu":
		return fmt.Sprintf("cpu profile (%ds)", c.seconds)
	case "heap":
		return "heap profile (" + c.sample + ")"
	}
	return c.kind + " profile"
}

type runStateKey struct{}

// runState 是一次运行里的抓取：名额和成功的那几次。
type runState struct {
	mu       sync.Mutex
	reserved int
	captures map[string]*capture
	order    []string
}

func newRunState() any { return &runState{captures: map[string]*capture{}} }

func (rs *runState) reserve() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.reserved >= maxPerRun {
		return false
	}
	rs.reserved++
	return true
}

func (rs *runState) refund() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.reserved--
}

func (rs *runState) store(c *capture) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.captures[c.id] = c
	rs.order = append(rs.order, c.id)
}

func (rs *runState) lookup(id string) (*capture, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if c, ok := rs.captures[id]; ok {
		return c, nil
	}
	if len(rs.order) == 0 {
		return nil, fmt.Errorf("base %s: there is no earlier successful pprof_capture in this run; capture once without base first", id)
	}
	return nil, fmt.Errorf("base %s is not a successful pprof_capture of this run (those are %s)", id, strings.Join(rs.order, ", "))
}

func (rs *runState) ids() []string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return slices.Clone(rs.order)
}

// cpuLimiter 管每个 Pod 的 CPU 剖析：同时只有一个，上一次结束后 cpuGap 之内不再开始。
type cpuLimiter struct {
	now  func() time.Time
	mu   sync.Mutex
	pods map[string]*cpuState
}

type cpuState struct {
	busy    bool
	lastEnd time.Time
}

// acquire 为 pod 开始一次 CPU 剖析。可以开始时返回 release（剖析结束时调用，成功失败都要调）；
// 不能时返回 nil 和写给模型看的原因。
func (l *cpuLimiter) acquire(pod string) (release func(), why string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	// 顺手清掉早就过了间隔的 Pod，免得 Pod 换了一茬又一茬之后表一直长。
	for k, s := range l.pods {
		if !s.busy && now.Sub(s.lastEnd) >= cpuGap {
			delete(l.pods, k)
		}
	}
	s, ok := l.pods[pod]
	switch {
	case ok && s.busy:
		return nil, "a CPU profile of this pod is already running. Only one CPU profile per pod at a time; use its result, or profile another replica."
	case ok:
		wait := cpuGap - now.Sub(s.lastEnd)
		return nil, fmt.Sprintf("the last CPU profile of this pod ended %s ago; CPU profiles of the same pod must be at least %s apart (about %s more). "+
			"Use the earlier result, profile another replica, or take a cheaper goroutine or heap profile meanwhile.",
			now.Sub(s.lastEnd).Round(time.Second), cpuGap, wait.Round(time.Second))
	}
	s = &cpuState{busy: true}
	l.pods[pod] = s
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		s.busy = false
		s.lastEnd = l.now()
	}, ""
}
