package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/loop"
	"github.com/palemoky/oppenheimer/services/agent/internal/report"
	"github.com/palemoky/oppenheimer/services/agent/internal/runlog"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool/kube"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool/loki"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool/prom"
	"github.com/palemoky/oppenheimer/services/agent/internal/trigger"
)

const serviceName = "agent"

// systemPrompt 是编译进去的常量：它在缓存的前缀里，不能带时间、运行号这类每次都变的东西。
//
//go:embed system.md
var systemPrompt string

// 高峰时段的处理方式（-peak）。
const (
	// peakDefer：告警触发的诊断留到高峰结束再跑，手动提问要带 force 才跑。
	peakDefer = "defer"
	// peakRun：不管时段，来了就跑。
	peakRun = "run"
)

const (
	// maxBodyBytes 是一次请求体的上限。
	maxBodyBytes = 1 << 20
	// shutdownTimeout 是停止时等在途的 HTTP 请求做完的最长时间。
	shutdownTimeout = 10 * time.Second
	// pruneEvery 是多久清理一次旧的运行记录。
	pruneEvery = time.Hour
)

type serveConfig struct {
	listen      string
	runsDir     string
	provider    string
	baseURL     string
	model       string
	effort      string
	fallback    bool
	price       string
	peak        string
	dailyBudget float64
	cooldown    time.Duration
	queueSize   int
	keepAge     time.Duration
	keepBytes   int64
	limits      loop.Limits

	prometheusURL string
	lokiURL       string
	kubeconfig    string
	inCluster     bool
}

func serveCmd(args []string, stderr io.Writer) int {
	cfg := serveConfig{limits: loop.DefaultLimits()}
	var logLevel string
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.listen, "listen", ":8080", "address to listen on")
	fs.StringVar(&cfg.runsDir, "runs-dir", "runs", "directory for run records")
	fs.StringVar(&cfg.provider, "provider", envOr("AGENT_PROVIDER", llm.ProviderAnthropic),
		"model backend (default $AGENT_PROVIDER, else anthropic): "+strings.Join(llm.Providers(), ", "))
	fs.StringVar(&cfg.model, "model", os.Getenv("AGENT_MODEL"), "model id (default $AGENT_MODEL, else the provider's default)")
	fs.StringVar(&cfg.baseURL, "base-url", os.Getenv("AGENT_BASE_URL"),
		"API base URL (default $AGENT_BASE_URL, else the provider's own); required for openai-compatible")
	fs.StringVar(&cfg.effort, "effort", "high", "thinking effort (output_config.effort or reasoning_effort); accepted values differ by provider, empty omits it")
	fs.BoolVar(&cfg.fallback, "fallback", true, "enable server-side refusal fallback (beta; anthropic only)")
	fs.StringVar(&cfg.price, "price", os.Getenv("AGENT_PRICE"),
		"USD per million tokens as input,output[,cache_write[,cache_read]] for a model that is not in the built-in table (default $AGENT_PRICE)")
	fs.StringVar(&cfg.peak, "peak", envOr("AGENT_PEAK", peakDefer),
		"what to do during the model's peak pricing hours (default $AGENT_PEAK, else defer): defer holds alert diagnoses until the peak ends and makes /ask require force; run ignores the hours. No effect for models without peak pricing")
	fs.Float64Var(&cfg.dailyBudget, "daily-budget-usd", 20, "stop starting runs once this much was spent in one UTC day")
	fs.DurationVar(&cfg.cooldown, "cooldown", time.Hour, "minimum time between two diagnoses of the same alert group")
	fs.IntVar(&cfg.queueSize, "queue", 5, "how many diagnoses may wait behind the running one")
	fs.DurationVar(&cfg.keepAge, "keep-age", 30*24*time.Hour, "delete run records older than this")
	fs.Int64Var(&cfg.keepBytes, "keep-bytes", 1536<<20, "delete the oldest run records beyond this total size")
	fs.IntVar(&cfg.limits.MaxTurns, "max-turns", cfg.limits.MaxTurns, "model turns per run before wrap-up")
	fs.Float64Var(&cfg.limits.MaxCostUSD, "max-cost-usd", cfg.limits.MaxCostUSD, "cost per run before wrap-up")
	fs.DurationVar(&cfg.limits.MaxDuration, "max-duration", cfg.limits.MaxDuration, "wall time per run before wrap-up")
	toolFlags(fs, &cfg)
	fs.StringVar(&logLevel, "log-level", "", "log level: debug, info, warn, error (default $"+logx.LevelEnv+", else info)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if logLevel == "" {
		logLevel = os.Getenv(logx.LevelEnv)
	}
	logger := logx.New(serviceName, stderr, logx.ParseLevel(logLevel))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdown, err := otelx.Setup(ctx, serviceName, logger)
	if err != nil {
		logger.ErrorContext(ctx, "telemetry setup failed", "error", err)
		return 1
	}
	// 最先注册、最后执行：停止时最后一次指标也要发出去。
	defer otelx.Shutdown(ctx, shutdown, logger)

	if err := serve(ctx, cfg, logger); err != nil {
		logger.ErrorContext(ctx, "agent failed", "error", err)
		return 1
	}
	return 0
}

func serve(ctx context.Context, cfg serveConfig, logger *slog.Logger) error {
	// 凭据从后端自己的环境变量里取（ANTHROPIC_API_KEY、DEEPSEEK_API_KEY 等）。没有凭据不挡启动：
	// webhook 照收、照记，诊断会以 api_error 结束并留下记录。
	if cfg.peak != peakDefer && cfg.peak != peakRun {
		return fmt.Errorf("config: -peak must be %s or %s, got %q", peakDefer, peakRun, cfg.peak)
	}
	opts := llm.Options{Provider: cfg.provider, Model: cfg.model, Effort: cfg.effort, Fallback: cfg.fallback, BaseURL: cfg.baseURL}
	if err := opts.Resolve(os.Getenv); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	cfg.model = opts.Model
	// 名字写错的模型在这里就失败：没有价格就卡不了费用的上限。表里没有的模型要用 -price 显式给，不猜。
	price, err := modelPrice(cfg)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	client, err := llm.New(opts)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	store, err := runlog.Open(cfg.runsDir, logger)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	tools, err := buildTools(cfg)
	if err != nil {
		return fmt.Errorf("tools: %w", err)
	}
	budget := trigger.NewDailyBudget(cfg.dailyBudget, spentToday(store, time.Now(), logger), nil)
	a := &app{
		cfg:    cfg,
		logger: logger,
		store:  store,
		budget: budget,
		runner: &loop.Runner{
			LLM:    client,
			Tools:  tools,
			System: systemPrompt,
			Limits: cfg.limits,
			Price:  price,
		},
		dispatcher: trigger.NewDispatcher(trigger.Config{
			Cooldown: cfg.cooldown, QueueSize: cfg.queueSize, Budget: budget,
			OnSkip: func(reason string) { recordSkip(context.Background(), reason) },
		}),
	}

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.listen, err)
	}
	// 不设 WriteTimeout：/ask 要等一次诊断做完（十分钟上下）。这个端口只在集群里可达。
	srv := &http.Server{Handler: a.mux(), ReadHeaderTimeout: 10 * time.Second}

	var wg sync.WaitGroup
	wg.Go(func() { a.work(ctx) })
	wg.Go(func() { a.pruneLoop(ctx) })
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	if opts.APIKey == "" && cfg.provider != llm.ProviderCompatible {
		logger.WarnContext(ctx, "no API key: every diagnosis will end in api_error", "provider", cfg.provider, "env", llm.KeyEnv(cfg.provider))
	}
	logger.InfoContext(ctx, "agent started", "listen", ln.Addr().String(), "provider", cfg.provider, "model", cfg.model,
		"effort", cfg.effort, "fallback", cfg.fallback && cfg.provider == llm.ProviderAnthropic, "peak", cfg.peak, "peak_pricing", price.Peak != nil, "tools", len(tools.Defs()), "runs_dir", cfg.runsDir)

	select {
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}
	// ctx 已经取消：正在跑的那次诊断会以 canceled 结束并写完记录，等它。
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	err = srv.Shutdown(stopCtx)
	wg.Wait()
	if err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	return nil
}

// modelPrice 返回配置的模型的单价：表里有的用表里的，没有的用 -price 给的。
// 表里有的模型不接受 -price：两个数不一样时不知道该信哪个。
func modelPrice(cfg serveConfig) (llm.Price, error) {
	price, err := llm.PriceOf(cfg.model)
	switch {
	case cfg.price == "":
		if err != nil {
			return llm.Price{}, fmt.Errorf("%w; give it with -price", err)
		}
		return price, nil
	case err == nil:
		return llm.Price{}, fmt.Errorf("model %q has a built-in price, -price is only for models without one", cfg.model)
	}
	return llm.ParsePrice(cfg.price)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// buildTools 登记只读工具。没有给地址（或者没有给 Kubernetes 的连接方式）的那一组不登记；
// 一组都没有时 Agent 只有循环自己加的 submit_report，仍然能启动。
func buildTools(cfg serveConfig) (*tool.Registry, error) {
	// 工具自己不设超时：每次调用的时限由循环经 ctx 给（loop.Limits.ToolTimeout）。
	client := &http.Client{}
	var tools []tool.Tool
	if cfg.prometheusURL != "" {
		tools = append(tools, prom.New(prom.Config{BaseURL: cfg.prometheusURL, HTTP: client})...)
	}
	if cfg.lokiURL != "" {
		tools = append(tools, loki.New(loki.Config{BaseURL: cfg.lokiURL, HTTP: client})...)
	}
	kubeCfg, err := kubeConfig(cfg)
	if err != nil {
		return nil, err
	}
	if kubeCfg != nil {
		cs, err := kubernetes.NewForConfig(kubeCfg)
		if err != nil {
			return nil, fmt.Errorf("kubernetes client: %w", err)
		}
		tools = append(tools, kube.New(kube.Config{Client: cs})...)
	}
	return tool.NewRegistry(tools...)
}

// kubeConfig 返回连 apiserver 的配置；两种方式都没有给时返回 nil（不登记 kube_* 工具）。
// 集群里用 Pod 自己的 ServiceAccount（只有白名单里那些资源的 get、list、watch，清单在 deploy/ 里）；
// 本机调试用 kubeconfig，那个身份的权限通常大得多，能读什么只靠工具里的白名单拦。
func kubeConfig(cfg serveConfig) (*rest.Config, error) {
	switch {
	case cfg.kubeconfig != "":
		c, err := clientcmd.BuildConfigFromFlags("", cfg.kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("kubeconfig %s: %w", cfg.kubeconfig, err)
		}
		return c, nil
	case cfg.inCluster:
		c, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("in-cluster kubernetes config: %w", err)
		}
		return c, nil
	}
	return nil, nil
}

// spentToday 从运行记录里加出今天已经花掉的钱：每天的预算不能因为重启就清零。
func spentToday(store *runlog.Store, now time.Time, logger *slog.Logger) float64 {
	runs, err := store.List()
	if err != nil {
		logger.Warn("cannot read past runs, daily budget starts from zero", "error", err)
		return 0
	}
	var spent float64
	for _, r := range runs {
		if trigger.Day(r.StartedAt) == trigger.Day(now) {
			spent += r.CostUSD
		}
	}
	return spent
}

type app struct {
	// now、after 为 nil 时用 time.Now、time.After；测试里换掉。
	now   func() time.Time
	after func(time.Duration) <-chan time.Time

	cfg        serveConfig
	logger     *slog.Logger
	store      *runlog.Store
	budget     *trigger.DailyBudget
	runner     *loop.Runner
	dispatcher *trigger.Dispatcher
}

func (a *app) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /alerts", a.handleAlerts)
	mux.HandleFunc("POST /ask", a.handleAsk)
	mux.HandleFunc("GET /runs", a.handleRuns)
	mux.HandleFunc("GET /runs/{id}/{file...}", a.handleRunFile)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

// handleAlerts 收 Alertmanager 的 webhook。收到就回 200，诊断在后台做：
// Alertmanager 对 5xx 会重发，对 4xx 不会，所以只有请求体本身不对才回 4xx。
func (a *app) handleAlerts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}
	var wh trigger.Webhook
	if err := json.Unmarshal(body, &wh); err != nil {
		a.logger.WarnContext(ctx, "invalid webhook body", "error", err)
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	reason := a.dispatcher.Offer(&wh, jsontext.Value(body))
	a.logger.InfoContext(ctx, "webhook received", "group_key", wh.GroupKey, "status", wh.Status,
		"alerts", len(wh.Alerts), "queued", reason == "", "skip_reason", reason)
	w.WriteHeader(http.StatusOK)
}

func (a *app) handleAsk(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Question string `json:"question"`
		// Force 为真时在高峰时段也马上跑（按高峰价）。
		Force bool `json:"force"`
	}
	if err := json.UnmarshalRead(http.MaxBytesReader(w, r.Body, maxBodyBytes), &req); err != nil || req.Question == "" {
		http.Error(w, `body must be {"question": "...", "force": false}`, http.StatusBadRequest)
		return
	}
	// 提问的人在等，不能替他留到几个小时以后：说明高峰到什么时候，让他自己决定等还是加 force。
	if until, peak := a.peakUntil(); peak && !req.Force {
		w.Header().Set("Retry-After", strconv.Itoa(int(until.Sub(a.clock()).Seconds())+1))
		http.Error(w, fmt.Sprintf("peak pricing hours for %s until %s; ask again after that, or set force to run now at the peak price",
			a.cfg.model, until.Format(time.RFC3339)), http.StatusConflict)
		return
	}
	reply, err := a.dispatcher.Ask(r.Context(), req.Question)
	switch {
	case errors.Is(err, trigger.ErrBusy):
		http.Error(w, err.Error(), http.StatusTooManyRequests)
	case err != nil:
		// 提问的人断开了，没有人在读响应。
	default:
		writeJSON(w, reply)
	}
}

func (a *app) handleRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := a.store.List()
	if err != nil {
		a.logger.ErrorContext(r.Context(), "list runs failed", "error", err)
		http.Error(w, "cannot list runs", http.StatusInternalServerError)
		return
	}
	writeJSON(w, runs)
}

func (a *app) handleRunFile(w http.ResponseWriter, r *http.Request) {
	// 路径经 os.Root 打开，出不了运行记录的目录。
	b, err := a.store.ReadFile(r.PathValue("id"), r.PathValue("file"))
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	// 文件里有模型写的和从日志里来的内容：一律按纯文本给，并且不让浏览器猜类型。
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(b) //nolint:gosec // G705：按 text/plain 加 nosniff 输出，不会被当成 HTML 执行
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.MarshalWrite(w, v)
}

// work 是唯一的工作协程：同一时刻只跑一次诊断。
func (a *app) clock() time.Time {
	if a.now != nil {
		return a.now()
	}
	return time.Now()
}

// peakUntil 回答现在是不是该躲开的高峰时段（配置成 defer，而且这个模型分时段计价），是的话返回高峰结束的时刻。
func (a *app) peakUntil() (time.Time, bool) {
	if a.cfg.peak != peakDefer {
		return time.Time{}, false
	}
	return a.runner.Price.PeakUntil(a.clock())
}

// work 是唯一的工作协程：同一时刻只跑一次诊断。
//
// 高峰时段里收到的告警诊断不跑，留在 held 里，高峰结束时按收到的顺序跑。留着的不占队列：
// 带 force 的手动提问照样能马上跑。held 只在内存里，进程重启就没了（和去重的状态一样）。
// 代价是诊断晚了几个小时：指标和日志还在，但 Pod、事件这些当时的状态可能已经没了，见设计 3d 第 10 节。
func (a *app) work(ctx context.Context) {
	after := a.after
	if after == nil {
		after = time.After
	}
	var held []trigger.Job
	var wake <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
			wake = nil
			jobs := held
			held = nil
			for _, job := range jobs {
				if ctx.Err() != nil {
					return
				}
				job.Question += fmt.Sprintf("\n\nNote: this diagnosis was held back during peak pricing hours. "+
					"The time above is when the alert notification arrived; the current time is now %s. "+
					"The alert may have resolved since: check its current state first, and query the time range around when it fired.",
					a.clock().UTC().Format(time.RFC3339))
				a.runJob(ctx, job)
			}
		case job := <-a.dispatcher.Jobs():
			until, peak := a.peakUntil()
			if !peak || job.Kind != trigger.KindAlert {
				a.runJob(ctx, job)
				continue
			}
			if len(held) >= a.cfg.queueSize {
				recordSkip(ctx, trigger.SkipPeakFull)
				a.logger.WarnContext(ctx, "alert diagnosis dropped: too many already held for off-peak hours", "held", len(held))
				continue
			}
			held = append(held, job)
			a.logger.InfoContext(ctx, "alert diagnosis held until peak pricing hours end", "until", until, "held", len(held))
			if wake == nil {
				wake = after(until.Sub(a.clock()))
			}
		}
	}
}

// noRecord 在建不了运行目录时顶替运行记录：诊断照做，只是没有留痕。
type noRecord struct{}

func (noRecord) Event(string, any)  {}
func (noRecord) Raw(string, []byte) {}

func (a *app) runJob(ctx context.Context, job trigger.Job) {
	// 排队期间预算可能已经被前面的运行用完了。
	if a.budget.Exhausted() {
		recordSkip(ctx, trigger.SkipDailyBudget)
		a.logger.WarnContext(ctx, "run skipped: daily budget exhausted", "trigger", job.Kind)
		if job.Reply != nil {
			job.Reply <- trigger.Reply{Outcome: trigger.SkipDailyBudget}
		}
		return
	}
	started := time.Now()
	id := runlog.NewID(started)
	run, err := a.store.Create(runlog.Meta{
		ID: id, Trigger: job.Kind, Question: job.Question, Alert: job.Alert,
		Provider: a.cfg.provider, Model: a.cfg.model, Effort: a.cfg.effort, Limits: a.cfg.limits.Record(),
		Version: version(), Prompt: promptHash(), StartedAt: started.UTC(),
	})
	var rec loop.Recorder = noRecord{}
	if err != nil {
		a.logger.ErrorContext(ctx, "cannot create run record, running without one", "run", id, "error", err)
	} else {
		rec = run
	}

	res := a.runner.Run(ctx, job.Question, rec)
	a.budget.Add(res.CostUSD)
	recordRun(ctx, job.Kind, &res)

	calls := make(map[string]int, len(res.Calls))
	total := 0
	for name, byResult := range res.Calls {
		for _, n := range byResult {
			calls[name] += n
			total += n
		}
	}
	md := report.Render(res.Report, res.Steps, report.Stats{
		RunID: id, Question: job.Question, Outcome: string(res.Outcome), Unverified: res.Unverified,
		LimitHit: res.LimitHit, Model: res.Model, Turns: res.Turns, ToolCalls: total,
		CostUSD: res.CostUSD, Duration: res.Duration.Round(time.Second).String(),
	})
	if run != nil {
		if res.Report != nil {
			if b, err := json.Marshal(res.Report, jsontext.WithIndent("  ")); err == nil {
				run.WriteFile("report.json", b)
			}
		}
		run.WriteFile("report.md", md)
		sum := runlog.Summary{
			ID: id, Trigger: job.Kind, Outcome: string(res.Outcome), Unverified: res.Unverified,
			LimitHit: res.LimitHit, Turns: res.Turns, ToolCalls: calls, Usage: res.Usage,
			CostUSD: res.CostUSD, StartedAt: started.UTC(), DurationMS: res.Duration.Milliseconds(),
		}
		if res.Err != nil {
			sum.Error = res.Err.Error()
		}
		run.Finish(sum)
	}

	attrs := []any{
		"run", id, "trigger", job.Kind, "outcome", string(res.Outcome), "turns", res.Turns,
		"tool_calls", total, "cost_usd", res.CostUSD, "cache_read_tokens", res.Usage.CacheRead,
		"duration", res.Duration.String(),
	}
	if res.Err != nil {
		a.logger.ErrorContext(ctx, "run finished", append(attrs, "error", res.Err)...)
	} else {
		a.logger.InfoContext(ctx, "run finished", attrs...)
	}
	if job.Reply != nil {
		job.Reply <- trigger.Reply{RunID: id, Outcome: string(res.Outcome), Report: string(md)}
	}
}

func (a *app) pruneLoop(ctx context.Context) {
	ticker := time.NewTicker(pruneEvery)
	defer ticker.Stop()
	for {
		if n, err := a.store.Prune(time.Now(), a.cfg.keepAge, a.cfg.keepBytes); err != nil {
			a.logger.WarnContext(ctx, "prune run records failed", "error", err)
		} else if n > 0 {
			a.logger.InfoContext(ctx, "pruned run records", "removed", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func promptHash() string {
	sum := sha256.Sum256([]byte(systemPrompt))
	return hex.EncodeToString(sum[:])
}

// version 返回构建时的提交号，取不到时是 unknown。
func version() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				return s.Value
			}
		}
	}
	return "unknown"
}
