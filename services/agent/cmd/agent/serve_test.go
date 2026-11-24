package main

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/loop"
	"github.com/palemoky/oppenheimer/services/agent/internal/runlog"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
	"github.com/palemoky/oppenheimer/services/agent/internal/trigger"
)

// fakeModel 每段对话都走同一个剧本：先查一次，再交报告。
type fakeModel struct {
	mu        sync.Mutex
	questions []string
}

func (f *fakeModel) NewConversation(string, []llm.ToolDef) llm.Conversation {
	return &fakeConversation{model: f}
}

type fakeConversation struct {
	model *fakeModel
	n     int
}

func (c *fakeConversation) Send(_ context.Context, turn llm.Turn) (*llm.Reply, error) {
	c.n++
	usage := llm.Usage{Input: 1000, Output: 100}
	if c.n == 1 {
		c.model.mu.Lock()
		c.model.questions = append(c.model.questions, turn.Text)
		c.model.mu.Unlock()
		return &llm.Reply{
			Model: "claude-opus-5-5", Stop: llm.StopToolUse, Usage: usage, Thinking: []string{"check up"},
			Calls: []llm.ToolCall{{ID: "t1", Name: "echo", Input: jsontext.Value(`{"query":"up"}`)}},
		}, nil
	}
	return &llm.Reply{
		Model: "claude-opus-5-5", Stop: llm.StopToolUse, Usage: usage,
		Calls: []llm.ToolCall{{ID: "t2", Name: "submit_report", Input: jsontext.Value(
			`{"summary":"db down","root_cause":"mysql pod gone","confidence":"high",` +
				`"evidence":[{"claim":"up is 0","steps":["s1"]}],"ruled_out":[],"next_checks":[],"suggested_actions":[]}`)}},
	}, nil
}

type echoTool struct{}

func (echoTool) Def() llm.ToolDef { return llm.ToolDef{Name: "echo", Schema: map[string]any{}} }
func (echoTool) Call(_ context.Context, input jsontext.Value) (tool.Result, error) {
	return tool.Result{Text: "up{job=\"mysql\"} 0", Raw: []byte(input)}, nil
}

func newTestApp(t *testing.T, dailyBudget float64, opts ...func(*app)) (*app, *fakeModel, *httptest.Server) {
	t.Helper()
	// 日志打到测试的输出里：留痕写失败只打日志，测试失败时要看得到。
	logger := slog.New(slog.NewTextHandler(t.Output(), nil))
	store, err := runlog.Open(t.TempDir(), logger)
	if err != nil {
		t.Fatal(err)
	}
	tools, err := tool.NewRegistry(echoTool{})
	if err != nil {
		t.Fatal(err)
	}
	price, _ := llm.PriceOf("claude-opus-5-5")
	model := &fakeModel{}
	budget := trigger.NewDailyBudget(dailyBudget, 0, nil)
	a := &app{
		cfg:    serveConfig{model: "claude-opus-5-5", effort: "high", runsDir: store.Dir(), limits: loop.DefaultLimits(), queueSize: 2},
		logger: logger, store: store, budget: budget,
		runner:     &loop.Runner{LLM: model, Tools: tools, System: systemPrompt, Limits: loop.DefaultLimits(), Price: price},
		dispatcher: trigger.NewDispatcher(trigger.Config{Cooldown: time.Hour, QueueSize: 2, Budget: budget}),
	}
	for _, opt := range opts {
		opt(a)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { a.work(ctx); close(done) }()
	srv := httptest.NewServer(a.mux())
	t.Cleanup(func() {
		srv.Close()
		cancel()
		<-done
		_ = store.Close()
	})
	return a, model, srv
}

func post(t *testing.T, url, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

const webhookBody = `{"version":"4","receiver":"webhook","status":"firing","groupKey":"{}:{alertname=\"MySQLDown\"}",
"alerts":[{"status":"firing","fingerprint":"abc","startsAt":"2026-11-24T03:00:00Z",
"labels":{"alertname":"MySQLDown","severity":"critical","instance":"mysql-order-0"},
"annotations":{"summary":"MySQL is down","runbook_url":"https://example.invalid/mysql.md"}}]}`

func TestAlertToReportEndToEnd(t *testing.T) {
	a, model, srv := newTestApp(t, 20)

	for _, body := range []string{"{not json", strings.Repeat("x", maxBodyBytes+1)} {
		if code, _ := post(t, srv.URL+"/alerts", body); code/100 != 4 {
			t.Errorf("bad body: status %d, want 4xx", code)
		}
	}
	if code, _ := post(t, srv.URL+"/alerts", webhookBody); code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	// 同一组马上再来一次：照样 200，但不再诊断。
	if code, _ := post(t, srv.URL+"/alerts", webhookBody); code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}

	var runs []runlog.Summary
	deadline := time.Now().Add(10 * time.Second)
	for {
		runs, _ = a.store.List()
		if len(runs) == 1 && runs[0].Outcome != runlog.OutcomeIncomplete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not finish: %+v", runs)
		}
		time.Sleep(10 * time.Millisecond)
	}
	run := runs[0]
	if run.Outcome != "reported" || run.Trigger != trigger.KindAlert || run.Turns != 2 || run.ToolCalls["echo"] != 1 || run.CostUSD <= 0 {
		t.Errorf("summary = %+v", run)
	}
	if len(model.questions) != 1 || !strings.Contains(model.questions[0], "Alert 1: MySQLDown") ||
		!strings.Contains(model.questions[0], `instance="mysql-order-0"`) {
		t.Errorf("questions = %q", model.questions)
	}

	code, list := get(t, srv.URL+"/runs")
	if code != http.StatusOK || !strings.Contains(list, run.ID) {
		t.Errorf("GET /runs = %d %s", code, list)
	}
	code, md := get(t, srv.URL+"/runs/"+run.ID+"/report.md")
	if code != http.StatusOK || !strings.Contains(md, "mysql pod gone") || !strings.Contains(md, `up{job="mysql"} 0`) {
		t.Errorf("report.md = %d\n%s", code, md)
	}
	if code, raw := get(t, srv.URL+"/runs/"+run.ID+"/raw/s1.json"); code != http.StatusOK || raw != `{"query":"up"}` {
		t.Errorf("raw = %d %q", code, raw)
	}
	if code, _ := get(t, srv.URL+"/runs/"+run.ID+"/nope"); code != http.StatusNotFound {
		t.Errorf("missing file: status %d", code)
	}
	var meta runlog.Meta
	_, metaJSON := get(t, srv.URL+"/runs/"+run.ID+"/meta.json")
	if err := json.Unmarshal([]byte(metaJSON), &meta); err != nil || meta.Prompt != promptHash() || !strings.Contains(string(meta.Alert), "MySQLDown") {
		t.Errorf("meta = %+v, err = %v", meta, err)
	}

	// 回放：只读磁盘，按顺序看得到模型的思考、调用、工具的结果和结局。
	var out, errOut bytes.Buffer
	if code := replayCmd([]string{"-runs-dir", a.cfg.runsDir, run.ID}, &out, &errOut); code != 0 {
		t.Fatalf("replay exit %d: %s", code, errOut.String())
	}
	for _, want := range []string{
		"turn 1", "Alert 1: MySQLDown", "[thinking]", "check up", `→ echo {"query":"up"}`,
		`[s1] echo {"query":"up"} → ok`, `up{job="mysql"} 0`, "→ submit_report", "end: reported",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("replay lacks %q:\n%s", want, out.String())
		}
	}
	out.Reset()
	if code := replayCmd([]string{"-runs-dir", a.cfg.runsDir}, &out, &errOut); code != 0 || !strings.Contains(out.String(), run.ID) {
		t.Errorf("replay list: exit %d, out %q", code, out.String())
	}
}

func TestAskEndToEnd(t *testing.T) {
	a, _, srv := newTestApp(t, 20)
	if code, _ := post(t, srv.URL+"/ask", `{}`); code != http.StatusBadRequest {
		t.Errorf("empty question: status %d", code)
	}
	reply, err := ask(t.Context(), srv.URL, "is anything wrong?", false)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Outcome != "reported" || reply.RunID == "" || !strings.Contains(reply.Report, "is anything wrong?") {
		t.Errorf("reply = %+v", reply)
	}
	// 重启后的预算从运行记录里加出来。
	if spent := spentToday(a.store, time.Now(), a.logger); spent <= 0 {
		t.Errorf("spentToday = %v", spent)
	}
}

func TestAskOverBudget(t *testing.T) {
	_, _, srv := newTestApp(t, 0)
	if _, err := ask(t.Context(), srv.URL, "q", false); err == nil || !strings.Contains(err.Error(), "429") {
		t.Errorf("err = %v, want 429", err)
	}
}

func TestModelPrice(t *testing.T) {
	deepseek, _ := llm.PriceOf("deepseek-v4-pro")
	for _, tc := range []struct {
		name, model, flag string
		want              llm.Price
		wantErr           bool
	}{
		{name: "from the table", model: "deepseek-v4-pro", want: deepseek},
		{name: "unknown model needs -price", model: "qwen-local", wantErr: true},
		{name: "unknown model with -price", model: "qwen-local", flag: "0,0", want: llm.Price{}},
		{name: "-price must not shadow the table", model: "deepseek-v4-pro", flag: "1,1", wantErr: true},
		{name: "bad -price", model: "qwen-local", flag: "cheap", wantErr: true},
	} {
		got, err := modelPrice(serveConfig{model: tc.model, price: tc.flag})
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("%s: price = %+v, err = %v", tc.name, got, err)
		}
	}
}

// 高峰时段（-peak=defer，模型分时段计价）：告警的诊断留着不跑，高峰结束才跑；手动提问被拒，带 force 才跑。
func TestPeakHoursDeferAlertsAndGateAsk(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 11, 24, 7, 0, 0, 0, time.UTC) // 周二，DeepSeek 的高峰是 06:00 到 10:00
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	wake := make(chan time.Time)
	waits := make(chan time.Duration, 1)
	a, model, srv := newTestApp(t, 20, func(a *app) {
		price, err := llm.PriceOf("deepseek-flash")
		if err != nil {
			t.Fatal(err)
		}
		a.runner.Price = price
		a.cfg.model, a.cfg.peak = "deepseek-flash", peakDefer
		a.now = clock
		a.after = func(d time.Duration) <-chan time.Time { waits <- d; return wake }
	})
	asked := func() int { model.mu.Lock(); defer model.mu.Unlock(); return len(model.questions) }

	if code, _ := post(t, srv.URL+"/alerts", webhookBody); code != http.StatusOK {
		t.Fatalf("webhook status = %d", code)
	}
	// 工作协程收下了这一个并定了闹钟：等到 10:00，也就是 3 小时。
	if d := <-waits; d != 3*time.Hour {
		t.Errorf("wait = %s, want 3h", d)
	}
	if n := asked(); n != 0 {
		t.Fatalf("model was called %d times during peak hours", n)
	}

	code, body := post(t, srv.URL+"/ask", `{"question":"anything wrong?"}`)
	if code != http.StatusConflict || !strings.Contains(body, "2026-11-24T10:00:00Z") {
		t.Errorf("ask during peak = %d %q", code, body)
	}
	if code, body := post(t, srv.URL+"/ask", `{"question":"anything wrong?","force":true}`); code != http.StatusOK || !strings.Contains(body, "reported") {
		t.Errorf("forced ask = %d %q", code, body)
	}
	if n := asked(); n != 1 {
		t.Fatalf("model calls after the forced ask = %d, want 1 (the held alert must still be waiting)", n)
	}

	// 高峰结束：留着的那一个现在跑，问题里写明了它是被推迟的、现在几点。
	mu.Lock()
	now = time.Date(2026, 11, 24, 10, 0, 1, 0, time.UTC)
	mu.Unlock()
	wake <- now
	deadline := time.Now().Add(10 * time.Second)
	for asked() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the held diagnosis did not run after the peak ended")
		}
		time.Sleep(10 * time.Millisecond)
	}
	model.mu.Lock()
	q := model.questions[1]
	model.mu.Unlock()
	if !strings.Contains(q, "MySQLDown") || !strings.Contains(q, "held back during peak pricing hours") || !strings.Contains(q, "2026-11-24T10:00:01Z") {
		t.Errorf("deferred question = %q", q)
	}
	// 低谷：提问不用 force。
	if code, _ := post(t, srv.URL+"/ask", `{"question":"and now?"}`); code != http.StatusOK {
		t.Errorf("ask off-peak = %d", code)
	}
	_ = a
}
