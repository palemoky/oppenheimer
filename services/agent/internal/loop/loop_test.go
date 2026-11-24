package loop

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/report"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// scripted 按脚本回放模型的回复，并记下每一轮收到的输入。
type scripted struct {
	t       *testing.T
	replies []func(turn llm.Turn) (*llm.Reply, error)
	turns   []llm.Turn
	defs    []llm.ToolDef
}

func (s *scripted) NewConversation(_ string, tools []llm.ToolDef) llm.Conversation {
	s.defs = tools
	return s
}

func (s *scripted) Send(ctx context.Context, turn llm.Turn) (*llm.Reply, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i := len(s.turns)
	s.turns = append(s.turns, turn)
	if i >= len(s.replies) {
		s.t.Fatalf("model called %d times, script has %d replies", i+1, len(s.replies))
	}
	return s.replies[i](turn)
}

func fixed(r llm.Reply) func(llm.Turn) (*llm.Reply, error) {
	return func(llm.Turn) (*llm.Reply, error) {
		if r.Model == "" {
			r.Model = "claude-opus-5-5"
		}
		return &r, nil
	}
}

func call(id, name, input string) llm.ToolCall {
	return llm.ToolCall{ID: id, Name: name, Input: jsontext.Value(input)}
}

func calls(cs ...llm.ToolCall) func(llm.Turn) (*llm.Reply, error) {
	return fixed(llm.Reply{Stop: llm.StopToolUse, Calls: cs, Usage: llm.Usage{Input: 1000, Output: 100}})
}

func reportInput(steps ...string) string {
	quoted := make([]string, len(steps))
	for i, s := range steps {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf(`{"summary":"s","root_cause":"rc","confidence":"medium",`+
		`"evidence":[{"claim":"c","steps":[%s]}],"ruled_out":[],"next_checks":[],"suggested_actions":[]}`,
		strings.Join(quoted, ","))
}

// fakeTool 是一个可以配置行为的工具。
type fakeTool struct {
	name string
	fn   func(ctx context.Context, input jsontext.Value) (tool.Result, error)
	n    atomic.Int32
}

func (f *fakeTool) Def() llm.ToolDef { return llm.ToolDef{Name: f.name, Schema: map[string]any{}} }

func (f *fakeTool) Call(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	f.n.Add(1)
	if f.fn == nil {
		return tool.Result{Text: "ok " + string(input), Raw: []byte(`{"raw":true}`)}, nil
	}
	return f.fn(ctx, input)
}

// memRecorder 把事件记在内存里。
type memRecorder struct {
	mu    sync.Mutex
	kinds []string
	raw   map[string][]byte
}

func (m *memRecorder) Event(kind string, _ any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kinds = append(m.kinds, kind)
}

func (m *memRecorder) Raw(file string, raw []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.raw == nil {
		m.raw = make(map[string][]byte)
	}
	m.raw[file] = raw
}

func (m *memRecorder) count(kind string) int {
	n := 0
	for _, k := range m.kinds {
		if k == kind {
			n++
		}
	}
	return n
}

func newRunner(t *testing.T, s *scripted, tools ...tool.Tool) *Runner {
	t.Helper()
	reg, err := tool.NewRegistry(tools...)
	if err != nil {
		t.Fatal(err)
	}
	price, err := llm.PriceOf("claude-opus-5-5")
	if err != nil {
		t.Fatal(err)
	}
	s.t = t
	return &Runner{LLM: s, Tools: reg, System: "sys", Limits: DefaultLimits(), Price: price}
}

func TestRunReportsWithParallelCalls(t *testing.T) {
	a, b := &fakeTool{name: "a"}, &fakeTool{name: "b"}
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
		calls(call("t1", "a", `{"q":1}`), call("t2", "b", `{"q":2}`)),
		calls(call("t3", report.ToolName, reportInput("s1", "s2"))),
	}}
	rec := &memRecorder{}
	res := newRunner(t, s, a, b).Run(t.Context(), "why", rec)

	if res.Outcome != OutcomeReported || res.Report == nil || res.Unverified {
		t.Fatalf("outcome = %s, report = %v, unverified = %v", res.Outcome, res.Report, res.Unverified)
	}
	if res.Turns != 2 {
		t.Errorf("turns = %d, want 2", res.Turns)
	}
	// 同一轮的两个结果必须在同一个 Turn 里，顺序和调用一致，开头带步骤号。
	got := s.turns[1].Results
	if len(got) != 2 || got[0].CallID != "t1" || got[1].CallID != "t2" {
		t.Fatalf("results = %+v", got)
	}
	if !strings.HasPrefix(got[0].Content, "[s1]\n") || !strings.HasPrefix(got[1].Content, "[s2]\n") {
		t.Errorf("results lack step ids: %q, %q", got[0].Content, got[1].Content)
	}
	if s.turns[0].Text != "why" {
		t.Errorf("first turn = %q", s.turns[0].Text)
	}
	// 交报告的工具排在最后，并且是 strict 的。
	last := s.defs[len(s.defs)-1]
	if last.Name != report.ToolName || !last.Strict {
		t.Errorf("last tool def = %+v", last)
	}
	if res.Calls["a"][CallOK] != 1 || res.Calls["b"][CallOK] != 1 {
		t.Errorf("calls = %v", res.Calls)
	}
	if string(rec.raw["s1.json"]) != `{"raw":true}` {
		t.Errorf("raw s1 = %q", rec.raw["s1.json"])
	}
	wantCost := 2 * (1000*4 + 100*20) * 1e-6
	if diff := res.CostUSD - wantCost; diff > 1e-12 || diff < -1e-12 {
		t.Errorf("cost = %v, want %v", res.CostUSD, wantCost)
	}
	if rec.count("model_response") != 2 || rec.count("tool_call") != 2 || rec.count("end") != 1 {
		t.Errorf("events = %v", rec.kinds)
	}
}

func TestRunReportMayCiteStepsFromSameTurn(t *testing.T) {
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
		calls(call("t1", "a", `{}`), call("t2", report.ToolName, reportInput("s1"))),
	}}
	res := newRunner(t, s, &fakeTool{name: "a"}).Run(t.Context(), "q", &memRecorder{})
	if res.Outcome != OutcomeReported {
		t.Fatalf("outcome = %s", res.Outcome)
	}
}

func TestRunToolErrorsGoBackToModel(t *testing.T) {
	boom := &fakeTool{name: "boom", fn: func(context.Context, jsontext.Value) (tool.Result, error) {
		return tool.Result{}, errors.New("bad PromQL: unexpected token")
	}}
	crash := &fakeTool{name: "crash", fn: func(context.Context, jsontext.Value) (tool.Result, error) {
		panic("nil map")
	}}
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
		calls(call("t1", "boom", `{}`), call("t2", "crash", `{}`), call("t3", "nope", `{}`)),
		// 引用出错的步骤会被打回。
		calls(call("t4", report.ToolName, reportInput("s1"))),
		calls(call("t5", "ok", `{}`)),
		calls(call("t6", report.ToolName, reportInput("s3"))),
	}}
	rec := &memRecorder{}
	res := newRunner(t, s, boom, crash, &fakeTool{name: "ok"}).Run(t.Context(), "q", rec)

	r := s.turns[1].Results
	if !r[0].IsError || !strings.Contains(r[0].Content, "bad PromQL") {
		t.Errorf("error result = %+v", r[0])
	}
	if !r[1].IsError || !strings.Contains(r[1].Content, "tool crashed") {
		t.Errorf("panic result = %+v", r[1])
	}
	if !r[2].IsError || !strings.Contains(r[2].Content, "Unknown tool") {
		t.Errorf("unknown tool result = %+v", r[2])
	}
	rej := s.turns[2].Results[0]
	if !rej.IsError || !strings.Contains(rej.Content, `"s1", which returned an error`) {
		t.Errorf("rejection = %+v", rej)
	}
	if res.Outcome != OutcomeReported || res.Unverified {
		t.Fatalf("outcome = %s, unverified = %v", res.Outcome, res.Unverified)
	}
	if rec.count("report_rejected") != 1 {
		t.Errorf("report_rejected events = %d", rec.count("report_rejected"))
	}
	// 未知的工具不占步骤号：s1、s2 是 boom、crash，s3 是 ok。
	if res.Steps["s3"].Tool != "ok" || len(res.Steps) != 3 {
		t.Errorf("steps = %v", res.Steps)
	}
}

func TestRunAcceptsSecondBadReportAsUnverified(t *testing.T) {
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
		calls(call("t1", report.ToolName, reportInput("s9"))),
		calls(call("t2", report.ToolName, reportInput("s9"))),
	}}
	res := newRunner(t, s).Run(t.Context(), "q", &memRecorder{})
	if res.Outcome != OutcomeReported || !res.Unverified || res.Report == nil {
		t.Fatalf("outcome = %s, unverified = %v", res.Outcome, res.Unverified)
	}
}

func TestRunUnparseableReportIsNeverAccepted(t *testing.T) {
	bad := calls(call("t", report.ToolName, `{"summary":`))
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){bad, bad, fixed(llm.Reply{Stop: llm.StopEndTurn}), fixed(llm.Reply{Stop: llm.StopEndTurn})}}
	res := newRunner(t, s).Run(t.Context(), "q", &memRecorder{})
	if res.Outcome != OutcomeNoReport || res.Report != nil {
		t.Fatalf("outcome = %s, report = %v", res.Outcome, res.Report)
	}
}

func TestRunNudgesOnceThenGivesUp(t *testing.T) {
	end := fixed(llm.Reply{Stop: llm.StopEndTurn, Text: "I think it is the database."})
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){end, end}}
	res := newRunner(t, s).Run(t.Context(), "q", &memRecorder{})
	if res.Outcome != OutcomeNoReport || res.Turns != 2 {
		t.Fatalf("outcome = %s, turns = %d", res.Outcome, res.Turns)
	}
	if s.turns[1].Text != nudgeText {
		t.Errorf("second turn = %q", s.turns[1].Text)
	}
}

func TestRunTurnLimitWrapsUp(t *testing.T) {
	a := &fakeTool{name: "a"}
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
		calls(call("t1", "a", `{}`)),
		calls(call("t2", "a", `{}`)),
		// 收尾的那一轮它还想查：不执行，报告照收。
		calls(call("t3", "a", `{}`), call("t4", report.ToolName, reportInput("s2"))),
	}}
	r := newRunner(t, s, a)
	r.Limits.MaxTurns = 2
	rec := &memRecorder{}
	res := r.Run(t.Context(), "q", rec)

	if res.Outcome != OutcomeReportedTruncated || res.LimitHit != LimitTurns {
		t.Fatalf("outcome = %s, limit = %s", res.Outcome, res.LimitHit)
	}
	if a.n.Load() != 2 {
		t.Errorf("tool executed %d times, want 2", a.n.Load())
	}
	// 说明附在带着工具结果的那一轮后面。
	wrap := s.turns[2]
	if len(wrap.Results) != 1 || !strings.Contains(wrap.Text, "turn limit") {
		t.Errorf("wrap-up turn = %+v", wrap)
	}
	if rec.count("limit_hit") != 1 {
		t.Errorf("limit_hit events = %d", rec.count("limit_hit"))
	}
}

func TestRunWrapUpChancesRunOut(t *testing.T) {
	a := &fakeTool{name: "a"}
	q := calls(call("t", "a", `{}`))
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){q, q, q}}
	r := newRunner(t, s, a)
	r.Limits.MaxTurns = 1
	res := r.Run(t.Context(), "q", &memRecorder{})
	if res.Outcome != OutcomeNoReport || res.Turns != 1+wrapUpTurns {
		t.Fatalf("outcome = %s, turns = %d", res.Outcome, res.Turns)
	}
	if a.n.Load() != 1 {
		t.Errorf("tool executed %d times, want 1", a.n.Load())
	}
	denied := s.turns[2].Results[0]
	if !denied.IsError || !strings.Contains(denied.Content, "Limit reached") {
		t.Errorf("denied = %+v", denied)
	}
}

func TestRunCostLimitWrapsUp(t *testing.T) {
	expensive := fixed(llm.Reply{
		Stop: llm.StopToolUse, Calls: []llm.ToolCall{call("t1", "a", `{}`)},
		Usage: llm.Usage{Output: 200_000}, // 20 美元每百万 × 20 万 = 4 美元
	})
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
		expensive,
		calls(call("t2", report.ToolName, reportInput("s1"))),
	}}
	res := newRunner(t, s, &fakeTool{name: "a"}).Run(t.Context(), "q", &memRecorder{})
	if res.Outcome != OutcomeReportedTruncated || res.LimitHit != LimitCost {
		t.Fatalf("outcome = %s, limit = %s", res.Outcome, res.LimitHit)
	}
}

func TestRunDurationLimitAndToolTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow := &fakeTool{name: "slow", fn: func(ctx context.Context, _ jsontext.Value) (tool.Result, error) {
			<-ctx.Done()
			return tool.Result{}, ctx.Err()
		}}
		nap := &fakeTool{name: "nap", fn: func(context.Context, jsontext.Value) (tool.Result, error) {
			time.Sleep(11 * time.Minute)
			return tool.Result{Text: "late"}, nil
		}}
		s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
			calls(call("t1", "slow", `{}`)),
			calls(call("t2", "nap", `{}`)),
			calls(call("t3", report.ToolName, reportInput("s2"))),
		}}
		// slow 等满工具的时限；nap 不看 ctx，睡过了整次运行的时限才返回。
		res := newRunner(t, s, slow, nap).Run(t.Context(), "q", &memRecorder{})

		if got := s.turns[1].Results[0]; !got.IsError || !strings.Contains(got.Content, "Timed out") {
			t.Errorf("slow result = %+v", got)
		}
		if res.Calls["slow"][CallTimeout] != 1 {
			t.Errorf("calls = %v", res.Calls)
		}
		if res.Outcome != OutcomeReportedTruncated || res.LimitHit != LimitDuration {
			t.Fatalf("outcome = %s, limit = %s", res.Outcome, res.LimitHit)
		}
	})
}

func TestRunMaxTokens(t *testing.T) {
	a := &fakeTool{name: "a"}
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
		// 被截断的回复里的工具调用参数可能不完整：不执行，但每个都要回结果。
		fixed(llm.Reply{Stop: llm.StopMaxTokens, Calls: []llm.ToolCall{call("t1", "a", `{"q":`)}}),
		calls(call("t2", report.ToolName, `{"summary":"s","root_cause":"unknown","confidence":"low","evidence":[],"ruled_out":[],"next_checks":[],"suggested_actions":[]}`)),
		calls(call("t3", report.ToolName, `{"summary":"s","root_cause":"unknown","confidence":"low","evidence":[],"ruled_out":[],"next_checks":[],"suggested_actions":[]}`)),
	}}
	res := newRunner(t, s, a).Run(t.Context(), "q", &memRecorder{})
	if a.n.Load() != 0 {
		t.Errorf("truncated call was executed")
	}
	first := s.turns[1]
	if len(first.Results) != 1 || !first.Results[0].IsError || !strings.Contains(first.Text, "output limit") {
		t.Errorf("turn after max_tokens = %+v", first)
	}
	// 没有任何步骤可引用：第一次打回，第二次按未核实收下。
	if res.Outcome != OutcomeReportedTruncated || !res.Unverified || res.LimitHit != LimitMaxTokens {
		t.Fatalf("outcome = %s, unverified = %v, limit = %s", res.Outcome, res.Unverified, res.LimitHit)
	}
}

func TestRunRefusal(t *testing.T) {
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
		fixed(llm.Reply{Stop: llm.StopRefusal, RefusalCategory: "cyber"}),
	}}
	res := newRunner(t, s).Run(t.Context(), "q", &memRecorder{})
	if res.Outcome != OutcomeRefused || res.Turns != 1 {
		t.Fatalf("outcome = %s", res.Outcome)
	}
}

func TestRunAPIErrorAndCancel(t *testing.T) {
	fail := func(llm.Turn) (*llm.Reply, error) { return nil, errors.New("529 overloaded") }
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){fail}}
	res := newRunner(t, s).Run(t.Context(), "q", &memRecorder{})
	if res.Outcome != OutcomeAPIError || res.Err == nil || res.Turns != 0 {
		t.Fatalf("outcome = %s, err = %v, turns = %d", res.Outcome, res.Err, res.Turns)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s = &scripted{}
	res = newRunner(t, s).Run(ctx, "q", &memRecorder{})
	if res.Outcome != OutcomeCanceled {
		t.Fatalf("outcome = %s", res.Outcome)
	}
}

func TestRunDuplicateSubmitInOneTurn(t *testing.T) {
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
		calls(call("t1", "a", `{}`)),
		calls(call("t2", report.ToolName, reportInput("s7")), call("t3", report.ToolName, reportInput("s1"))),
		calls(call("t4", report.ToolName, reportInput("s1"))),
	}}
	res := newRunner(t, s, &fakeTool{name: "a"}).Run(t.Context(), "q", &memRecorder{})
	got := s.turns[2].Results
	if len(got) != 2 || !got[0].IsError || !got[1].IsError || !strings.Contains(got[1].Content, "Only one") {
		t.Fatalf("results = %+v", got)
	}
	if res.Outcome != OutcomeReported || res.Unverified {
		t.Fatalf("outcome = %s, unverified = %v", res.Outcome, res.Unverified)
	}
}

func TestRunWrapUpKeepsLastParseableReport(t *testing.T) {
	q := calls(call("t1", "a", `{}`))
	bad := calls(call("t", report.ToolName, reportInput("s9")))
	s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){q, q, bad}}
	r := newRunner(t, s, &fakeTool{name: "a"})
	r.Limits.MaxTurns = 1
	// 第 2 轮是收尾的第一次机会（它还在查，被拒），第 3 轮是最后一次：报告不合格也收下。
	res := r.Run(t.Context(), "q", &memRecorder{})
	if res.Outcome != OutcomeReportedTruncated || !res.Unverified {
		t.Fatalf("outcome = %s, unverified = %v", res.Outcome, res.Unverified)
	}
}

// 参数不是合法 JSON 的调用（OpenAI 兼容的后端、没开严格模式时会有）不执行，错误还给模型；
// 报告这样坏掉一次不占“打回”的名额：之后内容上不合格的报告仍然能被打回一次。
func TestRunMalformedArgumentsGoBackToModel(t *testing.T) {
	bad := func(id, name string) llm.ToolCall {
		return llm.ToolCall{ID: id, Name: name, Input: jsontext.Value(`"{\"a\": \"x\"y\"}"`), Malformed: "invalid character 'y'"}
	}
	q := &fakeTool{name: "q"}
	model := &scripted{t: t, replies: []func(llm.Turn) (*llm.Reply, error){
		calls(bad("1", "q"), call("2", "q", `{}`)),
		calls(bad("3", report.ToolName)),
		calls(call("4", report.ToolName, reportInput("s9"))),
		calls(call("5", report.ToolName, reportInput("s1"))),
	}}
	res := newRunner(t, model, q).Run(t.Context(), "why", &memRecorder{})

	if res.Outcome != OutcomeReported || res.Unverified {
		t.Fatalf("outcome = %s, unverified = %v, err = %v", res.Outcome, res.Unverified, res.Err)
	}
	if q.n.Load() != 1 {
		t.Errorf("tool ran %d times, want 1 (the malformed call must not run)", q.n.Load())
	}
	first := model.turns[1].Results
	if len(first) != 2 || !first[0].IsError || !strings.Contains(first[0].Content, "not valid JSON") ||
		first[1].IsError || !strings.HasPrefix(first[1].Content, "[s1]") {
		t.Errorf("results after turn 1 = %+v", first)
	}
	if r := model.turns[2].Results; len(r) != 1 || !r[0].IsError || !strings.Contains(r[0].Content, "not valid JSON") {
		t.Errorf("malformed report result = %+v", r)
	}
	// 第三轮的报告引用了不存在的步骤：被打回（名额还在），第四轮才收下。
	if r := model.turns[3].Results; len(r) != 1 || !r[0].IsError || strings.Contains(r[0].Content, "not valid JSON") {
		t.Errorf("rejected report result = %+v", r)
	}
}

// slowTool 声明了比默认更长的时限。
type slowTool struct{ fakeTool }

func (s *slowTool) Timeout() time.Duration { return time.Hour }

// 工具拿到的 ctx 里有这次运行共享的 Scope 和自己的步骤号；同一次运行里各步看到的是同一个 Scope，
// 不同的运行是不同的。RawExt 决定原始结果的文件名；声明了 Timeouter 的工具拿到的时限更长。
func TestRunGivesToolsScopeStepAndTimeout(t *testing.T) {
	type seen struct {
		scope    *tool.Scope
		step     string
		deadline time.Duration
	}
	var mu sync.Mutex
	var got []seen
	record := func(ctx context.Context, _ jsontext.Value) (tool.Result, error) {
		scope, step := tool.CallFrom(ctx)
		dl, _ := ctx.Deadline()
		mu.Lock()
		got = append(got, seen{scope, step, time.Until(dl)})
		mu.Unlock()
		return tool.Result{Text: "ok", Raw: []byte("bin"), RawExt: ".pb.gz"}, nil
	}
	a := &slowTool{fakeTool{name: "a", fn: record}}
	run := func() *memRecorder {
		s := &scripted{replies: []func(llm.Turn) (*llm.Reply, error){
			calls(call("t1", "a", `{}`), call("t2", "a", `{}`)),
			calls(call("t3", report.ToolName, reportInput("s1", "s2"))),
		}}
		rec := &memRecorder{}
		if res := newRunner(t, s, a).Run(t.Context(), "q", rec); res.Outcome != OutcomeReported {
			t.Fatalf("outcome = %s", res.Outcome)
		}
		return rec
	}
	rec := run()
	run()

	if len(got) != 4 {
		t.Fatalf("tool called %d times, want 4", len(got))
	}
	steps := map[string]bool{got[0].step: true, got[1].step: true}
	if !steps["s1"] || !steps["s2"] {
		t.Errorf("steps in the first run = %q, %q; want s1 and s2", got[0].step, got[1].step)
	}
	if got[0].scope == nil || got[0].scope != got[1].scope {
		t.Error("calls in one run do not share a scope")
	}
	if got[2].scope == got[0].scope {
		t.Error("two runs share a scope")
	}
	if got[0].deadline < time.Hour-time.Minute {
		t.Errorf("deadline = %v, want the tool's own 1h timeout", got[0].deadline)
	}
	if string(rec.raw["s1.pb.gz"]) != "bin" {
		t.Errorf("raw files = %v, want s1.pb.gz", rec.raw)
	}
}
