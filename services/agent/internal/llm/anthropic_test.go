package llm

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go/v2"
	"github.com/anthropics/anthropic-sdk-go/v2/option"
)

const toolUseReply = `{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5",
"content":[{"type":"thinking","thinking":"look at up","signature":"sig-1"},
{"type":"text","text":"checking"},
{"type":"tool_use","id":"tu_1","name":"prom_query","input":{"query":"up"}}],
"stop_reason":"tool_use","stop_sequence":null,
"usage":{"input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":3,"cache_read_input_tokens":7}}`

const refusalReply = `{"id":"msg_2","type":"message","role":"assistant","model":"claude-opus-5-5","content":[],
"stop_reason":"refusal","stop_sequence":null,"stop_details":{"type":"refusal","category":"cyber","explanation":"x"},
"usage":{"input_tokens":1,"output_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}`

// fakeAPI 记下每次请求的头和体，按顺序回放响应；status 不是 200 的那一次返回错误。
type fakeAPI struct {
	t       *testing.T
	replies []string
	status  []int
	bodies  []map[string]any
	betas   []string
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		f.t.Errorf("request body: %v", err)
	}
	i := len(f.bodies)
	f.bodies = append(f.bodies, body)
	f.betas = append(f.betas, r.Header.Get("anthropic-beta"))
	if r.URL.Path != "/v1/messages" {
		f.t.Errorf("path = %s", r.URL.Path)
	}
	w.Header().Set("Content-Type", "application/json")
	if f.status[i] != http.StatusOK {
		w.WriteHeader(f.status[i])
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}`)
		return
	}
	_, _ = io.WriteString(w, f.replies[i])
}

func newConversation(t *testing.T, f *fakeAPI, cfg AnthropicConfig) Conversation {
	t.Helper()
	f.t = t
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	client := anthropic.NewClient(option.WithBaseURL(srv.URL), option.WithAPIKey("test"), option.WithMaxRetries(0))
	return NewAnthropic(client, cfg).NewConversation("system text", []ToolDef{
		{Name: "prom_query", Description: "run PromQL", Schema: map[string]any{
			"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}},
		}},
		{Name: "submit_report", Description: "report", Strict: true, Schema: map[string]any{
			"type": "object", "properties": map[string]any{}, "additionalProperties": false,
		}},
	})
}

func at(t *testing.T, v any, path ...any) any {
	t.Helper()
	for _, p := range path {
		switch key := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("at %v: not an object: %v", p, v)
			}
			v = m[key]
		case int:
			s, ok := v.([]any)
			if !ok || key >= len(s) {
				t.Fatalf("at %v: not an array of that length: %v", p, v)
			}
			v = s[key]
		}
	}
	return v
}

func TestAnthropicRequestShapeAndReply(t *testing.T) {
	f := &fakeAPI{replies: []string{toolUseReply, toolUseReply}, status: []int{200, 200}}
	conv := newConversation(t, f, AnthropicConfig{Model: "claude-opus-5-5", Effort: "high", Fallback: true})

	reply, err := conv.Send(t.Context(), Turn{Text: "why is it down"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Stop != StopToolUse || reply.Text != "checking" || reply.MessageID != "msg_1" || reply.Model != "claude-opus-5-5" {
		t.Errorf("reply = %+v", reply)
	}
	if len(reply.Thinking) != 1 || reply.Thinking[0] != "look at up" {
		t.Errorf("thinking = %v", reply.Thinking)
	}
	if len(reply.Calls) != 1 || reply.Calls[0].ID != "tu_1" || reply.Calls[0].Name != "prom_query" ||
		strings.Join(strings.Fields(string(reply.Calls[0].Input)), "") != `{"query":"up"}` {
		t.Errorf("calls = %+v", reply.Calls)
	}
	if reply.Usage != (Usage{Input: 10, Output: 5, CacheWrite: 3, CacheRead: 7}) {
		t.Errorf("usage = %+v", reply.Usage)
	}

	b := f.bodies[0]
	if at(t, b, "model") != "claude-opus-5-5" || at(t, b, "output_config", "effort") != "high" {
		t.Errorf("model/effort = %v / %v", at(t, b, "model"), at(t, b, "output_config"))
	}
	if at(t, b, "thinking", "type") != "adaptive" || at(t, b, "thinking", "display") != "summarized" {
		t.Errorf("thinking = %v", at(t, b, "thinking"))
	}
	// 两个缓存断点：系统提示的末尾（盖住工具和系统提示）和请求顶层（对话的尾部）。
	if at(t, b, "system", 0, "text") != "system text" || at(t, b, "system", 0, "cache_control", "type") != "ephemeral" {
		t.Errorf("system = %v", at(t, b, "system"))
	}
	if at(t, b, "cache_control", "type") != "ephemeral" {
		t.Errorf("top-level cache_control = %v", at(t, b, "cache_control"))
	}
	if at(t, b, "fallbacks") != "default" || !strings.Contains(f.betas[0], betaFallback) {
		t.Errorf("fallbacks = %v, beta header = %q", at(t, b, "fallbacks"), f.betas[0])
	}
	if at(t, b, "tools", 0, "name") != "prom_query" || at(t, b, "tools", 0, "strict") != nil {
		t.Errorf("tool 0 = %v", at(t, b, "tools", 0))
	}
	if at(t, b, "tools", 1, "strict") != true || at(t, b, "tools", 1, "input_schema", "additionalProperties") != false {
		t.Errorf("tool 1 = %v", at(t, b, "tools", 1))
	}
	if _, has := b["tool_choice"]; has {
		t.Error("tool_choice must not be set: forcing a tool is rejected by the model")
	}

	// 第二轮：历史里有第一轮的回复（思考块带着签名），工具结果排在文字前面。
	if _, err := conv.Send(t.Context(), Turn{
		Results: []ToolResult{{CallID: "tu_1", Content: "[s1]\nup 1", IsError: false}},
		Text:    "note",
	}); err != nil {
		t.Fatal(err)
	}
	b = f.bodies[1]
	if n := len(at(t, b, "messages").([]any)); n != 3 {
		t.Fatalf("messages = %d, want 3", n)
	}
	if at(t, b, "messages", 1, "role") != "assistant" || at(t, b, "messages", 1, "content", 0, "signature") != "sig-1" {
		t.Errorf("assistant turn = %v", at(t, b, "messages", 1))
	}
	if at(t, b, "messages", 1, "content", 2, "id") != "tu_1" {
		t.Errorf("tool_use not replayed: %v", at(t, b, "messages", 1, "content"))
	}
	user := at(t, b, "messages", 2, "content")
	if at(t, user, 0, "type") != "tool_result" || at(t, user, 0, "tool_use_id") != "tu_1" || at(t, user, 1, "text") != "note" {
		t.Errorf("user turn = %v", user)
	}
}

func TestAnthropicFailedSendLeavesHistoryUntouched(t *testing.T) {
	f := &fakeAPI{replies: []string{"", toolUseReply}, status: []int{400, 200}}
	conv := newConversation(t, f, AnthropicConfig{Model: "claude-opus-5-5", Effort: "medium"})

	if _, err := conv.Send(t.Context(), Turn{Text: "first"}); err == nil {
		t.Fatal("want an error")
	}
	if _, err := conv.Send(t.Context(), Turn{Text: "second"}); err != nil {
		t.Fatal(err)
	}
	b := f.bodies[1]
	if n := len(at(t, b, "messages").([]any)); n != 1 {
		t.Fatalf("messages = %d, want 1 (the failed turn must not stay in history)", n)
	}
	if at(t, b, "messages", 0, "content", 0, "text") != "second" {
		t.Errorf("message = %v", at(t, b, "messages", 0))
	}
	// 没开回退：不带 fallbacks，也不带那个 beta 头。
	if _, has := b["fallbacks"]; has || strings.Contains(f.betas[1], betaFallback) {
		t.Errorf("fallbacks sent although disabled: %v / %q", b["fallbacks"], f.betas[1])
	}
	if _, err := conv.Send(t.Context(), Turn{}); err == nil {
		t.Error("empty turn must be rejected")
	}
}

func TestAnthropicRefusal(t *testing.T) {
	f := &fakeAPI{replies: []string{refusalReply}, status: []int{200}}
	conv := newConversation(t, f, AnthropicConfig{Model: "claude-opus-5-5", Effort: "high"})
	reply, err := conv.Send(t.Context(), Turn{Text: "q"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Stop != StopRefusal || reply.RefusalCategory != "cyber" {
		t.Errorf("reply = %+v", reply)
	}
}

func TestPrice(t *testing.T) {
	p, err := PriceOf("claude-opus-5-5")
	if err != nil {
		t.Fatal(err)
	}
	u := Usage{Input: 1_000_000, Output: 100_000, CacheWrite: 200_000, CacheRead: 2_000_000}
	// 4 + 2 + 1 + 0.4
	if got := p.Cost(u); got < 7.3999 || got > 7.4001 {
		t.Errorf("cost = %v, want 7.4", got)
	}
	if _, err := PriceOf("claude-opus-9"); err == nil {
		t.Error("unknown model must be an error")
	}
	// 回退到表里没有的模型：按配置的模型的价算。
	if got := CostOf("some-other-model", p, Usage{Input: 1_000_000}, time.Now()); got != 4 {
		t.Errorf("fallback cost = %v, want 4", got)
	}
	if got := CostOf("claude-sonnet-5-5", p, Usage{Input: 1_000_000}, time.Now()); got != 2 {
		t.Errorf("served-by cost = %v, want 2", got)
	}
}
