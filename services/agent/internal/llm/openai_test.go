package llm

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// DeepSeek 的回复：思考内容在 reasoning_content 里，两种缓存命中的计数都给，content 是 null。
const chatToolReply = `

{"id":"cmpl-1","object":"chat.completion","model":"deepseek-v4-pro",
"choices":[{"index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":null,
"reasoning_content":"look at up","vendor_extra":{"keep":"me"},
"tool_calls":[{"id":"call_1","type":"function","function":{"name":"prom_query","arguments":"{\"query\":\"up\"}"}},
{"id":"call_2","type":"function","function":{"name":"prom_query","arguments":"{\"query\":"}}]}}],
"usage":{"prompt_tokens":100,"completion_tokens":20,"total_tokens":120,
"prompt_cache_hit_tokens":70,"prompt_cache_miss_tokens":30,
"prompt_tokens_details":{"cached_tokens":70},"completion_tokens_details":{"reasoning_tokens":12}}}`

const chatStopReply = `{"id":"cmpl-2","model":"m","choices":[{"finish_reason":"stop",
"message":{"role":"assistant","content":"done"}}],"usage":{"prompt_tokens":5,"completion_tokens":1}}`

// fakeChatAPI 记下每次请求的头和体，按顺序回放响应。
type fakeChatAPI struct {
	t       *testing.T
	replies []string
	status  []int
	bodies  []map[string]any
	raw     []string
	auth    []string
}

func (f *fakeChatAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		f.t.Errorf("request body: %v", err)
	}
	i := len(f.bodies)
	f.bodies = append(f.bodies, body)
	f.raw = append(f.raw, string(raw))
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	if r.URL.Path != "/chat/completions" || r.Method != http.MethodPost {
		f.t.Errorf("request = %s %s", r.Method, r.URL.Path)
	}
	if i >= len(f.replies) {
		f.t.Errorf("unexpected request %d", i)
		w.WriteHeader(http.StatusTeapot)
		return
	}
	if f.status[i] != http.StatusOK {
		w.WriteHeader(f.status[i])
	}
	_, _ = io.WriteString(w, f.replies[i])
}

func newChatConversation(t *testing.T, f *fakeChatAPI, cfg OpenAIConfig) Conversation {
	t.Helper()
	f.t = t
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	// 末尾多的斜杠不该变成路径里的两个斜杠。
	cfg.BaseURL = srv.URL + "/"
	client := NewOpenAI(cfg)
	client.retry.Initial = time.Microsecond
	return client.NewConversation("system text", []ToolDef{
		{Name: "prom_query", Description: "run PromQL", Schema: map[string]any{
			"type": "object", "required": []string{"query"}, "additionalProperties": false,
			"properties": map[string]any{"query": map[string]any{"type": "string"}, "time": map[string]any{"type": "string"}},
		}},
		{Name: "submit_report", Description: "report", Strict: true, Schema: map[string]any{
			"type": "object", "properties": map[string]any{}, "additionalProperties": false,
		}},
	})
}

func TestOpenAIRequestShapeAndReply(t *testing.T) {
	f := &fakeChatAPI{replies: []string{chatToolReply, chatStopReply}, status: []int{200, 200}}
	conv := newChatConversation(t, f, OpenAIConfig{Model: "deepseek-v4-pro", Effort: "high", APIKey: "sk-test"})

	reply, err := conv.Send(t.Context(), Turn{Text: "why is it down"})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Stop != StopToolUse || reply.Text != "" || reply.MessageID != "cmpl-1" || reply.Model != "deepseek-v4-pro" {
		t.Errorf("reply = %+v", reply)
	}
	if len(reply.Thinking) != 1 || reply.Thinking[0] != "look at up" {
		t.Errorf("thinking = %v", reply.Thinking)
	}
	if len(reply.Calls) != 2 || reply.Calls[0].ID != "call_1" || reply.Calls[0].Name != "prom_query" ||
		string(reply.Calls[0].Input) != `{"query":"up"}` {
		t.Errorf("calls = %+v", reply.Calls)
	}
	// 写坏的参数包成一个 JSON 字符串：仍然是合法的 JSON，工具按对象解析时会失败。
	if c := reply.Calls[1]; !c.Input.IsValid() || string(c.Input) != `"{\"query\":"` || c.Malformed == "" {
		t.Errorf("malformed arguments = %s, error = %q", c.Input, c.Malformed)
	}
	if reply.Calls[0].Malformed != "" {
		t.Errorf("valid arguments marked malformed: %q", reply.Calls[0].Malformed)
	}
	// 缓存命中的 70 从 prompt_tokens 里减出来，不能又算进 Input。
	if reply.Usage != (Usage{Input: 30, Output: 20, CacheRead: 70}) {
		t.Errorf("usage = %+v", reply.Usage)
	}

	b := f.bodies[0]
	if f.auth[0] != "Bearer sk-test" {
		t.Errorf("authorization = %q", f.auth[0])
	}
	if at(t, b, "model") != "deepseek-v4-pro" || at(t, b, "reasoning_effort") != "high" {
		t.Errorf("model/effort = %v / %v", at(t, b, "model"), at(t, b, "reasoning_effort"))
	}
	if at(t, b, "max_tokens") != float64(MaxOutputTokens) || b["max_completion_tokens"] != nil {
		t.Errorf("token limit = %v / %v", b["max_tokens"], b["max_completion_tokens"])
	}
	if at(t, b, "messages", 0, "role") != "system" || at(t, b, "messages", 0, "content") != "system text" {
		t.Errorf("system = %v", at(t, b, "messages", 0))
	}
	if at(t, b, "messages", 1, "role") != "user" || at(t, b, "messages", 1, "content") != "why is it down" {
		t.Errorf("user = %v", at(t, b, "messages", 1))
	}
	if at(t, b, "tools", 0, "type") != "function" || at(t, b, "tools", 0, "function", "name") != "prom_query" ||
		at(t, b, "tools", 0, "function", "parameters", "additionalProperties") != false {
		t.Errorf("tool 0 = %v", at(t, b, "tools", 0))
	}
	// 配置里没开严格模式（DeepSeek 的预设）：工具自己要了也不发。
	if at(t, b, "tools", 1, "function", "strict") != nil {
		t.Errorf("strict sent although disabled: %v", at(t, b, "tools", 1))
	}
	if _, has := b["tool_choice"]; has {
		t.Error("tool_choice must not be set: forcing a tool is rejected in thinking mode")
	}

	// 第二轮：第一轮的回复原样在历史里（思考内容和不认识的字段都在），每个工具结果是单独的一条 tool 消息，文字排在最后。
	reply, err = conv.Send(t.Context(), Turn{
		Results: []ToolResult{{CallID: "call_1", Content: "[s1]\nup 1"}, {CallID: "call_2", Content: "bad input", IsError: true}},
		Text:    "note",
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Stop != StopEndTurn || reply.Text != "done" || len(reply.Calls) != 0 {
		t.Errorf("reply = %+v", reply)
	}
	b = f.bodies[1]
	if n := len(at(t, b, "messages").([]any)); n != 6 {
		t.Fatalf("messages = %d, want 6", n)
	}
	assistant := at(t, b, "messages", 2)
	if at(t, assistant, "role") != "assistant" || at(t, assistant, "reasoning_content") != "look at up" ||
		at(t, assistant, "vendor_extra", "keep") != "me" || at(t, assistant, "tool_calls", 1, "id") != "call_2" {
		t.Errorf("assistant turn not replayed verbatim: %v", assistant)
	}
	if at(t, b, "messages", 3, "role") != "tool" || at(t, b, "messages", 3, "tool_call_id") != "call_1" ||
		at(t, b, "messages", 3, "content") != "[s1]\nup 1" {
		t.Errorf("tool result 1 = %v", at(t, b, "messages", 3))
	}
	if at(t, b, "messages", 4, "tool_call_id") != "call_2" || at(t, b, "messages", 4, "content") != "ERROR: bad input" {
		t.Errorf("tool result 2 = %v", at(t, b, "messages", 4))
	}
	if at(t, b, "messages", 5, "role") != "user" || at(t, b, "messages", 5, "content") != "note" {
		t.Errorf("user turn = %v", at(t, b, "messages", 5))
	}
	// 第二轮的请求体以第一轮的（去掉末尾的 "]}"）开头：工具、系统提示、已有的消息一个字节都没有变，前缀缓存才命得中。
	if prefix := strings.TrimSuffix(f.raw[0], "]}"); !strings.HasPrefix(f.raw[1], prefix) {
		t.Errorf("request 2 does not extend request 1 byte for byte:\n%s\n%s", f.raw[0], f.raw[1])
	}
}

func TestOpenAIPresetFields(t *testing.T) {
	f := &fakeChatAPI{replies: []string{chatStopReply}, status: []int{200}}
	conv := newChatConversation(t, f, OpenAIConfig{Model: "m", MaxTokensField: FieldMaxCompletionTokens, Strict: true})
	if _, err := conv.Send(t.Context(), Turn{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	b := f.bodies[0]
	if at(t, b, "max_completion_tokens") != float64(MaxOutputTokens) || b["max_tokens"] != nil {
		t.Errorf("token limit = %v / %v", b["max_tokens"], b["max_completion_tokens"])
	}
	if at(t, b, "tools", 0, "function", "strict") != nil || at(t, b, "tools", 1, "function", "strict") != true {
		t.Errorf("strict = %v / %v", at(t, b, "tools", 0, "function"), at(t, b, "tools", 1, "function"))
	}
	// 没有给 effort、没有给密钥：两样都不发。
	if _, has := b["reasoning_effort"]; has || f.auth[0] != "" {
		t.Errorf("reasoning_effort = %v, authorization = %q", b["reasoning_effort"], f.auth[0])
	}
}

func TestOpenAIStopReasons(t *testing.T) {
	reply := func(finish, message string) string {
		return `{"id":"x","model":"m","choices":[{"finish_reason":"` + finish + `","message":` + message + `}],"usage":{}}`
	}
	call := `{"role":"assistant","content":"","tool_calls":[{"id":"c","function":{"name":"t","arguments":""}}]}`
	for _, tc := range []struct {
		name, body string
		want       StopReason
		category   string
		calls      int
	}{
		{"stop", reply("stop", `{"role":"assistant","content":"a"}`), StopEndTurn, "", 0},
		{"tool calls", reply("tool_calls", call), StopToolUse, "", 1},
		// 有的后端带着工具调用却报 stop：按有工具调用算。
		{"tool calls reported as stop", reply("stop", call), StopToolUse, "", 1},
		// 被截断时工具调用照样传上去（循环要给每一个都回结果），结束原因是截断。
		{"truncated with calls", reply("length", call), StopMaxTokens, "", 1},
		{"truncated", reply("length", `{"role":"assistant","content":"a"}`), StopMaxTokens, "", 0},
		{"content filter", reply("content_filter", `{"role":"assistant","content":""}`), StopRefusal, "content_filter", 0},
		{"refusal field", reply("stop", `{"role":"assistant","content":null,"refusal":"no"}`), StopRefusal, "content_filter", 0},
		{"unknown", reply("something_new", `{"role":"assistant","content":"a"}`), StopReason("something_new"), "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeChatAPI{replies: []string{tc.body}, status: []int{200}}
			got, err := newChatConversation(t, f, OpenAIConfig{Model: "m"}).Send(t.Context(), Turn{Text: "q"})
			if err != nil {
				t.Fatal(err)
			}
			if got.Stop != tc.want || got.RefusalCategory != tc.category || len(got.Calls) != tc.calls {
				t.Errorf("stop = %q, category = %q, calls = %d", got.Stop, got.RefusalCategory, len(got.Calls))
			}
			// 空的参数串当成空对象。
			if tc.calls == 1 && string(got.Calls[0].Input) != "{}" {
				t.Errorf("input = %s", got.Calls[0].Input)
			}
		})
	}
}

func TestOpenAIRetries(t *testing.T) {
	interrupted := `{"id":"x","model":"m","choices":[{"finish_reason":"insufficient_system_resource","message":{"role":"assistant","content":"par"}}]}`
	t.Run("retries 429, 5xx and interrupted generations", func(t *testing.T) {
		f := &fakeChatAPI{replies: []string{`{"error":"slow down"}`, interrupted, chatStopReply}, status: []int{429, 200, 200}}
		reply, err := newChatConversation(t, f, OpenAIConfig{Model: "m"}).Send(t.Context(), Turn{Text: "q"})
		if err != nil || reply.Text != "done" {
			t.Fatalf("reply = %+v, err = %v", reply, err)
		}
		if len(f.bodies) != 3 {
			t.Errorf("requests = %d, want 3", len(f.bodies))
		}
	})
	t.Run("gives up after the retry budget", func(t *testing.T) {
		f := &fakeChatAPI{replies: []string{"a", "b", "overloaded"}, status: []int{503, 500, 503}}
		_, err := newChatConversation(t, f, OpenAIConfig{Model: "m"}).Send(t.Context(), Turn{Text: "q"})
		if err == nil || !strings.Contains(err.Error(), "status 503: overloaded") {
			t.Fatalf("err = %v", err)
		}
		if len(f.bodies) != openAIAttempts {
			t.Errorf("requests = %d, want %d", len(f.bodies), openAIAttempts)
		}
	})
	t.Run("does not retry what will not get better", func(t *testing.T) {
		for _, status := range []int{400, 401, 402, 422} {
			f := &fakeChatAPI{replies: []string{`{"error":{"message":"no"}}`}, status: []int{status}}
			if _, err := newChatConversation(t, f, OpenAIConfig{Model: "m"}).Send(t.Context(), Turn{Text: "q"}); err == nil {
				t.Errorf("status %d: want an error", status)
			}
			if len(f.bodies) != 1 {
				t.Errorf("status %d: requests = %d, want 1", status, len(f.bodies))
			}
		}
	})
	t.Run("stops waiting when the context ends", func(t *testing.T) {
		f := &fakeChatAPI{replies: []string{"busy"}, status: []int{503}}
		f.t = t
		srv := httptest.NewServer(f)
		t.Cleanup(srv.Close)
		client := NewOpenAI(OpenAIConfig{BaseURL: srv.URL, Model: "m"})
		client.retry.Initial, client.retry.Max = time.Hour, time.Hour
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err := client.NewConversation("s", nil).Send(ctx, Turn{Text: "q"})
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "status 503") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestOpenAIFailedSendLeavesHistoryUntouched(t *testing.T) {
	f := &fakeChatAPI{replies: []string{`{"error":"bad"}`, chatStopReply}, status: []int{400, 200}}
	conv := newChatConversation(t, f, OpenAIConfig{Model: "m"})

	if _, err := conv.Send(t.Context(), Turn{Text: "first"}); err == nil {
		t.Fatal("want an error")
	}
	if _, err := conv.Send(t.Context(), Turn{}); err == nil {
		t.Fatal("empty turn: want an error")
	}
	if _, err := conv.Send(t.Context(), Turn{Text: "second"}); err != nil {
		t.Fatal(err)
	}
	b := f.bodies[1]
	if n := len(at(t, b, "messages").([]any)); n != 2 {
		t.Fatalf("messages = %d, want 2 (system + the new turn; the failed turn must not stay in history)", n)
	}
	if at(t, b, "messages", 1, "content") != "second" {
		t.Errorf("message = %v", at(t, b, "messages", 1))
	}
}

func TestOpenAIRejectsUnusableResponses(t *testing.T) {
	for name, body := range map[string]string{
		"no choices": `{"id":"x","choices":[]}`,
		"not json":   `<html>gateway</html>`,
		"no message": `{"id":"x","choices":[{"finish_reason":"stop"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeChatAPI{replies: []string{body}, status: []int{200}}
			if _, err := newChatConversation(t, f, OpenAIConfig{Model: "m"}).Send(t.Context(), Turn{Text: "q"}); err == nil {
				t.Fatal("want an error")
			}
		})
	}
}
