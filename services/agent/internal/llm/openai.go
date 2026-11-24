package llm

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/pkg/retry"
)

// OpenAI 兼容的 Chat Completions 协议（DeepSeek、OpenAI 和大多数别的厂商、本地的推理服务都说这个）。
// 没有用哪一家的 SDK，原因见 ADR-0040：各家在这个协议上各加了自己的字段（DeepSeek 的 reasoning_content、
// 两种缓存命中的计数），而“回复原样放回历史”要求把认不出的字段也带回去，按原始 JSON 存最省事。

const (
	// openAIAttempts 是一次调用最多发几次请求（429、5xx、连接错误、回复被服务端中断时再试）。
	// 这是这条调用链上唯一的一层重试，和 Claude 那一侧 SDK 自己的 2 次重试对齐。
	openAIAttempts = 3
	// maxReplyBytes 是一次回复体的上限（16000 个输出 token 远小于它）。
	maxReplyBytes = 16 << 20
)

// 两种写法的输出上限字段。OpenAI 把 max_tokens 标成了废弃、推理模型不接受它；DeepSeek 的文档里只有 max_tokens。
const (
	FieldMaxTokens           = "max_tokens"
	FieldMaxCompletionTokens = "max_completion_tokens"
)

// OpenAIConfig 是 OpenAI 兼容协议这一侧的配置。
type OpenAIConfig struct {
	// BaseURL 不带末尾的 /chat/completions，例如 https://api.deepseek.com。
	BaseURL string
	// APIKey 为空时不带 Authorization 头（本地的推理服务）；该带而没带时 API 回 401，那次运行以 api_error 结束。
	APIKey string
	Model  string
	// Effort 是 reasoning_effort，为空时不发这个字段（不认它的后端会回 400）。
	Effort string
	// MaxTokensField 是输出上限用哪个字段名，为空时用 max_tokens。
	MaxTokensField string
	// Strict 为假时不把 ToolDef.Strict 发出去：DeepSeek 的严格模式只在它的 beta 地址上有。
	Strict bool
	// HTTP 为空时用一个不设超时的客户端：每次调用的时限由循环经 ctx 给。
	HTTP *http.Client
}

// OpenAI 用 OpenAI 兼容的 Chat Completions 实现 Client。
type OpenAI struct {
	cfg OpenAIConfig
	// retry 是重试的策略。测试里把间隔改短。
	retry retry.Policy
}

// NewOpenAI 返回实现。
func NewOpenAI(cfg OpenAIConfig) *OpenAI {
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{}
	}
	if cfg.MaxTokensField == "" {
		cfg.MaxTokensField = FieldMaxTokens
	}
	return &OpenAI{cfg: cfg, retry: retry.Policy{
		MaxAttempts: openAIAttempts, Initial: time.Second, Max: 8 * time.Second, Jitter: 0.5,
		Retryable: func(err error) bool { _, ok := errors.AsType[retryable](err); return ok },
	}}
}

type chatFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitzero"`
	Parameters  map[string]any `json:"parameters"`
	Strict      bool           `json:"strict,omitzero"`
}

type chatTool struct {
	Type     string       `json:"type"`
	Function chatFunction `json:"function"`
}

type chatRequest struct {
	Model               string           `json:"model"`
	Tools               jsontext.Value   `json:"tools,omitzero"`
	ReasoningEffort     string           `json:"reasoning_effort,omitzero"`
	MaxTokens           int              `json:"max_tokens,omitzero"`
	MaxCompletionTokens int              `json:"max_completion_tokens,omitzero"`
	Messages            []jsontext.Value `json:"messages"`
}

// chatMessage 是发出去的 system、user、tool 消息。assistant 的消息不经过它：回复里的那一条原样放回历史。
type chatMessage struct {
	Role       string `json:"role"`
	ToolCallID string `json:"tool_call_id,omitzero"`
	Content    string `json:"content"`
}

type chatToolCall struct {
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// chatReplyMessage 是回复里要读的字段。content 可能是 null（只有工具调用时），解出来是空串。
type chatReplyMessage struct {
	Content string `json:"content"`
	// ReasoningContent 是 DeepSeek 的思考内容；OpenAI 的 Chat Completions 不给思考内容。
	ReasoningContent string         `json:"reasoning_content"`
	Refusal          string         `json:"refusal"`
	ToolCalls        []chatToolCall `json:"tool_calls"`
}

type chatUsage struct {
	// PromptTokens 含缓存命中的部分（DeepSeek 的文档写明等于命中加未命中），和 Claude 的 input_tokens 口径不同。
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	// PromptCacheHitTokens 是 DeepSeek 的写法，PromptTokensDetails.CachedTokens 是 OpenAI 的写法（DeepSeek 两个都给）。
	PromptCacheHitTokens int64 `json:"prompt_cache_hit_tokens"`
	PromptTokensDetails  struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
}

type chatResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		FinishReason string         `json:"finish_reason"`
		Message      jsontext.Value `json:"message"`
	} `json:"choices"`
	Usage chatUsage `json:"usage"`
}

// NewConversation 实现 Client。
func (o *OpenAI) NewConversation(system string, tools []ToolDef) Conversation {
	c := &openAIConversation{client: o}
	c.messages, c.err = appendMessage(nil, chatMessage{Role: "system", Content: system})
	if c.err != nil || len(tools) == 0 {
		return c
	}
	defs := make([]chatTool, len(tools))
	for i, t := range tools {
		defs[i] = chatTool{Type: "function", Function: chatFunction{
			Name: t.Name, Description: t.Description, Parameters: t.Schema, Strict: t.Strict && o.cfg.Strict,
		}}
	}
	// 工具定义只编码一次、每一轮发同样的字节：Schema 是 map，json/v2 默认不保证 map 的键的顺序，
	// 每轮重新编码会让前缀每次都不一样，缓存就永远不命中。
	c.tools, c.err = json.Marshal(defs, json.Deterministic(true))
	return c
}

type openAIConversation struct {
	client *OpenAI
	tools  jsontext.Value
	// messages 是到目前为止的全部历史，只在 Send 成功后追加。assistant 的消息是回复里的原文。
	messages []jsontext.Value
	// err 是建对话时编码失败的错误（Schema 里有编不了的值），第一次 Send 时报出来。
	err error
}

func appendMessage(msgs []jsontext.Value, m chatMessage) ([]jsontext.Value, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return msgs, fmt.Errorf("encode %s message: %w", m.Role, err)
	}
	return append(msgs, b), nil
}

func (c *openAIConversation) Send(ctx context.Context, turn Turn) (*Reply, error) {
	if c.err != nil {
		return nil, c.err
	}
	if len(turn.Results) == 0 && turn.Text == "" {
		return nil, errors.New("empty turn")
	}
	// 复制一份再追加：失败时 c.messages 不能留下这些没有得到回复的消息。
	msgs := slices.Clone(c.messages)
	var err error
	// 这个协议里每个工具调用的结果是单独的一条 tool 消息，没有“出错”的标记，所以写在内容的开头。
	for _, r := range turn.Results {
		content := r.Content
		if r.IsError {
			content = "ERROR: " + content
		}
		if msgs, err = appendMessage(msgs, chatMessage{Role: "tool", ToolCallID: r.CallID, Content: content}); err != nil {
			return nil, err
		}
	}
	if turn.Text != "" {
		if msgs, err = appendMessage(msgs, chatMessage{Role: "user", Content: turn.Text}); err != nil {
			return nil, err
		}
	}

	cfg := c.client.cfg
	req := chatRequest{Model: cfg.Model, Tools: c.tools, ReasoningEffort: cfg.Effort, Messages: msgs}
	if cfg.MaxTokensField == FieldMaxCompletionTokens {
		req.MaxCompletionTokens = MaxOutputTokens
	} else {
		req.MaxTokens = MaxOutputTokens
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	resp, err := c.client.complete(ctx, body)
	if err != nil {
		return nil, fmt.Errorf("chat.completions: %w", err)
	}
	choice := resp.Choices[0]
	var msg chatReplyMessage
	if err := json.Unmarshal(choice.Message, &msg); err != nil {
		return nil, fmt.Errorf("chat.completions: decode message: %w", err)
	}
	// 回复里的消息原样放回历史。DeepSeek 要求带工具的请求把 reasoning_content 在之后的每一轮都带回去，
	// 少了就回 400；原样放回也不会丢掉别家加的、这里不认识的字段。
	msgs = append(msgs, choice.Message)
	c.messages = msgs

	cached := max(resp.Usage.PromptTokensDetails.CachedTokens, resp.Usage.PromptCacheHitTokens)
	reply := &Reply{
		MessageID: resp.ID,
		Model:     resp.Model,
		Text:      msg.Content,
		Stop:      stopReason(choice.FinishReason, &msg),
		Usage: Usage{
			// 换成 Claude 的口径：Input 不含缓存命中的部分，否则命中的那些会按全价再算一遍。
			// 这个协议没有单独的“缓存写”计数，CacheWrite 一直是 0。
			Input:     max(resp.Usage.PromptTokens-cached, 0),
			Output:    resp.Usage.CompletionTokens,
			CacheRead: cached,
		},
	}
	if reply.Stop == StopRefusal {
		reply.RefusalCategory = "content_filter"
	}
	if msg.ReasoningContent != "" {
		reply.Thinking = []string{msg.ReasoningContent}
	}
	for _, tc := range msg.ToolCalls {
		input, malformed := toolInput(tc.Function.Arguments)
		reply.Calls = append(reply.Calls, ToolCall{ID: tc.ID, Name: tc.Function.Name, Input: input, Malformed: malformed})
	}
	return reply, nil
}

// stopReason 把 finish_reason 换成循环认的结束原因；认不出的原样传上去（循环按“没有工具调用”处理）。
func stopReason(finish string, msg *chatReplyMessage) StopReason {
	switch {
	case finish == "length":
		return StopMaxTokens
	case finish == "content_filter", msg.Refusal != "" && msg.Content == "" && len(msg.ToolCalls) == 0:
		return StopRefusal
	case len(msg.ToolCalls) > 0:
		return StopToolUse
	case finish == "stop":
		return StopEndTurn
	}
	return StopReason(finish)
}

// toolInput 把工具调用的参数（协议里是一个装着 JSON 的字符串）变成 JSON 值。
// 不是合法 JSON 时（被截断、字符串里的引号没有转义）把原文包成一个 JSON 字符串（运行记录要求它是合法的 JSON），
// 并返回解析的错误：循环不执行这次调用，把错误还给模型让它重发。
func toolInput(arguments string) (input jsontext.Value, malformed string) {
	if strings.TrimSpace(arguments) == "" {
		return jsontext.Value("{}"), ""
	}
	var probe any
	err := json.Unmarshal([]byte(arguments), &probe)
	if err == nil {
		return jsontext.Value(arguments), ""
	}
	quoted, qerr := json.Marshal(arguments)
	if qerr != nil {
		quoted = jsontext.Value(`""`)
	}
	return quoted, err.Error()
}

// retryable 标出值得再试的错误。
type retryable struct{ error }

func (e retryable) Unwrap() error { return e.error }

// complete 发一次请求，失败时按 retry 重试。请求没有副作用（除了花钱），重复发是安全的。
func (o *OpenAI) complete(ctx context.Context, body []byte) (*chatResponse, error) {
	var resp *chatResponse
	err := retry.Do(ctx, o.retry, func(ctx context.Context, _ int) error {
		r, again, err := o.once(ctx, body)
		if err != nil {
			if again {
				return retryable{err}
			}
			return err
		}
		resp = r
		return nil
	})
	return resp, err
}

// once 发一次请求。retry 表示这个错误值得再试。
func (o *OpenAI) once(ctx context.Context, body []byte) (resp *chatResponse, retry bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.cfg.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if o.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+o.cfg.APIKey)
	}
	httpResp, err := o.cfg.HTTP.Do(req)
	if err != nil {
		return nil, true, err
	}
	defer func() { _ = httpResp.Body.Close() }()
	// DeepSeek 在排队和生成期间会先发空行保活，JSON 在最后；读完整个响应体再解析，开头的空白不影响。
	raw, err := io.ReadAll(io.LimitReader(httpResp.Body, maxReplyBytes))
	if err != nil {
		return nil, true, fmt.Errorf("read response: %w", err)
	}
	if httpResp.StatusCode != http.StatusOK {
		// 429 是限流或并发超限，5xx 是服务端的问题，都值得再试；400、401、402（余额不足）、422 再试也一样。
		retry = httpResp.StatusCode == http.StatusTooManyRequests || httpResp.StatusCode >= 500
		return nil, retry, fmt.Errorf("status %d: %s", httpResp.StatusCode, snippet(raw))
	}
	resp = &chatResponse{}
	if err := json.Unmarshal(raw, resp); err != nil {
		return nil, false, fmt.Errorf("decode response: %w", err)
	}
	if len(resp.Choices) == 0 || len(resp.Choices[0].Message) == 0 {
		return nil, false, fmt.Errorf("response has no message: %s", snippet(raw))
	}
	// DeepSeek 用 200 加这两个结束原因表示“推理资源不够、生成被中断”：回复是不完整的，不能放进历史。
	if f := resp.Choices[0].FinishReason; f == "insufficient_system_resource" || f == "aborted" {
		return nil, true, fmt.Errorf("generation interrupted by the server (finish_reason %s)", f)
	}
	return resp, false, nil
}

// snippet 返回错误信息里带的那一小段响应体。
func snippet(raw []byte) string {
	const limit = 500
	s := strings.TrimSpace(string(raw))
	if len(s) > limit {
		s = s[:limit] + "..."
	}
	return s
}
