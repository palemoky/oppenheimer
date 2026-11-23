// Package llm 是主循环和模型之间的一层薄接口。
//
// 循环只认这里的类型，不认 SDK 的类型，原因有两个：测试里可以换成按脚本回放的假实现；
// 对话的历史（含必须原样带回的思考块）收在实现里面，循环没有机会去改它。
// 历史只能在末尾追加是硬约束：思考块绑定在产生它的那段对话上，回头改写前面的消息会让它失效，
// 也会让缓存的前缀失效。设计见 docs/design/3d-diagnosis-agent.md 的 1.1、3.2。
package llm

import (
	"context"
	"encoding/json/jsontext"
)

// StopReason 是一次回复结束的原因。实现把认不出的值原样传上来，循环按“没有工具调用”处理。
type StopReason string

// 循环要区分的几种结束原因。
const (
	StopEndTurn   StopReason = "end_turn"
	StopToolUse   StopReason = "tool_use"
	StopMaxTokens StopReason = "max_tokens"
	StopRefusal   StopReason = "refusal"
)

// ToolDef 是给模型看的工具定义。
type ToolDef struct {
	Name        string
	Description string
	// Schema 是参数的 JSON Schema（type 为 object 的那一层）。
	Schema map[string]any
	// Strict 为真时由 API 保证参数合乎 Schema（要求 additionalProperties 为 false、字段全部 required）。
	Strict bool
}

// ToolCall 是模型发起的一次工具调用。
type ToolCall struct {
	ID    string
	Name  string
	Input jsontext.Value
}

// ToolResult 是一次工具调用的结果，CallID 对应 ToolCall.ID。
type ToolResult struct {
	CallID  string
	Content string
	IsError bool
}

// Turn 是发给模型的一轮输入。一轮回复里的所有工具调用，结果必须放在同一个 Turn 里；
// Text 排在结果之后。
type Turn struct {
	Results []ToolResult
	Text    string
}

// Usage 是一次回复的 token 用量，四个数分别计价。
type Usage struct {
	Input      int64 `json:"input"`
	Output     int64 `json:"output"`
	CacheWrite int64 `json:"cache_write"`
	CacheRead  int64 `json:"cache_read"`
}

// Add 把 o 累加进来。
func (u *Usage) Add(o Usage) {
	u.Input += o.Input
	u.Output += o.Output
	u.CacheWrite += o.CacheWrite
	u.CacheRead += o.CacheRead
}

// Reply 是模型的一次回复。
type Reply struct {
	// MessageID 是 API 给这条消息的编号，排查时拿它去对。
	MessageID string
	// Model 是实际作答的模型；发生拒绝回退时和请求的不一样。
	Model string
	// FellBack 表示这一轮是回退的模型答的。
	FellBack bool
	Text     string
	// Thinking 是思考的摘要（不是原始的思考过程，那个 API 不给）。
	Thinking []string
	Calls    []ToolCall
	Stop     StopReason
	// RefusalCategory 只在 Stop 为 StopRefusal 时有值。
	RefusalCategory string
	Usage           Usage
}

// Conversation 是一段只能往后追加的对话。不能并发调用。
type Conversation interface {
	// Send 把 turn 追加到对话末尾并返回模型的回复。失败时对话不变。
	Send(ctx context.Context, turn Turn) (*Reply, error)
}

// Client 开新的对话。system 和 tools 在一段对话里不变（它们是缓存的前缀）。
type Client interface {
	NewConversation(system string, tools []ToolDef) Conversation
}
