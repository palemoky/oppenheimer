package llm

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/anthropics/anthropic-sdk-go/v2"
)

// MaxOutputTokens 是一次回复的输出上限。不走流式，所以不取更大的值（大了会撞 HTTP 的超时）。
const MaxOutputTokens = 16000

// betaFallback 是服务端拒绝回退的 beta 标志。
const betaFallback anthropic.AnthropicBeta = "server-side-fallback-2026-07-01"

// AnthropicConfig 是 Claude API 这一侧的配置。
type AnthropicConfig struct {
	Model string
	// Effort 是 output_config.effort（low、medium、high、xhigh、max）。必须显式给：各模型的默认值不一样。
	Effort string
	// Fallback 为真时开服务端的拒绝回退（beta）：请求被安全分类器拒绝时由回退的模型在同一次调用里作答。
	Fallback bool
}

// Anthropic 用官方 SDK 实现 Client。走 beta 命名空间是因为拒绝回退只在那里有。
type Anthropic struct {
	client anthropic.Client
	cfg    AnthropicConfig
}

// NewAnthropic 返回实现。client 由调用方建（凭据、地址、重试次数都在它上面）。
// SDK 自己对 429、5xx 和连接错误重试，那是这条调用链上唯一的一层重试。
func NewAnthropic(client anthropic.Client, cfg AnthropicConfig) *Anthropic {
	return &Anthropic{client: client, cfg: cfg}
}

// NewConversation 实现 Client。
func (a *Anthropic) NewConversation(system string, tools []ToolDef) Conversation {
	params := anthropic.BetaMessageNewParams{
		Model:     a.cfg.Model,
		MaxTokens: MaxOutputTokens,
		// 缓存的前缀按“工具、系统提示、消息”的顺序算。系统提示末尾的断点盖住工具和系统提示；
		// 顶层的 CacheControl 让 API 自己把第二个断点放在对话的末尾，每一轮往后挪。
		System: []anthropic.BetaTextBlockParam{{
			Text:         system,
			CacheControl: anthropic.NewBetaCacheControlEphemeralParam(),
		}},
		CacheControl: anthropic.NewBetaCacheControlEphemeralParam(),
		// 摘要要显式打开（默认是空的），运行记录里才看得到它当时怎么想的。计费和不打开一样。
		Thinking: anthropic.BetaThinkingConfigParamUnion{OfAdaptive: &anthropic.BetaThinkingConfigAdaptiveParam{
			Display: anthropic.BetaThinkingConfigAdaptiveDisplaySummarized,
		}},
		OutputConfig: anthropic.BetaOutputConfigParam{Effort: anthropic.BetaOutputConfigEffort(a.cfg.Effort)},
	}
	if a.cfg.Fallback {
		params.Fallbacks = anthropic.BetaFallbacksParamOfDefault()
		params.Betas = []anthropic.AnthropicBeta{betaFallback}
	}
	for _, t := range tools {
		schema := anthropic.BetaToolInputSchema(t.Schema)
		tp := &anthropic.BetaToolParam{
			Name:        t.Name,
			Description: anthropic.String(t.Description),
			InputSchema: schema,
		}
		if t.Strict {
			tp.Strict = anthropic.Bool(true)
		}
		params.Tools = append(params.Tools, anthropic.BetaToolUnionParam{OfTool: tp})
	}
	return &anthropicConversation{client: a.client, params: params}
}

type anthropicConversation struct {
	client anthropic.Client
	// params.Messages 是到目前为止的全部历史，只在 Send 成功后追加。
	params anthropic.BetaMessageNewParams
}

func (c *anthropicConversation) Send(ctx context.Context, turn Turn) (*Reply, error) {
	blocks := make([]anthropic.BetaContentBlockParamUnion, 0, len(turn.Results)+1)
	for _, r := range turn.Results {
		blocks = append(blocks, anthropic.NewBetaToolResultBlock(r.CallID, r.Content, r.IsError))
	}
	if turn.Text != "" {
		blocks = append(blocks, anthropic.NewBetaTextBlock(turn.Text))
	}
	if len(blocks) == 0 {
		return nil, errors.New("empty turn")
	}

	params := c.params
	// 复制一份再追加：失败时 c.params.Messages 不能留下这条没有得到回复的消息。
	msgs := append(slices.Clone(c.params.Messages), anthropic.NewBetaUserMessage(blocks...))
	params.Messages = msgs
	resp, err := c.client.Beta.Messages.New(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("messages.new: %w", err)
	}
	// 回复原样放回历史（ToParam 带着思考块的签名），下一轮才接得上。
	msgs = append(msgs, resp.ToParam())
	c.params.Messages = msgs

	reply := &Reply{
		MessageID: resp.ID,
		Model:     resp.Model,
		Stop:      StopReason(resp.StopReason),
		Usage: Usage{
			Input:      resp.Usage.InputTokens,
			Output:     resp.Usage.OutputTokens,
			CacheWrite: resp.Usage.CacheCreationInputTokens,
			CacheRead:  resp.Usage.CacheReadInputTokens,
		},
	}
	if reply.Stop == StopRefusal {
		reply.RefusalCategory = string(resp.StopDetails.Category)
	}
	var text strings.Builder
	for _, block := range resp.Content {
		switch b := block.AsAny().(type) {
		case anthropic.BetaTextBlock:
			text.WriteString(b.Text)
		case anthropic.BetaThinkingBlock:
			if b.Thinking != "" {
				reply.Thinking = append(reply.Thinking, b.Thinking)
			}
		case anthropic.BetaToolUseBlock:
			// Input 字段是解析过的值；原始的 JSON 从 JSON.Input.Raw() 取，交给工具自己解析和校验。
			reply.Calls = append(reply.Calls, ToolCall{ID: b.ID, Name: b.Name, Input: jsontext.Value(b.JSON.Input.Raw())})
		case anthropic.BetaFallbackBlock:
			reply.FellBack = true
		}
	}
	reply.Text = text.String()
	return reply, nil
}
