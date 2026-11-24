package llm

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/anthropics/anthropic-sdk-go/v2"
	"github.com/anthropics/anthropic-sdk-go/v2/option"
)

// 后端的名字。它写进运行记录，不是指标的标签。
const (
	ProviderAnthropic = "anthropic"
	ProviderDeepSeek  = "deepseek"
	ProviderOpenAI    = "openai"
	// ProviderCompatible 是任何说 OpenAI 兼容协议的服务（别的厂商、本地的推理服务），地址必须自己给。
	ProviderCompatible = "openai-compatible"
)

// provider 是一个后端固定不变的那部分。除了 anthropic，其余几个只是同一个适配器的不同预设。
type provider struct {
	// baseURL 为空表示必须在配置里给（anthropic 除外：它的地址由 SDK 定）。
	baseURL string
	// keyEnv 是放密钥的环境变量。
	keyEnv string
	// defaultModel 为空表示必须在配置里给。
	defaultModel   string
	maxTokensField string
	strict         bool
}

// providers 里的事实是 2026-11-24 从各家的文档查的，见 docs/design/3d-diagnosis-agent.md 的第 9 节。
var providers = map[string]provider{
	ProviderAnthropic: {keyEnv: "ANTHROPIC_API_KEY", defaultModel: "claude-opus-5-5"},
	// 严格模式只在 https://api.deepseek.com/beta 上有，而且它支持的 JSON Schema 比报告的 Schema 窄，所以不开；
	// 报告合不合格由循环自己核对。
	ProviderDeepSeek: {
		baseURL: "https://api.deepseek.com", keyEnv: "DEEPSEEK_API_KEY", defaultModel: "deepseek-flash",
		maxTokensField: FieldMaxTokens,
	},
	ProviderOpenAI: {
		baseURL: "https://api.openai.com/v1", keyEnv: "OPENAI_API_KEY",
		maxTokensField: FieldMaxCompletionTokens, strict: true,
	},
	ProviderCompatible: {keyEnv: "LLM_API_KEY", maxTokensField: FieldMaxTokens},
}

// Providers 返回认识的后端的名字。
func Providers() []string {
	return slices.Sorted(maps.Keys(providers))
}

// Options 是选后端用的配置。
type Options struct {
	Provider string
	// Model 为空时用这个后端的默认模型。
	Model string
	// Effort 是思考的深度：anthropic 的 output_config.effort，其余的 reasoning_effort。各家接受的取值不完全一样，
	// 不在这里核对，写错了 API 会回 400。
	Effort string
	// Fallback 只对 anthropic 有意义（服务端的拒绝回退）。
	Fallback bool
	// BaseURL 为空时用这个后端自己的地址。
	BaseURL string
	// APIKey 为空时 Resolve 从这个后端的环境变量里取。
	APIKey string
}

// Resolve 把没有给的项补上（默认模型、地址、从环境变量取密钥），并核对后端的名字。
// 没有密钥不算错：Agent 照常启动，诊断会以 api_error 结束并留下记录。
func (o *Options) Resolve(getenv func(string) string) error {
	p, ok := providers[o.Provider]
	if !ok {
		return fmt.Errorf("unknown provider %q (known: %s)", o.Provider, strings.Join(Providers(), ", "))
	}
	if o.Model == "" {
		o.Model = p.defaultModel
	}
	if o.Model == "" {
		return fmt.Errorf("provider %s has no default model: set one", o.Provider)
	}
	if o.BaseURL == "" {
		o.BaseURL = p.baseURL
	}
	if o.BaseURL == "" && o.Provider != ProviderAnthropic {
		return fmt.Errorf("provider %s needs a base URL", o.Provider)
	}
	if o.APIKey == "" {
		o.APIKey = getenv(p.keyEnv)
	}
	return nil
}

// KeyEnv 返回这个后端放密钥的环境变量名，不认识的后端返回空。
func KeyEnv(name string) string { return providers[name].keyEnv }

// New 按配置返回一个后端。o 要先经过 Resolve。
func New(o Options) (Client, error) {
	p, ok := providers[o.Provider]
	if !ok {
		return nil, fmt.Errorf("unknown provider %q", o.Provider)
	}
	if o.Provider == ProviderAnthropic {
		// 没有给的项留给 SDK 自己从环境里取（ANTHROPIC_API_KEY、ANTHROPIC_BASE_URL 等）。
		var opts []option.RequestOption
		if o.APIKey != "" {
			opts = append(opts, option.WithAPIKey(o.APIKey))
		}
		if o.BaseURL != "" {
			opts = append(opts, option.WithBaseURL(o.BaseURL))
		}
		return NewAnthropic(anthropic.NewClient(opts...), AnthropicConfig{Model: o.Model, Effort: o.Effort, Fallback: o.Fallback}), nil
	}
	return NewOpenAI(OpenAIConfig{
		BaseURL: o.BaseURL, APIKey: o.APIKey, Model: o.Model, Effort: o.Effort,
		MaxTokensField: p.maxTokensField, Strict: p.strict,
	}), nil
}
