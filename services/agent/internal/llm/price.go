package llm

import "fmt"

// Price 是一个模型的单价，单位是美元每百万 token。
type Price struct {
	Input      float64
	Output     float64
	CacheWrite float64
	CacheRead  float64
}

// prices 是 2026-11-24 从 claude-api 参考（缓存日期 2026-11-03）抄的。缓存写按输入价的 1.25 倍（5 分钟的缓存）。
// 没有覆盖的情况：Haiku 5.5 超过 10 万 token 的提示是另一个价；回退到表里没有的模型时按请求的模型算。
// 费用只用来卡上限和做统计，账单以 Console 为准。
var prices = map[string]Price{
	"claude-fable-5-2":  {Input: 8, Output: 40, CacheWrite: 10, CacheRead: 0.20},
	"claude-opus-5-5":   {Input: 4, Output: 20, CacheWrite: 5, CacheRead: 0.20},
	"claude-sonnet-5-5": {Input: 2, Output: 10, CacheWrite: 2.5, CacheRead: 0.10},
	"claude-haiku-5-5":  {Input: 0.10, Output: 0.50, CacheWrite: 0.125, CacheRead: 0.01},
}

// PriceOf 返回模型的单价。表里没有的模型返回错误：启动时就该失败，不猜一个价。
func PriceOf(model string) (Price, error) {
	p, ok := prices[model]
	if !ok {
		return Price{}, fmt.Errorf("no price for model %q", model)
	}
	return p, nil
}

// Cost 返回这些用量按 p 计的美元数。
func (p Price) Cost(u Usage) float64 {
	const perToken = 1e-6
	return (float64(u.Input)*p.Input +
		float64(u.Output)*p.Output +
		float64(u.CacheWrite)*p.CacheWrite +
		float64(u.CacheRead)*p.CacheRead) * perToken
}

// CostOf 按实际作答的模型计价；它不在表里时（回退到了别的模型）退回 fallback 的价。
func CostOf(model string, fallback Price, u Usage) float64 {
	if p, ok := prices[model]; ok {
		return p.Cost(u)
	}
	return fallback.Cost(u)
}
