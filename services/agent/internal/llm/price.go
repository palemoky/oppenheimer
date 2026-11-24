package llm

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Price 是一个模型的单价，单位是美元每百万 token。
type Price struct {
	Input      float64
	Output     float64
	CacheWrite float64
	CacheRead  float64
	// Peak 不为 nil 表示这个模型分时段计价：上面四个数是高峰时段的，其余时间乘 Peak.OffPeakFactor。
	Peak *PeakSchedule
}

// PeakSchedule 是一家厂商的高峰时段。只在工作日（UTC 的周一到周五）有高峰；
// 厂商说的“法定节假日不算”这里不知道，节假日的工作日会被当成高峰（费用估高、该跑的被推迟，方向都是保守的）。
type PeakSchedule struct {
	// OffPeakFactor 是高峰以外的时间价格乘的系数。
	OffPeakFactor float64
	// Hours 是高峰的时段，UTC 的小时，左闭右开，按时间排好、互不重叠。
	Hours [][2]int
}

// deepseekPeak 是 2026-11-24 从 DeepSeek 的价格页抄的：UTC 周一到周五 01:00 到 04:00、06:00 到 10:00 是高峰，其余时间半价。
var deepseekPeak = &PeakSchedule{OffPeakFactor: 0.5, Hours: [][2]int{{1, 4}, {6, 10}}}

// PeakUntil 回答 t 是不是在高峰时段里；是的话 until 是这一段高峰结束的时刻。p 没有分时段计价时总是返回 false。
func (p Price) PeakUntil(t time.Time) (until time.Time, peak bool) {
	if p.Peak == nil {
		return time.Time{}, false
	}
	t = t.UTC()
	if wd := t.Weekday(); wd == time.Saturday || wd == time.Sunday {
		return time.Time{}, false
	}
	for _, h := range p.Peak.Hours {
		if t.Hour() >= h[0] && t.Hour() < h[1] {
			return time.Date(t.Year(), t.Month(), t.Day(), h[1], 0, 0, 0, time.UTC), true
		}
	}
	return time.Time{}, false
}

// At 返回 t 那个时刻实际的单价。按发请求的时刻算；一次调用跨过时段边界时厂商按哪一头算，文档没有写。
func (p Price) At(t time.Time) Price {
	if p.Peak == nil {
		return p
	}
	if _, peak := p.PeakUntil(t); peak {
		return p
	}
	f := p.Peak.OffPeakFactor
	p.Input, p.Output, p.CacheWrite, p.CacheRead = p.Input*f, p.Output*f, p.CacheWrite*f, p.CacheRead*f
	return p
}

// prices 是 2026-11-24 从 claude-api 参考（缓存日期 2026-11-03）抄的。缓存写按输入价的 1.25 倍（5 分钟的缓存）。
// 没有覆盖的情况：Haiku 5.5 超过 10 万 token 的提示是另一个价；回退到表里没有的模型时按请求的模型算。
// 费用只用来卡上限和做统计，账单以 Console 为准。
//
// DeepSeek 的两个是 2026-11-24 从它的价格页抄的，填的是高峰时段的价，其余时间是一半（见 deepseekPeak）。
// 它没有“缓存写”这个价，适配器也不会报这个数，表里填 0。
// 各家的模型名不重名，所以表只按模型名查。
var prices = map[string]Price{
	"deepseek-flash":  {Input: 0.30, Output: 1.20, CacheRead: 0.006, Peak: deepseekPeak},
	"deepseek-v4-pro": {Input: 1.32, Output: 3.96, CacheRead: 0.044, Peak: deepseekPeak},

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

// ParsePrice 解析配置里手写的单价：“输入,输出,缓存写,缓存读”，美元每百万 token，后两个可以省（按 0 算）。
// 给表里没有的模型用（别的厂商、本地的推理服务写 0,0）。
func ParsePrice(s string) (Price, error) {
	parts := strings.Split(s, ",")
	if len(parts) < 2 || len(parts) > 4 {
		return Price{}, fmt.Errorf("price %q: want input,output[,cache_write[,cache_read]]", s)
	}
	var v [4]float64
	for i, part := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || f < 0 {
			return Price{}, fmt.Errorf("price %q: %q is not a non-negative number", s, part)
		}
		v[i] = f
	}
	return Price{Input: v[0], Output: v[1], CacheWrite: v[2], CacheRead: v[3]}, nil
}

// Cost 返回这些用量按 p 计的美元数。
func (p Price) Cost(u Usage) float64 {
	const perToken = 1e-6
	return (float64(u.Input)*p.Input +
		float64(u.Output)*p.Output +
		float64(u.CacheWrite)*p.CacheWrite +
		float64(u.CacheRead)*p.CacheRead) * perToken
}

// CostOf 按实际作答的模型在 at 那个时刻的价计；模型不在表里时（回退到了别的模型）退回 fallback 的价。
func CostOf(model string, fallback Price, u Usage, at time.Time) float64 {
	p, ok := prices[model]
	if !ok {
		p = fallback
	}
	return p.At(at).Cost(u)
}
