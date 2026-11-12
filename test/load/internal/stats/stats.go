// Package stats 收集延迟样本并计算分位数。
//
// 压测的样本量在百万以内，直接保存全部样本、结束时排序，结果精确，内存也只是每个样本 8 字节。
package stats

import (
	"math"
	"slices"
	"sync"
	"time"
)

// Recorder 保存延迟样本，可以被多个 goroutine 同时调用。
type Recorder struct {
	mu      sync.Mutex
	samples []time.Duration
}

// Add 记录一个样本。
func (r *Recorder) Add(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d)
	r.mu.Unlock()
}

// Summary 是一组样本的统计。
type Summary struct {
	Count int
	Mean  time.Duration
	P50   time.Duration
	P90   time.Duration
	P99   time.Duration
	Max   time.Duration
}

// Summary 计算当前样本的统计，不影响后续的 Add。
func (r *Recorder) Summary() Summary {
	r.mu.Lock()
	sorted := slices.Clone(r.samples)
	r.mu.Unlock()
	return Summarize(sorted)
}

// Summarize 计算样本的统计。会对 samples 原地排序。
func Summarize(samples []time.Duration) Summary {
	if len(samples) == 0 {
		return Summary{}
	}
	slices.Sort(samples)
	var sum time.Duration
	for _, d := range samples {
		sum += d
	}
	return Summary{
		Count: len(samples),
		Mean:  sum / time.Duration(len(samples)),
		P50:   Percentile(samples, 0.50),
		P90:   Percentile(samples, 0.90),
		P99:   Percentile(samples, 0.99),
		Max:   samples[len(samples)-1],
	}
}

// Percentile 用最近秩法（nearest-rank）取已排序样本的 p 分位（0 < p ≤ 1）：
// 第 ceil(p·n) 小的样本。结果一定是某个真实样本，不做插值。
func Percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := int(math.Ceil(p * float64(n)))
	return sorted[min(max(rank, 1), n)-1]
}
