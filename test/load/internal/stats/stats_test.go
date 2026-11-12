package stats

import (
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	var s []time.Duration
	for i := range 100 {
		s = append(s, time.Duration(i+1)*time.Millisecond)
	}
	for _, tc := range []struct {
		p    float64
		want time.Duration
	}{
		{0.50, 50 * time.Millisecond},
		{0.90, 90 * time.Millisecond},
		{0.99, 99 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{0.001, 1 * time.Millisecond},
	} {
		if got := Percentile(s, tc.p); got != tc.want {
			t.Errorf("p%.3f = %v, want %v", tc.p, got, tc.want)
		}
	}
	if got := Percentile(nil, 0.5); got != 0 {
		t.Errorf("empty = %v", got)
	}
	if got := Percentile([]time.Duration{7}, 0.99); got != 7 {
		t.Errorf("single = %v", got)
	}
}

func TestSummarizeUnsorted(t *testing.T) {
	s := []time.Duration{5, 1, 4, 2, 3}
	got := Summarize(s)
	want := Summary{Count: 5, Mean: 3, P50: 3, P90: 5, P99: 5, Max: 5}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if (Summarize(nil) != Summary{}) {
		t.Error("empty summary not zero")
	}
}

func TestRecorderConcurrent(t *testing.T) {
	var r Recorder
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				r.Add(time.Duration(rand.IntN(1000)) * time.Microsecond) //nolint:gosec // 测试数据
			}
		})
	}
	// 并发读取不影响写入。
	wg.Go(func() { _ = r.Summary() })
	wg.Wait()
	s := r.Summary()
	if s.Count != 8000 {
		t.Errorf("count = %d", s.Count)
	}
	if s.P50 > s.P90 || s.P90 > s.P99 || s.P99 > s.Max {
		t.Errorf("percentiles not monotonic: %+v", s)
	}
}
