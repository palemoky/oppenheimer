package kafkax

import (
	"testing"
	"time"
)

// 退避按次数翻倍、不超过上限，并且带抖动：同一次失败算出来的等待落在 [0.8d, 1.2d] 里，不是一个固定的数。
func TestBackoffDoublesIsCappedAndJittered(t *testing.T) {
	t.Parallel()
	c := &Consumer{cfg: ConsumerConfig{RetryInitial: 100 * time.Millisecond, RetryMax: time.Second}}
	for attempt, base := range map[int]time.Duration{
		1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond,
		4: 800 * time.Millisecond, 5: time.Second, 9: time.Second,
	} {
		seen := map[time.Duration]bool{}
		for range 200 {
			d := c.backoff(attempt)
			if d < base-base/5 || d > base+base/5 {
				t.Fatalf("backoff(%d) = %v, want within 20%% of %v", attempt, d, base)
			}
			seen[d] = true
		}
		if len(seen) < 20 {
			t.Errorf("backoff(%d) took only %d distinct values in 200 calls, want jitter", attempt, len(seen))
		}
	}
}
