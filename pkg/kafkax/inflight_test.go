package kafkax

import (
	"math/rand/v2"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func offsetOf(rec *kgo.Record) int64 {
	if rec == nil {
		return -1
	}
	return rec.Offset
}

func TestPrefixTrackerAdvancesOnlyOverContiguousDone(t *testing.T) {
	t.Parallel()
	var tr prefixTracker
	for _, off := range []int64{5, 6, 7, 8} {
		tr.add(&kgo.Record{Offset: off})
	}
	// 后面的先完成：前缀不动。
	for _, off := range []int64{7, 6} {
		if got := offsetOf(tr.complete(off)); got != -1 {
			t.Fatalf("complete(%d) advanced to %d while 5 is unfinished", off, got)
		}
	}
	if tr.len() != 4 {
		t.Fatalf("len = %d, want 4: finished records behind an unfinished one still count", tr.len())
	}
	// 最前面的完成：前缀一次越过 5、6、7，停在 8 前面。
	if got := offsetOf(tr.complete(5)); got != 7 {
		t.Fatalf("complete(5) advanced to %d, want 7", got)
	}
	if tr.len() != 1 {
		t.Fatalf("len = %d, want 1", tr.len())
	}
	if got := offsetOf(tr.complete(8)); got != 8 {
		t.Fatalf("complete(8) advanced to %d, want 8", got)
	}
	if tr.len() != 0 || tr.pending != nil {
		t.Fatalf("tracker not empty after everything completed: %d", tr.len())
	}
}

// 偏移量有空洞（压缩、事务控制记录）时照样工作；不认识的偏移量和重复完成都不改变状态。
func TestPrefixTrackerGapsAndUnknownOffsets(t *testing.T) {
	t.Parallel()
	var tr prefixTracker
	for _, off := range []int64{10, 13, 40} {
		tr.add(&kgo.Record{Offset: off})
	}
	for _, off := range []int64{11, 9, 41} {
		if got := offsetOf(tr.complete(off)); got != -1 || tr.len() != 3 {
			t.Fatalf("complete(%d) of an unknown offset changed the tracker (advanced to %d, len %d)", off, got, tr.len())
		}
	}
	if got := offsetOf(tr.complete(13)); got != -1 {
		t.Fatalf("complete(13) advanced to %d", got)
	}
	if got := offsetOf(tr.complete(13)); got != -1 {
		t.Fatalf("completing 13 twice advanced to %d", got)
	}
	if got := offsetOf(tr.complete(10)); got != 13 {
		t.Fatalf("complete(10) advanced to %d, want 13", got)
	}
	if got := offsetOf(tr.complete(10)); got != -1 {
		t.Fatalf("completing an already popped offset advanced to %d", got)
	}
}

// 随机的分发和完成顺序下，前缀始终等于“最小的未完成偏移量之前的最后一条”，而且只增不减。
func TestPrefixTrackerRandomOrder(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		var (
			tr         prefixTracker
			next       int64
			unfinished = map[int64]bool{}
			inFlight   []int64
			committed  int64 = -1
		)
		for step := 0; step < 300 || len(inFlight) > 0; step++ {
			if step < 300 && (len(inFlight) == 0 || rng.IntN(2) == 0) {
				next += 1 + int64(rng.IntN(3)) // 带空洞
				tr.add(&kgo.Record{Offset: next})
				unfinished[next] = true
				inFlight = append(inFlight, next)
				continue
			}
			i := rng.IntN(len(inFlight))
			off := inFlight[i]
			inFlight[i] = inFlight[len(inFlight)-1]
			inFlight = inFlight[:len(inFlight)-1]
			delete(unfinished, off)

			if got := tr.complete(off); got != nil {
				if got.Offset <= committed {
					t.Fatalf("prefix went from %d back to %d", committed, got.Offset)
				}
				committed = got.Offset
			}
			for u := range unfinished {
				if u <= committed {
					t.Fatalf("prefix %d passed the unfinished offset %d", committed, u)
				}
			}
		}
		if committed != next || tr.len() != 0 {
			t.Fatalf("after everything completed: prefix %d, want %d; len %d", committed, next, tr.len())
		}
	}
}
