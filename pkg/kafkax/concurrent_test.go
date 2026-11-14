package kafkax_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// 本文件测试分区内按 key 并发消费（ConsumerConfig.Concurrency > 1，设计文档 1d 的 2.2）。

// keyed 返回 n 条消息，key 在 keys 个值里轮转，value 是 "<key>#<该 key 的第几条>"。
func keyed(topic string, n, keys int) []kafkax.Record {
	recs := make([]kafkax.Record, n)
	for i := range n {
		k := "k" + strconv.Itoa(i%keys)
		recs[i] = kafkax.Record{Topic: topic, Key: []byte(k), Value: []byte(k + "#" + strconv.Itoa(i/keys))}
	}
	return recs
}

// distinctKeys 返回 n 条 key 各不相同的消息，value 是 "m<i>"，全部在一个分区的 topic 上位点就是 i。
func distinctKeys(topic string, n int) []kafkax.Record {
	recs := make([]kafkax.Record, n)
	for i := range n {
		recs[i] = kafkax.Record{Topic: topic, Key: []byte("key-" + strconv.Itoa(i)), Value: []byte("m" + strconv.Itoa(i))}
	}
	return recs
}

func concurrentConfig(t *testing.T, e env, concurrency int) kafkax.ConsumerConfig {
	cfg := e.consumerConfig(t)
	cfg.Concurrency = concurrency
	return cfg
}

// 同一个 key 的消息按位点顺序、一次一条地处理；不同 key 确实并行了；最后位点提交到末尾。
func TestConcurrentConsumerKeepsPerKeyOrder(t *testing.T) {
	t.Parallel()
	const (
		keys    = 24
		perKey  = 25
		total   = keys * perKey
		workers = 8
	)
	e := newEnv(t, 1)
	mustSend(t, newProducer(t, e.brokers), keyed(e.topic, total, keys)...)

	var (
		mu       sync.Mutex
		seen     = map[string][]int{}
		busy     = map[string]bool{}
		overlap  []string
		inFlight atomic.Int32
		peak     atomic.Int32
		done     atomic.Int32
	)
	startConsumer(t, concurrentConfig(t, e, workers), func(_ context.Context, m kafkax.Message) error {
		key, seqText, _ := strings.Cut(string(m.Value), "#")
		seq, err := strconv.Atoi(seqText)
		if err != nil {
			return kafkax.Permanent(err)
		}
		mu.Lock()
		if busy[key] {
			overlap = append(overlap, string(m.Value))
		}
		busy[key] = true
		mu.Unlock()
		if n := inFlight.Add(1); n > peak.Load() {
			peak.Store(n) // 只用来判断“有没有并行过”，不需要精确的最大值
		}

		time.Sleep(2 * time.Millisecond)

		inFlight.Add(-1)
		mu.Lock()
		busy[key] = false
		seen[key] = append(seen[key], seq)
		mu.Unlock()
		done.Add(1)
		return nil
	})
	waitFor(t, "all messages", func() bool { return done.Load() >= total })
	waitFor(t, "commit to the end", func() bool { return committed(t, e.brokers, e.group, e.topic)[0] == total })

	mu.Lock()
	defer mu.Unlock()
	if len(overlap) > 0 {
		t.Errorf("messages of one key were handled concurrently: %v", overlap)
	}
	for key, seqs := range seen {
		want := make([]int, perKey)
		for i := range want {
			want[i] = i
		}
		if !slices.Equal(seqs, want) {
			t.Errorf("key %s handled in order %v, want 0..%d ascending", key, seqs, perKey-1)
		}
	}
	if peak.Load() < 2 {
		t.Errorf("peak concurrency = %d, want at least 2 with %d lanes", peak.Load(), workers)
	}
	if peak.Load() > workers {
		t.Errorf("peak concurrency = %d, more than the %d lanes", peak.Load(), workers)
	}
}

// 位点只提交连续完成的前缀：m2 卡住时，后面做完的再多也只提交到 2；
// 强制停止后新的消费者从 m2 重新开始，m2 之后那些已经做完的会重复投递。
func TestConcurrentConsumerCommitsOnlyContiguousPrefix(t *testing.T) {
	t.Parallel()
	const total = 40
	e := newEnv(t, 1)
	mustSend(t, newProducer(t, e.brokers), distinctKeys(e.topic, total)...)

	first := &recorder{}
	r := startConsumer(t, concurrentConfig(t, e, 8), func(ctx context.Context, m kafkax.Message) error {
		if string(m.Value) == "m2" {
			<-ctx.Done() // 一直卡到被强制停止
			return ctx.Err()
		}
		first.add(m)
		return nil
	})
	// m0、m1 的位点在 m2 之前，即使和 m2 同一条 lane 也排在它前面，一定会完成。
	// m2 之后的 37 条分在 8 条 lane 上，不和 m2 同 lane 的都会完成。
	waitFor(t, "commit up to m2", func() bool { return committed(t, e.brokers, e.group, e.topic)[0] == 2 })
	waitFor(t, "messages after m2 to be handled", func() bool {
		return slices.ContainsFunc(first.all(), func(m kafkax.Message) bool { return m.Offset > 2 })
	})
	// 多等一个提交周期，位点仍然不能越过 m2。
	time.Sleep(1500 * time.Millisecond)
	if got := committed(t, e.brokers, e.group, e.topic)[0]; got != 2 {
		t.Fatalf("committed = %d while m2 is unfinished, want 2", got)
	}
	handledAfter := 0
	for _, m := range first.all() {
		if m.Offset > 2 {
			handledAfter++
		}
	}

	if err := r.stop(t, 100*time.Millisecond); err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if got := committed(t, e.brokers, e.group, e.topic)[0]; got != 2 {
		t.Fatalf("committed = %d after a forced stop, want 2", got)
	}

	second := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		second.add(m)
		return nil
	})
	waitFor(t, "redelivery from m2", func() bool { return second.len() >= total-2 })
	got := second.values()
	want := make([]string, 0, total-2)
	for i := 2; i < total; i++ {
		want = append(want, "m"+strconv.Itoa(i))
	}
	if !slices.Equal(got, want) {
		t.Fatalf("second consumer got %v, want m2..m%d in order", got, total-1)
	}
	t.Logf("%d messages after m2 were handled by the first consumer and delivered again to the second", handledAfter)
}

// 未提交的消息数到达窗口后停止分发：m0 卡住时，最多只有 MaxUncommitted-1 条别的消息被处理。
func TestConcurrentConsumerStopsDispatchingAtWindow(t *testing.T) {
	t.Parallel()
	const (
		total  = 200
		window = 16
	)
	e := newEnv(t, 1)
	mustSend(t, newProducer(t, e.brokers), distinctKeys(e.topic, total)...)

	release := make(chan struct{})
	var handled atomic.Int32
	cfg := concurrentConfig(t, e, 4)
	cfg.MaxUncommitted = window
	startConsumer(t, cfg, func(ctx context.Context, m kafkax.Message) error {
		if string(m.Value) == "m0" {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		handled.Add(1)
		return nil
	})

	// m0 占着窗口里的一个位置，所以别的消息最多处理 window-1 条。和 m0 同 lane 的排在它后面，可能更少。
	time.Sleep(time.Second)
	if n := handled.Load(); n > window-1 {
		t.Fatalf("handled %d messages while m0 is stuck, want at most %d (window %d)", n, window-1, window)
	}
	if got := committed(t, e.brokers, e.group, e.topic)[0]; got != 0 {
		t.Fatalf("committed = %d while m0 is stuck, want nothing committed", got)
	}

	close(release)
	waitFor(t, "all messages after release", func() bool { return handled.Load() >= total })
	waitFor(t, "commit to the end", func() bool { return committed(t, e.brokers, e.group, e.topic)[0] == total })
}

// 重试只停住它所在的 lane；重试耗尽进死信的消息算作完成，位点越过它。
func TestConcurrentConsumerRetryAndDeadLetterStayInLane(t *testing.T) {
	t.Parallel()
	const total = 60
	e := newEnv(t, 1)
	mustSend(t, newProducer(t, e.brokers), distinctKeys(e.topic, total)...)

	var (
		mu       sync.Mutex
		attempts = map[string]int{}
	)
	rec := &recorder{}
	cfg := concurrentConfig(t, e, 8)
	cfg.MaxAttempts = 3
	startConsumer(t, cfg, func(_ context.Context, m kafkax.Message) error {
		v := string(m.Value)
		mu.Lock()
		attempts[v]++
		n := attempts[v]
		mu.Unlock()
		switch {
		case v == "m5" && n < 3:
			return errors.New("transient")
		case v == "m7":
			return errors.New("always fails")
		case v == "m9":
			return kafkax.Permanent(errors.New("poison"))
		}
		rec.add(m)
		return nil
	})
	waitFor(t, "everything but the two dead letters", func() bool { return rec.distinct() >= total-2 })
	waitFor(t, "commit to the end", func() bool { return committed(t, e.brokers, e.group, e.topic)[0] == total })

	dead := readAll(t, e.brokers, e.dlq, 2)
	got := []string{string(dead[0].Value), string(dead[1].Value)}
	slices.Sort(got)
	if !slices.Equal(got, []string{"m7", "m9"}) {
		t.Errorf("dead letters = %v, want [m7 m9]", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts["m5"] != 3 || attempts["m7"] != 3 || attempts["m9"] != 1 {
		t.Errorf("attempts m5=%d m7=%d m9=%d, want 3, 3, 1", attempts["m5"], attempts["m7"], attempts["m9"])
	}
	for v, n := range attempts {
		if n != 1 && v != "m5" && v != "m7" {
			t.Errorf("%s was handled %d times, want once", v, n)
		}
	}
}

// 正常停止：等在途的做完，提交连续前缀；重启后不丢消息，重复的只可能是前缀之后已经做完的那些。
func TestConcurrentConsumerGracefulStopLosesNothing(t *testing.T) {
	t.Parallel()
	const total = 3000
	e := newEnv(t, 4)
	mustSend(t, newProducer(t, e.brokers), keyed(e.topic, total, 500)...)

	var (
		mu    sync.Mutex
		count = map[string]int{}
	)
	handler := func(_ context.Context, m kafkax.Message) error {
		time.Sleep(200 * time.Microsecond)
		mu.Lock()
		count[string(m.Value)]++
		mu.Unlock()
		return nil
	}
	distinct := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(count)
	}

	r := startConsumer(t, concurrentConfig(t, e, 8), handler)
	waitFor(t, "some progress", func() bool { return distinct() >= total/5 })
	if err := r.stop(t, 15*time.Second); err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	afterFirst := distinct()
	var committedFirst int64
	for _, off := range committed(t, e.brokers, e.group, e.topic) {
		committedFirst += off
	}
	// 正常停止时每条 lane 做完手上的那一条，已分发没开始的放弃，所以已提交的不会多于已处理的。
	if committedFirst > int64(afterFirst) {
		t.Fatalf("committed %d offsets but only %d messages were handled", committedFirst, afterFirst)
	}

	startConsumer(t, concurrentConfig(t, e, 8), handler)
	waitFor(t, "all messages", func() bool { return distinct() >= total })
	waitFor(t, "commit to the end", func() bool {
		var sum int64
		for _, off := range committed(t, e.brokers, e.group, e.topic) {
			sum += off
		}
		return sum == total
	})

	mu.Lock()
	defer mu.Unlock()
	dup := 0
	for _, n := range count {
		if n > 1 {
			dup++
		}
	}
	// 重复的上界：停止时每个分区的窗口里已完成、但排在未完成的后面的那些。
	if limit := 4 * 1024; dup > limit {
		t.Errorf("%d messages were handled more than once, more than 4 partitions x window", dup)
	}
	t.Logf("first consumer handled %d and committed %d; %d messages were handled twice", afterFirst, committedFirst, dup)
}

// 两个成员的组里中途加入第二个成员（再平衡）：每条消息至少处理一次，位点最终到末尾。
func TestConcurrentConsumerRebalance(t *testing.T) {
	t.Parallel()
	const total = 4000
	e := newEnv(t, 4)
	mustSend(t, newProducer(t, e.brokers), keyed(e.topic, total, 400)...)

	var (
		mu    sync.Mutex
		count = map[string]int{}
	)
	handler := func(_ context.Context, m kafkax.Message) error {
		time.Sleep(300 * time.Microsecond)
		mu.Lock()
		count[string(m.Value)]++
		mu.Unlock()
		return nil
	}
	distinct := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(count)
	}
	startConsumer(t, concurrentConfig(t, e, 8), handler)
	waitFor(t, "first member to make progress", func() bool { return distinct() >= total/10 })
	startConsumer(t, concurrentConfig(t, e, 8), handler)

	waitFor(t, "all messages", func() bool { return distinct() >= total })
	waitFor(t, "commit to the end", func() bool {
		var sum int64
		for _, off := range committed(t, e.brokers, e.group, e.topic) {
			sum += off
		}
		return sum == total
	})
}

func TestConcurrencyIsValidated(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	noop := func(context.Context, kafkax.Message) error { return nil }

	cfg := concurrentConfig(t, e, -1)
	if _, err := kafkax.NewConsumer(ctx, cfg, noop); err == nil {
		t.Error("negative concurrency accepted")
	}
	// 延迟消费者依赖分区内按到期时间有序，不能并发。
	delay := kafkax.DelayConsumerConfig{ConsumerConfig: concurrentConfig(t, e, 2)}
	if _, err := kafkax.NewDelayConsumer(ctx, delay, noop); err == nil {
		t.Error("delay consumer with concurrency 2 accepted")
	} else if !strings.Contains(err.Error(), "delay") {
		t.Errorf("unexpected error: %v", err)
	}
	delay.Concurrency = 1
	c, err := kafkax.NewDelayConsumer(ctx, delay, noop)
	if err != nil {
		t.Fatalf("delay consumer with concurrency 1: %v", err)
	}
	if err := c.Stop(ctx); err != nil {
		t.Errorf("stop: %v", fmt.Errorf("%w", err))
	}
}
