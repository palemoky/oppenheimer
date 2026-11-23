package kafkax_test

import (
	"context"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

func startDelayConsumer(t *testing.T, cfg kafkax.ConsumerConfig, clock kafkax.Clock, h kafkax.Handler) *runner {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	c, err := kafkax.NewDelayConsumer(ctx, kafkax.DelayConsumerConfig{ConsumerConfig: cfg, Clock: clock}, h)
	if err != nil {
		t.Fatalf("new delay consumer: %v", err)
	}
	return start(t, c)
}

func TestDelayConsumerWaitsUntilDue(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	clock := newFakeClock()
	mustSend(t, p,
		kafkax.NewDelayRecord(e.topic, []byte("o1"), []byte("late"), clock.Now().Add(15*time.Minute)),
		// 没有 due-at 的消息视为立即到期，但排在未到期消息后面，要等前面的处理完。
		kafkax.Record{Topic: e.topic, Value: []byte("no-header")},
	)

	rec := &recorder{}
	startDelayConsumer(t, e.consumerConfig(t), clock, func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	})
	waitFor(t, "consumer waiting on due time", func() bool { return clock.pending() == 1 })
	time.Sleep(300 * time.Millisecond)
	if rec.len() != 0 {
		t.Fatalf("processed %v before due", rec.values())
	}

	clock.Advance(15*time.Minute - time.Second)
	time.Sleep(200 * time.Millisecond)
	if rec.len() != 0 {
		t.Fatalf("processed %v a second before due", rec.values())
	}
	clock.Advance(time.Second)
	waitFor(t, "messages after due", func() bool { return rec.len() >= 2 })
	if got := rec.values(); !slices.Equal(got, []string{"late", "no-header"}) {
		t.Fatalf("got %v, want [late no-header]", got)
	}
}

func TestDelayConsumerRealClock(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	due := time.Now().Add(time.Second)
	mustSend(t, p, kafkax.NewDelayRecord(e.topic, nil, []byte("v"), due))

	got := make(chan time.Time, 1)
	startDelayConsumer(t, e.consumerConfig(t), nil, func(context.Context, kafkax.Message) error {
		got <- time.Now()
		return nil
	})
	select {
	case at := <-got:
		if at.Before(due) {
			t.Fatalf("processed at %v, before due %v", at, due)
		}
	case <-time.After(waitTimeout):
		t.Fatal("delay message never processed")
	}
}

func TestDelayConsumerPartitionsIndependent(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 2)
	clock := newFakeClock()
	now := clock.Now()
	// 分区 0 的消息 15 分钟后到期，分区 1 的消息已经到期。
	sendDelayToPartitions(t, e, []time.Time{now.Add(15 * time.Minute), now.Add(-time.Second)})

	rec := &recorder{}
	startDelayConsumer(t, e.consumerConfig(t), clock, func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	})
	waitFor(t, "partition 1 processed", func() bool { return rec.len() >= 1 })
	if got := rec.values(); !slices.Equal(got, []string{"p1"}) {
		t.Fatalf("got %v, want [p1] while partition 0 waits", got)
	}
	clock.Advance(15 * time.Minute)
	waitFor(t, "partition 0 processed", func() bool { return rec.len() >= 2 })
}

// sendDelayToPartitions 把到期时间为 dues[i] 的消息 "p<i>" 发到分区 i。
func sendDelayToPartitions(t *testing.T, e env, dues []time.Time) {
	t.Helper()
	values := make([]string, len(dues))
	for i := range dues {
		values[i] = "p" + strconv.Itoa(i)
	}
	sendToPartitionsWithHeaders(t, e.brokers, e.topic, values, func(i int) map[string]string {
		return kafkax.NewDelayRecord("", nil, nil, dues[i]).Headers
	})
}

func TestDelayConsumerStopWhileWaiting(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	clock := newFakeClock()
	mustSend(t, p, kafkax.NewDelayRecord(e.topic, nil, []byte("v"), clock.Now().Add(15*time.Minute)))

	rec := &recorder{}
	h := func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	}
	r := startDelayConsumer(t, e.consumerConfig(t), clock, h)
	waitFor(t, "consumer waiting on due time", func() bool { return clock.pending() == 1 })

	begin := time.Now()
	if err := r.stop(t, 10*time.Second); err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if d := time.Since(begin); d > 5*time.Second {
		t.Fatalf("Stop took %v while waiting", d)
	}
	if rec.len() != 0 || len(committed(t, e.brokers, e.group, e.topic)) != 0 {
		t.Fatal("waiting message was processed or committed")
	}

	// 时钟过了到期时间后，新的同组消费者重新读到这条消息并处理。
	clock.Advance(15 * time.Minute)
	startDelayConsumer(t, e.consumerConfig(t), clock, h)
	waitFor(t, "message after restart", func() bool { return rec.len() >= 1 })
}

// 以下三个测试是延迟消费者加并发（Concurrency > 1）：等到期在分发时做，到期之后按 key 并行。

// 已经到期的消息确实并行了，全部处理完之后位点提交到末尾。
func TestDelayConsumerConcurrentAfterDue(t *testing.T) {
	t.Parallel()
	const total = 16
	e := newEnv(t, 1)
	clock := newFakeClock()
	recs := make([]kafkax.Record, total)
	for i := range total {
		recs[i] = kafkax.NewDelayRecord(e.topic, []byte("order-"+strconv.Itoa(i)), []byte("m"+strconv.Itoa(i)), clock.Now().Add(-time.Second))
	}
	mustSend(t, newProducer(t, e.brokers), recs...)

	var inFlight, peak atomic.Int32
	release := make(chan struct{})
	rec := &recorder{}
	startDelayConsumer(t, concurrentConfig(t, e, 4), clock, func(ctx context.Context, m kafkax.Message) error {
		if n := inFlight.Add(1); n > peak.Load() {
			peak.Store(n) // 只用来判断“有没有并行过”，不需要精确的最大值
		}
		defer inFlight.Add(-1)
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		rec.add(m)
		return nil
	})
	// 16 个不同的 key 分到 4 条 lane，全落在同一条上的概率可以忽略；串行的话这里永远等不到 2。
	waitFor(t, "two handlers in flight", func() bool { return inFlight.Load() >= 2 })
	close(release)
	waitFor(t, "all processed", func() bool { return rec.distinct() == total })
	waitFor(t, "offset committed to the end", func() bool { return committed(t, e.brokers, e.group, e.topic)[0] == total })
	if peak.Load() < 2 {
		t.Fatalf("peak in-flight %d, want at least 2", peak.Load())
	}
}

// 队首没到期时，它后面已经到期的消息也不能处理：并发不能让后面的越过队首。
// 队首之前已经处理完的那一条的位点可以提交，没到期的那条不能。
func TestDelayConsumerConcurrentHeadBlocks(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	clock := newFakeClock()
	now := clock.Now()
	mustSend(t, newProducer(t, e.brokers),
		kafkax.NewDelayRecord(e.topic, []byte("a"), []byte("due-0"), now.Add(-time.Second)),
		kafkax.NewDelayRecord(e.topic, []byte("b"), []byte("late-1"), now.Add(15*time.Minute)),
		kafkax.NewDelayRecord(e.topic, []byte("c"), []byte("due-2"), now.Add(-time.Second)),
		kafkax.NewDelayRecord(e.topic, []byte("d"), []byte("due-3"), now.Add(-time.Second)),
	)

	rec := &recorder{}
	startDelayConsumer(t, concurrentConfig(t, e, 4), clock, func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	})
	waitFor(t, "consumer waiting on the head", func() bool { return clock.pending() == 1 })
	waitFor(t, "first message committed", func() bool { return committed(t, e.brokers, e.group, e.topic)[0] == 1 })
	time.Sleep(300 * time.Millisecond)
	if got := rec.values(); !slices.Equal(got, []string{"due-0"}) {
		t.Fatalf("processed %v while the head waits, want [due-0]", got)
	}
	if at := committed(t, e.brokers, e.group, e.topic)[0]; at != 1 {
		t.Fatalf("committed offset %d while the head waits, want 1", at)
	}

	clock.Advance(15 * time.Minute)
	waitFor(t, "the rest processed", func() bool { return rec.distinct() == 4 })
	waitFor(t, "offset committed to the end", func() bool { return committed(t, e.brokers, e.group, e.topic)[0] == 4 })
}

// 并发的延迟消费者在等到期时被停止：马上返回，没到期的那条没有处理、位点没有越过它，重启之后重新读到。
func TestDelayConsumerConcurrentStopWhileWaiting(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	clock := newFakeClock()
	mustSend(t, newProducer(t, e.brokers),
		kafkax.NewDelayRecord(e.topic, []byte("a"), []byte("late"), clock.Now().Add(15*time.Minute)),
		kafkax.NewDelayRecord(e.topic, []byte("b"), []byte("after"), clock.Now().Add(15*time.Minute)),
	)

	rec := &recorder{}
	h := func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	}
	r := startDelayConsumer(t, concurrentConfig(t, e, 4), clock, h)
	waitFor(t, "consumer waiting on due time", func() bool { return clock.pending() == 1 })

	begin := time.Now()
	if err := r.stop(t, 10*time.Second); err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if d := time.Since(begin); d > 5*time.Second {
		t.Fatalf("Stop took %v while waiting", d)
	}
	if rec.len() != 0 || len(committed(t, e.brokers, e.group, e.topic)) != 0 {
		t.Fatal("waiting message was processed or committed")
	}

	clock.Advance(15 * time.Minute)
	startDelayConsumer(t, concurrentConfig(t, e, 4), clock, h)
	waitFor(t, "messages after restart", func() bool { return rec.distinct() == 2 })
}
