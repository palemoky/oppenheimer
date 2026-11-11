package kafkax_test

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/palemoky/Oppenheimer/pkg/kafkax"
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
