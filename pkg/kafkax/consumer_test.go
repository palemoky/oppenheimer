package kafkax_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/Oppenheimer/pkg/kafkax"
)

func values(prefix string, n int) []kafkax.Record {
	recs := make([]kafkax.Record, n)
	for i := range n {
		recs[i] = kafkax.Record{Key: []byte("k" + strconv.Itoa(i%64)), Value: []byte(prefix + strconv.Itoa(i))}
	}
	return recs
}

func withTopic(topic string, recs []kafkax.Record) []kafkax.Record {
	for i := range recs {
		recs[i].Topic = topic
	}
	return recs
}

func TestConsumerProcessesAllAndCommits(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 4)
	p := newProducer(t, e.brokers)
	const n = 100
	mustSend(t, p, withTopic(e.topic, values("m", n))...)

	rec := &recorder{}
	r := startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	})
	waitFor(t, "all messages", func() bool { return rec.len() >= n })
	if err := r.stop(t, 15*time.Second); err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if got := rec.len(); got != n || rec.distinct() != n {
		t.Fatalf("handled %d messages (%d distinct), want %d exactly once", got, rec.distinct(), n)
	}

	// 每个分区提交的位点等于该分区收到的最大位点 + 1。
	want := make(map[int32]int64)
	for _, m := range rec.all() {
		want[m.Partition] = max(want[m.Partition], m.Offset+1)
	}
	if got := committed(t, e.brokers, e.group, e.topic); !mapsEqual(got, want) {
		t.Fatalf("committed = %v, want %v", got, want)
	}

	// 同组的新消费者不会再收到已处理的消息，只收到之后新发的。
	rec2 := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		rec2.add(m)
		return nil
	})
	mustSend(t, p, kafkax.Record{Topic: e.topic, Key: []byte("k0"), Value: []byte("after-restart")})
	waitFor(t, "message after restart", func() bool { return rec2.len() >= 1 })
	time.Sleep(200 * time.Millisecond)
	if got := rec2.values(); !slices.Equal(got, []string{"after-restart"}) {
		t.Fatalf("restarted consumer got %v, want only after-restart", got)
	}
}

func mapsEqual(a, b map[int32]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestConsumerCommitsOnlyAfterSuccess(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	mustSend(t, p, withTopic(e.topic, values("m", 3))...)

	// 第一个消费者：m0 成功，m1 一直阻塞到 ctx 被取消（模拟处理到一半被强制停止）。
	blocked := make(chan struct{})
	r := startConsumer(t, e.consumerConfig(t), func(ctx context.Context, m kafkax.Message) error {
		if string(m.Value) == "m1" {
			close(blocked)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	select {
	case <-blocked:
	case <-time.After(waitTimeout):
		t.Fatal("handler never reached m1")
	}
	// Stop 期限很短：handler 不会在期限内返回，Stop 取消 handler 的 ctx 后返回。
	if err := r.stop(t, 100*time.Millisecond); err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if got := committed(t, e.brokers, e.group, e.topic); got[0] != 1 {
		t.Fatalf("committed = %v, want partition 0 at 1", got)
	}

	// 新的同组消费者必须从 m1 重新开始。
	rec := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		rec.add(m)
		return nil
	})
	waitFor(t, "m1 and m2", func() bool { return rec.len() >= 2 })
	if got := rec.values(); !slices.Equal(got, []string{"m1", "m2"}) {
		t.Fatalf("second consumer got %v, want [m1 m2]", got)
	}
}

func TestConsumerRetryBlocksPartition(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	mustSend(t, p, withTopic(e.topic, values("m", 5))...)

	var (
		mu    sync.Mutex
		calls []string
		fails int
	)
	rec := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, string(m.Value))
		if string(m.Value) == "m1" && fails < 2 {
			fails++
			return errors.New("transient")
		}
		rec.add(m)
		return nil
	})
	waitFor(t, "all messages", func() bool { return rec.len() >= 5 })

	mu.Lock()
	defer mu.Unlock()
	want := []string{"m0", "m1", "m1", "m1", "m2", "m3", "m4"}
	if !slices.Equal(calls, want) {
		t.Fatalf("handler calls = %v, want %v", calls, want)
	}
}

func TestConsumerDeadLetter(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	mustSend(t, p,
		kafkax.Record{Topic: e.topic, Key: []byte("kp"), Value: []byte("perm"), Headers: map[string]string{"trace": "t1"}},
		kafkax.Record{Topic: e.topic, Key: []byte("kf"), Value: []byte("fail")},
		kafkax.Record{Topic: e.topic, Key: []byte("ko"), Value: []byte("ok")},
	)

	var (
		mu    sync.Mutex
		calls = make(map[string]int)
	)
	done := make(chan struct{})
	cfg := e.consumerConfig(t)
	cfg.MaxAttempts = 3
	startConsumer(t, cfg, func(_ context.Context, m kafkax.Message) error {
		mu.Lock()
		defer mu.Unlock()
		calls[string(m.Value)]++
		switch string(m.Value) {
		case "perm":
			return kafkax.Permanent(errors.New("bad payload"))
		case "fail":
			return fmt.Errorf("attempt %d failed", calls["fail"])
		default:
			close(done)
			return nil
		}
	})
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("message after dead letters was not processed")
	}
	mu.Lock()
	if calls["perm"] != 1 || calls["fail"] != 3 || calls["ok"] != 1 {
		t.Errorf("calls = %v, want perm:1 fail:3 ok:1", calls)
	}
	mu.Unlock()

	dead := readAll(t, e.brokers, e.dlq, 2)
	if len(dead) != 2 {
		t.Fatalf("got %d dead letters, want 2", len(dead))
	}
	type want struct {
		key, value, err, offset, attempts string
		extra                             map[string]string
	}
	wants := []want{
		{key: "kp", value: "perm", err: "bad payload", offset: "0", attempts: "1", extra: map[string]string{"trace": "t1"}},
		{key: "kf", value: "fail", err: "attempt 3 failed", offset: "1", attempts: "3"},
	}
	for i, w := range wants {
		d := dead[i]
		h := make(map[string]string)
		for _, kv := range d.Headers {
			h[kv.Key] = string(kv.Value)
		}
		if string(d.Key) != w.key || string(d.Value) != w.value {
			t.Errorf("dead letter %d: key=%s value=%s, want %s %s", i, d.Key, d.Value, w.key, w.value)
		}
		checks := map[string]string{
			kafkax.HeaderDLQError:     w.err,
			kafkax.HeaderDLQTopic:     e.topic,
			kafkax.HeaderDLQPartition: "0",
			kafkax.HeaderDLQOffset:    w.offset,
			kafkax.HeaderDLQGroup:     e.group,
			kafkax.HeaderDLQAttempts:  w.attempts,
		}
		maps.Copy(checks, w.extra)
		for k, v := range checks {
			if h[k] != v {
				t.Errorf("dead letter %d: header %s = %q, want %q", i, k, h[k], v)
			}
		}
	}
	waitFor(t, "commit after dead letters", func() bool {
		return committed(t, e.brokers, e.group, e.topic)[0] == 3
	})
}

func TestConsumerPartitionOrder(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 3)
	p := newProducer(t, e.brokers)
	const n = 300
	mustSend(t, p, withTopic(e.topic, values("m", n))...)

	var (
		mu       sync.Mutex
		last     = make(map[int32]int64)
		inFlight = make(map[int32]int)
		bad      []string
	)
	rec := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		mu.Lock()
		if l, ok := last[m.Partition]; ok && m.Offset <= l {
			bad = append(bad, fmt.Sprintf("partition %d: offset %d after %d", m.Partition, m.Offset, l))
		}
		last[m.Partition] = m.Offset
		inFlight[m.Partition]++
		if inFlight[m.Partition] > 1 {
			bad = append(bad, fmt.Sprintf("partition %d: concurrent handlers", m.Partition))
		}
		mu.Unlock()

		time.Sleep(time.Millisecond)

		mu.Lock()
		inFlight[m.Partition]--
		mu.Unlock()
		rec.add(m)
		return nil
	})
	waitFor(t, "all messages", func() bool { return rec.len() >= n })
	mu.Lock()
	defer mu.Unlock()
	if len(bad) > 0 {
		t.Fatalf("ordering violations: %v", bad)
	}
}

func TestConsumerPartitionsInParallel(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 4)
	sendToPartitions(t, e.brokers, e.topic, "p0", "p1", "p2", "p3")

	// 每个 handler 都等到 4 个分区同时在处理中才返回：串行处理的话永远等不到。
	var active atomic.Int32
	all := make(chan struct{})
	var once sync.Once
	rec := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(ctx context.Context, m kafkax.Message) error {
		if active.Add(1) == 4 {
			once.Do(func() { close(all) })
		}
		select {
		case <-all:
		case <-time.After(waitTimeout):
			return kafkax.Permanent(errors.New("partitions were not processed in parallel"))
		case <-ctx.Done():
			return ctx.Err()
		}
		rec.add(m)
		return nil
	})
	waitFor(t, "all partitions", func() bool { return rec.len() >= 4 })
}

func TestConsumerRebalance(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 4)
	p := newProducer(t, e.brokers)
	const n = 400
	mustSend(t, p, withTopic(e.topic, values("m", n))...)

	recA, recB := &recorder{}, &recorder{}
	slow := func(rec *recorder) kafkax.Handler {
		return func(_ context.Context, m kafkax.Message) error {
			time.Sleep(5 * time.Millisecond)
			rec.add(m)
			return nil
		}
	}
	startConsumer(t, e.consumerConfig(t), slow(recA))
	waitFor(t, "consumer A to start processing", func() bool { return recA.len() >= 20 })
	startConsumer(t, e.consumerConfig(t), slow(recB))

	union := func() int {
		seen := make(map[string]struct{})
		for _, r := range []*recorder{recA, recB} {
			for _, v := range r.values() {
				seen[v] = struct{}{}
			}
		}
		return len(seen)
	}
	waitFor(t, "consumer B to get partitions", func() bool { return recB.len() > 0 })
	// 允许重复（再平衡前没来得及提交的会被新持有者再处理一次），不允许丢失。
	waitFor(t, "all messages", func() bool { return union() >= n })
	t.Logf("A handled %d, B handled %d, distinct %d", recA.len(), recB.len(), union())
	// 收回分区时先等在途消息处理完并提交，B 从 A 处理到的位置接着处理，所以这里不应有重复。
	if dup := recA.len() + recB.len() - union(); dup != 0 {
		t.Errorf("%d messages handled twice across rebalance, want 0 (revoke should commit before handing over)", dup)
	}

	// 再发一批，两个消费者都应该分到。
	mustSend(t, p, withTopic(e.topic, values("n", 100))...)
	aBefore, bBefore := recA.len(), recB.len()
	waitFor(t, "second batch", func() bool { return union() >= n+100 })
	if recA.len() == aBefore || recB.len() == bBefore {
		t.Errorf("second batch: A handled %d, B handled %d, want both > 0", recA.len()-aBefore, recB.len()-bBefore)
	}
}

func TestConsumerHandlerPanic(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	mustSend(t, p, withTopic(e.topic, values("m", 2))...)

	var panicked atomic.Bool
	rec := &recorder{}
	startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		if string(m.Value) == "m0" && panicked.CompareAndSwap(false, true) {
			panic("boom")
		}
		rec.add(m)
		return nil
	})
	waitFor(t, "both messages", func() bool { return rec.len() >= 2 })
	if got := rec.values(); !slices.Equal(got, []string{"m0", "m1"}) {
		t.Fatalf("got %v, want [m0 m1]", got)
	}
}

func TestConsumerStopInterruptsRetry(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	mustSend(t, p, withTopic(e.topic, values("m", 1))...)

	var calls atomic.Int32
	cfg := e.consumerConfig(t)
	cfg.RetryInitial = time.Minute
	cfg.RetryMax = time.Minute
	r := startConsumer(t, cfg, func(context.Context, kafkax.Message) error {
		calls.Add(1)
		return errors.New("always fails")
	})
	waitFor(t, "first attempt", func() bool { return calls.Load() >= 1 })

	begin := time.Now()
	if err := r.stop(t, 10*time.Second); err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if d := time.Since(begin); d > 5*time.Second {
		t.Fatalf("Stop took %v while waiting to retry", d)
	}
	if got := committed(t, e.brokers, e.group, e.topic); len(got) != 0 {
		t.Fatalf("committed = %v, want nothing", got)
	}
}

func TestNewConsumerRequiresDLQTopic(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	cfg := e.consumerConfig(t)
	cfg.DLQSuffix = ".missing-dlq"
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	if _, err := kafkax.NewConsumer(ctx, cfg, func(context.Context, kafkax.Message) error { return nil }); err == nil {
		t.Fatal("NewConsumer without dlq topic: want error")
	}
}

func TestConsumerStopWaitsForInFlight(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	mustSend(t, p, withTopic(e.topic, values("m", 2))...)

	started := make(chan struct{})
	var finished atomic.Bool
	r := startConsumer(t, e.consumerConfig(t), func(_ context.Context, m kafkax.Message) error {
		if string(m.Value) == "m0" {
			close(started)
			time.Sleep(300 * time.Millisecond)
			finished.Store(true)
		}
		return nil
	})
	select {
	case <-started:
	case <-time.After(waitTimeout):
		t.Fatal("handler never started")
	}
	if err := r.stop(t, 10*time.Second); err != nil {
		t.Fatalf("Start returned %v, want nil", err)
	}
	if !finished.Load() {
		t.Fatal("Stop returned before the in-flight handler finished")
	}
	// m0 处理完后停止：至少提交到 1；m1 可能在停止前也处理完了。
	if got := committed(t, e.brokers, e.group, e.topic)[0]; got < 1 {
		t.Fatalf("committed offset = %d, want >= 1", got)
	}
}
