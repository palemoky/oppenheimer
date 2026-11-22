package kafkax_test

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// 本文件测 kafkax.consumer.oldest_pending_age。各用例的 topic 和消费组是独有的，用这两个标签找自己的数据点。

const oldestMetric = "kafkax.consumer.oldest_pending_age"

var errStuck = errors.New("stuck")

// oldestAge 返回 e 的消费者在 e.topic 上的 oldest_pending_age，found 表示有这个数据点。
func oldestAge(t *testing.T, e env) (v int64, found bool) {
	t.Helper()
	m, ok := findMetric(collect(t), oldestMetric)
	if !ok {
		return 0, false
	}
	g, ok := m.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("metric %s is %T, want Gauge[int64]", oldestMetric, m.Data)
	}
	want := attribute.NewSet(consumerAttrs(e)...)
	for _, dp := range g.DataPoints {
		if dp.Attributes.Equals(&want) {
			return dp.Value, true
		}
	}
	return 0, false
}

// waitAge 等到 oldest_pending_age 满足 cond，返回那时的值。
func waitAge(t *testing.T, e env, what string, cond func(int64) bool) int64 {
	t.Helper()
	var last int64
	waitFor(t, what, func() bool {
		v, ok := oldestAge(t, e)
		last = v
		return ok && cond(v)
	})
	return last
}

// timedRecord 是一条带指定时间戳（CreateTime）的消息。
type timedRecord struct {
	key, value string
	ts         time.Time
	headers    map[string]string
}

// produceAt 按顺序发到 topic 的 0 号分区，时间戳由调用方指定。
func produceAt(t *testing.T, brokers []string, topic string, trs ...timedRecord) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	cl, err := kafkax.NewClient(ctx, kafkax.Config{Brokers: brokers}, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()
	recs := make([]*kgo.Record, len(trs))
	for i, tr := range trs {
		recs[i] = &kgo.Record{Topic: topic, Partition: 0, Key: []byte(tr.key), Value: []byte(tr.value), Timestamp: tr.ts}
		for k, v := range tr.headers {
			recs[i].Headers = append(recs[i].Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func TestOldestPendingAgeIsZeroWithoutMessages(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 2)
	startConsumer(t, e.consumerConfig(t), func(context.Context, kafkax.Message) error { return nil })
	if v := waitAge(t, e, "gauge registered", func(int64) bool { return true }); v != 0 {
		t.Fatalf("oldest pending age = %d without messages, want 0", v)
	}
}

// 一条消息一直失败（原地重试、不进死信）：这个值随时间增长；它成功之后回到 0。
func TestOldestPendingAgeGrowsWhileAMessageIsStuck(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	produceAt(t, e.brokers, e.topic, timedRecord{key: "k", value: "v", ts: time.Now().Add(-100 * time.Second)})

	var succeed atomic.Bool
	cfg := e.consumerConfig(t)
	cfg.MaxAttempts = math.MaxInt // 和各服务的主要消费者一样，普通错误永远不进死信
	startConsumer(t, cfg, func(context.Context, kafkax.Message) error {
		if succeed.Load() {
			return nil
		}
		return errStuck
	})

	first := waitAge(t, e, "age of the stuck message", func(v int64) bool { return v >= 100 })
	if first >= 130 {
		t.Fatalf("oldest pending age = %d, want about 100", first)
	}
	waitAge(t, e, "age to grow", func(v int64) bool { return v > first })

	succeed.Store(true)
	waitAge(t, e, "age back to 0 after success", func(v int64) bool { return v == 0 })
}

// 并发消费：一个 key 被挡住，别的 key 照常做完。报的是挡住位点的那条（最小的未完成位点）的年龄。
func TestOldestPendingAgeWithLanesReportsTheBlockingKey(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	now := time.Now()
	// 挡住的那条在中间：它前面的 a 做完之后，tracker 的第一条就是它。
	produceAt(t, e.brokers, e.topic,
		timedRecord{key: "a", value: "a", ts: now.Add(-10 * time.Second)},
		timedRecord{key: "stuck", value: "stuck", ts: now.Add(-200 * time.Second)},
		timedRecord{key: "b", value: "b", ts: now.Add(-10 * time.Second)},
		timedRecord{key: "c", value: "c", ts: now.Add(-10 * time.Second)},
	)

	var release atomic.Bool
	rec := &recorder{}
	cfg := e.consumerConfig(t)
	cfg.Concurrency = 4
	cfg.MaxAttempts = math.MaxInt
	startConsumer(t, cfg, func(_ context.Context, m kafkax.Message) error {
		if string(m.Key) == "stuck" && !release.Load() {
			return errStuck
		}
		rec.add(m)
		return nil
	})

	// 只等排在它前面的 a：lane 的哈希种子每个进程随机（见 laneOf），b、c 可能和 stuck 落在同一条 lane 上、
	// 排在它后面出不来。a 的位点在前，无论分到哪条 lane 都会先做完，之后 tracker 的第一条就是 stuck。
	waitFor(t, "the key ahead of the blocking one done", func() bool { return rec.len() >= 1 })
	if v := waitAge(t, e, "age of the blocking key", func(v int64) bool { return v >= 200 }); v >= 230 {
		t.Fatalf("oldest pending age = %d, want about 200", v)
	}

	release.Store(true)
	waitFor(t, "all four done after release", func() bool { return rec.len() == 4 })
	waitAge(t, e, "age back to 0 after release", func(v int64) bool { return v == 0 })
}

// 延迟消费者：没到期的不算待处理；到期之后从到期时刻算起。
func TestOldestPendingAgeIgnoresDelayMessagesNotYetDue(t *testing.T) {
	t.Parallel()
	e := newEnv(t, 1)
	p := newProducer(t, e.brokers)
	clock := newFakeClock()
	mustSend(t, p, kafkax.NewDelayRecord(e.topic, []byte("o1"), []byte("v"), clock.Now().Add(15*time.Minute)))

	entered := make(chan struct{})
	release := make(chan struct{})
	startDelayConsumer(t, e.consumerConfig(t), clock, func(ctx context.Context, _ kafkax.Message) error {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	waitFor(t, "consumer waiting on due time", func() bool { return clock.pending() == 1 })
	// 等到期的那段时间里多读几次，都应当是 0。
	for range 3 {
		if v := waitAge(t, e, "gauge while waiting", func(int64) bool { return true }); v != 0 {
			t.Fatalf("oldest pending age = %d while the message is not due, want 0", v)
		}
		time.Sleep(50 * time.Millisecond)
	}

	clock.Advance(15*time.Minute + 30*time.Second)
	select {
	case <-entered:
	case <-time.After(waitTimeout):
		t.Fatal("handler not called after due")
	}
	// 时钟是假的，不再走：到期 30 秒后，正好 30。
	if v, _ := oldestAge(t, e); v != 30 {
		t.Fatalf("oldest pending age = %d after due, want 30", v)
	}

	close(release)
	waitAge(t, e, "age back to 0 after the handler returns", func(v int64) bool { return v == 0 })
}
