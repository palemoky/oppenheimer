package kafkax_test

import (
	"context"
	"crypto/rand"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/testkafka"
)

// waitTimeout 是单个等待的上限，保证出问题时测试失败而不是挂住。
const waitTimeout = 30 * time.Second

// env 是一个测试的 Kafka 环境：独立的 topic（带死信 topic）和消费者组名。
type env struct {
	brokers []string
	topic   string
	dlq     string
	group   string
}

func newEnv(t *testing.T, partitions int32) env {
	t.Helper()
	brokers := testkafka.New(t)
	topic := testkafka.Topic(t, brokers, partitions)
	testkafka.TopicNamed(t, brokers, topic+".dlq", 1)
	return env{
		brokers: brokers,
		topic:   topic,
		dlq:     topic + ".dlq",
		group:   "group-" + strings.ToLower(rand.Text()),
	}
}

func (e env) consumerConfig(t *testing.T) kafkax.ConsumerConfig {
	return kafkax.ConsumerConfig{
		Brokers:      e.brokers,
		ClientID:     "kafkax-test",
		Group:        e.group,
		Topics:       []string{e.topic},
		Logger:       testLogger(t),
		RetryInitial: 10 * time.Millisecond,
		RetryMax:     50 * time.Millisecond,
	}
}

func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func newProducer(t *testing.T, brokers []string) *kafkax.Producer {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	p, err := kafkax.NewProducer(ctx, kafkax.Config{Brokers: brokers, ClientID: "kafkax-test"})
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func mustSend(t *testing.T, p *kafkax.Producer, recs ...kafkax.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	if _, err := p.Send(ctx, recs...); err != nil {
		t.Fatalf("send: %v", err)
	}
}

// sendToPartitions 把 values[i] 发到分区 i，用于需要精确控制分区的测试。
func sendToPartitions(t *testing.T, brokers []string, topic string, values ...string) {
	t.Helper()
	sendToPartitionsWithHeaders(t, brokers, topic, values, nil)
}

// sendToPartitionsWithHeaders 同 sendToPartitions，headers 非空时为第 i 条记录附加 headers(i)。
func sendToPartitionsWithHeaders(t *testing.T, brokers []string, topic string, values []string, headers func(i int) map[string]string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	cl, err := kafkax.NewClient(ctx, kafkax.Config{Brokers: brokers}, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()
	recs := make([]*kgo.Record, len(values))
	for i, v := range values {
		recs[i] = &kgo.Record{Topic: topic, Partition: int32(i), Value: []byte(v)}
		if headers != nil {
			for k, hv := range headers(i) {
				recs[i].Headers = append(recs[i].Headers, kgo.RecordHeader{Key: k, Value: []byte(hv)})
			}
		}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// readAll 直接（不经过消费者组）从头读 topic，直到读到 n 条。
func readAll(t *testing.T, brokers []string, topic string, n int) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	cl, err := kafkax.NewClient(ctx, kafkax.Config{Brokers: brokers}, kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()
	var out []*kgo.Record
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %s: got %d of %d records: %v", topic, len(out), n, ctx.Err())
		}
		fs.EachError(func(topic string, p int32, err error) { t.Logf("fetch %s/%d: %v", topic, p, err) })
		out = append(out, fs.Records()...)
	}
	return out
}

// committed 返回消费者组在 topic 各分区上已提交的位点（没有提交的分区不出现）。
func committed(t *testing.T, brokers []string, group, topic string) map[int32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	cl, err := kafkax.NewClient(ctx, kafkax.Config{Brokers: brokers})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets: %v", err)
	}
	out := make(map[int32]int64)
	for _, ps := range resp {
		for p, o := range ps {
			if o.Topic == topic && o.Err == nil && o.At >= 0 {
				out[p] = o.At
			}
		}
	}
	return out
}

// runner 在后台运行一个消费者。
type runner struct {
	c      *kafkax.Consumer
	cancel context.CancelFunc
	result chan error
	once   sync.Once
	err    error
}

// start 在后台运行 c，测试结束时停止并断言 Start 返回 nil。
func start(t *testing.T, c *kafkax.Consumer) *runner {
	t.Helper()
	// Start 的 ctx 不能用 t.Context()：它在 Cleanup 之前就取消了，那就变成了强制停止。
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{c: c, cancel: cancel, result: make(chan error, 1)}
	go func() { r.result <- c.Start(ctx) }()
	t.Cleanup(func() {
		if err := r.stop(t, 15*time.Second); err != nil {
			t.Errorf("consumer stop: %v", err)
		}
	})
	return r
}

func startConsumer(t *testing.T, cfg kafkax.ConsumerConfig, h kafkax.Handler) *runner {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	c, err := kafkax.NewConsumer(ctx, cfg, h)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	return start(t, c)
}

// stop 调用 Stop 并等 Start 返回，返回 Start 的错误。可以重复调用。
// Stop 自身的错误（例如期限太短）只记日志，由调用方通过 Start 的返回值和后续行为判断。
func (r *runner) stop(t *testing.T, timeout time.Duration) error {
	r.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := r.c.Stop(ctx); err != nil {
			t.Logf("Stop returned: %v", err)
		}
		select {
		case r.err = <-r.result:
		case <-time.After(waitTimeout):
			r.cancel()
			t.Errorf("Start did not return %v after Stop", waitTimeout)
			r.err = <-r.result
		}
		r.cancel()
	})
	return r.err
}

// waitFor 轮询 cond 直到为真，超时则终止测试。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// recorder 记录 handler 收到的消息。
type recorder struct {
	mu   sync.Mutex
	msgs []kafkax.Message
}

func (r *recorder) add(m kafkax.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
}

func (r *recorder) all() []kafkax.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]kafkax.Message(nil), r.msgs...)
}

func (r *recorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func (r *recorder) values() []string {
	var out []string
	for _, m := range r.all() {
		out = append(out, string(m.Value))
	}
	return out
}

// distinct 返回收到过的不同 value 的个数。
func (r *recorder) distinct() int {
	seen := make(map[string]struct{})
	for _, m := range r.all() {
		seen[string(m.Value)] = struct{}{}
	}
	return len(seen)
}

// fakeClock 是可以手动拨动的时钟。
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeWaiter
}

type fakeWaiter struct {
	at time.Time
	ch chan time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Now()} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- f.now
		return ch
	}
	f.waiters = append(f.waiters, fakeWaiter{at: f.now.Add(d), ch: ch})
	return ch
}

// Advance 拨快时钟并唤醒已到期的等待者。
func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	keep := f.waiters[:0]
	for _, w := range f.waiters {
		if w.at.After(f.now) {
			keep = append(keep, w)
			continue
		}
		w.ch <- f.now
	}
	f.waiters = keep
}

// pending 返回正在等待的等待者个数。
func (f *fakeClock) pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}
