package indexer

import (
	"context"
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/palemoky/oppenheimer/api/eventx"
	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/testkafka"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

// startConsumer 按 main 里的配置（MaxAttempts 取最大值）启动消费者，测试结束时停止。
func startConsumer(t *testing.T, brokers []string, topic string, ix *Indexer) {
	t.Helper()
	c, err := kafkax.NewConsumer(t.Context(), kafkax.ConsumerConfig{
		Brokers:      brokers,
		ClientID:     "orderindex-test",
		Group:        topic + ".group",
		Topics:       []string{topic},
		MaxAttempts:  math.MaxInt,
		RetryInitial: 10 * time.Millisecond,
		RetryMax:     100 * time.Millisecond,
	}, ix.Handler(DefaultWriteTimeout))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Start(context.Background()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Stop(ctx); err != nil {
			t.Errorf("stop consumer: %v", err)
		}
		if err := <-done; err != nil {
			t.Errorf("consumer: %v", err)
		}
	})
}

func send(t *testing.T, p *kafkax.Producer, topic string, msgs ...*cdcv1.RowChange) {
	t.Helper()
	recs := make([]kafkax.Record, len(msgs))
	for i, m := range msgs {
		v, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		recs[i] = kafkax.Record{Topic: topic, Key: eventx.OrderKey(m.GetAfter()["order_id"].GetInt()), Value: v}
	}
	if _, err := p.Send(t.Context(), recs...); err != nil {
		t.Fatal(err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

func setupKafka(t *testing.T) (brokers []string, topic string, p *kafkax.Producer) {
	t.Helper()
	brokers = testkafka.New(t)
	topic = testkafka.Topic(t, brokers, 2)
	testkafka.TopicNamed(t, brokers, topic+eventx.DLQSuffix, 2)
	p, err := kafkax.NewProducer(t.Context(), kafkax.Config{Brokers: brokers, ClientID: "orderindex-test-producer"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return brokers, topic, p
}

// 经过真的 Kafka：几条 RowChange（含旧版本晚到、重复、无商家）被消费者写进索引。
func TestConsumesFromKafka(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	brokers, topic, p := setupKafka(t)
	startConsumer(t, brokers, topic, ix)

	send(t, p, topic,
		change(cdcv1.Op_OP_INSERT, image(0, 101, 0)), // 还没有商家，跳过
		change(cdcv1.Op_OP_UPDATE, image(11, 101, 2)),
		change(cdcv1.Op_OP_UPDATE, image(11, 101, 1)), // 晚到的旧版本
		change(cdcv1.Op_OP_UPDATE, image(11, 101, 2)), // 重复
		change(cdcv1.Op_OP_READ, image(12, 102, 5)),
		change(cdcv1.Op_OP_UPDATE, image(13, 103, 1)),
		change(cdcv1.Op_OP_UPDATE, image(13, 103, 3)),
	)
	want := map[[2]int64]int64{{11, 101}: 2, {12, 102}: 5, {13, 103}: 3}
	ok := waitFor(t, 30*time.Second, func() bool {
		for k, v := range want {
			r, err := st.Get(t.Context(), k[0], k[1])
			if err != nil || r.Version != v {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatal("index did not converge from kafka")
	}
	for k, v := range want {
		r, _ := st.Get(t.Context(), k[0], k[1])
		if w := wantRow(t, k[0], k[1], v); !sameImage(r, w) {
			t.Errorf("order %d: row = %+v, want %+v", k[1], r, w)
		}
	}
}

// 缺列的消息不被跳过：分区停在它上面持续重试，排在它后面的同分区消息不被应用，也不进死信。
// 修不好就一直停着；这里用“同一个 key 的下一条消息迟迟不出现”来观察停住，用另一个分区的消息证明消费者还活着。
func TestSchemaErrorStallsPartition(t *testing.T) {
	t.Parallel()
	ix, st := newIndexer(t)
	brokers, topic, p := setupKafka(t)
	startConsumer(t, brokers, topic, ix)

	bad := image(21, 201, 1)
	delete(bad, "currency")
	send(t, p, topic, change(cdcv1.Op_OP_UPDATE, bad), change(cdcv1.Op_OP_UPDATE, image(21, 201, 2)))

	// 找一个落在另一个分区的订单：key 的哈希决定分区，换几个订单号总能碰到。
	var probe int64
	for id := int64(300); id < 400; id++ {
		send(t, p, topic, change(cdcv1.Op_OP_UPDATE, image(22, id, 1)))
		if waitFor(t, 5*time.Second, func() bool { _, err := st.Get(t.Context(), 22, id); return err == nil }) {
			probe = id
			break
		}
	}
	if probe == 0 {
		t.Fatal("no message on the other partition was consumed")
	}
	// 再等一会儿，确认坏消息之后的那条始终没被应用。
	if waitFor(t, 2*time.Second, func() bool {
		_, err := st.Get(t.Context(), 21, 201)
		return !errors.Is(err, store.ErrNotFound)
	}) {
		t.Fatal("message after the bad one was applied; the bad message was skipped")
	}
	t.Logf("partition stalled on the bad message; probe order %d on the other partition consumed (key %s)",
		probe, strconv.FormatInt(probe, 10))
}
