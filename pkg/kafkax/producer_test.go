package kafkax_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/palemoky/Oppenheimer/pkg/kafkax"
	"github.com/palemoky/Oppenheimer/pkg/testkafka"
)

func TestProducerSend(t *testing.T) {
	t.Parallel()
	brokers := testkafka.New(t)
	topic := testkafka.Topic(t, brokers, 2)
	p := newProducer(t, brokers)

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	errs, err := p.Send(ctx,
		kafkax.Record{Topic: topic, Key: []byte("k1"), Value: []byte("v1"), Headers: map[string]string{"h": "1"}},
		kafkax.Record{Topic: topic, Key: []byte("k2"), Value: []byte("v2")},
	)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(errs) != 2 || errs[0] != nil || errs[1] != nil {
		t.Fatalf("errs = %v, want two nils", errs)
	}

	got := make(map[string]string)
	for _, r := range readAll(t, brokers, topic, 2) {
		got[string(r.Key)] = string(r.Value)
		if string(r.Key) == "k1" && (len(r.Headers) != 1 || r.Headers[0].Key != "h" || string(r.Headers[0].Value) != "1") {
			t.Errorf("k1 headers = %v", r.Headers)
		}
	}
	if got["k1"] != "v1" || got["k2"] != "v2" {
		t.Errorf("read back %v", got)
	}
}

func TestProducerSameKeySamePartitionInOrder(t *testing.T) {
	t.Parallel()
	brokers := testkafka.New(t)
	topic := testkafka.Topic(t, brokers, 4)
	p := newProducer(t, brokers)

	const perKey = 50
	keys := []string{"order-1", "order-2", "order-3"}
	var recs []kafkax.Record
	for i := range perKey {
		for _, k := range keys {
			recs = append(recs, kafkax.Record{Topic: topic, Key: []byte(k), Value: []byte(strconv.Itoa(i))})
		}
	}
	mustSend(t, p, recs...)

	partition := make(map[string]int32)
	lastOffset := make(map[string]int64)
	next := make(map[string]int)
	for _, r := range readAll(t, brokers, topic, len(recs)) {
		k := string(r.Key)
		if p, ok := partition[k]; ok && p != r.Partition {
			t.Fatalf("key %s in partitions %d and %d", k, p, r.Partition)
		}
		partition[k] = r.Partition
		if last, ok := lastOffset[k]; ok && r.Offset <= last {
			t.Fatalf("key %s: offset %d after %d", k, r.Offset, last)
		}
		lastOffset[k] = r.Offset
		// readAll 按分区内位点顺序返回同一分区的记录。
		if want := strconv.Itoa(next[k]); string(r.Value) != want {
			t.Fatalf("key %s: got value %s, want %s", k, r.Value, want)
		}
		next[k]++
	}
	for _, k := range keys {
		if next[k] != perKey {
			t.Errorf("key %s: got %d records, want %d", k, next[k], perKey)
		}
	}
}

func TestProducerUnknownTopic(t *testing.T) {
	t.Parallel()
	brokers := testkafka.New(t)
	topic := testkafka.Topic(t, brokers, 1)
	p := newProducer(t, brokers)

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	missing := topic + "-missing"
	errs, err := p.Send(ctx,
		kafkax.Record{Topic: topic, Value: []byte("a")},
		kafkax.Record{Topic: missing, Value: []byte("b")},
		kafkax.Record{Topic: topic, Value: []byte("c")},
	)
	if err == nil {
		t.Fatal("send to missing topic: want error")
	}
	if len(errs) != 3 {
		t.Fatalf("len(errs) = %d, want 3", len(errs))
	}
	if errs[0] != nil || errs[2] != nil {
		t.Errorf("records to existing topic failed: %v, %v", errs[0], errs[2])
	}
	if errs[1] == nil {
		t.Error("record to missing topic: want error")
	}
	t.Logf("missing topic error: %v", errs[1])
	readAll(t, brokers, topic, 2)
}
