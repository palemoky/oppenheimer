package kafkax_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// PartitionOf 算出来的分区就是 Producer 真的发到的分区。order 受理时靠它在发消息之前知道订单会排进哪个分区，
// 两边不一致的话按分区计数的队列上限就数错了队。key 用订单号的十进制写法，和 eventx 的 key 一样。
func TestPartitionOfMatchesProducer(t *testing.T) {
	t.Parallel()
	const (
		partitions = 8
		total      = 200
	)
	e := newEnv(t, partitions)
	recs := make([]kafkax.Record, total)
	for i := range total {
		// 订单号是很大的整数，相邻的只差在低位上。
		recs[i] = kafkax.Record{Topic: e.topic, Key: []byte(strconv.FormatInt(912486003548160000+int64(i)*7, 10)), Value: []byte("v")}
	}
	mustSend(t, newProducer(t, e.brokers), recs...)

	seen := make(map[int32]bool)
	for _, r := range readAll(t, e.brokers, e.topic, total) {
		if want := kafkax.PartitionOf(r.Key, partitions); r.Partition != want {
			t.Fatalf("key %s landed on partition %d, PartitionOf says %d", r.Key, r.Partition, want)
		}
		seen[r.Partition] = true
	}
	if len(seen) < 2 {
		t.Fatalf("all %d keys landed on %d partition(s); the test did not exercise the hash", total, len(seen))
	}

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	cl, err := kafkax.NewClient(ctx, kafkax.Config{Brokers: e.brokers})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()
	if n, err := kafkax.TopicPartitions(ctx, cl, e.topic); err != nil || n != partitions {
		t.Fatalf("TopicPartitions = %d, %v; want %d", n, err, partitions)
	}
	if _, err := kafkax.TopicPartitions(ctx, cl, e.topic+"-missing"); err == nil {
		t.Fatal("TopicPartitions of a missing topic returned no error")
	}
}
