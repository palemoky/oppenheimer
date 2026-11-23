package kafkax

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PartitionOf 返回 key 非空的消息在有 partitions 个分区的 topic 上会被 Producer 发到哪个分区。
//
// 用途：发消息之前就要知道它会排进哪个分区的队列（order 受理时按落库的分区分别计数，见 stockgate.HoldRequest.Queue）。
// Producer 用的是 franz-go 默认的分区器（kgo.StickyKeyPartitioner），对有 key 的消息和 Kafka 的 Java 客户端一样：
// murmur2 哈希，去掉符号位，对分区数取模。这里是同一个算法的另一份实现（franz-go 没有导出它的 murmur2），
// 两边的一致由测试钉住：TestPartitionOfMatchesProducer 把消息真的发出去，核对落到的分区。
// 以后给 Producer 换分区器，那个测试会失败，这个函数要跟着改。
//
// 算出来的是“按这个分区数会去哪”。topic 的分区数变了而调用方手里的还是旧数，结果就和实际不一致；
// 调用方只应把它用在“估计”上（计数、限流），不能用在正确性依赖它的地方。partitions <= 0 时返回 0。
func PartitionOf(key []byte, partitions int32) int32 {
	if partitions <= 0 {
		return 0
	}
	// 去掉符号位之后小于 2^31，取模的结果小于 partitions，两次转换都不会溢出。
	return int32((murmur2(key) & 0x7fffffff) % uint32(partitions)) //nolint:gosec // 见上一行
}

// murmur2 是 Kafka 用的 32 位 MurmurHash2（org.apache.kafka.common.utils.Utils.murmur2），种子固定。
func murmur2(data []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	h := seed ^ uint32(len(data)) //nolint:gosec // 和 Java 的实现一样按 32 位截断
	n := len(data) / 4
	for i := range n {
		k := uint32(data[i*4]) | uint32(data[i*4+1])<<8 | uint32(data[i*4+2])<<16 | uint32(data[i*4+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	tail := data[n*4:]
	switch len(tail) {
	case 3:
		h ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(tail[0])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// TopicPartitions 返回 topic 现有的分区数。topic 不存在时返回错误。
func TopicPartitions(ctx context.Context, client *kgo.Client, topic string) (int32, error) {
	details, err := kadm.NewClient(client).ListTopics(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("kafkax: list topic %s: %w", topic, err)
	}
	d, ok := details[topic]
	if !ok {
		return 0, fmt.Errorf("kafkax: topic %s not found", topic)
	}
	if d.Err != nil {
		return 0, fmt.Errorf("kafkax: topic %s: %w", topic, d.Err)
	}
	return int32(len(d.Partitions)), nil //nolint:gosec // 分区数远小于 2^31
}
