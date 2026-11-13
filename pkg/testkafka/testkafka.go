// Package testkafka 为集成测试提供隔离的 Kafka topic。
//
// 测试连接由环境变量 OPPENHEIMER_TEST_KAFKA_BROKERS 指定的 Kafka（本地用 make up 启动的那个）。
// broker 关闭了自动建 topic，每个测试用 Topic 显式创建名字唯一的 topic，测试结束后删除，
// 因此测试之间互不影响、可以并行。
package testkafka

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// BrokersEnv 是指定测试用 Kafka 的环境变量，逗号分隔。
const BrokersEnv = "OPPENHEIMER_TEST_KAFKA_BROKERS"

// New 返回 broker 地址。
//
// 未设置 BrokersEnv 时跳过测试；但在 CI 里（CI 环境变量非空）视为失败，
// 避免集成测试因为配置遗漏而悄悄不跑。
func New(t testing.TB) (brokers []string) {
	t.Helper()
	v := os.Getenv(BrokersEnv)
	if v == "" {
		if os.Getenv("CI") != "" {
			t.Fatalf("%s is not set in CI", BrokersEnv)
		}
		t.Skipf("%s is not set; run `make up` and `make test`", BrokersEnv)
	}
	for b := range strings.SplitSeq(v, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		t.Fatalf("%s has no broker addresses: %q", BrokersEnv, v)
	}
	return brokers
}

// Topic 创建一个名字唯一的 topic（partitions 个分区），测试结束时删除，返回 topic 名。
// 需要配套的 topic 时（例如死信 topic），用返回的名字调用 TopicNamed。
func Topic(t testing.TB, brokers []string, partitions int32) string {
	t.Helper()
	name := "test-" + strings.ToLower(rand.Text())
	TopicNamed(t, brokers, name, partitions)
	return name
}

// TopicNamed 创建指定名字的 topic（partitions 个分区），测试结束时删除。已存在时视为失败。
func TopicNamed(t testing.TB, brokers []string, name string, partitions int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := kafkax.NewClient(ctx, kafkax.Config{Brokers: brokers, ClientID: "testkafka"})
	if err != nil {
		t.Fatalf("connect to test kafka: %v", err)
	}
	adm := kadm.NewClient(client)

	resp, err := adm.CreateTopic(ctx, partitions, 1, nil, name)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		client.Close()
		t.Fatalf("create topic %s: %v", name, err)
	}
	// 后注册的先执行：删除 topic 发生在被测代码通过 t.Cleanup 关闭消费者之后。
	t.Cleanup(func() {
		// 测试的 ctx 此时已取消，清理用独立的 ctx。
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		defer client.Close()
		if _, err := adm.DeleteTopic(ctx, name); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Logf("delete topic %s: %v", name, err)
		}
	})

	// 等 topic 的元数据可见且每个分区都有 leader，否则紧接着的生产或消费可能先碰到 UNKNOWN_TOPIC。
	for {
		details, err := adm.ListTopics(ctx, name)
		if err == nil {
			if d, ok := details[name]; ok && d.Err == nil && len(d.Partitions) == int(partitions) && leadersReady(d) {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("topic %s not ready: %v", name, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func leadersReady(d kadm.TopicDetail) bool {
	for _, p := range d.Partitions {
		if p.Err != nil || p.Leader < 0 {
			return false
		}
	}
	return true
}
