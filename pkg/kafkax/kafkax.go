// Package kafkax 封装 Kafka 客户端：建连接、建 topic、生产者和消费者。
//
// 底层用 franz-go（见 ADR-0007）。本包的目标是把“至少一次投递”所需的几件事固定下来，
// 让各服务不必各写一遍：同步发送并确认、处理成功后才提交位点、失败重试与死信。
package kafkax

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config 是连接参数。
type Config struct {
	// Brokers 是引导地址列表，例如 []string{"127.0.0.1:9092"}。
	Brokers []string
	// ClientID 出现在 broker 的日志和指标里，一般用服务名。
	ClientID string
}

// NewClient 创建底层客户端并确认 broker 可达。调用方负责 Close。
// 生产者和消费者的构造函数在此基础上追加各自的选项。
func NewClient(ctx context.Context, cfg Config, opts ...kgo.Opt) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafkax: no brokers")
	}
	base := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}
	if cfg.ClientID != "" {
		base = append(base, kgo.ClientID(cfg.ClientID))
	}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafkax: new client: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafkax: ping %v: %w", cfg.Brokers, err)
	}
	return client, nil
}

// TopicSpec 描述一个要确保存在的 topic。
type TopicSpec struct {
	Name       string
	Partitions int32
	// ReplicationFactor 为 0 时取 1（本机单节点）。集群上由部署清单管理 topic，不走这里。
	ReplicationFactor int16
}

// EnsureTopics 创建不存在的 topic，已存在的不动（也不修改分区数）。
//
// broker 关闭了自动建 topic（拼错名字不应该悄悄生成新 topic），
// 所以服务在启动时显式声明自己用到的 topic。
func EnsureTopics(ctx context.Context, client *kgo.Client, specs ...TopicSpec) error {
	adm := kadm.NewClient(client)
	for _, spec := range specs {
		rf := spec.ReplicationFactor
		if rf == 0 {
			rf = 1
		}
		resp, err := adm.CreateTopic(ctx, spec.Partitions, rf, nil, spec.Name)
		if err == nil {
			err = resp.Err
		}
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafkax: create topic %s: %w", spec.Name, err)
		}
	}
	return waitTopicsVisible(ctx, adm, specs)
}

// topicPollInterval 是等待新建的 topic 出现在元数据里时的轮询间隔。
const topicPollInterval = 50 * time.Millisecond

// waitTopicsVisible 等到所有 topic 都能在元数据里查到，受 ctx 的时限约束。
//
// CreateTopics 返回成功只表示控制器接受了创建，broker 的元数据缓存是异步更新的。
// 紧接着查询或订阅这个 topic，可能得到 UNKNOWN_TOPIC_OR_PARTITION。
// 所以“确保存在”要等到真的查得到、并且每个分区都有 leader 才算完成，否则调用方下一步（例如消费者启动时检查死信 topic）
// 会在慢一点的机器上偶发失败。这个问题在本机不出现，是 CI 上暴露的。
func waitTopicsVisible(ctx context.Context, adm *kadm.Client, specs []TopicSpec) error {
	names := make([]string, len(specs))
	for i, spec := range specs {
		names[i] = spec.Name
	}
	for {
		missing := ""
		details, err := adm.ListTopics(ctx, names...)
		if err != nil {
			return fmt.Errorf("kafkax: list topics: %w", err)
		}
		for _, name := range names {
			d, ok := details[name]
			switch {
			case !ok, errors.Is(d.Err, kerr.UnknownTopicOrPartition):
				missing = name
			case d.Err != nil:
				return fmt.Errorf("kafkax: topic %s: %w", name, d.Err)
			case !leadersReady(d):
				// topic 可见但分区还没选出 leader，这时生产和消费同样会失败。
				missing = name
			}
		}
		if missing == "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafkax: topic %s not ready after creation: %w", missing, ctx.Err())
		case <-time.After(topicPollInterval):
		}
	}
}

func leadersReady(d kadm.TopicDetail) bool {
	for _, p := range d.Partitions {
		if p.Err != nil || p.Leader < 0 {
			return false
		}
	}
	return len(d.Partitions) > 0
}
