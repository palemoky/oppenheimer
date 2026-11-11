// Package kafkax 封装 Kafka 客户端：建连接、建 topic、生产者和消费者。
//
// 底层用 franz-go（见 ADR-0007）。本包的目标是把“至少一次投递”所需的几件事固定下来，
// 让各服务不必各写一遍：同步发送并确认、处理成功后才提交位点、失败重试与死信。
package kafkax

import (
	"context"
	"errors"
	"fmt"

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
	return nil
}
