package kafkax_test

import (
	"crypto/rand"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/testkafka"
)

// 防止：EnsureTopics 返回后 topic 还查不到。创建是异步生效的，服务启动时紧接着创建消费者，
// 消费者会检查死信 topic 是否存在；在慢一点的机器上（CI）曾因此启动失败。
// 这里用另一个全新的连接立即查询，并且重复多次，让这个时序问题有机会暴露。
func TestEnsureTopicsVisibleImmediately(t *testing.T) {
	brokers := testkafka.New(t)
	cfg := kafkax.Config{Brokers: brokers, ClientID: "ensure-test"}
	ctx := t.Context()

	creator, err := kafkax.NewClient(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(creator.Close)

	for range 10 {
		name := "test-ensure-" + strings.ToLower(rand.Text())
		specs := []kafkax.TopicSpec{{Name: name, Partitions: 4}, {Name: name + ".dlq", Partitions: 4}}
		if err := kafkax.EnsureTopics(ctx, creator, specs...); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = kadm.NewClient(creator).DeleteTopics(t.Context(), name, name+".dlq")
		})

		other, err := kafkax.NewClient(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		details, err := kadm.NewClient(other).ListTopics(ctx, name, name+".dlq")
		other.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range specs {
			d, ok := details[spec.Name]
			if !ok || d.Err != nil || len(d.Partitions) != 4 {
				t.Fatalf("topic %s not visible right after EnsureTopics: %+v", spec.Name, d)
			}
		}
		// 已存在时再调用一次：不报错、不改动。
		if err := kafkax.EnsureTopics(ctx, creator, specs...); err != nil {
			t.Fatalf("EnsureTopics on existing topics: %v", err)
		}
	}
}
