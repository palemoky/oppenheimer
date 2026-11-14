package capture

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// CheckpointStore 读写位点 topic。topic 是压缩保留、只有一个分区，key 是流水线名，值是 cdc.v1.Checkpoint；
// 值为空的消息是墓碑，表示这条流水线没有位点（要重做全量）。
type CheckpointStore struct {
	kafka  kafkax.Config
	topic  string
	writer *kgo.Client
}

// NewCheckpointStore 创建位点存储。writer 用来写位点，必须是 acks=all 的生产者；调用方负责关闭它。
// 读位点时另建一个只读的客户端，用完即关。
func NewCheckpointStore(kafka kafkax.Config, topic string, writer *kgo.Client) *CheckpointStore {
	return &CheckpointStore{kafka: kafka, topic: topic, writer: writer}
}

// Load 把位点 topic 从头读到当前的高水位，返回 name 的最新一条位点。没有记录或最新一条是墓碑时返回 nil。
func (s *CheckpointStore) Load(ctx context.Context, name string) (*cdcv1.Checkpoint, error) {
	ends, err := kadm.NewClient(s.writer).ListEndOffsets(ctx, s.topic)
	if err != nil {
		return nil, fmt.Errorf("list end offset of %s: %w", s.topic, err)
	}
	end, ok := ends.Lookup(s.topic, 0)
	if !ok {
		return nil, fmt.Errorf("checkpoint topic %s has no partition 0", s.topic)
	}
	if end.Err != nil {
		return nil, fmt.Errorf("end offset of %s: %w", s.topic, end.Err)
	}
	if end.Offset <= 0 {
		return nil, nil
	}

	reader, err := kafkax.NewClient(ctx, s.kafka, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		s.topic: {0: kgo.NewOffset().AtStart()},
	}))
	if err != nil {
		return nil, fmt.Errorf("connect checkpoint reader: %w", err)
	}
	defer reader.Close()

	var latest *kgo.Record
	last := int64(-1)
	for last < end.Offset-1 {
		fetches := reader.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("read checkpoint topic %s: %w", s.topic, err)
		}
		var ferr error
		fetches.EachError(func(_ string, _ int32, err error) { ferr = errors.Join(ferr, err) })
		if ferr != nil {
			return nil, fmt.Errorf("read checkpoint topic %s: %w", s.topic, ferr)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			last = max(last, r.Offset)
			if r.Offset < end.Offset && string(r.Key) == name {
				latest = r
			}
		})
	}
	if latest == nil || len(latest.Value) == 0 {
		return nil, nil
	}
	cp := &cdcv1.Checkpoint{}
	if err := proto.Unmarshal(latest.Value, cp); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint of %s: %w", name, err)
	}
	return cp, nil
}

// Save 写入 name 的位点并等待确认。
func (s *CheckpointStore) Save(ctx context.Context, name string, cp *cdcv1.Checkpoint) error {
	cp.UpdateMicros = time.Now().UnixMicro()
	value, err := proto.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	return s.write(ctx, name, value)
}

// Reset 给 name 写一条墓碑。之后这条流水线再启动时会重做全量。
func (s *CheckpointStore) Reset(ctx context.Context, name string) error {
	return s.write(ctx, name, nil)
}

func (s *CheckpointStore) write(ctx context.Context, name string, value []byte) error {
	rec := &kgo.Record{Topic: s.topic, Key: []byte(name), Value: value}
	if err := s.writer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("write checkpoint of %s: %w", name, err)
	}
	return nil
}
