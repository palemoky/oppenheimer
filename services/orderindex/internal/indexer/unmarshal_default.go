//go:build !faults

package indexer

import (
	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// unmarshalChanges 反序列化一批消息，返回要交给 Apply 的变更；有消息反序列化不了时返回按顺序第一条的错误
// （kafkax.Permanent）。
func unmarshalChanges(msgs []kafkax.Message) ([]*cdcv1.RowChange, error) {
	return unmarshalInOrder(msgs)
}
