//go:build faults

package indexer

import (
	"sync"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/fault"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

var compactBatch = fault.Switch("f08")

// unmarshalSpan 是一个 goroutine 反序列化的消息条数。不超过它的批直接逐条做，不值得起 goroutine。
const unmarshalSpan = 32

// unmarshalChanges 反序列化一批消息，返回要交给 Apply 的变更；有消息反序列化不了时返回按顺序第一条的错误
// （kafkax.Permanent）。
//
// 积压或者从头重建时一批有几百条，proto.Unmarshal（每条两个 map、十几个 Value）是这一批里最重的 CPU，
// 压在分区的那一个 worker 上串行做。这里按块分给几个 goroutine。
//
// 顺带在这里按 key 收拢：cdc.order.orders 以订单号为 key、按 key 压缩保留，同一个分区里同一个 key
// 后到的那条就是这个订单最新的映像，前面的到了 Apply 也会在批内合并时被丢掉。所以每个 key 只占一个位置，
// 按到达的顺序写进去，后面的覆盖前面的；交给 Apply 的就是这一批里每个订单的一条，按第一次出现的顺序排。
// 没有 key 的消息各占一个位置，不收拢。
func unmarshalChanges(msgs []kafkax.Message) ([]*cdcv1.RowChange, error) {
	if !compactBatch.On() || len(msgs) <= unmarshalSpan {
		return unmarshalInOrder(msgs)
	}
	// at[i] 是第 i 条消息在 latest 里的位置。
	at := make([]int, len(msgs))
	first := make(map[string]int, len(msgs))
	n := 0
	for i, msg := range msgs {
		if len(msg.Key) == 0 {
			at[i], n = n, n+1
			continue
		}
		j, ok := first[string(msg.Key)]
		if !ok {
			j, n = n, n+1
			first[string(msg.Key)] = j
		}
		at[i] = j
	}
	latest := make([]*cdcv1.RowChange, n)
	errs := make([]error, len(msgs))
	var wg sync.WaitGroup
	for start := 0; start < len(msgs); start += unmarshalSpan {
		end := min(start+unmarshalSpan, len(msgs))
		wg.Go(func() {
			for i := start; i < end; i++ {
				ch, err := unmarshalOne(msgs[i])
				if err != nil {
					errs[i] = err
					continue
				}
				latest[at[i]] = ch
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return latest, nil
}
