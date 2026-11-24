//go:build !faults

package indexer

import cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"

// decodeChanges 解码一批变更，返回和 changes 一一对应的结果；有消息出错时返回按顺序第一条的错误。
func decodeChanges(changes []*cdcv1.RowChange) ([]decodedChange, error) {
	return decodeInOrder(changes)
}
