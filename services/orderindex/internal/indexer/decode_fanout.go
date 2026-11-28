//go:build faults

package indexer

import (
	"math"
	"sync"
	"time"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/fault"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

var fanout = fault.Switch("f07")

// decodeSpan 是一个 goroutine 解码的消息条数。
//
// 积压或者从头重建时一批有几百条，逐列取值、解析时间都压在分区的那一个 worker 上串行做，写库之前 CPU 先排一轮队。
// 这里把一批按块分给几个 goroutine 解码，结果按下标放回，顺序和逐条解码时一样；只有一条的批就是一个块。
const decodeSpan = 64

// decodeChanges 解码一批变更，返回和 changes 一一对应的结果；有消息出错时返回按顺序第一条的错误。
func decodeChanges(changes []*cdcv1.RowChange) ([]decodedChange, error) {
	if !fanout.On() {
		return decodeInOrder(changes)
	}
	out := make([]decodedChange, len(changes))
	errs := make([]error, len(changes))
	var wg sync.WaitGroup
	for start := 0; start < len(changes); start += decodeSpan {
		part := changes[start:min(start+decodeSpan, len(changes))]
		wg.Go(func() {
			for i, ch := range part {
				row, kind, archived, err := decodeChangeWith(ch, rowDirect)
				out[start+i], errs[start+i] = decodedChange{row: row, kind: kind, archived: archived}, err
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// rowDirect 是 decodeRow 的快速路径，约定和它相同。
//
// orders 的行形状是固定的（BIGINT、CHAR、DATETIME），这里按类型直接取值，省掉 decoder 每列的类型分支和错误记录。
// 形状和预期不符（商家是 NULL、类型不对、缺列、取值越界）时交给 decodeRow，由它给出准确的结果或 ErrSchema。
func rowDirect(img map[string]*cdcv1.Value) (store.Row, bool, error) {
	merchant, ok1 := intOf(img, colMerchantID)
	order, ok2 := intOf(img, colOrderID)
	user, ok3 := intOf(img, colUserID)
	amount, ok4 := intOf(img, colTotalAmount)
	version, ok5 := intOf(img, colVersion)
	status, ok6 := intOf(img, colStatus)
	currency, ok7 := img[colCurrency].GetKind().(*cdcv1.Value_Str)
	created, ok8 := datetimeOf(img, colCreatedAt)
	paid, ok9 := img[colPaidAt]
	wellFormed := ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7 && ok8 && ok9
	if !wellFormed || merchant <= 0 || order <= 0 || status < math.MinInt8 || status > math.MaxInt8 {
		return decodeRow(img)
	}
	r := store.Row{
		MerchantID:  merchant,
		OrderID:     order,
		UserID:      user,
		Status:      int32(status),
		TotalAmount: amount,
		Currency:    currency.Str,
		CreatedAt:   created,
		Version:     version,
	}
	// 状态按订单的进度排：PAID 及以后的都付过款，带着 paid_at；之前的还没有。
	if status >= int64(orderv1.OrderStatus_ORDER_STATUS_PAID) {
		t, err := time.ParseInLocation(datetimeLayout, paid.GetKind().(*cdcv1.Value_Str).Str, time.UTC)
		if err != nil {
			return decodeRow(img)
		}
		r.PaidAt = t
	} else if paid.GetKind() != nil {
		return decodeRow(img)
	}
	return r, true, nil
}

// intOf 取一个有符号整数列；列缺失、是 NULL 或者不是有符号整数时 ok 为 false。
func intOf(img map[string]*cdcv1.Value, col string) (int64, bool) {
	k, ok := img[col].GetKind().(*cdcv1.Value_Int)
	if !ok {
		return 0, false
	}
	return k.Int, true
}

// datetimeOf 取一个 DATETIME 列，按 UTC 解释；列缺失、是 NULL 或者解析不了时 ok 为 false。
func datetimeOf(img map[string]*cdcv1.Value, col string) (time.Time, bool) {
	k, ok := img[col].GetKind().(*cdcv1.Value_Str)
	if !ok {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation(datetimeLayout, k.Str, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
