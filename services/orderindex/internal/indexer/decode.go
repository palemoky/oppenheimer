package indexer

import (
	"errors"
	"fmt"
	"math"
	"time"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/services/orderindex/internal/store"
)

// ErrSchema 表示消息里缺少索引需要的列，或者列的类型、取值不对。它说明源表的结构被改坏了
// （删列、改名、改类型），按设计文档 1c 4.8 不能跳过：handler 返回它，消费停在这条消息上持续报错，
// 直到源表或本服务修好。
var ErrSchema = errors.New("indexer: row does not match the expected schema")

// 源表 orders_N 的列名（services/order/migrations）。
const (
	colOrderID     = "order_id"
	colUserID      = "user_id"
	colMerchantID  = "merchant_id"
	colStatus      = "status"
	colTotalAmount = "total_amount"
	colCurrency    = "currency"
	colCreatedAt   = "created_at"
	colPaidAt      = "paid_at"
	colVersion     = "version"
)

// datetimeLayout 是 MySQL DATETIME 的文本形式。解析时 Go 允许秒后面带任意位数的小数，也允许没有小数，
// 所以 "2026-06-01 12:00:00"、"...:00.1"、"...:00.123456" 都能用这一个 layout。
const datetimeLayout = "2006-01-02 15:04:05"

// decodeRow 从 after 映像里取出索引需要的列。ok 为 false 表示 merchant_id 是 NULL，这个订单不进索引。
// 需要的列缺失、类型不对或解析不了时返回包着 ErrSchema 的错误。不认识的列忽略。
func decodeRow(after map[string]*cdcv1.Value) (r store.Row, ok bool, err error) {
	d := decoder{after: after}
	merchantID, present := d.nullableInt(colMerchantID)
	if d.err == nil && !present {
		return store.Row{}, false, nil
	}
	r = store.Row{
		MerchantID:  merchantID,
		OrderID:     d.int(colOrderID),
		UserID:      d.int(colUserID),
		TotalAmount: d.int(colTotalAmount),
		Currency:    d.str(colCurrency),
		CreatedAt:   d.datetime(colCreatedAt),
		Version:     d.int(colVersion),
	}
	status := d.int(colStatus)
	if paidAt, present := d.nullableDatetime(colPaidAt); present {
		r.PaidAt = paidAt
	}
	if d.err != nil {
		return store.Row{}, false, d.err
	}
	switch {
	case r.MerchantID <= 0:
		return store.Row{}, false, fmt.Errorf("%w: merchant_id %d is not positive", ErrSchema, r.MerchantID)
	case r.OrderID <= 0:
		return store.Row{}, false, fmt.Errorf("%w: order_id %d is not positive", ErrSchema, r.OrderID)
	case status < math.MinInt8 || status > math.MaxInt8:
		return store.Row{}, false, fmt.Errorf("%w: status %d out of TINYINT range", ErrSchema, status)
	}
	r.Status = int32(status)
	return r, true, nil
}

// decoder 逐列取值，记下第一个错误，之后的取值都返回零值。
type decoder struct {
	after map[string]*cdcv1.Value
	err   error
}

func (d *decoder) fail(col, format string, args ...any) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: column %s: %s", ErrSchema, col, fmt.Sprintf(format, args...))
	}
}

// value 返回列的值。列不在 map 里时记错误；present 为 false 表示 NULL。
func (d *decoder) value(col string) (v *cdcv1.Value, present bool) {
	if d.err != nil {
		return nil, false
	}
	v, exists := d.after[col]
	if !exists {
		d.fail(col, "missing")
		return nil, false
	}
	if v.GetKind() == nil {
		return v, false
	}
	return v, true
}

// nullableInt 取一个整数列，present 为 false 表示 NULL。
func (d *decoder) nullableInt(col string) (n int64, present bool) {
	v, present := d.value(col)
	if !present {
		return 0, false
	}
	switch k := v.GetKind().(type) {
	case *cdcv1.Value_Int:
		return k.Int, true
	case *cdcv1.Value_Uint:
		if k.Uint > math.MaxInt64 {
			d.fail(col, "unsigned value %d overflows int64", k.Uint)
			return 0, false
		}
		return int64(k.Uint), true
	default:
		d.fail(col, "want an integer, got %T", k)
		return 0, false
	}
}

// int 取一个 NOT NULL 的整数列。
func (d *decoder) int(col string) int64 {
	n, present := d.nullableInt(col)
	if !present {
		d.fail(col, "unexpected NULL")
	}
	return n
}

// str 取一个 NOT NULL 的字符列。
func (d *decoder) str(col string) string {
	v, present := d.value(col)
	if !present {
		d.fail(col, "unexpected NULL")
		return ""
	}
	k, ok := v.GetKind().(*cdcv1.Value_Str)
	if !ok {
		d.fail(col, "want a string, got %T", v.GetKind())
		return ""
	}
	return k.Str
}

// nullableDatetime 取一个 DATETIME 列，按 UTC 解释。present 为 false 表示 NULL。
func (d *decoder) nullableDatetime(col string) (t time.Time, present bool) {
	v, present := d.value(col)
	if !present {
		return time.Time{}, false
	}
	k, ok := v.GetKind().(*cdcv1.Value_Str)
	if !ok {
		d.fail(col, "want a datetime string, got %T", v.GetKind())
		return time.Time{}, false
	}
	t, err := time.ParseInLocation(datetimeLayout, k.Str, time.UTC)
	if err != nil {
		d.fail(col, "parse datetime %q: %v", k.Str, err)
		return time.Time{}, false
	}
	return t, true
}

// datetime 取一个 NOT NULL 的 DATETIME 列。
func (d *decoder) datetime(col string) time.Time {
	t, present := d.nullableDatetime(col)
	if !present {
		d.fail(col, "unexpected NULL")
	}
	return t
}
