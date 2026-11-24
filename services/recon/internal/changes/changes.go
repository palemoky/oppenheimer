// Package changes 跟着 CDC 的变更核对派生数据：订单每变一次，稍后去派生端（商家索引、从库、冷库）
// 按主键读那一行，看它是不是这条变更说的样子（设计文档 3e 的 4.3，ADR-0039）。
//
// # 判定靠版本号，不靠等
//
// 订单行上有递增的 version。设这条变更的版本是 v，派生端那一行的版本是 d：
//
//	d > v                派生端已经是更晚的版本：通过。那个更晚的版本自己有一条变更，会被单独核对。
//	d = v，内容相同       一致：通过。
//	d = v，内容不同       同一个版本两边内容不一样：不一致。不会自己好，第一次看到就报。
//	d < v 或者没有这一行   还没追上：隔一会儿再查；超过 LagAfter 仍然如此才报“落后”。
//
// 没有版本号的话，“派生端的值和变更里的不一样”分不清是没追上、已经被更晚的变更覆盖、还是坏了，只能靠等一会儿再比；
// 有了它，三种情况各有各的判定。核对之前等的那几秒（Delay）只是为了少做无用的复查，取多少不影响对错。
//
// # 它是只读的核对，不是数据通路
//
// 所以出错时的取舍和业务消费者相反：
//
//   - 派生端读不到（库连不上、超时）：返回错误让消费者停在这一批上，不把“读不到”当成不一致，也不往前走。
//   - 消息本身解不开：计数、打日志、跳过。业务消费者遇到这种消息要停（否则数据会丢）；核对程序停了只是自己瞎了，
//     而这条消息的问题商家索引的消费者会报。
//   - 重复消费没有后果。
//
// # 它看不到的
//
// 不经过源库 binlog 的改动（有人直接改了派生端的一行）；CDC 自己漏掉或者搞错的变更（它和索引的消费者读的是
// 同一个 topic）；不在 CDC 里的表。这些靠逐行的树（recon 的每小时窗口和每天全量）。
//
// 等着复查的变更只在内存里：位点在第一次核对之后就提交了，进程重启时正在等的那些不会再被核对。
// 它们是“当时还没追上”的，重启前后那一小段由每小时的逐行对账盖住。
package changes

import (
	"errors"
	"fmt"
	"time"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
)

// SourceTable 是要核对的逻辑表名。别的表的变更忽略。
const SourceTable = "orders"

// 判定和解码要用到的列。其余的列由各个目标自己从映像里取。
const (
	ColOrderID    = "order_id"
	ColVersion    = "version"
	ColMerchantID = "merchant_id"
)

// ErrImage 表示变更的映像缺少要用的列或者类型不对。
var ErrImage = errors.New("changes: row image does not have the expected columns")

// Verdict 是一条变更在一个目标上的判定。
type Verdict int

const (
	// OK 派生端和这条变更一致，或者已经是更晚的版本。
	OK Verdict = iota + 1
	// Behind 派生端还没追上这条变更。隔一会儿再查。
	Behind
	// Mismatch 派生端和这条变更不一致，不会自己好。
	Mismatch
)

// Derived 是派生端那一行相对于一条变更的样子，由目标读出来之后填。
type Derived struct {
	// Found 表示派生端有这一行。
	Found bool
	// Version 是派生端那一行记的源行版本。
	Version int64
	// Diff 在版本相同而内容不同时是第一个不同的列名（给人看的）；内容相同时为空。版本不同时不用填。
	Diff string
}

// Judge 是包注释里那张表。v 是变更的版本。
func Judge(v int64, d Derived) Verdict {
	switch {
	case !d.Found || d.Version < v:
		return Behind
	case d.Version > v:
		return OK
	case d.Diff != "":
		return Mismatch
	default:
		return OK
	}
}

// Change 是解码后的一条变更。
type Change struct {
	OrderID int64
	Version int64
	// MerchantID 为 0 表示这一行的 merchant_id 是 NULL（还没预占成功的订单，不进商家索引）。
	MerchantID int64
	// Slot 是源行所在的 slot。
	Slot int
	// Deleted 表示这是一条 DELETE。orders 的 DELETE 只在归档时出现，Image 是这一行离开热库前的最终映像。
	Deleted bool
	// Image 是这条变更说的整行：DELETE 是 before，其余是 after。
	Image map[string]*cdcv1.Value
	// Commit 是事务在主库上提交的时刻。全量阶段读到的行没有，是零值。
	Commit time.Time
}

// Decode 解码一条变更。ok 为 false 表示这条变更不用核对（别的表、不认识的操作）。
// 映像里缺要用的列时返回包着 ErrImage 的错误。
func Decode(ch *cdcv1.RowChange) (c Change, ok bool, err error) {
	if ch.GetTable() != SourceTable {
		return Change{}, false, nil
	}
	c = Change{Slot: int(ch.GetSlot())}
	switch ch.GetOp() {
	case cdcv1.Op_OP_INSERT, cdcv1.Op_OP_UPDATE, cdcv1.Op_OP_READ:
		c.Image = ch.GetAfter()
	case cdcv1.Op_OP_DELETE:
		c.Image, c.Deleted = ch.GetBefore(), true
	default:
		return Change{}, false, nil
	}
	if cm := ch.GetCommitMicros(); cm > 0 {
		c.Commit = time.UnixMicro(cm)
	}
	var present bool
	if c.OrderID, present, err = imageInt(c.Image, ColOrderID); err != nil || !present || c.OrderID <= 0 {
		return Change{}, false, imageErr(ch, ColOrderID, err)
	}
	if c.Version, present, err = imageInt(c.Image, ColVersion); err != nil || !present {
		return Change{}, false, imageErr(ch, ColVersion, err)
	}
	if c.MerchantID, _, err = imageInt(c.Image, ColMerchantID); err != nil {
		return Change{}, false, imageErr(ch, ColMerchantID, err)
	}
	return c, true, nil
}

func imageErr(ch *cdcv1.RowChange, col string, err error) error {
	if err == nil {
		err = errors.New("null or not positive")
	}
	return fmt.Errorf("%w: %s op=%s slot=%d gtid=%q column %s: %w", ErrImage, ch.GetSource(), ch.GetOp(), ch.GetSlot(), ch.GetGtid(), col, err)
}

// imageInt 取映像里的一个整数列。present 为 false 表示 NULL。列不在映像里、不是整数时返回错误。
func imageInt(img map[string]*cdcv1.Value, col string) (v int64, present bool, err error) {
	val, exists := img[col]
	if !exists {
		return 0, false, errors.New("missing")
	}
	switch k := val.GetKind().(type) {
	case nil:
		return 0, false, nil
	case *cdcv1.Value_Int:
		return k.Int, true, nil
	default:
		return 0, false, fmt.Errorf("is %T, want an integer", k)
	}
}
