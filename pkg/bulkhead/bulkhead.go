// Package bulkhead 是进程内的并发上限：同时最多 N 个调用在使用某个资源，多出来的立刻被拒绝。
//
// 用在代理看不到的地方（设计文档 1d 的 5.7）：进程内部对一个数据库连接池的争用。服务之间的并发上限在
// Envoy 边车上（ADR-0020）。名字取自船的水密隔舱：一个舱进水不淹到别的舱。
package bulkhead

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// ErrFull 表示名额已满。调用方应当立刻返回“资源耗尽、稍后重试”，而不是排队：
// 排队等于把上限从“同时在用的”变成了“同时在用的加在等的”，后者没有上限。
var ErrFull = errors.New("bulkhead: full")

// Bulkhead 是一个不排队的计数信号量。并发安全。nil 的 *Bulkhead 不限制，方便把它做成可选的。
type Bulkhead struct {
	limit    int64
	inFlight atomic.Int64
	rejected atomic.Int64
}

// New 返回上限为 limit 的 Bulkhead。limit 必须为正。
func New(limit int) (*Bulkhead, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("bulkhead: limit must be positive, got %d", limit)
	}
	return &Bulkhead{limit: int64(limit)}, nil
}

// Acquire 占一个名额。成功时返回 release，用完必须调用一次（重复调用无害）；名额已满时返回 ErrFull。
func (b *Bulkhead) Acquire() (release func(), err error) {
	if b == nil {
		return func() {}, nil
	}
	// 先加再判断，超了就退回去。不用“先读、够再加”：两步之间别的协程也在加，会超过上限。
	if b.inFlight.Add(1) > b.limit {
		b.inFlight.Add(-1)
		b.rejected.Add(1)
		return nil, ErrFull
	}
	var once atomic.Bool
	return func() {
		if once.CompareAndSwap(false, true) {
			b.inFlight.Add(-1)
		}
	}, nil
}

// InFlight 返回当前占用的名额数。给日志和测试用。
func (b *Bulkhead) InFlight() int64 {
	if b == nil {
		return 0
	}
	return b.inFlight.Load()
}

// Rejected 返回被拒绝的累计次数。
func (b *Bulkhead) Rejected() int64 {
	if b == nil {
		return 0
	}
	return b.rejected.Load()
}
