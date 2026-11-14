// Package archive 实现订单的冷热归档：冷热的划分规则（本文件）和归档任务。
// 设计见 docs/design/1c-sharding.md 第 6 节，决定见 ADR-0016。
package archive

import (
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
)

// MinHotAge 是订单在热库里至少停留的时间，按订单号里的受理时间算。
//
// 它是读和归档之间的约定，所以是常量而不是配置：
//   - 读路径：订单号新于 now - MinHotAge 的订单一定不在冷库，查不到就是不存在，不去冷库找；
//   - 归档任务：只归档早于 now - MinArchiveAge 的订单；
//   - 落库：受理时间早于 now - MinHotAge 的订单不再插入热库。
//
// 配成两个不一致的值，后果是已归档的订单查不到，或者热库里重新出现一个已归档的订单号。
// 一旦有数据归档就不能调大（读路径会不去冷库找那些已经归档、按新值却算“热”的订单）；调小是安全的。
const MinHotAge = 30 * 24 * time.Hour

// MinArchiveAge 是归档截止点的下限。比 MinHotAge 多出的一天吸收各进程之间的时钟偏差。
const MinArchiveAge = MinHotAge + 24*time.Hour

// DefaultArchiveAge 是归档任务默认的截止点：受理超过 90 天的终态订单。
const DefaultArchiveAge = 90 * 24 * time.Hour

// MaybeCold 报告订单 orderID 在 now 时刻是否可能已经归档，只看订单号里的时间。
// false 表示一定不在冷库。true 不表示在冷库：它可能还没被归档，也可能不是终态而永远留在热库。
func MaybeCold(orderID int64, now time.Time) bool {
	return idgen.Time(orderID).Before(now.Add(-MinHotAge))
}
