// Package sharding 计算订单数据的分片位置，不访问数据库。
//
// 数据分成 Slots 个逻辑分片（slot），分布在 DBs 个物理库上：
//
//	slot = 分片键 & (Slots-1)
//	db   = slot % DBs
//
// 订单号由 idgen 生成，最低 6 位是用户号的低 6 位（分片基因），所以按订单号和按用户号
// 算出的 slot 相同。表名带全局 slot 号（orders_3），一个 slot 的表可以整体搬到另一个库，
// 不用改名。布局的取舍见 ADR-0012。
package sharding

import (
	"fmt"
	"math/bits"

	"github.com/palemoky/Oppenheimer/pkg/idgen"
)

// MaxSlots 是 slot 数的上限，等于分片基因能表示的取值个数。
const MaxSlots = 1 << 6

// Config 描述分片布局。两个数都必须是 2 的幂，且 DBs 不超过 Slots。
type Config struct {
	Slots int
	DBs   int
}

// 订单数据的分片布局（ADR-0012）：8 个 slot 分在 2 个库上。order 服务、对账工具和测试都引用这里，
// 不各写一份：两处不一致时，按另一种布局去读的一方会悄悄漏掉分片。
//
// 写成常量而不是配置：配置错了数据就找不到了，改它是一次数据迁移，不是一次重启。
// 用常量而不是包级变量，是为了不让任何地方在运行时改掉它。
const (
	OrderSlots = 8
	OrderDBs   = 2
)

// OrderLayout 返回订单数据的分片布局，即 Config{Slots: OrderSlots, DBs: OrderDBs}。
func OrderLayout() Config { return Config{Slots: OrderSlots, DBs: OrderDBs} }

// Router 按分片键计算 slot 和库号。零值不可用，用 New 构造。并发安全（只读）。
type Router struct {
	slots int
	dbs   int
}

// New 校验 cfg 并返回 Router。
func New(cfg Config) (*Router, error) {
	switch {
	case !isPow2(cfg.Slots) || cfg.Slots > MaxSlots:
		return nil, fmt.Errorf("sharding: slots %d must be a power of two in [1, %d]", cfg.Slots, MaxSlots)
	case !isPow2(cfg.DBs) || cfg.DBs > cfg.Slots:
		return nil, fmt.Errorf("sharding: dbs %d must be a power of two in [1, slots=%d]", cfg.DBs, cfg.Slots)
	}
	return &Router{slots: cfg.Slots, dbs: cfg.DBs}, nil
}

func isPow2(n int) bool { return n > 0 && bits.OnesCount(uint(n)) == 1 }

// Slots 返回 slot 总数。
func (r *Router) Slots() int { return r.slots }

// DBs 返回物理库数。
func (r *Router) DBs() int { return r.dbs }

// ForUser 返回用户的数据所在的 slot。
func (r *Router) ForUser(userID int64) int { return int(idgen.GeneOf(userID)) & (r.slots - 1) }

// ForOrder 返回订单所在的 slot，依据订单号里的分片基因。
// 对 idgen 以 user_id 为分片键生成的订单号，ForOrder(orderID) == ForUser(userID)。
func (r *Router) ForOrder(orderID int64) int { return int(idgen.Gene(orderID)) & (r.slots - 1) }

// DB 返回 slot 所在的库号。slot 越界时 panic：它只能来自 ForUser、ForOrder 或 SlotsOf。
func (r *Router) DB(slot int) int {
	if slot < 0 || slot >= r.slots {
		panic(fmt.Sprintf("sharding: slot %d out of range [0, %d)", slot, r.slots))
	}
	return slot % r.dbs
}

// SlotsOf 返回库 db 上的全部 slot，升序。
func (r *Router) SlotsOf(db int) []int {
	if db < 0 || db >= r.dbs {
		panic(fmt.Sprintf("sharding: db %d out of range [0, %d)", db, r.dbs))
	}
	out := make([]int, 0, r.slots/r.dbs)
	for s := db; s < r.slots; s += r.dbs {
		out = append(out, s)
	}
	return out
}

// Table 返回 slot 上的物理表名，例如 Table("orders", 3) == "orders_3"。
func Table(base string, slot int) string { return fmt.Sprintf("%s_%d", base, slot) }
