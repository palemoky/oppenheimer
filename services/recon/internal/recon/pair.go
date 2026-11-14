// Package recon 实现对账：把两侧的数据各自投影成同样的行，在数据库里按订单号的区间算摘要，
// 自上而下只展开摘要不同的区间，定位到行。设计见 docs/design/1d-protection.md 第 7 节。
package recon

import (
	"database/sql"

	"github.com/palemoky/oppenheimer/pkg/merkle"
)

// DB 是一个带名字的连接池，名字用于报告和日志。
type DB struct {
	Name string
	Conn *sql.DB
}

// Table 是一侧里的一份物理数据：一张分片表，或者几张表的连接。
type Table struct {
	DB *DB
	// Label 用于报告，例如 "orders_3"。
	Label string
	// From 是 FROM 子句的内容，可以带别名和 JOIN。Side 里的表达式引用这里的别名。
	From string
	// Where 是追加的过滤条件（不带 WHERE、AND），可以为空。
	Where string
	// RangeIndexed 表示 Side.Key 上的范围条件能走索引。为 false 时按订单号区间取数是全表扫描，
	// 所以同一层要展开的节点必须合成一条语句（设计文档 7.5）。
	RangeIndexed bool
}

// Side 是一次比对的一侧：若干份物理数据，用同一个投影。各表的摘要相加就是这一侧的摘要，
// 所以两侧的分片方式可以不同；同一侧的各表之间行不能重复。
type Side struct {
	// Name 用于报告，例如 "orders (hot + cold)"。
	Name   string
	Tables []Table
	// Key 是订单号的表达式，决定一行落在树的哪个节点。两侧必须是同一个值。
	Key string
	// ID 是行的身份：两侧 ID 相同的行是“同一行”。第一列必须是订单号。
	ID []merkle.Column
	// Cols 是要比的列。两侧的列数、顺序、含义必须相同，名字以左侧为准。
	Cols []merkle.Column
}

// Comparison 是一次比对：左右两侧应当是同一个行的集合。
type Comparison struct {
	// Name 用于报告和命令行，例如 "index"、"replica/orders"。
	Name string
	Left Side
	// Right 为 nil 时右侧是之前存下来的一棵树（归档的前后对比），只能定位到节点。
	Right *Side
	// Repairable 为 true 时右侧是左侧的派生数据，可以按左侧修复（只有商家索引）。
	Repairable bool
}

// Pair 是命令行上的一个对账对象，含一次或几次比对（例如主从要比三张表）。
type Pair struct {
	Name        string
	Comparisons []Comparison
}

// Topology 是对账要连的全部数据库。只有被选中的 Pair 用到的库才需要给出。
type Topology struct {
	// OrderPrimaries 是订单库的主库，个数和顺序与 order 服务的 MYSQL_DSNS 一致。
	OrderPrimaries []*DB
	// OrderReplicas 与 OrderPrimaries 一一对应；为空时订单库的读取落在主库上。
	OrderReplicas []*DB
	// Archive 是冷库；为 nil 时各对的订单侧不包含已归档的订单。
	Archive *DB
	// MerchantIndex 是商家索引库，个数和顺序与 orderindex 的 MYSQL_DSNS 一致。
	MerchantIndex []*DB
	Payment       *DB
	Inventory     *DB
}
