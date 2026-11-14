package recon

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/palemoky/oppenheimer/pkg/merkle"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

// 本文件是五对的定义。每个投影写的是一条不变式：两侧按它投影之后应当是同一个集合。
// 不变式写错，对账就会漏报或误报，所以改这里之前先读设计文档 7.7。

// Pair 的名字。
const (
	PairReplica = "replica"
	PairIndex   = "index"
	PairPayment = "payment"
	PairStock   = "stock"
	PairArchive = "archive"
)

// PairNames 是全部的 Pair，按建议的执行顺序。
var PairNames = []string{PairReplica, PairIndex, PairPayment, PairStock, PairArchive}

// 状态码，与各服务表里的注释一致（proto 的枚举值）。
const (
	orderPendingPayment = 2
	orderPaid           = 3

	paymentSucceeded = 2

	reservationReserved  = 1
	reservationConfirmed = 2
)

// seededUserBase 是造数工具的用户号起点（seed.UserBase）。造出来的订单只在订单库和商家索引里，
// 在 payment、inventory 里没有对应的行，所以 payment、stock 两对要排除它们。
const seededUserBase int64 = 1 << 40

var notSeeded = "user_id < " + strconv.FormatInt(seededUserBase, 10)

func and(conds ...string) string {
	out := ""
	for _, c := range conds {
		if c == "" {
			continue
		}
		if out != "" {
			out += " AND "
		}
		out += c
	}
	return out
}

func intCol(name string) merkle.Column { return merkle.Column{Name: name, Expr: name} }
func nullInt(name string) merkle.Column {
	return merkle.Column{Name: name, Expr: name, Nullable: true}
}

func bytesCol(name string) merkle.Column {
	return merkle.Column{Name: name, Expr: name, Kind: merkle.Bytes}
}

// 订单库三张表的“除时间外的全部列”，用于主从和归档：两侧是同一份数据的原样拷贝。
// 冷库的表多一个 archived_at（时间，不比），payment_results_archive 的主键多了 order_id，列是一样的。
type tableSpec struct {
	base     string // 热库的表名前缀，带 slot 号
	cold     string // 冷库的表名
	id, cols []merkle.Column
}

var orderTables = []tableSpec{
	{
		base: "orders", cold: "orders_archive",
		id: []merkle.Column{intCol("order_id")},
		cols: []merkle.Column{
			intCol("user_id"), nullInt("merchant_id"), intCol("version"), intCol("status"), intCol("total_amount"),
			bytesCol("currency"), bytesCol("idempotency_key"), bytesCol("request_hash"), nullInt("payment_id"),
			{Name: "fail_reason", Expr: "fail_reason", Kind: merkle.Bytes, Nullable: true},
		},
	},
	{
		base: "order_items", cold: "order_items_archive",
		id:   []merkle.Column{intCol("order_id"), intCol("sku_id")},
		cols: []merkle.Column{intCol("quantity"), intCol("unit_price")},
	},
	{
		// 热表的主键是 payment_id，按订单号的范围走 idx_payment_results_order。
		base: "payment_results", cold: "payment_results_archive",
		id:   []merkle.Column{intCol("order_id"), intCol("payment_id")},
		cols: []merkle.Column{intCol("succeeded"), intCol("amount"), intCol("disposition")},
	},
}

type builder struct {
	topo   Topology
	router *sharding.Router
}

// hot 对订单库的每个 slot 调用一次 mk，返回各 slot 的表。dbs 是按库序号排列的连接。
func (b *builder) hot(dbs []*DB, mk func(slot int) (from, where string)) []Table {
	var out []Table
	for i, db := range dbs {
		for _, slot := range b.router.SlotsOf(i) {
			from, where := mk(slot)
			out = append(out, Table{DB: db, Label: db.Name + "." + from, From: from, Where: where, RangeIndexed: true})
		}
	}
	return out
}

// readers 返回读订单库用的连接：有从库用从库。
func (b *builder) readers() []*DB {
	if len(b.topo.OrderReplicas) > 0 {
		return b.topo.OrderReplicas
	}
	return b.topo.OrderPrimaries
}

// hotAndCold 返回“热库各 slot 加冷库”的表。没有冷库时只有热库。
func (b *builder) hotAndCold(mkHot func(slot int) (from, where string), coldFrom, coldWhere string) []Table {
	return b.hotAndColdOn(b.readers(), mkHot, coldFrom, coldWhere)
}

func (b *builder) hotAndColdOn(dbs []*DB, mkHot func(slot int) (from, where string), coldFrom, coldWhere string) []Table {
	out := b.hot(dbs, mkHot)
	if a := b.topo.Archive; a != nil {
		out = append(out, Table{DB: a, Label: a.Name + "." + coldFrom, From: coldFrom, Where: coldWhere, RangeIndexed: true})
	}
	return out
}

// Build 按名字构造 Pair。topo 里缺它需要的库时返回错误。
func Build(name string, topo Topology) (Pair, error) {
	if len(topo.OrderPrimaries) == 0 {
		return Pair{}, errors.New("recon: no order databases")
	}
	if n := len(topo.OrderReplicas); n != 0 && n != len(topo.OrderPrimaries) {
		return Pair{}, fmt.Errorf("recon: %d order replicas for %d primaries", n, len(topo.OrderPrimaries))
	}
	router, err := sharding.New(sharding.OrderLayout(len(topo.OrderPrimaries)))
	if err != nil {
		return Pair{}, fmt.Errorf("recon: order shard router: %w", err)
	}
	b := &builder{topo: topo, router: router}
	switch name {
	case PairReplica:
		return b.replica()
	case PairIndex:
		return b.index()
	case PairPayment:
		return b.payment()
	case PairStock:
		return b.stock()
	case PairArchive:
		return b.archive()
	}
	return Pair{}, fmt.Errorf("recon: unknown pair %q", name)
}

// replica：从库是主库的原样拷贝。三张表各比一次，所有库的 slot 加在一起。
func (b *builder) replica() (Pair, error) {
	if len(b.topo.OrderReplicas) == 0 {
		return Pair{}, errors.New("recon: pair replica needs the order replicas")
	}
	p := Pair{Name: PairReplica}
	for _, t := range orderTables {
		mk := func(slot int) (string, string) { return sharding.Table(t.base, slot), "" }
		p.Comparisons = append(p.Comparisons, Comparison{
			Name:  PairReplica + "/" + t.base,
			Left:  Side{Name: "primary", Tables: b.hot(b.topo.OrderPrimaries, mk), Key: "order_id", ID: t.id, Cols: t.cols},
			Right: &Side{Name: "replica", Tables: b.hot(b.topo.OrderReplicas, mk), Key: "order_id", ID: t.id, Cols: t.cols},
		})
	}
	return p, nil
}

// archive：归档是搬家，“热库加冷库”在归档前后是同一个集合。右侧是归档前存下来的树。
//
// 热库读主库：和文件比没有逐行比，也就没有复查，从库落后造成的在途差异没有地方过滤。
// 同样的原因，存快照和比对都要在归档任务没有在跑的时候做：热库和冷库不是同一时刻扫的，
// 两次扫描之间被搬走的订单会被数两次或一次都没数到。
func (b *builder) archive() (Pair, error) {
	if b.topo.Archive == nil {
		return Pair{}, errors.New("recon: pair archive needs the archive database")
	}
	p := Pair{Name: PairArchive}
	for _, t := range orderTables {
		mk := func(slot int) (string, string) { return sharding.Table(t.base, slot), "" }
		p.Comparisons = append(p.Comparisons, Comparison{
			Name: PairArchive + "/" + t.base,
			Left: Side{Name: "hot + cold", Tables: b.hotAndColdOn(b.topo.OrderPrimaries, mk, t.cold, ""), Key: "order_id", ID: t.id, Cols: t.cols},
		})
	}
	return p, nil
}

// index：每个有商家的订单（热库或冷库）在商家索引里恰好有一行，五个列相同；索引里没有别的行。
// 索引行的 src_version 是它同步到的订单行版本，追平之后等于订单行的 version。
func (b *builder) index() (Pair, error) {
	if len(b.topo.MerchantIndex) != sharding.MerchantIndexDBs {
		return Pair{}, fmt.Errorf("recon: pair index needs %d merchant index databases, got %d", sharding.MerchantIndexDBs, len(b.topo.MerchantIndex))
	}
	ir, err := sharding.New(sharding.MerchantIndexLayout())
	if err != nil {
		return Pair{}, fmt.Errorf("recon: merchant index shard router: %w", err)
	}
	id := []merkle.Column{intCol("order_id")}
	const hasMerchant = "merchant_id IS NOT NULL"
	left := Side{
		Name:   "orders",
		Tables: b.hotAndCold(func(slot int) (string, string) { return sharding.Table("orders", slot), hasMerchant }, "orders_archive", hasMerchant),
		Key:    "order_id", ID: id,
		Cols: []merkle.Column{intCol("merchant_id"), intCol("user_id"), intCol("status"), intCol("total_amount"), intCol("version")},
	}
	right := Side{
		Name: "merchant index", Key: "order_id", ID: id,
		Cols: []merkle.Column{intCol("merchant_id"), intCol("user_id"), intCol("status"), intCol("total_amount"), {Name: "version", Expr: "src_version"}},
	}
	for i, db := range b.topo.MerchantIndex {
		for _, slot := range ir.SlotsOf(i) {
			from := sharding.Table("merchant_orders", slot)
			// 主键是 (merchant_id, order_id)，没有以订单号开头的索引。
			right.Tables = append(right.Tables, Table{DB: db, Label: db.Name + "." + from, From: from})
		}
	}
	return Pair{Name: PairIndex, Comparisons: []Comparison{{Name: PairIndex, Left: left, Right: &right, Repairable: true}}}, nil
}

// payment：付清的订单和成功的支付单一一对应，支付单号、金额、币种相同（load verify 的第 7、8 项）。
//
// 订单 PAID 而支付单不是 SUCCEEDED（或没有）表现为“只在左”；支付单 SUCCEEDED 而订单不是 PAID
// 表现为“只在右”（正常的中间状态是订单已关单、支付单等着被退款，过了水位线还在就是没退）。
func (b *builder) payment() (Pair, error) {
	if b.topo.Payment == nil {
		return Pair{}, errors.New("recon: pair payment needs the payment database")
	}
	id := []merkle.Column{intCol("order_id")}
	where := and("status = "+strconv.Itoa(orderPaid), notSeeded)
	left := Side{
		Name:   "paid orders",
		Tables: b.hotAndCold(func(slot int) (string, string) { return sharding.Table("orders", slot), where }, "orders_archive", where),
		Key:    "order_id", ID: id,
		// PAID 的订单 payment_id 不应为 NULL；照实投影，为 NULL 时表现为两边不同。
		Cols: []merkle.Column{nullInt("payment_id"), {Name: "amount", Expr: "total_amount"}, bytesCol("currency")},
	}
	right := Side{
		Name: "succeeded payments", Key: "order_id", ID: id,
		Tables: []Table{{
			DB: b.topo.Payment, Label: b.topo.Payment.Name + ".payments", From: "payments",
			Where: and("status = "+strconv.Itoa(paymentSucceeded), notSeeded), RangeIndexed: true, // uk_payments_order
		}},
		Cols: []merkle.Column{intCol("payment_id"), intCol("amount"), bytesCol("currency")},
	}
	return Pair{Name: PairPayment, Comparisons: []Comparison{{Name: PairPayment, Left: left, Right: &right}}}, nil
}

// stock：占着库存的订单明细和没有释放的预占明细一一对应，数量、单价、“占着还是已售”相同
// （load verify 的第 4 到 6 项，细到每个订单的每个 SKU）。
//
// 两侧都把“已释放”投影成“没有这一行”：订单侧只取 PENDING_PAYMENT 和 PAID，预占侧只取 RESERVED 和
// CONFIRMED。这样预占之前就失败的订单（没有预占记录）和先释放后失败的订单（预占记录是 RELEASED）是一回事。
// 订单已取消而预占没释放表现为“只在右”，订单付清而预占被释放表现为“只在左”。
func (b *builder) stock() (Pair, error) {
	if b.topo.Inventory == nil {
		return Pair{}, errors.New("recon: pair stock needs the inventory database")
	}
	id := []merkle.Column{{Name: "order_id", Expr: "i.order_id"}, {Name: "sku_id", Expr: "i.sku_id"}}
	cols := func(state string) []merkle.Column {
		return []merkle.Column{
			{Name: "quantity", Expr: "i.quantity"},
			{Name: "unit_price", Expr: "i.unit_price"},
			// 1 占着（未支付），2 已售。
			{Name: "state", Expr: state},
		}
	}
	join := func(items, orders string) string {
		return items + " i JOIN " + orders + " o ON o.order_id = i.order_id"
	}
	where := and(fmt.Sprintf("o.status IN (%d, %d)", orderPendingPayment, orderPaid), "o."+notSeeded)
	left := Side{
		Name: "order items holding stock",
		Tables: b.hotAndCold(func(slot int) (string, string) {
			return join(sharding.Table("order_items", slot), sharding.Table("orders", slot)), where
		}, join("order_items_archive", "orders_archive"), where),
		Key: "i.order_id", ID: id,
		Cols: cols(fmt.Sprintf("CASE o.status WHEN %d THEN 2 ELSE 1 END", orderPaid)),
	}
	right := Side{
		Name: "reservations not released", Key: "i.order_id", ID: id,
		Tables: []Table{{
			DB: b.topo.Inventory, Label: b.topo.Inventory.Name + ".reservation_item",
			From:         "reservation_item i JOIN reservation o ON o.order_id = i.order_id",
			Where:        fmt.Sprintf("o.status IN (%d, %d)", reservationReserved, reservationConfirmed),
			RangeIndexed: true,
		}},
		Cols: cols(fmt.Sprintf("CASE o.status WHEN %d THEN 2 ELSE 1 END", reservationConfirmed)),
	}
	return Pair{Name: PairStock, Comparisons: []Comparison{{Name: PairStock, Left: left, Right: &right}}}, nil
}
