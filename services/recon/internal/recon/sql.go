package recon

import (
	"math"
	"slices"
	"strings"

	"github.com/palemoky/oppenheimer/pkg/merkle"
)

// 本文件拼对账的 SQL。表名、表达式都来自 pairs.go 的常量（Table.From、Table.Where、Side 的列），
// 值一律走占位符，所以拼接是安全的；用到这些语句的地方以此为由标注 nolint:gosec。

// keyRange 是订单号上的闭区间 [lo, hi]，和 merkle.Node.Range 一样用闭区间。
type keyRange struct{ lo, hi uint64 }

func nodeRange(n merkle.Node) keyRange {
	lo, hi := n.Range()
	return keyRange{lo: lo, hi: hi}
}

// sqlArgs 返回区间在 BIGINT（有符号）上的两端。订单号是正的 int64，区间整个落在负数那一半
// （下界超过 MaxInt64）时没有任何行，ok 为 false。
func (r keyRange) sqlArgs() (lo, hi int64, ok bool) {
	if r.lo > math.MaxInt64 {
		return 0, 0, false
	}
	return int64(r.lo), int64(min(r.hi, math.MaxInt64)), true //nolint:gosec // 两端都已限制在 MaxInt64 以内
}

// keyOf 把数据库里读到的订单号换成树上的键。订单号不为负；万一读到负数（数据坏了），
// 它会落在树的右半边，摘要照样对不上，不会被悄悄忽略。
func keyOf(k int64) uint64 { return uint64(k) } //nolint:gosec // 见上

// mergeRanges 把按下界升序的区间里首尾相接的合并成一个。
func mergeRanges(rs []keyRange) []keyRange {
	var out []keyRange
	for _, r := range rs {
		if n := len(out); n > 0 && out[n-1].hi != math.MaxUint64 && out[n-1].hi+1 == r.lo {
			out[n-1].hi = r.hi
			continue
		}
		out = append(out, r)
	}
	return out
}

// cond 是一组用 AND 连接的条件和它们的参数。
type cond struct {
	parts []string
	args  []any
}

func (c *cond) add(part string, args ...any) {
	c.parts = append(c.parts, part)
	c.args = append(c.args, args...)
}

// where 返回 " WHERE ..."，没有条件时是空串。
func (c *cond) where() string {
	if len(c.parts) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(c.parts, " AND ")
}

// baseCond 是一张表上所有语句都带的条件：水位线和表自己的过滤条件。bound 为 0 时不设上界。
func baseCond(s *Side, t Table, bound int64) *cond {
	c := &cond{}
	if bound > 0 {
		c.add(s.Key+" < ?", bound)
	}
	if t.Where != "" {
		c.add("(" + t.Where + ")")
	}
	return c
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// addRange 加一个订单号的区间条件。区间里不可能有行时返回 false。
func addRange(c *cond, key string, r keyRange) bool {
	lo, hi, ok := r.sqlArgs()
	if !ok {
		return false
	}
	c.add(key+" BETWEEN ? AND ?", lo, hi)
	return true
}

// addRanges 用 OR 加多个区间。没有可能有行的区间时返回 false。
func addRanges(c *cond, key string, rs []keyRange) bool {
	var (
		parts []string
		args  []any
	)
	for _, r := range rs {
		lo, hi, ok := r.sqlArgs()
		if !ok {
			continue
		}
		parts = append(parts, key+" BETWEEN ? AND ?")
		args = append(args, lo, hi)
	}
	if len(parts) == 0 {
		return false
	}
	c.add("("+strings.Join(parts, " OR ")+")", args...)
	return true
}

// canonAll 是行哈希用的投影：ID 加 Cols。
func (s *Side) canonAll() string {
	return merkle.CanonExpr(slices.Concat(s.ID, s.Cols))
}

// digestSQL 是按节点聚合摘要的语句。第一个参数是 shift，其后是 c 的参数。
func digestSQL(s *Side, t Table, c *cond) string {
	hi, lo := merkle.SumExprs("h")
	return "SELECT k >> ? AS node, COUNT(*), " + hi + ", " + lo +
		" FROM (SELECT " + s.Key + " AS k, " + merkle.HashExpr(s.canonAll()) + " AS h FROM " + t.From + c.where() +
		") x GROUP BY node ORDER BY node"
}

// rowsSQL 是逐行比读行的语句：订单号、ID 的规范串、Cols 的规范串。
func rowsSQL(s *Side, t Table, c *cond) string {
	return "SELECT " + s.Key + ", " + merkle.CanonExpr(s.ID) + ", " + merkle.CanonExpr(s.Cols) + " FROM " + t.From + c.where()
}

// edgeSQL 是取一张表最小（desc 为 false）或最大订单号的语句。用 ORDER BY ... LIMIT 1 而不是 MIN/MAX：
// 单表时两者一样走索引的一端；From 是连接时 MIN/MAX 会把整个连接算一遍，ORDER BY 驱动表的键加 LIMIT 1
// 沿驱动表的索引找到第一个连得上的行就停。不带 Table.Where：它的列多半不在索引里，带上可能要扫很远，
// 而这里只是为了分段，范围宽一点没有关系。
func edgeSQL(s *Side, t Table, c *cond, desc bool) string {
	dir := " ASC"
	if desc {
		dir = " DESC"
	}
	return "SELECT " + s.Key + " FROM " + t.From + c.where() + " ORDER BY " + s.Key + dir + " LIMIT 1"
}
