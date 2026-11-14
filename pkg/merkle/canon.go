package merkle

import (
	"encoding/hex"
	"strconv"
	"strings"
)

// 规范字符串：一行投影出的各列，用 Sep 连接。
//
//   - 整数写成十进制（负数带 '-'）。
//   - 字符串和二进制写成大写的十六进制，这样内容里出现分隔符或空值标记也不会混淆。
//   - NULL 写成 Null。它不是合法的十进制数，也不是合法的十六进制串（空串的十六进制是空串）。
//
// 时间和浮点数不放进投影：两边的精度或时区不一致时，同样的数据会算出不同的哈希。
const (
	Sep  = "|"
	Null = "~"
)

// Kind 是投影里一列的编码方式。
type Kind int

const (
	// Int 是整数列（含 TINYINT 的状态码）。
	Int Kind = iota
	// Bytes 是字符串或二进制列。
	Bytes
)

// Column 是投影里的一列。
type Column struct {
	// Name 用于报告。
	Name string
	// Expr 是这一列的 SQL 表达式，可以是列名，也可以是把两边不同的表示映射成同一个值的表达式。
	Expr string
	Kind Kind
	// Nullable 为 true 时表达式可能得到 NULL。写错成 false 的后果是 CONCAT_WS 悄悄跳过这一列，
	// (1, NULL, 2) 和 (1, 2, NULL) 拼成同一个串。
	Nullable bool
}

// CanonExpr 返回算出规范字符串的 SQL 表达式。Go 这一侧的对应实现是 Canon。
func CanonExpr(cols []Column) string {
	var b strings.Builder
	b.WriteString("CONCAT_WS('" + Sep + "'")
	for _, c := range cols {
		e := c.Expr
		if c.Kind == Bytes {
			e = "HEX(" + e + ")"
		}
		if c.Nullable {
			e = "COALESCE(" + e + ", '" + Null + "')"
		}
		b.WriteString(", " + e)
	}
	b.WriteString(")")
	return b.String()
}

// HashExpr 返回由规范字符串的表达式算出行哈希（64 个十六进制字符）的 SQL 表达式。
func HashExpr(canon string) string { return "SHA2(" + canon + ", 256)" }

// SumExprs 返回对行哈希列 h 求摘要的两个聚合表达式，结果依次是 Digest 的 Hi 和 Lo，
// 都是 BIGINT UNSIGNED，可以直接读进 uint64。
//
// MySQL 对 BIGINT UNSIGNED 求和得到精确的 DECIMAL（不溢出、不转浮点），取模之后转回无符号整数，
// 和 Digest.Add 的回绕加法是同一个结果。
func SumExprs(h string) (hi, lo string) {
	half := func(from int) string {
		return "CAST(SUM(CAST(CONV(SUBSTRING(" + h + ", " + strconv.Itoa(from) + ", 16), 16, 10) AS UNSIGNED)) % 18446744073709551616 AS UNSIGNED)"
	}
	return half(1), half(17)
}

// Canon 在 Go 这一侧拼规范字符串，结果必须与 CanonExpr 在数据库里算出的逐字节相同
// （sql_test.go 对着真实的 MySQL 核对）。零值可用。
type Canon struct {
	b []byte
	n int
}

func (c *Canon) sep() {
	if c.n > 0 {
		c.b = append(c.b, Sep...)
	}
	c.n++
}

// Int 追加一个整数列。
func (c *Canon) Int(v int64) *Canon {
	c.sep()
	c.b = strconv.AppendInt(c.b, v, 10)
	return c
}

// Bytes 追加一个字符串或二进制列。
func (c *Canon) Bytes(p []byte) *Canon {
	c.sep()
	c.b = append(c.b, strings.ToUpper(hex.EncodeToString(p))...)
	return c
}

// Null 追加一个值为 NULL 的列。
func (c *Canon) Null() *Canon {
	c.sep()
	c.b = append(c.b, Null...)
	return c
}

// String 返回规范字符串。
func (c *Canon) String() string { return string(c.b) }
