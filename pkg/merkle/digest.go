// Package merkle 是对账用的哈希树，设计见 docs/design/1d-protection.md 第 7 节，决定见 ADR-0022。
//
// 它不是教科书意义上的 Merkle 树。教科书里父节点的哈希是子节点哈希拼接后的哈希，目的是防篡改和
// 给出包含证明。这里每个节点的摘要是它下面所有行哈希的和（外加行数），目的只有一个：
// 用少量的摘要找出两份数据里不一样的那一小段。换来的三条性质是对账需要的：
//
//   - 与顺序无关：数据库按什么顺序扫、分几次扫，摘要一样。
//   - 可以相加：同一段键的摘要可以由各分片各算各的再加起来，两边的分片方式不必相同。
//   - 任何一层的节点都能直接算，不必先有它的子节点，所以可以只在不同的地方才往下要一层。
//
// 代价是不抗有意构造的碰撞。对账面对的是程序缺陷和运维失误，不是攻击者。
//
// 树是键（订单号）上的基数树：一个节点是键的一个前缀，也就是键上一段连续的区间（见 Node）。
// 本包是纯计算，不碰数据库；摘要在数据库里怎么算见 sql.go，它和 Canon、HashRow 是同一个规范的两份实现。
package merkle

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// Hash 是一行的哈希：规范字符串的 SHA-256 的前 128 位，按大端拆成两个 64 位。
type Hash struct{ Hi, Lo uint64 }

// HashRow 返回规范字符串 canon（见 Canon 和 CanonExpr）的行哈希。
// 与 SQL 里 SHA2(canon, 256) 的十六进制串的第 1 到 16、17 到 32 个字符是同一个数。
func HashRow(canon string) Hash {
	sum := sha256.Sum256([]byte(canon))
	return Hash{Hi: binary.BigEndian.Uint64(sum[0:8]), Lo: binary.BigEndian.Uint64(sum[8:16])}
}

// Digest 是一组行的摘要：行数，加上各行哈希的两半各自对 2^64 取模的和。
// 零值是空集的摘要。Digest 可以直接用 == 比较。
type Digest struct {
	Count  uint64
	Hi, Lo uint64
}

// Of 返回只含一行的摘要。
func Of(h Hash) Digest { return Digest{Count: 1, Hi: h.Hi, Lo: h.Lo} }

// Add 返回两组不相交的行并在一起的摘要。无符号加法溢出时回绕，正好是对 2^64 取模。
func (d Digest) Add(o Digest) Digest {
	return Digest{Count: d.Count + o.Count, Hi: d.Hi + o.Hi, Lo: d.Lo + o.Lo}
}

// Sub 返回从 d 里去掉 o 那些行之后的摘要。o 必须是 d 的子集，否则结果没有意义。
func (d Digest) Sub(o Digest) Digest {
	return Digest{Count: d.Count - o.Count, Hi: d.Hi - o.Hi, Lo: d.Lo - o.Lo}
}

// IsZero 报告 d 是不是空集的摘要。
func (d Digest) IsZero() bool { return d == Digest{} }

// String 返回 “行数:32 位十六进制”，用于报告。
func (d Digest) String() string { return fmt.Sprintf("%d:%016x%016x", d.Count, d.Hi, d.Lo) }
