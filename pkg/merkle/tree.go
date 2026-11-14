package merkle

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// FanBits 是每往下一层多看的键的位数，树是 2^FanBits 叉的。
const FanBits = 4

// RootShift 是根节点的 Shift：键的 64 位全部抹掉，只剩一个节点。
const RootShift = 64

// Node 标识树上的一个节点：键右移 Shift 位之后等于 Index 的所有键，也就是区间
// [Index << Shift, (Index + 1) << Shift)。Shift 越小节点越细。
type Node struct {
	Shift uint
	Index uint64
}

// Root 是根节点，覆盖全部的键。
var Root = Node{Shift: RootShift}

// NodeOf 返回键 key 在 shift 这一层所属的节点。
func NodeOf(key uint64, shift uint) Node {
	if shift >= 64 {
		return Node{Shift: shift}
	}
	return Node{Shift: shift, Index: key >> shift}
}

// Range 返回节点覆盖的键的闭区间 [lo, hi]。用闭区间是因为最右边的节点的上界开区间写不进 uint64。
func (n Node) Range() (lo, hi uint64) {
	if n.Shift >= 64 {
		return 0, ^uint64(0)
	}
	lo = n.Index << n.Shift
	return lo, lo | (1<<n.Shift - 1)
}

// Parent 返回上一层包含 n 的节点。
func (n Node) Parent() Node {
	up := n.Shift + FanBits
	if up >= 64 {
		return Node{Shift: up}
	}
	// n.Shift < 60，所以 n.Index 的位数足够右移。
	return Node{Shift: up, Index: n.Index >> FanBits}
}

// Contains 报告 n 是否包含更细一层或同一层的节点 c。
func (n Node) Contains(c Node) bool {
	if c.Shift > n.Shift {
		return false
	}
	if n.Shift >= 64 {
		return true
	}
	return c.Index>>(n.Shift-c.Shift) == n.Index
}

func (n Node) String() string { return fmt.Sprintf("%d/%d", n.Index, n.Shift) }

// Entry 是一个节点和它的摘要。
type Entry struct {
	Node   Node
	Digest Digest
}

// Tree 是放在内存里的一棵树：只存叶子这一层（LeafShift）的摘要，上面各层由叶子相加得到。
// 摘要为空的叶子不存，所以“没有这个节点”和“这个节点下没有行”是一回事。
//
// Tree 不是并发安全的。
type Tree struct {
	leafShift uint
	leaves    map[uint64]Digest
	// upper 缓存叶子以上各层的摘要，键是 Shift；叶子有改动时作废。
	upper map[uint]map[uint64]Digest
}

// NewTree 返回一棵空树。leafShift 是叶子层的 Shift，必须小于 RootShift，
// 并且与 RootShift 的差是 FanBits 的倍数（这样从根往下恰好能走到叶子层）。
func NewTree(leafShift uint) (*Tree, error) {
	if leafShift >= RootShift || (RootShift-leafShift)%FanBits != 0 {
		return nil, fmt.Errorf("merkle: leaf shift %d must be below %d and differ from it by a multiple of %d", leafShift, RootShift, FanBits)
	}
	return &Tree{leafShift: leafShift, leaves: map[uint64]Digest{}}, nil
}

// LeafShift 返回叶子层的 Shift。
func (t *Tree) LeafShift() uint { return t.leafShift }

// Len 返回非空叶子的个数。
func (t *Tree) Len() int { return len(t.leaves) }

func (t *Tree) set(index uint64, d Digest) {
	if d.IsZero() {
		delete(t.leaves, index)
	} else {
		t.leaves[index] = d
	}
	t.upper = nil
}

// AddLeaf 把摘要 d 加到叶子 index 上。数据库按叶子聚合出的结果用它放进来；
// 同一个叶子可以加多次（各分片各算各的），结果是它们的和。
func (t *Tree) AddLeaf(index uint64, d Digest) { t.set(index, t.leaves[index].Add(d)) }

// Add 加入键为 key、行哈希为 h 的一行。
func (t *Tree) Add(key uint64, h Hash) { t.AddLeaf(key>>t.leafShift, Of(h)) }

// Remove 去掉键为 key、行哈希为 h 的一行。这一行必须在树里，否则树的内容不再有意义。
func (t *Tree) Remove(key uint64, h Hash) {
	i := key >> t.leafShift
	t.set(i, t.leaves[i].Sub(Of(h)))
}

// Merge 把 o 的全部内容加进 t。两棵树的叶子层必须相同，两边的行必须不相交。
func (t *Tree) Merge(o *Tree) error {
	if o.leafShift != t.leafShift {
		return fmt.Errorf("merkle: merge trees with leaf shifts %d and %d", t.leafShift, o.leafShift)
	}
	for i, d := range o.leaves {
		t.AddLeaf(i, d)
	}
	return nil
}

// level 返回 shift 这一层全部非空节点的摘要。调用方不能修改返回的 map。
func (t *Tree) level(shift uint) map[uint64]Digest {
	if shift == t.leafShift {
		return t.leaves
	}
	if m, ok := t.upper[shift]; ok {
		return m
	}
	below := t.level(shift - FanBits)
	m := make(map[uint64]Digest, len(below)/(1<<FanBits)+1)
	for i, d := range below {
		p := Node{Shift: shift - FanBits, Index: i}.Parent().Index
		m[p] = m[p].Add(d)
	}
	// 上层的节点只要有一个非空的子节点，行数就不为零，所以不会是零值，不需要像叶子那样删。
	if t.upper == nil {
		t.upper = map[uint]map[uint64]Digest{}
	}
	t.upper[shift] = m
	return m
}

func (t *Tree) valid(shift uint) bool {
	return shift >= t.leafShift && shift <= RootShift && (shift-t.leafShift)%FanBits == 0
}

// Root 返回整棵树的摘要。
func (t *Tree) Root() Digest { return t.level(RootShift)[0] }

// Get 返回节点 n 的摘要，n 下没有行时是零值。n 不在这棵树的层上（比叶子细，或 Shift 对不上）时返回错误。
func (t *Tree) Get(n Node) (Digest, error) {
	if !t.valid(n.Shift) {
		return Digest{}, fmt.Errorf("merkle: node %v is not on a level of a tree with leaf shift %d: %w", n, t.leafShift, ErrTooDeep)
	}
	return t.level(n.Shift)[n.Index], nil
}

// ErrTooDeep 表示向一棵树要它叶子层以下的节点。
var ErrTooDeep = errors.New("merkle: below the leaf level of the tree")

// Children 实现 Source：返回 parents 各自的非空子节点，按 Index 升序。
// parents 必须在同一层，并且在叶子层之上。
func (t *Tree) Children(_ context.Context, parents []Node) ([]Entry, error) {
	if len(parents) == 0 {
		return nil, nil
	}
	shift := parents[0].Shift
	if !t.valid(shift) || shift == t.leafShift {
		return nil, fmt.Errorf("merkle: children of %v in a tree with leaf shift %d: %w", parents[0], t.leafShift, ErrTooDeep)
	}
	lvl := t.level(shift - FanBits)
	var out []Entry
	for _, p := range parents {
		if p.Shift != shift {
			return nil, fmt.Errorf("merkle: parents %v and %v are on different levels", parents[0], p)
		}
		// 子节点的 Index 是父节点的 Index 后面接 FanBits 位。根的子节点 Shift 是 60，Index 只有 4 位。
		base := uint64(0)
		if shift < 64 {
			base = p.Index << FanBits
		}
		for i := range uint64(1 << FanBits) {
			if d, ok := lvl[base|i]; ok {
				out = append(out, Entry{Node: Node{Shift: shift - FanBits, Index: base | i}, Digest: d})
			}
		}
	}
	slices.SortFunc(out, func(a, b Entry) int { return cmp.Compare(a.Node.Index, b.Node.Index) })
	return out, nil
}

// Leaves 返回全部非空叶子，按 Index 升序。
func (t *Tree) Leaves() []Entry {
	out := make([]Entry, 0, len(t.leaves))
	for _, i := range slices.Sorted(maps.Keys(t.leaves)) {
		out = append(out, Entry{Node: Node{Shift: t.leafShift, Index: i}, Digest: t.leaves[i]})
	}
	return out
}

// 序列化格式：魔数，叶子层的 Shift，叶子个数，然后每个叶子 32 字节（Index、Count、Hi、Lo），按 Index 升序。
// 都是大端。同样内容的树序列化结果相同。
var magic = [8]byte{'O', 'P', 'M', 'E', 'R', 'K', 'L', '1'}

const (
	headerSize = len(magic) + 8 + 8
	leafSize   = 32
)

// MarshalBinary 实现 encoding.BinaryMarshaler。
func (t *Tree) MarshalBinary() ([]byte, error) {
	b := make([]byte, 0, headerSize+leafSize*len(t.leaves))
	b = append(b, magic[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(t.leafShift))
	b = binary.BigEndian.AppendUint64(b, uint64(len(t.leaves)))
	for _, e := range t.Leaves() {
		b = binary.BigEndian.AppendUint64(b, e.Node.Index)
		b = binary.BigEndian.AppendUint64(b, e.Digest.Count)
		b = binary.BigEndian.AppendUint64(b, e.Digest.Hi)
		b = binary.BigEndian.AppendUint64(b, e.Digest.Lo)
	}
	return b, nil
}

// UnmarshalBinary 实现 encoding.BinaryUnmarshaler，t 原有的内容被替换。
func (t *Tree) UnmarshalBinary(b []byte) error {
	if len(b) < headerSize || [8]byte(b[:8]) != magic {
		return errors.New("merkle: not a serialized tree")
	}
	shift := binary.BigEndian.Uint64(b[8:16])
	n := binary.BigEndian.Uint64(b[16:24])
	body := b[headerSize:]
	if n != uint64(len(body))/leafSize || len(body)%leafSize != 0 {
		return fmt.Errorf("merkle: serialized tree declares %d leaves, has %d bytes of leaves", n, len(body))
	}
	if shift >= RootShift {
		return fmt.Errorf("merkle: serialized tree has leaf shift %d", shift)
	}
	nt, err := NewTree(uint(shift))
	if err != nil {
		return err
	}
	for ; len(body) > 0; body = body[leafSize:] {
		i := binary.BigEndian.Uint64(body[0:8])
		d := Digest{Count: binary.BigEndian.Uint64(body[8:16]), Hi: binary.BigEndian.Uint64(body[16:24]), Lo: binary.BigEndian.Uint64(body[24:32])}
		if _, dup := nt.leaves[i]; dup || d.IsZero() {
			return fmt.Errorf("merkle: serialized tree has a duplicate or empty leaf %d", i)
		}
		nt.leaves[i] = d
	}
	*t = *nt
	return nil
}
