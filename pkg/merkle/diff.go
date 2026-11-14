package merkle

import (
	"cmp"
	"context"
	"fmt"
	"slices"
)

// Source 是能按节点给出摘要的一份数据：内存里的 Tree，或者背后是数据库的聚合查询。
type Source interface {
	// Children 返回 parents 各自的子节点（下一层，Shift 小 FanBits）里非空的那些，按 Index 升序。
	// parents 在同一层、按 Index 升序、互不重复。一次给出一层里所有要展开的节点，
	// 是为了让背后是数据库的实现能把它们合成一条语句。
	Children(ctx context.Context, parents []Node) ([]Entry, error)
}

// Options 控制 Diff 往下走到哪里为止。
type Options struct {
	// MinShift 是最细的一层：到了这一层的节点不再展开。必须能从 RootShift 整步走到。
	MinShift uint
	// MaxRows 不为零时，两边行数都不超过它的节点不再展开（交给调用方逐行比）。
	MaxRows uint64
}

// Difference 是一个两边摘要不同、没有再往下展开的节点。一边没有这个节点时那一边是零值。
type Difference struct {
	Node Node
	A, B Digest
}

// LevelStats 是 Diff 在一层上做的事。
type LevelStats struct {
	// Shift 是被比较的节点所在的层。
	Shift uint
	// Expanded 是为了得到这一层而展开的上一层节点数。
	Expanded int
	// Compared 是这一层两边合起来出现过的节点数（也就是传回来再比的摘要对数）。
	Compared int
	// Differing 是其中摘要不同的。
	Differing int
}

// Stats 是一次 Diff 的统计，从上到下每层一项。
type Stats struct {
	Levels []LevelStats
}

// Compared 返回各层比较过的节点总数。
func (s Stats) Compared() int {
	n := 0
	for _, l := range s.Levels {
		n += l.Compared
	}
	return n
}

// Diff 从根往下比较 a 和 b，只展开摘要不同的节点，返回按 Index 升序的、不再展开的差异节点。
//
// 一个摘要不同的节点在两种情况下不再展开：到了 opts.MinShift 这一层；或者 opts.MaxRows 不为零
// 并且两边的行数都不超过它。所以返回的节点不一定在同一层，但互不包含，按键的顺序排列。
//
// 两边相同时只比较根的子节点这一层。
func Diff(ctx context.Context, a, b Source, opts Options) ([]Difference, Stats, error) {
	if opts.MinShift >= RootShift || (RootShift-opts.MinShift)%FanBits != 0 {
		return nil, Stats{}, fmt.Errorf("merkle: min shift %d must be below %d and differ from it by a multiple of %d", opts.MinShift, RootShift, FanBits)
	}
	var (
		out   []Difference
		stats Stats
	)
	frontier := []Node{Root}
	for len(frontier) > 0 {
		shift := frontier[0].Shift - FanBits
		ea, err := a.Children(ctx, frontier)
		if err != nil {
			return nil, stats, fmt.Errorf("merkle: left side at shift %d: %w", shift, err)
		}
		eb, err := b.Children(ctx, frontier)
		if err != nil {
			return nil, stats, fmt.Errorf("merkle: right side at shift %d: %w", shift, err)
		}
		if err := checkEntries(ea, frontier, shift); err != nil {
			return nil, stats, fmt.Errorf("merkle: left side: %w", err)
		}
		if err := checkEntries(eb, frontier, shift); err != nil {
			return nil, stats, fmt.Errorf("merkle: right side: %w", err)
		}

		ls := LevelStats{Shift: shift, Expanded: len(frontier)}
		var next []Node
		// 两边都按 Index 升序，归并。
		for i, j := 0, 0; i < len(ea) || j < len(eb); {
			var d Difference
			switch {
			case j == len(eb) || (i < len(ea) && ea[i].Node.Index < eb[j].Node.Index):
				d = Difference{Node: ea[i].Node, A: ea[i].Digest}
				i++
			case i == len(ea) || eb[j].Node.Index < ea[i].Node.Index:
				d = Difference{Node: eb[j].Node, B: eb[j].Digest}
				j++
			default:
				d = Difference{Node: ea[i].Node, A: ea[i].Digest, B: eb[j].Digest}
				i++
				j++
			}
			ls.Compared++
			if d.A == d.B {
				continue
			}
			ls.Differing++
			small := opts.MaxRows > 0 && d.A.Count <= opts.MaxRows && d.B.Count <= opts.MaxRows
			if shift <= opts.MinShift || small {
				out = append(out, d)
			} else {
				next = append(next, d.Node)
			}
		}
		stats.Levels = append(stats.Levels, ls)
		frontier = next
	}
	// 各层的差异节点是分层追加的，按键的顺序重排。互不包含，所以按区间下界排就是键的顺序。
	sortByKey(out)
	return out, stats, nil
}

func sortByKey(ds []Difference) {
	slices.SortFunc(ds, func(x, y Difference) int {
		lx, _ := x.Node.Range()
		ly, _ := y.Node.Range()
		return cmp.Compare(lx, ly)
	})
}

// checkEntries 核对一个 Source 的回答：都在 shift 这一层、严格升序、每个都属于某个被展开的节点、摘要非空。
// Source 的缺陷（多给、少排序）不核对的话只会表现成错误的对账结果。
func checkEntries(es []Entry, parents []Node, shift uint) error {
	p := 0
	for i, e := range es {
		if e.Node.Shift != shift {
			return fmt.Errorf("entry %v is not at shift %d", e.Node, shift)
		}
		if e.Digest.IsZero() {
			return fmt.Errorf("entry %v has an empty digest", e.Node)
		}
		if i > 0 && es[i-1].Node.Index >= e.Node.Index {
			return fmt.Errorf("entries %v and %v are not in ascending order", es[i-1].Node, e.Node)
		}
		for p < len(parents) && !parents[p].Contains(e.Node) {
			p++
		}
		if p == len(parents) {
			return fmt.Errorf("entry %v is not under any expanded node", e.Node)
		}
	}
	return nil
}
