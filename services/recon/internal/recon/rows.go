package recon

import (
	"cmp"
	"context"
	"database/sql"
	"slices"
	"strings"
	"sync"
)

// row 是逐行比读回的一行：订单号、ID 的规范串、要比的列的规范串，和它来自哪张表。
type row struct {
	key   int64
	id    string
	val   string
	table string
}

func (s *sideSource) countRows(rs []row) {
	n := 0
	for _, r := range rs {
		n += len(r.id) + len(r.val)
	}
	s.st.rows.Add(int64(len(rs)))
	s.st.bytes.Add(int64(n))
}

// readRows 对 t 发一条读行的语句。
func (s *sideSource) readRows(ctx context.Context, t Table, c *cond) ([]row, error) {
	q := rowsSQL(s.side, t, c) //nolint:gosec // 表名和表达式来自 pairs.go 的常量，值走占位符
	var out []row
	err := s.query(ctx, t, q, c.args, func(r *sql.Rows) error {
		x := row{table: t.Label}
		if err := r.Scan(&x.key, &x.id, &x.val); err != nil {
			return err
		}
		out = append(out, x)
		return nil
	})
	s.countRows(out)
	return out, err
}

// collector 并发地收集各条语句读回的行。
type collector struct {
	mu   sync.Mutex
	rows []row
}

func (c *collector) add(rs []row) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = append(c.rows, rs...)
}

// readRanges 读出 ranges（按下界升序、互不相交）里的全部行。有订单号索引的表一个区间一条语句
// （merge 为 true 时先把首尾相接的区间合并）；没有的把全部区间用 OR 合成一条语句。
// 全量模式下预先读好的表从内存里取。
func (s *sideSource) readRanges(ctx context.Context, ranges []keyRange, merge bool) ([]row, error) {
	if merge {
		ranges = mergeRanges(ranges)
	}
	var col collector
	g, gctx := newGroup(ctx)
	for ti, t := range s.side.Tables {
		if pre, ok := s.preloaded[ti]; ok {
			col.add(rowsIn(pre, ranges))
			continue
		}
		if t.RangeIndexed {
			for _, r := range ranges {
				g.Go(func() error {
					c := baseCond(s.side, t, s.since, s.bound)
					if !addRange(c, s.side.Key, r) {
						return nil
					}
					rs, err := s.readRows(gctx, t, c)
					col.add(rs)
					return err
				})
			}
			continue
		}
		for chunk := range slices.Chunk(ranges, maxListArgs) {
			g.Go(func() error {
				c := baseCond(s.side, t, s.since, s.bound)
				if !addRanges(c, s.side.Key, chunk) {
					return nil
				}
				rs, err := s.readRows(gctx, t, c)
				col.add(rs)
				return err
			})
		}
	}
	err := g.Wait()
	return col.rows, err
}

// rowsIn 从按订单号升序的 rows 里取出落在 ranges 里的。
func rowsIn(rows []row, ranges []keyRange) []row {
	var out []row
	for _, r := range ranges {
		lo, hi, ok := r.sqlArgs()
		if !ok {
			continue
		}
		i, _ := slices.BinarySearchFunc(rows, lo, func(x row, k int64) int { return cmp.Compare(x.key, k) })
		for ; i < len(rows) && rows[i].key <= hi; i++ {
			out = append(out, rows[i])
		}
	}
	return out
}

// readKeys 按订单号重读（复查用），每批最多 maxListArgs 个。
func (s *sideSource) readKeys(ctx context.Context, keys []int64) ([]row, error) {
	var col collector
	g, gctx := newGroup(ctx)
	for _, t := range s.side.Tables {
		for chunk := range slices.Chunk(keys, maxListArgs) {
			g.Go(func() error {
				c := baseCond(s.side, t, s.since, s.bound)
				args := make([]any, len(chunk))
				for i, k := range chunk {
					args[i] = k
				}
				c.add(s.side.Key+" IN ("+placeholders(len(chunk))+")", args...)
				rs, err := s.readRows(gctx, t, c)
				col.add(rs)
				return err
			})
		}
	}
	err := g.Wait()
	return col.rows, err
}

// preload 把没有订单号索引的表各用一条语句整个读出来，按订单号排序（全量模式）。
func (s *sideSource) preload(ctx context.Context) error {
	var mu sync.Mutex
	pre := map[int][]row{}
	g, gctx := newGroup(ctx)
	for ti, t := range s.side.Tables {
		if t.RangeIndexed {
			continue
		}
		g.Go(func() error {
			rs, err := s.readRows(gctx, t, baseCond(s.side, t, s.since, s.bound))
			if err != nil {
				return err
			}
			slices.SortFunc(rs, func(a, b row) int { return cmp.Compare(a.key, b.key) })
			mu.Lock()
			pre[ti] = rs
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	s.preloaded = pre
	return nil
}

// Class 是一条差异的类别。
type Class string

// 差异的类别。
const (
	// OnlyLeft 是只在左侧有的行。
	OnlyLeft Class = "only_left"
	// OnlyRight 是只在右侧有的行。
	OnlyRight Class = "only_right"
	// Mismatch 是两侧都有、要比的列不同的行。
	Mismatch Class = "mismatch"
	// Duplicate 是在同一侧出现不止一次的行（例如同一个订单在两个 slot 里各有一份）。
	Duplicate Class = "duplicate"
)

// Classes 是全部类别，按报告的顺序。
var Classes = []Class{OnlyLeft, OnlyRight, Mismatch, Duplicate}

// Version 是一行在一侧的一份：它的列的规范串和所在的表。
type Version struct {
	Table string `json:"table"`
	Value string `json:"value"`
}

// Finding 是一个 ID 上的差异：两侧各读到的全部份（没有时为空）。
type Finding struct {
	Class Class `json:"class"`
	// Key 是订单号。
	Key int64 `json:"key"`
	// ID 是 ID 的规范串。
	ID    string    `json:"id"`
	Left  []Version `json:"left"`
	Right []Version `json:"right"`
}

// state 是一条差异的“两侧的值”，复查时比较两次读到的是否完全相同。不含表名：
// 同一个值换了张表（例如归档把它从热库搬到了冷库）不算变了。
func (f Finding) state() string { return values(f.Left) + "\x01" + values(f.Right) }

// values 返回一侧各份的值，排序后连起来。
func values(vs []Version) string {
	s := make([]string, len(vs))
	for i, v := range vs {
		s[i] = v.Value
	}
	slices.Sort(s)
	return strings.Join(s, "\x00")
}

// classify 按 ID 把两侧的行配对，返回不一致的那些，按订单号、ID 排序。
func classify(left, right []row) []Finding {
	type pair struct {
		key         int64
		left, right []Version
	}
	byID := map[string]*pair{}
	get := func(r row) *pair {
		p, ok := byID[r.id]
		if !ok {
			p = &pair{key: r.key}
			byID[r.id] = p
		}
		return p
	}
	for _, r := range left {
		p := get(r)
		p.left = append(p.left, Version{Table: r.table, Value: r.val})
	}
	for _, r := range right {
		p := get(r)
		p.right = append(p.right, Version{Table: r.table, Value: r.val})
	}
	var out []Finding
	for id, p := range byID {
		f := Finding{Key: p.key, ID: id, Left: p.left, Right: p.right}
		switch {
		case len(p.left) > 1 || len(p.right) > 1:
			// 两侧是同样的多份（主库上的重复被原样复制到了从库）时摘要相同，树模式看不到它；
			// 这里也不报，全量模式和树模式的结果才一致。重复要在“它不该重复”的那一对上发现。
			if values(p.left) == values(p.right) {
				continue
			}
			f.Class = Duplicate
		case len(p.right) == 0:
			f.Class = OnlyLeft
		case len(p.left) == 0:
			f.Class = OnlyRight
		case p.left[0].Value != p.right[0].Value:
			f.Class = Mismatch
		default:
			continue
		}
		sortVersions(f.Left)
		sortVersions(f.Right)
		out = append(out, f)
	}
	sortFindings(out)
	return out
}

func sortVersions(vs []Version) {
	slices.SortFunc(vs, func(a, b Version) int {
		return cmp.Or(cmp.Compare(a.Table, b.Table), cmp.Compare(a.Value, b.Value))
	})
}

func sortFindings(fs []Finding) {
	slices.SortFunc(fs, func(a, b Finding) int { return cmp.Or(cmp.Compare(a.Key, b.Key), cmp.Compare(a.ID, b.ID)) })
}
