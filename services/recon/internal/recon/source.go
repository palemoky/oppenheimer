package recon

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/palemoky/oppenheimer/pkg/merkle"
)

const (
	// TopShift 是第一遍聚合的层：一个节点约 18.6 小时，7 年约 3000 个节点（设计文档 7.5）。
	// 这一层以上的节点由内存里的树回答，这一层以下的向数据库要。
	TopShift = 48
	// segmentShift 是第一遍按订单号分段的粒度：一段约 198 天，一段一条语句，
	// 不在一个库上开一个扫全表的长查询。
	segmentShift = 56
	// maxListArgs 是一条语句里 IN 列表或 OR 区间的上限。MySQL 的预处理语句最多 65535 个占位符，
	// 这里远低于它；超过时拆成几条语句，对没有订单号索引的表意味着多扫几遍。
	maxListArgs = 1000
)

// sideStats 是一侧发出的语句和收到的数据量。并发安全。
type sideStats struct {
	statements atomic.Int64 // 发出的语句数
	digests    atomic.Int64 // 收到的摘要行数
	rows       atomic.Int64 // 逐行比、复查、全量模式读回的行数
	bytes      atomic.Int64 // 这些行的规范串的字节数
}

// sideSource 是一侧数据：背后是若干张表的聚合查询，实现 merkle.Source。
//
// 第一遍（scan）在 TopShift 上把整侧聚合成一棵内存里的树；之后 Children 对 TopShift 以上的节点
// 从树里回答，对 TopShift 及以下的节点发聚合语句。
type sideSource struct {
	side  *Side
	bound int64
	// since 是窗口的下界（含），0 表示没有。和 bound 一样，这一侧的每条语句都带。
	since int64
	lim   *Limiter
	st    *sideStats
	tree  *merkle.Tree

	// preloaded 只在全量模式下有：没有订单号索引的表（按 Tables 的下标）一次读出来的全部行，按订单号升序。
	preloaded map[int][]row
}

func newSideSource(s *Side, since, bound int64, lim *Limiter) *sideSource {
	return &sideSource{side: s, since: since, bound: bound, lim: lim, st: &sideStats{}}
}

// query 在 t 的库上执行一条语句，受 Limiter 约束，逐行调用 scan。
func (s *sideSource) query(ctx context.Context, t Table, q string, args []any, scan func(*sql.Rows) error) error {
	release, err := s.lim.acquire(ctx, t.DB)
	if err != nil {
		return err
	}
	defer release()
	s.st.statements.Add(1)
	rows, err := t.DB.Conn.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("recon: query %s: %w", t.Label, err)
	}
	defer rows.Close() //nolint:errcheck // 只读；错误由 rows.Err 报告
	for rows.Next() {
		if err := scan(rows); err != nil {
			return fmt.Errorf("recon: scan %s: %w", t.Label, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("recon: read %s: %w", t.Label, err)
	}
	return nil
}

// digests 对 t 发一条聚合语句，返回 shift 这一层的节点摘要。
func (s *sideSource) digests(ctx context.Context, t Table, shift uint, c *cond) ([]merkle.Entry, error) {
	q := digestSQL(s.side, t, c) //nolint:gosec // 表名和表达式来自 pairs.go 的常量，值走占位符
	var out []merkle.Entry
	err := s.query(ctx, t, q, append([]any{shift}, c.args...), func(r *sql.Rows) error {
		var e merkle.Entry
		if err := r.Scan(&e.Node.Index, &e.Digest.Count, &e.Digest.Hi, &e.Digest.Lo); err != nil {
			return err
		}
		e.Node.Shift = shift
		out = append(out, e)
		return nil
	})
	s.st.digests.Add(int64(len(out)))
	return out, err
}

// edge 返回 t 里水位线以下最小（desc 为 false）或最大的订单号，没有行时 ok 为 false。
func (s *sideSource) edge(ctx context.Context, t Table, desc bool) (k int64, ok bool, err error) {
	// 带上窗口：有下界时分段从下界开始，不会为窗口之前的几年数据各发一条读不到行的语句。
	c := &cond{}
	if s.since > 0 {
		c.add(s.side.Key+" >= ?", s.since)
	}
	if s.bound > 0 {
		c.add(s.side.Key+" < ?", s.bound)
	}
	q := edgeSQL(s.side, t, c, desc) //nolint:gosec // 同 digests
	err = s.query(ctx, t, q, c.args, func(r *sql.Rows) error {
		ok = true
		return r.Scan(&k)
	})
	return k, ok, err
}

// span 返回 t 里水位线以下的订单号的范围。
func (s *sideSource) span(ctx context.Context, t Table) (keyRange, bool, error) {
	lo, ok, err := s.edge(ctx, t, false)
	if err != nil || !ok {
		return keyRange{}, false, err
	}
	hi, ok, err := s.edge(ctx, t, true)
	if err != nil || !ok {
		return keyRange{}, false, err
	}
	return keyRange{lo: keyOf(lo), hi: keyOf(hi)}, true, nil
}

// segments 把 r 按 shift 这一层的节点切开。
func segments(r keyRange, shift uint) []keyRange {
	var out []keyRange
	for i := r.lo >> shift; ; i++ {
		seg := nodeRange(merkle.Node{Shift: shift, Index: i})
		out = append(out, keyRange{lo: max(seg.lo, r.lo), hi: min(seg.hi, r.hi)})
		if seg.hi >= r.hi {
			return out
		}
	}
}

// scan 是第一遍：把整侧在 TopShift 上聚合成一棵树。各表、各段并发，每个库上受 Limiter 约束。
func (s *sideSource) scan(ctx context.Context) error {
	tree, err := merkle.NewTree(TopShift)
	if err != nil {
		return err
	}
	var mu sync.Mutex
	add := func(es []merkle.Entry) {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range es {
			tree.AddLeaf(e.Node.Index, e.Digest)
		}
	}
	g, gctx := newGroup(ctx)
	for _, t := range s.side.Tables {
		g.Go(func() error {
			if !t.RangeIndexed {
				// 按订单号分段也是全表扫描，只能一条语句。
				es, err := s.digests(gctx, t, TopShift, baseCond(s.side, t, s.since, s.bound))
				add(es)
				return err
			}
			r, ok, err := s.span(gctx, t)
			if err != nil || !ok {
				return err
			}
			sg, sctx := newGroup(gctx)
			for _, seg := range segments(r, segmentShift) {
				sg.Go(func() error {
					c := baseCond(s.side, t, s.since, s.bound)
					if !addRange(c, s.side.Key, seg) {
						return nil
					}
					es, err := s.digests(sctx, t, TopShift, c)
					add(es)
					return err
				})
			}
			return sg.Wait()
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("recon: first pass of %s: %w", s.side.Name, err)
	}
	s.tree = tree
	return nil
}

// Children 实现 merkle.Source。
func (s *sideSource) Children(ctx context.Context, parents []merkle.Node) ([]merkle.Entry, error) {
	if len(parents) == 0 {
		return nil, nil
	}
	shift := parents[0].Shift
	if shift > TopShift {
		if s.tree == nil {
			return nil, errors.New("recon: children asked before the first pass")
		}
		return s.tree.Children(ctx, parents)
	}
	if shift < merkle.FanBits {
		return nil, fmt.Errorf("recon: children of %v: %w", parents[0], merkle.ErrTooDeep)
	}
	child := shift - merkle.FanBits

	var (
		mu  sync.Mutex
		sum = map[uint64]merkle.Digest{}
	)
	add := func(es []merkle.Entry) {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range es {
			sum[e.Node.Index] = sum[e.Node.Index].Add(e.Digest)
		}
	}
	var ranges []keyRange
	for _, p := range parents {
		ranges = append(ranges, nodeRange(p))
	}
	ranges = mergeRanges(ranges)

	g, gctx := newGroup(ctx)
	for _, t := range s.side.Tables {
		if t.RangeIndexed {
			// 一段连续的父节点一条范围语句。
			for _, r := range ranges {
				g.Go(func() error {
					c := baseCond(s.side, t, s.since, s.bound)
					if !addRange(c, s.side.Key, r) {
						return nil
					}
					es, err := s.digests(gctx, t, child, c)
					add(es)
					return err
				})
			}
			continue
		}
		// 按订单号取数是全表扫描：这一层所有的父节点合成一条语句，一层一次全表扫描。
		for chunk := range slices.Chunk(parents, maxListArgs) {
			g.Go(func() error {
				c := baseCond(s.side, t, s.since, s.bound)
				args := []any{shift}
				for _, p := range chunk {
					args = append(args, p.Index)
				}
				c.add("("+s.side.Key+" >> ?) IN ("+placeholders(len(chunk))+")", args...)
				es, err := s.digests(gctx, t, child, c)
				add(es)
				return err
			})
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	out := make([]merkle.Entry, 0, len(sum))
	for i, d := range sum {
		if !d.IsZero() {
			out = append(out, merkle.Entry{Node: merkle.Node{Shift: child, Index: i}, Digest: d})
		}
	}
	slices.SortFunc(out, func(a, b merkle.Entry) int { return cmp.Compare(a.Node.Index, b.Node.Index) })
	return out, nil
}
