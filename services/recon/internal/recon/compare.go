package recon

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/merkle"
)

// Mode 是比对的做法。
type Mode string

const (
	// ModeTree 先比摘要，只在摘要不同的区间里逐行比（设计文档 7.5）。
	ModeTree Mode = "tree"
	// ModeFull 不算摘要，把两侧全部读出来逐行比。是性能对比的基线（设计文档 7.12），结果必须和 ModeTree 相同。
	ModeFull Mode = "full"
)

// 默认值。
const (
	DefaultMaxRows     = 2000
	DefaultMinShift    = 28 // 一个节点 64 毫秒
	DefaultRecheck     = 5 * time.Second
	DefaultMaxExamples = 50
)

// Options 控制一次比对。
type Options struct {
	Mode Mode
	// Bound 是水位线：只比订单号小于它的行。0 表示不设上界。
	Bound int64
	// Since 是窗口的下界：只比订单号不小于它的行。0 表示不设下界（全量）。
	// 设了下界就是增量对账：代价和窗口里的数据量成正比，窗口之前的数据后来被改坏不会被发现，
	// 所以要配合定期的全量（设计文档 7.17）。右侧是存下来的树时不支持。
	Since int64
	// MaxRows 是两侧行数都不超过它的差异节点直接逐行比；0 取 DefaultMaxRows。
	MaxRows uint64
	// MinShift 是 Diff 最细的一层；0 取 DefaultMinShift。
	MinShift uint
	// Recheck 是逐行比之后、复查之前等多久。0 表示不复查，第一次的结果直接算确认。
	Recheck time.Duration
	// MaxExamples 是报告里每类差异的例子数；0 取 DefaultMaxExamples。
	MaxExamples int
	// Limiter 限制每个库上同时在跑的语句数；nil 时用默认的上限新建一个（只在这次比对里有效）。
	Limiter *Limiter
	// Against 是右侧为 nil 的比对（archive）用的树，按 Comparison.Name 查。
	Against map[string]*merkle.Tree
	// Repairer 不为 nil 并且比对是 Repairable 的时候，修复确认的差异。
	Repairer *Repairer
	Logger   *slog.Logger

	// BeforeRecheck、BeforeRepair 是测试钩子：在逐行比之后、复查之前，和修复之前执行。
	BeforeRecheck func(ctx context.Context)
	BeforeRepair  func(ctx context.Context)
}

func (o Options) withDefaults() Options {
	if o.Mode == "" {
		o.Mode = ModeTree
	}
	if o.MaxRows == 0 {
		o.MaxRows = DefaultMaxRows
	}
	if o.MinShift == 0 {
		o.MinShift = DefaultMinShift
	}
	if o.MaxExamples <= 0 {
		o.MaxExamples = DefaultMaxExamples
	}
	if o.Limiter == nil {
		o.Limiter = NewLimiter(0)
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// Compare 执行一次比对。
func Compare(ctx context.Context, c Comparison, o Options) (*ComparisonReport, error) {
	o = o.withDefaults()
	start := time.Now()
	rep := &ComparisonReport{Name: c.Name, Mode: o.Mode, Bound: o.Bound, Since: o.Since, Counts: map[Class]int{}}
	if o.Since > 0 {
		rep.SinceTime = idgen.Time(o.Since)
	}
	if o.Since > 0 && o.Bound > 0 && o.Since >= o.Bound {
		return nil, fmt.Errorf("recon: window is empty: since %d is not below bound %d", o.Since, o.Bound)
	}
	if o.Bound > 0 {
		rep.BoundTime = idgen.Time(o.Bound)
	}
	log := o.Logger.With("comparison", c.Name, "mode", string(o.Mode))

	left := newSideSource(&c.Left, o.Since, o.Bound, o.Limiter)
	var right *sideSource
	if c.Right != nil {
		right = newSideSource(c.Right, o.Since, o.Bound, o.Limiter)
	}
	defer func() {
		left.report(&rep.Left)
		if right != nil {
			right.report(&rep.Right)
		}
		rep.Timings.Total = ms(time.Since(start))
	}()

	if right == nil {
		if o.Since > 0 {
			return nil, fmt.Errorf("recon: comparison %s compares against a saved tree and does not support a lower bound", c.Name)
		}
		against := o.Against[c.Name]
		if against == nil {
			return nil, fmt.Errorf("recon: comparison %s needs a saved tree to compare against", c.Name)
		}
		if o.Mode != ModeTree {
			return nil, fmt.Errorf("recon: comparison %s compares against a saved tree and supports only mode %s", c.Name, ModeTree)
		}
		return rep, compareAgainst(ctx, left, against, rep, log)
	}

	var (
		first []Finding
		err   error
	)
	switch o.Mode {
	case ModeTree:
		first, err = compareTree(ctx, left, right, o, rep, log)
	case ModeFull:
		first, err = compareFull(ctx, left, right, rep, log)
	default:
		err = fmt.Errorf("recon: unknown mode %q", o.Mode)
	}
	if err != nil {
		return nil, err
	}

	confirmed := first
	if len(first) > 0 && o.Recheck > 0 {
		if o.BeforeRecheck != nil {
			o.BeforeRecheck(ctx)
		}
		t := time.Now()
		if err := sleep(ctx, o.Recheck); err != nil {
			return nil, err
		}
		var unstable []Finding
		confirmed, unstable, rep.Dropped, err = recheck(ctx, left, right, first)
		if err != nil {
			return nil, err
		}
		rep.Unstable = len(unstable)
		rep.UnstableExamples = examples(unstable, &c.Left, o.MaxExamples)
		rep.Timings.Recheck = ms(time.Since(t))
		log.Info("recheck done", "first", len(first), "confirmed", len(confirmed), "unstable", len(unstable), "dropped", rep.Dropped)
	}
	rep.Findings = confirmed
	for _, f := range confirmed {
		rep.Counts[f.Class]++
	}
	rep.Examples = examples(confirmed, &c.Left, o.MaxExamples)

	if c.Repairable && o.Repairer != nil && len(confirmed) > 0 {
		if o.BeforeRepair != nil {
			o.BeforeRepair(ctx)
		}
		t := time.Now()
		rp, err := o.Repairer.repair(ctx, c, left, right, confirmed)
		if err != nil {
			return nil, err
		}
		rep.Repair = rp
		rep.Timings.Repair = ms(time.Since(t))
		log.Info("repair done", "orders", rp.Orders, "fixed", rp.Fixed, "remaining", rp.Remaining)
	}
	return rep, nil
}

// compareTree 是树模式的第 1 到 3 步：第一遍、Diff、逐行比。返回第一次逐行比的结果。
func compareTree(ctx context.Context, left, right *sideSource, o Options, rep *ComparisonReport, log *slog.Logger) ([]Finding, error) {
	t := time.Now()
	g, gctx := newGroup(ctx)
	g.Go(func() error { return left.scan(gctx) })
	g.Go(func() error { return right.scan(gctx) })
	if err := g.Wait(); err != nil {
		return nil, err
	}
	rep.Timings.FirstPass = ms(time.Since(t))
	lr, rr := left.tree.Root(), right.tree.Root()
	rep.Left.Rows, rep.Left.Root = lr.Count, lr.String()
	rep.Right.Rows, rep.Right.Root = rr.Count, rr.String()
	log.Info("first pass done", "left_rows", lr.Count, "right_rows", rr.Count, "ms", rep.Timings.FirstPass)
	if lr == rr {
		rep.RootsEqual = true
		return nil, nil
	}

	t = time.Now()
	diffs, stats, err := merkle.Diff(ctx, left, right, merkle.Options{MinShift: o.MinShift, MaxRows: o.MaxRows})
	if err != nil {
		return nil, fmt.Errorf("recon: diff %s: %w", rep.Name, err)
	}
	rep.Levels = stats.Levels
	rep.DiffNodes = len(diffs)
	rep.Timings.Diff = ms(time.Since(t))
	log.Info("diff done", "nodes", len(diffs), "compared", stats.Compared(), "ms", rep.Timings.Diff)

	t = time.Now()
	ranges := make([]keyRange, len(diffs))
	for i, d := range diffs {
		ranges[i] = nodeRange(d.Node)
	}
	first, err := readAndClassify(ctx, left, right, func(s *sideSource, ctx context.Context) ([]row, error) {
		return s.readRanges(ctx, ranges, true)
	})
	rep.Timings.Rows = ms(time.Since(t))
	return first, err
}

// readAndClassify 让两侧并发地各读一次，配对。
func readAndClassify(ctx context.Context, left, right *sideSource, read func(*sideSource, context.Context) ([]row, error)) ([]Finding, error) {
	var lrows, rrows []row
	g, gctx := newGroup(ctx)
	g.Go(func() (err error) { lrows, err = read(left, gctx); return err })
	g.Go(func() (err error) { rrows, err = read(right, gctx); return err })
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return classify(lrows, rrows), nil
}

// compareFull 是全量模式：没有订单号索引的表一条语句整个读进来，其余的按 segmentShift 一段一段读，
// 一段两侧读完就比，比完丢掉。
func compareFull(ctx context.Context, left, right *sideSource, rep *ComparisonReport, log *slog.Logger) ([]Finding, error) {
	t := time.Now()
	g, gctx := newGroup(ctx)
	g.Go(func() error { return left.preload(gctx) })
	g.Go(func() error { return right.preload(gctx) })
	if err := g.Wait(); err != nil {
		return nil, err
	}
	// 两侧所有表的订单号范围的并集。
	var (
		span  keyRange
		found bool
	)
	extend := func(r keyRange) {
		if !found {
			span, found = r, true
			return
		}
		span.lo, span.hi = min(span.lo, r.lo), max(span.hi, r.hi)
	}
	for _, s := range []*sideSource{left, right} {
		for ti, tb := range s.side.Tables {
			if pre, ok := s.preloaded[ti]; ok {
				if len(pre) > 0 {
					extend(keyRange{lo: keyOf(pre[0].key), hi: keyOf(pre[len(pre)-1].key)})
				}
				continue
			}
			r, ok, err := s.span(ctx, tb)
			if err != nil {
				return nil, err
			}
			if ok {
				extend(r)
			}
		}
	}
	var out []Finding
	if found {
		// 和第一遍一样按 segmentShift 分段，一段一张表一条语句。按更细的节点分段的话，
		// 数据稀的地方大部分语句读不到行，语句数比行数还多，拿它当基线不公平。
		for _, seg := range segments(span, segmentShift) {
			ranges := []keyRange{seg}
			fs, err := readAndClassify(ctx, left, right, func(s *sideSource, ctx context.Context) ([]row, error) {
				return s.readRanges(ctx, ranges, false)
			})
			if err != nil {
				return nil, err
			}
			out = append(out, fs...)
		}
	}
	rep.Left.Rows = uint64(left.st.rows.Load())   //nolint:gosec // 计数不为负
	rep.Right.Rows = uint64(right.st.rows.Load()) //nolint:gosec // 计数不为负
	rep.Timings.Rows = ms(time.Since(t))
	log.Info("full scan done", "left_rows", rep.Left.Rows, "right_rows", rep.Right.Rows, "findings", len(out), "ms", rep.Timings.Rows)
	return out, nil
}

// compareAgainst 是右侧为存下来的树的比对：左侧第一遍之后，两棵树比到 TopShift 为止，差异节点进报告。
func compareAgainst(ctx context.Context, left *sideSource, against *merkle.Tree, rep *ComparisonReport, log *slog.Logger) error {
	rep.NodeOnly = true
	if against.LeafShift() != TopShift {
		return fmt.Errorf("recon: saved tree for %s has leaf shift %d, want %d", rep.Name, against.LeafShift(), TopShift)
	}
	t := time.Now()
	if err := left.scan(ctx); err != nil {
		return err
	}
	rep.Timings.FirstPass = ms(time.Since(t))
	lr, rr := left.tree.Root(), against.Root()
	rep.Left.Rows, rep.Left.Root = lr.Count, lr.String()
	rep.Right = SideReport{Name: "saved tree", Rows: rr.Count, Root: rr.String()}
	if lr == rr {
		rep.RootsEqual = true
		return nil
	}
	t = time.Now()
	diffs, stats, err := merkle.Diff(ctx, left.tree, against, merkle.Options{MinShift: TopShift})
	if err != nil {
		return fmt.Errorf("recon: diff %s: %w", rep.Name, err)
	}
	rep.Levels = stats.Levels
	rep.DiffNodes = len(diffs)
	for _, d := range diffs {
		rep.Nodes = append(rep.Nodes, nodeDiff(d))
	}
	rep.Timings.Diff = ms(time.Since(t))
	log.Info("diff against saved tree done", "nodes", len(diffs))
	return nil
}

// recheck 把 first 涉及的订单号在两侧重读、重新配对。两次的 (左值, 右值) 完全相同的是确认的差异；
// 重读时已经一致的丢掉；仍然不同但值变了的是 unstable。
func recheck(ctx context.Context, left, right *sideSource, first []Finding) (confirmed, unstable []Finding, dropped int, err error) {
	keys := findingKeys(first)
	second, err := readAndClassify(ctx, left, right, func(s *sideSource, ctx context.Context) ([]row, error) {
		return s.readKeys(ctx, keys)
	})
	if err != nil {
		return nil, nil, 0, err
	}
	now := make(map[string]Finding, len(second))
	for _, f := range second {
		now[f.ID] = f
	}
	for _, f := range first {
		s, ok := now[f.ID]
		switch {
		case !ok:
			dropped++
		case s.state() == f.state():
			confirmed = append(confirmed, s)
		default:
			unstable = append(unstable, s)
		}
	}
	return confirmed, unstable, dropped, nil
}

// findingKeys 返回差异涉及的订单号，升序、去重。
func findingKeys(fs []Finding) []int64 {
	keys := make([]int64, 0, len(fs))
	for _, f := range fs {
		keys = append(keys, f.Key)
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Snapshot 对右侧为 nil 的比对（archive）的左侧做第一遍，返回在 TopShift 上的树，用于之后的 Compare。
func Snapshot(ctx context.Context, c Comparison, bound int64, lim *Limiter) (*merkle.Tree, SideReport, error) {
	if lim == nil {
		lim = NewLimiter(0)
	}
	s := newSideSource(&c.Left, 0, bound, lim)
	var rep SideReport
	if err := s.scan(ctx); err != nil {
		return nil, rep, err
	}
	s.report(&rep)
	root := s.tree.Root()
	rep.Rows, rep.Root = root.Count, root.String()
	return s.tree, rep, nil
}
