package pprof

import (
	"cmp"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/google/pprof/profile"
)

// 给模型的是摘要（设计 4a 的 3.4）：
//   - cpu、heap、mutex、block：按函数聚合，前 topFuncs 个的 flat、cum 和占比，再加占比最大的 topStacks 条完整调用栈。
//   - goroutine：按调用栈聚合，数量最多的前 topGoroutineStacks 组，每组的数量、推断的等待状态和最里面几帧。
//   - 带 base 时给增长最多的那几项。
const (
	topFuncs           = 20
	topStacks          = 3
	topGoroutineStacks = 15
	// maxStackFrames 是“完整调用栈”最多给几帧（最里面的那些），再深的写明省掉了几帧。
	maxStackFrames = 30
	// goroutineFrames 是每组 goroutine 给的帧数（去掉最里面的运行时帧之后）。
	goroutineFrames = 8
)

// frame 是调用栈里的一帧。内联的函数展开成单独的帧。
type frame struct {
	fn   string
	file string
	line int64
}

func (f frame) String() string {
	if f.file == "" || f.file == "?" || f.line <= 0 {
		return f.fn
	}
	return fmt.Sprintf("%s (%s:%d)", f.fn, path.Base(f.file), f.line)
}

// frames 返回一个样本的调用栈，最里面的在前。
func frames(s *profile.Sample) []frame {
	out := make([]frame, 0, len(s.Location))
	for _, loc := range s.Location {
		if len(loc.Line) == 0 {
			out = append(out, frame{fn: fmt.Sprintf("0x%x", loc.Address)})
			continue
		}
		// Line 里最前面的是被内联得最深的那个函数，最后一个是它们被内联进去的调用方。
		for _, ln := range loc.Line {
			f := frame{fn: "?", line: ln.Line}
			if ln.Function != nil {
				f.fn, f.file = ln.Function.Name, ln.Function.Filename
			}
			out = append(out, f)
		}
	}
	return out
}

// stack 是按函数名聚合的一条调用栈。frames 取第一次见到的样本（同一个函数里不同行的样本合在一起）。
type stack struct {
	frames []frame
	value  int64
}

// stats 是一个剖析按函数、按调用栈聚合的结果。
type stats struct {
	total  int64
	flat   map[string]int64
	cum    map[string]int64
	stacks map[string]*stack
}

func stackKey(fs []frame) string {
	var b strings.Builder
	for _, f := range fs {
		b.WriteString(f.fn)
		b.WriteByte('\n')
	}
	return b.String()
}

// aggregate 按 value 的第 idx 项聚合。scale 乘在每个值上（对比 CPU 剖析时按时长换算 base 用）。
func aggregate(p *profile.Profile, idx int, scale float64) *stats {
	st := &stats{flat: map[string]int64{}, cum: map[string]int64{}, stacks: map[string]*stack{}}
	seen := map[string]bool{}
	for _, s := range p.Sample {
		if idx >= len(s.Value) {
			continue
		}
		v := s.Value[idx]
		if scale != 1 {
			v = int64(float64(v) * scale)
		}
		if v == 0 {
			continue
		}
		fs := frames(s)
		st.total += v
		if len(fs) == 0 {
			continue
		}
		st.flat[fs[0].fn] += v
		clear(seen)
		for _, f := range fs {
			if !seen[f.fn] { // 递归的函数只算一次 cum
				seen[f.fn] = true
				st.cum[f.fn] += v
			}
		}
		key := stackKey(fs)
		if sk, ok := st.stacks[key]; ok {
			sk.value += v
		} else {
			st.stacks[key] = &stack{frames: fs, value: v}
		}
	}
	return st
}

// sampleIndex 找名字是 name 的样本类型。
func sampleIndex(p *profile.Profile, name string) (int, error) {
	for i, st := range p.SampleType {
		if st.Type == name {
			return i, nil
		}
	}
	names := make([]string, 0, len(p.SampleType))
	for _, st := range p.SampleType {
		names = append(names, st.Type)
	}
	return 0, fmt.Errorf("profile has no sample type %q (it has %s)", name, strings.Join(names, ", "))
}

// unitOf 返回样本类型的单位。
func unitOf(p *profile.Profile, idx int) string {
	return p.SampleType[idx].Unit
}

// format 按单位写一个值：时间、字节、个数。
func format(v int64, unit string) string {
	neg := v < 0
	a := v
	if neg {
		a = -v
	}
	var s string
	switch unit {
	case "nanoseconds":
		switch {
		case a >= 1e9:
			s = fmt.Sprintf("%.2fs", float64(a)/1e9)
		case a >= 1e6:
			s = fmt.Sprintf("%.1fms", float64(a)/1e6)
		case a >= 1e3:
			s = fmt.Sprintf("%.1fµs", float64(a)/1e3)
		default:
			s = fmt.Sprintf("%dns", a)
		}
	case "bytes":
		switch {
		case a >= 1<<30:
			s = fmt.Sprintf("%.2fGiB", float64(a)/(1<<30))
		case a >= 1<<20:
			s = fmt.Sprintf("%.1fMiB", float64(a)/(1<<20))
		case a >= 1<<10:
			s = fmt.Sprintf("%.1fKiB", float64(a)/(1<<10))
		default:
			s = fmt.Sprintf("%dB", a)
		}
	default:
		s = strconv.FormatInt(a, 10)
	}
	if neg {
		return "-" + s
	}
	return s
}

func signed(v int64, unit string) string {
	if v > 0 {
		return "+" + format(v, unit)
	}
	return format(v, unit)
}

func pct(v, total int64) string {
	if total == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", float64(v)*100/float64(total))
}

// ranked 按值从大到小排 m 的键，值相同按名字排（输出稳定）。只留值大于 0 的。
func ranked(m map[string]int64) []string {
	keys := slices.Collect(maps.Keys(m))
	keys = slices.DeleteFunc(keys, func(k string) bool { return m[k] <= 0 })
	slices.SortFunc(keys, func(a, b string) int {
		return cmp.Or(cmp.Compare(m[b], m[a]), cmp.Compare(a, b))
	})
	return keys
}

func writeStack(b *strings.Builder, fs []frame, limit int) {
	for i, f := range fs {
		if i == limit {
			fmt.Fprintf(b, "     ... %d more outer frames\n", len(fs)-limit)
			break
		}
		fmt.Fprintf(b, "     %s\n", f)
	}
}

// writeFlat 写单个剖析的按函数聚合和最大的几条调用栈。
func writeFlat(b *strings.Builder, st *stats, unit, sample string) {
	funcs := ranked(st.flat)
	fmt.Fprintf(b, "\nTop %d functions by flat (self) %s, of %d functions with samples:\n", min(topFuncs, len(funcs)), sample, len(st.cum))
	fmt.Fprintf(b, "  %10s %6s %10s %6s  %s\n", "flat", "flat%", "cum", "cum%", "function")
	for _, fn := range funcs[:min(topFuncs, len(funcs))] {
		fmt.Fprintf(b, "  %10s %6s %10s %6s  %s\n",
			format(st.flat[fn], unit), pct(st.flat[fn], st.total), format(st.cum[fn], unit), pct(st.cum[fn], st.total), fn)
	}
	stacks := slices.Collect(maps.Values(st.stacks))
	slices.SortFunc(stacks, func(a, b *stack) int {
		return cmp.Or(cmp.Compare(b.value, a.value), cmp.Compare(stackKey(a.frames), stackKey(b.frames)))
	})
	fmt.Fprintf(b, "\nTop %d call stacks (innermost frame first):\n", min(topStacks, len(stacks)))
	for i, s := range stacks[:min(topStacks, len(stacks))] {
		fmt.Fprintf(b, "  #%d %s (%s)\n", i+1, format(s.value, unit), pct(s.value, st.total))
		writeStack(b, s.frames, maxStackFrames)
	}
}

// writeDiff 写两次剖析之间增长最多的函数和调用栈。cur、base 是同一种样本类型。
func writeDiff(b *strings.Builder, cur, base *stats, unit, sample string) {
	fmt.Fprintf(b, "Total %s: %s -> %s (%s).\n", sample, format(base.total, unit), format(cur.total, unit), signed(cur.total-base.total, unit))

	delta := map[string]int64{}
	for fn, v := range cur.flat {
		delta[fn] += v
	}
	for fn, v := range base.flat {
		delta[fn] -= v
	}
	grew := ranked(delta)
	shrank, shrankBy := 0, int64(0)
	for _, v := range delta {
		if v < 0 {
			shrank++
			shrankBy += v
		}
	}
	if len(grew) == 0 {
		b.WriteString("\nNo function grew between the two captures.\n")
	} else {
		fmt.Fprintf(b, "\nTop %d functions by growth of flat (self) %s, of %d that grew:\n", min(topFuncs, len(grew)), sample, len(grew))
		fmt.Fprintf(b, "  %10s %10s %10s  %s\n", "growth", "before", "after", "function")
		for _, fn := range grew[:min(topFuncs, len(grew))] {
			fmt.Fprintf(b, "  %10s %10s %10s  %s\n", signed(delta[fn], unit), format(base.flat[fn], unit), format(cur.flat[fn], unit), fn)
		}
	}
	if shrank > 0 {
		fmt.Fprintf(b, "(%d functions shrank, %s in total; not listed.)\n", shrank, format(shrankBy, unit))
	}

	type grown struct {
		frames        []frame
		before, after int64
	}
	var stacks []grown
	for key, s := range cur.stacks {
		var before int64
		if bs, ok := base.stacks[key]; ok {
			before = bs.value
		}
		if s.value > before {
			stacks = append(stacks, grown{s.frames, before, s.value})
		}
	}
	slices.SortFunc(stacks, func(a, b grown) int {
		return cmp.Or(cmp.Compare(b.after-b.before, a.after-a.before), cmp.Compare(stackKey(a.frames), stackKey(b.frames)))
	})
	if len(stacks) > 0 {
		fmt.Fprintf(b, "\nTop %d call stacks by growth (innermost frame first):\n", min(topStacks, len(stacks)))
		for i, s := range stacks[:min(topStacks, len(stacks))] {
			fmt.Fprintf(b, "  #%d %s (%s -> %s)\n", i+1, signed(s.after-s.before, unit), format(s.before, unit), format(s.after, unit))
			writeStack(b, s.frames, maxStackFrames)
		}
	}
}

// waitStates 按调用栈里的运行时帧推断 goroutine 在等什么。goroutine 剖析（proto 格式）不带等待状态和时长，
// 只能从帧推断。看的是最里面那一串运行时帧加上紧挨着的第一个非运行时帧（time.Sleep、syscall.*），
// 按这张表的顺序找，第一个有帧匹配的算数：顺序有意义（gopark 几乎总在，放最后；RWMutexR 在 RWMutex 前面）。
var waitStates = []struct{ prefix, state string }{
	{"runtime.chanrecv", "chan receive"},
	{"runtime.chansend", "chan send"},
	{"runtime.selectgo", "select"},
	{"runtime.block", "select with no cases (blocked forever)"},
	{"internal/poll.runtime_pollWait", "IO wait"},
	{"runtime.netpollblock", "IO wait"},
	{"sync.runtime_SemacquireRWMutexR", "sync.RWMutex RLock"},
	{"internal/sync.runtime_SemacquireRWMutexR", "sync.RWMutex RLock"},
	{"sync.runtime_SemacquireRWMutex", "sync.RWMutex Lock"},
	{"internal/sync.runtime_SemacquireRWMutex", "sync.RWMutex Lock"},
	{"sync.runtime_SemacquireMutex", "sync.Mutex Lock"},
	{"internal/sync.runtime_SemacquireMutex", "sync.Mutex Lock"},
	{"sync.runtime_SemacquireWaitGroup", "sync.WaitGroup Wait"},
	{"sync.runtime_Semacquire", "semaphore acquire"},
	{"sync.runtime_notifyListWait", "sync.Cond Wait"},
	{"time.Sleep", "sleep"},
	{"runtime.cgocall", "cgo call"},
	{"syscall.", "syscall"},
	{"internal/runtime/syscall.", "syscall"},
	{"runtime.gopark", "parked (other)"},
}

func waitState(fs []frame) string {
	n := 0
	for n < len(fs) && runtimeFrame(fs[n].fn) {
		n++
	}
	inner := fs[:min(n+1, len(fs))]
	for _, w := range waitStates {
		for _, f := range inner {
			if strings.HasPrefix(f.fn, w.prefix) {
				return w.state
			}
		}
	}
	return "running or runnable"
}

// runtimeFrame 是 goroutine 摘要里从最里面去掉的帧：运行时内部，状态里已经说了它们在干什么。
func runtimeFrame(fn string) bool {
	return strings.HasPrefix(fn, "runtime.") || strings.HasPrefix(fn, "internal/") || strings.HasPrefix(fn, "sync.runtime_")
}

// visibleFrames 去掉最里面的运行时帧（全是运行时帧时不去），最多留 n 帧，返回留下的和省掉的外层帧数。
func visibleFrames(fs []frame, n int) ([]frame, int) {
	i := 0
	for i < len(fs) && runtimeFrame(fs[i].fn) {
		i++
	}
	if i == len(fs) {
		i = 0
	}
	fs = fs[i:]
	if len(fs) > n {
		return fs[:n], len(fs) - n
	}
	return fs, 0
}

func writeGoroutineGroup(b *strings.Builder, fs []frame) {
	shown, more := visibleFrames(fs, goroutineFrames)
	for _, f := range shown {
		fmt.Fprintf(b, "     %s\n", f)
	}
	if more > 0 {
		fmt.Fprintf(b, "     ... %d more outer frames\n", more)
	}
}

// writeGoroutines 写 goroutine 剖析的按调用栈聚合。
func writeGoroutines(b *strings.Builder, st *stats) {
	stacks := slices.Collect(maps.Values(st.stacks))
	slices.SortFunc(stacks, func(a, b *stack) int {
		return cmp.Or(cmp.Compare(b.value, a.value), cmp.Compare(stackKey(a.frames), stackKey(b.frames)))
	})
	fmt.Fprintf(b, "Total %d goroutines in %d distinct call stacks.\n", st.total, len(stacks))
	fmt.Fprintf(b, "\nTop %d stacks by goroutine count. The state is inferred from the runtime frames; this profile does not record how long a goroutine has been waiting. Frames are innermost first, runtime internals dropped.\n", min(topGoroutineStacks, len(stacks)))
	for i, s := range stacks[:min(topGoroutineStacks, len(stacks))] {
		fmt.Fprintf(b, "  #%d %d goroutines (%s), state: %s\n", i+1, s.value, pct(s.value, st.total), waitState(s.frames))
		writeGoroutineGroup(b, s.frames)
	}
	if rest := len(stacks) - topGoroutineStacks; rest > 0 {
		var n int64
		for _, s := range stacks[topGoroutineStacks:] {
			n += s.value
		}
		fmt.Fprintf(b, "(%d more stacks with %d goroutines not listed.)\n", rest, n)
	}
}

// writeGoroutineDiff 写两次 goroutine 剖析之间增长最多的调用栈。
func writeGoroutineDiff(b *strings.Builder, cur, base *stats) {
	fmt.Fprintf(b, "Total goroutines: %d -> %d (%+d).\n", base.total, cur.total, cur.total-base.total)
	type grown struct {
		frames        []frame
		before, after int64
	}
	var stacks []grown
	var shrank int64
	for key, s := range cur.stacks {
		var before int64
		if bs, ok := base.stacks[key]; ok {
			before = bs.value
		}
		if s.value > before {
			stacks = append(stacks, grown{s.frames, before, s.value})
		}
	}
	for key, bs := range base.stacks {
		var after int64
		if s, ok := cur.stacks[key]; ok {
			after = s.value
		}
		if after < bs.value {
			shrank += bs.value - after
		}
	}
	slices.SortFunc(stacks, func(a, b grown) int {
		return cmp.Or(cmp.Compare(b.after-b.before, a.after-a.before), cmp.Compare(stackKey(a.frames), stackKey(b.frames)))
	})
	if len(stacks) == 0 {
		b.WriteString("\nNo call stack gained goroutines between the two captures.\n")
	} else {
		fmt.Fprintf(b, "\nTop %d stacks by growth in goroutine count, of %d that grew (state inferred from the runtime frames; innermost first, runtime internals dropped):\n",
			min(topGoroutineStacks, len(stacks)), len(stacks))
		for i, s := range stacks[:min(topGoroutineStacks, len(stacks))] {
			fmt.Fprintf(b, "  #%d %+d (%d -> %d), state: %s\n", i+1, s.after-s.before, s.before, s.after, waitState(s.frames))
			writeGoroutineGroup(b, s.frames)
		}
	}
	if shrank > 0 {
		fmt.Fprintf(b, "(Stacks that lost goroutines: %d fewer in total; not listed.)\n", shrank)
	}
}
