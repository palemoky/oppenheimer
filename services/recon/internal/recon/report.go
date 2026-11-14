package recon

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/merkle"
)

// Report 是一次运行的报告。
type Report struct {
	StartedAt  time.Time           `json:"started_at"`
	FinishedAt time.Time           `json:"finished_at"`
	Results    []*ComparisonReport `json:"comparisons"`
}

// Outstanding 返回全部比对里还没解决的差异数（见 ComparisonReport.Outstanding）。
func (r *Report) Outstanding() int {
	n := 0
	for _, c := range r.Results {
		n += c.Outstanding()
	}
	return n
}

// ComparisonReport 是一次比对的结果。
type ComparisonReport struct {
	Name string `json:"name"`
	Mode Mode   `json:"mode"`
	// Bound 是水位线（订单号的上界，不含），0 表示没有上界。BoundTime 是它对应的时间。
	Bound     int64     `json:"bound"`
	BoundTime time.Time `json:"bound_time,omitzero"`
	// Since 是窗口的下界（订单号，含），0 表示没有下界。SinceTime 是它对应的时间。
	Since     int64     `json:"since,omitzero"`
	SinceTime time.Time `json:"since_time,omitzero"`

	Left  SideReport `json:"left"`
	Right SideReport `json:"right"`
	// RootsEqual 为 true 时两侧的根摘要相同，没有往下比（树模式）。
	RootsEqual bool `json:"roots_equal"`
	// Levels 是 merkle.Diff 的逐层统计。
	Levels []merkle.LevelStats `json:"levels,omitempty"`
	// DiffNodes 是 Diff 给出、交给逐行比的差异节点数。
	DiffNodes int     `json:"diff_nodes"`
	Timings   Timings `json:"timings_ms"`

	// Counts 是确认的差异按类别的条数。
	Counts map[Class]int `json:"counts"`
	// Unstable 是复查时值又变了的条数（不算差异）；Dropped 是复查时已经一致、丢掉的条数。
	Unstable int `json:"unstable"`
	Dropped  int `json:"dropped_on_recheck"`
	// Examples 是确认的差异的例子，每类最多 max-examples 条；UnstableExamples 是复查时的那一份。
	Examples         []Example `json:"examples,omitempty"`
	UnstableExamples []Example `json:"unstable_examples,omitempty"`

	// NodeOnly 为 true 时右侧是存下来的树（archive），结果只到节点，在 Nodes 里。
	NodeOnly bool `json:"node_only,omitzero"`
	// Nodes 只用于右侧是文件的比对（archive）：摘要不同的节点，只能定位到这一层。
	Nodes []NodeDiff `json:"nodes,omitempty"`

	Repair *RepairReport `json:"repair,omitempty"`

	// Findings 是全部确认的差异，不进 JSON（数量可能很大，例子在 Examples 里）。
	Findings []Finding `json:"-"`
}

// Outstanding 是这次比对还没解决的差异数：确认的差异（修复过的取修复之后剩下的），加上差异节点。
func (c *ComparisonReport) Outstanding() int {
	if c.Repair != nil {
		return c.Repair.Remaining + len(c.Nodes)
	}
	return len(c.Findings) + len(c.Nodes)
}

// SideReport 是一侧的统计。
type SideReport struct {
	Name string `json:"name"`
	// Rows 是这一侧水位线以下的行数（树模式取根摘要的行数，全量模式是读回的行数）。
	Rows uint64 `json:"rows"`
	Root string `json:"root,omitempty"`
	// Statements 是发出的语句数，Digests 是收到的摘要行数。
	Statements int64 `json:"statements"`
	Digests    int64 `json:"digests"`
	// RowsRead、Bytes 是逐行比、复查、全量模式读回的行数和规范串的字节数。
	RowsRead int64 `json:"rows_read"`
	Bytes    int64 `json:"bytes"`
}

func (s *sideSource) report(r *SideReport) {
	r.Name = s.side.Name
	r.Statements = s.st.statements.Load()
	r.Digests = s.st.digests.Load()
	r.RowsRead = s.st.rows.Load()
	r.Bytes = s.st.bytes.Load()
}

// Timings 是各阶段的耗时，单位毫秒。
type Timings struct {
	FirstPass float64 `json:"first_pass"`
	Diff      float64 `json:"diff"`
	Rows      float64 `json:"rows"`
	Recheck   float64 `json:"recheck"`
	Repair    float64 `json:"repair,omitzero"`
	Total     float64 `json:"total"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// Field 是一列的名字和展示用的值。
type Field struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ExampleRow 是一行在一侧的一份。
type ExampleRow struct {
	Table  string  `json:"table"`
	Values []Field `json:"values"`
}

// Example 是一条差异的展示形式。
type Example struct {
	Class     Class        `json:"class"`
	OrderID   int64        `json:"order_id"`
	OrderTime time.Time    `json:"order_time"`
	ID        []Field      `json:"id"`
	Left      []ExampleRow `json:"left"`
	Right     []ExampleRow `json:"right"`
}

// NodeDiff 是一个摘要不同的节点：订单号上的一段区间，两侧各自的摘要。
type NodeDiff struct {
	Node     string    `json:"node"`
	From     int64     `json:"from_key"`
	To       int64     `json:"to_key"`
	FromTime time.Time `json:"from_time"`
	ToTime   time.Time `json:"to_time"`
	Left     string    `json:"left"`
	Right    string    `json:"right"`
}

func nodeDiff(d merkle.Difference) NodeDiff {
	lo, hi, _ := nodeRange(d.Node).sqlArgs()
	return NodeDiff{
		Node: d.Node.String(), From: lo, To: hi, FromTime: idgen.Time(lo), ToTime: idgen.Time(hi),
		Left: d.A.String(), Right: d.B.String(),
	}
}

// examples 把差异换成展示形式，每类最多 limit 条。
func examples(fs []Finding, side *Side, limit int) []Example {
	per := map[Class]int{}
	var out []Example
	for _, f := range fs {
		if per[f.Class] >= limit {
			continue
		}
		per[f.Class]++
		ex := Example{
			Class: f.Class, OrderID: f.Key, OrderTime: idgen.Time(f.Key),
			ID: decode(f.ID, side.ID),
		}
		for _, v := range f.Left {
			ex.Left = append(ex.Left, ExampleRow{Table: v.Table, Values: decode(v.Value, side.Cols)})
		}
		for _, v := range f.Right {
			ex.Right = append(ex.Right, ExampleRow{Table: v.Table, Values: decode(v.Value, side.Cols)})
		}
		out = append(out, ex)
	}
	return out
}

// decode 把规范串拆回各列。Bytes 的列在规范串里是十六进制，解回来；不是可打印的文本时保留十六进制。
func decode(canon string, cols []merkle.Column) []Field {
	parts := strings.Split(canon, merkle.Sep)
	if len(parts) != len(cols) {
		// 不会发生：整数、十六进制、空值标记里都没有分隔符。
		return []Field{{Name: "raw", Value: canon}}
	}
	out := make([]Field, len(cols))
	for i, c := range cols {
		v := parts[i]
		switch {
		case v == merkle.Null:
			v = "NULL"
		case c.Kind == merkle.Bytes:
			v = showBytes(v)
		}
		out[i] = Field{Name: c.Name, Value: v}
	}
	return out
}

func showBytes(h string) string {
	b, err := hex.DecodeString(h)
	if err != nil || !utf8.Valid(b) {
		return "0x" + h
	}
	for _, r := range string(b) {
		if !unicode.IsPrint(r) {
			return "0x" + h
		}
	}
	return strconv.Quote(string(b))
}

// WriteText 把报告写成人读的文本。
func (r *Report) WriteText(w io.Writer) error {
	var b strings.Builder
	for _, c := range r.Results {
		c.writeText(&b)
	}
	fmt.Fprintf(&b, "total outstanding differences: %d\n", r.Outstanding())
	_, err := io.WriteString(w, b.String())
	return err
}

func (c *ComparisonReport) writeText(b *strings.Builder) {
	bound := "none"
	if c.Bound > 0 {
		bound = fmt.Sprintf("%d (%s)", c.Bound, c.BoundTime.Format(time.RFC3339))
	}
	since := "none"
	if c.Since > 0 {
		since = fmt.Sprintf("%d (%s)", c.Since, c.SinceTime.Format(time.RFC3339))
	}
	fmt.Fprintf(b, "== %s  mode=%s  since=%s  bound=%s\n", c.Name, c.Mode, since, bound)
	side := func(tag string, s SideReport) {
		fmt.Fprintf(b, "  %-5s %s: rows=%d root=%s statements=%d digests=%d rows_read=%d bytes=%d\n",
			tag, s.Name, s.Rows, cmp.Or(s.Root, "-"), s.Statements, s.Digests, s.RowsRead, s.Bytes)
	}
	side("left", c.Left)
	side("right", c.Right)
	if c.RootsEqual {
		b.WriteString("  roots equal\n")
	}
	for _, l := range c.Levels {
		fmt.Fprintf(b, "  level shift=%d expanded=%d compared=%d differing=%d\n", l.Shift, l.Expanded, l.Compared, l.Differing)
	}
	t := c.Timings
	fmt.Fprintf(b, "  timings(ms): first_pass=%.1f diff=%.1f rows=%.1f recheck=%.1f repair=%.1f total=%.1f\n",
		t.FirstPass, t.Diff, t.Rows, t.Recheck, t.Repair, t.Total)
	if c.NodeOnly {
		// 右侧是文件：只能定位到节点。
		fmt.Fprintf(b, "  differing nodes: %d (located to node only: the right side is a saved tree)\n", len(c.Nodes))
		for _, n := range c.Nodes {
			fmt.Fprintf(b, "    node %s keys [%d, %d] time [%s, %s] left=%s right=%s\n",
				n.Node, n.From, n.To, n.FromTime.Format(time.RFC3339Nano), n.ToTime.Format(time.RFC3339Nano), n.Left, n.Right)
		}
		return
	}
	fmt.Fprintf(b, "  diff nodes=%d", c.DiffNodes)
	for _, cl := range Classes {
		fmt.Fprintf(b, " %s=%d", cl, c.Counts[cl])
	}
	fmt.Fprintf(b, " unstable=%d dropped_on_recheck=%d\n", c.Unstable, c.Dropped)
	for _, ex := range c.Examples {
		writeExample(b, ex, "")
	}
	for _, ex := range c.UnstableExamples {
		writeExample(b, ex, "unstable ")
	}
	if rp := c.Repair; rp != nil {
		fmt.Fprintf(b, "  repair: orders=%d upserted=%d deleted=%d skipped=%d fixed=%d remaining=%d\n",
			rp.Orders, rp.Upserted, rp.Deleted, rp.Skipped, rp.Fixed, rp.Remaining)
		for _, s := range rp.Notes {
			fmt.Fprintf(b, "    %s\n", s)
		}
	}
}

func writeExample(b *strings.Builder, ex Example, prefix string) {
	fmt.Fprintf(b, "  %s%s order %d (%s) id:%s\n", prefix, ex.Class, ex.OrderID, ex.OrderTime.Format(time.RFC3339Nano), fields(ex.ID))
	for _, r := range ex.Left {
		fmt.Fprintf(b, "      left  %s:%s\n", r.Table, fields(r.Values))
	}
	for _, r := range ex.Right {
		fmt.Fprintf(b, "      right %s:%s\n", r.Table, fields(r.Values))
	}
}

func fields(fs []Field) string {
	var b strings.Builder
	for _, f := range fs {
		b.WriteString(" " + f.Name + "=" + f.Value)
	}
	return b.String()
}
