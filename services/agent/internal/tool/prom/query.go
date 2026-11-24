package prom

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// 裁剪的上限（设计 3.4）。
const (
	maxVectorSeries = 50
	maxMatrixSeries = 20
	maxPoints       = 60
)

const emptyText = "no series matched. Note that a misspelled metric or label name also gives an empty result rather than an error; " +
	`check that the metric exists, e.g. count by (__name__) ({__name__=~"prefix.*"}), or drop label matchers one at a time.`

// ---- prom_query ----

type queryTool struct{ c *client }

type queryArgs struct {
	Query string `json:"query"`
	Time  string `json:"time"`
}

func (t *queryTool) Def() llm.ToolDef {
	return fnDef("prom_query",
		`Run a PromQL instant query (Prometheus /api/v1/query) and return the value of each series at one point in time. `+
			`At most 50 series are shown (highest values first when cut) with the total count; labels shared by all shown series are printed once on top. `+
			`Values have 4 significant digits. Aggregate with sum by (...) or topk(...) when you expect many series.`,
		object(map[string]any{
			"query": str("PromQL expression."),
			"time":  str("Evaluation time; default now. " + timeDesc),
		}, "query"))
}

func (t *queryTool) Call(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	var a queryArgs
	if err := tool.Decode(input, &a); err != nil {
		return tool.Result{}, err
	}
	if strings.TrimSpace(a.Query) == "" {
		return tool.Result{}, errors.New("query is required")
	}
	at, err := tool.ParseTime(a.Time, t.c.now())
	if err != nil {
		return tool.Result{}, err
	}
	resp, err := t.c.get(ctx, "/api/v1/query", url.Values{"query": {a.Query}, "time": {unixParam(at)}})
	if err != nil {
		return tool.Result{}, err
	}
	var b strings.Builder
	if err := formatData(&b, resp.data, "at "+at.UTC().Format(time.RFC3339), t.c.hidden); err != nil {
		return tool.Result{}, err
	}
	appendNotes(&b, resp.notes)
	return tool.Result{Text: b.String(), Raw: resp.raw}, nil
}

// ---- prom_query_range ----

type rangeTool struct{ c *client }

type rangeArgs struct {
	Query string `json:"query"`
	Start string `json:"start"`
	End   string `json:"end"`
	Step  string `json:"step"`
}

func (t *rangeTool) Def() llm.ToolDef {
	return fnDef("prom_query_range",
		`Run a PromQL range query (Prometheus /api/v1/query_range) and return each series over time. `+
			`At most 20 series (those with the highest maximum when cut) and at most 60 points per series: if step is missing or too small it is raised and the output says so. `+
			`Each series shows min, max and last value, then time=value pairs. Prefer this over prom_query when you need to see when something started.`,
		object(map[string]any{
			"query": str("PromQL expression."),
			"start": str("Start of the range. " + timeDesc),
			"end":   str("End of the range; default now. " + timeDesc),
			"step":  str("Resolution, e.g. 30s, 1m, 5m (or seconds as a number). Optional; raised automatically to keep at most 60 points per series."),
		}, "query", "start"))
}

func (t *rangeTool) Call(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	var a rangeArgs
	if err := tool.Decode(input, &a); err != nil {
		return tool.Result{}, err
	}
	if strings.TrimSpace(a.Query) == "" {
		return tool.Result{}, errors.New("query is required")
	}
	if strings.TrimSpace(a.Start) == "" {
		return tool.Result{}, errors.New("start is required, e.g. -1h or 2026-11-24T03:00:00Z")
	}
	now := t.c.now()
	start, err := tool.ParseTime(a.Start, now)
	if err != nil {
		return tool.Result{}, fmt.Errorf("start: %w", err)
	}
	end, err := tool.ParseTime(a.End, now)
	if err != nil {
		return tool.Result{}, fmt.Errorf("end: %w", err)
	}
	if !end.After(start) {
		return tool.Result{}, fmt.Errorf("start (%s) must be before end (%s)", start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
	}

	// 每条序列的点数是 floor((end-start)/step)+1，要不超过 maxPoints，step 至少是 (end-start)/(maxPoints-1)。
	minStep := niceStep(end.Sub(start) / (maxPoints - 1))
	step, stepNote := minStep, fmt.Sprintf("step %s (chosen automatically for at most %d points per series)", fmtDur(minStep), maxPoints)
	if a.Step != "" {
		asked, err := parseStep(a.Step)
		if err != nil {
			return tool.Result{}, err
		}
		if asked >= minStep {
			step, stepNote = asked, "step "+fmtDur(asked)
		} else {
			stepNote = fmt.Sprintf("step raised from %s to %s so each series has at most %d points", fmtDur(asked), fmtDur(minStep), maxPoints)
		}
	}

	q := url.Values{
		"query": {a.Query},
		"start": {unixParam(start)},
		"end":   {unixParam(end)},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
	resp, err := t.c.get(ctx, "/api/v1/query_range", q)
	if err != nil {
		return tool.Result{}, err
	}
	var b strings.Builder
	header := fmt.Sprintf("from %s to %s, %s", start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), stepNote)
	if err := formatData(&b, resp.data, header, t.c.hidden); err != nil {
		return tool.Result{}, err
	}
	appendNotes(&b, resp.notes)
	return tool.Result{Text: b.String(), Raw: resp.raw}, nil
}

// niceSteps 是自动选 step 时用的整齐的值，时间点读起来规整。
var niceSteps = []time.Duration{
	time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 15 * time.Second, 30 * time.Second,
	time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 15 * time.Minute, 30 * time.Minute,
	time.Hour, 2 * time.Hour, 3 * time.Hour, 6 * time.Hour, 12 * time.Hour, 24 * time.Hour,
}

// niceStep 返回不小于 d 的最小的整齐 step；超出表的范围时按小时向上取整。
func niceStep(d time.Duration) time.Duration {
	for _, s := range niceSteps {
		if s >= d {
			return s
		}
	}
	return (d + time.Hour - 1).Truncate(time.Hour)
}

// parseStep 接受 Go 的时长写法（30s、1m30s）或者秒数。
func parseStep(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	var d time.Duration
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		d = time.Duration(f * float64(time.Second))
	} else if d, err = time.ParseDuration(s); err != nil {
		return 0, fmt.Errorf("step %q: use a duration like 30s, 1m or 5m, or a number of seconds", s)
	}
	if d <= 0 {
		return 0, fmt.Errorf("step %q must be positive", s)
	}
	return d, nil
}

// fmtDur 把 1m0s 写成 1m、1h0m0s 写成 1h。
func fmtDur(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// ---- 结果的解析和裁剪 ----

type queryData struct {
	ResultType string         `json:"resultType"`
	Result     jsontext.Value `json:"result"`
}

type series struct {
	Metric map[string]string `json:"metric"`
	Value  *point            `json:"value"`
	Values []point           `json:"values"`
	// 原生直方图的样本不展开，只说明有。
	Histogram  jsontext.Value   `json:"histogram"`
	Histograms []jsontext.Value `json:"histograms"`
}

// point 是 Prometheus 的 [时间戳, "值"]。
type point struct {
	T float64
	V string
}

func (p *point) UnmarshalJSON(b []byte) error {
	var pair []jsontext.Value
	if err := json.Unmarshal(b, &pair); err != nil {
		return err
	}
	if len(pair) != 2 {
		return fmt.Errorf("sample has %d elements, want 2", len(pair))
	}
	if err := json.Unmarshal(pair[0], &p.T); err != nil {
		return err
	}
	return json.Unmarshal(pair[1], &p.V)
}

func (p *point) time() time.Time {
	return time.UnixMilli(int64(p.T * 1000)).UTC()
}

func (p *point) float() float64 {
	f, err := strconv.ParseFloat(p.V, 64)
	if err != nil {
		return nan
	}
	return f
}

// formatData 按结果的类型写出给模型看的文本。header 说明查的时间（即时查询的时刻或范围查询的区间）。
// 标签指着隐藏的命名空间的序列当它不存在（tool.Hidden）；聚合掉了命名空间标签的结果认不出来，照常给。
func formatData(b *strings.Builder, data jsontext.Value, header string, hidden *tool.Hidden) error {
	var d queryData
	if err := json.Unmarshal(data, &d); err != nil {
		return fmt.Errorf("cannot parse the Prometheus query result: %w", err)
	}
	switch d.ResultType {
	case "scalar", "string":
		var p point
		if err := json.Unmarshal(d.Result, &p); err != nil {
			return fmt.Errorf("cannot parse the Prometheus %s result: %w", d.ResultType, err)
		}
		v := p.V
		if d.ResultType == "scalar" {
			v = fmtVal(p.float())
		}
		fmt.Fprintf(b, "%s %s: %s\n", d.ResultType, header, v)
		return nil
	case "vector", "matrix":
	default:
		return fmt.Errorf("unexpected Prometheus result type %q", d.ResultType)
	}
	var ss []series
	if err := json.Unmarshal(d.Result, &ss); err != nil {
		return fmt.Errorf("cannot parse the Prometheus %s result: %w", d.ResultType, err)
	}
	ss = slices.DeleteFunc(ss, func(s series) bool { return hidden.Labels(s.Metric) })
	if len(ss) == 0 {
		fmt.Fprintf(b, "%s %s: %s\n", d.ResultType, header, emptyText)
		return nil
	}
	if d.ResultType == "vector" {
		formatVector(b, ss, header)
	} else {
		formatMatrix(b, ss, header)
	}
	return nil
}

func formatVector(b *strings.Builder, ss []series, header string) {
	total := len(ss)
	fmt.Fprintf(b, "vector %s: %d series", header, total)
	if total > maxVectorSeries {
		// 只在要裁的时候排序：没裁时保留 Prometheus 给的顺序（查询里可能用了 sort()）。
		sortDesc(ss, func(s series) float64 { return s.Value.floatOrNaN() })
		ss = ss[:maxVectorSeries]
		fmt.Fprintf(b, ", showing the %d with the highest values. Aggregate with topk(...) or sum by (...) to see fewer series", maxVectorSeries)
	}
	b.WriteString("\n")
	common := writeCommon(b, ss)
	for _, s := range ss {
		v := "(no value)"
		switch {
		case s.Value != nil:
			v = fmtVal(s.Value.float())
		case len(s.Histogram) > 0:
			v = "(native histogram; use histogram_quantile, histogram_count or histogram_sum)"
		}
		fmt.Fprintf(b, "%s %s\n", fmtLabels(s.Metric, common), v)
	}
}

func formatMatrix(b *strings.Builder, ss []series, header string) {
	total := len(ss)
	type stat struct {
		s            series
		lo, hi, last float64
		points       int
	}
	stats := make([]stat, 0, len(ss))
	for _, s := range ss {
		st := stat{s: s, lo: nan, hi: nan, last: nan, points: len(s.Values)}
		for _, p := range s.Values {
			v := p.float()
			st.last = v
			if math.IsNaN(v) { // NaN 不参与最小最大
				continue
			}
			if math.IsNaN(st.lo) || v < st.lo {
				st.lo = v
			}
			if math.IsNaN(st.hi) || v > st.hi {
				st.hi = v
			}
		}
		stats = append(stats, st)
	}

	fmt.Fprintf(b, "matrix %s: %d series", header, total)
	if total > maxMatrixSeries {
		sortDesc(stats, func(st stat) float64 { return st.hi })
		stats = stats[:maxMatrixSeries]
		fmt.Fprintf(b, ", showing the %d with the highest maximum. Aggregate with topk(...) or sum by (...) to see fewer series", maxMatrixSeries)
	}
	b.WriteString("\n")

	shown := make([]series, len(stats))
	for i, st := range stats {
		shown[i] = st.s
	}
	common := writeCommon(b, shown)

	// 时间点只写到时分秒，日期在开头写一次；跨天时每个点带上月日。
	layout := "15:04:05"
	var first, last time.Time
	for _, st := range stats {
		if n := len(st.s.Values); n > 0 {
			t0, t1 := st.s.Values[0].time(), st.s.Values[n-1].time()
			if first.IsZero() || t0.Before(first) {
				first = t0
			}
			if t1.After(last) {
				last = t1
			}
		}
	}
	if first.Format(time.DateOnly) != last.Format(time.DateOnly) {
		layout = "01-02T15:04:05"
		b.WriteString("times are UTC\n")
	} else if !first.IsZero() {
		fmt.Fprintf(b, "times are UTC on %s\n", first.Format(time.DateOnly))
	}

	for _, st := range stats {
		pts := st.s.Values
		fmt.Fprintf(b, "%s min=%s max=%s last=%s", fmtLabels(st.s.Metric, common), fmtVal(st.lo), fmtVal(st.hi), fmtVal(st.last))
		if len(pts) > maxPoints {
			// 范围查询的 step 已经保证不超过；即时查询里的范围选择器（foo[1h]）返回原始样本，可能超过。
			fmt.Fprintf(b, " (%d points, showing the last %d)", len(pts), maxPoints)
			pts = pts[len(pts)-maxPoints:]
		} else {
			fmt.Fprintf(b, " (%d points)", len(pts))
		}
		if len(st.s.Histograms) > 0 {
			fmt.Fprintf(b, " (%d native histogram samples not shown; use histogram_quantile, histogram_count or histogram_sum)", len(st.s.Histograms))
		}
		b.WriteString("\n ")
		for _, p := range pts {
			fmt.Fprintf(b, " %s=%s", p.time().Format(layout), fmtVal(p.float()))
		}
		b.WriteString("\n")
	}
}

func (p *point) floatOrNaN() float64 {
	if p == nil {
		return nan
	}
	return p.float()
}
