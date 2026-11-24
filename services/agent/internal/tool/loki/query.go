package loki

import (
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

const (
	// QueryName 是 loki_query 的工具名。
	QueryName = "loki_query"

	defaultLimit = 100
	maxLimit     = 300
	maxLineBytes = 500
	// maxOtherKeys 是 JSON 日志里除固定的几个字段之外最多保留的字段数。
	maxOtherKeys = 6
	// maxSeries、maxPoints 是指标查询结果的上限。
	maxSeries = 20
	maxPoints = 60
	// pointsPerLine 是指标查询每行写几个点。
	pointsPerLine = 10
	// textBudget 是日志行能用的字节数：给模型的结果总共 tool.MaxResultBytes，留出头尾的说明。
	// 自己先按行裁到这个数，说明里的“给了多少行”才是准的；交给 tool.Cap 去截，最后几行和说明会被一起截掉。
	textBudget = tool.MaxResultBytes - 2<<10
)

const queryDescription = `Run a LogQL query against Loki over a time range. ` +
	`Logs in this system are JSON lines written by Go's log/slog (keys like time, level, msg, error, trace_id). ` +
	`Always filter in LogQL instead of reading everything: a stream selector plus line filters, e.g. ` +
	`{namespace="prod", app="order"} |= "timeout", or {app="order"} | json | level="ERROR". ` +
	`Keep ranges short (minutes, not hours) and around the time of the incident. ` +
	`Log queries return at most "limit" lines (default 100, max 300), newest first unless direction is "forward"; ` +
	`JSON lines are shown compacted to level, msg, error, trace_id and a few other fields. ` +
	`Metric queries such as sum by (app) (count_over_time({namespace="prod"} |= "error" [1m])) or rate(...) return time series instead, ` +
	`with the step chosen so each series has about 60 points. ` +
	`If you are unsure which labels or values exist, call loki_labels first: a wrong label silently returns nothing.`

type queryTool struct{ c *client }

type queryArgs struct {
	LogQL     string `json:"logql"`
	Start     string `json:"start"`
	End       string `json:"end"`
	Limit     int    `json:"limit"`
	Direction string `json:"direction"`
}

func (t *queryTool) Def() llm.ToolDef {
	return llm.ToolDef{
		Name:        QueryName,
		Description: queryDescription,
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"logql":     map[string]any{"type": "string", "description": `The LogQL query: a log query ({app="order"} |= "error") or a metric query (rate({app="order"} |= "error" [1m])).`},
				"start":     map[string]any{"type": "string", "description": `Start of the range: RFC 3339 (2026-11-24T03:00:00Z) or relative to now (-30m). Default -15m.`},
				"end":       map[string]any{"type": "string", "description": `End of the range, same formats. Default now.`},
				"limit":     map[string]any{"type": "integer", "minimum": 1, "maximum": maxLimit, "description": `Maximum number of log lines for log queries. Default 100, max 300.`},
				"direction": map[string]any{"type": "string", "enum": []string{"backward", "forward"}, "description": `backward (default) returns the newest lines in the range, forward the oldest.`},
			},
			"required":             []string{"logql"},
			"additionalProperties": false,
		},
	}
}

// queryResponse 是 /loki/api/v1/query_range 的响应。result 的结构取决于 resultType。
type queryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string         `json:"resultType"`
		Result     jsontext.Value `json:"result"`
	} `json:"data"`
}

// stream 是日志查询的一条流。values 的每一项是 [纳秒时间戳（字符串）, 日志行]，
// 请求结构化元数据时还有第三项，这里不用。
type stream struct {
	Stream map[string]string  `json:"stream"`
	Values [][]jsontext.Value `json:"values"`
}

// series 是指标查询的一条序列。values 的每一项是 [秒（数字）, 值（字符串）]。
type series struct {
	Metric map[string]string  `json:"metric"`
	Values [][]jsontext.Value `json:"values"`
}

// queryInfo 是格式化结果时要用到的请求参数。
type queryInfo struct {
	start, end time.Time
	limit      int
	clamped    int // 模型要的 limit 超过上限时是它要的数，否则为 0
	backward   bool
	step       time.Duration
}

func (t *queryTool) Call(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	var args queryArgs
	if err := tool.Decode(input, &args); err != nil {
		return tool.Result{}, err
	}
	logql := strings.TrimSpace(args.LogQL)
	if logql == "" {
		return tool.Result{}, errors.New(`logql is required, e.g. {app="order"} |= "error"`)
	}
	info := queryInfo{limit: args.Limit, backward: true}
	switch args.Direction {
	case "", "backward":
	case "forward":
		info.backward = false
	default:
		return tool.Result{}, fmt.Errorf(`direction %q: use "backward" (newest first) or "forward" (oldest first)`, args.Direction)
	}
	switch {
	case info.limit < 0:
		return tool.Result{}, fmt.Errorf("limit %d: must be between 1 and %d", info.limit, maxLimit)
	case info.limit == 0:
		info.limit = defaultLimit
	case info.limit > maxLimit:
		info.clamped, info.limit = info.limit, maxLimit
	}
	var err error
	if info.start, info.end, err = timeRange(args.Start, args.End, "-15m", t.c.now()); err != nil {
		return tool.Result{}, err
	}
	info.step = stepFor(info.end.Sub(info.start))

	params := url.Values{}
	params.Set("query", logql)
	setRange(params, info.start, info.end)
	params.Set("limit", strconv.Itoa(info.limit))
	params.Set("direction", cmp.Or(args.Direction, "backward"))
	// step 只对指标查询起作用，日志查询忽略它，所以总是带上。
	params.Set("step", strconv.FormatInt(int64(info.step/time.Second), 10))

	body, err := t.c.get(ctx, "/loki/api/v1/query_range", params)
	if err != nil {
		return tool.Result{}, err
	}
	var resp queryResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return tool.Result{}, fmt.Errorf("the Loki response could not be parsed: %w", err)
	}
	if resp.Status != "success" {
		return tool.Result{}, fmt.Errorf("the Loki server answered with status %q", resp.Status)
	}

	var text string
	switch resp.Data.ResultType {
	case "streams":
		var streams []stream
		if err := json.Unmarshal(resp.Data.Result, &streams); err != nil {
			return tool.Result{}, fmt.Errorf("the Loki response could not be parsed: %w", err)
		}
		text, err = formatStreams(streams, info)
	case "matrix":
		var ss []series
		if err := json.Unmarshal(resp.Data.Result, &ss); err != nil {
			return tool.Result{}, fmt.Errorf("the Loki response could not be parsed: %w", err)
		}
		text, err = formatMatrix(ss, info)
	default:
		return tool.Result{}, fmt.Errorf("the Loki server returned result type %q, which this tool does not handle; use a log query or a range metric query", resp.Data.ResultType)
	}
	if err != nil {
		return tool.Result{}, err
	}
	return tool.Result{Text: text, Raw: body}, nil
}

// noiseLabels 是不给模型看的流标签：每行都不同的、和别的标签重复的、只描述采集管道的。
// Pod 名、容器名、命名空间、服务名留着。
var noiseLabels = map[string]bool{
	"observed_timestamp":          true,
	"k8s_pod_uid":                 true,
	"k8s_container_restart_count": true,
	"scope_name":                  true,
	"scope_version":               true,
	"severity_number":             true,
	"severity_text":               true, // 和 detected_level、日志里的 level 重复
	"log_iostream":                true,
	"log_file_path":               true,
}

// stepFor 让指标查询的每条序列大约 maxPoints 个点，最小 1 秒。
func stepFor(d time.Duration) time.Duration {
	// 点数是 floor(区间/step)+1，所以除的是 maxPoints-1：除以 maxPoints 会多出一个点（对着真的 Loki 跑出来是 61 个）。
	step := d / (maxPoints - 1)
	// 向上取到整秒：往下取会让点数又多出一个。
	if rem := step % time.Second; rem != 0 || step*(maxPoints-1) < d {
		step += time.Second - rem
	}
	return max(step, time.Second)
}

// ---- 日志 ----

type entry struct {
	ts     int64
	stream int
	line   string
}

// group 是连续的、内容相同的若干条日志，打印第一条的时间。
type group struct {
	ts   int64
	text string
	n    int
}

func formatStreams(streams []stream, info queryInfo) (string, error) {
	var entries []entry
	for i, s := range streams {
		for _, v := range s.Values {
			if len(v) < 2 {
				return "", fmt.Errorf("the Loki response has a log entry with %d fields, expected 2", len(v))
			}
			var tsText, line string
			if err := json.Unmarshal(v[0], &tsText); err != nil {
				return "", fmt.Errorf("the Loki response has a bad timestamp: %w", err)
			}
			if err := json.Unmarshal(v[1], &line); err != nil {
				return "", fmt.Errorf("the Loki response has a bad log line: %w", err)
			}
			ts, err := strconv.ParseInt(tsText, 10, 64)
			if err != nil {
				return "", fmt.Errorf("the Loki response has a bad timestamp %q", tsText)
			}
			entries = append(entries, entry{ts: ts, stream: i, line: line})
		}
	}
	if len(entries) == 0 {
		return fmt.Sprintf("No log lines matched between %s. "+
			"A wrong label name or value in the stream selector also gives an empty result: "+
			"check them with loki_labels, then loosen the line filters or widen the range.", fmtRange(info.start, info.end)), nil
	}

	// 各条流合成一个按时间排的列表。稳定排序：同一时刻的保持 Loki 给的顺序。
	slices.SortStableFunc(entries, func(a, b entry) int {
		if info.backward {
			return cmp.Compare(b.ts, a.ts)
		}
		return cmp.Compare(a.ts, b.ts)
	})

	// 先去掉对诊断没有用的标签。对着真的 Loki 跑出来的：Collector 写进来的每一行都带 observed_timestamp（每行不同）、
	// k8s_pod_uid 这些结构化元数据，Loki 把它们和标签一起返回，于是每一行自成一条流：
	// 行首的标记比日志本身还长，连续相同的行也永远合并不了。
	labelSets := make([]map[string]string, len(streams))
	for i := range streams {
		streams[i].Stream = maps.Clone(streams[i].Stream)
		maps.DeleteFunc(streams[i].Stream, func(k, _ string) bool { return noiseLabels[k] })
		labelSets[i] = streams[i].Stream
	}
	common := commonLabels(labelSets)

	// 合并连续相同的行时比较的是内容（流的标记加日志），不含时间，否则时间不同的重复行永远合不起来。
	var groups []group
	tagged := false
	for _, e := range entries {
		text, keys := compactLine(e.line)
		tag := streamTag(streams[e.stream].Stream, common, keys)
		if tag != "" {
			tagged = true
			text = tag + " " + text
		}
		if n := len(groups); n > 0 && groups[n-1].text == text {
			groups[n-1].n++
			continue
		}
		groups = append(groups, group{ts: e.ts, text: text, n: 1})
	}

	layout := timeLayout(entries[0].ts, entries[len(entries)-1].ts)
	lines := make([]string, len(groups))
	for i, g := range groups {
		lines[i] = time.Unix(0, g.ts).UTC().Format(layout) + " " + g.text
	}
	// 上面已经合并过，这里不会再有相邻的相同行；TrimLines 负责截长行。组数不超过条数，条数不超过 limit，所以不会丢行。
	lines, _ = tool.TrimLines(lines, info.limit, maxLineBytes, false)
	for i, g := range groups {
		if g.n > 1 {
			lines[i] += fmt.Sprintf("  [repeated %d times]", g.n)
		}
	}
	shown, used := 0, 0
	for _, l := range lines {
		if used+len(l)+1 > textBudget {
			break
		}
		used += len(l) + 1
		shown++
	}

	var b strings.Builder
	order := "newest first"
	if !info.backward {
		order = "oldest first"
	}
	fmt.Fprintf(&b, "Loki returned %d log entries from %d stream(s) between %s (limit %d, %s).\n",
		len(entries), len(streams), fmtRange(info.start, info.end), info.limit, order)
	if len(groups) < len(entries) {
		fmt.Fprintf(&b, "Consecutive identical lines are collapsed: %d entries became %d lines.\n", len(entries), len(groups))
	}
	if shown < len(lines) {
		fmt.Fprintf(&b, "Showing the first %d of %d lines (output size limit).\n", shown, len(lines))
	} else {
		fmt.Fprintf(&b, "Showing all %d lines.\n", len(lines))
	}
	if layout == clockLayout {
		fmt.Fprintf(&b, "Times are UTC on %s.\n", time.Unix(0, entries[0].ts).UTC().Format(time.DateOnly))
	} else {
		b.WriteString("Times are UTC.\n")
	}
	if len(common) > 0 {
		fmt.Fprintf(&b, "Labels on every line: %s\n", renderLabels(common))
	}
	if tagged {
		b.WriteString("Lines are prefixed with the stream labels that differ between streams.\n")
	}
	b.WriteString("\n")
	for _, l := range lines[:shown] {
		b.WriteString(l)
		b.WriteString("\n")
	}
	if info.clamped > 0 {
		fmt.Fprintf(&b, "\n[limit %d was lowered to the maximum of %d]", info.clamped, maxLimit)
	}
	if len(entries) >= info.limit {
		fmt.Fprintf(&b, "\n[limit reached: Loki returned %d entries, the limit, so more lines may exist in this range. "+
			"Narrow the query with a line filter (|= \"...\", | json | level=\"ERROR\") or a shorter range rather than raising the limit.]", len(entries))
	}
	if shown < len(lines) {
		fmt.Fprintf(&b, "\n[%d lines not shown because of the output size limit; narrow the query]", len(lines)-shown)
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

const (
	clockLayout = "15:04:05.000"
	fullLayout  = "2006-01-02 15:04:05.000"
)

// timeLayout：所有时间在同一天时只打时分秒（日期在开头写一次），跨天时每行带日期。
func timeLayout(a, b int64) string {
	da := time.Unix(0, a).UTC().Format(time.DateOnly)
	db := time.Unix(0, b).UTC().Format(time.DateOnly)
	if da == db {
		return clockLayout
	}
	return fullLayout
}

// commonLabels 返回所有标签集合里都有、而且值都一样的标签。
func commonLabels(sets []map[string]string) map[string]string {
	if len(sets) == 0 {
		return nil
	}
	common := maps.Clone(sets[0])
	for _, s := range sets[1:] {
		maps.DeleteFunc(common, func(k, v string) bool {
			sv, ok := s[k]
			return !ok || sv != v
		})
	}
	return common
}

// streamTag 是一行日志前面的流标记：这条流里不是所有流共有的标签。
// 日志行是 JSON 时跳过和它的字段同名的标签：它们是 `| json` 从这一行里提取出来的，行里已经有了。
func streamTag(labels, common map[string]string, jsonKeys map[string]bool) string {
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		if _, ok := common[k]; ok || jsonKeys[k] {
			continue
		}
		parts = append(parts, k+"="+quoteIfNeeded(labels[k]))
	}
	if len(parts) == 0 {
		return ""
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func renderLabels(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		parts = append(parts, k+"="+strconv.Quote(labels[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// leadKeys 是 JSON 日志里总是保留、按这个顺序放在前面的字段。
var leadKeys = []string{"level", "msg", "error", "err", "trace_id"}

// dropKeys 是丢掉的字段：时间已经打在行首了。
var dropKeys = map[string]bool{"time": true, "ts": true}

// compactLine 把 JSON 日志压成 level=... msg="..." key=value 的样子，返回压好的行和行里的全部字段名。
// 不是 JSON 对象的行原样返回。
func compactLine(line string) (string, map[string]bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "{") {
		return line, nil
	}
	var obj map[string]jsontext.Value
	if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
		return line, nil
	}
	keys := make(map[string]bool, len(obj))
	for k := range obj {
		keys[k] = true
	}
	var parts []string
	for _, k := range leadKeys {
		if v, ok := obj[k]; ok {
			parts = append(parts, k+"="+renderValue(v))
		}
	}
	others, omitted := 0, 0
	for _, k := range slices.Sorted(maps.Keys(obj)) {
		if dropKeys[k] || slices.Contains(leadKeys, k) {
			continue
		}
		v := obj[k]
		if others >= maxOtherKeys || !isScalar(v) {
			omitted++
			continue
		}
		parts = append(parts, k+"="+renderValue(v))
		others++
	}
	if omitted > 0 {
		parts = append(parts, fmt.Sprintf("(+%d more fields)", omitted))
	}
	return strings.Join(parts, " "), keys
}

func isScalar(v jsontext.Value) bool {
	k := v.Kind()
	return k != '{' && k != '['
}

// renderValue 把一个 JSON 值写成 logfmt 的样子：字符串需要时加引号，其余的写原文（对象、数组压成一行）。
func renderValue(v jsontext.Value) string {
	if v.Kind() == '"' {
		var s string
		if err := json.Unmarshal(v, &s); err == nil {
			return quoteIfNeeded(s)
		}
	}
	c := slices.Clone(v)
	if err := c.Compact(); err != nil {
		return string(v)
	}
	return string(c)
}

func quoteIfNeeded(s string) string {
	if s == "" || strings.ContainsFunc(s, func(r rune) bool {
		return r <= ' ' || r == '"' || r == '=' || r == '\\' || !strconv.IsPrint(r)
	}) {
		return strconv.Quote(s)
	}
	return s
}

// ---- 指标 ----

type point struct {
	t time.Time
	v float64
}

type parsedSeries struct {
	labels       map[string]string
	key          string // 排序时打破平局用
	points       []point
	lo, hi, last float64
}

func formatMatrix(raw []series, info queryInfo) (string, error) {
	if len(raw) == 0 {
		return fmt.Sprintf("The metric query returned no series between %s. "+
			"A wrong label name or value in the stream selector also gives an empty result: "+
			"check them with loki_labels, or widen the range.", fmtRange(info.start, info.end)), nil
	}
	all := make([]parsedSeries, 0, len(raw))
	labelSets := make([]map[string]string, 0, len(raw))
	for _, s := range raw {
		ps := parsedSeries{labels: s.Metric, key: renderLabels(s.Metric), lo: math.Inf(1), hi: math.Inf(-1)}
		for _, v := range s.Values {
			if len(v) < 2 {
				return "", fmt.Errorf("the Loki response has a sample with %d fields, expected 2", len(v))
			}
			var sec float64
			var val string
			if err := json.Unmarshal(v[0], &sec); err != nil {
				return "", fmt.Errorf("the Loki response has a bad sample time: %w", err)
			}
			if err := json.Unmarshal(v[1], &val); err != nil {
				return "", fmt.Errorf("the Loki response has a bad sample value: %w", err)
			}
			f, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return "", fmt.Errorf("the Loki response has a bad sample value %q", val)
			}
			whole, frac := math.Modf(sec)
			ps.points = append(ps.points, point{t: time.Unix(int64(whole), int64(frac*1e9)), v: f})
			ps.lo, ps.hi, ps.last = min(ps.lo, f), max(ps.hi, f), f
		}
		all = append(all, ps)
		labelSets = append(labelSets, s.Metric)
	}
	// 超过上限时留峰值最高的那些：排查时要看的通常是最异常的。
	slices.SortStableFunc(all, func(a, b parsedSeries) int {
		return cmp.Or(cmp.Compare(b.hi, a.hi), cmp.Compare(a.key, b.key))
	})
	common := commonLabels(labelSets)

	first, last := info.start, info.end
	layout := "15:04:05"
	sameDay := first.UTC().Format(time.DateOnly) == last.UTC().Format(time.DateOnly)
	if !sameDay {
		layout = "01-02 15:04:05"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "The metric query returned %d series between %s (step %s).\n", len(all), fmtRange(info.start, info.end), info.step)
	if len(all) > maxSeries {
		fmt.Fprintf(&b, "Showing the %d series with the highest peak value; aggregate with sum by (label) (...) or topk(...) to get fewer series.\n", maxSeries)
	}
	if sameDay {
		fmt.Fprintf(&b, "Times are UTC on %s. Values have 4 significant digits.\n", first.UTC().Format(time.DateOnly))
	} else {
		b.WriteString("Times are UTC. Values have 4 significant digits.\n")
	}
	if len(common) > 0 {
		fmt.Fprintf(&b, "Labels on every series: %s\n", renderLabels(common))
	}
	for _, s := range all[:min(len(all), maxSeries)] {
		own := maps.Clone(s.labels)
		maps.DeleteFunc(own, func(k, _ string) bool { _, ok := common[k]; return ok })
		b.WriteString("\n")
		if len(s.points) == 0 {
			fmt.Fprintf(&b, "%s no points\n", renderLabels(own))
			continue
		}
		fmt.Fprintf(&b, "%s min=%s max=%s last=%s", renderLabels(own), fmtValue(s.lo), fmtValue(s.hi), fmtValue(s.last))
		pts := s.points
		if len(pts) > maxPoints {
			fmt.Fprintf(&b, " (last %d of %d points)", maxPoints, len(pts))
			pts = pts[len(pts)-maxPoints:]
		}
		b.WriteString("\n")
		for chunk := range slices.Chunk(pts, pointsPerLine) {
			for _, p := range chunk {
				fmt.Fprintf(&b, "  %s=%s", p.t.UTC().Format(layout), fmtValue(p.v))
			}
			b.WriteString("\n")
		}
	}
	if len(all) > maxSeries {
		fmt.Fprintf(&b, "\n[%d of %d series not shown]", len(all)-maxSeries, len(all))
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

func fmtValue(v float64) string {
	return strconv.FormatFloat(v, 'g', 4, 64)
}
