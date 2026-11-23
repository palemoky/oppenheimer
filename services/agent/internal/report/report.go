// Package report 定义诊断报告的结构、校验和渲染。设计见 3d 的 3.5。
//
// 模型通过 submit_report 工具交报告。每条证据引用这次运行里的步骤号（s1、s2……），
// 报告里的“查询和结果”由程序按步骤号从运行记录里取出来渲染，不采信模型自己抄的数。
package report

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// ToolName 是交报告的工具名。
const ToolName = "submit_report"

// 置信度的取值。
const (
	ConfidenceHigh   = "high"
	ConfidenceMedium = "medium"
	ConfidenceLow    = "low"
)

// excerptLines 是渲染时每一步结果最多带的行数。
const excerptLines = 15

// Report 是 submit_report 的参数。
type Report struct {
	Summary          string     `json:"summary"`
	RootCause        string     `json:"root_cause"`
	Confidence       string     `json:"confidence"`
	Evidence         []Evidence `json:"evidence"`
	RuledOut         []RuledOut `json:"ruled_out"`
	NextChecks       []string   `json:"next_checks"`
	SuggestedActions []string   `json:"suggested_actions"`
}

// Evidence 是一条结论和支撑它的步骤。
type Evidence struct {
	Claim string   `json:"claim"`
	Steps []string `json:"steps"`
}

// RuledOut 是一个被排除的猜测和排除它的步骤。
type RuledOut struct {
	Hypothesis string   `json:"hypothesis"`
	Steps      []string `json:"steps"`
}

// Description 是给模型看的工具说明。
const Description = `Submit the final diagnosis report. Call this exactly once, when you have enough evidence or cannot make further progress. ` +
	`Every evidence item must cite the step ids (like "s3") of tool calls made in this run whose results support the claim; ` +
	`the report shown to humans is rendered from those steps, so a claim without a real step behind it is rejected. ` +
	`If you could not determine the root cause, say so in root_cause, set confidence to "low", and list what a human should check next.`

// Schema 返回 submit_report 的参数 Schema。配合 strict 使用：每一层 additionalProperties 为 false，字段全部 required。
func Schema() map[string]any {
	str := map[string]any{"type": "string"}
	steps := map[string]any{
		"type":        "array",
		"items":       str,
		"description": `Step ids of tool calls in this run, e.g. ["s2","s5"].`,
	}
	strList := func(desc string) map[string]any {
		return map[string]any{"type": "array", "items": str, "description": desc}
	}
	object := func(props map[string]any, required ...string) map[string]any {
		return map[string]any{
			"type":                 "object",
			"properties":           props,
			"required":             required,
			"additionalProperties": false,
		}
	}
	return object(map[string]any{
		"summary":    map[string]any{"type": "string", "description": "One or two sentences: what is wrong and how bad it is."},
		"root_cause": map[string]any{"type": "string", "description": "The most likely root cause, or a plain statement that it could not be determined and where the investigation got stuck."},
		"confidence": map[string]any{"type": "string", "enum": []string{ConfidenceHigh, ConfidenceMedium, ConfidenceLow}},
		"evidence": map[string]any{
			"type":        "array",
			"description": "Claims that support the root cause, each backed by steps. At least one.",
			"items": object(map[string]any{
				"claim": str,
				"steps": steps,
			}, "claim", "steps"),
		},
		"ruled_out": map[string]any{
			"type":        "array",
			"description": "Hypotheses you checked and excluded. May be empty.",
			"items": object(map[string]any{
				"hypothesis": str,
				"steps":      steps,
			}, "hypothesis", "steps"),
		},
		"next_checks":       strList("What a human should check next. May be empty."),
		"suggested_actions": strList("Suggested remediation steps. Suggestions only; you never execute changes. May be empty."),
	}, "summary", "root_cause", "confidence", "evidence", "ruled_out", "next_checks", "suggested_actions")
}

// Parse 解析模型给的参数。
func Parse(input jsontext.Value) (*Report, error) {
	var r Report
	if err := json.Unmarshal(input, &r); err != nil {
		return nil, fmt.Errorf("report is not valid JSON for the schema: %w", err)
	}
	return &r, nil
}

// Step 是报告可以引用的一步工具调用。
type Step struct {
	ID    string
	Tool  string
	Input jsontext.Value
	// Text 是模型当时看到的（裁剪后的）结果。
	Text string
	// Failed 表示这一步出错了；出错的步骤不能当证据。
	Failed bool
}

// Validate 核对报告能不能溯源：有证据，每条证据至少引用一步，引用的步骤都存在并且没有失败。
// 返回的错误是写给模型看的（它会据此改一次）。
func (r *Report) Validate(steps map[string]Step) error {
	var problems []string
	if strings.TrimSpace(r.Summary) == "" {
		problems = append(problems, "summary is empty")
	}
	if strings.TrimSpace(r.RootCause) == "" {
		problems = append(problems, "root_cause is empty")
	}
	if !slices.Contains([]string{ConfidenceHigh, ConfidenceMedium, ConfidenceLow}, r.Confidence) {
		problems = append(problems, fmt.Sprintf("confidence %q is not one of high, medium, low", r.Confidence))
	}
	if len(r.Evidence) == 0 {
		problems = append(problems, "evidence is empty: cite at least one step, even when the root cause is unknown (cite what you did observe)")
	}
	check := func(where string, cited []string, required bool) {
		if required && len(cited) == 0 {
			problems = append(problems, where+" cites no steps")
		}
		for _, id := range cited {
			step, ok := steps[id]
			switch {
			case !ok:
				problems = append(problems, fmt.Sprintf("%s cites step %q, which does not exist in this run", where, id))
			case step.Failed:
				problems = append(problems, fmt.Sprintf("%s cites step %q, which returned an error and cannot be evidence", where, id))
			}
		}
	}
	for i, e := range r.Evidence {
		check(fmt.Sprintf("evidence[%d]", i), e.Steps, true)
	}
	for i, ro := range r.RuledOut {
		// 排除一个猜测可以是推理出来的，不强制要求步骤；但引用了就得是真的。
		check(fmt.Sprintf("ruled_out[%d]", i), ro.Steps, false)
	}
	if len(problems) > 0 {
		return errors.New("report rejected: " + strings.Join(problems, "; "))
	}
	return nil
}

// Stats 是渲染在报告末尾的运行统计。
type Stats struct {
	RunID      string
	Question   string
	Outcome    string
	Unverified bool
	LimitHit   string
	Model      string
	Turns      int
	ToolCalls  int
	CostUSD    float64
	Duration   string
}

// Render 把报告渲染成 Markdown。r 为 nil 时（没有交报告）只渲染统计。
func Render(r *Report, steps map[string]Step, st Stats) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "# Diagnosis %s\n\n", st.RunID)
	fmt.Fprintf(&b, "> %s\n\n", strings.ReplaceAll(strings.TrimSpace(st.Question), "\n", "\n> "))
	if r == nil {
		fmt.Fprintf(&b, "**No report was produced** (outcome: `%s`).\n\n", st.Outcome)
	} else {
		if st.Unverified {
			b.WriteString("**Unverified:** the report cites steps that could not be matched to this run. Treat every claim as unsupported.\n\n")
		}
		if st.LimitHit != "" {
			fmt.Fprintf(&b, "**Truncated:** the run hit its %s limit and was asked to report with the evidence it had.\n\n", st.LimitHit)
		}
		fmt.Fprintf(&b, "## Summary\n\n%s\n\n", r.Summary)
		fmt.Fprintf(&b, "## Root cause (confidence: %s)\n\n%s\n\n", r.Confidence, r.RootCause)
		b.WriteString("## Evidence\n\n")
		for i, e := range r.Evidence {
			fmt.Fprintf(&b, "### %d. %s\n\n", i+1, e.Claim)
			renderSteps(&b, e.Steps, steps)
		}
		if len(r.RuledOut) > 0 {
			b.WriteString("## Ruled out\n\n")
			for _, ro := range r.RuledOut {
				fmt.Fprintf(&b, "- %s", ro.Hypothesis)
				if len(ro.Steps) > 0 {
					fmt.Fprintf(&b, " (%s)", strings.Join(ro.Steps, ", "))
				}
				b.WriteString("\n")
			}
			b.WriteString("\n")
		}
		renderList(&b, "Next checks", r.NextChecks)
		renderList(&b, "Suggested actions", r.SuggestedActions)
	}
	b.WriteString("## Run\n\n")
	fmt.Fprintf(&b, "- outcome: `%s`\n- model: `%s`\n- turns: %d, tool calls: %d\n- cost: $%.4f\n- duration: %s\n",
		st.Outcome, st.Model, st.Turns, st.ToolCalls, st.CostUSD, st.Duration)
	return []byte(b.String())
}

func renderSteps(b *strings.Builder, cited []string, steps map[string]Step) {
	for _, id := range cited {
		step, ok := steps[id]
		if !ok {
			fmt.Fprintf(b, "- `%s`: **not found in this run**\n\n", id)
			continue
		}
		fmt.Fprintf(b, "- `%s` `%s` %s\n\n", id, step.Tool, fence(string(step.Input)))
		lines := strings.Split(strings.TrimRight(step.Text, "\n"), "\n")
		more := 0
		if len(lines) > excerptLines {
			more = len(lines) - excerptLines
			lines = lines[:excerptLines]
		}
		b.WriteString("  ```\n")
		for _, line := range lines {
			fmt.Fprintf(b, "  %s\n", strings.ReplaceAll(line, "```", "'''"))
		}
		if more > 0 {
			fmt.Fprintf(b, "  … %d more lines in the run log\n", more)
		}
		b.WriteString("  ```\n\n")
	}
}

func renderList(b *strings.Builder, title string, items []string) {
	if len(items) == 0 {
		return
	}
	fmt.Fprintf(b, "## %s\n\n", title)
	for _, item := range items {
		fmt.Fprintf(b, "- %s\n", item)
	}
	b.WriteString("\n")
}

// fence 把一小段文字包成行内代码；里面有反引号时换成两个反引号的写法。
func fence(s string) string {
	if strings.Contains(s, "`") {
		return "`` " + s + " ``"
	}
	return "`" + s + "`"
}
