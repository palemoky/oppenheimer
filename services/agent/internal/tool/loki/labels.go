package loki

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

const (
	// LabelsName 是 loki_labels 的工具名。
	LabelsName = "loki_labels"
	// maxLabelValues 是一次最多列出的标签名或标签值。
	maxLabelValues = 200
)

// labelName 是 Loki 合法的标签名。不合法的在发请求前就拦下，顺便保证它能安全地放进路径。
var labelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

const labelsDescription = `List the label names that exist in Loki, or, with "label", the values of one label. ` +
	`Use it before loki_query when you are not sure which labels or values exist (for example the app, namespace, pod or container label), ` +
	`because a wrong label name or value in a stream selector silently returns no logs. ` +
	`Only labels seen between start and end are listed (default: the last hour).`

type labelsTool struct{ c *client }

type labelsArgs struct {
	Label string `json:"label"`
	Start string `json:"start"`
	End   string `json:"end"`
}

func (t *labelsTool) Def() llm.ToolDef {
	return llm.ToolDef{
		Name:        LabelsName,
		Description: labelsDescription,
		Schema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"label": map[string]any{"type": "string", "description": `A label name such as "app". Omit it to list the label names instead of one label's values.`},
				"start": map[string]any{"type": "string", "description": `Start of the range: RFC 3339 (2026-11-24T03:00:00Z) or relative to now (-1h). Default -1h.`},
				"end":   map[string]any{"type": "string", "description": `End of the range, same formats. Default now.`},
			},
			"additionalProperties": false,
		},
	}
}

type labelsResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

func (t *labelsTool) Call(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	var args labelsArgs
	if err := tool.Decode(input, &args); err != nil {
		return tool.Result{}, err
	}
	args.Label = strings.TrimSpace(args.Label)
	if args.Label != "" && !labelName.MatchString(args.Label) {
		return tool.Result{}, fmt.Errorf("label %q is not a valid label name (letters, digits and underscores, not starting with a digit)", args.Label)
	}
	start, end, err := timeRange(args.Start, args.End, "-1h", t.c.now())
	if err != nil {
		return tool.Result{}, err
	}

	params := url.Values{}
	setRange(params, start, end)
	path := "/loki/api/v1/labels"
	if args.Label != "" {
		path = "/loki/api/v1/label/" + url.PathEscape(args.Label) + "/values"
	}
	body, err := t.c.get(ctx, path, params)
	if err != nil {
		return tool.Result{}, err
	}
	var resp labelsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return tool.Result{}, fmt.Errorf("the Loki response could not be parsed: %w", err)
	}
	if resp.Status != "success" {
		return tool.Result{}, fmt.Errorf("the Loki server answered with status %q", resp.Status)
	}
	return tool.Result{Text: formatLabels(args.Label, resp.Data, fmtRange(start, end)), Raw: body}, nil
}

func formatLabels(label string, values []string, span string) string {
	what := "label names"
	if label != "" {
		what = fmt.Sprintf("values of label %q", label)
	}
	var b strings.Builder
	if len(values) == 0 {
		fmt.Fprintf(&b, "No %s found between %s.", what, span)
		if label != "" {
			b.WriteString(" The label may not exist (call loki_labels without \"label\" to list the names), or it has no values in this range; try a longer range such as start=-6h.")
		} else {
			b.WriteString(" Try a longer range such as start=-6h.")
		}
		return b.String()
	}
	values = slices.Clone(values)
	slices.Sort(values)
	values = slices.Compact(values)
	shown := values[:min(len(values), maxLabelValues)]
	if len(shown) < len(values) {
		fmt.Fprintf(&b, "%d %s between %s; showing the first %d in sorted order. Use a shorter range to see fewer.\n", len(values), what, span, len(shown))
	} else {
		fmt.Fprintf(&b, "%d %s between %s:\n", len(values), what, span)
	}
	b.WriteString(strings.Join(shown, "\n"))
	return b.String()
}
