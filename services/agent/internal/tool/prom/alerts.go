package prom

import (
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/llm"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// maxAlerts 是 prom_alerts 最多列出的告警数。
const maxAlerts = 100

// 告警的状态。/api/v1/alerts 不返回 inactive 的。
const (
	stateFiring  = "firing"
	statePending = "pending"
)

type alert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	State       string            `json:"state"`
	// ActiveAt 用字符串接：Prometheus 在某些版本里会给零值或省略，按 time.Time 解析会让整次调用失败。
	ActiveAt string `json:"activeAt"`
	Value    string `json:"value"`
}

func (a alert) activeAt() time.Time {
	t, err := time.Parse(time.RFC3339Nano, a.ActiveAt)
	if err != nil {
		return time.Time{}
	}
	return t
}

// ---- prom_alerts ----

type alertsTool struct{ c *client }

type alertsArgs struct {
	State string `json:"state"`
}

func (t *alertsTool) Def() llm.ToolDef {
	return fnDef("prom_alerts",
		`List the alerts that are currently firing or pending in Prometheus (/api/v1/alerts), one per line: alertname, state, severity, when it became active, value and the other labels. `+
			`Firing alerts come first, then by activation time (earliest first). At most 100 are shown with the total count. Use prom_rule to see what an alert's rule checks.`,
		object(map[string]any{
			"state": map[string]any{"type": "string", "enum": []string{stateFiring, statePending}, "description": "Only list alerts in this state; default both."},
		}))
}

func (t *alertsTool) Call(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	var a alertsArgs
	if err := tool.Decode(input, &a); err != nil {
		return tool.Result{}, err
	}
	if a.State != "" && a.State != stateFiring && a.State != statePending {
		return tool.Result{}, fmt.Errorf("state %q: use %q, %q, or leave it out for both", a.State, stateFiring, statePending)
	}
	resp, err := t.c.get(ctx, "/api/v1/alerts", nil)
	if err != nil {
		return tool.Result{}, err
	}
	var data struct {
		Alerts []alert `json:"alerts"`
	}
	if err := json.Unmarshal(resp.data, &data); err != nil {
		return tool.Result{}, fmt.Errorf("cannot parse the Prometheus alerts: %w", err)
	}
	alerts := slices.DeleteFunc(data.Alerts, func(al alert) bool {
		return (a.State != "" && al.State != a.State) || t.c.hidden.Labels(al.Labels)
	})
	slices.SortStableFunc(alerts, func(x, y alert) int {
		// firing 在前，同一状态里先激活的在前：最早出问题的地方往往离根因最近。
		if c := cmp.Compare(rank(x.State), rank(y.State)); c != 0 {
			return c
		}
		if c := x.activeAt().Compare(y.activeAt()); c != 0 {
			return c
		}
		return cmp.Compare(x.Labels["alertname"], y.Labels["alertname"])
	})

	var b strings.Builder
	total := len(alerts)
	firing := 0
	for _, al := range alerts {
		if al.State == stateFiring {
			firing++
		}
	}
	switch {
	case total == 0 && a.State != "":
		fmt.Fprintf(&b, "no %s alerts\n", a.State)
	case total == 0:
		b.WriteString("no alerts are firing or pending\n")
	default:
		fmt.Fprintf(&b, "%d alerts (%d firing, %d pending)", total, firing, total-firing)
		if total > maxAlerts {
			fmt.Fprintf(&b, ", showing the first %d; filter with state or query ALERTS{...} with prom_query to see others", maxAlerts)
			alerts = alerts[:maxAlerts]
		}
		b.WriteString("\n")
	}
	now := t.c.now()
	for _, al := range alerts {
		b.WriteString(fmtAlert(al, now) + "\n")
	}
	appendNotes(&b, resp.notes)
	return tool.Result{Text: b.String(), Raw: resp.raw}, nil
}

func rank(state string) int {
	switch state {
	case stateFiring:
		return 0
	case statePending:
		return 1
	}
	return 2
}

func fmtAlert(al alert, now time.Time) string {
	var b strings.Builder
	b.WriteString(cmp.Or(al.Labels["alertname"], "(no alertname)") + " " + al.State)
	if sev := al.Labels["severity"]; sev != "" {
		b.WriteString(" severity=" + sev)
	}
	if at := al.activeAt(); !at.IsZero() && at.Year() > 1 {
		fmt.Fprintf(&b, " active since %s (%s ago)", at.UTC().Format(time.RFC3339), fmtDur(now.Sub(at).Round(time.Second)))
	}
	if al.Value != "" {
		v := point{V: al.Value}
		b.WriteString(" value=" + fmtVal(v.float()))
	}
	rest := maps.Clone(al.Labels)
	delete(rest, "alertname")
	delete(rest, "severity")
	if len(rest) > 0 {
		b.WriteString(" " + fmtLabels(rest, nil))
	}
	return b.String()
}

// ---- prom_rule ----

type ruleTool struct{ c *client }

type ruleArgs struct {
	Alertname string `json:"alertname"`
}

type ruleGroup struct {
	Name  string `json:"name"`
	File  string `json:"file"`
	Rules []rule `json:"rules"`
}

type rule struct {
	Type          string            `json:"type"`
	Name          string            `json:"name"`
	Query         string            `json:"query"`
	Duration      float64           `json:"duration"`
	KeepFiringFor float64           `json:"keepFiringFor"`
	Labels        map[string]string `json:"labels"`
	Annotations   map[string]string `json:"annotations"`
	State         string            `json:"state"`
	Health        string            `json:"health"`
	LastError     string            `json:"lastError"`
	Alerts        []alert           `json:"alerts"`
}

func (t *ruleTool) Def() llm.ToolDef {
	return fnDef("prom_rule",
		`Show the Prometheus alerting rule(s) with this alert name (/api/v1/rules): the PromQL expression, "for" duration, labels, annotations (including runbook_url), and the rule's current state and health. `+
			`Start here when diagnosing an alert: it tells you exactly what the alert measures and the threshold.`,
		object(map[string]any{
			"alertname": str(`The alert name, exactly as in the alertname label (case-sensitive).`),
		}, "alertname"))
}

func (t *ruleTool) Call(ctx context.Context, input jsontext.Value) (tool.Result, error) {
	var a ruleArgs
	if err := tool.Decode(input, &a); err != nil {
		return tool.Result{}, err
	}
	name := strings.TrimSpace(a.Alertname)
	if name == "" {
		return tool.Result{}, errors.New("alertname is required")
	}
	// 不用 rule_name[] 在服务端过滤：找不到时要从全部规则里给出相近的名字。规则总共几十条，响应不大。
	resp, err := t.c.get(ctx, "/api/v1/rules", url.Values{"type": {"alert"}})
	if err != nil {
		return tool.Result{}, err
	}
	var data struct {
		Groups []ruleGroup `json:"groups"`
	}
	if err := json.Unmarshal(resp.data, &data); err != nil {
		return tool.Result{}, fmt.Errorf("cannot parse the Prometheus rules: %w", err)
	}

	type match struct {
		g ruleGroup
		r rule
	}
	var found []match
	similar := map[string]bool{}
	lower := strings.ToLower(name)
	for _, g := range data.Groups {
		for _, r := range g.Rules {
			if r.Type != "" && r.Type != "alerting" {
				continue
			}
			if r.Name == name {
				r.Alerts = slices.DeleteFunc(r.Alerts, func(al alert) bool { return t.c.hidden.Labels(al.Labels) })
				found = append(found, match{g, r})
				continue
			}
			l := strings.ToLower(r.Name)
			if strings.Contains(l, lower) || strings.Contains(lower, l) {
				similar[r.Name] = true
			}
		}
	}
	if len(found) == 0 {
		msg := fmt.Sprintf("no alerting rule named %q (names are case-sensitive)", name)
		if len(similar) > 0 {
			names := slices.Sorted(maps.Keys(similar))
			msg += "; similar names: " + strings.Join(names[:min(len(names), 10)], ", ")
		} else {
			msg += "; use prom_alerts to see the names of current alerts"
		}
		return tool.Result{}, errors.New(msg)
	}

	var b strings.Builder
	if len(found) > 1 {
		fmt.Fprintf(&b, "%d alerting rules are named %s\n\n", len(found), name)
	}
	for i, m := range found {
		if i > 0 {
			b.WriteString("\n")
		}
		writeRule(&b, m.g, m.r)
	}
	appendNotes(&b, resp.notes)
	return tool.Result{Text: b.String(), Raw: resp.raw}, nil
}

func writeRule(b *strings.Builder, g ruleGroup, r rule) {
	fmt.Fprintf(b, "alert %s (group %s", r.Name, g.Name)
	if g.File != "" {
		fmt.Fprintf(b, ", file %s", g.File)
	}
	b.WriteString(")\n")
	b.WriteString("expr: " + indent(r.Query) + "\n")
	fmt.Fprintf(b, "for: %s\n", fmtDur(seconds(r.Duration)))
	if r.KeepFiringFor > 0 {
		fmt.Fprintf(b, "keep_firing_for: %s\n", fmtDur(seconds(r.KeepFiringFor)))
	}
	if len(r.Labels) > 0 {
		b.WriteString("labels: " + fmtLabels(r.Labels, nil) + "\n")
	}
	if len(r.Annotations) > 0 {
		b.WriteString("annotations:\n")
		for _, k := range slices.Sorted(maps.Keys(r.Annotations)) {
			fmt.Fprintf(b, "  %s: %s\n", k, indent(r.Annotations[k]))
		}
	}
	firing := 0
	for _, al := range r.Alerts {
		if al.State == stateFiring {
			firing++
		}
	}
	fmt.Fprintf(b, "state: %s, health: %s, active alerts: %d (%d firing, %d pending)\n",
		cmp.Or(r.State, "unknown"), cmp.Or(r.Health, "unknown"), len(r.Alerts), firing, len(r.Alerts)-firing)
	if r.LastError != "" {
		b.WriteString("last evaluation error: " + r.LastError + "\n")
	}
}

func seconds(f float64) time.Duration {
	return time.Duration(f * float64(time.Second))
}

// indent 让多行的表达式和注解在输出里缩进，不和下一个字段混在一起。
func indent(s string) string {
	return strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n    ")
}
