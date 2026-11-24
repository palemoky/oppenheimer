package report

import (
	"encoding/json/jsontext"
	"strings"
	"testing"
)

func steps() map[string]Step {
	return map[string]Step{
		"s1": {ID: "s1", Tool: "prom_query", Input: jsontext.Value(`{"query":"up{job=\"mysql\"}"}`), Text: "up 0\n" + strings.Repeat("line\n", 30)},
		"s2": {ID: "s2", Tool: "loki_query", Input: jsontext.Value(`{"logql":"{app=\"order\"}"}`), Text: "Error: bad", Failed: true},
	}
}

func valid() *Report {
	return &Report{
		Summary: "MySQL is down", RootCause: "pod evicted", Confidence: ConfidenceHigh,
		Evidence: []Evidence{{Claim: "mysql_up is 0", Steps: []string{"s1"}}},
		RuledOut: []RuledOut{{Hypothesis: "network", Steps: nil}},
	}
}

func TestValidate(t *testing.T) {
	if err := valid().Validate(steps()); err != nil {
		t.Fatalf("valid report rejected: %v", err)
	}
	cases := []struct {
		name string
		edit func(*Report)
		want string
	}{
		{"no evidence", func(r *Report) { r.Evidence = nil }, "evidence is empty"},
		{"evidence without steps", func(r *Report) { r.Evidence[0].Steps = nil }, "evidence[0] cites no steps"},
		{"unknown step", func(r *Report) { r.Evidence[0].Steps = []string{"s9"} }, `"s9", which does not exist`},
		{"failed step", func(r *Report) { r.Evidence[0].Steps = []string{"s2"} }, `"s2", which returned an error`},
		{"ruled_out cites unknown", func(r *Report) { r.RuledOut[0].Steps = []string{"s8"} }, `ruled_out[0] cites step "s8"`},
		{"bad confidence", func(r *Report) { r.Confidence = "certain" }, `confidence "certain"`},
		{"empty root cause", func(r *Report) { r.RootCause = "  " }, "root_cause is empty"},
	}
	for _, tc := range cases {
		r := valid()
		tc.edit(r)
		err := r.Validate(steps())
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to contain %q", tc.name, err, tc.want)
		}
	}
}

func TestParse(t *testing.T) {
	r, err := Parse(jsontext.Value(`{"summary":"s","root_cause":"r","confidence":"low","evidence":[{"claim":"c","steps":["s1"]}],"ruled_out":[],"next_checks":["x"],"suggested_actions":[]}`))
	if err != nil || r.Evidence[0].Steps[0] != "s1" || r.NextChecks[0] != "x" {
		t.Fatalf("r = %+v, err = %v", r, err)
	}
	if _, err := Parse(jsontext.Value(`{"summary":`)); err == nil {
		t.Error("truncated JSON must fail")
	}
}

// strict 模式要求每一层对象都写 additionalProperties: false，并且 properties 里的字段全部 required。
func TestSchemaIsStrict(t *testing.T) {
	var walk func(path string, node any)
	walk = func(path string, node any) {
		m, ok := node.(map[string]any)
		if !ok {
			return
		}
		if m["type"] == "object" {
			if m["additionalProperties"] != false {
				t.Errorf("%s: additionalProperties is not false", path)
			}
			props := m["properties"].(map[string]any)
			required := m["required"].([]string)
			if len(required) != len(props) {
				t.Errorf("%s: %d properties but %d required", path, len(props), len(required))
			}
			for _, name := range required {
				if _, ok := props[name]; !ok {
					t.Errorf("%s: required %q is not a property", path, name)
				}
			}
			for name, p := range props {
				walk(path+"."+name, p)
			}
		}
		if items, ok := m["items"]; ok {
			walk(path+"[]", items)
		}
	}
	walk("$", Schema())
}

func TestRender(t *testing.T) {
	r := valid()
	r.Evidence = append(r.Evidence, Evidence{Claim: "ghost", Steps: []string{"s9"}})
	r.NextChecks = []string{"check the node"}
	md := string(Render(r, steps(), Stats{
		RunID: "run-1", Question: "why\nis it down", Outcome: "reported_truncated", Unverified: true,
		LimitHit: "turns", Model: "claude-opus-5-5", Turns: 3, ToolCalls: 2, CostUSD: 0.1234, Duration: "42s",
	}))
	for _, want := range []string{
		"# Diagnosis run-1", "> why\n> is it down", "**Unverified:**", "hit its turns limit",
		"## Root cause (confidence: high)", "### 1. mysql_up is 0",
		// 证据下面的查询和结果是从步骤里取的，不是模型写的。
		"`s1` `prom_query` `{\"query\":\"up{job=\\\"mysql\\\"}\"}`", "  up 0\n", "more lines in the run log",
		"`s9`: **not found in this run**", "- network\n", "## Next checks", "cost: $0.1234",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("report lacks %q:\n%s", want, md)
		}
	}
	if strings.Contains(md, "Suggested actions") {
		t.Error("empty sections must be omitted")
	}

	none := string(Render(nil, nil, Stats{RunID: "run-2", Question: "q", Outcome: "refused"}))
	if !strings.Contains(none, "**No report was produced** (outcome: `refused`)") {
		t.Errorf("no-report rendering:\n%s", none)
	}
}
