package prom

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

var now = time.Date(2026, 11, 24, 3, 0, 0, 0, time.UTC)

func unix(t time.Time) string { return strconv.FormatInt(t.Unix(), 10) }

// fakeProm 是一个假的 Prometheus：按路径返回固定的状态码和响应体，记下最后一次请求。
type fakeProm struct {
	srv    *httptest.Server
	tools  map[string]tool.Tool
	mu     sync.Mutex
	last   *url.URL
	status int
	body   string
}

func newFake(t *testing.T, status int, body string) *fakeProm {
	t.Helper()
	f := &fakeProm{status: status, body: body}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.last = r.URL
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = w.Write([]byte(f.body))
	}))
	t.Cleanup(f.srv.Close)
	f.tools = map[string]tool.Tool{}
	for _, tl := range New(Config{BaseURL: f.srv.URL + "/", Now: func() time.Time { return now }}) {
		f.tools[tl.Def().Name] = tl
	}
	return f
}

func (f *fakeProm) call(t *testing.T, name, input string) (tool.Result, error) {
	t.Helper()
	return f.tools[name].Call(t.Context(), jsontext.Value(input))
}

func (f *fakeProm) lastURL() *url.URL {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

func success(data string) string { return `{"status":"success","data":` + data + `}` }

func vector(series ...string) string {
	return success(`{"resultType":"vector","result":[` + strings.Join(series, ",") + `]}`)
}

func sample(labels, value string) string {
	return fmt.Sprintf(`{"metric":{%s},"value":[%d,%q]}`, labels, now.Unix(), value)
}

func TestNewDefs(t *testing.T) {
	f := newFake(t, 200, "")
	for _, name := range []string{"prom_query", "prom_query_range", "prom_alerts", "prom_rule"} {
		tl, ok := f.tools[name]
		if !ok {
			t.Fatalf("tool %s missing", name)
		}
		def := tl.Def()
		if def.Description == "" || def.Schema["additionalProperties"] != false {
			t.Errorf("%s: def = %+v", name, def)
		}
	}
}

func TestRequests(t *testing.T) {
	cases := []struct {
		name, tool, input string
		path              string
		params            map[string]string
	}{
		{
			"instant default time", "prom_query", `{"query":"up"}`, "/api/v1/query",
			map[string]string{"query": "up", "time": unix(now)},
		},
		{
			"instant relative time", "prom_query", `{"query":"up","time":"-30m"}`, "/api/v1/query",
			map[string]string{"query": "up", "time": unix(now.Add(-30 * time.Minute))},
		},
		{
			"instant absolute time", "prom_query", `{"query":"up","time":"2026-11-24T01:00:00Z"}`, "/api/v1/query",
			map[string]string{"time": unix(time.Date(2026, 11, 24, 1, 0, 0, 0, time.UTC))},
		},
		{
			"range relative", "prom_query_range", `{"query":"rate(x[5m])","start":"-1h"}`, "/api/v1/query_range",
			map[string]string{"query": "rate(x[5m])", "start": unix(now.Add(-time.Hour)), "end": unix(now), "step": "120"},
		},
		{
			"range end and step", "prom_query_range", `{"query":"x","start":"-2h","end":"-1h","step":"5m"}`, "/api/v1/query_range",
			map[string]string{"start": unix(now.Add(-2 * time.Hour)), "end": unix(now.Add(-time.Hour)), "step": "300"},
		},
		{"alerts", "prom_alerts", `{}`, "/api/v1/alerts", map[string]string{}},
		{"rules", "prom_rule", `{"alertname":"X"}`, "/api/v1/rules", map[string]string{"type": "alert"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t, 200, success(`{"resultType":"vector","result":[],"alerts":[],"groups":[]}`))
			_, _ = f.call(t, tc.tool, tc.input) // 结果不重要（prom_rule 找不到会报错），只看请求
			u := f.lastURL()
			if u == nil {
				t.Fatal("no request")
			}
			if u.Path != tc.path {
				t.Errorf("path = %s, want %s", u.Path, tc.path)
			}
			for k, want := range tc.params {
				if got := u.Query().Get(k); got != want {
					t.Errorf("param %s = %q, want %q", k, got, want)
				}
			}
		})
	}
}

func TestVectorFormatting(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		want     []string
		notWant  []string
		maxLines int
	}{
		{
			name: "single series keeps all labels",
			body: vector(sample(`"__name__":"up","job":"mysql"`, "1")),
			want: []string{"1 series", `up{job="mysql"} 1`},
		},
		{
			name: "common labels factored out, 4 significant digits",
			body: vector(
				sample(`"__name__":"lat","job":"order","pod":"a"`, "1234567"),
				sample(`"__name__":"lat","job":"order","pod":"b"`, "0.123456"),
				sample(`"__name__":"lat","job":"order","pod":"c"`, "NaN"),
			),
			want:    []string{`labels common to all shown series: lat{job="order"}`, `{pod="a"} 1235000`, `{pod="b"} 0.1235`, `{pod="c"} NaN`},
			notWant: []string{`pod="a", job`, "0.123456"},
		},
		{
			name: "empty result",
			body: vector(),
			want: []string{"no series matched", "misspelled metric or label name"},
		},
		{
			name: "scalar",
			body: success(`{"resultType":"scalar","result":[1795500000,"3.14159"]}`),
			want: []string{"scalar at 2026-11-24T03:00:00Z: 3.142"},
		},
		{
			name: "warnings passed on",
			body: `{"status":"success","data":{"resultType":"vector","result":[]},"warnings":["result truncated"]}`,
			want: []string{"warning: result truncated"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t, 200, tc.body)
			res, err := f.call(t, "prom_query", `{"query":"q"}`)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(res.Text, w) {
					t.Errorf("text lacks %q:\n%s", w, res.Text)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(res.Text, w) {
					t.Errorf("text contains %q:\n%s", w, res.Text)
				}
			}
			if string(res.Raw) != tc.body {
				t.Error("Raw is not the full response body")
			}
		})
	}
}

func TestVectorSeriesCap(t *testing.T) {
	var ss []string
	for i := range 120 {
		ss = append(ss, sample(fmt.Sprintf(`"job":"x","pod":"p%d"`, i), strconv.Itoa(i)))
	}
	f := newFake(t, 200, vector(ss...))
	res, err := f.call(t, "prom_query", `{"query":"q"}`)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(res.Text), "\n")
	// 头一行、共有标签一行、50 条序列
	if len(lines) != 52 {
		t.Fatalf("got %d lines:\n%s", len(lines), res.Text)
	}
	if !strings.Contains(lines[0], "120 series, showing the 50 with the highest values") || !strings.Contains(lines[0], "topk") {
		t.Errorf("header = %q", lines[0])
	}
	if lines[1] != `labels common to all shown series: {job="x"}` {
		t.Errorf("common = %q", lines[1])
	}
	if lines[2] != `{pod="p119"} 119` || lines[51] != `{pod="p70"} 70` {
		t.Errorf("first/last = %q / %q", lines[2], lines[51])
	}
}

func matrix(n, points int, step time.Duration) string {
	var ss []string
	start := now.Add(-time.Duration(points-1) * step)
	for i := range n {
		var vs []string
		for j := range points {
			vs = append(vs, fmt.Sprintf(`[%d,"%d"]`, start.Add(time.Duration(j)*step).Unix(), i*1000+j))
		}
		ss = append(ss, fmt.Sprintf(`{"metric":{"job":"x","pod":"p%d"},"values":[%s]}`, i, strings.Join(vs, ",")))
	}
	return success(`{"resultType":"matrix","result":[` + strings.Join(ss, ",") + `]}`)
}

func TestRangeFormatting(t *testing.T) {
	f := newFake(t, 200, matrix(25, 31, 2*time.Minute))
	res, err := f.call(t, "prom_query_range", `{"query":"q","start":"-1h","step":"10s"}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{
		"step raised from 10s to 2m so each series has at most 60 points",
		"25 series, showing the 20 with the highest maximum",
		`labels common to all shown series: {job="x"}`,
		"times are UTC on 2026-11-24",
		`{pod="p24"} min=24000 max=24030 last=24030 (31 points)`,
		"02:00:00=24000 02:02:00=24000",
		"03:00:00=24030",
	} {
		if !strings.Contains(res.Text, w) {
			t.Errorf("text lacks %q:\n%s", w, res.Text)
		}
	}
	if strings.Contains(res.Text, `{pod="p4"}`) {
		t.Errorf("series with the lowest maximum not cut:\n%s", res.Text)
	}
	if got := f.lastURL().Query().Get("step"); got != "120" {
		t.Errorf("step = %s", got)
	}
}

// 即时查询里的范围选择器返回原始样本，可能超过 60 个点。
func TestMatrixPointCap(t *testing.T) {
	f := newFake(t, 200, matrix(1, 100, 15*time.Second))
	res, err := f.call(t, "prom_query", `{"query":"x[25m]"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Text, "(100 points, showing the last 60)") || !strings.Contains(res.Text, "min=0 max=99 last=99") {
		t.Errorf("text:\n%s", res.Text)
	}
	if n := strings.Count(res.Text, "="); n != 60+3+2 { // 60 个点、min/max/last、两个标签
		t.Errorf("%d '=' in text:\n%s", n, res.Text)
	}
}

func TestRangeStep(t *testing.T) {
	// 自动选的 step 要让每条序列最多 60 个点：floor(range/step)+1 <= 60。
	for _, r := range []time.Duration{time.Minute, 59 * time.Second, 10 * time.Minute, time.Hour, 3 * time.Hour, 7 * 24 * time.Hour, 30 * 24 * time.Hour} {
		step := niceStep(r / (maxPoints - 1))
		if points := int(r/step) + 1; points > maxPoints || step < time.Second {
			t.Errorf("range %s: step %s gives %d points", r, step, points)
		}
	}
	cases := []struct{ input, wantStep, wantNote string }{
		{`{"query":"q","start":"-1h"}`, "120", "step 2m (chosen automatically"},
		{`{"query":"q","start":"-1h","step":"1m"}`, "120", "step raised from 1m to 2m"},
		{`{"query":"q","start":"-1h","step":"5m"}`, "300", "step 5m:"},
		{`{"query":"q","start":"-1h","step":"600"}`, "600", "step 10m:"},
		{`{"query":"q","start":"-10m"}`, "15", "step 15s (chosen"},
	}
	for _, tc := range cases {
		f := newFake(t, 200, success(`{"resultType":"matrix","result":[]}`))
		res, err := f.call(t, "prom_query_range", tc.input)
		if err != nil {
			t.Fatalf("%s: %v", tc.input, err)
		}
		if got := f.lastURL().Query().Get("step"); got != tc.wantStep {
			t.Errorf("%s: step = %s, want %s", tc.input, got, tc.wantStep)
		}
		if !strings.Contains(res.Text, tc.wantNote) {
			t.Errorf("%s: text lacks %q:\n%s", tc.input, tc.wantNote, res.Text)
		}
	}
}

func TestBadArguments(t *testing.T) {
	cases := []struct{ tool, input, want string }{
		{"prom_query", `{"query":"up","qurey":"x"}`, "invalid arguments"},
		{"prom_query", `{"query":"  "}`, "query is required"},
		{"prom_query", `{"query":"up","time":"yesterday"}`, "RFC 3339"},
		{"prom_query_range", `{"query":"up","start":"-1h","resolution":"1m"}`, "invalid arguments"},
		{"prom_query_range", `{"query":"up"}`, "start is required"},
		{"prom_query_range", `{"query":"up","start":"-1h","end":"-2h"}`, "must be before end"},
		{"prom_query_range", `{"query":"up","start":"-1h","step":"1x"}`, "use a duration like"},
		{"prom_query_range", `{"query":"up","start":"-1h","step":"-5s"}`, "must be positive"},
		{"prom_alerts", `{"state":"inactive"}`, `state "inactive"`},
		{"prom_alerts", `{"severity":"critical"}`, "invalid arguments"},
		{"prom_rule", `{}`, "alertname is required"},
		{"prom_rule", `{"name":"X"}`, "invalid arguments"},
	}
	for _, tc := range cases {
		f := newFake(t, 200, vector())
		_, err := f.call(t, tc.tool, tc.input)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s %s: err = %v, want %q", tc.tool, tc.input, err, tc.want)
		}
		if f.lastURL() != nil {
			t.Errorf("%s %s: request sent despite bad arguments", tc.tool, tc.input)
		}
	}
}

func TestPrometheusErrors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   []string
	}{
		{
			"bad promql", 400, `{"status":"error","errorType":"bad_data","error":"1:5: parse error: unexpected right parenthesis ')'"}`,
			[]string{"bad_data", "unexpected right parenthesis", "Fix the query"},
		},
		{
			"execution", 422, `{"status":"error","errorType":"execution","error":"many-to-many matching not allowed"}`,
			[]string{"execution", "many-to-many"},
		},
		{
			"timeout", 503, `{"status":"error","errorType":"timeout","error":"query timed out in expression evaluation"}`,
			[]string{"timed out", "Narrow the query"},
		},
		{"no json", 502, `<html>bad gateway</html>`, []string{"HTTP 502", "without an error message"}},
		{"not an api response", 200, `{"foo":1}`, []string{"not a Prometheus API response"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t, tc.status, tc.body)
			_, err := f.call(t, "prom_query", `{"query":"sum(up))"}`)
			if err == nil {
				t.Fatal("no error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("err = %v, want it to contain %q", err, w)
				}
			}
			if strings.Contains(err.Error(), f.srv.URL) || strings.Contains(err.Error(), "/api/v1") {
				t.Errorf("err leaks the internal URL: %v", err)
			}
		})
	}
}

func TestTransportErrors(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer srv.Close()
		q := New(Config{BaseURL: srv.URL})[0]
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err := q.Call(ctx, jsontext.Value(`{"query":"up"}`))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
		if strings.Contains(err.Error(), srv.URL) {
			t.Errorf("err leaks the internal URL: %v", err)
		}
	})
	t.Run("connection refused", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		base := srv.URL
		srv.Close()
		_, err := New(Config{BaseURL: base})[0].Call(t.Context(), jsontext.Value(`{"query":"up"}`))
		if err == nil || !strings.Contains(err.Error(), "cannot reach Prometheus") || strings.Contains(err.Error(), "/api/v1") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("body too large", func(t *testing.T) {
		f := newFake(t, 200, `{"status":"success","data":"`+strings.Repeat("x", maxBodyBytes)+`"}`)
		_, err := f.call(t, "prom_query", `{"query":"up"}`)
		if err == nil || !strings.Contains(err.Error(), "larger than 8 MiB") {
			t.Errorf("err = %v", err)
		}
	})
}

func alertJSON(name, state, severity, activeAt string, extra string) string {
	return fmt.Sprintf(`{"labels":{"alertname":%q,"severity":%q%s},"annotations":{"summary":"s"},"state":%q,"activeAt":%q,"value":"1.23456e+02"}`,
		name, severity, extra, state, activeAt)
}

func TestAlerts(t *testing.T) {
	body := success(`{"alerts":[` + strings.Join([]string{
		alertJSON("Pending1", "pending", "warning", "2026-11-24T02:50:00Z", ``),
		alertJSON("FiringLate", "firing", "critical", "2026-11-24T02:55:00Z", `,"service":"order"`),
		alertJSON("FiringEarly", "firing", "warning", "2026-11-24T02:30:00.123Z", ``),
	}, ",") + `]}`)
	cases := []struct {
		input string
		want  []string
	}{
		{`{}`, []string{
			"3 alerts (2 firing, 1 pending)",
			"FiringEarly firing severity=warning active since 2026-11-24T02:30:00Z (30m ago) value=123.5",
			`FiringLate firing severity=critical active since 2026-11-24T02:55:00Z (5m ago) value=123.5 {service="order"}`,
			"Pending1 pending severity=warning",
		}},
		{`{"state":"pending"}`, []string{"1 alerts (0 firing, 1 pending)", "Pending1 pending"}},
		{`{"state":"firing"}`, []string{"2 alerts (2 firing, 0 pending)", "FiringEarly", "FiringLate"}},
	}
	for _, tc := range cases {
		f := newFake(t, 200, body)
		res, err := f.call(t, "prom_alerts", tc.input)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(res.Text), "\n")
		if len(lines) != len(tc.want) {
			t.Errorf("%s: got %d lines, want %d:\n%s", tc.input, len(lines), len(tc.want), res.Text)
			continue
		}
		for i, w := range tc.want {
			if !strings.HasPrefix(lines[i], w) {
				t.Errorf("%s: line %d = %q, want prefix %q", tc.input, i, lines[i], w)
			}
		}
	}

	t.Run("cap", func(t *testing.T) {
		var as []string
		for i := range 130 {
			as = append(as, alertJSON(fmt.Sprintf("A%03d", i), "firing", "info", now.Add(-time.Duration(i)*time.Minute).Format(time.RFC3339), ""))
		}
		f := newFake(t, 200, success(`{"alerts":[`+strings.Join(as, ",")+`]}`))
		res, err := f.call(t, "prom_alerts", `{}`)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(res.Text), "\n")
		if len(lines) != 101 || !strings.Contains(lines[0], "130 alerts") || !strings.Contains(lines[0], "showing the first 100") {
			t.Errorf("%d lines, header %q", len(lines), lines[0])
		}
		if !strings.HasPrefix(lines[1], "A129 ") { // 最早激活的在前
			t.Errorf("first alert = %q", lines[1])
		}
	})

	t.Run("none", func(t *testing.T) {
		f := newFake(t, 200, success(`{"alerts":[]}`))
		res, err := f.call(t, "prom_alerts", `{}`)
		if err != nil || !strings.Contains(res.Text, "no alerts are firing or pending") {
			t.Errorf("text = %q, err = %v", res.Text, err)
		}
	})
}

const rulesBody = `{"status":"success","data":{"groups":[{"name":"order.rules","file":"/etc/prometheus/rules/order.yaml","rules":[
{"type":"alerting","name":"OrderHighErrorRate","query":"sum(rate(errors[5m]))\n  / sum(rate(requests[5m])) > 0.05","duration":300,
 "labels":{"severity":"critical"},"annotations":{"summary":"order errors","runbook_url":"https://runbooks/order-errors"},
 "state":"firing","health":"ok","alerts":[{"labels":{},"state":"firing"},{"labels":{},"state":"pending"}]},
{"type":"alerting","name":"OrderHighLatency","query":"p99 > 1","duration":600,"labels":{"severity":"warning"},"annotations":{},"state":"inactive","health":"ok","alerts":[]}
]},{"name":"other","file":"x.yaml","rules":[
{"type":"alerting","name":"PaymentHighErrorRate","query":"x > 1","duration":3600,"labels":{},"annotations":{},"state":"inactive","health":"err","lastError":"bad","alerts":[]}
]}]}}`

func TestRule(t *testing.T) {
	cases := []struct {
		name, alertname string
		want            []string
		wantErr         string
	}{
		{"found", "OrderHighErrorRate", []string{
			"alert OrderHighErrorRate (group order.rules, file /etc/prometheus/rules/order.yaml)",
			"expr: sum(rate(errors[5m]))\n      / sum(rate(requests[5m])) > 0.05",
			"for: 5m\n",
			`labels: {severity="critical"}`,
			"  runbook_url: https://runbooks/order-errors",
			"state: firing, health: ok, active alerts: 2 (1 firing, 1 pending)",
		}, ""},
		{"last error", "PaymentHighErrorRate", []string{"for: 1h\n", "health: err", "last evaluation error: bad"}, ""},
		{"missing with similar", "HighErrorRate", nil, `no alerting rule named "HighErrorRate" (names are case-sensitive); similar names: OrderHighErrorRate, PaymentHighErrorRate`},
		{"missing", "DiskFull", nil, "use prom_alerts"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t, 200, rulesBody)
			res, err := f.call(t, "prom_rule", fmt.Sprintf(`{"alertname":%q}`, tc.alertname))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(res.Text, w) {
					t.Errorf("text lacks %q:\n%s", w, res.Text)
				}
			}
			if strings.Contains(res.Text, "OrderHighLatency") {
				t.Errorf("other rules leaked into the result:\n%s", res.Text)
			}
		})
	}
}

// 工具要能并发调用（-race 下跑）。
func TestConcurrentCalls(t *testing.T) {
	f := newFake(t, 200, vector(sample(`"job":"a"`, "1"), sample(`"job":"b"`, "2")))
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			if _, err := f.call(t, "prom_query", fmt.Sprintf(`{"query":"up","time":"-%dm"}`, i+1)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}
