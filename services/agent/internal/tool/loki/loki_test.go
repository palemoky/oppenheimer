package loki

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

// fakeLoki 记下最后一次请求，按 handler 回应。
type fakeLoki struct {
	mu      sync.Mutex
	path    string
	query   url.Values
	handler http.HandlerFunc
}

func (f *fakeLoki) last() (string, url.Values) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.path, f.query
}

func newTools(t *testing.T, h http.HandlerFunc) (query, labels tool.Tool, f *fakeLoki) {
	t.Helper()
	f = &fakeLoki{handler: h}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.path, f.query = r.URL.Path, r.URL.Query()
		f.mu.Unlock()
		f.handler(w, r)
	}))
	t.Cleanup(srv.Close)
	tools := New(Config{BaseURL: srv.URL + "/", HTTP: srv.Client(), Now: func() time.Time { return now }})
	return tools[0], tools[1], f
}

func reply(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

// streamsBody 拼一个日志查询的响应。每条流是 标签 JSON 和若干 [纳秒, 行]。
type testStream struct {
	labels string
	values [][2]string
}

func streamsBody(streams ...testStream) string {
	var parts []string
	for _, s := range streams {
		var vals []string
		for _, v := range s.values {
			vals = append(vals, fmt.Sprintf("[%q,%s]", v[0], strconv.Quote(v[1])))
		}
		parts = append(parts, fmt.Sprintf(`{"stream":%s,"values":[%s]}`, s.labels, strings.Join(vals, ",")))
	}
	return `{"status":"success","data":{"resultType":"streams","result":[` + strings.Join(parts, ",") + `],"stats":{}}}`
}

// ns 是 now 之前 sec 秒（可以带小数）的纳秒时间戳。
func ns(sec float64) string {
	return strconv.FormatInt(now.Add(-time.Duration(sec*float64(time.Second))).UnixNano(), 10)
}

func call(t *testing.T, tl tool.Tool, input string) (tool.Result, error) {
	t.Helper()
	return tl.Call(t.Context(), jsontext.Value(input))
}

func mustCall(t *testing.T, tl tool.Tool, input string) string {
	t.Helper()
	res, err := call(t, tl, input)
	if err != nil {
		t.Fatalf("call %s: %v", input, err)
	}
	if len(res.Raw) == 0 {
		t.Error("Raw is empty")
	}
	return res.Text
}

func TestQueryParams(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  map[string]string
		text  string // Text 里要有的内容
	}{
		{
			name:  "defaults",
			input: `{"logql":"{app=\"order\"}"}`,
			want: map[string]string{
				"query": `{app="order"}`, "limit": "100", "direction": "backward",
				"start": strconv.FormatInt(now.Add(-15*time.Minute).UnixNano(), 10),
				"end":   strconv.FormatInt(now.UnixNano(), 10),
				"step":  "15",
			},
		},
		{
			name:  "explicit range, forward, limit clamped",
			input: `{"logql":"{app=\"order\"}","start":"2026-11-24T01:00:00Z","end":"-30m","limit":1000,"direction":"forward"}`,
			want: map[string]string{
				"limit": "300", "direction": "forward",
				"start": strconv.FormatInt(time.Date(2026, 11, 24, 1, 0, 0, 0, time.UTC).UnixNano(), 10),
				"end":   strconv.FormatInt(now.Add(-30*time.Minute).UnixNano(), 10),
				"step":  "90",
			},
			text: "limit 1000 was lowered to the maximum of 300",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, _, f := newTools(t, reply(streamsBody(testStream{`{"app":"order"}`, [][2]string{{ns(1), "x"}}})))
			text := mustCall(t, q, tc.input)
			path, got := f.last()
			if path != "/loki/api/v1/query_range" {
				t.Errorf("path = %s", path)
			}
			for k, v := range tc.want {
				if got.Get(k) != v {
					t.Errorf("%s = %q, want %q", k, got.Get(k), v)
				}
			}
			if !strings.Contains(text, tc.text) {
				t.Errorf("text does not contain %q:\n%s", tc.text, text)
			}
		})
	}
}

func TestQueryBadArguments(t *testing.T) {
	q, _, f := newTools(t, reply(streamsBody()))
	cases := []struct{ input, want string }{
		{`{"logql":"{app=\"a\"}","lines":5}`, "invalid arguments"},
		{`{"logql":"  "}`, "logql is required"},
		{`{"logql":"{a=\"b\"}","direction":"up"}`, `direction "up"`},
		{`{"logql":"{a=\"b\"}","limit":-1}`, "limit -1"},
		{`{"logql":"{a=\"b\"}","start":"yesterday"}`, "start: time"},
		{`{"logql":"{a=\"b\"}","start":"-5m","end":"-10m"}`, "must be before end"},
	}
	for _, tc := range cases {
		_, err := call(t, q, tc.input)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to contain %q", tc.input, err, tc.want)
		}
	}
	if path, _ := f.last(); path != "" {
		t.Errorf("bad arguments must not reach Loki, got a request to %s", path)
	}
}

func TestStreamMergeOrderAndLabels(t *testing.T) {
	body := streamsBody(
		testStream{`{"app":"order","namespace":"prod","pod":"order-1"}`, [][2]string{{ns(1), "a1"}, {ns(3), "a3"}}},
		testStream{`{"app":"order","namespace":"prod","pod":"order-2"}`, [][2]string{{ns(2), "b2"}, {ns(4.5), "b4"}}},
	)
	cases := []struct {
		name, direction string
		want            []string
	}{
		{"backward", "backward", []string{
			"02:59:59.000 [pod=order-1] a1",
			"02:59:58.000 [pod=order-2] b2",
			"02:59:57.000 [pod=order-1] a3",
			"02:59:55.500 [pod=order-2] b4",
		}},
		{"forward", "forward", []string{
			"02:59:55.500 [pod=order-2] b4",
			"02:59:57.000 [pod=order-1] a3",
			"02:59:58.000 [pod=order-2] b2",
			"02:59:59.000 [pod=order-1] a1",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, _, _ := newTools(t, reply(body))
			text := mustCall(t, q, `{"logql":"{app=\"order\"}","direction":"`+tc.direction+`"}`)
			if !strings.Contains(text, strings.Join(tc.want, "\n")) {
				t.Errorf("lines not in order, want\n%s\ngot\n%s", strings.Join(tc.want, "\n"), text)
			}
			for _, s := range []string{
				`Labels on every line: {app="order", namespace="prod"}`,
				"Loki returned 4 log entries from 2 stream(s)",
				"Showing all 4 lines",
				"Times are UTC on 2026-11-24",
			} {
				if !strings.Contains(text, s) {
					t.Errorf("text does not contain %q:\n%s", s, text)
				}
			}
			if strings.Contains(text, "limit reached") {
				t.Error("limit notice shown below the limit")
			}
		})
	}
}

func TestSingleStreamHasNoTag(t *testing.T) {
	q, _, _ := newTools(t, reply(streamsBody(testStream{`{"app":"order"}`, [][2]string{{ns(1), "hello"}}})))
	text := mustCall(t, q, `{"logql":"{app=\"order\"}"}`)
	if !strings.Contains(text, "\n02:59:59.000 hello") || strings.Contains(text, "prefixed") {
		t.Errorf("text:\n%s", text)
	}
}

func TestCompactLine(t *testing.T) {
	cases := []struct {
		name, line, want string
	}{
		{
			name: "slog line",
			line: `{"time":"2026-11-24T02:59:59Z","level":"ERROR","msg":"reserve stock failed","error":"rpc error: code = Unavailable","trace_id":"abc123","order_id":42,"sku":"S-1"}`,
			want: `level=ERROR msg="reserve stock failed" error="rpc error: code = Unavailable" trace_id=abc123 order_id=42 sku=S-1`,
		},
		{
			name: "at most 6 other scalar keys, objects dropped",
			line: `{"ts":1,"msg":"m","h":8,"g":7,"f":6,"e":5,"d":4,"c":3,"b":2,"a":{"x":1}}`,
			want: `msg=m b=2 c=3 d=4 e=5 f=6 g=7 (+2 more fields)`,
		},
		{
			name: "err key and object error kept compact",
			line: `{"level":"WARN","err":{"code": 5},"empty":"","ok":true,"n":null}`,
			want: `level=WARN err={"code":5} empty="" n=null ok=true`,
		},
		{"plain text", `GET /health 200`, `GET /health 200`},
		{"broken json", `{"level":"INFO"`, `{"level":"INFO"`},
		{"json array", `[1,2]`, `[1,2]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := compactLine(tc.line)
			if got != tc.want {
				t.Errorf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestJSONExtractedLabelsAreNotTagged(t *testing.T) {
	// `| json` 把字段提成标签，每行一条流；和行里字段同名的标签不该再出现在流标记里。
	body := streamsBody(
		testStream{`{"app":"order","level":"ERROR","msg":"a","pod":"p1"}`, [][2]string{{ns(1), `{"level":"ERROR","msg":"a"}`}}},
		testStream{`{"app":"order","level":"ERROR","msg":"b","pod":"p2"}`, [][2]string{{ns(2), `{"level":"ERROR","msg":"b"}`}}},
	)
	q, _, _ := newTools(t, reply(body))
	text := mustCall(t, q, `{"logql":"{app=\"order\"} | json"}`)
	want := "02:59:59.000 [pod=p1] level=ERROR msg=a\n02:59:58.000 [pod=p2] level=ERROR msg=b"
	if !strings.Contains(text, want) {
		t.Errorf("want\n%s\ngot\n%s", want, text)
	}
}

func TestTruncateAndCollapse(t *testing.T) {
	long := strings.Repeat("x", 800)
	body := streamsBody(testStream{`{"app":"order"}`, [][2]string{
		{ns(1), "same"}, {ns(2), "same"}, {ns(3), "same"}, {ns(4), long}, {ns(5), "same"},
	}})
	q, _, _ := newTools(t, reply(body))
	text := mustCall(t, q, `{"logql":"{app=\"order\"}"}`)
	for _, s := range []string{
		"02:59:59.000 same  [repeated 3 times]\n",
		"\n02:59:55.000 same",
		"Consecutive identical lines are collapsed: 5 entries became 3 lines",
		"… [+313 bytes]", // 500 字节里时间和空格占 13 个，x 留了 487 个
	} {
		if !strings.Contains(text, s+"") {
			t.Errorf("text does not contain %q:\n%s", s, text)
		}
	}
	if strings.Contains(text, "02:59:58.000") {
		t.Error("the repeated lines after the first must not be printed")
	}
}

func TestLimitReached(t *testing.T) {
	var vals [][2]string
	for i := range 5 {
		vals = append(vals, [2]string{ns(float64(i + 1)), fmt.Sprintf("line %d", i)})
	}
	q, _, _ := newTools(t, reply(streamsBody(testStream{`{"app":"order"}`, vals})))
	text := mustCall(t, q, `{"logql":"{app=\"order\"}","limit":5}`)
	if !strings.Contains(text, "limit reached: Loki returned 5 entries") || !strings.Contains(text, "shorter range") {
		t.Errorf("text:\n%s", text)
	}
}

func TestOutputSizeLimit(t *testing.T) {
	var vals [][2]string
	for i := range 300 {
		vals = append(vals, [2]string{ns(float64(i + 1)), fmt.Sprintf("%03d %s", i, strings.Repeat("y", 400))})
	}
	q, _, _ := newTools(t, reply(streamsBody(testStream{`{"app":"order"}`, vals})))
	text := mustCall(t, q, `{"logql":"{app=\"order\"}","limit":300}`)
	if len(text) > tool.MaxResultBytes {
		t.Errorf("text is %d bytes, over %d", len(text), tool.MaxResultBytes)
	}
	if !strings.Contains(text, "lines not shown because of the output size limit") || !strings.Contains(text, "Showing the first ") {
		t.Errorf("size notice missing:\n%s", text[:300])
	}
}

func TestEmptyResult(t *testing.T) {
	q, _, _ := newTools(t, reply(streamsBody()))
	text := mustCall(t, q, `{"logql":"{app=\"nope\"}"}`)
	if !strings.Contains(text, "No log lines matched") || !strings.Contains(text, "loki_labels") {
		t.Errorf("text: %s", text)
	}
}

func TestMatrix(t *testing.T) {
	sec := func(d time.Duration) string { return strconv.FormatInt(now.Add(-d).Unix(), 10) }
	var series []string
	for i := range 25 {
		var pts []string
		for j := range 70 {
			pts = append(pts, fmt.Sprintf(`[%s,"%d.123456"]`, sec(time.Duration(70-j)*15*time.Second), i*100+j))
		}
		series = append(series, fmt.Sprintf(`{"metric":{"app":"order","pod":"p%02d"},"values":[%s]}`, i, strings.Join(pts, ",")))
	}
	body := `{"status":"success","data":{"resultType":"matrix","result":[` + strings.Join(series, ",") + `]}}`
	q, _, _ := newTools(t, reply(body))
	text := mustCall(t, q, `{"logql":"sum by (pod) (count_over_time({app=\"order\"}[1m]))"}`)
	for _, s := range []string{
		"returned 25 series",
		`Labels on every series: {app="order"}`,
		`{pod="p24"} min=2400 max=2469 last=2469 (last 60 of 70 points)`,
		"[5 of 25 series not shown]",
		"topk",
		"02:59:45=2469\n",
	} {
		if !strings.Contains(text, s) {
			t.Errorf("text does not contain %q", s)
		}
	}
	// 留峰值最高的 20 条：p05 到 p24，p04 及以下不在。
	if strings.Contains(text, `{pod="p04"}`) || !strings.Contains(text, `{pod="p05"}`) {
		t.Error("wrong series kept")
	}
	if strings.Count(text, "\n{pod=") != 20 {
		t.Errorf("%d series shown, want 20", strings.Count(text, "\n{pod="))
	}
}

func TestEmptyMatrix(t *testing.T) {
	q, _, _ := newTools(t, reply(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	text := mustCall(t, q, `{"logql":"rate({app=\"nope\"}[1m])"}`)
	if !strings.Contains(text, "returned no series") || !strings.Contains(text, "loki_labels") {
		t.Errorf("text: %s", text)
	}
}

func TestErrors(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		want    string
	}{
		{"bad logql", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "parse error at line 1, col 7: syntax error: unexpected IDENTIFIER", http.StatusBadRequest)
		}, "rejected the request (HTTP 400): parse error at line 1, col 7: syntax error: unexpected IDENTIFIER; fix"},
		{"server error", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "too many outstanding requests", http.StatusTooManyRequests)
		}, "HTTP 429: too many outstanding requests"},
		{"not success", reply(`{"status":"error","data":{}}`), `status "error"`},
		{"bad json", reply(`{"status":`), "could not be parsed"},
		{"vector", reply(`{"status":"success","data":{"resultType":"vector","result":[]}}`), `result type "vector"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, _, _ := newTools(t, tc.handler)
			_, err := call(t, q, `{"logql":"{app=\"order\"}"}`)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want it to contain %q", err, tc.want)
			}
			if err != nil && strings.Contains(err.Error(), "127.0.0.1") {
				t.Errorf("error leaks the address: %v", err)
			}
		})
	}
}

func TestTransportErrorHidesAddress(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	addr := srv.URL
	srv.Close()
	q := New(Config{BaseURL: addr, Now: func() time.Time { return now }})[0]
	_, err := call(t, q, `{"logql":"{app=\"order\"}"}`)
	if err == nil || !strings.Contains(err.Error(), "cannot reach the Loki server") || strings.Contains(err.Error(), "127.0.0.1") {
		t.Errorf("err = %v", err)
	}
}

func TestBodyTooLarge(t *testing.T) {
	big := streamsBody(testStream{`{"app":"order"}`, [][2]string{{ns(1), strings.Repeat("z", maxBody)}}})
	q, _, _ := newTools(t, reply(big))
	_, err := call(t, q, `{"logql":"{app=\"order\"}"}`)
	if err == nil || !strings.Contains(err.Error(), "larger than 8 MiB") {
		t.Errorf("err = %v", err)
	}
}

func TestDeadline(t *testing.T) {
	q, _, _ := newTools(t, func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := q.Call(ctx, jsontext.Value(`{"logql":"{app=\"order\"}"}`))
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "did not answer in time") {
		t.Errorf("err = %v", err)
	}
}

func TestLabels(t *testing.T) {
	var many []string
	for i := range 250 {
		many = append(many, fmt.Sprintf("%q", fmt.Sprintf("v%03d", 249-i)))
	}
	cases := []struct {
		name, input, body, path string
		want, notWant           []string
	}{
		{
			name: "names", input: `{}`, path: "/loki/api/v1/labels",
			body: `{"status":"success","data":["pod","app","namespace"]}`,
			want: []string{"3 label names between 2026-11-24T02:00:00Z and 2026-11-24T03:00:00Z:\napp\nnamespace\npod"},
		},
		{
			name: "values capped", input: `{"label":"pod","start":"-6h"}`, path: "/loki/api/v1/label/pod/values",
			body:    `{"status":"success","data":[` + strings.Join(many, ",") + `]}`,
			want:    []string{`250 values of label "pod"`, "showing the first 200", "\nv000\n", "\nv199"},
			notWant: []string{"v200"},
		},
		{
			name: "no values", input: `{"label":"app"}`, path: "/loki/api/v1/label/app/values",
			body: `{"status":"success","data":[]}`,
			want: []string{`No values of label "app" found`, "without \"label\""},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, l, f := newTools(t, reply(tc.body))
			text := mustCall(t, l, tc.input)
			path, params := f.last()
			if path != tc.path {
				t.Errorf("path = %s, want %s", path, tc.path)
			}
			if params.Get("end") != strconv.FormatInt(now.UnixNano(), 10) || params.Get("start") == "" {
				t.Errorf("params = %v", params)
			}
			for _, s := range tc.want {
				if !strings.Contains(text, s) {
					t.Errorf("text does not contain %q:\n%s", s, text)
				}
			}
			for _, s := range tc.notWant {
				if strings.Contains(text, s) {
					t.Errorf("text contains %q", s)
				}
			}
		})
	}
}

func TestLabelsBadArguments(t *testing.T) {
	_, l, f := newTools(t, reply(`{"status":"success","data":[]}`))
	for _, tc := range []struct{ input, want string }{
		{`{"label":"../x"}`, "not a valid label name"},
		{`{"name":"app"}`, "invalid arguments"},
	} {
		_, err := call(t, l, tc.input)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v", tc.input, err)
		}
	}
	if path, _ := f.last(); path != "" {
		t.Errorf("bad arguments must not reach Loki, got %s", path)
	}
}

func TestDefs(t *testing.T) {
	tools := New(Config{BaseURL: "http://loki"})
	if tools[0].Def().Name != QueryName || tools[1].Def().Name != LabelsName {
		t.Fatal("unexpected tool names")
	}
	if _, err := tool.NewRegistry(tools...); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentCalls(t *testing.T) {
	q, _, _ := newTools(t, reply(streamsBody(testStream{`{"app":"order"}`, [][2]string{{ns(1), `{"level":"INFO","msg":"ok"}`}}})))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := q.Call(t.Context(), jsontext.Value(`{"logql":"{app=\"order\"}"}`)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}
