package loki

import (
	"encoding/json/jsontext"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

func hiddenTools(t *testing.T, body string) (query, labels tool.Tool) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	tools := New(Config{BaseURL: srv.URL, HTTP: srv.Client(), Now: func() time.Time { return now }, Hidden: tool.NewHidden("chaos-mesh", "")})
	return tools[0], tools[1]
}

func TestHiddenNamespaceStreamsDropped(t *testing.T) {
	ts := now.Add(-time.Minute).UnixNano()
	line := func(ns, msg string) string {
		return `{"stream":{"k8s_namespace_name":"` + ns + `","k8s_pod_name":"p-` + ns + `"},"values":[["` + strconv.FormatInt(ts, 10) + `","` + msg + `"]]}`
	}
	body := `{"status":"success","data":{"resultType":"streams","result":[` +
		line("chaos-mesh", "applying netem delay 300ms") + `,` + line("shop", "order placed") + `]}}`
	q, _ := hiddenTools(t, body)
	res, err := q.Call(t.Context(), jsontext.Value(`{"logql":"{k8s_namespace_name=~\".+\"}"}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Text, "chaos") || strings.Contains(res.Text, "netem") {
		t.Errorf("hidden stream leaked:\n%s", res.Text)
	}
	if !strings.Contains(res.Text, "order placed") {
		t.Errorf("the other stream is gone:\n%s", res.Text)
	}
}

func TestHiddenNamespaceSeriesDropped(t *testing.T) {
	body := `{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"k8s_namespace_name":"chaos-mesh"},"values":[[1795489200,"7"]]},` +
		`{"metric":{"k8s_namespace_name":"shop"},"values":[[1795489200,"3"]]}]}}`
	q, _ := hiddenTools(t, body)
	res, err := q.Call(t.Context(), jsontext.Value(`{"logql":"sum by (k8s_namespace_name) (count_over_time({k8s_namespace_name=~\".+\"}[1m]))"}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Text, "chaos") || !strings.Contains(res.Text, "shop") {
		t.Errorf("want only the shop series:\n%s", res.Text)
	}
}

func TestHiddenNamespaceNotAmongLabelValues(t *testing.T) {
	_, l := hiddenTools(t, `{"status":"success","data":["chaos-mesh","monitoring","shop"]}`)
	res, err := l.Call(t.Context(), jsontext.Value(`{"label":"k8s_namespace_name"}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Text, "chaos") || !strings.Contains(res.Text, "monitoring") || !strings.Contains(res.Text, "shop") {
		t.Errorf("want monitoring and shop only:\n%s", res.Text)
	}
	// 别的标签的值不动：只有装着命名空间的标签才按名单筛。
	res, err = l.Call(t.Context(), jsontext.Value(`{"label":"app"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Text, "chaos-mesh") {
		t.Errorf("values of another label must not be filtered:\n%s", res.Text)
	}
}
