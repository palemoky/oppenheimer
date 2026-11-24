package prom

import (
	"encoding/json/jsontext"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

func hiddenCall(t *testing.T, body, name, input string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	for _, tl := range New(Config{BaseURL: srv.URL, Now: func() time.Time { return now }, Hidden: tool.NewHidden("chaos-mesh", "")}) {
		if tl.Def().Name == name {
			res, err := tl.Call(t.Context(), jsontext.Value(input))
			if err != nil {
				t.Fatalf("%s %s: %v", name, input, err)
			}
			return res.Text
		}
	}
	t.Fatalf("no tool %s", name)
	return ""
}

func TestHiddenNamespaceSeriesDropped(t *testing.T) {
	series := func(ns, pod string) string {
		return `{"metric":{"__name__":"container_memory_working_set_bytes","namespace":"` + ns + `","pod":"` + pod + `"},"value":[1795489200,"1048576"]}`
	}
	body := `{"status":"success","data":{"resultType":"vector","result":[` + series("chaos-mesh", "chaos-daemon-x") + `,` + series("shop", "order-1") + `]}}`
	text := hiddenCall(t, body, "prom_query", `{"query":"container_memory_working_set_bytes"}`)
	if strings.Contains(text, "chaos") || !strings.Contains(text, "order-1") {
		t.Errorf("want only the shop series:\n%s", text)
	}

	// 只剩隐藏的序列时，和查不到任何东西的回答一样。
	only := `{"status":"success","data":{"resultType":"vector","result":[` + series("chaos-mesh", "chaos-daemon-x") + `]}}`
	none := `{"status":"success","data":{"resultType":"vector","result":[]}}`
	q := `{"query":"container_memory_working_set_bytes{namespace=\"chaos-mesh\"}"}`
	if got, want := hiddenCall(t, only, "prom_query", q), hiddenCall(t, none, "prom_query", q); got != want {
		t.Errorf("hidden-only result:\n%s\nempty result:\n%s", got, want)
	}

	matrix := `{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"k8s_namespace_name":"chaos-mesh"},"values":[[1795489200,"1"]]},` +
		`{"metric":{"k8s_namespace_name":"shop"},"values":[[1795489200,"2"]]}]}}`
	text = hiddenCall(t, matrix, "prom_query_range", `{"query":"x","start":"-5m"}`)
	if strings.Contains(text, "chaos") || !strings.Contains(text, "shop") {
		t.Errorf("want only the shop series:\n%s", text)
	}
}

func TestHiddenNamespaceAlertsDropped(t *testing.T) {
	alert := func(ns string) string {
		return `{"labels":{"alertname":"ContainerMemoryNearLimit","namespace":"` + ns + `","severity":"warning"},"annotations":{},"state":"firing","activeAt":"2026-11-24T02:00:00Z","value":"0.95"}`
	}
	body := `{"status":"success","data":{"alerts":[` + alert("chaos-mesh") + `,` + alert("shop") + `]}}`
	text := hiddenCall(t, body, "prom_alerts", `{}`)
	if strings.Contains(text, "chaos") || !strings.Contains(text, "1 alerts (1 firing, 0 pending)") {
		t.Errorf("want one alert, the shop one:\n%s", text)
	}

	rules := `{"status":"success","data":{"groups":[{"name":"g","file":"f","rules":[{"type":"alerting","name":"ContainerMemoryNearLimit","query":"x > 0.9","state":"firing","health":"ok","alerts":[` +
		alert("chaos-mesh") + `,` + alert("shop") + `]}]}]}}`
	text = hiddenCall(t, rules, "prom_rule", `{"alertname":"ContainerMemoryNearLimit"}`)
	if strings.Contains(text, "chaos") || !strings.Contains(text, "active alerts: 1 (1 firing, 0 pending)") {
		t.Errorf("want the rule with only the shop alert:\n%s", text)
	}
}
