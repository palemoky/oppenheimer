package alertsink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

var reader = sdkmetric.NewManualReader()

// 包级的仪表建在全局 meter 的代理上，在这里装上 provider 之后它们才有实际的记录。
func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	os.Exit(m.Run())
}

// alertCount 返回 alertsink.alerts 里 status、severity 这一组标签的累计值。
func alertCount(t *testing.T, status, severity string) int64 {
	t.Helper()
	var total int64
	for _, dp := range collect[metricdata.Sum[int64]](t, "alertsink.alerts").DataPoints {
		s, _ := dp.Attributes.Value("status")
		v, _ := dp.Attributes.Value("severity")
		if s.AsString() == status && v.AsString() == severity {
			total += dp.Value
		}
	}
	return total
}

// watchdog 返回 gauge 的值，没有记录过时 ok 为 false。
func watchdog(t *testing.T) (v int64, ok bool) {
	t.Helper()
	g := collect[metricdata.Gauge[int64]](t, "alertsink.watchdog.last_received")
	if len(g.DataPoints) == 0 {
		return 0, false
	}
	return g.DataPoints[0].Value, true
}

func collect[T metricdata.Aggregation](t *testing.T, name string) T {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	var zero T
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				agg, ok := m.Data.(T)
				if !ok {
					t.Fatalf("metric %s has type %T", name, m.Data)
				}
				return agg
			}
		}
	}
	return zero
}

func newServer(t *testing.T, now func() time.Time) (*httptest.Server, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&syncWriter{w: &buf}, nil))
	srv := httptest.NewServer(NewHandler(logger, now).Mux())
	t.Cleanup(srv.Close)
	return srv, &buf
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func post(t *testing.T, srv *httptest.Server, body string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/alerts", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func alertJSON(name, status, severity string) string {
	return fmt.Sprintf(`{"status":%q,"labels":{"alertname":%q,"severity":%q,"instance":"x"},`+
		`"annotations":{"summary":"s of %s"},"startsAt":"2026-11-23T10:00:00Z","endsAt":"0001-01-01T00:00:00Z","fingerprint":"fp-%s"}`,
		status, name, severity, name, name)
}

func webhookJSON(alerts ...string) string {
	return `{"version":"4","receiver":"sink","status":"firing","groupKey":"{}:{alertname=\"x\"}","truncatedAlerts":0,` +
		`"groupLabels":{},"commonLabels":{},"extra":1,"alerts":[` + strings.Join(alerts, ",") + `]}`
}

func TestAcceptsAndCountsEveryAlert(t *testing.T) {
	srv, logs := newServer(t, nil)
	before := alertCount(t, "firing", "critical")
	beforeW := alertCount(t, "resolved", "warning")

	body := webhookJSON(alertJSON("A", "firing", "critical"), alertJSON("B", "firing", "critical"), alertJSON("C", "resolved", "warning"))
	if code := post(t, srv, body); code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if got := alertCount(t, "firing", "critical") - before; got != 2 {
		t.Errorf("firing/critical delta = %d, want 2", got)
	}
	if got := alertCount(t, "resolved", "warning") - beforeW; got != 1 {
		t.Errorf("resolved/warning delta = %d, want 1", got)
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d log lines, want 3:\n%s", len(lines), logs)
	}
	for _, want := range []string{`"alertname":"A"`, `"fingerprint":"fp-A"`, `"group_key"`, `"receiver":"sink"`, `"summary":"s of A"`, `"instance":"x"`, `"starts_at":"2026-11-23T10:00:00Z"`} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("log line lacks %s: %s", want, lines[0])
		}
	}
}

func TestWatchdogTimestamp(t *testing.T) {
	var mu sync.Mutex
	clock := time.Unix(1_800_000_000, 0)
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clock
	}
	srv, _ := newServer(t, now)

	// 别的告警不更新。
	prev, hadPrev := watchdog(t)
	if code := post(t, srv, webhookJSON(alertJSON("Other", "firing", "warning"))); code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if v, ok := watchdog(t); ok != hadPrev || v != prev {
		t.Errorf("non-Watchdog alert changed the gauge: %d,%v -> %d,%v", prev, hadPrev, v, ok)
	}

	// firing 的 Watchdog 更新成当前时刻。
	post(t, srv, webhookJSON(alertJSON("Watchdog", "firing", "none")))
	if v, ok := watchdog(t); !ok || v != 1_800_000_000 {
		t.Errorf("gauge = %d,%v, want 1800000000", v, ok)
	}

	// resolved 的 Watchdog 不更新。
	mu.Lock()
	clock = time.Unix(1_800_000_100, 0)
	mu.Unlock()
	post(t, srv, webhookJSON(alertJSON("Watchdog", "resolved", "none")))
	if v, _ := watchdog(t); v != 1_800_000_000 {
		t.Errorf("resolved Watchdog moved the gauge to %d", v)
	}

	// 再一次 firing 才前进。
	post(t, srv, webhookJSON(alertJSON("Watchdog", "firing", "none")))
	if v, _ := watchdog(t); v != 1_800_000_100 {
		t.Errorf("gauge = %d, want 1800000100", v)
	}
}

func TestUnknownValuesFoldToOther(t *testing.T) {
	srv, _ := newServer(t, nil)
	b1 := alertCount(t, "other", "other")
	b2 := alertCount(t, "firing", "none")
	body := webhookJSON(
		alertJSON("A", "weird", "page-me"),
		`{"status":"firing","labels":{"alertname":"NoSev"}}`,
	)
	if code := post(t, srv, body); code != http.StatusOK {
		t.Fatalf("status = %d", code)
	}
	if got := alertCount(t, "other", "other") - b1; got != 1 {
		t.Errorf("other/other delta = %d, want 1", got)
	}
	if got := alertCount(t, "firing", "none") - b2; got != 1 {
		t.Errorf("firing/none delta = %d, want 1", got)
	}
}

func TestRejects(t *testing.T) {
	srv, _ := newServer(t, nil)
	if code := post(t, srv, `{not json`); code != http.StatusBadRequest {
		t.Errorf("invalid JSON: status = %d, want 400", code)
	}
	big := webhookJSON(alertJSON("A", "firing", "critical")) + strings.Repeat(" ", MaxBodyBytes)
	if code := post(t, srv, big); code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body: status = %d, want 413", code)
	}
	resp := get(t, srv.URL+"/alerts")
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /alerts: status = %d, want 405", resp.StatusCode)
	}
}

func TestHealthz(t *testing.T) {
	srv, _ := newServer(t, nil)
	resp := get(t, srv.URL+"/healthz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestConcurrentPosts(t *testing.T) {
	srv, _ := newServer(t, nil)
	before := alertCount(t, "firing", "info")
	const n = 32
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			if code := post(t, srv, webhookJSON(alertJSON("A", "firing", "info"), alertJSON("Watchdog", "firing", "info"))); code != http.StatusOK {
				t.Errorf("status = %d", code)
			}
		})
	}
	wg.Wait()
	if got := alertCount(t, "firing", "info") - before; got != 2*n {
		t.Errorf("delta = %d, want %d", got, 2*n)
	}
}

func TestServeShutsDownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, ln, Config{Listen: ln.Addr().String(), Logger: slog.New(slog.DiscardHandler)})
	}()
	resp := get(t, "http://"+ln.Addr().String()+"/healthz")
	resp.Body.Close()
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Serve returned %v", err)
	}
}

func get(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	return resp
}
