package gwapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDoSetsHeadersAndParsesRetryAfter(t *testing.T) {
	var got http.Header
	var host string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, host = r.Header.Clone(), r.Host
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":"RATE_LIMITED","message":"user"}}`))
	}))
	defer srv.Close()

	c := Client{HTTP: srv.Client(), Base: srv.URL, Host: "gw.example", LoadRun: "probe", Timeout: time.Second}
	res := c.Do(t.Context(), http.MethodPost, PathOrders, 42, "k-1", []byte(`{}`))
	if res.Err != nil || res.Status != http.StatusTooManyRequests {
		t.Fatalf("Do: status %d, err %v", res.Status, res.Err)
	}
	if res.RetryAfter != 3*time.Second || res.ErrorCode() != "RATE_LIMITED" {
		t.Errorf("retry after %v, code %q", res.RetryAfter, res.ErrorCode())
	}
	if host != "gw.example" || got.Get(HeaderUserID) != "42" || got.Get(HeaderIdemKey) != "k-1" ||
		got.Get(HeaderLoadRun) != "probe" || got.Get("Content-Type") != "application/json" {
		t.Errorf("host %q, headers %v", host, got)
	}

	// 不带 Host、LoadRun 时不发这两个头（load run 的请求和以前一样）。
	c = Client{HTTP: srv.Client(), Base: srv.URL, Timeout: time.Second}
	c.Do(t.Context(), http.MethodGet, PathOrders, 42, "", nil)
	if _, ok := got[HeaderLoadRun]; ok || got.Get(HeaderIdemKey) != "" {
		t.Errorf("unexpected headers %v", got)
	}
}

func TestDoTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer srv.Close()
	c := Client{HTTP: srv.Client(), Base: srv.URL, Timeout: 20 * time.Millisecond}
	if res := c.Do(t.Context(), http.MethodGet, PathOrders, 1, "", nil); !res.TimedOut() {
		t.Errorf("TimedOut() = false, err %v", res.Err)
	}
}

func TestParseRetryAfter(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"": 0, "0": 0, "-1": 0, "1": time.Second, " 5 ": 5 * time.Second, "Wed, 21 Oct 2015 07:28:00 GMT": 0,
		"999999": time.Hour,
	} {
		if got := parseRetryAfter(in); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}
