package degrade_test

import (
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/degrade"
)

func newSet() *degrade.Set {
	return degrade.New("svc", slog.New(slog.DiscardHandler))
}

func TestSwitchDefaultsToAuto(t *testing.T) {
	t.Parallel()
	s := newSet()
	sw := s.Switch("cache", "queries skip the cache")
	if sw.Off() {
		t.Fatal("new switch is off, want auto")
	}
	var none *degrade.Switch
	if none.Off() {
		t.Fatal("nil switch reports off, want false")
	}
}

func TestApply(t *testing.T) {
	t.Parallel()
	s := newSet()
	cache := s.Switch("cache", "")
	archive := s.Switch("archive_read", "")
	replica := s.Switch("replica_read", "")
	if err := s.Apply(" cache=off , archive_read = off,replica_read=auto,"); err != nil {
		t.Fatal(err)
	}
	if !cache.Off() || !archive.Off() || replica.Off() {
		t.Fatalf("cache off=%v archive off=%v replica off=%v, want true true false", cache.Off(), archive.Off(), replica.Off())
	}
	if err := s.Apply(""); err != nil {
		t.Fatalf("empty spec: %v", err)
	}

	for _, bad := range []string{"cache", "cache=on", "nope=off"} {
		if err := s.Apply(bad); err == nil {
			t.Errorf("Apply(%q) = nil, want an error", bad)
		}
	}
	if err := s.Apply("nope=off"); !errors.Is(err, degrade.ErrUnknownSwitch) {
		t.Fatalf("Apply with unknown name = %v, want ErrUnknownSwitch", err)
	}
}

func TestSnapshotCombinesModeAndStatus(t *testing.T) {
	t.Parallel()
	s := newSet()
	manual := s.Switch("admission", "reject new orders")
	auto := s.Switch("cache", "skip the cache")
	var degraded atomic.Bool
	auto.Watch(func() degrade.Status {
		return degrade.Status{Degraded: degraded.Load(), Detail: "breaker"}
	})

	check := func(want []degrade.View) {
		t.Helper()
		got := s.Snapshot()
		if len(got) != len(want) {
			t.Fatalf("snapshot = %+v, want %+v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("snapshot[%d] = %+v, want %+v", i, got[i], want[i])
			}
		}
	}
	// 登记的顺序就是展示的顺序。
	check([]degrade.View{
		{Name: "admission", Mode: "auto", Desc: "reject new orders"},
		{Name: "cache", Mode: "auto", Detail: "breaker", Desc: "skip the cache"},
	})

	// 自动降级：mode 仍是 auto，degraded 为真。
	degraded.Store(true)
	check([]degrade.View{
		{Name: "admission", Mode: "auto", Desc: "reject new orders"},
		{Name: "cache", Mode: "auto", Degraded: true, Detail: "breaker", Desc: "skip the cache"},
	})

	// 人工关掉：依赖正常，degraded 也为真。
	degraded.Store(false)
	if err := s.SetMode("cache", degrade.ModeOff); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMode("admission", degrade.ModeOff); err != nil {
		t.Fatal(err)
	}
	if !manual.Off() {
		t.Fatal("admission not off after SetMode")
	}
	check([]degrade.View{
		{Name: "admission", Mode: "off", Degraded: true, Desc: "reject new orders"},
		{Name: "cache", Mode: "off", Degraded: true, Detail: "breaker", Desc: "skip the cache"},
	})
}

func TestRegisterTwicePanics(t *testing.T) {
	t.Parallel()
	s := newSet()
	s.Switch("cache", "")
	defer func() {
		if recover() == nil {
			t.Fatal("registering the same switch twice did not panic")
		}
	}()
	s.Switch("cache", "")
}

func TestHTTP(t *testing.T) {
	t.Parallel()
	s := newSet()
	sw := s.Switch("cache", "skip the cache")
	mux := http.NewServeMux()
	s.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	type body struct {
		Service  string         `json:"service"`
		Switches []degrade.View `json:"switches"`
		Error    string         `json:"error"`
	}
	do := func(method, path string) (int, body) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var b body
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatalf("%s %s: decode %q: %v", method, path, raw, err)
		}
		return resp.StatusCode, b
	}

	code, b := do(http.MethodGet, "/degrade")
	if code != http.StatusOK || b.Service != "svc" || len(b.Switches) != 1 || b.Switches[0].Mode != "auto" {
		t.Fatalf("GET /degrade = %d %+v", code, b)
	}

	code, b = do(http.MethodPut, "/degrade/cache?mode=off")
	if code != http.StatusOK || !sw.Off() || !b.Switches[0].Degraded || b.Switches[0].Mode != "off" {
		t.Fatalf("PUT mode=off = %d %+v, switch off = %v", code, b, sw.Off())
	}
	code, _ = do(http.MethodPut, "/degrade/cache?mode=auto")
	if code != http.StatusOK || sw.Off() {
		t.Fatalf("PUT mode=auto = %d, switch off = %v", code, sw.Off())
	}

	if code, b = do(http.MethodPut, "/degrade/nope?mode=off"); code != http.StatusNotFound || b.Error == "" {
		t.Fatalf("PUT unknown switch = %d %+v, want 404 with an error", code, b)
	}
	if code, b = do(http.MethodPut, "/degrade/cache?mode=on"); code != http.StatusBadRequest || b.Error == "" {
		t.Fatalf("PUT bad mode = %d %+v, want 400 with an error", code, b)
	}
	if code, b = do(http.MethodPut, "/degrade/cache"); code != http.StatusBadRequest || b.Error == "" {
		t.Fatalf("PUT without mode = %d %+v, want 400 with an error", code, b)
	}
}

// 开关在请求路径上被并发读，同时被人修改。带 -race 运行。
func TestConcurrentUse(t *testing.T) {
	t.Parallel()
	s := newSet()
	sw := s.Switch("cache", "")
	sw.Watch(func() degrade.Status { return degrade.Status{} })
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			for j := range 500 {
				_ = sw.Off()
				if i == 0 {
					mode := degrade.ModeAuto
					if j%2 == 0 {
						mode = degrade.ModeOff
					}
					if err := s.SetMode("cache", mode); err != nil {
						t.Error(err)
					}
				}
				_ = s.Snapshot()
			}
		})
	}
	wg.Wait()
}
