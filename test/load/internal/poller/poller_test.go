package poller

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/test/load/internal/ledger"
)

func writeLedger(t *testing.T, path string, entries ...ledger.Entry) {
	t.Helper()
	var buf bytes.Buffer
	w := ledger.NewWriter(&buf)
	for _, e := range entries {
		if err := w.Write(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
}

func testConfig(url, path string) Config {
	return Config{
		BaseURL: url, OrdersFrom: path, Concurrency: 4, Interval: time.Millisecond, Duration: 300 * time.Millisecond,
		RequestTimeout: time.Second, Recent: 3, Refresh: 20 * time.Millisecond, Seed: 1,
		Logger: slog.New(slog.DiscardHandler),
	}
}

func TestPollQueriesRecentAcceptedOrdersWithUserHeader(t *testing.T) {
	var mu sync.Mutex
	got := map[string]string{} // 订单号 → 收到的 X-User-Id
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method %s", r.Method)
		}
		mu.Lock()
		got[r.URL.Path] = r.Header.Get("X-User-Id")
		mu.Unlock()
		_, _ = w.Write([]byte(`{"status":"creating"}`))
	}))
	t.Cleanup(srv.Close)

	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	// 文件一开始还不存在，之后才写入（模拟和 load run 同时开始）；只有 submit 成功的行算数，只保留最近 3 个。
	go func() {
		time.Sleep(50 * time.Millisecond)
		writeLedger(t, path,
			ledger.Entry{Op: ledger.OpSubmit, Outcome: ledger.OutcomeRejected, UserID: 9, IdempotencyKey: "a"},
			ledger.Entry{Op: ledger.OpSubmit, Outcome: ledger.OutcomeOK, OrderID: "101", UserID: 1, IdempotencyKey: "b"},
			ledger.Entry{Op: ledger.OpSubmit, Outcome: ledger.OutcomeOK, OrderID: "102", UserID: 2, IdempotencyKey: "c"},
			ledger.Entry{Op: ledger.OpSubmit, Outcome: ledger.OutcomeOK, OrderID: "103", UserID: 3, IdempotencyKey: "d"},
			ledger.Entry{Op: ledger.OpSubmit, Outcome: ledger.OutcomeOK, OrderID: "104", UserID: 4, IdempotencyKey: "e"},
		)
	}()

	rep, err := Run(t.Context(), testConfig(srv.URL, path))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Requests == 0 || rep.Statuses["200"] != rep.Requests {
		t.Fatalf("requests %d statuses %v", rep.Requests, rep.Statuses)
	}
	if rep.OrdersSeen != 4 {
		t.Errorf("orders seen %d, want 4", rep.OrdersSeen)
	}
	mu.Lock()
	defer mu.Unlock()
	if _, old := got["/api/v1/orders/101"]; old {
		t.Error("the oldest order should have been dropped from the recent window")
	}
	for id, user := range map[string]string{"102": "2", "103": "3", "104": "4"} {
		if got["/api/v1/orders/"+id] != user {
			t.Errorf("order %s queried with user %q, want %q (got %v)", id, got["/api/v1/orders/"+id], user, got)
		}
	}
}

func TestPollTextSourceAndPartialLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "orders.txt")
	if err := os.WriteFile(path, []byte("7 70\n8 80\n9 9"), 0o600); err != nil { // 最后一行不完整
		t.Fatal(err)
	}
	s := &source{path: path, max: 10}
	if err := s.refresh(); err != nil {
		t.Fatal(err)
	}
	if s.seen.Load() != 2 {
		t.Fatalf("seen %d, want 2 (partial last line must wait)", s.seen.Load())
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("0\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := s.refresh(); err != nil {
		t.Fatal(err)
	}
	if s.seen.Load() != 3 || s.ring[2] != (target{orderID: "9", userID: "90"}) {
		t.Fatalf("seen %d ring %v", s.seen.Load(), s.ring)
	}
}

func TestValidate(t *testing.T) {
	c := testConfig("http://127.0.0.1:1", "x")
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.OrdersFrom = ""
	if err := c.Validate(); err == nil {
		t.Error("want error without orders-from")
	}
}
