package ledger

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConcurrentWriteAndReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	const goroutines, perG = 16, 200
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range perG {
				e := Entry{
					Time:           time.Now(),
					Op:             OpSubmit,
					OrderID:        "9007199254740993",
					UserID:         9007199254740993,
					SKUID:          int64(g),
					Quantity:       int32(i),
					IdempotencyKey: fmt.Sprintf("k-%d-%d", g, i),
					Attempt:        1,
					HTTPStatus:     202,
					OrderStatus:    "creating",
					Outcome:        OutcomeOK,
				}
				if err := w.Write(e); err != nil {
					t.Error(err)
					return
				}
				if i%50 == 0 {
					if err := w.Flush(); err != nil {
						t.Error(err)
					}
				}
			}
		})
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := w.Write(Entry{}); err == nil {
		t.Error("write after close succeeded")
	}

	entries, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != goroutines*perG {
		t.Fatalf("read %d entries, want %d", len(entries), goroutines*perG)
	}
	seen := map[string]bool{}
	for _, e := range entries {
		if e.UserID != 9007199254740993 || e.Op != OpSubmit || e.Outcome != OutcomeOK || e.HTTPStatus != 202 {
			t.Fatalf("entry %+v", e)
		}
		seen[e.IdempotencyKey] = true
	}
	if len(seen) != goroutines*perG {
		t.Errorf("distinct keys = %d", len(seen))
	}
}

func TestEncoding(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	ts := time.Date(2026, 11, 12, 1, 2, 3, 4, time.UTC)
	e := Entry{
		Time: ts, Op: OpPollFinal, Stage: StageLanding, UserID: 9007199254740993, SKUID: 1001, Quantity: 2,
		IdempotencyKey: "k", OrderStatus: "pending_payment", Outcome: OutcomeTimeout, LatencyMicros: 1500,
	}
	if err := w.Write(e); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	line := buf.String()
	for _, want := range []string{
		`"ts":"2026-11-12T01:02:03.000000004Z"`, `"user_id":"9007199254740993"`, `"sku_id":"1001"`,
		`"order_id":""`, `"stage":"landing"`, `"http_status":0`, `"outcome":"timeout"`, `"latency_us":1500`,
	} {
		if !strings.Contains(line, want) {
			t.Errorf("line %s missing %s", line, want)
		}
	}
	if strings.Contains(line, `"attempt"`) || strings.Contains(line, `"error"`) {
		t.Errorf("empty optional fields present: %s", line)
	}
	if !strings.HasSuffix(line, "}\n") || strings.Count(line, "\n") != 1 {
		t.Errorf("not a single JSON line: %q", line)
	}
	var got []Entry
	for e, err := range Read(&buf) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, e)
	}
	if len(got) != 1 || got[0] != e {
		t.Errorf("round trip = %+v, want %+v", got, e)
	}
}

func TestReadTruncatedLastLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.jsonl")
	content := `{"ts":"2026-11-12T00:00:00Z","op":"submit","order_id":"1","user_id":"1","sku_id":"1","quantity":1,` +
		`"idempotency_key":"a","http_status":202,"order_status":"creating","outcome":"ok","latency_us":1}` + "\n\n" +
		`{"ts":"2026-11-12T00:00:01Z","op":"pay","order_`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	var ok, bad int
	for _, err := range Read(strings.NewReader(content)) {
		if err != nil {
			bad++
			if !strings.Contains(err.Error(), "line 3") {
				t.Errorf("error without line number: %v", err)
			}
			continue
		}
		ok++
	}
	if ok != 1 || bad != 1 {
		t.Errorf("ok %d bad %d, want 1 and 1", ok, bad)
	}
	entries, err := ReadFile(path)
	if err == nil || len(entries) != 1 {
		t.Errorf("ReadFile = %d entries, err %v; want 1 entry and an error", len(entries), err)
	}
}
