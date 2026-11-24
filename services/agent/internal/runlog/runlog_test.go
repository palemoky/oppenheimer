package runlog

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func open(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func TestRunRoundTrip(t *testing.T) {
	s, dir := open(t)
	started := time.Date(2026, 11, 24, 3, 15, 2, 0, time.UTC)
	id := NewID(started)
	if !strings.HasPrefix(id, "20261124T031502Z-") || len(id) != len("20261124T031502Z-")+4 {
		t.Fatalf("id = %q", id)
	}
	run, err := s.Create(Meta{ID: id, Trigger: "alert", Question: "q", Model: "m", StartedAt: started})
	if err != nil {
		t.Fatal(err)
	}

	// 工具是并发执行的：事件并发写，每行仍然完整，序号不重不漏。
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() { run.Event("tool_call", map[string]int{"n": i}) })
	}
	wg.Wait()
	run.Raw("s1.json", []byte(`{"data":1}`))
	run.Raw("s2.json", nil) // 空的不写
	run.WriteFile("report.md", []byte("# r"))

	// 没有 summary.json 之前，列表里它是没有正常结束的。
	runs, err := s.List()
	if err != nil || len(runs) != 1 || runs[0].Outcome != OutcomeIncomplete || runs[0].ID != id {
		t.Fatalf("runs = %+v, err = %v", runs, err)
	}

	run.Finish(Summary{ID: id, Trigger: "alert", Outcome: "reported", Turns: 3, CostUSD: 0.5, StartedAt: started})
	run.Event("late", nil) // Finish 之后的事件被忽略，不能 panic

	events, err := s.Events(id)
	if err != nil || len(events) != 50 {
		t.Fatalf("events = %d, err = %v", len(events), err)
	}
	for i, ev := range events {
		if ev.Seq != i+1 || ev.Kind != "tool_call" {
			t.Fatalf("event %d = %+v", i, ev)
		}
	}
	if b, err := s.ReadFile(id, "raw/s1.json"); err != nil || string(b) != `{"data":1}` {
		t.Errorf("raw = %q, err = %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(dir, id, "raw", "s2.json")); !os.IsNotExist(err) {
		t.Errorf("empty raw result was written: %v", err)
	}
	runs, _ = s.List()
	if len(runs) != 1 || runs[0].Outcome != "reported" || runs[0].CostUSD != 0.5 {
		t.Errorf("runs = %+v", runs)
	}
}

func TestEventsDropsTornLastLine(t *testing.T) {
	s, dir := open(t)
	run, err := s.Create(Meta{ID: "20261124T000000Z-aaaa"})
	if err != nil {
		t.Fatal(err)
	}
	run.Event("a", 1)
	run.Finish(Summary{ID: run.ID()})
	f, err := os.OpenFile(filepath.Join(dir, run.ID(), eventsFile), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"seq":2,"kind":"b","da`)
	_ = f.Close()

	events, err := s.Events(run.ID())
	if err != nil || len(events) != 1 || events[0].Kind != "a" {
		t.Fatalf("events = %+v, err = %v", events, err)
	}
}

func TestReadFileCannotEscape(t *testing.T) {
	s, dir := open(t)
	if err := os.WriteFile(filepath.Join(filepath.Dir(dir), "secret"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][2]string{{"..", "secret"}, {"x", "../../secret"}, {"/etc", "passwd"}} {
		if b, err := s.ReadFile(c[0], c[1]); err == nil {
			t.Errorf("ReadFile(%q, %q) = %q, want an error", c[0], c[1], b)
		}
	}
}

func TestPrune(t *testing.T) {
	s, dir := open(t)
	now := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	mk := func(age time.Duration, size int) string {
		id := NewID(now.Add(-age))
		run, err := s.Create(Meta{ID: id})
		if err != nil {
			t.Fatal(err)
		}
		run.WriteFile("blob", make([]byte, size))
		run.Finish(Summary{ID: id})
		return id
	}
	old := mk(40*24*time.Hour, 10)
	big := mk(3*time.Hour, 5000)
	mid := mk(2*time.Hour, 5000)
	newest := mk(time.Hour, 5000)
	// 不是我们建的目录不碰。
	if err := os.Mkdir(filepath.Join(dir, "lost+found"), 0o750); err != nil {
		t.Fatal(err)
	}

	removed, err := s.Prune(now, 30*24*time.Hour, 12000)
	if err != nil || removed != 2 {
		t.Fatalf("removed = %d, err = %v", removed, err)
	}
	for id, want := range map[string]bool{old: false, big: false, mid: true, newest: true, "lost+found": true} {
		_, err := os.Stat(filepath.Join(dir, id))
		if (err == nil) != want {
			t.Errorf("%s: exists = %v, want %v", id, err == nil, want)
		}
	}
}
