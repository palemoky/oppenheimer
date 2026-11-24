package tool

import (
	"encoding/json/jsontext"
	"strings"
	"testing"
	"time"
)

func TestDecode(t *testing.T) {
	var in struct {
		Query string `json:"query"`
	}
	if err := Decode(jsontext.Value(`{"query":"up"}`), &in); err != nil || in.Query != "up" {
		t.Fatalf("in = %+v, err = %v", in, err)
	}
	if err := Decode(nil, &in); err != nil {
		t.Errorf("empty input: %v", err)
	}
	if err := Decode(jsontext.Value(`{"qurey":"up"}`), &in); err == nil || !strings.Contains(err.Error(), "invalid arguments") {
		t.Errorf("unknown field: err = %v", err)
	}
}

func TestParseTime(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	cases := map[string]time.Time{
		"":                     now,
		"now":                  now,
		"-30m":                 now.Add(-30 * time.Minute),
		"2026-11-24T03:00:00Z": time.Date(2026, 11, 24, 3, 0, 0, 0, time.UTC),
	}
	cases["-2h"] = now.Add(-2 * time.Hour)
	cases["-7d"] = now.Add(-7 * 24 * time.Hour)
	cases["\t-90s\n"] = now.Add(-90 * time.Second) // 两头的空白不算错
	for in, want := range cases {
		got, err := ParseTime(in, now)
		if err != nil || !got.Equal(want) {
			t.Errorf("ParseTime(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"-30", "yesterday", "30m", "1795500000", "--5m", "-1.5d", "-d"} {
		if _, err := ParseTime(bad, now); err == nil {
			t.Errorf("ParseTime(%q) accepted", bad)
		}
	}
}

func TestTrimLines(t *testing.T) {
	lines := []string{"a", "b", "b", "b", "c", strings.Repeat("x", 9) + "中文", "d", "d"}
	out, dropped := TrimLines(lines, 10, 10, false)
	want := []string{"a", "b  [repeated 3 times]", "c", "xxxxxxxxx… [+6 bytes]", "d  [repeated 2 times]"}
	if dropped != 0 || strings.Join(out, "|") != strings.Join(want, "|") {
		t.Errorf("out = %q, dropped = %d", out, dropped)
	}
	head, dropped := TrimLines(lines, 2, 10, false)
	if dropped != 3 || strings.Join(head, "|") != "a|b  [repeated 3 times]" {
		t.Errorf("head = %q, dropped = %d", head, dropped)
	}
	tail, dropped := TrimLines(lines, 2, 10, true)
	if dropped != 3 || strings.Join(tail, "|") != "xxxxxxxxx… [+6 bytes]|d  [repeated 2 times]" {
		t.Errorf("tail = %q, dropped = %d", tail, dropped)
	}
	if out, dropped := TrimLines(nil, 5, 5, true); len(out) != 0 || dropped != 0 {
		t.Errorf("empty input: %q, %d", out, dropped)
	}
}
