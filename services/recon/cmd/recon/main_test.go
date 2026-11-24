package main

import (
	"slices"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/services/recon/internal/recon"
)

func TestParseFlags(t *testing.T) {
	o, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(o.pairs, []string{"replica", "index", "payment", "stock"}) || o.settle != 30*time.Minute ||
		o.recheck != recon.DefaultRecheck || o.mode != recon.ModeTree || o.maxRows != recon.DefaultMaxRows {
		t.Errorf("defaults: %+v", o)
	}
	o, err = parseFlags([]string{"-against", "f.json"})
	if err != nil || !slices.Equal(o.pairs, []string{"archive"}) {
		t.Errorf("-against alone: %+v, %v", o, err)
	}
	for _, bad := range [][]string{
		{"-pairs", "nope"},
		{"-pairs", "archive"},
		{"-pairs", "index", "-against", "f.json"},
		{"-snapshot-out", "a", "-against", "b"},
		{"-mode", "fast"},
		{"-settle", "-1s"},
		{"-since", "-1s"},
		{"-since", "10m"},
		{"-since", "24h", "-against", "f.json"},
		{"-max-rows", "0"},
		{"-from", "yesterday"},
		{"-from", "2026-11-24T03:00:00Z", "-since", "2h"},
		{"-from", "2026-11-24T03:00:00Z", "-to", "2026-11-24T03:00:00Z"},
		{"-since", "2h", "-to", "2026-11-24T03:00:00Z"},
		{"-from", "2026-11-24T03:00:00Z", "-against", "f.json"},
		{"extra"},
	} {
		if _, err := parseFlags(bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}

func TestWindow(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	from := time.Date(2026, 11, 24, 3, 0, 0, 0, time.UTC)
	to := time.Date(2026, 11, 24, 3, 30, 0, 0, time.UTC)
	parse := func(args ...string) options {
		t.Helper()
		o, err := parseFlags(args)
		if err != nil {
			t.Fatalf("%q: %v", args, err)
		}
		return o
	}
	cases := []struct {
		name         string
		o            options
		since, bound int64
	}{
		{"defaults: full run up to now minus settle", parse(), 0, idgen.LowerBound(now.Add(-30 * time.Minute))},
		{"relative window", parse("-since", "2h"), idgen.LowerBound(now.Add(-2 * time.Hour)), idgen.LowerBound(now.Add(-30 * time.Minute))},
		// -to 取代 -settle：实验那一段的上界就是实验结束的时刻，不再往前让 30 分钟。
		{"absolute window", parse("-from", "2026-11-24T03:00:00Z", "-to", "2026-11-24T03:30:00Z"), idgen.LowerBound(from), idgen.LowerBound(to)},
		{"absolute lower bound only", parse("-from", "2026-11-24T03:00:00Z"), idgen.LowerBound(from), idgen.LowerBound(now.Add(-30 * time.Minute))},
		{"absolute upper bound only", parse("-to", "2026-11-24T03:30:00Z"), 0, idgen.LowerBound(to)},
		{"no upper bound", parse("-settle", "0"), 0, 0},
	}
	for _, tc := range cases {
		since, bound := tc.o.window(now)
		if since != tc.since || bound != tc.bound {
			t.Errorf("%s: window = [%d, %d), want [%d, %d)", tc.name, since, bound, tc.since, tc.bound)
		}
	}
}
