package main

import (
	"slices"
	"testing"
	"time"

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
		{"extra"},
	} {
		if _, err := parseFlags(bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}
