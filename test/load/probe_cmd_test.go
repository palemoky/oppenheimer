package main

import (
	"slices"
	"testing"
)

func TestParseIDRanges(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want []int64
	}{
		{"7", []int64{7}},
		{"1001,1002", []int64{1001, 1002}},
		{"900001-900003, 5", []int64{900001, 900002, 900003, 5}},
		{" 3-3 ,", []int64{3}},
	} {
		got, err := parseIDRanges(tt.in)
		if err != nil || !slices.Equal(got, tt.want) {
			t.Errorf("parseIDRanges(%q) = %v, %v; want %v", tt.in, got, err, tt.want)
		}
	}
	for _, in := range []string{"", ",", "a", "5-3", "0-2", "1-x", "1-200000", "-5"} {
		if got, err := parseIDRanges(in); err == nil {
			t.Errorf("parseIDRanges(%q) = %v, want an error", in, got)
		}
	}
}

func TestProbeRejectsBadFlags(t *testing.T) {
	var out discard
	for _, args := range [][]string{
		{},                                 // 没有 -skus
		{"-skus", "1", "-user-base", "1"},  // 不在压测号段
		{"-skus", "1", "-addr", "foo"},     // 地址不对
		{"-skus", "1", "-interval", "0s"},  // 间隔为 0
		{"-skus", "1", "-max-in-flight=0"}, // 没有在途名额
	} {
		if code := probeCmd(args, &out); code != 2 {
			t.Errorf("probe %v exited with %d, want 2", args, code)
		}
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
