package seedreport

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"testing"
)

func id(v int64) int64 { return v }

func TestMergeDesc(t *testing.T) {
	lists := [][]int64{{9, 5, 1}, {}, {8, 7, 2}, {6}}
	for _, tc := range []struct {
		skip, limit int
		want        []int64
	}{
		{0, 3, []int64{9, 8, 7}},
		{2, 3, []int64{7, 6, 5}},
		{5, 10, []int64{2, 1}},
		{7, 3, []int64{}},
		{0, 0, nil},
		{100, 1, []int64{}},
	} {
		got := MergeDesc(lists, id, tc.skip, tc.limit)
		if !slices.Equal(got, tc.want) {
			t.Errorf("skip %d limit %d: got %v, want %v", tc.skip, tc.limit, got, tc.want)
		}
	}
	if got := MergeDesc[int64](nil, id, 0, 5); len(got) != 0 {
		t.Errorf("no lists: got %v", got)
	}
}

// 与“全部拼起来排序再切片”的结果相同，包括重复的 key。
func TestMergeDescMatchesSort(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		k := r.IntN(9)
		lists := make([][]int64, k)
		var all []int64
		for i := range lists {
			for range r.IntN(30) {
				lists[i] = append(lists[i], r.Int64N(50))
			}
			slices.SortFunc(lists[i], func(a, b int64) int { return cmp.Compare(b, a) })
			all = append(all, lists[i]...)
		}
		slices.SortFunc(all, func(a, b int64) int { return cmp.Compare(b, a) })
		skip, limit := r.IntN(40), 1+r.IntN(20)
		want := all[min(skip, len(all)):min(skip+limit, len(all))]
		if got := MergeDesc(lists, id, skip, limit); !slices.Equal(got, want) {
			t.Fatalf("lists %v skip %d limit %d: got %v, want %v", lists, skip, limit, got, want)
		}
	}
}
