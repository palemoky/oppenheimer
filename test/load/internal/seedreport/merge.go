package seedreport

import "container/heap"

// MergeDesc 对 k 个各自按 key 降序排好的列表做 k 路归并，丢掉前 skip 条，返回接下来最多 limit 条。
//
// 这是散发加归并的协调方（设计文档 1c 的 5.1）：每个分片返回自己的前若干条，归并后才知道全局的顺序。
// key 相同的元素按列表下标先后输出，结果是确定的。用堆实现，代价 O((skip+limit) log k)。
func MergeDesc[T any](lists [][]T, key func(T) int64, skip, limit int) []T {
	if limit <= 0 {
		return nil
	}
	h := &cursorHeap[T]{key: key}
	for i, l := range lists {
		if len(l) > 0 {
			h.items = append(h.items, cursor{list: i})
		}
	}
	h.lists = lists
	heap.Init(h)
	out := make([]T, 0, limit)
	for h.Len() > 0 && len(out) < limit {
		c := &h.items[0]
		v := lists[c.list][c.pos]
		if skip > 0 {
			skip--
		} else {
			out = append(out, v)
		}
		c.pos++
		if c.pos == len(lists[c.list]) {
			heap.Pop(h)
		} else {
			heap.Fix(h, 0)
		}
	}
	return out
}

type cursor struct{ list, pos int }

type cursorHeap[T any] struct {
	lists [][]T
	items []cursor
	key   func(T) int64
}

func (h *cursorHeap[T]) Len() int { return len(h.items) }

func (h *cursorHeap[T]) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	ka, kb := h.key(h.lists[a.list][a.pos]), h.key(h.lists[b.list][b.pos])
	if ka != kb {
		return ka > kb
	}
	return a.list < b.list
}

func (h *cursorHeap[T]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *cursorHeap[T]) Push(x any) { h.items = append(h.items, x.(cursor)) } //nolint:forcetypeassert // heap 只会推入 cursor

func (h *cursorHeap[T]) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}
