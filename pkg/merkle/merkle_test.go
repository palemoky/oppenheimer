package merkle

import (
	"context"
	"errors"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

func rowHash(key uint64, val int64) Hash {
	var c Canon
	return HashRow(c.Int(int64(key)).Int(val).String()) //nolint:gosec // 测试里的键都小于 2^63
}

func mustTree(t testing.TB, shift uint) *Tree {
	t.Helper()
	tr, err := NewTree(shift)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestHashRowMatchesSHA256Prefix(t *testing.T) {
	// SHA-256("abc") = ba7816bf8f01cfea 414140de5dae2223 ...
	h := HashRow("abc")
	if h.Hi != 0xba7816bf8f01cfea || h.Lo != 0x414140de5dae2223 {
		t.Fatalf("HashRow(abc) = %016x %016x", h.Hi, h.Lo)
	}
}

func TestCanonDistinguishesNullPosition(t *testing.T) {
	var a, b, c Canon
	a.Int(1).Null().Int(2)
	b.Int(1).Int(2).Null()
	if a.String() != "1|~|2" || b.String() != "1|2|~" {
		t.Fatalf("canon = %q, %q", a.String(), b.String())
	}
	// 内容里有分隔符和空值标记的字符串不会和别的列混淆；空串不同于 NULL。
	c.Int(-5).Bytes([]byte("a|~")).Bytes(nil).Bytes([]byte{0x00, 0xff})
	if got := c.String(); got != "-5|617C7E||00FF" {
		t.Fatalf("canon = %q", got)
	}
}

func TestCanonExpr(t *testing.T) {
	got := CanonExpr([]Column{
		{Expr: "order_id"},
		{Expr: "merchant_id", Nullable: true},
		{Expr: "currency", Kind: Bytes},
		{Expr: "fail_reason", Kind: Bytes, Nullable: true},
	})
	want := "CONCAT_WS('|', order_id, COALESCE(merchant_id, '~'), HEX(currency), COALESCE(HEX(fail_reason), '~'))"
	if got != want {
		t.Fatalf("CanonExpr = %s\nwant        %s", got, want)
	}
}

func TestDigestAddSub(t *testing.T) {
	a, b, c := Of(HashRow("a")), Of(HashRow("b")), Of(HashRow("c"))
	if a.Add(b).Add(c) != c.Add(a).Add(b) {
		t.Fatal("Add is not order independent")
	}
	if a.Add(b).Sub(b) != a {
		t.Fatal("Sub does not undo Add")
	}
	// 同一行出现两次不会抵消（XOR 会）。
	if a.Add(a).IsZero() || a.Add(a) == a {
		t.Fatal("a row added twice cancels out")
	}
	// 回绕。
	big := Digest{Count: 1, Hi: ^uint64(0), Lo: ^uint64(0)}
	if got := big.Add(Digest{Count: 1, Hi: 2, Lo: 1}); got != (Digest{Count: 2, Hi: 1, Lo: 0}) {
		t.Fatalf("wrapping add = %+v", got)
	}
	if !a.Sub(a).IsZero() {
		t.Fatal("a - a is not empty")
	}
}

func TestNode(t *testing.T) {
	n := NodeOf(0x1234_5678_9abc_def0, 48)
	if n.Index != 0x1234 {
		t.Fatalf("NodeOf = %v", n)
	}
	if lo, hi := n.Range(); lo != 0x1234_0000_0000_0000 || hi != 0x1234_ffff_ffff_ffff {
		t.Fatalf("Range = %x, %x", lo, hi)
	}
	if lo, hi := Root.Range(); lo != 0 || hi != ^uint64(0) {
		t.Fatalf("root Range = %x, %x", lo, hi)
	}
	if p := n.Parent(); p != (Node{Shift: 52, Index: 0x123}) {
		t.Fatalf("Parent = %v", p)
	}
	if p := (Node{Shift: 60, Index: 0xf}).Parent(); p != Root {
		t.Fatalf("Parent of top-level node = %v", p)
	}
	if lo, hi := (Node{Shift: 60, Index: 0xf}).Range(); lo != 0xf<<60 || hi != ^uint64(0) {
		t.Fatalf("last top-level Range = %x, %x", lo, hi)
	}
	for _, c := range []struct {
		n, c Node
		want bool
	}{
		{Root, n, true},
		{n.Parent(), n, true},
		{n, n, true},
		{n, n.Parent(), false},
		{Node{Shift: 52, Index: 0x124}, n, false},
	} {
		if got := c.n.Contains(c.c); got != c.want {
			t.Errorf("%v.Contains(%v) = %v", c.n, c.c, got)
		}
	}
}

func TestNewTreeRejectsBadShift(t *testing.T) {
	for _, s := range []uint{64, 65, 47, 1} {
		if _, err := NewTree(s); err == nil {
			t.Errorf("NewTree(%d) succeeded", s)
		}
	}
	for _, s := range []uint{0, 48, 60} {
		if _, err := NewTree(s); err != nil {
			t.Errorf("NewTree(%d): %v", s, err)
		}
	}
}

// 随机的一批行：键分散在很多叶子里。
func randomRows(r *rand.Rand, n int) map[uint64]int64 {
	rows := make(map[uint64]int64, n)
	for len(rows) < n {
		rows[r.Uint64()>>1] = r.Int64()
	}
	return rows
}

func build(t testing.TB, shift uint, rows map[uint64]int64) *Tree {
	tr := mustTree(t, shift)
	for k, v := range rows {
		tr.Add(k, rowHash(k, v))
	}
	return tr
}

func TestTreeMergeEqualsAddingEveryRow(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	rows := randomRows(r, 5000)
	whole := build(t, 48, rows)

	// 按键的低位拆成 3 份（模拟分片），各建一棵树再合并；另一边按叶子摘要相加。
	parts := []*Tree{mustTree(t, 48), mustTree(t, 48), mustTree(t, 48)}
	for k, v := range rows {
		parts[k%3].Add(k, rowHash(k, v))
	}
	merged, byLeaf := mustTree(t, 48), mustTree(t, 48)
	for _, p := range parts {
		if err := merged.Merge(p); err != nil {
			t.Fatal(err)
		}
		for _, e := range p.Leaves() {
			byLeaf.AddLeaf(e.Node.Index, e.Digest)
		}
	}
	if merged.Root() != whole.Root() || byLeaf.Root() != whole.Root() {
		t.Fatalf("roots differ: whole %v merged %v byLeaf %v", whole.Root(), merged.Root(), byLeaf.Root())
	}
	if whole.Root().Count != 5000 {
		t.Fatalf("root count = %d", whole.Root().Count)
	}
	if !slices.Equal(merged.Leaves(), whole.Leaves()) {
		t.Fatal("leaves differ after merge")
	}
	if err := merged.Merge(mustTree(t, 44)); err == nil {
		t.Fatal("merging trees with different leaf shifts succeeded")
	}
}

func TestTreeRemoveRestoresAndDropsEmptyLeaves(t *testing.T) {
	tr := mustTree(t, 48)
	tr.Add(1<<50, rowHash(1<<50, 7))
	before, n := tr.Root(), tr.Len()
	tr.Add(9<<50, rowHash(9<<50, 1))
	if tr.Root() == before {
		t.Fatal("root did not change after Add (stale cache?)")
	}
	tr.Remove(9<<50, rowHash(9<<50, 1))
	if tr.Root() != before || tr.Len() != n {
		t.Fatalf("after Remove: root %v len %d, want %v len %d", tr.Root(), tr.Len(), before, n)
	}
}

func TestTreeLevelsAreSumsOfChildren(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	tr := build(t, 48, randomRows(r, 3000))
	for shift := uint(RootShift); shift > 48; shift -= FanBits {
		var parents []Node
		if shift == RootShift {
			parents = []Node{Root}
		} else {
			seen := map[Node]bool{}
			for _, l := range tr.Leaves() {
				p := NodeOf(l.Node.Index<<48, shift)
				if !seen[p] {
					seen[p] = true
					parents = append(parents, p)
				}
			}
		}
		kids, err := tr.Children(t.Context(), parents)
		if err != nil {
			t.Fatal(err)
		}
		sums := map[Node]Digest{}
		for _, k := range kids {
			sums[k.Node.Parent()] = sums[k.Node.Parent()].Add(k.Digest)
		}
		for _, p := range parents {
			got, err := tr.Get(p)
			if err != nil {
				t.Fatal(err)
			}
			if got != sums[p] || got.IsZero() {
				t.Fatalf("node %v = %v, children sum to %v", p, got, sums[p])
			}
		}
	}
	if _, err := tr.Children(t.Context(), []Node{{Shift: 48, Index: 1}}); !errors.Is(err, ErrTooDeep) {
		t.Fatalf("children of a leaf: %v", err)
	}
	if _, err := tr.Get(Node{Shift: 44}); !errors.Is(err, ErrTooDeep) {
		t.Fatalf("Get below leaf level: %v", err)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	tr := build(t, 44, randomRows(r, 2000))
	b, err := tr.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var got Tree
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}
	if got.LeafShift() != 44 || got.Root() != tr.Root() || !slices.Equal(got.Leaves(), tr.Leaves()) {
		t.Fatal("round trip changed the tree")
	}
	b2, _ := got.MarshalBinary()
	if !slices.Equal(b, b2) {
		t.Fatal("serialization is not deterministic")
	}
	for name, bad := range map[string][]byte{
		"empty":     nil,
		"magic":     append([]byte("XXXXXXXX"), b[8:]...),
		"truncated": b[:len(b)-1],
		"short":     b[:len(b)-leafSize],
	} {
		if err := new(Tree).UnmarshalBinary(bad); err == nil {
			t.Errorf("%s: UnmarshalBinary succeeded", name)
		}
	}
}

func TestDiffIdenticalTreesComparesOneLevel(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	rows := randomRows(r, 4000)
	a, b := build(t, 48, rows), build(t, 48, rows)
	ds, st, err := Diff(t.Context(), a, b, Options{MinShift: 48})
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 0 || len(st.Levels) != 1 || st.Levels[0].Shift != 60 || st.Levels[0].Differing != 0 {
		t.Fatalf("diffs %v stats %+v", ds, st)
	}
}

// 随机改 k 行（改值、删除、新增），Diff 找出的叶子恰好是这些行所在的叶子。
func TestDiffFindsExactlyTheChangedLeaves(t *testing.T) {
	for _, k := range []int{1, 7, 200} {
		t.Run(strconv.Itoa(k), func(t *testing.T) {
			r := rand.New(rand.NewPCG(uint64(k), 9)) //nolint:gosec // k 是小的正数
			rows := randomRows(r, 20000)
			a := build(t, 48, rows)

			keys := slices.Sorted(func(yield func(uint64) bool) {
				for key := range rows {
					if !yield(key) {
						return
					}
				}
			})
			want := map[Node]bool{}
			changed := maps.Clone(rows)
			for i := range k {
				switch i % 3 {
				case 0: // 改值
					key := keys[r.IntN(len(keys))]
					changed[key] = rows[key] + 1
					want[NodeOf(key, 48)] = true
				case 1: // 删除
					key := keys[r.IntN(len(keys))]
					delete(changed, key)
					want[NodeOf(key, 48)] = true
				case 2: // 新增
					key := r.Uint64() >> 1
					if _, ok := rows[key]; ok {
						continue
					}
					changed[key] = 1
					want[NodeOf(key, 48)] = true
				}
			}
			b := build(t, 48, changed)

			ds, st, err := Diff(t.Context(), a, b, Options{MinShift: 48})
			if err != nil {
				t.Fatal(err)
			}
			got := map[Node]bool{}
			for i, d := range ds {
				got[d.Node] = true
				if d.Node.Shift != 48 || d.A == d.B {
					t.Fatalf("bad difference %+v", d)
				}
				if i > 0 && ds[i-1].Node.Index >= d.Node.Index {
					t.Fatal("differences are not in key order")
				}
				da, _ := a.Get(d.Node)
				db, _ := b.Get(d.Node)
				if d.A != da || d.B != db {
					t.Fatalf("difference %v carries digests %v/%v, trees have %v/%v", d.Node, d.A, d.B, da, db)
				}
			}
			if len(got) != len(want) {
				t.Fatalf("found %d differing leaves, want %d", len(got), len(want))
			}
			for n := range want {
				if !got[n] {
					t.Fatalf("leaf %v was changed but not reported", n)
				}
			}
			// 只展开不同的节点：每层比较的节点数不超过 16 倍的上一层差异数。
			for i, l := range st.Levels {
				limit := 1 << FanBits
				if i > 0 {
					limit *= st.Levels[i-1].Differing
				}
				if l.Compared > limit {
					t.Fatalf("level %d compared %d nodes, limit %d: %+v", l.Shift, l.Compared, limit, st)
				}
			}
			if k == 1 && st.Compared() > 4*(1<<FanBits) {
				t.Fatalf("one changed row compared %d nodes", st.Compared())
			}
		})
	}
}

func TestDiffStopsAtSmallNodes(t *testing.T) {
	// 两簇行：一簇很密（一个叶子里 500 行），一簇很稀。各改一行。
	rows := map[uint64]int64{}
	dense := uint64(0x0123) << 48
	for i := range uint64(500) {
		rows[dense|i<<38] = 1
	}
	sparse := uint64(0x4567) << 48
	rows[sparse] = 1
	a := build(t, 36, rows)
	rows[dense|3<<38] = 2
	rows[sparse] = 2
	b := build(t, 36, rows)

	ds, _, err := Diff(t.Context(), a, b, Options{MinShift: 36, MaxRows: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 2 {
		t.Fatalf("differences = %+v", ds)
	}
	// 稀的那一簇在最上面一层就停了；密的那一簇一直分到行数不超过 100。
	if ds[1].Node != (Node{Shift: 60, Index: 0x4}) {
		t.Fatalf("sparse cluster stopped at %v", ds[1].Node)
	}
	if ds[0].Node.Shift >= 48 || ds[0].A.Count > 100 || !ds[0].Node.Contains(NodeOf(dense|3<<38, 36)) {
		t.Fatalf("dense cluster stopped at %v with %d rows", ds[0].Node, ds[0].A.Count)
	}
}

func TestDiffOneSideEmpty(t *testing.T) {
	a := mustTree(t, 48)
	a.Add(5<<50, rowHash(5<<50, 1))
	ds, _, err := Diff(t.Context(), a, mustTree(t, 48), Options{MinShift: 48})
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 1 || !ds[0].B.IsZero() || ds[0].A.Count != 1 {
		t.Fatalf("differences = %+v", ds)
	}
}

type fakeSource func(parents []Node) ([]Entry, error)

func (f fakeSource) Children(_ context.Context, parents []Node) ([]Entry, error) { return f(parents) }

func TestDiffRejectsMisbehavingSource(t *testing.T) {
	good := mustTree(t, 48)
	good.Add(1<<60, rowHash(1, 1))
	d := Of(HashRow("x"))
	for name, es := range map[string][]Entry{
		"wrong level": {{Node: Node{Shift: 56, Index: 1}, Digest: d}},
		"unsorted":    {{Node: Node{Shift: 60, Index: 2}, Digest: d}, {Node: Node{Shift: 60, Index: 1}, Digest: d}},
		"duplicate":   {{Node: Node{Shift: 60, Index: 2}, Digest: d}, {Node: Node{Shift: 60, Index: 2}, Digest: d}},
		"empty":       {{Node: Node{Shift: 60, Index: 2}}},
	} {
		bad := fakeSource(func([]Node) ([]Entry, error) { return es, nil })
		if _, _, err := Diff(t.Context(), good, bad, Options{MinShift: 48}); err == nil {
			t.Errorf("%s: Diff accepted the source", name)
		}
	}
	// 第二层里给出不属于被展开节点的子节点。
	stray := fakeSource(func(parents []Node) ([]Entry, error) {
		if parents[0] == Root {
			return []Entry{{Node: Node{Shift: 60, Index: 1}, Digest: d}}, nil
		}
		return []Entry{{Node: Node{Shift: 56, Index: 0x7f}, Digest: d}}, nil
	})
	if _, _, err := Diff(t.Context(), good, stray, Options{MinShift: 48}); err == nil {
		t.Error("stray child: Diff accepted the source")
	}
	boom := errors.New("boom")
	failing := fakeSource(func([]Node) ([]Entry, error) { return nil, boom })
	if _, _, err := Diff(t.Context(), good, failing, Options{MinShift: 48}); !errors.Is(err, boom) {
		t.Errorf("source error not returned: %v", err)
	}
	if _, _, err := Diff(t.Context(), good, good, Options{MinShift: 47}); err == nil {
		t.Error("Diff accepted a min shift off the levels")
	}
}

func BenchmarkDiff(b *testing.B) {
	r := rand.New(rand.NewPCG(11, 12))
	rows := randomRows(r, 1_000_000)
	left := build(b, 44, rows)
	for _, k := range []int{0, 1, 100, 10_000, 100_000} {
		changed := make(map[uint64]int64, len(rows))
		n := 0
		for key, v := range rows {
			if n < k {
				v++
				n++
			}
			changed[key] = v
		}
		right := build(b, 44, changed)
		b.Run(strconv.Itoa(k), func(b *testing.B) {
			var st Stats
			for b.Loop() {
				_, st, _ = Diff(b.Context(), left, right, Options{MinShift: 44})
			}
			b.ReportMetric(float64(st.Compared()), "nodes")
		})
	}
}
