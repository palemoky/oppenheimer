//go:build faults && !race

package indexer

import (
	"slices"
	"testing"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// 这个文件钉住开关 f08 开着时的行为（答案在 chaos/scenarios/ 里，这里不写）。开着的路径上有意没有同步，
// 所以这个文件不参加 -race 的构建（make test-faults 带 -race，跑不到这里）；要单独不带 -race 跑：
// 照 Makefile 导出 OPPENHEIMER_TEST_* 之后 go test -tags faults ./...
//
// 测试都不并行：开关是进程级的。

// tries 是重复的次数。出错要两个 goroutine 真的交错，单次不一定撞上；实测几乎每次都撞上，这里只要求出现过。
const tries = 200

// 不满足触发条件的批开着和关着结果相同：不超过一块的批（有重复 key 也一样），多块但 key 各不相同的批。
func TestUnmarshalChangesOnWithoutTrigger(t *testing.T) {
	setCompact(t, true)

	small := straddling(t, 3, 7000)[unmarshalSpan/2 : unmarshalSpan+unmarshalSpan/2]
	want, err := unmarshalInOrder(small)
	if err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalChanges(small)
	if err != nil {
		t.Fatal(err)
	}
	sameChanges(t, got, want)

	distinct := make([]kafkax.Message, 0, 3*unmarshalSpan+5)
	for i := range int64(3*unmarshalSpan + 5) {
		distinct = append(distinct, keyed(t, change(cdcv1.Op_OP_UPDATE, image(3, 7100+i, 2)), 7100+i, i))
	}
	want, err = unmarshalInOrder(distinct)
	if err != nil {
		t.Fatal(err)
	}
	for range tries {
		got, err := unmarshalChanges(distinct)
		if err != nil {
			t.Fatal(err)
		}
		sameChanges(t, got, want)
	}

	bad := slices.Concat(distinct, []kafkax.Message{{Key: []byte("1"), Value: []byte{0xff}}})
	if _, err := unmarshalChanges(bad); !kafkax.IsPermanent(err) {
		t.Fatalf("garbage: err = %v, want permanent", err)
	}
}

// 开着时，同一个订单的两条落在两块里：第二块先写完它的那一条，第一块后写，旧的映像盖掉新的。
// 交给 Apply 的那一条可能是旧版本，而新版本已经不在这一批里了。
func TestUnmarshalChangesKeepsOlderImage(t *testing.T) {
	setCompact(t, true)
	const base = 8000
	msgs := straddling(t, 3, base)
	stale := 0
	for range tries {
		got, err := unmarshalChanges(msgs)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2*unmarshalSpan-1 {
			t.Fatalf("got %d changes, want one per order (%d)", len(got), 2*unmarshalSpan-1)
		}
		// 订单 base 第一次出现在第一块的最后，位置是 unmarshalSpan-1。
		r, _, err := decodeRow(got[unmarshalSpan-1].GetAfter())
		if err != nil {
			t.Fatal(err)
		}
		if r.OrderID != base {
			t.Fatalf("position %d holds order %d, want %d", unmarshalSpan-1, r.OrderID, base)
		}
		if r.Version == 2 {
			stale++
		}
	}
	t.Logf("older image kept in %d of %d tries", stale, tries)
	if stale == 0 {
		t.Fatalf("the newer image always won in %d tries", tries)
	}
}

// 经 Handler 写进索引：旧版本落进去，版本号和内容是同一个真实的旧映像（不是拼出来的），
// 新版本不再写；之后关着开关重放新版本（或者订单再变一次）就修好了。
func TestHandlerWritesOlderImage(t *testing.T) {
	ix, st := newIndexer(t)
	h := ix.Handler(DefaultWriteTimeout)
	const merchant = 4
	setCompact(t, true)
	for n := range int64(tries) {
		base := 9000 + n*1000
		if err := h(t.Context(), straddling(t, merchant, base)); err != nil {
			t.Fatal(err)
		}
		got, err := st.Get(t.Context(), merchant, base)
		if err != nil {
			t.Fatal(err)
		}
		if got.Version == 3 {
			continue
		}
		if want := wantRow(t, merchant, base, 2); !sameImage(got, want) {
			t.Fatalf("order %d: got %+v, want the whole version 2 image %+v", base, got, want)
		}
		t.Logf("order %d left at version 2 after %d tries", base, n+1)

		setCompact(t, false)
		if err := h(t.Context(), []kafkax.Message{keyed(t, change(cdcv1.Op_OP_UPDATE, image(merchant, base, 3)), base, 0)}); err != nil {
			t.Fatal(err)
		}
		got, err = st.Get(t.Context(), merchant, base)
		if err != nil {
			t.Fatal(err)
		}
		if want := wantRow(t, merchant, base, 3); !sameImage(got, want) {
			t.Fatalf("after replay: got %+v, want %+v", got, want)
		}
		return
	}
	t.Fatalf("the newer image always reached the index in %d tries", tries)
}
