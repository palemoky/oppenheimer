//go:build faults

package indexer

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/fault"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
)

// 这个文件只在带 faults 标签的构建里编译，钉住开关 f08 关着时的行为（答案在 chaos/scenarios/ 里，这里不写）。
// 开着时的用例在 unmarshal_spread_test.go 里，那个文件不参加 -race 的构建。

func setCompact(t *testing.T, on bool) {
	t.Helper()
	if err := fault.Set("f08", on, time.Minute); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fault.Set("f08", false, 0) })
}

// keyed 把一条变更序列化成 key 是订单号的 kafka 消息，和 CDC 发的一样。
func keyed(t testing.TB, m *cdcv1.RowChange, order, offset int64) kafkax.Message {
	t.Helper()
	msg := message(t, m, offset)
	msg.Key = []byte(strconv.FormatInt(order, 10))
	return msg
}

// straddling 返回两块长的一批：订单 base 的版本 2 是第一块的最后一条，版本 3 是第二块的第一条；
// 其余每条是一个不同的订单（版本 2）。商家都是 merchant。
func straddling(t testing.TB, merchant, base int64) []kafkax.Message {
	t.Helper()
	msgs := make([]kafkax.Message, 0, 2*unmarshalSpan)
	for i := range int64(2 * unmarshalSpan) {
		order, version := base+1+i, int64(2)
		switch i {
		case unmarshalSpan - 1:
			order = base
		case unmarshalSpan:
			order, version = base, 3
		}
		msgs = append(msgs, keyed(t, change(cdcv1.Op_OP_UPDATE, image(merchant, order, version)), order, 1000+i))
	}
	return msgs
}

func sameChanges(t *testing.T, got, want []*cdcv1.RowChange) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d changes, want %d", len(got), len(want))
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Errorf("change %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

// 不能和别的测试并行：开关是进程级的。
//
// 关着时多块长、有重复 key 的批也逐条一一对应地交出去，坏消息照旧是 Permanent。
func TestUnmarshalChangesSwitchOff(t *testing.T) {
	setCompact(t, false)
	msgs := straddling(t, 3, 5000)
	want, err := unmarshalInOrder(msgs)
	if err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalChanges(msgs)
	if err != nil {
		t.Fatal(err)
	}
	sameChanges(t, got, want)

	bad := slices.Concat(msgs, []kafkax.Message{{Key: []byte("1"), Value: []byte{0xff, 0xff, 0xff}}})
	if _, err := unmarshalChanges(bad); !kafkax.IsPermanent(err) {
		t.Fatalf("garbage: err = %v, want permanent", err)
	}
}

// 关着时经 Handler 写进索引的是每个订单的最高版本，Merged 计着批内被合并掉的那条。
func TestHandlerSwitchOffWritesLatest(t *testing.T) {
	setCompact(t, false)
	ix, st := newIndexer(t)
	const merchant, base = 4, 6000
	if err := ix.Handler(DefaultWriteTimeout)(t.Context(), straddling(t, merchant, base)); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(t.Context(), merchant, base)
	if err != nil {
		t.Fatal(err)
	}
	if want := wantRow(t, merchant, base, 3); !sameImage(got, want) {
		t.Fatalf("order %d: got %+v, want version 3 %+v", base, got, want)
	}
	if c := ix.Stats().take(); c.Merged != 1 || c.Written != 2*unmarshalSpan-1 {
		t.Errorf("counts = %+v, want Merged 1, Written %d", c, 2*unmarshalSpan-1)
	}
}
