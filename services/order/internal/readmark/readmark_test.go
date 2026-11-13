package readmark_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/palemoky/Oppenheimer/pkg/testredis"
	"github.com/palemoky/Oppenheimer/services/order/internal/readmark"
	"github.com/palemoky/Oppenheimer/services/order/internal/store"
)

const orderID = 4242

func TestPinExpires(t *testing.T) {
	t.Parallel()
	rdb, prefix := testredis.New(t)
	m := readmark.New(rdb, prefix)
	if err := m.Pin(t.Context(), orderID, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if mk, err := m.Get(t.Context(), orderID); err != nil || !mk.Pinned {
		t.Fatalf("Get after Pin = %+v, %v; want pinned", mk, err)
	}
	time.Sleep(150 * time.Millisecond)
	if mk, err := m.Get(t.Context(), orderID); err != nil || mk.Pinned {
		t.Fatalf("Get after ttl = %+v, %v; want not pinned", mk, err)
	}
}

func TestStampOnlyMovesForward(t *testing.T) {
	t.Parallel()
	rdb, prefix := testredis.New(t)
	m := readmark.New(rdb, prefix)
	stamp := func(at int64, gtid string) {
		t.Helper()
		if err := m.Stamp(t.Context(), orderID, store.Position{GTID: gtid, At: at}, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	want := func(gtid string) {
		t.Helper()
		mk, err := m.Get(t.Context(), orderID)
		if err != nil {
			t.Fatal(err)
		}
		if mk.GTID != gtid {
			t.Fatalf("GTID = %q, want %q", mk.GTID, gtid)
		}
	}
	stamp(200, "u:1-5")
	want("u:1-5")
	stamp(100, "u:1-3") // 较早读到的位置较晚写进来：不覆盖
	want("u:1-5")
	stamp(200, "u:1-4") // 相同的 At 也不覆盖
	want("u:1-5")
	stamp(300, "u:1-7,v:1-2")
	want("u:1-7,v:1-2")
	if ttl := rdb.PTTL(t.Context(), prefix+"order:rw:gtid:"+strconv.Itoa(orderID)).Val(); ttl <= 0 {
		t.Errorf("gtid mark ttl = %s, want positive", ttl)
	}
}

// 旧位置不覆盖，但会续期：较晚的那次写之后，标记应当至少再保留 ttl。
func TestStampRenewsOnOlder(t *testing.T) {
	t.Parallel()
	rdb, prefix := testredis.New(t)
	m := readmark.New(rdb, prefix)
	key := prefix + "order:rw:gtid:" + strconv.Itoa(orderID)
	if err := m.Stamp(t.Context(), orderID, store.Position{GTID: "u:1-5", At: 200}, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := m.Stamp(t.Context(), orderID, store.Position{GTID: "u:1-3", At: 100}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if ttl := rdb.PTTL(t.Context(), key).Val(); ttl < 30*time.Second {
		t.Errorf("ttl = %s, want renewed to about a minute", ttl)
	}
}

func TestGetReadsBothWithPrefix(t *testing.T) {
	t.Parallel()
	rdb, prefix := testredis.New(t)
	m := readmark.New(rdb, prefix)
	if mk, err := m.Get(t.Context(), orderID); err != nil || mk != (readmark.Mark{}) {
		t.Fatalf("Get on nothing = %+v, %v; want zero", mk, err)
	}
	if err := m.Pin(t.Context(), orderID, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := m.Stamp(t.Context(), orderID, store.Position{GTID: "u:1-9", At: 1}, time.Minute); err != nil {
		t.Fatal(err)
	}
	mk, err := m.Get(t.Context(), orderID)
	if err != nil {
		t.Fatal(err)
	}
	if mk != (readmark.Mark{Pinned: true, GTID: "u:1-9"}) {
		t.Errorf("Get = %+v", mk)
	}
	for _, k := range []string{"pin", "gtid"} {
		if n := rdb.Exists(t.Context(), prefix+"order:rw:"+k+":"+strconv.Itoa(orderID)).Val(); n != 1 {
			t.Errorf("key %s not found under prefix", k)
		}
	}
	// 别的订单不受影响。
	if mk, err := m.Get(t.Context(), orderID+1); err != nil || mk != (readmark.Mark{}) {
		t.Errorf("other order = %+v, %v; want zero", mk, err)
	}
	if err := m.Stamp(t.Context(), orderID, store.Position{At: 2}, time.Minute); err == nil {
		t.Error("Stamp with empty gtid succeeded, want error")
	}
}

// 并发写入不同的位置，最后留下的是 At 最大的那个。
func TestStampConcurrent(t *testing.T) {
	t.Parallel()
	rdb, prefix := testredis.New(t)
	m := readmark.New(rdb, prefix)
	const n = 64
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			at := int64(i + 1)
			if err := m.Stamp(t.Context(), orderID, store.Position{GTID: "u:1-" + strconv.FormatInt(at, 10), At: at}, time.Minute); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	mk, err := m.Get(t.Context(), orderID)
	if err != nil {
		t.Fatal(err)
	}
	if want := "u:1-" + strconv.Itoa(n); mk.GTID != want {
		t.Errorf("GTID = %q, want %q", mk.GTID, want)
	}
}
