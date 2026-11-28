//go:build faults

package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/fault"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 这个文件只在带 faults 标签的构建里编译。它钉住的是故障点 f06 的行为（答案在 chaos/scenarios/ 里，这里不写）：
// 开关关着时和平时的构建一样；开着时，有结果的从库扫描不受影响，没有结果的那种表现出预期的症状。
//
// 不并行：开关是进程级的，打开期间别的用例在从库上的扫描也会受影响。
func TestReplicaScanVariant(t *testing.T) {
	f := newFixture(t)
	replicas := f.sameReplicas(t) // 每个池最多 4 条连接
	st := f.withReplicas(t, replicas)

	o := f.newOrder(t, 1, "k")
	if err := st.InsertOrder(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	full := f.router.ForOrder(o.OrderID)
	db := f.router.DB(full)
	empty := -1
	for _, slot := range f.router.SlotsOf(db) {
		if slot != full {
			empty = slot
			break
		}
	}
	if empty < 0 {
		t.Fatalf("db %d has no second slot", db)
	}
	later := o.UpdatedAt.Add(time.Hour)
	scan := func(ctx context.Context, slot int, src store.Source) ([]store.OrderRef, error) {
		return st.ListStaleCreating(ctx, slot, later, 100, src)
	}
	expect := func(slot, want int) {
		t.Helper()
		refs, err := scan(t.Context(), slot, store.Replica)
		if err != nil {
			t.Fatalf("scan slot %d on replica: %v", slot, err)
		}
		if len(refs) != want {
			t.Fatalf("scan slot %d on replica: %d refs, want %d", slot, len(refs), want)
		}
	}
	inUse := func() int { return replicas[db].Stats().InUse }

	// 关着：有结果、没结果都和平时一样，用完的连接都回到池子里。
	for range 10 {
		expect(full, 1)
		expect(empty, 0)
	}
	if n := inUse(); n != 0 {
		t.Fatalf("switch off: %d replica connections in use after the scans, want 0", n)
	}

	if err := fault.Set("f06", true, time.Minute); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fault.Set("f06", false, 0) })

	// 开着，有结果：不受影响。
	for range 10 {
		expect(full, 1)
	}
	if n := inUse(); n != 0 {
		t.Fatalf("switch on, non-empty scans: %d replica connections in use, want 0", n)
	}

	// 开着，没有结果：每次扫描之后池子里少一条连接，直到用完。
	maxOpen := replicas[db].Stats().MaxOpenConnections
	for i := range maxOpen {
		expect(empty, 0)
		if n := inUse(); n != i+1 {
			t.Fatalf("switch on, after %d empty scans: %d replica connections in use, want %d", i+1, n, i+1)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := scan(ctx, full, store.Replica); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("switch on, replica pool used up: scan error = %v, want deadline exceeded", err)
	}
	if w := replicas[db].Stats().WaitCount; w == 0 {
		t.Error("switch on, replica pool used up: no waits counted")
	}
	// 主库的池子是另一个，不受影响。
	if refs, err := scan(t.Context(), full, store.Primary); err != nil || len(refs) != 1 {
		t.Fatalf("primary scan with the replica pool used up = %d refs, %v; want 1, nil", len(refs), err)
	}

	// 关掉开关之后已经占住的连接不会回来。
	if err := fault.Set("f06", false, 0); err != nil {
		t.Fatal(err)
	}
	if n := inUse(); n != maxOpen {
		t.Fatalf("switch turned off: %d replica connections in use, want still %d", n, maxOpen)
	}
	ctx2, cancel2 := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel2()
	if _, err := scan(ctx2, full, store.Replica); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("switch turned off, replica pool still used up: scan error = %v, want deadline exceeded", err)
	}
}
