package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/errx"
)

// fakeGateMemory 代替 Gate.Memory：测试用的 Redis 没有设 maxmemory，也不能为了一个用例把它写满。
type fakeGateMemory struct {
	used, limit int64
	err         error
}

func (f *fakeGateMemory) probe(context.Context) (int64, int64, error) { return f.used, f.limit, f.err }

// watchGateMemory 给 e.svc 打开水位检查（85%），返回可以改数的假内存和手动驱动的采样器。
func watchGateMemory(t *testing.T, e *env) (*fakeGateMemory, *GateMemoryWatcher) {
	t.Helper()
	mem := &fakeGateMemory{used: 100, limit: 1000}
	e.svc.gateMemPercent = DefaultGateMemoryWatermarkPercent
	e.svc.gateMemProbe = mem.probe
	w, err := NewGateMemoryWatcher(e.svc, 0)
	if err != nil {
		t.Fatal(err)
	}
	return mem, w
}

func TestGateMemorySampleFull(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		used, limit int64
		want        bool
	}{
		{849, 1000, false},
		{850, 1000, true},
		{1000, 1000, true},
		{1 << 40, 0, false}, // 没有设上限
	} {
		m := gateMemorySample{used: c.used, limit: c.limit}
		if got := m.full(85); got != c.want {
			t.Errorf("used %d of %d: full = %v, want %v", c.used, c.limit, got, c.want)
		}
	}
}

// 闸门内存到水位：拒绝新的受理，闸门和消息都没有动；已经受理的订单照常落库；降回去之后恢复。
func TestGateMemoryWatermarkRejectsAdmission(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	mem, w := watchGateMemory(t, e)
	w.RunOnce(t.Context())
	queued := e.submit(t, 1, "queued", items(skuA, 1))
	sent := e.sender.sendCount()

	mem.used = 850
	w.RunOnce(t.Context())
	_, err := submitWith(t, e.svc, 1, "full")
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonStockGateUnavailable)
	if e.sender.sendCount() != sent {
		t.Error("a message was sent for a rejected order")
	}
	e.wantGate(t, skuA, 999, 1)

	// 留出来的内存就是给这个的：水位以上，已经受理的订单仍然能认领占用、落库。
	if err := e.land(t, queued.GetOrderId()); err != nil {
		t.Fatalf("land an accepted order while the gate is above the watermark: %v", err)
	}
	mustGetStatus(t, e.svc, queued.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)

	mem.used = 849
	w.RunOnce(t.Context())
	// 被拒绝时幂等键没有写进去，同一个键可以重新下单。
	if _, err := submitWith(t, e.svc, 1, "full"); err != nil {
		t.Fatalf("submit after the gate memory dropped below the watermark: %v", err)
	}
}

// 采不到新的数时沿用上一次的，直到它过期；过期之后当作不知道，照常受理（闸门不可用由熔断器管）。
func TestGateMemorySampleExpires(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	mem, w := watchGateMemory(t, e)
	mem.used = 900
	w.RunOnce(t.Context())

	mem.err = errors.New("redis is down")
	e.clock.Advance(gateMemoryMaxAge)
	w.RunOnce(t.Context())
	_, err := submitWith(t, e.svc, 1, "k")
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonStockGateUnavailable)

	e.clock.Advance(time.Millisecond)
	w.RunOnce(t.Context())
	if _, err := submitWith(t, e.svc, 1, "k"); err != nil {
		t.Fatalf("submit with an expired memory sample: %v", err)
	}
}

// 没开检查（水位 0）或闸门没有设内存上限时不拒绝。
func TestGateMemoryCheckOff(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	mem, w := watchGateMemory(t, e)
	mem.used, mem.limit = 1<<40, 0
	w.RunOnce(t.Context())
	if _, err := submitWith(t, e.svc, 1, "no-limit"); err != nil {
		t.Fatalf("submit when the gate has no maxmemory: %v", err)
	}

	mem.used, mem.limit = 999, 1000
	e.svc.gateMemPercent = 0
	w.RunOnce(t.Context())
	if _, err := submitWith(t, e.svc, 1, "check-off"); err != nil {
		t.Fatalf("submit with the watermark check off: %v", err)
	}
}

func TestNewRejectsBadGateMemoryWatermark(t *testing.T) {
	t.Parallel()
	for _, p := range []int{-1, 100} {
		if _, err := New(Deps{}, Config{GateMemoryWatermarkPercent: p}); err == nil {
			t.Errorf("New with watermark %d%% succeeded, want an error", p)
		}
	}
}
