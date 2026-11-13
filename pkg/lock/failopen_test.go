package lock

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/palemoky/oppenheimer/pkg/testredis"
)

// stubGuard 按预设返回结果。
type stubGuard struct {
	ok  bool
	err error
}

func (s *stubGuard) Allow(context.Context) (bool, error) { return s.ok, s.err }

func TestFailOpenPassesThroughDecisions(t *testing.T) {
	inner := &stubGuard{}
	g := FailOpen(inner, slog.New(slog.DiscardHandler))
	for _, want := range []bool{true, false} {
		inner.ok = want
		got, err := g.Allow(t.Context())
		if err != nil || got != want {
			t.Errorf("inner=%v: got (%v, %v)", want, got, err)
		}
	}
}

func TestFailOpenAllowsOnError(t *testing.T) {
	inner := &stubGuard{err: errors.New("redis down")}
	g := FailOpen(inner, slog.New(slog.DiscardHandler))
	got, err := g.Allow(t.Context())
	if err != nil || !got {
		t.Fatalf("got (%v, %v), want (true, nil)", got, err)
	}
}

// 进程退出时 ctx 已取消，加锁请求因此失败。这不是“锁不可用”，不能放行。
func TestFailOpenDoesNotAllowWhenContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	inner := &stubGuard{err: context.Canceled}
	g := FailOpen(inner, slog.New(slog.DiscardHandler))
	got, err := g.Allow(ctx)
	if got || err == nil {
		t.Fatalf("got (%v, %v), want (false, error)", got, err)
	}
}

func TestFailOpenLogsAreRateLimited(t *testing.T) {
	var buf bytes.Buffer
	inner := &stubGuard{err: errors.New("redis down")}
	g := FailOpen(inner, slog.New(slog.NewTextHandler(&buf, nil)))
	now := time.Now()
	g.now = func() time.Time { return now }

	count := func(sub string) int { return strings.Count(buf.String(), sub) }
	allow := func() {
		t.Helper()
		if _, err := g.Allow(t.Context()); err != nil {
			t.Fatalf("Allow: %v", err)
		}
	}

	for range 100 {
		allow()
	}
	if n := count("lock unavailable"); n != 1 {
		t.Fatalf("warn logged %d times within the interval, want 1", n)
	}
	now = now.Add(failOpenWarnEvery)
	allow()
	if n := count("lock unavailable"); n != 2 {
		t.Fatalf("warn logged %d times after the interval, want 2", n)
	}

	// 恢复只记一次；再次故障时立即告警，不受限频影响。
	inner.err = nil
	allow()
	allow()
	if n := count("lock available again"); n != 1 {
		t.Fatalf("recovery logged %d times, want 1", n)
	}
	inner.err = errors.New("redis down again")
	allow()
	if n := count("lock unavailable"); n != 3 {
		t.Fatalf("warn logged %d times after a second outage, want 3", n)
	}
}

// 用真实的 Leader 和连不上的 Redis：两个实例都被放行，对应“退化成每个副本都跑”。
func TestFailOpenWithUnreachableRedis(t *testing.T) {
	dead := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1", // 没有服务监听
		DialTimeout: 100 * time.Millisecond,
		MaxRetries:  -1,
		// 默认会重拨 5 次、每次间隔 100 毫秒，测试里不需要。
		DialerRetries: 1,
	})
	t.Cleanup(func() { _ = dead.Close() })

	for range 2 {
		l := newLocker(t, dead, "t:unreachable:")
		leader := l.Leader("job")
		if ok, err := leader.Allow(t.Context()); ok || err == nil {
			t.Fatalf("bare Leader: got (%v, %v), want (false, error)", ok, err)
		}
		ok, err := FailOpen(leader, slog.New(slog.DiscardHandler)).Allow(t.Context())
		if err != nil || !ok {
			t.Fatalf("FailOpen: got (%v, %v), want (true, nil)", ok, err)
		}
	}
}

// Redis 正常时 FailOpen 不改变互斥：只有一个实例被放行。
func TestFailOpenKeepsExclusionWhenRedisIsHealthy(t *testing.T) {
	client, prefix := testredis.New(t)
	ctx := testCtx(t)
	l := newLocker(t, client, prefix)
	a, b := l.Leader("job"), l.Leader("job")
	t.Cleanup(func() {
		_ = a.Close(context.WithoutCancel(ctx))
		_ = b.Close(context.WithoutCancel(ctx))
	})
	ga := FailOpen(a, slog.New(slog.DiscardHandler))
	gb := FailOpen(b, slog.New(slog.DiscardHandler))

	if ok, err := ga.Allow(ctx); err != nil || !ok {
		t.Fatalf("first: got (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := gb.Allow(ctx); err != nil || ok {
		t.Fatalf("second: got (%v, %v), want (false, nil)", ok, err)
	}
}
