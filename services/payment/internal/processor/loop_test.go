package processor

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// newLoopProcessor 返回每轮只执行 round 的 Processor，用来单独测试调度与生命周期。
func newLoopProcessor(round func(ctx context.Context)) *Processor {
	p := New(nil, nil, slog.New(slog.DiscardHandler), Config{Interval: time.Second})
	p.round = round
	return p
}

func TestStartRunsImmediatelyThenEveryInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rounds atomic.Int32
		p := newLoopProcessor(func(context.Context) { rounds.Add(1) })

		errc := make(chan error, 1)
		go func() { errc <- p.Start(t.Context()) }()

		synctest.Wait()
		if n := rounds.Load(); n != 1 {
			t.Fatalf("after start: %d rounds, want 1", n)
		}
		time.Sleep(999 * time.Millisecond)
		synctest.Wait()
		if n := rounds.Load(); n != 1 {
			t.Fatalf("before interval: %d rounds, want 1", n)
		}
		time.Sleep(3*time.Second + time.Millisecond)
		synctest.Wait()
		// 启动时一轮，加上第 1、2、3、4 秒各一轮。
		if n := rounds.Load(); n != 5 {
			t.Fatalf("after 4s: %d rounds, want 5", n)
		}

		if err := p.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := <-errc; err != nil {
			t.Fatalf("Start = %v, want nil", err)
		}
	})
}

func TestStopWaitsForInFlightRound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		roundErr := make(chan error, 1)
		p := newLoopProcessor(func(ctx context.Context) {
			<-release
			roundErr <- ctx.Err()
		})

		startCtx, cancelStart := context.WithCancel(t.Context())
		errc := make(chan error, 1)
		go func() { errc <- p.Start(startCtx) }()
		synctest.Wait()

		// app.Run 先取消 Start 的 ctx 再调用 Stop；进行中的一轮不应因此被打断。
		cancelStart()
		stopc := make(chan error, 1)
		go func() { stopc <- p.Stop(t.Context()) }()
		synctest.Wait()
		select {
		case err := <-stopc:
			t.Fatalf("Stop returned %v before the round finished", err)
		default:
		}

		close(release)
		if err := <-stopc; err != nil {
			t.Fatalf("Stop = %v, want nil", err)
		}
		if err := <-errc; err != nil {
			t.Fatalf("Start = %v, want nil", err)
		}
		if err := <-roundErr; err != nil {
			t.Errorf("round context was cancelled: %v", err)
		}
	})
}

func TestStopDeadlineCancelsRound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cancelled atomic.Bool
		p := newLoopProcessor(func(ctx context.Context) {
			<-ctx.Done()
			cancelled.Store(true)
		})
		errc := make(chan error, 1)
		go func() { errc <- p.Start(t.Context()) }()
		synctest.Wait()

		stopCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		if err := p.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop = %v, want DeadlineExceeded", err)
		}
		if !cancelled.Load() {
			t.Error("round was not cancelled after stop deadline")
		}
		if err := <-errc; err != nil {
			t.Fatalf("Start = %v, want nil", err)
		}
	})
}

func TestStopBeforeStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rounds atomic.Int32
		p := newLoopProcessor(func(context.Context) { rounds.Add(1) })

		if err := p.Stop(t.Context()); err != nil {
			t.Fatalf("Stop = %v, want nil", err)
		}
		if err := p.Start(t.Context()); err != nil {
			t.Fatalf("Start = %v, want nil", err)
		}
		if n := rounds.Load(); n != 0 {
			t.Errorf("%d rounds after stop, want 0", n)
		}
	})
}
