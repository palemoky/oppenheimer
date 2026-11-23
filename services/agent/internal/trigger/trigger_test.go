package trigger

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newClock() *clock { return &clock{t: time.Date(2026, 11, 24, 23, 0, 0, 0, time.UTC)} }

func firing(name, fp string, extra ...string) Alert {
	labels := map[string]string{"alertname": name, "severity": "warning"}
	for i := 0; i+1 < len(extra); i += 2 {
		labels[extra[i]] = extra[i+1]
	}
	return Alert{Status: "firing", Labels: labels, Fingerprint: fp, Annotations: map[string]string{"summary": "s"}}
}

func group(key string, alerts ...Alert) *Webhook {
	return &Webhook{GroupKey: key, Status: "firing", Alerts: alerts}
}

func newDispatcher(c *clock, queue int, budget Budget) (*Dispatcher, *[]string) {
	var skips []string
	var mu sync.Mutex
	d := NewDispatcher(Config{
		Cooldown: time.Hour, QueueSize: queue, Budget: budget, Now: c.Now,
		OnSkip: func(r string) { mu.Lock(); skips = append(skips, r); mu.Unlock() },
	})
	return d, &skips
}

func TestOfferFilters(t *testing.T) {
	c := newClock()
	d, skips := newDispatcher(c, 10, NewDailyBudget(1, 0, c.Now))
	resolved := firing("A", "1")
	resolved.Status = "resolved"

	cases := []struct {
		name string
		wh   *Webhook
		want string
	}{
		{"resolved only", group("g1", resolved), SkipResolved},
		{"watchdog", group("g2", firing("Watchdog", "2")), SkipFiltered},
		{"info", group("g3", firing("X", "3", "severity", "info")), SkipFiltered},
		{"synthetic", group("g4", firing("X", "4", "synthetic", "true")), SkipFiltered},
		{"real", group("g5", firing("MySQLDown", "5"), resolved), ""},
	}
	for _, tc := range cases {
		if got := d.Offer(tc.wh, nil); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
	if len(*skips) != 4 {
		t.Errorf("skips = %v", *skips)
	}
	job := <-d.Jobs()
	if job.Kind != KindAlert || !strings.Contains(job.Question, "Alert 1: MySQLDown") || strings.Contains(job.Question, "Alert 2") {
		t.Errorf("job = %+v", job)
	}
	if !strings.Contains(job.Question, "Current time: 2026-11-24T23:00:00Z") {
		t.Errorf("question lacks the current time:\n%s", job.Question)
	}
}

func TestOfferCooldown(t *testing.T) {
	c := newClock()
	d, _ := newDispatcher(c, 10, NewDailyBudget(1, 0, c.Now))

	if got := d.Offer(group("g", firing("A", "1")), nil); got != "" {
		t.Fatalf("first = %q", got)
	}
	c.Advance(5 * time.Minute)
	if got := d.Offer(group("g", firing("A", "1")), nil); got != SkipCooldown {
		t.Errorf("repeat = %q, want cooldown", got)
	}
	// 组里多了一条新的：再诊断一次。
	if got := d.Offer(group("g", firing("A", "1"), firing("A", "2")), nil); got != "" {
		t.Errorf("new alert in group = %q, want queued", got)
	}
	// 之后只剩旧的那一条：两条都见过了。
	if got := d.Offer(group("g", firing("A", "2")), nil); got != SkipCooldown {
		t.Errorf("known alert = %q, want cooldown", got)
	}
	// 别的组不受影响。
	if got := d.Offer(group("other", firing("A", "1")), nil); got != "" {
		t.Errorf("other group = %q", got)
	}
	c.Advance(time.Hour)
	if got := d.Offer(group("g", firing("A", "1")), nil); got != "" {
		t.Errorf("after cooldown = %q, want queued", got)
	}
}

func TestOfferQueueFullDoesNotStartCooldown(t *testing.T) {
	c := newClock()
	d, _ := newDispatcher(c, 1, NewDailyBudget(1, 0, c.Now))
	if got := d.Offer(group("a", firing("A", "1")), nil); got != "" {
		t.Fatal(got)
	}
	if got := d.Offer(group("b", firing("B", "2")), nil); got != SkipQueueFull {
		t.Fatalf("got %q, want queue_full", got)
	}
	<-d.Jobs()
	// 没排进去的那一组下次重发时还能进。
	if got := d.Offer(group("b", firing("B", "2")), nil); got != "" {
		t.Errorf("retry = %q, want queued", got)
	}
}

func TestOfferConcurrent(t *testing.T) {
	c := newClock()
	d, _ := newDispatcher(c, 100, NewDailyBudget(1, 0, c.Now))
	var wg sync.WaitGroup
	var mu sync.Mutex
	queued := 0
	for range 50 {
		wg.Go(func() {
			if d.Offer(group("same", firing("A", "1")), nil) == "" {
				mu.Lock()
				queued++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if queued != 1 {
		t.Errorf("queued = %d, want 1", queued)
	}
}

func TestDailyBudget(t *testing.T) {
	c := newClock()
	b := NewDailyBudget(20, 19.5, c.Now)
	d, _ := newDispatcher(c, 10, b)
	if b.Exhausted() {
		t.Fatal("exhausted too early")
	}
	b.Add(0.6)
	if got := d.Offer(group("g", firing("A", "1")), nil); got != SkipDailyBudget {
		t.Errorf("offer = %q, want daily_budget", got)
	}
	if _, err := d.Ask(t.Context(), "q"); !errors.Is(err, ErrBusy) {
		t.Errorf("ask err = %v, want ErrBusy", err)
	}
	c.Advance(time.Hour) // 过了 UTC 的零点
	if b.Exhausted() {
		t.Error("budget did not reset on a new day")
	}
}

func TestAsk(t *testing.T) {
	c := newClock()
	d, _ := newDispatcher(c, 1, NewDailyBudget(1, 0, c.Now))
	go func() {
		job := <-d.Jobs()
		job.Reply <- Reply{RunID: "r1", Outcome: "reported", Report: job.Question}
	}()
	r, err := d.Ask(t.Context(), "what is wrong")
	if err != nil || r.RunID != "r1" || !strings.HasPrefix(r.Report, "what is wrong\n\nCurrent time: ") {
		t.Fatalf("reply = %+v, err = %v", r, err)
	}

	// 提问的人走了：Ask 返回，任务仍在队列里。
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := d.Ask(ctx, "q"); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
	if _, err := d.Ask(t.Context(), "q"); !errors.Is(err, ErrBusy) {
		t.Errorf("err = %v, want ErrBusy (queue full)", err)
	}
}
