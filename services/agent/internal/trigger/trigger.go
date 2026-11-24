// Package trigger 决定哪些告警值得诊断一次，并把要做的诊断排进队列。设计见 3d 的 3.6 和第 2 节第 6 项。
//
// 规则：
//   - 只处理 firing 的告警。不假设“恢复”之前见过“触发”（一直被抑制的告警会直接发来恢复通知）。
//   - 跳过 Watchdog、info 级别、带 synthetic="true" 的合成告警。
//   - 同一组告警（groupKey）在冷却期内只诊断一次，除非组里出现了新的告警。
//   - 队列有上限，排不进去就丢掉（记下来）。webhook 不等诊断结果。
package trigger

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

// 触发方式，指标的标签。
const (
	KindAlert = "alert"
	KindAsk   = "ask"
)

// 跳过的原因，指标的标签。
const (
	SkipResolved    = "resolved"
	SkipFiltered    = "filtered"
	SkipCooldown    = "cooldown"
	SkipQueueFull   = "queue_full"
	SkipDailyBudget = "daily_budget"
	// SkipPeakFull 是高峰时段里留着等低谷再跑的诊断已经满了，这一个丢掉。
	SkipPeakFull = "peak_hold_full"
)

// ErrBusy 表示队列满了或者当天的预算用完了，Ask 没有被接受。
var ErrBusy = errors.New("agent is busy or over budget")

// Webhook 是 Alertmanager webhook（version "4"）里用到的字段。
type Webhook struct {
	Receiver        string  `json:"receiver"`
	Status          string  `json:"status"`
	GroupKey        string  `json:"groupKey"`
	TruncatedAlerts int     `json:"truncatedAlerts"`
	Alerts          []Alert `json:"alerts"`
}

// Alert 是 webhook 里的一条告警。
type Alert struct {
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    time.Time         `json:"startsAt"`
	Fingerprint string            `json:"fingerprint"`
}

// Job 是排进队列的一次诊断。
type Job struct {
	Kind     string
	Question string
	// Alert 是触发它的 webhook 原文，手动提问时为空。
	Alert jsontext.Value
	// Reply 不为 nil 时（手动提问），做完之后把结果发到这里。带 1 个缓冲，发的一方不会被挡住。
	Reply chan Reply
}

// Reply 是手动提问的结果。
type Reply struct {
	RunID   string `json:"run_id"`
	Outcome string `json:"outcome"`
	Report  string `json:"report"`
}

// Budget 回答“今天还能不能再花钱”。
type Budget interface {
	Exhausted() bool
}

// Config 是 Dispatcher 的配置。
type Config struct {
	// Cooldown 是同一组告警两次诊断之间至少隔多久。
	Cooldown time.Duration
	// QueueSize 是排队的上限（不含正在跑的那一个）。
	QueueSize int
	Budget    Budget
	// Now 为 nil 时用 time.Now。
	Now func() time.Time
	// OnSkip 在跳过一次触发时被调用（记指标），可以为 nil。
	OnSkip func(reason string)
}

type seen struct {
	at           time.Time
	fingerprints map[string]struct{}
}

// Dispatcher 筛选、去重、排队。方法可以并发调用。
//
// 去重的状态只在内存里：重启之后同一组告警可能多诊断一次，这个代价可以接受。
type Dispatcher struct {
	cfg   Config
	queue chan Job

	mu   sync.Mutex
	seen map[string]seen
}

// NewDispatcher 返回 Dispatcher。
func NewDispatcher(cfg Config) *Dispatcher {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.OnSkip == nil {
		cfg.OnSkip = func(string) {}
	}
	return &Dispatcher{cfg: cfg, queue: make(chan Job, cfg.QueueSize), seen: make(map[string]seen)}
}

// Jobs 是工作协程读的队列。
func (d *Dispatcher) Jobs() <-chan Job { return d.queue }

// Offer 处理一次 webhook。返回空串表示排进了队列，否则是跳过的原因。
func (d *Dispatcher) Offer(wh *Webhook, raw jsontext.Value) string {
	reason := d.offer(wh, raw)
	if reason != "" {
		d.cfg.OnSkip(reason)
	}
	return reason
}

func (d *Dispatcher) offer(wh *Webhook, raw jsontext.Value) string {
	firing := 0
	var relevant []Alert
	for _, a := range wh.Alerts {
		if a.Status != "firing" {
			continue
		}
		firing++
		if diagnosable(a.Labels) {
			relevant = append(relevant, a)
		}
	}
	switch {
	case firing == 0:
		return SkipResolved
	case len(relevant) == 0:
		return SkipFiltered
	case d.cfg.Budget.Exhausted():
		return SkipDailyBudget
	}

	now := d.cfg.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	// 顺手清掉过了冷却期的记录，免得这张表只增不减。
	maps.DeleteFunc(d.seen, func(_ string, s seen) bool { return now.Sub(s.at) >= d.cfg.Cooldown })

	prev, ok := d.seen[wh.GroupKey]
	if ok && !hasNew(prev.fingerprints, relevant) {
		return SkipCooldown
	}
	job := Job{Kind: KindAlert, Question: Question(wh, relevant, now), Alert: raw}
	select {
	case d.queue <- job:
	default:
		// 没排进去就不记“见过”：下一次重发（5 分钟后）还有机会。
		return SkipQueueFull
	}
	fps := prev.fingerprints
	if fps == nil {
		fps = make(map[string]struct{}, len(relevant))
	}
	for _, a := range relevant {
		fps[a.Fingerprint] = struct{}{}
	}
	d.seen[wh.GroupKey] = seen{at: now, fingerprints: fps}
	return ""
}

// Ask 把一次手动提问排进队列并等结果。队列满或者预算用完时返回 ErrBusy。
func (d *Dispatcher) Ask(ctx context.Context, question string) (Reply, error) {
	if d.cfg.Budget.Exhausted() {
		d.cfg.OnSkip(SkipDailyBudget)
		return Reply{}, fmt.Errorf("%w: daily budget exhausted", ErrBusy)
	}
	job := Job{
		Kind:     KindAsk,
		Question: question + "\n\nCurrent time: " + d.cfg.Now().UTC().Format(time.RFC3339),
		Reply:    make(chan Reply, 1),
	}
	select {
	case d.queue <- job:
	default:
		d.cfg.OnSkip(SkipQueueFull)
		return Reply{}, fmt.Errorf("%w: queue full", ErrBusy)
	}
	select {
	case r := <-job.Reply:
		return r, nil
	case <-ctx.Done():
		// 提问的人走了；诊断照常做完并落盘，只是没人等它。
		return Reply{}, ctx.Err()
	}
}

func diagnosable(labels map[string]string) bool {
	return labels["alertname"] != "Watchdog" &&
		labels["severity"] != "info" &&
		labels["synthetic"] != "true"
}

func hasNew(known map[string]struct{}, alerts []Alert) bool {
	for _, a := range alerts {
		if _, ok := known[a.Fingerprint]; !ok {
			return true
		}
	}
	return false
}

// Question 把一组告警写成给模型的问题。输出是确定的（标签按名字排），方便对照运行记录。
func Question(wh *Webhook, alerts []Alert, now time.Time) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Alertmanager sent %d firing alert(s) in one group. Diagnose the most likely common cause.\n", len(alerts))
	fmt.Fprintf(&b, "Current time: %s\n", now.UTC().Format(time.RFC3339))
	if wh.TruncatedAlerts > 0 {
		fmt.Fprintf(&b, "Note: %d more alert(s) in this group were truncated from the notification.\n", wh.TruncatedAlerts)
	}
	for i, a := range alerts {
		fmt.Fprintf(&b, "\nAlert %d: %s\n", i+1, a.Labels["alertname"])
		fmt.Fprintf(&b, "  firing since: %s\n", a.StartsAt.UTC().Format(time.RFC3339))
		fmt.Fprintf(&b, "  labels: %s\n", pairs(a.Labels, "alertname"))
		for _, k := range slices.Sorted(maps.Keys(a.Annotations)) {
			fmt.Fprintf(&b, "  %s: %s\n", k, strings.TrimSpace(a.Annotations[k]))
		}
	}
	return b.String()
}

func pairs(m map[string]string, skip string) string {
	parts := make([]string, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if k != skip {
			parts = append(parts, fmt.Sprintf("%s=%q", k, m[k]))
		}
	}
	return strings.Join(parts, ", ")
}

// DailyBudget 是按 UTC 自然日累计的花费上限。
type DailyBudget struct {
	limit float64
	now   func() time.Time

	mu    sync.Mutex
	day   string
	spent float64
}

// NewDailyBudget 返回上限为 limit 美元的预算。spentToday 是今天已经花掉的（重启时从运行记录里加出来）。
func NewDailyBudget(limit, spentToday float64, now func() time.Time) *DailyBudget {
	if now == nil {
		now = time.Now
	}
	return &DailyBudget{limit: limit, now: now, day: Day(now()), spent: spentToday}
}

// Day 返回 t 所在的 UTC 日期。
func Day(t time.Time) string { return t.UTC().Format(time.DateOnly) }

// Add 记一笔花费。
func (b *DailyBudget) Add(cost float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roll()
	b.spent += cost
}

// Exhausted 实现 Budget。
func (b *DailyBudget) Exhausted() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roll()
	return b.spent >= b.limit
}

func (b *DailyBudget) roll() {
	if today := Day(b.now()); today != b.day {
		b.day, b.spent = today, 0
	}
}
