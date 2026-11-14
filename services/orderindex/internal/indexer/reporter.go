package indexer

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// DefaultReportInterval 是汇总日志的间隔（设计文档 1c 4.10）。
const DefaultReportInterval = 10 * time.Second

// Reporter 每隔一段时间输出一行同步汇总日志：应用的条数（改了行的、因版本旧没改的分开计，两者是估计）、
// 实际写库的行数和批内合并掉的条数、跳过和忽略的条数、其中来自归档 DELETE 的条数、端到端延迟的最大值和平均值。
// 这段时间没有消息时不输出。实现 app.Component。
//
// 按批写之后拿不到逐行的结果（见 Counts.Changed），所以“改了行”和“没改”的字段名带 _est：
// 两者之和精确等于有商家的消息数，分配是估计（changed_est 可能偏大）。written、merged 是精确的。
//
// 已知的不足（设计文档 1c 4.10）：没有写入就没有延迟可算，“没有变更”和“同步卡住了”在日志上分不开，
// 要靠消费组的积压来区分。
type Reporter struct {
	stats    *Stats
	log      *slog.Logger
	interval time.Duration

	stopOnce sync.Once
	stop     chan struct{}
}

// NewReporter 返回输出 stats 的 Reporter。interval 不大于 0 时用 DefaultReportInterval。
func NewReporter(stats *Stats, logger *slog.Logger, interval time.Duration) *Reporter {
	if interval <= 0 {
		interval = DefaultReportInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Reporter{stats: stats, log: logger, interval: interval, stop: make(chan struct{})}
}

// Name 实现 app.Component。
func (r *Reporter) Name() string { return "orderindex-sync-reporter" }

// Start 实现 app.Component：定期输出，直到 Stop 或 ctx 取消。停止前把最后一段输出掉。
func (r *Reporter) Start(ctx context.Context) error {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			r.Flush()
		case <-r.stop:
			r.Flush()
			return nil
		case <-ctx.Done():
			r.Flush()
			return nil
		}
	}
}

// Stop 实现 app.Component。
func (r *Reporter) Stop(context.Context) error {
	r.stopOnce.Do(func() { close(r.stop) })
	return nil
}

// Flush 输出并清零当前一段的统计；没有消息时不输出。
func (r *Reporter) Flush() {
	w := r.stats.take()
	if w.empty() {
		return
	}
	attrs := []any{
		"interval", r.interval.String(),
		// changed_est + stale_est = 有商家的消息数；分配是估计，见 Counts.Changed。
		"changed_est", w.Changed,
		"stale_est", w.Stale,
		// written 是合并后写库的行数，merged 是批内被同一订单更高版本合并掉的条数，written + merged = 上面两者之和。
		"written", w.Written,
		"merged", w.Merged,
		"skipped_no_merchant", w.SkippedNoMerchant,
		"ignored", w.Ignored,
		// archived 是其中来自 DELETE（归档）的条数，已经算在 changed_est/stale_est/skipped_no_merchant 里。
		"archived", w.Archived,
		"lag_samples", w.LagCount,
	}
	if w.LagCount > 0 {
		attrs = append(attrs,
			"lag_max_ms", w.LagMax.Milliseconds(),
			"lag_avg_ms", (w.LagSum / time.Duration(w.LagCount)).Milliseconds(),
		)
	}
	r.log.Info("orderindex sync summary", attrs...)
}
