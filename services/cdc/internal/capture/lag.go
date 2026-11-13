package capture

import (
	"log/slog"
	"time"
)

// lagStats 汇总一段时间内增量消息的捕获延迟（发出时刻减事务在主库的提交时刻，设计 4.10）。
// 只有流水线自己的 goroutine 访问。
type lagStats struct {
	count, sum, max int64 // 微秒
}

func (s *lagStats) observe(micros int64) {
	micros = max(micros, 0) // 时钟偏差可能让它略小于 0
	s.count++
	s.sum += micros
	s.max = max(s.max, micros)
}

// report 输出一行汇总并清零。这段时间没有消息就不输出。
func (s *lagStats) report(log *slog.Logger) {
	if s.count == 0 {
		return
	}
	log.Info("capture lag",
		"messages", s.count,
		"max", time.Duration(s.max)*time.Microsecond,
		"avg", time.Duration(s.sum/s.count)*time.Microsecond)
	*s = lagStats{}
}
