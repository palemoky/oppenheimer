package alertsink

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// 标签的取值是有限的枚举：status 三种、severity 五种。alertname、实例名只进日志，不进标签。
var (
	statuses   = [...]string{"firing", "resolved", "other"}
	severities = [...]string{"critical", "warning", "info", "none", "other"}
)

var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/test/load/internal/alertsink")

	alertsTotal = otelx.Counter(meter, "alertsink.alerts",
		"Alerts received from Alertmanager, by status (firing, resolved, other) and severity (critical, warning, info, none, other).")
	// watchdogLastReceived 是最近一次收到 firing 的 Watchdog 的 Unix 时间（秒）。同步的 gauge：第一次记录之前
	// 不导出这条序列（不会报 0），所以“告警这条路断了”的规则用 time() - 它，序列不存在时另写 absent()。
	watchdogLastReceived = otelx.Gauge(meter, "alertsink.watchdog.last_received",
		"Unix time in seconds when a firing Watchdog alert was last received.")
)

// 每种标签组合的选项预先建好，热路径上不分配。
var alertOpts [len(statuses)][len(severities)]metric.MeasurementOption

func init() {
	for i, s := range statuses {
		for j, v := range severities {
			alertOpts[i][j] = metric.WithAttributes(attribute.String("status", s), attribute.String("severity", v))
		}
	}
}

// alertOptions 把 status 和 severity 归到枚举里（其他值是 other，severity 缺失是 none）。
func alertOptions(status, severity string) metric.MeasurementOption {
	si := len(statuses) - 1
	for i, s := range statuses[:si] {
		if status == s {
			si = i
		}
	}
	vi := len(severities) - 1
	if severity == "" {
		severity = "none"
	}
	for j, v := range severities[:vi] {
		if severity == v {
			vi = j
		}
	}
	return alertOpts[si][vi]
}
