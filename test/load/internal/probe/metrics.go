package probe

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// step 是一个周期里的一步。
type step int

const (
	stepStock step = iota
	stepCreate
	stepLand
	stepPay
	stepPaid
	stepCancel
	stepList
	numSteps
)

var stepNames = [numSteps]string{"stock", "create", "land", "pay", "paid", "cancel", "list"}

func (s step) String() string { return stepNames[s] }

// waiting 报告这一步是不是“等结果”：它的耗时是排队的时间，可能几十秒，进 .delay 的直方图。
func (s step) waiting() bool { return s == stepLand || s == stepPaid }

// outcome 是一步的结果。
type outcome int

const (
	outcomeOK outcome = iota
	// outcomeRejected 是系统有意的拒绝：429（限流、背压、读名额满）。
	outcomeRejected
	// outcomeSoldOut 是下单时 SKU 卖不了：库存不足，或者超出限购（SKU 的配置不适合探针）。
	outcomeSoldOut
	// outcomeDegraded 是 503 且带降级的原因码（不是 UNAVAILABLE）。
	outcomeDegraded
	// outcomeError 是其他 5xx 和 4xx、连接失败、响应解析不了、订单状态不符合预期。
	outcomeError
	// outcomeTimeout 是单个请求超时，或者轮询到时限还没等到结果。
	outcomeTimeout
	numOutcomes
)

var outcomeNames = [numOutcomes]string{"ok", "rejected", "sold_out", "degraded", "error", "timeout"}

func (o outcome) String() string { return outcomeNames[o] }

// failed 报告这个结果是否说明系统坏了。rejected、sold_out、degraded 是系统按设计做出的反应，不算。
func (o outcome) failed() bool { return o == outcomeError || o == outcomeTimeout }

// cycleOutcome 是一个周期的结果。
type cycleOutcome int

const (
	cycleOK cycleOutcome = iota
	// cycleIncomplete 是有一步被系统有意拒绝（rejected、sold_out、degraded），后面的步骤没有做。
	cycleIncomplete
	cycleFailed
	// cycleSkipped 是在途的周期到了上限，这一拍没有发。
	cycleSkipped
	numCycleOutcomes
)

var cycleNames = [numCycleOutcomes]string{"ok", "incomplete", "failed", "skipped"}

func (c cycleOutcome) String() string { return cycleNames[c] }

// 指标在包级变量里建一次（设计文档 1d 的 8.8）。标签只有上面三组有限的枚举，订单号、用户号、SKU 只进日志。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/test/load/internal/probe")

	stepsTotal = otelx.Counter(meter, "probe.steps",
		"Probe steps by step and outcome (ok, rejected, sold_out, degraded, error, timeout).")
	// stepDuration 是一次请求的耗时（stock、create、pay、cancel、list），只记 ok 的：
	// 被 429 立刻拒绝的会把分位数拉低，超时的全堆在超时值上，它们的数量看 probe.steps。
	stepDuration = otelx.Histogram(meter, "probe.step.duration",
		"Latency of successful probe requests, by step.")
	// stepDelay 是等待的两步（land、paid）从上一步成功到等到结果的时间。名字以 .delay 结尾，
	// otelx.DurationView 给它用 WaitBuckets（到 30 分钟）：落库积压时是几十秒，DurationBuckets 只到 10 秒。
	stepDelay = otelx.Histogram(meter, "probe.step.delay",
		"Time from the previous step's success to the awaited order status (land, paid), successful waits only.")
	cyclesTotal = otelx.Counter(meter, "probe.cycles",
		"Probe cycles by outcome (ok, incomplete, failed, skipped).")
)

// 每种标签组合的选项预先建好，热路径上不分配。
var (
	stepOutcomeOpts [numSteps][numOutcomes]metric.MeasurementOption
	stepOpts        [numSteps]metric.MeasurementOption
	cycleOpts       [numCycleOutcomes]metric.MeasurementOption
)

func init() {
	for s := range numSteps {
		sa := attribute.String("step", s.String())
		stepOpts[s] = metric.WithAttributes(sa)
		for o := range numOutcomes {
			stepOutcomeOpts[s][o] = metric.WithAttributes(sa, attribute.String("outcome", o.String()))
		}
	}
	for c := range numCycleOutcomes {
		cycleOpts[c] = metric.WithAttributes(attribute.String("outcome", c.String()))
	}
}
