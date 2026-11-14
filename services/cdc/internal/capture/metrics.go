package capture

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/otelx"
)

// CDC 的指标（设计文档 1d 的 8.8）。标签只有流水线名（配置里有限的几条）和 op。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/cdc/internal/capture")

	events = otelx.Counter(meter, "cdc.events",
		"交给 Kafka 生产者的行变更数（异步发送，不等确认），按流水线和 op（insert、update、delete、read=全量）分")
)

// lagDesc 是 cdc.lag_ms 的描述。
const lagDesc = "同步延迟（毫秒）：最后一个读完的事务在源库的提交时间到现在的时长。" +
	"已知的局限：源库没有写入时这个数会一直涨（没有新事件不代表落后），要等源库心跳（后面的阶段）。" +
	"流水线还没读完过任何一个带提交时间的事务时（启动后、全量期间）报 0"

// newEventOptions 建好一条流水线各个 op 的标签，热路径上不分配。
func newEventOptions(pipeline string) map[cdcv1.Op]metric.AddOption {
	o := make(map[cdcv1.Op]metric.AddOption, len(cdcv1.Op_name))
	for v, name := range cdcv1.Op_name {
		op := strings.ToLower(strings.TrimPrefix(name, "OP_"))
		o[cdcv1.Op(v)] = metric.WithAttributes(attribute.String("pipeline", pipeline), attribute.String("op", op))
	}
	return o
}

// countEvent 记一条交给生产者的变更。
func (p *Pipeline) countEvent(ctx context.Context, op cdcv1.Op) {
	if opt, ok := p.eventOpts[op]; ok {
		events.Add(ctx, 1, opt)
	}
}

// recordCommit 记下刚读完的事务在源库的提交时间（微秒），给 cdc.lag_ms 用。没有提交时间（为 0）时不记。
func (p *Pipeline) recordCommit(commitMicros int64) {
	if commitMicros > 0 {
		p.lastCommitMicros.Store(commitMicros)
	}
}

// lagMillis 是 cdc.lag_ms 的读数，在导出的 goroutine 里调用。
func (p *Pipeline) lagMillis() int64 {
	last := p.lastCommitMicros.Load()
	if last <= 0 {
		return 0
	}
	return max(time.Now().UnixMicro()-last, 0) / 1000
}
