package main

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/services/agent/internal/loop"
)

// 标签都是有限的枚举：触发方式、结束原因、跳过的原因、工具名（登记过的那几个）、调用结果、token 的种类。
// 告警名、运行号只进日志和运行记录。一次诊断要几十秒到几分钟，这里不是热路径，标签现用现建。
var (
	meter = otelx.Meter("github.com/palemoky/oppenheimer/services/agent")

	runsTotal    = otelx.Counter(meter, "agent.runs", "Diagnosis runs by trigger (alert, ask) and outcome.")
	skipsTotal   = otelx.Counter(meter, "agent.triggers.skipped", "Triggers that did not start a run, by reason.")
	toolCalls    = otelx.Counter(meter, "agent.tool.calls", "Tool calls by tool and result (ok, error, timeout).")
	tokensTotal  = otelx.Counter(meter, "agent.tokens", "Tokens by kind (input, output, cache_write, cache_read).")
	turnsTotal   = otelx.Counter(meter, "agent.run.turns", "Model turns across all runs.")
	runDuration  = otelx.Histogram(meter, "agent.run.duration", "Wall time of one diagnosis run in seconds.")
	costMicroUSD = otelx.Counter(meter, "agent.cost.micro_usd", "Estimated model cost in millionths of a US dollar.")
)

func recordSkip(ctx context.Context, reason string) {
	skipsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func recordRun(ctx context.Context, trigger string, res *loop.Result) {
	runsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("trigger", trigger), attribute.String("outcome", string(res.Outcome))))
	for name, byResult := range res.Calls {
		for result, n := range byResult {
			toolCalls.Add(ctx, int64(n), metric.WithAttributes(
				attribute.String("tool", name), attribute.String("result", result)))
		}
	}
	for kind, n := range map[string]int64{
		"input": res.Usage.Input, "output": res.Usage.Output,
		"cache_write": res.Usage.CacheWrite, "cache_read": res.Usage.CacheRead,
	} {
		tokensTotal.Add(ctx, n, metric.WithAttributes(attribute.String("kind", kind)))
	}
	turnsTotal.Add(ctx, int64(res.Turns))
	runDuration.Record(ctx, res.Duration.Seconds())
	costMicroUSD.Add(ctx, int64(res.CostUSD*1e6))
}
