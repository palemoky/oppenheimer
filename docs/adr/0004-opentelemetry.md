# ADR-0004：用 OpenTelemetry 做埋点标准

- 状态：已采纳。其中“链路 → SkyWalking”一条已被 [ADR-0024](0024-tempo-for-traces.md) 修改为 Tempo（2026-11-15），下面的“待验证”一节随之取消；正文保留当时的原样
- 日期：2026-11-11

## 背景

最初的方案是 Prometheus client 打指标、skywalking-go 做链路。这样埋点代码与具体后端绑定，而 OpenTelemetry 已经是可观测埋点的事实标准，面试中也是高频话题。

## 决定

- 业务代码只依赖 OpenTelemetry Go SDK：链路和指标用 OTel API，日志用 `slog` 并注入 trace id 和 span id。
- 集群里部署 OpenTelemetry Collector，服务统一用 OTLP 上报，由 Collector 分发：
  - 指标 → Prometheus
  - 日志 → Loki
  - 链路 → SkyWalking OAP（通过它的 OTLP 接收器）
- SkyWalking 保留，作为链路后端和拓扑分析工具。

## 备选方案

- **skywalking-go 原生探针**：编译期自动插桩，接入省事，SkyWalking 的原生功能最完整。但埋点与后端绑定，换后端要重新接入。
- **去掉 SkyWalking，换 Tempo 或 Jaeger**：与 OTel 配合更直接，但 SkyWalking 是明确想学的组件。

## 待验证

SkyWalking 对 OTLP 链路的支持程度与原生探针有差异（展示方式、拓扑和服务指标的完整度）。阶段 3a 需要实测：

1. OTLP 链路在 SkyWalking 里能否满足排查需要。
2. 若不能，选一个服务同时接 skywalking-go 做对比，再决定是双上报、换链路后端，还是接受差异。

结论补充到本文。

## 后果

- 埋点代码与后端解耦，后端可替换。
- 多一个 Collector 需要部署和占用内存（约 0.5GB）。
- Collector 本身成为故障点，要纳入监控，也可以作为阶段 4 的一个实验对象。
- 阶段 1 就按 OTel 埋点，本地 compose 可以先只导出到标准输出或一个轻量后端。
