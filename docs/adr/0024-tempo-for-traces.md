# ADR-0024：链路后端用 Grafana Tempo，不部署 SkyWalking

- 状态：已采纳（修改 ADR-0004 的“链路 → SkyWalking”一条，ADR-0004 的其余部分不变）
- 日期：2026-11-15

## 背景

ADR-0004 定的是业务代码只依赖 OpenTelemetry、统一用 OTLP 上报，链路的后端用 SkyWalking，理由是“SkyWalking 是明确想学的组件”，同时留了一个待验证项：OTLP 链路在 SkyWalking 里够不够用，不够要再接一个 skywalking-go 做对比。

阶段 1d 做完埋点之后情况变了：

- 本机的后端是 `otel-lgtm`，里面的链路存储就是 Tempo。1d 步骤六里拆落库耗时、查熔断器误触发，用的都是 Tempo 的 TraceQL 和 Prometheus 的连接池指标（设计文档 1d 的 8.16、8.18）。
- 诊断 Agent（阶段 3d 起）要对接每一个后端的查询接口。

## 决定

1. 集群上的链路后端用 Grafana Tempo，不部署 SkyWalking（OAP、BanyanDB、UI 都不要）。
2. 可观测的后端统一成 Grafana 这一套：Prometheus（指标，PromQL）、Loki（日志，LogQL）、Tempo（链路，TraceQL），Grafana 做界面。
3. 服务拓扑用 Tempo 的 metrics-generator 生成 service graph；它产生的指标进 Prometheus。
4. ADR-0004 里“验证 OTLP 链路在 SkyWalking 里是否够用”的待验证项取消。

## 理由

- **一套查询模型。** 三种信号都在 Grafana 里，用 trace id 互相跳转是内建的（从日志到链路、从指标的 exemplar 到链路）。Agent 的工具只需要三个结构相同的查询接口，证据之间靠 trace id 串起来。
- **本机和集群一致。** 本机已经在用 Tempo，查法、属性名、踩过的坑直接带到集群上，不用学两套。
- **内存。** 集群可分配的内存约 28 GB，是整个项目最紧的资源。原来给 SkyWalking OAP 加 BanyanDB 留了 4 GB；Tempo 单体模式要的少得多（具体多少阶段 3a 实测）。
- **少一个不确定项。** SkyWalking 对 OTLP 链路的支持和原生探针有差异，原计划要专门验证，不够还要双上报。Tempo 原生就是 OTLP。

## 放弃了什么

- SkyWalking 这个组件本身的经验（探针、OAP 的分析、它的拓扑和告警）。面试里被问到 SkyWalking 时只能讲原理和对比，讲不了实操。它能讲的东西（链路的数据模型、采样、拓扑从哪来、存储的取舍）用 OTel 加 Tempo 都能讲，而且 OTel 现在更常被问。
- 开箱即用的服务拓扑和服务级指标。Tempo 要打开 metrics-generator 才有，功能也少一些。

## 备选方案

- **保留 SkyWalking**：见上，代价是多一套查询接口、4 GB 内存和一个待验证项。
- **Jaeger**：只管链路，界面成熟。Agent 要多对接一套接口，和日志、指标之间的跳转要自己配。
- **Tempo 和 SkyWalking 都部署**：内存不够，而且两份链路数据没有必要。

## 后果

- `GOALS.md` 的技术栈、路线图、阶段 3 和阶段 5 的计划、内存预算、版本表都相应修改。
- Tempo 不建索引，只按 trace id 查是快的；按属性搜索（TraceQL）要扫块，数据量大了会慢。保留期和采样率要在阶段 3a 按实测定。
- Tempo 要对象存储或本地盘。集群上用本地盘还是 NAS 上的 MinIO，阶段 3a 决定。
