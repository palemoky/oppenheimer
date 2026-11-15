# 阶段 3：可观测、压测基线、SLO 与告警

**目标**：系统的状态看得见、说得清；知道它正常时长什么样、极限在哪里；出问题时有告警。
**设备**：Ubuntu（集群）、Mac mini（压测发起）
**开工前**：请主人打开 Mac mini。

## 3a 可观测三件套

- [ ] kube-prometheus-stack：Prometheus、Grafana、Alertmanager、node-exporter、kube-state-metrics
- [ ] OpenTelemetry Collector：服务统一用 OTLP 上报，由 Collector 分发指标、日志、链路（见 ADR-0004）
- [ ] Collector 的部署形态（DaemonSet 还是 Deployment）、批处理、采样、内存限制，以及对 Collector 自身的监控
- [ ] 业务服务指标接入 Prometheus，按 RED 方法整理：请求量、错误率、延迟
- [ ] 中间件 exporter：MySQL、Redis、Kafka（含消费延迟）
- [ ] 日志：结构化日志（JSON，带 trace id），Loki 收集
- [ ] 链路：Grafana Tempo（单体模式），由 Collector 经 OTLP 写入（ADR-0024，原计划是 SkyWalking）
  - 存储放本地盘还是 NAS 上的对象存储；保留期和采样率按实测的数据量定
  - 打开 metrics-generator，生成 service graph（服务拓扑）和 span 指标，写进 Prometheus
  - 尾部采样在 Collector 里做（只留出错的和慢的），和 1d 的头部采样对比（设计文档 1d 的 8.7）
- [ ] 链路打通 HTTP → gRPC → Kafka → MySQL、Redis
- [ ] Go 运行时指标与持续剖析入口：goroutine 数、堆、GC、pprof 端点（阶段 4 的泄漏类故障要用）
- [ ] 指标、日志、链路通过 trace id 互相跳转
- [ ] Grafana 大盘：业务总览、单服务、中间件、集群资源
- [ ] 监控数据保留策略与资源占用评估（内存紧张，见 hardware.md）
- [ ] 按 QPS 的 HPA（gateway、order）：prometheus-adapter 或 KEDA 提供自定义指标（从 2c 挪过来的，设计文档 2c 第 9 节）

## 3b 压测

- [ ] 评估并决定是否从 OpenAPI 规范（`api/openapi/gateway.yaml`，见 ADR-0005）生成客户端：k6 脚本的请求结构、`test/e2e` 里手写的请求和响应结构都可以由规范生成。这是「1a 补充」推迟过来的任务，做或不做都要在这里写明结论
- [ ] k6 脚本：普通下单、秒杀、混合读写场景
- [ ] 压测数据准备与清理工具，压测流量标记
- [ ] 基线测试：记录各接口在正常负载下的吞吐与延迟分位数
- [ ] 阶梯加压找拐点，定位第一个瓶颈
- [ ] 至少完成三轮「压测 → 定位瓶颈 → 优化 → 再压测」，每轮写记录（pprof、火焰图、慢查询、连接池等）
- [ ] 长稳测试：中等负载持续数小时，观察内存泄漏、goroutine 泄漏
- [ ] 容量结论：当前硬件下各服务的单副本能力与扩容系数

## 3c SLO 与告警

- [ ] 定义 SLI 和 SLO：下单可用性、下单延迟、支付回调处理时延
- [ ] 错误预算与多窗口燃烧率告警
- [ ] 基础设施告警：节点、Pod 重启、磁盘、主从延迟、Kafka 消费积压、Redis 内存
- [ ] 业务告警：下单成功率骤降、对账差异、库存异常
- [ ] Alertmanager 路由、分组、抑制、静默；通知渠道（webhook，为阶段 5 的 Agent 留接口）
- [ ] 每条告警配一份 runbook（`docs/runbooks/`）
- [ ] 告警质量检查：人为触发每条告警，确认能响且不误报

## 3d 最小诊断 Agent

提前做一个最小版本，让它跟着阶段 4 的每次故障实验一起跑、一起迭代，而不是等实验全做完再开始。完整设计见 [phase-5-agent.md](phase-5-agent.md)。

动工前先查最新的 Claude API 和 Agent SDK 文档。

- [ ] Agent 主循环（Go + Claude API 工具调用）：多轮推理、终止条件、步数与费用上限
- [ ] 三个只读工具：Prometheus 查询（PromQL 即时与范围）、Loki 日志查询、Kubernetes（get、describe、events、logs）
  - Kubernetes 工具从阶段 5 提前到这里（2026-11-15）：阶段 4 的故障里有一大类是 Pod 被杀、OOM、节点失联、调度不上，只有指标和日志时 Agent 只能从数据的缺口去猜
  - 用 client-go，单独的 ServiceAccount，只给 get、list、watch；不给 exec、不给任何写权限，权限写在清单里入库
- [ ] 工具输出的裁剪，控制上下文长度
- [ ] 触发方式：Alertmanager webhook 和命令行手动提问
- [ ] 输出 Markdown 报告：根因判断、证据（每条结论附查询和结果）、置信度
- [ ] 全过程留痕：每次运行的工具调用、token 用量、耗时落盘，可回放
- [ ] 用 3c 里人为触发告警的场景试跑几次，确认链路通

这一版不追求准确率，目标是把骨架和留痕做好，阶段 4 开始就能积累真实运行记录。

## 验收标准

- 任意一个慢请求，能从大盘下钻到链路再到日志，定位到具体的服务和调用。
- 有书面的压测基线和容量结论，`docs/GOALS.md` 里的目标量级据此修正。
- 每条告警都被真实触发验证过，并有对应 runbook。
- 告警触发后，最小 Agent 能自动产出一份带证据的报告，运行记录可回放。

## 面试考点

OpenTelemetry 的架构（API、SDK、Collector、OTLP）与上下文传播；指标、日志、链路各自解决什么问题；Prometheus 拉模型、数据模型、PromQL、直方图与分位数；链路追踪的上下文传播与采样；SLI、SLO、错误预算；告警疲劳与如何设计好告警；压测方法论与常见瓶颈；Go 性能分析（pprof、trace、GC 调优）。
