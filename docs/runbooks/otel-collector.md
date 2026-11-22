# OpenTelemetry Collector

覆盖的告警：`OtelGatewayDown`（critical）、`OtelCollectorRefusing`（warning）、`OtelCollectorExportFailing`（warning）

## 这条告警在说什么

Collector 在命名空间 `monitoring`：`otel-gateway`（Deployment，一个副本，收六个服务、探针、对账推来的 OTLP 再转发给 Prometheus、Tempo、Loki）和 `otel-agent`（DaemonSet，每个节点读容器日志，Pod 名 `otel-agent-agent-*`）。服务的指标是**推**上来的（[ADR-0030](../adr/0030-observability-backends.md)），所以它不在时服务一侧没有 `up`。Collector 自己的指标（8888 端口）由 Prometheus 直接抓（`job="monitoring/otel-gateway-agent"` 是 gateway）。

- `OtelGatewayDown`：`absent(up{job="monitoring/otel-gateway-agent"} == 1)`，2 分钟。没有一个就绪的抓取目标。`for` 比各条“没有数据”（5 到 10 分钟）短，让它先响。
- `OtelCollectorRefusing`：receiver 的 `otelcol_receiver_refused_{spans,metric_points,log_records}` 在 10 分钟内有增长（三种取最大），`for: 2m`。含“从不存在到第一次出现”那一半（`x > 0 unless x offset 10m`）。一般是内存限制器（`memory_limiter`，限额是容器上限 1Gi 的 80%，突发 20%）在起作用，被拒的数据客户端会重试或丢弃。
- `OtelCollectorExportFailing`：exporter 的 `otelcol_exporter_send_failed_{spans,metric_points,log_records}` 在 10 分钟内有增长，`for: 2m`。往后端（Tempo、Prometheus、Loki）发失败，数据在队列里积压或被丢。

出处：[3a 笔记第 10 节](../notes/3a-observability.md)（Collector 停机实验）、[设计 1.3 第 2 条、4.4](../design/3c-slo-alerting.md)。

抑制：`OtelGatewayDown` 在响时，`ServiceMetricsMissing`、`ProbeNoData`、`CDCPipelineMissing`、`PaymentTotalsCheckStalled`、`AlertDeliveryStalled` 这些“没有数据”的告警不通知（它们是它的结果）。

## 影响

- **`OtelGatewayDown` 响着时，业务告警和 SLO 是瞎的**：序列不进 Prometheus，按阈值写的业务规则和燃烧率不会响。这时出了业务问题**没有告警**，要靠自己看（探针、Telegram 之外的渠道）。这是 critical 的原因。
- 服务本身不受影响：导出失败只丢遥测（3a 实验：60855 个请求 0 失败，延迟不变）。
- 丢什么（3a 第 10 节）：指标是累计值，下一次送到的数里包含了中间丢的，图上断了总数不丢；**链路丢了**，补不回来（SDK 队列 2048 个 span，满了丢）；日志在磁盘上，改完配置后不丢，gateway 回来后补齐。
- 拒收和导出失败（warning）：大盘和 SLO 出现缺口，对用户无影响。当天看。

## 先看什么

1. 哪一条、哪个 Collector：

   ```
   scripts/cluster/monitoring.sh promql 'up{job=~"monitoring/otel-.*"}'
   scripts/cluster/monitoring.sh promql 'increase(otelcol_receiver_refused_spans[10m])'
   scripts/cluster/monitoring.sh promql 'increase(otelcol_exporter_send_failed_spans[10m])'
   scripts/cluster/monitoring.sh promql 'increase(otelcol_exporter_send_failed_log_records[10m])'
   ```
   把 `spans` 换成 `metric_points`、`log_records` 逐个看，标签里有 `receiver` 和 `exporter`。
2. Pod 状态、有没有重启或 OOMKilled：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring get pods -o wide | rg otel
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring describe pod <otel-gateway 的 Pod 名> | rg -A8 'Last State|Events'
   ```
3. Collector 日志（`Dropping data`、`ingestion rate limit exceeded`、`memory usage is above`）：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring logs deploy/otel-gateway --tail=100
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring logs ds/otel-agent-agent --tail=100
   ```
4. 失败的是哪个后端，它自己还活着吗：`scripts/cluster/monitoring.sh status`（不健康的抓取目标、Pod 在哪个节点）；`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring get pods | rg 'tempo|loki|prometheus'`。
5. 内存：`scripts/cluster/monitoring.sh promql 'container_memory_working_set_bytes{namespace="monitoring",pod=~"otel-gateway.*"}'`，对照上限 1Gi。

## 常见的原因和各自的处理

1. **gateway 被缩掉、被重启或 OOMKilled。** 3a 验证 5 里自己缩到 0 过（5 分钟后改回 1）：`kubectl -n monitoring scale deploy/otel-gateway --replicas=1`。必须是 1 个副本：尾部采样要看到一条链路的全部 span，多副本要在前面加按 trace id 分流的一层（没做）。计划内停之前先静默 `OtelGatewayDown`：`scripts/cluster/monitoring.sh silence 'alertname=OtelGatewayDown' 30m '计划内停 gateway'`。
2. **Loki 限额拒了日志。** 3a 第 10 节：agent 把积压的日志一起发，gateway 合成 6.9 MB 的一批，超过 Loki 的突发上限，永远 429，5 分钟后丢。已改：gateway 日志管线单独 `batch/logs`，每批最多 2000 条；Loki 限额放宽到每秒 16 MB、突发 32 MB。日志里再出现 `ingestion rate limit exceeded` 先看这一条。
3. **agent 的队列堵满。** 队列改成堵住（`block_on_overflow`）后，agent 内存到过 204 Mi（上限 256 Mi，余量不大，3a 留下的问题）；gateway 回来后 `filelog` 追读，每批超过 gateway 的 gRPC 接收上限 4 MiB 会整批丢（已限批次 2000 条）。
4. **后端（Tempo）挂了或过载。** `send_failed_spans` 累计 120 多万（导出到 Tempo），`refused_spans` 累计 14 万，是压测那两天留下的，**没有回头查是哪一天、丢了多少**（设计第 8 节）。过载时 Tempo 被 OOMKilled 过（上限 2 Gi，PROGRESS 里 3b 夜里的记录）。先让后端活过来。
5. **内存限制器拒收。** gateway 在大流量下，`memory_limiter` 拒收。`memory_limiter` 的拒收在压力下的行为没有专门压过（3a 笔记 15.3）。降低流量或把 `limits` 提到更高（改 `deploy/cluster/monitoring/otel-gateway.yaml`，`scripts/cluster/monitoring.sh collector`）。
6. **链路在 Collector 停机时必丢**：不是故障，不用补，链路本来就是采样的（3a 第 10 节）。

## 怎么确认恢复了

- `scripts/cluster/monitoring.sh promql 'up{job="monitoring/otel-gateway-agent"}'` 是 1；`OtelGatewayDown` 在下一个求值周期消失，被它抑制的“没有数据”类告警也随之恢复或自己消失。
- 拒收、导出失败：`increase(...[10m])` 回到 0，窗口过去后告警消失（`increase` 的窗口是 10 分钟）。
- gateway 回来后看业务序列回来了：服务的指标在大盘上续上、`ProbeNoData` 这类被抑制的告警没有留下；日志的补齐要等几分钟。
- Collector 不丢业务数据，所以不需要对账；但这期间发生的业务问题没有告警记录，回看这段时间的探针和对账结果：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)`，差异应为 0。

## 怎么人为触发它

按[设计 5 节第 2 层](../design/3c-slo-alerting.md)的“可用性”一类办法，3a 验证 5 做过 Collector 停机：`kubectl -n monitoring scale deploy/otel-gateway --replicas=0`，2 分钟后 `OtelGatewayDown` 响，再缩回 1；同时验证抑制（`ProbeNoData` 等不通知）。`OtelCollectorExportFailing`：停掉 Tempo 或 Loki（设计 3a 把“停 Loki 时看 gateway 到 Loki 的重试”留给阶段 4，待定，会丢数据）。`OtelCollectorRefusing`：把 `memory_limiter` 的限额临时调低，或加压（待定，没有专门压过）。不想真的弄坏时，改阈值验证“路是通的”并写明。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
