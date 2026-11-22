# 常驻探针：下单到支付这条路断了，或探针自己没数据

覆盖的告警：`ProbeFailing`（critical）、`ProbeIncomplete`（warning）、`ProbeNoData`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.probe` 组。设计：[3c-slo-alerting.md](../design/3c-slo-alerting.md) 第 3.6 节、第 8 节“第 2 步的另一半”“探针上线”。

## 这条告警在说什么

探针是集群里常驻的一个 Deployment（`probe`，命名空间 `oppenheimer`，放在 k3s-server 上），从入口的地址进来，每秒约一次“查库存、下单、轮询到落库、支付（每 10 个周期改成取消）、轮询到已支付”，每 5 个周期查一次订单列表。清单 `deploy/cluster/probe/deployment.yaml`，`make probe` 安装。

- **`ProbeFailing`**：`probe_cycles_total` 里 `outcome` 是 `failed` 或 `skipped` 的 5 分钟速率占全部周期的比例超过 0.5，持续 3 分钟。算上 `skipped` 是因为周期卡住不结束时新周期因在途上限被跳过，failed 很少而 skipped 很多，同样是路不通。`incomplete` 不算（见下一条）。平时 90 分钟 5399 个周期全部是 `ok`（设计 8“探针下的基线”）。阈值取一半而不是“有失败就响”：单个周期失败由 SLO 的燃烧率管，这条只管“断了”。
- **`ProbeIncomplete`**：`outcome="incomplete"`（被 429 拒绝、售罄、带降级原因码的 503）占全部周期的比例超过 0.5，持续 10 分钟。不是出错，但探针走不到落库和支付，那两条 SLO 这段时间没有数据。2026-11-23 从 `ProbeFailing` 里拆出来的：背压的 429 按 SLO 的定义不是坏的，售罄是探针自己的配置问题，都不该报成“路断了”。
- **`ProbeNoData`**：5 分钟的周期速率是 0（或没有序列），而 `kube_deployment_spec_replicas{deployment="probe"}` 大于 0，持续 10 分钟。正式压测前 `probe-stop` 把副本数缩到 0 时不响。

`outcome` 的含义：`ok`；`incomplete`（被拒 429、售罄、降级）；`failed`（有一步是 error 或 timeout）；`skipped`。

## 影响

`ProbeFailing`：入口走一遍下单到支付的路，一半以上不成功，说明这条路对真实用户也是断的，即使 SLO 的规则因没有流量而算不出来。几分钟内要看。

`ProbeNoData`：没人在盯“路断了没有”；没有流量时 SLO 的规则算不出来，CDC 的延迟指标也没有源库写入可量，几条规则同时变瞎（规则注释）。当天看。

## 先看什么

1. 周期的结局分布和断在哪一步：
   `scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(probe_cycles_total{k8s_namespace_name="oppenheimer"}[5m]))'`
   `scripts/cluster/monitoring.sh promql 'sum by (step, outcome) (rate(probe_steps_total{k8s_namespace_name="oppenheimer"}[5m]))'`
2. 探针 Pod 在不在、有没有重启、日志：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide | rg probe`
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs deploy/probe --tail=50`
3. 下单的路本身：`scripts/cluster/monitoring.sh promql 'slo:order_create_availability:error_ratio_rate5m'`，同时看有没有 SLO 告警在响（`scripts/cluster/monitoring.sh alerts`，见 [slo-order-create-availability.md](slo-order-create-availability.md)）。
4. 如果是 `ProbeNoData`：先看别的服务的指标有没有：`scripts/cluster/monitoring.sh promql 'count by (service_name) (go_goroutine_count{k8s_namespace_name="oppenheimer"})'`。都没有是 Collector 的问题（看 `OtelGatewayDown`），只有 probe 没有是探针自己。
5. 入口：`IngressGateway5xxHigh`、`IngressRejectingRequests` 有没有在响（探针从入口进，限流、入口上限都会让它失败）。

## 常见的原因和各自的处理

- **下单的路真的断了**（gateway、order、inventory、payment 之一不可用，或 Redis 闸门、MySQL 不可用）：按 `probe_steps_total` 的 `step` 定位。`create` 失败看 gateway、order、闸门；`land` 失败看落库（[order-backlog.md](order-backlog.md)、[consumer-blocked.md](consumer-blocked.md)）；`pay`、`paid` 失败看 payment 和支付到订单的链路（[outbox.md](outbox.md)）。
- **探针自己的配置坏了，不是路断了。** 设计 8“第 2 步的另一半”第 1 点：SKU 卖完、没建、没装进闸门时是 `sold_out` 或 `error`。探针的 SKU 是 9001 到 9010，各 10 亿库存、限购 0；限购不是 0 会被记成 `sold_out`。处理：`scripts/cluster/loadtest.sh skus 9001 10` 重建。这种情况现在报成 `ProbeIncomplete`（售罄）；SKU 没建、没装进闸门时是 `error`，仍然算进 `ProbeFailing`。
- **探针被入口限流拒绝（429，记成 `rejected`、周期 `incomplete`）。** 压测时如果发压机和探针出口是同一个地址会共用按地址的限流；入口看到的各是什么地址没有核对（设计 8）。
- **背压在拒绝（`BACKLOGGED` 也是 429）。** 过拐点时探针周期会变成 `incomplete`，报成 `ProbeIncomplete`（warning），这是保护在起作用，不是路断了（推断，没有看到过这样响）。用 `sum by (step, outcome) (rate(probe_steps_total{k8s_namespace_name="oppenheimer",outcome!="ok"}[5m]))` 分清是 `rejected`、`sold_out` 还是 `degraded`。
- **Collector 挂了。** 探针的指标也没有，`ProbeNoData` 会响；靠抑制（Collector 的告警抑制服务的“没有数据”，设计 3.5）。
- **探针进程卡住。** `skipped` 持续增长说明探针或入口卡住了（设计 8）。处理：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer rollout restart deploy/probe`。
- **压测前后忘了恢复。** `scripts/cluster/services.sh probe-stop`、`probe-start`。副本数是 0 时 `ProbeNoData` 不响；停着忘了开，其他规则会变瞎，这条帮不上。
- 镜像 tag 是工作区的，提交之后要再 `make probe` 换成干净的（设计 8）。

## 怎么确认恢复了

- `ProbeFailing`：失败比例回到 0 并持续 5 分钟以上：`scripts/cluster/monitoring.sh promql '(sum(rate(probe_cycles_total{k8s_namespace_name="oppenheimer",outcome=~"failed|skipped"}[5m])) or vector(0)) / sum(rate(probe_cycles_total{k8s_namespace_name="oppenheimer"}[5m]))'`。
- `ProbeNoData`：`scripts/cluster/monitoring.sh promql 'sum(rate(probe_cycles_total{k8s_namespace_name="oppenheimer"}[5m]))'` 约 1（每秒一个周期）。
- 探针的订单也是订单，钱和库存的收尾照样是对账差异为 0：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节没有专门列探针，待定，建议：

- `ProbeFailing`：把探针的 SKU 换成不存在的号（改 Deployment 的 SKU 配置，`make probe` 重装），周期变成 `error` 或 `sold_out`；或者像 SLO 那样把 inventory 缩到 0（会同时触发可用性的告警）。
- `ProbeNoData`：`probe-stop` 不会触发（副本数 0 时规则不响）。建议让探针 Pod 活着但指标送不出去，例如停 Collector（会同时触发 `OtelGatewayDown`、`ServiceMetricsMissing`，顺便检验抑制）。没有验证过。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
