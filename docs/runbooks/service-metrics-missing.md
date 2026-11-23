# 某个服务没有指标了

覆盖的告警：`ServiceMetricsMissing`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.telemetry` 组。出处：[ADR-0030](../adr/0030-observability-backends.md)、设计 [3c](../design/3c-slo-alerting.md) 第 1.3 节第 2 条。

## 这条告警在说什么

指标是服务自己用 OTLP 推给 Collector 的，没有 `up` 这个序列（ADR-0030）。服务挂了、推不出去时，序列停在最后一个值上然后消失，上面所有按指标算的规则（SLO、业务告警）都没有值、都不响。所以这条每个服务挑一个只要进程活着就一定在报的运行时指标 `go_goroutine_count`，按清单显式地查：`gateway`、`order`、`inventory`、`payment`、`orderindex`、`cdc`，任何一个 `absent(...)` 就报，持续 5 分钟。加上序列过期的 5 分钟，所以通知里写“10 分钟没有报任何指标”。

清单是写死的：加服务时规则要跟着加。它**不覆盖**探针（`ProbeNoData` 管）和 `recon-totals`（`PaymentTotalsCheckStalled` 管）。

## 影响

这个服务的指标没有了，按它的指标算的告警在这段时间不会响，是“监控瞎了”，不是业务本身一定坏了。但服务整个不在时业务当然也坏了。当天看；如果同时有别的告警说同一个服务不可用，先处理那个。

## 先看什么

1. 哪个服务没有了，是一个还是全部：
   `scripts/cluster/monitoring.sh promql 'count by (service_name) (go_goroutine_count{k8s_namespace_name="oppenheimer"})'`
2. 全部没有的话是 Collector 的问题：`scripts/cluster/monitoring.sh alerts` 看 `OtelGatewayDown`（critical，它先响，Alertmanager 用它抑制这条）、`OtelCollectorRefusing`、`OtelCollectorExportFailing`。`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring get pods | rg otel`
3. 只有一个服务：Pod 在不在、有没有重启、卡在什么：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide`
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer describe pod <Pod 名>`（看上次终止的原因：OOMKilled 等）
4. 服务的日志（有没有“导出失败”的报错，指标推不出去）：`{service_name="<服务>"} | json | level=~"WARN|ERROR"`；`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer logs deploy/<服务> --tail=100`
5. 服务本身通不通（业务探针）：`scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(probe_cycles_total{k8s_namespace_name="oppenheimer"}[5m]))'`；Prometheus 的抓取目标：`scripts/cluster/monitoring.sh status`。

## 常见的原因和各自的处理

- **服务整个不在（Pod 被杀、OOMKilled、CrashLoop）。** 3b 的长稳 4 小时里没有容器重启（round-9）；OOMKilled 的实例见 3a（Tempo 在压测中被 OOMKilled 过，round-1 第 5 节）。处理：看上次终止的原因，恢复副本。
- **Collector 挂了或拒收。** 六个服务的指标一起没有，`OtelGatewayDown` 先响（for 2 分钟，比各条“没有数据”短）并抑制这条。3a 记录：Collector 的 `send_failed_spans` 累计 120 多万（导出到 Tempo）、`refused_spans` 累计 14 万，是压测那两天留下的，没有回头查哪一天（设计 8）。处理：重启 Collector 前先看日志和内存限制器；服务本身不受影响（导出失败只丢遥测）。
- **服务推不到 Collector（网络、网格）。** 推断，没有遇到过：只有一个服务没有指标而 Pod 是好的，看它的日志和到 Collector 的连通。
- **新加了服务或改了服务名。** 清单是写死的；改名后旧名字的 `absent` 会一直报。处理：改规则（`deploy/cluster/monitoring/rules/business.yaml`），`make rules-test`，`scripts/cluster/monitoring.sh rules`。

## 怎么确认恢复了

- `scripts/cluster/monitoring.sh promql 'count by (service_name) (go_goroutine_count{k8s_namespace_name="oppenheimer"})'` 里六个服务都在。
- 按这个服务的指标算的告警恢复求值：例如 `scripts/cluster/monitoring.sh promql 'slo:order_create_availability:error_ratio_rate5m'` 有值。
- 服务不在的期间可能漏了告警：这段时间里订单、库存、钱有没有出事，以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节没有专门列。待定，建议：把一个不要紧的服务缩到 0，例如 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer scale deploy/orderindex --replicas=0`，序列 5 分钟过期加 5 分钟的 `for` 后响（会同时触发 `ConsumerNotConsuming`、`CDCLagging` 等，做的时候看抑制是否符合预期），做完缩回 2。停 Collector 会触发 `OtelGatewayDown` 并抑制这条，用来检验抑制。没有验证过。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
