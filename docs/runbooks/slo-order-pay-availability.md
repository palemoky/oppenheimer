# 支付可用性的错误预算在烧

覆盖的告警：`SLOOrderPayAvailabilityBurnFast`（critical）、`SLOOrderPayAvailabilityBurnMedium`（critical）、`SLOOrderPayAvailabilityBurnSlow`（warning）

规则：[slo.yaml](../../deploy/cluster/monitoring/rules/slo.yaml) 的 `slo.order_pay_availability` 组。出处：[ADR-0037](../adr/0037-slo-and-burn-rate-alerts.md)、设计 [3c-slo-alerting.md](../design/3c-slo-alerting.md) 第 3.2、3.3 节和第 8 节“第 4a 项”。

## 这条告警在说什么

SLI：gateway 上 `POST /api/v1/orders/{order_id}/pay` 的 5xx 占全部请求的比例。499 不计入；409（订单已经不是待支付）、429、其他 4xx 都算好的。目标 99.9%（7 天），预算 0.001。指标里的 `http_route` 是 gin 的写法：`/api/v1/orders/:order_id/pay`。

它是 2026-11-23 加的第五个 SLO：payment 整个不在的 26 分钟里 `/pay` 一直返回 503，原来的四个 SLO 一个都没响（可用性只看下单的接口，支付处理时延要有支付结果才有观测值）。

三档的窗口、倍数、`for` 和下单可用性完全一样，表见 [slo-order-create-availability.md](slo-order-create-availability.md)。通知里的 `$value` 是短窗口的比例。没有流量时不响；探针每单都付款，平时每秒约 1 次。

**它和支付处理时延的 SLO 管的是两段**：这一条管“付款的请求有没有被接下来”（同步的一步，建支付单），[slo-order-payment-delay.md](slo-order-payment-delay.md) 管“支付出了结果之后订单多久变成已支付”（异步的一步）。

## 影响

用户点了付款拿到 5xx，支付单没有建出来，钱没有动。订单留在待支付，15 分钟的时限到了会被关单、放回预占的库存。用户可以重试（支付的请求是幂等的）。快的一档几分钟内要看：拖过 15 分钟，这段时间下的单会成批关掉。

## 先看什么

1. 现在多糟：
   `scripts/cluster/monitoring.sh promql 'slo:order_pay_availability:error_ratio_rate5m'`
2. 是哪个状态码：
   `scripts/cluster/monitoring.sh promql 'sum by (http_response_status_code) (rate(http_server_request_duration_seconds_count{service_name="gateway",k8s_namespace_name="oppenheimer",http_route="/api/v1/orders/:order_id/pay",http_request_method="POST"}[5m]))'`
3. payment 在不在：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods | rg payment`；`scripts/cluster/monitoring.sh alerts` 里有没有 `ServiceMetricsMissing{payment}`、`MeshEndpointEjected`、`MySQLDown`。
4. 同时在响的：`ProbeFailing`、`IngressGateway5xxHigh` 多半和它一起响（第 4a 项里就是这两条先发的 Telegram）。见 [probe.md](probe.md)。
5. gateway 和 payment 的错误日志：`{service_name="gateway"} | json | level="ERROR"`、`{service_name="payment"} | json | level="ERROR"`。

## 常见的原因和各自的处理

- **payment 整个不可用（真做过，第 4a 项）。** `/pay` 全部是 503，探针的周期全部失败。处理：恢复副本数；恢复后 5 分钟内相关的告警都会走。期间下的单到时限被正常关单，没有退款发生（支付单没建出来）。
- **payment 的库慢或者不在。** 推断，没有遇到过。看 `MySQLDown`、`MySQLConnectionsHigh`。
- **order 不可用。** 付款要先向 order 确认订单的状态和金额（推断自调用链，没有专门验证过），order 不在时下单可用性的 SLO 会一起响。
- **压测打过拐点。** 三轮过载压测里 `/pay` 两天合计约 1470 个 503（占不到千分之一）。过载时响了要记下是哪一轮。

预算烧完了怎么办：见 [README.md](README.md) 的总则。

## 怎么确认恢复了

- `slo:order_pay_availability:error_ratio_rate5m` 回到 0。快档恢复之后紧跟着来的中档、慢档多半是尾巴，判断的办法同 [slo-order-create-availability.md](slo-order-create-availability.md) 的同名一节。
- 探针的周期里 `ok` 占满：`scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(probe_cycles_total{k8s_namespace_name="oppenheimer"}[5m]))'`。
- 停了超过 15 分钟的话，等 30 分钟后对账差异为 0：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。

## 怎么人为触发它

把 payment 缩到 0（第 4a 项的做法）：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer scale deploy/payment --replicas=0`，探针不停，按单元测试快的一档约 3 到 4 分钟响；做完缩回 2。会烧真预算，critical 会发 Telegram。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)，并且要主人对具体做法的同意。

## 历史

- 2026-11-23 加的规则。同一天 09:25 到 09:51 UTC 的第 4a 项（payment 缩到 0）发生在这条规则之前，那时它还不存在。**规则加了之后没有在集群上真实触发过。**
