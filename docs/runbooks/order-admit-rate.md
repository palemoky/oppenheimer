# 下单受理成功率比一小时前低了一半以上

覆盖的告警：`OrderAdmitRateDropped`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.admission` 组。出处：设计 [3c](../design/3c-slo-alerting.md) 第 4.2 节（计划；3a 大盘一节）。

## 这条告警在说什么

`outcome="admitted"` 的 10 分钟速率，除以去掉业务上正常拒绝之后的全部受理请求（去掉 `insufficient_stock`、`purchase_limit_exceeded`、`duplicate`、`invalid_argument`、`sku_not_found`、`idempotency_key_reused`），小于“一小时前同一个比值”的一半；并且现在的受理请求速率不低于每秒 0.2 个；持续 10 分钟。去掉业务上正常的拒绝，大促时售罄变多就不会响。

它和可用性 SLO 互补：这里连 429、409 之外的“非 5xx 的失败”也算，比如降级。一小时前没有流量时（探针停过、服务刚恢复的头一个小时）没有可比的基线，退回绝对的门槛：受理比例低于一半就算（2026-11-26 改的，之前这种时候不响）。

`$value` 是现在的比值，通知里写的“不到一小时前的一半”是和一小时前比。

## 影响

受理成功的比例明显掉了：用户下单越来越多地被拒（背压 429、闸门不可用 503、库存服务不可用 503、降级开关）。数据没有风险（被拒的请求什么都没发生）。当天看；如果同时有 SLO 燃烧率告警，先处理那边。

## 先看什么

1. 哪一种结局变多了（本规则的描述就是让你按 `outcome` 看）：
   `scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(order_admissions_total{k8s_namespace_name="oppenheimer"}[10m]))'`
   对比一小时前：`scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(order_admissions_total{k8s_namespace_name="oppenheimer"}[10m] offset 1h))'`
2. 现在的比值和一小时前的比值：
   `scripts/cluster/monitoring.sh promql 'sum(rate(order_admissions_total{k8s_namespace_name="oppenheimer",outcome="admitted"}[10m])) / sum(rate(order_admissions_total{k8s_namespace_name="oppenheimer",outcome!~"insufficient_stock|purchase_limit_exceeded|duplicate|invalid_argument|sku_not_found|idempotency_key_reused"}[10m]))'`
3. 按结局走：`backlogged` 多 → [order-backlog.md](order-backlog.md)；`gate_memory_full` 多 → [order-gate-memory.md](order-gate-memory.md)；各种 `*_unavailable` 多 → 看闸门、inventory、订单库（[breaker.md](breaker.md)）；降级开关 → [degrade.md](degrade.md)。
4. gateway 侧的状态码：`scripts/cluster/monitoring.sh promql 'sum by (http_response_status_code) (rate(http_server_request_duration_seconds_count{service_name="gateway",k8s_namespace_name="oppenheimer",http_route="/api/v1/orders",http_request_method="POST"}[10m]))'`，看是不是只有一部分请求走到了 order（限流在 gateway 一侧，这个指标看不到）。
5. order 的日志：`{service_name="order"} | json | level=~"WARN|ERROR"`。

## 常见的原因和各自的处理

这条规则没有真的响过，下面是各个结局在文档里的已知来源，先按第 1 步的结局对号。

- **`backlogged` 变多：落库跟不上。** 见 [order-backlog.md](order-backlog.md)（压测过拐点时必然出现）。
- **`gate_memory_full`：闸门内存到水位。** 见 [order-gate-memory.md](order-gate-memory.md)。
- **闸门或 inventory 的 `*_unavailable`：** 闸门熔断器打开（`STOCK_GATE_UNAVAILABLE`）、落库健康熔断器打开（`ORDER_LANDING_UNAVAILABLE`）、inventory 被边车摘除。3b round-3 第 4 节是一次盘的停顿造成的 5 秒；已修。
- **降级开关被关（`admission` 开关）：** [degrade.md](degrade.md)。
- **正常结局被误归为“不正常”或反过来**（推断）：规则只排除了六种业务结局；以后 order 新增结局时这里要跟着改，否则新的业务拒绝会算进分母。
- **探针占主要流量的时候，比值反映的是探针**（设计 8：平时探针就是全部的流量）。

## 怎么确认恢复了

- 第 2 步的比值回到一小时前的水平之上（至少回到一半以上），告警在两者回到阈值之上后恢复。
- 第 1 步里不正常的结局速率回到 0。
- 探针周期：`scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(probe_cycles_total{k8s_namespace_name="oppenheimer"}[5m]))'` 里 `ok` 占满。
- 涉及订单的最后以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节没有专门列。待定，建议：压测打过拐点（`backlogged` 变多），但要保证一小时前有流量作对照；或者用降级开关关掉 `admission`（见 [degrade.md](degrade.md)，内部端口 `PUT /degrade`），探针的周期会变成 `incomplete`；一小时前的对照要先有。需要先让系统空转至少一小时、有探针流量，然后注入。没有验证过。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
