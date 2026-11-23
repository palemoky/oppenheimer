# 支付处理时延的错误预算在烧

覆盖的告警：`SLOOrderPaymentDelayBurnFast`（critical）、`SLOOrderPaymentDelayBurnMedium`（critical）、`SLOOrderPaymentDelayBurnSlow`（warning）

规则：[slo.yaml](../../deploy/cluster/monitoring/rules/slo.yaml) 的 `slo.order_payment_delay` 组。设计：[3c-slo-alerting.md](../design/3c-slo-alerting.md) 第 2 节第 4 项、第 3.2、3.3 节、第 8 节“第 1 步”。

## 这条告警在说什么

SLI：订单从待支付变成已支付时，记“现在减去支付事件里出结果的时刻”，超过 5 秒的占比，指标 `order_payment_delay_seconds`（`le="5"` 以外的）。它量的是 payment 的 outbox 等待、Kafka、order 消费三段加起来，也就是用户付完钱到订单显示已支付等了多久。重复投递、要求退款、事件里没有时刻的都不记。目标 99%（7 天），预算 0.01。

**5 秒是暂定的**：空载基线 p99 约 0.2 秒（探针下 4861 次全部在 1 秒内），有负载时的数还没量过（3b 第 1 轮支付每秒 76 笔就到顶，10 单/秒冒烟 p99 0.36 秒），等在 125 单/秒上量一次再定要不要收到 2 秒（桶边界有 1、2、5）。出处：设计 8“探针下的基线”。

| 档 | 燃烧率 | 慢比例阈值 | 窗口（长、短） | for | 级别 | 烧完一周预算 | 长窗口烧掉的预算占比（自己推的） |
|---|---|---|---|---|---|---|---|
| 快 | 14.4 | 14.4% | 1h、5m | 2m | critical | 约 12 小时 | 约 8.6% |
| 中 | 6 | 6% | 6h、30m | 5m | critical | 约 28 小时 | 约 21% |
| 慢 | 3 | 3% | 1d、2h | 15m | warning | 约 2.3 天 | 约 43% |

短窗口里“变成已支付”的速率不低于每秒 0.2 个才算数。`$value` 是短窗口的比例。**这条链整个断了时没有观测值，这几条会自己消失**，断了归 `PaidOrdersNotLanding`（[outbox.md](outbox.md)）管。

## 影响

用户付了钱，订单迟迟显示待支付。支付时限是 15 分钟，到了订单会被关掉并触发退款，所以慢到分钟级就有“付了钱订单被关”的风险。快的一档几分钟内要看。

## 先看什么

1. 现在的比例和 p99：
   `scripts/cluster/monitoring.sh promql 'slo:order_payment_delay:error_ratio_rate5m'`
   `scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le) (rate(order_payment_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`
2. 慢在哪一段。支付出结果本身（支付单创建到出结果）：
   `scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le) (rate(payment_result_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`
   outbox 等待：
   `scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le, service_name, messaging_destination_name) (rate(outbox_relay_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`
3. order 的 `payment.events` 消费者有没有被挡住：
   `scripts/cluster/monitoring.sh promql 'max by (service_name, messaging_consumer_group_name, messaging_destination_name) (kafkax_consumer_oldest_pending_age_seconds{k8s_namespace_name="oppenheimer"})'`
   `scripts/cluster/monitoring.sh promql 'sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup="order.payment-events"})'`
4. 支付在出结果吗：`scripts/cluster/monitoring.sh promql 'sum by (result) (rate(payment_results_total{k8s_namespace_name="oppenheimer"}[5m]))'`
5. payment 和 order 的日志：`{service_name="payment"} | json | level=~"WARN|ERROR"`、`{service_name="order"} | json | level=~"WARN|ERROR"`。

## 常见的原因和各自的处理

- **payment 在 outbox 上串行排队、支付出结果跟不上。** 3b 第 1 轮查出的就是这条路（设计第 2 节第 4 项）：原来一轮一笔一笔串行，每秒约 17 笔；改成每轮并发 16 笔（`PROCESSOR_CONCURRENCY`）后每秒 76 笔，但仍然是上限，需求超过它就排队（[round-1](../notes/3b-rounds/round-1-replica-flush-and-payment.md) 第 1、4 节）。处理：看 `payment_result_delay_seconds`，超过就是这个；降低支付的到达速率。
- **盘进入慢的一档**，支付单的提交 p99 本来就是 0.1 到 0.16 秒（round-1 第 4 节）。
- **order 的 `payment.events` 消费者被一条消息挡住**（见 [consumer-blocked.md](consumer-blocked.md)）。
- **outbox 中继跟不上**（3b round-1 里到过分钟级，见 [outbox.md](outbox.md)）。
- **inventory 消费 `order.events` 跟不上**（约 105 条/秒）。它管的是支付之后预占变扣减，不在这条链上，对这个 SLI 没有直接影响（round-1 第 5 节）。
- 阈值 5 秒太紧（推断）：这条阈值是暂定的，如果只在有负载时响而用户没有抱怨，先怀疑阈值而不是系统。

预算烧完了怎么办：见 [README.md](README.md)。

## 怎么确认恢复了

- `slo:order_payment_delay:error_ratio_rate5m` 回到接近 0；`order_payment_delay_seconds` 的 p99 回到 1 秒以内。
- 支付到订单的链路是通的：`scripts/cluster/monitoring.sh promql 'sum(increase(order_transitions_total{k8s_namespace_name="oppenheimer",from="pending_payment",to="paid"}[10m]))'` 大于 0。
- 钱：对账差异为 0。`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，另外 `PaymentTotalsMismatch` 没有在响（见 [payment-totals.md](payment-totals.md)、[recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节第 2 层：停 payment、停 order 的消费者（3a 用过）。待定，建议：停 payment 的 Deployment 几分钟再恢复，恢复后积压的支付单出结果，`order_payment_delay_seconds` 里出现大于 5 秒的观测值（指标的行为是推的，没有验证过）：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer scale deploy/payment --replicas=0`，之后 `--replicas=2`。探针每秒一单，其中约九成付款（每 10 个周期取消一个），所以会有观测值。压测打过支付出结果的上限（每秒 76 笔以上）也能触发。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
