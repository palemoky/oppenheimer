# 支付单处理得慢（支付单创建到出结果）

覆盖的告警：`PaymentResultSlow`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.pipeline` 组。出处：[round-1](../notes/3b-rounds/round-1-replica-flush-and-payment.md)（p99 到 60 秒说明处理支付单的进程跟不上）、设计 [3c](../design/3c-slo-alerting.md) 第 4.2 节。

## 这条告警在说什么

`payment_result_delay_seconds` 量的是支付单从创建到出结果的时间（模拟的支付渠道是秒级的）。p99（5 分钟速率的直方图）超过 60 秒，持续 5 分钟。支付的时限是 15 分钟，到了时限订单会被关掉（并退款），所以 60 秒是一个提前量。

注意它量的只是 payment 这一段。用户付完钱到订单显示已支付的整段时间，由 `order_payment_delay_seconds`（支付处理时延的 SLO，见 [slo-order-payment-delay.md](slo-order-payment-delay.md)）量，包含 outbox、Kafka、order 消费。

## 影响

支付单在排队。用户付了钱，订单迟迟还是待支付；再慢下去会撞上 15 分钟的支付时限，订单被关掉并退款。当天看；p99 到几分钟时要马上看。

## 先看什么

1. 现在的 p99 和支付出结果的速率：
   `scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le) (rate(payment_result_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`
   `scripts/cluster/monitoring.sh promql 'sum by (result) (rate(payment_results_total{k8s_namespace_name="oppenheimer"}[5m]))'`
2. payment 的 Pod 和日志：
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide | rg payment`
   `{service_name="payment"} | json | level=~"WARN|ERROR"`
3. 订单侧有没有跟着慢：`scripts/cluster/monitoring.sh promql 'slo:order_payment_delay:error_ratio_rate5m'`；outbox：`scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le, service_name, messaging_destination_name) (rate(outbox_relay_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`（见 [outbox.md](outbox.md)）。
4. 同时的写入压力：落库延迟和积压（支付单、落库、outbox 抢同一块盘）：`scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le) (rate(order_persist_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`；节点的刷盘看 Grafana 节点大盘。
5. payment 的消费者有没有被挡住：`scripts/cluster/monitoring.sh promql 'max by (service_name, messaging_consumer_group_name, messaging_destination_name) (kafkax_consumer_oldest_pending_age_seconds{k8s_namespace_name="oppenheimer",service_name="payment"})'`（见 [consumer-blocked.md](consumer-blocked.md)）。

## 常见的原因和各自的处理

- **处理支付单的进程跟不上（真遇到过）。** round-1：原来一轮取 100 笔、一笔一笔串行、一笔一个事务，不管需求多少都只有每秒约 17 笔，等结果的时间随积压上涨。改成每轮同时处理 16 笔（`PROCESSOR_CONCURRENCY`），同时提交的事务被 MySQL 的组提交合进同一次刷盘，出结果到每秒 76 笔，从创建到出结果 p99 0.5 秒（模拟延迟 100 毫秒加一轮的间隔 200 毫秒）。**76 笔/秒仍是上限**，需求超过它就排队。处理：降低支付的到达速率；`PROCESSOR_CONCURRENCY` 调大要先量（没有记录调大后的结果）。
- **盘在慢的一档。** payment 的提交 p99 本来就是 0.1 到 0.16 秒（round-1 第 4 节），盘慢时更长；落库、支付、outbox 抢同一块盘，写入上限由盘决定（[3b-capacity.md](../notes/3b-capacity.md) 第 1 节）。
- **payment 的库慢或连不上。** 看 payment 的 Pod 日志和 MySQL 的告警（`MySQLDown`）。
- **处理支付单的 goroutine panic 或整批失败。** round-1 记录：整批都失败时不空转重试（否则数据库不可用时变成不停重试的循环）；panic 在并发 goroutine 里自己接。处理：看日志。
- 副本数（推断，没有遇到过）：同一笔被两个实例同时拿到时靠条件更新，只有一个写事件，不会重复。

## 怎么确认恢复了

- p99 回到 1 秒以内：第 1 步的第一条；探针下空载 p99 是 0.2 秒量级的（设计 8“探针下的基线”）。
- 没有支付单被超时关单：`scripts/cluster/monitoring.sh promql 'max(recon_totals_oldest_open_age{k8s_namespace_name="oppenheimer"})'` 低于 20 分钟，`PaymentReplyOverdue` 不响（[payment-totals.md](payment-totals.md)）。
- 钱的收尾：`PaymentTotalsMismatch` 没在响，对账差异为 0：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节第 2 层：停 payment。`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer scale deploy/payment --replicas=0`，探针每秒一单、九成付款，恢复后积压的支付单出结果时 `payment_result_delay_seconds` 里出现几十秒到几分钟的观测值（行为是推的，没有验证过）。停的时间不要超过 15 分钟的支付时限，否则订单会被关掉并退款。做完 `--replicas=2`。压测打过支付出结果的上限（每秒 76 笔以上）也能触发。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
