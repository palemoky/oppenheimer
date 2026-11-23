# outbox 发得慢，或支付成功了订单不变已支付

覆盖的告警：`OutboxRelaySlow`（warning）、`PaidOrdersNotLanding`（critical）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.pipeline` 组。出处：[ADR-0008](../adr/0008-outbox-and-events.md)、[round-1](../notes/3b-rounds/round-1-replica-flush-and-payment.md)、设计 [3c](../design/3c-slo-alerting.md) 第 8 节“第 1 步”。

## 这条告警在说什么

服务在同一个本地事务里写业务变更和 `outbox` 表的事件，每个服务的中继轮询 `outbox`、发到 Kafka、确认后标记。订阅方至少一次消费，自己幂等。

- **`OutboxRelaySlow`**：`outbox_relay_delay_seconds`（消息从写进表到 Kafka 确认）按服务和 topic 的 p99 超过 30 秒，持续 5 分钟。平时是几十毫秒；round-1 里中继跟不上时到过分钟级。盲区：**中继整个停住时一条都不记，直方图没有新点，这条不响**（设计 8 第 1 步）。
- **`PaidOrdersNotLanding`**：5 分钟内 `payment_results_total{result="succeeded"}` 增加超过 10，而 `order_transitions_total{from="pending_payment",to="paid"}` 增加不到 1，持续 2 分钟（链路断了之后约 7 分钟响；2026-11-23 从“10 分钟加 5 分钟”收紧的，原来响的时候第一批订单已经到 15 分钟的关单时限了）。支付一直在出成功的结果，而订单 5 分钟没有一单变成已支付：payment 的 outbox、`payment.events`、order 的消费者这条链整个断了。支付处理时延的 SLO 这时没有观测值、算不出来，所以单独写。

## 影响

`OutboxRelaySlow`：下游（支付结果到订单、库存落账、订单变成已支付）都跟着晚；`SLOOrderPaymentDelayBurn*` 可能同时响。当天看。`PaidOrdersNotLanding`：**用户付了钱而订单停在待支付**。下单 15 分钟后关单任务会把这些订单关掉并触发退款；告警响的时候最早的一批离关单还有几分钟，现在就看。

## 先看什么

1. outbox 等待的 p99，哪个服务哪个 topic：
   `scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le, service_name, messaging_destination_name) (rate(outbox_relay_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`
2. 支付在出结果而订单不变已支付吗：
   `scripts/cluster/monitoring.sh promql 'sum(increase(payment_results_total{k8s_namespace_name="oppenheimer",result="succeeded"}[10m]))'`
   `scripts/cluster/monitoring.sh promql 'sum(increase(order_transitions_total{k8s_namespace_name="oppenheimer",from="pending_payment",to="paid"}[10m]))'`
3. 断在哪一段：order 的 `payment.events` 消费者是否被挡住、积压多少：
   `scripts/cluster/monitoring.sh promql 'max by (service_name, messaging_consumer_group_name, messaging_destination_name) (kafkax_consumer_oldest_pending_age_seconds{k8s_namespace_name="oppenheimer"})'`
   `scripts/cluster/monitoring.sh promql 'sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup="order.payment-events"})'`
4. payment 在出结果吗、慢不慢：`scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le) (rate(payment_result_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`（见 [payment-backlog.md](payment-backlog.md)）。
5. 日志和 Pod：`{service_name="payment"} | json | level=~"WARN|ERROR"`、`{service_name="order"} | json | level=~"WARN|ERROR"`；`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide`；Kafka：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get pods`。

## 常见的原因和各自的处理

- **中继跟不上（真遇到过）。** 3b round-1：支付这条路在 outbox 上串行排队，原来一轮一笔一笔、每秒约 17 笔；改成每轮并发 16 笔后每秒 76 笔（round-1 第 1、4 节）。另外 1b 记录过“轮询拖垮落库”（ADR-0008 里的 outbox 轮询间隔、对库的压力）。处理：看中继的批量和轮询间隔、库的写入压力、Kafka 的生产延迟。
- **盘在慢的一档**，提交变慢（round-1 第 5 节、round-2 第 10 节）。
- **Kafka 不可用或生产慢。** 看 Kafka 的告警（`KafkaBrokerMissing`、`KafkaUnderReplicatedPartitions`、`KafkaPartitionsOffline`）。
- **order 的 `payment.events` 消费者被挡住。** 看 [consumer-blocked.md](consumer-blocked.md)；这是 `PaidOrdersNotLanding` 的另一半。
- **payment 的 outbox 发不出去、中继整个停了。** `OutboxRelaySlow` 此时不响，靠 `PaidOrdersNotLanding` 和消费者一侧的规则发现（规则注释就是这么设计的）。处理：重启 payment 的 Pod 前先看日志里中继为什么停；`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer rollout restart deploy/payment`（outbox 里的消息不会丢，至少一次投递）。
- 支付成功的事件里没有 `occur_time`（推断，没有遇到过）：这类事件不记 `order_payment_delay_seconds`，但订单仍会变成已支付，不会触发 `PaidOrdersNotLanding`。

## 怎么确认恢复了

- `PaidOrdersNotLanding`：第 2 步里订单变已支付的增量大于 0；订单状态是 `pending_payment` 的支付成功订单在下降。
- `OutboxRelaySlow`：第 1 步 p99 回到几十毫秒到几秒。
- `kafka_consumergroup_lag{consumergroup="order.payment-events"}` 清空。
- 钱：`PaymentTotalsMismatch` 没在响（[payment-totals.md](payment-totals.md)）；对账差异为 0：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。付了钱被关单的订单要核对退款（15 分钟的支付时限）。

## 怎么人为触发它

设计第 5 节第 2 层：消费者被挡住、outbox、支付处理时延：把 orderindex、order 的消费者停掉（3a 用过）；停 payment。建议（没有验证过）：`PaidOrdersNotLanding`：停 order 的 `payment.events` 消费者，探针每秒一单、约九成付款，5 分钟内成功的支付超过 10 笔，再加 2 分钟的 `for`，约 7 分钟响；停得久了会有订单到 15 分钟的支付时限，所以做的时候要预期会有订单被关掉并退款，只在 `oppenheimer-e2e` 里做更稳。`OutboxRelaySlow`：待定，需要让中继变慢但不停，例如给 Kafka 加网络延迟，没有现成办法。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
