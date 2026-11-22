# 消费者被一条消息挡住，或者没有在消费

覆盖的告警：`ConsumerBlocked`（warning）、`ConsumerBlockedTooLong`（critical）、`ConsumerNotConsuming`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.pipeline` 组。出处：[ADR-0021](../adr/0021-degradation-and-lifecycle.md)（普通错误不设重试上限）、[ADR-0017](../adr/0017-keyed-concurrent-consume.md)（窗口）、[round-9](../notes/3b-rounds/round-9-soak.md) 3.4 节（topic 保留 24 小时）、设计 [3c](../design/3c-slo-alerting.md) 第 8 节“第 1 步”。

## 这条告警在说什么

消费者对普通错误不设重试上限，依赖不可用时让分区停住等它恢复；停住的时间就是指标 `kafkax_consumer_oldest_pending_age_seconds`：进程里最老的一条还没处理完的消息待了多久（每个 topic 取各分区最大值，同一消费组的几个实例各报各的，所以规则取 `max`）。它是下界：没拉到进程里的消息它看不到。

- **`ConsumerBlocked`**：上面这个值超过 10 分钟，持续 5 分钟。
- **`ConsumerBlockedTooLong`**：超过 2 小时，持续 5 分钟。**Kafka 的保留期是 24 小时**，过了之后没消费的消息会被删掉，那时丢的是数据（round-9 3.4）。
- **`ConsumerNotConsuming`**：Kafka Exporter 的 `kafka_consumergroup_lag` 大于 0（有积压），并且 `kafka_consumergroup_current_offset` 的 10 分钟 `delta` 等于 0（位点没动），持续 10 分钟。只看五个消费组：`order.order-submit`、`order.payment-events`、`inventory.order-events`、`payment.order-events`、`orderindex.cdc-orders`。`order.close.delay` 不看：它的“积压”是还没到期的延迟关单消息，是机制本身（稳定在 11.4 万，round-9 2.3）。它能发现消费者整个不在、卡在再均衡、进程活着但不拉消息，这些 `ConsumerBlocked` 量不到。注意 `delta` 窗口 10 分钟加 `for` 10 分钟，实际从位点停下算起最早约 20 分钟才响，比规则注释里的“10 分钟”长。

## 影响

`ConsumerBlocked`：它后面同一分区（或同一个键）的消息都被挡着，对应的那一步停了：`order.submit` 停了就是不落库（订单受理了查不到，占用过 60 秒被回收）；`payment.events` 停了订单不变已支付；`cdc-orders` 停了商家索引不更新。当天看，但要快，因为会扩散。`ConsumerBlockedTooLong`：再不处理，24 小时后消息被删，数据丢了；现在就要决定修依赖或人工移走那条消息。`NotConsuming`：同上，当天看。

## 先看什么

1. 谁被挡、挡了多久：
   `scripts/cluster/monitoring.sh promql 'max by (service_name, messaging_consumer_group_name, messaging_destination_name) (kafkax_consumer_oldest_pending_age_seconds{k8s_namespace_name="oppenheimer"})'`
2. 积压多少、位点动没动：
   `scripts/cluster/monitoring.sh promql 'sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup=~"order\\.order-submit|order\\.payment-events|inventory\\.order-events|payment\\.order-events|orderindex\\.cdc-orders"})'`
   `scripts/cluster/monitoring.sh promql 'sum by (consumergroup, topic) (delta(kafka_consumergroup_current_offset{consumergroup=~"order\\.order-submit|order\\.payment-events|inventory\\.order-events|payment\\.order-events|orderindex\\.cdc-orders"}[10m]))'`
3. 持续重试的日志（ADR-0021：看日志里持续重试的 WARN，错误信息指向哪个依赖）：
   `{service_name="<上面的 service_name>"} | json | level=~"WARN|ERROR"`
4. 它依赖的东西好不好：依赖是 MySQL、Redis 闸门、inventory、Kafka。`scripts/cluster/monitoring.sh alerts` 看有没有 `MySQLDown`、`RedisDown`、`BreakerOpen`、`OrderLandingBreakerOpen`、Kafka 的告警（见 [breaker.md](breaker.md)）；Pod：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide`，Kafka：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get pods`。
5. 消费组的状态（成员数、有没有在再均衡）：
   `POD=$(KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get pods -l strimzi.io/cluster=kafka,strimzi.io/broker-role=true -o jsonpath='{.items[0].metadata.name}')`
   `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka exec $POD -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order.order-submit`

## 常见的原因和各自的处理

- **依赖暂时不可用（设计上就是这样停的）。** ADR-0021：落库、订单事件的消费者对普通错误无限重试，依赖恢复后自己继续。处理：恢复依赖，不要动消费者。
- **一条没被识别的坏消息挡住分区。** ADR-0021 的后果、ADR-0014（一条缺列的消息会让 CDC 所在分区全部订单停止同步）。handler 应当把“数据本身不对”的错误标成 `Permanent`（进死信，见 [consumer-dead-letter.md](consumer-dead-letter.md)）。处理：找到那条消息，改代码把错误标成 `Permanent`；或者人工把它移走：没有找到现成的“跳过某个位点”的操作手册（ADR-0014 说“放到阶段 3”，3c 没写）。建议（没有验证过）：把该消费组的 Deployment 缩到 0，用 `kafka-consumer-groups.sh --group <组> --topic <topic>:<分区> --reset-offsets --to-offset <位点+1> --execute` 越过，再放回。
- **被杀的消费者名下分区停摆。** 1b：会话超时默认 45 秒，整整停摆 45 秒，受理的订单有 28% 到 33% 被回收；已改成 10 秒。现在应该只有 10 秒左右。
- **inventory 消费 `order.events` 跟不上。** round-1 第 5 节：处理速率停在约 105 条/秒，积压涨到 2 万条；是慢，不是挡住（`delta` 不为 0）。原因没有查清。
- **库存服务处理事件时 Redis 不可用。** 1b：该消费者重试调到 60 次（约 5 分钟），宁可停住等 Redis 恢复，防止 MySQL 已释放而占用没归还。
- **窗口停住。** ADR-0017：并发消费时分发偏移领先最小未完成偏移超过窗口（1024）就停止分发，行为和串行时一样，已提交位点可能落后实际进度最多一个窗口。
- **指标本身的盲区**：消费者整个不在，`kafkax_consumer_oldest_pending_age_seconds` 没有新值，只有 `NotConsuming` 管得到；Kafka Exporter 没有命名空间的标签，`oppenheimer-e2e` 如果用同名的消费组会混在一起（没有核对，设计 8）。

## 怎么确认恢复了

- 最老消息的年龄回到几秒：第 1 步的查询回到 0 附近。
- 积压在下降、位点在动：第 2 步两条，`delta` 大于 0 且 lag 单调下降。
- 对账：积压清完之后，订单、库存、商家索引的差异为 0：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)），先等积压清完，否则 `recon` 会把还没追上的报成差异。
- 被挡过超过 24 小时的（极端情况）要查有没有消息被 Kafka 删掉：对账会发现订单没落库。

## 怎么人为触发它

设计第 5 节第 2 层：把 orderindex、order 的消费者停掉（3a 用过），`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer scale deploy/orderindex --replicas=0`，停掉之后有积压（探针每秒一单会产生消息），`ConsumerNotConsuming` 约 20 分钟后响。`ConsumerBlocked` 要消费者活着但被挡住，停掉的触发的是 `NotConsuming`。触发 `ConsumerBlocked` 要让 handler 在一个依赖上无限重试，例如把 MySQL 或 inventory 弄不可用；待定，建议在 `oppenheimer-e2e` 里做。`ConsumerBlockedTooLong` 要挡 2 小时，建议临时把阈值改到当前值以下，只验证“路是通的”。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
