# Kafka 分区副本不足、没有 leader

覆盖的告警：`KafkaPartitionsOffline`（critical）、`KafkaUnderReplicatedPartitions`（warning）

## 这条告警在说什么

Kafka 在命名空间 `kafka`，Strimzi 管的，三个 broker `kafka-dual-0..2`（KRaft，每个同时是 controller 和 broker），副本数 3、`min.insync.replicas=2`、生产者 `acks=all`（[2b 笔记 7.3](../notes/2b-middleware.md)）。

- `KafkaPartitionsOffline`：`max(kafka_controller_kafkacontroller_offlinepartitionscount) > 0`，2 分钟。控制器统计的“没有 leader 的分区数”，只有当前的活动控制器报非 0，所以取 max。不用 Exporter 的 leader 标签：没有 leader 时那个指标的值没有确切约定（设计第 8 节“第 4 步的另一半”）。
- `KafkaUnderReplicatedPartitions`：`sum by (topic) (kafka_topic_partition_under_replicated_partition) > 0`，5 分钟。Kafka Exporter 的这个指标每个分区是 0 或 1，按 topic 加起来；ISR 少于副本数。5 分钟是为了让滚动重启（Strimzi 一次一个 broker）和 leader 换人过得去。

出处：[设计 4.4](../design/3c-slo-alerting.md)。抑制：`KafkaBrokerMissing` 在响时 `KafkaUnderReplicatedPartitions` 不通知（broker 不在时副本不足是同一件事）。

## 影响

- 离线分区：不能读写。生产者发不出去，outbox 积压；消费者停住；下单受理的落库和 CDC、支付、关单这些异步环节都挂在 Kafka 上。critical，现在就看。
- 副本不足：读写照常，但没有余量。ISR 掉到 1 个时 `min.insync.replicas=2` 让写入被拒；再掉一个 broker 就可能写入失败或丢消息。warning，当天看，滚动期间的短暂出现是正常的。
- 数据风险：副本不足时 leader 一坏，只在 leader 上的消息丢；以对账差异为 0 收尾。

## 先看什么

1. 哪个 topic、多少分区：

   ```
   scripts/cluster/monitoring.sh promql 'sum by (topic) (kafka_topic_partition_under_replicated_partition)'
   scripts/cluster/monitoring.sh promql 'max(kafka_controller_kafkacontroller_offlinepartitionscount)'
   scripts/cluster/monitoring.sh promql 'kafka_brokers'
   ```
2. broker 在不在、有没有在滚动：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get pods -o wide
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get kafka kafka -o jsonpath='{.status.conditions}'
   ```
3. 哪些分区、副本和 ISR 是谁（在 broker 里）：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka exec kafka-dual-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-kafka-bootstrap.kafka.svc:9092 --describe --under-replicated-partitions
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka exec kafka-dual-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-kafka-bootstrap.kafka.svc:9092 --describe --unavailable-partitions
   ```
4. broker 日志，磁盘是否满：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka logs kafka-dual-1 --tail=100`；Loki：`{k8s_namespace_name="kafka", k8s_pod_name="kafka-dual-1"}`。
5. 对业务的影响：消费延迟 `scripts/cluster/monitoring.sh promql 'kafka_consumergroup_lag'`（`order.order-close-delay` 组的高值是延迟关单的机制本身，不是积压，ADR-0009）。

## 常见的原因和各自的处理

1. **broker 在滚动或刚重启。** 改配置、改 requests、保留期都会滚动三个 broker（round-8、round-9 都做过）。副本会在重启后追上，这是预期，几分钟内自己消失。**但滚动期间下单会有失败**：3a 里滚动重启 broker 时 31 个下单失败（90 请求/秒的探针，59 秒内，下单 p99 到 3 秒），原因没有查，看起来是 leader 换人时生产者等过了这一跳的时限（[3a 笔记 15.1](../notes/3a-observability.md)）。滚动前先静默：`scripts/cluster/monitoring.sh silence 'alertname=KafkaUnderReplicatedPartitions' 30m '滚动 broker'`。
2. **一个 broker 被删或挂了。** 持续生产时删一个 broker：没有报错，最长一条延迟 16 秒，恢复后没有落后副本（2b 笔记 7.6）。Strimzi 会立刻把 Pod 建回来。
3. **磁盘满。** 保留期原来是默认的 7 天不限大小，125 单/秒下每个 broker 每天约 19 GiB，盘 20Gi，会撑满（round-9）。现在是 24 小时加每分区 4 GiB 兜底，`cdc.order.orders` 是压缩保留的，不受保留期管，是最大的 topic（每个 broker 5.5 GiB）。磁盘告警归节点那几条；这里的处理是看 `df` 和日志，调保留或扩盘。
4. **operator 调和失败导致的状态不一致。** operator 坏了 broker 不受影响，但改配置不生效，坏了之后恢复可能不完整：见 [kafka-operator.md](kafka-operator.md)。
5. **两个 broker 同时不在。** 剩下一个拒绝写入（`min.insync.replicas=2`），元数据的多数票也没了（合一模式，2b 笔记 7.3 补充）。这一条**没有测过**：Strimzi 会立刻重建 Pod，要用别的办法停（阶段 4 的 Chaos Mesh）。
6. **节点资源挤压导致副本追不上**（推断，没有遇到过）：节点 CPU 高、盘慢时 follower 落后。看节点和 broker 的 CPU、磁盘延迟。

## 怎么确认恢复了

- `sum(kafka_topic_partition_under_replicated_partition)` 回到 0；`max(kafka_controller_kafkacontroller_offlinepartitionscount)` 是 0；告警 2 分钟（离线）或 5 分钟（副本不足）内消失。
- 第 3 步两条命令都没有输出；`kafka_consumergroup_lag` 回到平时（除延迟关单的组）；outbox 的积压消下去。
- 出过离线分区的，必须以对账为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)`。

## 怎么人为触发它

设计 5 节第 2 层没有为这两条列专门的办法；可用 `make cluster-fault` 或滚动 broker 的方式（待定，建议）：副本不足，在 broker 滚动期间自然出现，或临时把规则的 `for: 5m` 改短、删一个 broker Pod 观察（Strimzi 会很快重建，窗口短）。离线分区要失去整个 ISR（通常是三个 broker 都不可用，只掉两个时剩下的那个仍是 leader，只是拒绝写入），影响业务，没有安全的办法，建议只做单元测试加“把阈值改到 `>= 0`”的验证，并说明这只验证了“路是通的”。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
