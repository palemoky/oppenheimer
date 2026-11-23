# Kafka broker 少于 3 个

覆盖的告警：`KafkaBrokerMissing`（critical）

## 这条告警在说什么

`kafka_brokers < 3 or absent(kafka_brokers)`，持续 2 分钟。`kafka_brokers` 是 Kafka Exporter 看到的 broker 数；三个 broker（`kafka-dual-0..2`）才有三副本。Exporter 连不上 Kafka 时这个序列可能直接没有，所以另用 `absent` 补上，这时告警里的值是 1，表示“指标没有了”，不是 1 个 broker。

出处：[设计 4.4](../design/3c-slo-alerting.md)。抑制：它在响时 `KafkaUnderReplicatedPartitions` 不通知（[kafka-partitions.md](kafka-partitions.md)）。

注意：Exporter 是另一个 Pod（Strimzi 的 `kafkaExporter`，Pod 名 `kafka-kafka-exporter-*`），它自己挂了、broker 其实都好的时候，这条也会响（`absent` 那一半）。先分清是哪一种。**Exporter 连不上 Kafka 时 `kafka_brokers` 是不是真的消失，没有实测**，只有单元测试。

## 影响

- 少一个 broker：读写照常（ISR 还有 2 个），但没有余量；再掉一个，`min.insync.replicas=2` 让写入被拒，下单受理的落库、outbox、CDC 全停。
- 数据风险：少一个时没有。再少一个有写入失败或丢消息的风险（见 [kafka-partitions.md](kafka-partitions.md)）。
- critical，现在就看；一般 Strimzi 会立刻重建 Pod，几十秒内回来。

## 先看什么

1. 是 broker 少了还是指标没了：

   ```
   scripts/cluster/monitoring.sh promql 'kafka_brokers'
   scripts/cluster/monitoring.sh promql 'up{namespace="kafka"}'
   scripts/cluster/monitoring.sh promql 'max(kafka_controller_kafkacontroller_offlinepartitionscount)'
   ```
2. Pod 状态（broker 和 exporter 都看）：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get pods -o wide
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka describe pod kafka-dual-2 | rg -A8 'Last State|Events'
   ```
3. broker 日志：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka logs kafka-dual-2 --tail=100`（上一次崩溃加 `--previous`）。
4. Strimzi 的状态和 operator 是不是在调和：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get kafka kafka -o jsonpath='{.status.conditions}'
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka logs deploy/strimzi-cluster-operator --tail=50
   ```
5. 节点和磁盘：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl get nodes`；`kubectl -n kafka get pvc`（每个 broker 20Gi 的 local-path 卷，`deleteClaim: false`）。

## 常见的原因和各自的处理

1. **滚动重启。** 改配置、保留期、requests 时 operator 逐个滚动（round-8、round-9），某一时刻少一个；每个 broker 要几分钟。`for: 2m` 可能在滚动时响，这是预期：计划内的先静默，`scripts/cluster/monitoring.sh silence 'alertname=KafkaBrokerMissing' 30m '滚动 broker'`。
2. **宿主机重启、虚拟机调内存、节点掉电。** 全部中间件自己恢复，Kafka 无落后副本（2b 笔记 18 号发现，设计 2b 第 10 节）。等就是了；节点没回来看节点。
3. **broker 起不来（磁盘满、卷问题）。** 保留期曾经是默认的 7 天不限大小，会撑满 20Gi 盘（round-9）；已改成 24 小时和每分区 4 GiB。日志里有磁盘错误就先腾空间。`deleteClaim: false`，不要为了起来而删 PVC，删卷是破坏性操作，先问主人。
4. **operator 坏了，Pod 掉了建不回来。** operator 不在或调和失败时，broker 掉了之后的恢复可能不完整：见 [kafka-operator.md](kafka-operator.md)。
5. **只有 Exporter 不在。** Pod `kafka-kafka-exporter-*` 的状态；它也由 operator 管。恢复后序列回来。
6. **OOMKilled**（推断，没有遇到过）：broker 容器上限 1.5 Gi、堆 768 MB；`describe` 的 `Last State`。

## 怎么确认恢复了

- `scripts/cluster/monitoring.sh promql 'kafka_brokers'` 回到 3，告警 2 分钟内消失。
- 三个 Pod 都是 `Ready`；`kubectl -n kafka exec kafka-dual-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-kafka-bootstrap.kafka.svc:9092 --describe --under-replicated-partitions` 无输出（带 `KUBECONFIG=~/.kube/oppenheimer.yaml`）；`kafka_consumergroup_lag` 回到平时。
- 出过写入失败的，以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)`。

## 怎么人为触发它

设计 5 节第 2 层没有为这条专门写办法（“`Kafka` 资源不是 `Ready`”那条才要想不伤 broker 的办法）。2b 里做过：持续生产时 `kubectl -n kafka delete pod kafka-dual-2`，没有报错，最长一条延迟 16 秒（2b 笔记 7.6），但 Strimzi 很快就重建，`for: 2m` 撑不满；要稳定触发，临时把规则 `kafka_brokers < 3` 改成 `< 4`（待定，建议），下发，2 分钟后响，再改回，并说明这只验证了“路是通的”。Exporter 缺失的那一半：`kubectl -n kafka scale deploy/kafka-kafka-exporter --replicas=0`（Exporter 由 operator 管，可能被它改回，待定）。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

- 2026-11-23 07:31 UTC，`KafkaBrokerMissing` 第一次真实触发（告警质量检查第 18 项，人为的：Strimzi operator 缩到 0 之后删掉 `kafka-dual-2`，停 8.5 分钟）：删掉约 3 分钟后响，Telegram 发了；12 个 topic 的 `KafkaUnderReplicatedPartitions` 全部被它抑制，没有送出去。恢复时副本不足先消失（Pod 起来 30 秒内同步完），`kafka_brokers` 约 35 秒后才回到 3，没有漏出通知；欠的数据多的时候这个顺序会反过来（设计 [3c](../design/3c-slo-alerting.md) 第 8 节“第 18 项”）。**operator 缩回 1 之后 5 分钟才把 Pod 建回来**（它的第一次调和一直卡到超时），这段时间 `kubectl -n kafka get strimzipodset` 仍然显示 3/3：看到 broker 少一个而 operator 刚重启过，先等 5 分钟再怀疑别的。停着的时候三副本的 topic 读写不受影响，探针照常。
