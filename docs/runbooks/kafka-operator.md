# Strimzi operator 不再调和 Kafka

覆盖的告警：`KafkaOperatorNotReconciling`（warning）、`KafkaOperatorMetricsMissing`（warning）

## 这条告警在说什么

operator 是命名空间 `kafka` 里的 Deployment `strimzi-cluster-operator`，管 Kafka 资源 `kafka`（三个 broker `kafka-dual-0..2`）。它每 2 分钟定时调和一次。指标由 PodMonitor `strimzi-operator`（`deploy/cluster/monitoring/kafka-podmonitor.yaml`）抓，只留 `strimzi_` 开头的。

- `KafkaOperatorNotReconciling`：两半，满足任一，持续 10 分钟。
  - `sum by (namespace) (increase(strimzi_reconciliations_successful_total{kind="Kafka"}[20m])) < 1`：成功的调和计数 20 分钟没有涨。
  - operator 看得到 Kafka 资源（`strimzi_resources{kind="Kafka"}` 大于 0）而成功的计数根本不存在：重启之后一次都没成功过，计数器从未出现，`increase()` 没有结果（round-8 那次就是这种）。
  窗口 20 分钟加 `for` 10 分钟，所以告警里写“已经 30 分钟没有成功调和”；滚动三个 broker 的一次调和要几分钟，窗口留了余量。
- `KafkaOperatorMetricsMissing`：`absent(strimzi_resources{kind="Kafka"})`，10 分钟。operator 不在或抓不到；上一条靠这些指标，没有了就不响，所以单独写。

出处：[3b round-8 记录 6.3 节](../notes/3b-rounds/round-8-cpu-requests.md)（设计 3c 第 4.4 节、第 8 节“第 4 步的另一半”：计划里写的是“Kafka 资源的 Ready 条件”，改成直接看 operator 的调和计数）。

## 影响

- broker 不受影响，照常收发，用户无感。
- 但 Kafka 资源的改动（配置、保留期、扩容、requests）不会生效，证书到期不会续，broker 掉了之后的恢复可能不完整。
- 这是一个静悄悄的故障：2026-11-20 那次两天没人发现（见历史）。warning，当天处理，不要拖到证书到期。

## 先看什么

1. 哪一条在响，operator 指标还在不在：

   ```
   scripts/cluster/monitoring.sh promql 'sum by (namespace) (increase(strimzi_reconciliations_successful_total{kind="Kafka"}[20m]))'
   scripts/cluster/monitoring.sh promql 'sum by (namespace) (strimzi_resources{kind="Kafka"})'
   scripts/cluster/monitoring.sh promql 'sum by (namespace) (strimzi_reconciliations_successful_total{kind="Kafka"})'
   ```
2. **Kafka 资源的状态**（这是最直接的）：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get kafka kafka
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get kafka kafka -o jsonpath='{.status.conditions}'
   ```
   `NotReady` 和里面的 message 通常就是原因。
3. operator 的 Pod 和日志：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get pods -l strimzi.io/kind=cluster-operator -o wide
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka logs deploy/strimzi-cluster-operator --tail=100
   ```
   Loki：`{k8s_namespace_name="kafka"} |= "ERROR"`，加上 Pod 名过滤。
4. operator 的环境变量，是否带着 `KUBERNETES_SERVICE_DNS_DOMAIN=cluster.local`：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get deploy strimzi-cluster-operator -o yaml | rg -A1 KUBERNETES_SERVICE_DNS_DOMAIN`。
5. Prometheus 抓得到吗：`scripts/cluster/monitoring.sh promql 'up{namespace="kafka"}'`；`scripts/cluster/monitoring.sh status` 里不健康的目标。

## 常见的原因和各自的处理

1. **启动时猜集群域名失败（2026-11-20 那次）。** 宿主机重启后 3 分钟 operator 启动，赶在集群 DNS 就绪之前，用 DNS 查 `kubernetes.default` 落到上游，拿到代理的假地址（198.18.x.x），猜出的“域名”是一串垃圾，之后每次调和在生成证书时失败：`IllegalArgumentException: Invalid DNS name: kafka-kafka-bootstrap.kafka.svc.19818.25.232 kubernetes.default`。修法已经做了：`scripts/cluster/middleware.sh kafka` 装 operator 时用 `extraEnvs` 写死 `KUBERNETES_SERVICE_DNS_DOMAIN=cluster.local`（chart 取默认值时不渲染这个变量，等于交给探测）。如果新装的集群或重装后又出现，先看第 4 步。
2. **operator 的 Pod 不在或在重启循环。** 此时 `KafkaOperatorMetricsMissing` 响，`NotReconciling` 不响。`describe pod` 看原因；`rollout restart deploy/strimzi-cluster-operator` 通常能让它重新来过。
3. **调和成功但很慢，被 20 分钟窗口卡住**（推断，没有遇到过）：滚动 broker 一次要几分钟，余量是设计出来的；正在做大的改动时先看日志。
4. **Kafka 资源本身有错**（推断，没有遇到过）：配置不合法、版本不被支持。Strimzi 1.3.1 只支持 Kafka 4.2.0 到 4.3.2（[2b 笔记 7.4](../notes/2b-middleware.md)）；日志和 `status.conditions` 会指出。
5. **指标没抓到**（推断，没有遇到过）：PodMonitor 的选择器 `strimzi.io/kind: cluster-operator` 对不上，或端口名 `http`（8080）变了。operator 好好的、Kafka 状态是 Ready 而只有 `MetricsMissing` 响时看它。

## 怎么确认恢复了

- `kubectl -n kafka get kafka kafka` 是 `READY True`。
- 成功计数在涨：`scripts/cluster/monitoring.sh promql 'sum by (namespace) (increase(strimzi_reconciliations_successful_total{kind="Kafka"}[20m]))'` 大于等于 1；告警在 `for` 之后消失。
- 积压的改动应用了：卡了很久的话，operator 恢复后会把攒下的改动一次应用，三个 broker 会逐个滚动（2026-11-20 那次就是修好后才滚动）；等滚完，`kafka_brokers` 是 3、没有副本不足（[kafka-brokers.md](kafka-brokers.md)、[kafka-partitions.md](kafka-partitions.md)）。
- 滚动过 broker 的，以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)`。

## 怎么人为触发它

设计 5 节第 2 层：这一条“要想一个不伤 broker 的办法，实现时定，定不出来就只做单元测试并写明”。待定，建议：把 `strimzi-cluster-operator` 缩到 0 副本（`kubectl -n kafka scale deploy/strimzi-cluster-operator --replicas=0`），10 分钟后 `KafkaOperatorMetricsMissing` 响；调和计数 20 分钟窗口加 10 分钟 `for` 之后 `NotReconciling` 才响（指标消失之后计数那一条的行为没有验证过，可以顺带观察）；做完缩回 1。operator 不在不影响 broker，是最安全的办法。要复现 round-8 的“调和失败但指标在”，把 operator 的 `KUBERNETES_SERVICE_DNS_DOMAIN` 改成一个假值再滚动（影响 Kafka 的改动，不建议）。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

- 2026-11-20 16:26 UTC 到 2026-11-22：宿主机重启后 3 分钟，operator 启动赶在集群 DNS 就绪之前，每次调和都因 `Invalid DNS name` 失败，`Kafka` 资源两天都是 `NotReady`。broker 不依赖 operator，这两天的压测没受影响，也没有任何东西报出来，是改 broker 的 requests 之后 broker 没有滚动才发现的（[round-8 记录 6.3 节](../notes/3b-rounds/round-8-cpu-requests.md)）。当时还没有这条告警，它是为这件事写的。修法：写死 `KUBERNETES_SERVICE_DNS_DOMAIN=cluster.local`，应用后 operator 恢复调和，三个 broker 逐个滚动完成。
- 这条告警本身（还没有真的响过）。
