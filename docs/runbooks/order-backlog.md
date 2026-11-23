# 受理的队列在变长：背压在拒绝，或快到等待上限

覆盖的告警：`OrderBackpressureRejecting`（warning）、`OrderQueueNearWaitLimit`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.admission` 组。出处：[ADR-0011](../adr/0011-admission-backpressure.md)、[round-1](../notes/3b-rounds/round-1-replica-flush-and-payment.md) 第 6 节、[round-3](../notes/3b-rounds/round-3-overload-behavior.md) 第 1.3、2.2 节、设计 [3c](../design/3c-slo-alerting.md) 第 2 节第 2 项。

## 这条告警在说什么

下单分两段：gateway 受理（Redis 闸门预扣，返回 202），order 消费 Kafka 的 `order.submit` 落库。中间的队列就是闸门里“已受理、未认领”的占用。背压在 `Hold` 里按三条拒绝（`429 BACKLOGGED`）：未认领的占用数达到 `ADMIT_MAX_QUEUE`（集群上是 1250，等于 30 秒 × 单个分区的落库速率 44 单/秒；2026-11-23 以前是 2500，ADR-0035）；排队超过 `ADMIT_MAX_WAIT`（30 秒）的占用多于 `ADMIT_MAX_STALE`（集群上是 20，2026-11-23 以前是默认的 100）个；落库超过 30 秒没有动静时，未认领的占用超过同一个数。

- **`OrderBackpressureRejecting`**：`outcome="backlogged"` 的受理占全部受理请求的比例（5 分钟速率）超过 5%，持续 10 分钟。压测打过拐点时它本来就该响（保护在起作用），所以是 warning。出处：设计第 2 节第 2 项（背压是承诺的一部分，不进 SLO，单独配这一条）。
- **`OrderQueueNearWaitLimit`**：`max(order_gate_unclaimed_holds) ÷ sum(rate(order_persists_total[5m]))`，也就是队列里的订单按现在的落库速率要等多久，超过 30 秒的八成（24 秒），持续 5 分钟。队列长度只有回收任务的 leader 报，所以取 max。落库速率是 0 而队列里有东西时比值是无穷大，同样响，那是落库停了。出处：round-3 第 1.3 节（队列长度 ÷ 落库速率 要小于 `ADMIT_MAX_WAIT`，限制等待时间只能限制队列长度）。

## 影响

`BackpressureRejecting`：部分用户下单拿到 429（带 `Retry-After: 1`），重试即可，没有数据风险。这是系统在保护自己，但说明落库跟不上受理。`QueueNearWaitLimit`：再长下去背压会开始拒绝；更糟的是排队超过回收的宽限期（60 秒）的订单会被回收任务当成孤儿还掉，用户看到“订单成功后消失”（round-1 的 608 个）。落库停了（`+Inf`）时要几分钟内处理。

## 先看什么

1. 现在的拒绝比例、各种结局：
   `scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(order_admissions_total{k8s_namespace_name="oppenheimer"}[5m]))'`
   是背压三条里的哪一条在拒绝（2026-11-23 加的指标，镜像 `61f66d2` 之后的才有）：
   `scripts/cluster/monitoring.sh promql 'sum by (rule) (rate(order_admission_backlog_rejections_total{k8s_namespace_name="oppenheimer"}[5m]))'`
   `queue` 是队列到了 `ADMIT_MAX_QUEUE`（来得太多）；`stale` 是排队超过 30 秒的多于 `ADMIT_MAX_STALE` 个（有的队列排得太久，看落库延迟的 p99 和两个 Pod 的差别）；`stalled` 是落库 30 秒没有动静（消费者停了，按 [consumer-blocked.md](consumer-blocked.md) 查）。
2. 队列长度、落库速率、折成的等待秒数：
   `scripts/cluster/monitoring.sh promql 'max(order_gate_unclaimed_holds{k8s_namespace_name="oppenheimer"})'`
   `scripts/cluster/monitoring.sh promql 'sum(rate(order_persists_total{k8s_namespace_name="oppenheimer"}[5m]))'`
   `scripts/cluster/monitoring.sh promql 'max(order_gate_unclaimed_holds{k8s_namespace_name="oppenheimer"}) / sum(rate(order_persists_total{k8s_namespace_name="oppenheimer"}[5m]))'`
3. 消费者有没有被挡住、有没有积压：
   `scripts/cluster/monitoring.sh promql 'max by (service_name, messaging_consumer_group_name, messaging_destination_name) (kafkax_consumer_oldest_pending_age_seconds{k8s_namespace_name="oppenheimer"})'`
   `scripts/cluster/monitoring.sh promql 'sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup="order.order-submit"})'`
4. 落库的延迟和盘：`scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le) (rate(order_persist_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`；节点的刷盘看 Grafana 节点大盘。
5. order 的日志（`hold_missing` 表示占用已被回收）：`{service_name="order"} | json | level=~"WARN|ERROR"`；落库健康的熔断器：`scripts/cluster/monitoring.sh promql 'max by (service_name, breaker) (breaker_state{k8s_namespace_name="oppenheimer",breaker=~"landing-db-.*"})'`（见 [breaker.md](breaker.md)）。

## 常见的原因和各自的处理

- **到达速率超过落库能力，盘的同步刷盘到顶（最常见）。** 持续 125 到 150 单/秒是普通下单的上限（round-0、round-2 第 10 节、[3b-capacity.md](../notes/3b-capacity.md)）；round-3 的 244 单/秒档背压第一次按队列长度触发，每秒拒绝 13.5 个。处理：降低受理速率；这是预期的保护，不是故障。
- **队列上限是静态的，换盘或加分片后没有跟着重算。** round-1 第 6 节：默认 1 万单在落库 100 单/秒时要排 100 秒，超过 60 秒的宽限期，608 个订单被回收；round-3 改成 2500。处理：落库速率变了就重算 `ADMIT_MAX_QUEUE`（`deploy/cluster/services/order.yaml` 里有算法）。
- **全局的上限只管得住总数，管不住单条队列（2026-11-23 压测量到，ADR-0035）。** 落库是 4 个分区、每个分区 24 条 lane 各落各的，队列在它们之间会不均；上限 2500 时落库延迟 p99 到过 57 秒（2500 ÷ 单个分区的 44 单/秒）。上限已改成 1250。背压的第 2 条限制的是排队超过 30 秒的占用的个数（100），不是时长。看两个 order Pod 的落库延迟差多少：`scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le, k8s_pod_name) (rate(order_persist_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`。
- **全局的上限在慢的分片上偏松。** round-3 2.2：上限是全局一个数，两个分片的落库速率不同，慢的分片排得长，落库延迟 p99 是 27.8 秒而不是算出来的 12 秒，离 30 秒的上限余量比算的小。还没有按分片分开。
- **盘进入慢的一档**（刷盘耗时翻倍）：写入上限更低（round-1 第 5 节、round-2 第 10 节）。
- **order 的消费者被一条消息挡住或不在了**（落库速率 0，比值 `+Inf`）：看 [consumer-blocked.md](consumer-blocked.md)。
- **一次盘的停顿被熔断器、边车摘除放大成 5 秒不落库。** round-3 第 4 节，已修（落库健康熔断器加 1 秒持续条件，`Reserve` 单独 3 秒路由）。
- **闸门 Redis 满了认领不了占用**：[order-gate-memory.md](order-gate-memory.md)。
- **inventory 的 `Reserve` 慢或 inventory 不可用**：落库要调它，看 inventory 的 Pod 和 `MeshEndpointEjected`。
- 轮询放大：等待的订单越多轮询越多，把读路径打满，反过来拖慢落库的下游（round-3 第 1.1 节，已加 `Retry-After` 和退避）。

## 怎么确认恢复了

- 拒绝比例回到 0：`scripts/cluster/monitoring.sh promql 'sum(rate(order_admissions_total{k8s_namespace_name="oppenheimer",outcome="backlogged"}[5m]))'`。
- 队列清空、等待时间回到几秒以内：第 2 步的比值回到 24 以下并稳定（平时队列几十个、落库延迟 p99 约 3 秒）。
- 没有订单被回收：order 日志里没有新的 `hold_missing`。
- 对账差异为 0，受理过的订单全部在库里：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）；压测之后用 `scripts/cluster/loadtest.sh verify`。

## 怎么人为触发它

设计第 5 节第 2 层：压测打过写入的拐点（3b 的普通下单阶梯：125、156、195、244 单/秒各 3 分钟，244 档背压触发，round-3 的运行号 `l5a-order-overload` 可参照）。**244 档的拒绝比例只有约 5.7%，贴着 5% 的线，各 3 分钟也不够 `for` 的 10 分钟**；2026-11-23 实际用的是 125、156、195 各 3 分钟再 280 单/秒 25 分钟（运行号 `3c14-order-ladder`），它同时会让落库时延的三档、`NodeCPUHigh`、`OrderReadBulkheadRejecting`、`RateLimitFallingBack` 响，停压后 `ConsumerBlocked`（延迟关单落后）和 `ServerDiskLow`（链路把 k3s-server 的盘写掉约 8 GB）也会响。`OrderQueueNearWaitLimit` 也可以靠停 order 的消费者（落库速率 0，`+Inf`），待定：停 order 同时会让受理失败，更干净的办法是在 `oppenheimer-e2e` 里做，没有验证过。压测时本机不跑测试和构建，正式一轮之前先 `make down`。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

- 2026-11-23 02:50 UTC，`OrderBackpressureRejecting` 第一次真的响（压测，设计 [3c](../design/3c-slo-alerting.md) 第 8 节“第 14 项”）：普通下单 280 单/秒，受理稳定在约 190 单/秒，每秒拒绝约 90 个（约三分之一）；从背压开始拒绝到告警响约 11 分钟（5 分钟的速率加 `for` 10 分钟），停压后约 5 分钟恢复。第 1 到 4 步的查询在现场都能跑。队列一直顶在上限（2497），折成的等待 13 到 14 秒，`OrderQueueNearWaitLimit` 没有响：队列上限 2500 在落库 190 单/秒时把这个比值压在 24 秒以下，它只在落库速率掉到约 104 单/秒以下才够得着。
- 同一次里落库延迟的 p99 到过 57 秒（上限按总速率算是 13 秒），离回收的宽限期（60 秒）只差 3 秒；没有订单被回收（落库结局里没有 `hold_missing`，受理 380792、落库 380790）。“全局的上限在慢的分片上偏松”那一条在 280 单/秒时比 244 单/秒时（27.8 秒）严重得多。
