# 落库时延的错误预算在烧

覆盖的告警：`SLOOrderPersistDelayBurnFast`（critical）、`SLOOrderPersistDelayBurnMedium`（critical）、`SLOOrderPersistDelayBurnSlow`（warning）

规则：[slo.yaml](../../deploy/cluster/monitoring/rules/slo.yaml) 的 `slo.order_persist_delay` 组。设计：[3c-slo-alerting.md](../design/3c-slo-alerting.md) 第 3.2、3.3 节。

## 这条告警在说什么

SLI：从受理（202）到订单落库超过 10 秒的订单占全部落库订单的比例，量的是直方图 `order_persist_delay_seconds`（`le="10"` 以外的）。这是异步的 SLO：用户拿到“已受理”，订单要多久才真的存在。目标 99%（7 天），预算 0.01。10 秒的出处：基线 p99 是 2.4 到 3.2 秒（125 单/秒 4 小时，[round-9](../notes/3b-rounds/round-9-soak.md) 2.3），背压的 `ADMIT_MAX_WAIT`（30 秒）是它的上界（设计第 2 节第 3 项）。探针下 p99 85 毫秒。

| 档 | 燃烧率 | 慢订单比例阈值 | 窗口（长、短） | for | 级别 | 烧完一周预算 | 长窗口烧掉的预算占比（自己推的） |
|---|---|---|---|---|---|---|---|
| 快 | 14.4 | 14.4% | 1h、5m | 2m | critical | 约 12 小时 | 约 8.6% |
| 中 | 6 | 6% | 6h、30m | 5m | critical | 约 28 小时 | 约 21% |
| 慢 | 3 | 3% | 1d、2h | 15m | warning | 约 2.3 天 | 约 43% |

短窗口里落库速率不低于每秒 0.2 个才算数。`$value` 是短窗口的比例。注意：落库停住时没有新的观测值，这个比例没有分子也没有分母，**告警会自己消失**（设计 8 的行为 1）；落库完全停了要靠 [order-backlog.md](order-backlog.md) 的 `OrderQueueNearWaitLimit`（落库速率为 0 时比值是无穷大）和 [consumer-blocked.md](consumer-blocked.md) 发现。

## 影响

用户拿到“已受理”之后订单迟迟查不到。超过回收的宽限期（60 秒）还没落库的订单，占用会被回收任务当成孤儿还掉，用户看到“下单成功，之后订单不见了”（[round-1](../notes/3b-rounds/round-1-replica-flush-and-payment.md) 第 6 节，608 个）。所以快的一档要几分钟内处理；慢订单里数据本身没丢，但再慢下去就会丢受理。

## 先看什么

1. 现在的比例和 p99：
   `scripts/cluster/monitoring.sh promql 'slo:order_persist_delay:error_ratio_rate5m'`
   `scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le) (rate(order_persist_delay_seconds_bucket{k8s_namespace_name="oppenheimer"}[5m])))'`
2. 落库速率和队列长度、折成等待时间：
   `scripts/cluster/monitoring.sh promql 'sum(rate(order_persists_total{k8s_namespace_name="oppenheimer"}[5m]))'`
   `scripts/cluster/monitoring.sh promql 'max(order_gate_unclaimed_holds{k8s_namespace_name="oppenheimer"})'`
3. 消费者积压和是否被挡住：
   `scripts/cluster/monitoring.sh promql 'sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup="order.order-submit"})'`
   `scripts/cluster/monitoring.sh promql 'max by (service_name, messaging_consumer_group_name, messaging_destination_name) (kafkax_consumer_oldest_pending_age_seconds{k8s_namespace_name="oppenheimer"})'`
4. 背压在不在拒绝：`scripts/cluster/monitoring.sh promql 'sum(rate(order_admissions_total{k8s_namespace_name="oppenheimer",outcome="backlogged"}[5m]))'`。
5. order 的日志（落库的错误、`hold_missing`）：`{service_name="order"} | json | level=~"WARN|ERROR"`；Pod：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide | rg order`。

## 常见的原因和各自的处理

- **盘的同步刷盘到顶（最常见）。** 写入持续 125 到 150 单/秒是上限，过了落库延迟先上涨，之后 `429 BACKLOGGED`（[3b-capacity.md](../notes/3b-capacity.md) 第 3 节，round-0、round-2 第 10 节）。盘进入“慢的一档”（刷盘耗时翻倍）时上限更低。3b 过载轮次里落库时延的错误比例最高到过 75%（设计 8）。处理：降低受理速率；没有别的办法（主人已定接受这个上限）。
- **队列上限太大，排队时间超过回收宽限期。** round-1 第 6 节 608 个订单消失；round-3 第 1.3 节把 `ADMIT_MAX_QUEUE` 定成 2500（125 单/秒 × 20 秒）。换盘、加分片之后这个数要重算（`deploy/cluster/services/order.yaml` 里有算法）。
- **一次盘的停顿被边车和熔断器放大成 5 秒不落库。** round-3 第 4 节，已修（熔断器 1 秒持续条件，`Reserve` 单独 3 秒路由）。
- **inventory 消费 `order.events` 跟不上（约 105 条/秒的上限）。** round-1 第 5 节：影响的是预占迟迟不结算，订单状态不受影响。它为什么停在约 105 条/秒没有查清。
- **order.submit 的消费者被一条消息挡住。** 看 [consumer-blocked.md](consumer-blocked.md)。
- **闸门 Redis 满了，认领不了占用。** 见 [order-gate-memory.md](order-gate-memory.md)（round-2 第 6 节，5672 个已受理订单卡住）。
- 分区 rebalance、消费者 Pod 被杀（1b 记录：被杀的消费者名下分区停摆 45 秒，后来把会话超时改成 10 秒）。

预算烧完了怎么办：见 [README.md](README.md)。

## 怎么确认恢复了

- 短窗口比例降到阈值以下：`slo:order_persist_delay:error_ratio_rate5m` 回到 0 附近，`order_gate_unclaimed_holds` 回到几十以下。
- 积压清空：`kafka_consumergroup_lag{consumergroup="order.order-submit"}` 回到几百以内（平时的水平，round-9 2.3）。
- 对账：受理过的订单全部落库、没有被回收。`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，差异为 0（[recon.md](recon.md)）。注意 `recon` 在消费者没追上时会把“还没追上”报成差异（设计 8“`recon` 的 CronJob”第 2 点），先等积压清掉。

## 怎么人为触发它

设计第 5 节第 2 层：压测打过写入的拐点（3b 的普通下单阶梯，156 单/秒以上落库延迟 p99 超过 9 秒）；或者停 order 的消费者几分钟（3a 用过停消费者的办法）：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer scale deploy/order --replicas=0`（这会同时让受理失败，更干净的办法待定，建议只在 `oppenheimer-e2e` 里试）。恢复后积压的订单落库时延都很长，快的一档在几分钟内响。压测前先 `make down`。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
