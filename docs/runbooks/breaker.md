# 熔断器打开

覆盖的告警：`BreakerOpen`（warning）、`OrderLandingBreakerOpen`（critical）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.protection` 组。出处：[ADR-0021](../adr/0021-degradation-and-lifecycle.md)（含 2026-11-14 的补充）、[round-3](../notes/3b-rounds/round-3-overload-behavior.md) 第 4 节、[1d 笔记](../notes/1d-protection.md) 第 6 节。

## 这条告警在说什么

`pkg/breaker` 是进程里的熔断器，保护边车看不到的依赖（Redis、MySQL 的从库和冷库）：连续失败 5 次（`BreakerFailures`）打开，冷却 5 秒后半开，半开只放一个探测。指标 `breaker_state`：0 闭合、1 打开、2 半开。闸门和落库健康的熔断器还要求失败持续至少 1 秒才打开（`MinFailureSpan`），免得一次停顿被放大成 5 秒不可用（ADR-0021 的补充、round-3 第 4 节）。服务之间的熔断和并发上限在 Envoy 边车里，不在这里（见 `MeshEndpointEjected`、`MeshConcurrencyLimitOverflow`）。

- **`BreakerOpen`**：`breaker_state >= 1` 且名字不是 `landing-db-*`，持续 2 分钟。写成 `>= 1`，打开和半开来回切时“等于 1”会不断重新计时。熔断器名字有：`stock-gate`（order 的闸门 Redis）、`archive`（冷库）、`order-cache`（order 的缓存 Redis）、`stock-cache`（inventory 的缓存）、`cache_<名字>`（`pkg/cache` 的缓存）。
- **`OrderLandingBreakerOpen`**（critical）：名字是 `landing-db-<分片号>` 的，持续 2 分钟。它打开说明 order 判定这个订单库不能落库（inventory 调不通、订单库写不进去持续失败），落到这个分片上的下单在受理时就被拒绝（`503 ORDER_LANDING_UNAVAILABLE`）。两个分片，所以约一半用户下不了单。

## 影响

`BreakerOpen`：功能还在，但变慢或变少了。`stock-gate` 打开时所有下单被拒（`503 STOCK_GATE_UNAVAILABLE`），已受理的订单留在 Kafka，Redis 恢复后继续落库；`order-cache`、`stock-cache` 打开时读走降级路径；`archive` 打开时旧订单的查询快速失败。当天看，`stock-gate` 的要马上看。`OrderLandingBreakerOpen`：约一半的下单被拒，现在就要看。

## 先看什么

1. 哪个服务哪个熔断器：
   `scripts/cluster/monitoring.sh promql 'max by (service_name, breaker) (breaker_state{k8s_namespace_name="oppenheimer"})'`
2. 被熔断的依赖好不好：`scripts/cluster/monitoring.sh alerts`（`RedisDown`、`MySQLDown`、`RedisGateMemoryHigh`、`MySQLReplicaLagHigh`）；Pod：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis get pods`、`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n mysql get pods`。
3. 状态变化的日志（状态变化时各记一条）：`{service_name="order"} | json | level=~"WARN|ERROR"`，换成第 1 步的 `service_name`。
4. `landing-db-*`：落库在失败的原因，看 order 的日志和 inventory（`Reserve` 超时、订单库写不进去）；同时看边车有没有摘除 inventory：`scripts/cluster/monitoring.sh alerts` 里 `MeshEndpointEjected`。
5. 影响面：`scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(order_admissions_total{k8s_namespace_name="oppenheimer"}[5m]))'`。

## 常见的原因和各自的处理

- **盘的一次停顿被放大（真遇到过，已修）。** round-3 第 4 节：盘卡一下，inventory 的 `Reserve` 超过 1 秒的时限，几十个在途调用同时超时；落库健康熔断器连续 5 次一下凑满，边车又把两个 inventory 副本一起摘除 5 秒，受理被拒 5 秒。修法：熔断器加 1 秒持续条件，`Reserve` 单独一条 3 秒的路由（ADR-0020 补充）。修完后 `l6a` 轮 `ORDER_LANDING_UNAVAILABLE` 从 892 降到 0。再出现要看这两处是不是被改回去了。
- **熔断器自己把短暂抖动放大（1d 记录）。** 一次约 300 毫秒的停顿让十几个在途调用同时失败，闸门的熔断器打开 5 秒，落库跟着停 5 秒；加了 `MinFailureSpan`。
- **查询把闸门 Redis 的连接池占满，别的调用排队超时被算成闸门的失败。** 1d 记录：展示读读闸门占用前先占名额（`READ_GATE_MAX_CONCURRENCY`，默认 16）。
- **缓存的 Redis 挂住，熔断器打开之前每个请求多等 1 到 2 秒。** 1d 记录，已给进入缓存的读加 200 毫秒的时限。
- **依赖真的坏了。** Redis 闸门不可用：已受理的订单留在 Kafka，恢复后继续落库；故障时间会吃掉支付窗口，超过 15 分钟支付时限的直接失败（ADR-0021 的后果）。处理：恢复 Redis（Sentinel 会切主，`RedisSentinelBelowQuorum`）。订单库不可用：处理主库，看 `MySQLDown`、复制。
- **inventory 整个不可用。** 3a 的故障注入、1d 的 `make fault`：边车摘除加落库健康熔断器打开，恢复后全部落库。处理：恢复 inventory。
- 熔断器在半开时探测一直失败（推断，没有遇到过）：依赖在抖动，先看依赖。

## 怎么确认恢复了

- `scripts/cluster/monitoring.sh promql 'max by (service_name, breaker) (breaker_state{k8s_namespace_name="oppenheimer"})'` 回到 0。
- 下单恢复：`scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(order_admissions_total{k8s_namespace_name="oppenheimer"}[5m]))'` 里只剩 `admitted` 和业务上的结局；探针周期回到 `ok`。
- 熔断期间已受理的订单全部落库、没有被回收：对账差异为 0，`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）；库存无漂移（[inventory-anomaly.md](inventory-anomaly.md)）。

## 怎么人为触发它

设计第 5 节没有专门列；1d 的 `make cluster-fault`（在 `oppenheimer-e2e` 里，挂住、杀掉 inventory）验证过熔断和落库健康熔断器的打开和恢复（round-3 第 5.1 节：inventory 挂住时落库健康熔断器打开）。待定，建议：`OrderLandingBreakerOpen`：把 inventory 缩到 0，2 分钟的 `for` 后响（`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer scale deploy/inventory --replicas=0`，做完缩回 2）；`BreakerOpen`：暂停缓存或闸门的 Redis（1d 用“暂停 Redis 30 秒”验证过，要 2 分钟以上才响，做的时候注意闸门暂停会让所有下单被拒，建议只在 `oppenheimer-e2e` 里做，或者暂停缓存的 Redis）。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
