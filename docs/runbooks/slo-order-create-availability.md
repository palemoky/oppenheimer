# 下单可用性的错误预算在烧

覆盖的告警：`SLOOrderCreateAvailabilityBurnFast`（critical）、`SLOOrderCreateAvailabilityBurnMedium`（critical）、`SLOOrderCreateAvailabilityBurnSlow`（warning）

规则：[slo.yaml](../../deploy/cluster/monitoring/rules/slo.yaml) 的 `slo.order_create_availability` 组。设计：[3c-slo-alerting.md](../design/3c-slo-alerting.md) 第 3.2、3.3 节。

## 这条告警在说什么

SLI：gateway 上 `POST /api/v1/orders` 的 5xx 占全部请求的比例。499（客户端断开）不计入；409（库存不足）、429（限流、背压）、其他 4xx 都算好的。目标 99.9%（7 天），预算 0.001。出处：设计第 2 节第 2、3 项。

燃烧率 = 错误比例 ÷ 0.001。每档要长、短两个窗口同时超过才响，并且短窗口里有效请求不低于每秒 0.2 个（5 分钟 60 个）：

| 档 | 燃烧率 | 错误比例阈值 | 窗口（长、短） | for | 级别 | 这个速度烧完一周的预算 | 长窗口烧掉的预算占比（燃烧率 × 长窗口 ÷ 7 天，自己推的） |
|---|---|---|---|---|---|---|---|
| 快 | 14.4 | 1.44% | 1h、5m | 2m | critical | 约 12 小时 | 约 8.6% |
| 中 | 6 | 0.6% | 6h、30m | 5m | critical | 约 28 小时 | 约 21% |
| 慢 | 3 | 0.3% | 1d、2h | 15m | warning | 约 2.3 天 | 约 43% |

通知里的 `$value` 是短窗口的比例，也就是“现在有多糟”（短窗口写在表达式最左边）。没有流量时错误比例是 0 除以 0，不响；“路断了所以没请求”归探针告警管（见 [probe.md](probe.md)）。

## 影响

用户下单拿到 5xx。快的一档说明正在大面积失败，数据面上是“没受理”而不是“受理了又丢”：5xx 的请求没有拿到 202，用户可以带同一个幂等键重试。几分钟内要看。中的一档是持续的小比例失败，当天内处理。慢的一档当天看，不紧急。

## 先看什么

1. 哪一档在响、现在多糟、剩多少预算（7 天窗口，没有现成的记录规则，这条查询较重）：
   `scripts/cluster/monitoring.sh promql 'slo:order_create_availability:error_ratio_rate5m'`
   `scripts/cluster/monitoring.sh promql '1 - ((sum(increase(http_server_request_duration_seconds_count{service_name="gateway",k8s_namespace_name="oppenheimer",http_route="/api/v1/orders",http_request_method="POST",http_response_status_code=~"5.."}[7d])) or vector(0)) / sum(increase(http_server_request_duration_seconds_count{service_name="gateway",k8s_namespace_name="oppenheimer",http_route="/api/v1/orders",http_request_method="POST",http_response_status_code!="499"}[7d]))) / 0.001'`（1 是整份预算没动，0 是烧完，负数是超了）
2. 是哪个状态码：
   `scripts/cluster/monitoring.sh promql 'sum by (http_response_status_code) (rate(http_server_request_duration_seconds_count{service_name="gateway",k8s_namespace_name="oppenheimer",http_route="/api/v1/orders",http_request_method="POST",http_response_status_code=~"5.."}[5m]))'`
   503 多半是保护机制在拒绝（看第 3 步的结局），500 才像是代码或依赖在报错。
3. order 那一侧的受理结局，看是哪一种拒绝：
   `scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(order_admissions_total{k8s_namespace_name="oppenheimer"}[5m]))'`
4. 同时有哪些告警在响：`scripts/cluster/monitoring.sh alerts`。熔断器、闸门内存、探针这些“原因一级”的告警通常和它在同一组里，先往那里看（见 [breaker.md](breaker.md)、[order-gate-memory.md](order-gate-memory.md)、[order-backlog.md](order-backlog.md)）。
5. gateway 和 order 的错误日志：
   `{service_name="gateway"} | json | level="ERROR"`、`{service_name="order"} | json | level="ERROR"`；Pod 是否在重启：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods`

## 常见的原因和各自的处理

- **压测打过拐点。** 3b 里有意把到达速率推过写入的拐点时，5xx 出在这些轮次，可用性 1 小时窗口最高到过 3.5%（设计 8“第 3 步的前一半”），这几轮重跑快的一档会响，是想要的。处理：不是故障，记下是哪一轮。记录：[round-3](../notes/3b-rounds/round-3-overload-behavior.md)、[round-4](../notes/3b-rounds/round-4-mixed-and-flash-ladders.md)。
- **入口或边车的并发上限在拒绝（`503 UO`）。** 过拐点时每层保护从里到外依次动：背压、读名额、边车上限、入口上限（round-4）。处理：看 `IngressRejectingRequests`、`MeshConcurrencyLimitOverflow` 有没有在响；负载降下来就好。
- **落库健康的熔断器被一次盘的停顿打开，受理被拒 `503 ORDER_LANDING_UNAVAILABLE`。** round-3 第 4 节：盘卡一下，inventory 的 `Reserve` 超时，边车把两个副本一起摘除 5 秒。已经修过（熔断器加了 1 秒的持续条件，`Reserve` 单独 3 秒的路由，ADR-0020 的补充），再出现要查这两处有没有被改回去。
- **闸门 Redis 内存到 85%，`503 STOCK_GATE_UNAVAILABLE`。** 见 [order-gate-memory.md](order-gate-memory.md)。
- **某个服务 Pod 重启或被摘除（滚动发布、OOMKilled）。** 推断，没有专门遇到过。round-9 的 4 小时里 5xx 共 25 个、散落各处，不是集中的故障。处理：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods` 看重启次数，`describe pod` 看上次终止的原因。
- **inventory 整个不可用。** 3a 用“把 inventory 缩到 0”做过故障注入，受理在边车处直接失败。处理：恢复副本数。
- 数据库或 Kafka 慢（推断，没有遇到过导致 5xx 的情况）：先看 [outbox.md](outbox.md)、[consumer-blocked.md](consumer-blocked.md)。

预算烧完了怎么办：见 [README.md](README.md) 的总则（停止做会烧预算的实验，先处理原因）。

## 怎么确认恢复了

- 告警恢复的条件：短窗口（5m、30m、2h）的比例掉回阈值以下，或者有效请求速率低于 0.2。故障停了之后快的一档约 5 分钟恢复（单元测试钉下来的行为，设计 8“第 3 步的后一半”）。长窗口要几小时才降下来，但告警要两个窗口同时超过才响，所以不影响。
- `scripts/cluster/monitoring.sh promql 'slo:order_create_availability:error_ratio_rate5m'` 回到 0 附近。
- 下单真的通：`scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(probe_cycles_total{k8s_namespace_name="oppenheimer"}[5m]))'` 里 `ok` 占满。
- 凡是动过订单、库存的故障，最后要以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`，然后 `kubectl -n oppenheimer logs job/<名字>`，退出码 0 才算（详见 [recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节第 2 层的办法：

- 把 inventory 缩到 0 一段时间（3a 用过）：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer scale deploy/inventory --replicas=0`，探针每秒一单，全部失败，快的一档约 3 到 4 分钟响（单元测试的结果）；做完缩回 2。
- `make cluster-fault`（在 `oppenheimer-e2e` 里，不进长期运行的预算）。
- 压测打过拐点（会烧真预算，正式的一轮之前先 `make down`，见 AGENTS.md）。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
