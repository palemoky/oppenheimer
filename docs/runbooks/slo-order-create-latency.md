# 下单延迟的错误预算在烧

覆盖的告警：`SLOOrderCreateLatencyBurnFast`（critical）、`SLOOrderCreateLatencyBurnMedium`（critical）、`SLOOrderCreateLatencyBurnSlow`（warning）

规则：[slo.yaml](../../deploy/cluster/monitoring/rules/slo.yaml) 的 `slo.order_create_latency` 组。设计：[3c-slo-alerting.md](../design/3c-slo-alerting.md) 第 3.2、3.3 节。

## 这条告警在说什么

SLI：gateway 上 `POST /api/v1/orders` 受理成功（202）的请求里，耗时超过 200 毫秒的比例（直方图 `le="0.2"` 那一档以外的）。只数成功的请求：失败的已经在可用性里扣过一次，快速失败不该让延迟变好看。目标 99%（7 天），预算 0.01。200 毫秒来自 `GOALS.md` 的“受理 p99 低于 200 毫秒”，基线是 125 单/秒时 p99 45 毫秒（[3b-capacity.md](../notes/3b-capacity.md) 第 3 节），探针下 p99 约 10 毫秒（设计 8“探针下的基线”）。

燃烧率 = 慢请求比例 ÷ 0.01。每档两个窗口同时超过，且短窗口里 202 的速率不低于每秒 0.2 个：

| 档 | 燃烧率 | 慢请求比例阈值 | 窗口（长、短） | for | 级别 | 这个速度烧完一周的预算 | 长窗口烧掉的预算占比（自己推的） |
|---|---|---|---|---|---|---|---|
| 快 | 14.4 | 14.4% | 1h、5m | 2m | critical | 约 12 小时 | 约 8.6% |
| 中 | 6 | 6% | 6h、30m | 5m | critical | 约 28 小时 | 约 21% |
| 慢 | 3 | 3% | 1d、2h | 15m | warning | 约 2.3 天 | 约 43% |

`$value` 是短窗口的比例。直方图里没有 0.2 这一档的桶时比例是 100%（“好的”那一项补了 0），会大声误报；改 SLO 的阈值只能取桶的边界，改之前先查 `le`。

## 影响

用户下单变慢但成功。慢到客户端超时重试时会放大负载。数据没有风险。快的一档说明受理路径整体变慢，通常是系统正在过载，几分钟内要看；慢的一档当天看。

## 先看什么

1. 现在的比例和各档位的分布：
   `scripts/cluster/monitoring.sh promql 'slo:order_create_latency:error_ratio_rate5m'`
   `scripts/cluster/monitoring.sh promql 'histogram_quantile(0.99, sum by (le) (rate(http_server_request_duration_seconds_bucket{service_name="gateway",k8s_namespace_name="oppenheimer",http_route="/api/v1/orders",http_request_method="POST",http_response_status_code="202"}[5m])))'`
2. 节点是不是满了（过拐点前最先动的是 CPU 和盘）：看 Grafana 的节点大盘，或 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl top nodes`。
3. 保护机制有没有在动，说明系统在过载：
   `scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(order_admissions_total{k8s_namespace_name="oppenheimer"}[5m]))'`（`backlogged` 在涨说明落库跟不上，见 [order-backlog.md](order-backlog.md)）
4. 熔断器有没有打开：`scripts/cluster/monitoring.sh promql 'max by (service_name, breaker) (breaker_state{k8s_namespace_name="oppenheimer"})'`（见 [breaker.md](breaker.md)）。
5. gateway、order 的日志：`{service_name="gateway"} | json | level=~"WARN|ERROR"`；有没有 Pod 重启：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods`。

## 常见的原因和各自的处理

- **到达速率超过拐点。** 普通下单持续 125 到 150 单/秒是写入的上限（盘的同步刷盘）；读多写少的混合是 1907 请求/秒（节点 CPU）。过了拐点受理 p99 先变慢：混合读写拐点上 94 到 191 毫秒，过了之后全部接口一起慢五到八倍（[3b-capacity.md](../notes/3b-capacity.md) 第 3 节）。3b 的过载轮次里下单延迟的错误比例最高到过 45%（设计 8）。处理：不是故障，降低负载；记录哪一轮。
- **轮询把读路径打满，拖慢受理。** 落库一排队，等结果的客户端每个订单都在轮询，查询量和排队的订单数成正比（round-0、round-3 第 1.1 节）。已经加了 `Retry-After` 和退避，195 单/秒时受理 p99 97 毫秒；244 档仍然 280 毫秒。处理：看 `READ_BUSY` 的速率（[order-read-busy.md](order-read-busy.md)），降低到达速率。
- **两个 order Pod 挤在同一个节点上，节点 CPU 到 97%。** round-1 第 4 节遇到过。处理：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide` 看 Pod 的分布，滚动一个。
- **盘进入“慢的一档”（刷盘耗时翻倍）。** round-1、round-2 第 10 节：持续写入十几分钟后一次刷盘从 4 毫秒到 11 毫秒，静止后恢复。处理：没有办法加速它，降低写入。
- **入口单个 Envoy Pod 或 gateway CPU 到顶**（售罄的秒杀 8000 请求/秒，round-4）。
- **CPU requests 写得不对、节点超卖时中间件分到的 CPU 权重少**（round-8 第 3 节）。
- 追踪看哪一段慢（推断，没有为这条告警专门做过）：Tempo 里按 gateway 的 `POST /api/v1/orders` 找慢的链路。

预算烧完了怎么办：见 [README.md](README.md)。

## 怎么确认恢复了

- 短窗口（5m、30m、2h）的比例掉回阈值以下，或者 202 的速率低于 0.2。
- `scripts/cluster/monitoring.sh promql 'slo:order_create_latency:error_ratio_rate5m'` 回到接近 0（探针下平时是 0）。
- 探针的周期都成功：`scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(probe_cycles_total{k8s_namespace_name="oppenheimer"}[5m]))'` 里 `ok` 占满。
- 过载之后有订单受理了的，最后以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（见 [recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节第 2 层：压测打过拐点（3b 的普通下单阶梯，195 单/秒以上）；或者 `make cluster-fault` 的场景。把 inventory 缩到 0 主要触发可用性，不一定触发延迟（失败的请求不进这个 SLI），所以延迟要靠压测或者给 gateway 到 order 的路径加延迟（待定，建议用 `make cluster-fault` 里已有的“挂住 inventory”场景看受理延迟是否变慢，没有验证过）。压测时本机不跑测试和构建，正式一轮之前先 `make down`。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过）
