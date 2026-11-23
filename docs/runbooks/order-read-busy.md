# order 的展示读并发名额在拒绝

覆盖的告警：`OrderReadBulkheadRejecting`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.protection` 组。出处：[1d 笔记](../notes/1d-protection.md) 5.8（舱壁隔离）、[round-3](../notes/3b-rounds/round-3-overload-behavior.md)、[round-4](../notes/3b-rounds/round-4-mixed-and-flash-ladders.md)、[round-9](../notes/3b-rounds/round-9-soak.md) 2.3 节。

## 这条告警在说什么

`pkg/bulkhead` 是不排队的计数信号量：给一类调用单独划一份资源上限，同时最多 N 个，多出来的立刻拒绝（返回 `429 READ_BUSY`；1d 时没有 `Retry-After`，round-3 之后 gateway 一律带 `Retry-After: 1`，第 1.1 节）。order 的展示读对每个订单库的并发上限是 16（连接池 25，给落库留 9 个）、冷库 8、读闸门占用 `READ_GATE_MAX_CONCURRENCY`（默认 16）。指标 `order_bulkhead_rejected_total{name}`，`name` 是 `read_db_<分片号>`、`read_gate`、`archive`。

规则：`sum by (name) (rate(order_bulkhead_rejected_total[5m])) > 1`，每秒 1 次以上，持续 10 分钟。阈值取每秒 1 次的依据：长稳时平时每分钟十来次（每秒 0.2 上下，round-9 2.3：4 小时里 `read_db_1` 共 2349 次，`read_gate` 每半小时一两百次）。

## 影响

这一类读多出来的并发直接返回“忙”（429），客户端重试都成功了（round-9）。说明这类读变慢了（它读的库或缓存慢），名额是在保护下单不被拖累：给写路径留数据库连接，让展示读不能把连接池占满。不会影响下单本身。当天看。

## 先看什么

1. 哪一个名额在拒绝、多快：
   `scripts/cluster/monitoring.sh promql 'sum by (name) (rate(order_bulkhead_rejected_total{k8s_namespace_name="oppenheimer"}[5m]))'`
2. 是不是在过拐点（读被打满）：`scripts/cluster/monitoring.sh promql 'sum by (http_response_status_code) (rate(http_server_request_duration_seconds_count{service_name="gateway",k8s_namespace_name="oppenheimer"}[5m]))'` 看 429 和总量；落库积压：`scripts/cluster/monitoring.sh promql 'max(order_gate_unclaimed_holds{k8s_namespace_name="oppenheimer"})'`（落库一排队，轮询的读成倍增加，见 [order-backlog.md](order-backlog.md)）。
3. 读的库慢不慢：`MySQLReplicaLagHigh`、`MySQLConnectionsHigh` 在不在响（`scripts/cluster/monitoring.sh alerts`），节点 CPU（Grafana 节点大盘）；闸门 Redis 慢不慢（`name="read_gate"` 时）。
4. 熔断器：`scripts/cluster/monitoring.sh promql 'max by (service_name, breaker) (breaker_state{k8s_namespace_name="oppenheimer"})'`（从库不健康时展示读直接读主库，会占主库名额，见 [breaker.md](breaker.md)）。
5. order 的日志：`{service_name="order"} | json | level=~"WARN|ERROR"`。

## 常见的原因和各自的处理

- **客户端轮询放大（真遇到过）。** 落库一排队，每个未落库的订单都在轮询，查询量和排队订单数成正比；被拒之后客户端 100 毫秒后照样再来（round-0、round-3 第 1.1 节）。已加 `Retry-After` 和退避，195 单/秒时 `READ_BUSY` 从每秒 192 次降到 2 次；244 单/秒档仍然 77 次/秒。处理：降低到达速率，处理落库积压；根本办法是服务端推送或给出按积压算的等待时间（没有做）。
- **秒杀有货时轮询放大五倍。** round-4 2.2：500 次迭代/秒就让订单读的名额每秒拒绝 182 次。
- **长稳时的零星拒绝（正常）。** round-9：每分钟十来次一直有，客户端重试都成功。两个分片很不对称：`read_db_1` 4 小时 2349 次，`read_db_0` 26 次，而两个分片的读一样多，order 看到的查询 p99 也一样。**原因没有查**。
- **读的库慢**（节点 CPU 满、盘慢、从库延迟）：名额会先满。处理：看 [order-backlog.md](order-backlog.md) 的同类原因。
- **闸门 Redis 慢**（`name="read_gate"`）：展示读读闸门占用前先占名额，占不到就只读订单行（1d 补充）；不影响正确性。
- **阈值与基线的关系**（推断）：基线约 0.2 次/秒，阈值 1 次/秒只有五倍余量；`read_db_1` 的不对称如果长期恶化会碰到。

## 怎么确认恢复了

- `scripts/cluster/monitoring.sh promql 'sum by (name) (rate(order_bulkhead_rejected_total{k8s_namespace_name="oppenheimer"}[5m]))'` 回到 0.2 上下或更低。
- 用户侧：gateway 的 429 比例回到 0 附近；`slo:order_create_latency:error_ratio_rate5m` 没有被带高。
- 读的拒绝不影响数据，不需要对账收尾；如果是过载引起的，按 [order-backlog.md](order-backlog.md) 的恢复条件，以对账差异为 0 收尾（[recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节第 2 层：压测打过拐点，订单读的名额在最后几档动过（round-4 1907 档起每秒 1 到 7 次，`READ_BUSY` 一共 793 个）。待定，建议：把 `READ_GATE_MAX_CONCURRENCY` 或订单库读上限临时调到 1 再用 k6 打读，不改仓库的默认值（改 Deployment 的环境变量，做完恢复），或者临时把阈值改到当前值以下，只验证“路是通的”。没有验证过。压测前先 `make down`。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

- 长稳时的读拒绝是每秒 0.2 上下，低于阈值。
- 2026-11-23 02:47 UTC 第一次真的响（压测，设计 [3c](../design/3c-slo-alerting.md) 第 8 节“第 14 项”）：195 单/秒那一档进 `pending`，280 单/秒时响。拒绝的是 `read_gate` 这个名额（每秒 34 到 53 次），读库的两个名额每秒不到 1 次，熔断器没有开；gateway 上查订单的 429 每秒约 46 个。原因是落库排队（队列顶在上限 2500）让轮询成倍增加，和第 2 步说的一致。停压后约 5 分钟恢复。
