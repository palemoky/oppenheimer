# 限流退回进程内计数

覆盖的告警：`RateLimitFallingBack`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.protection` 组。出处：[ADR-0019](../adr/0019-gateway-rate-limiting.md)、[round-4](../notes/3b-rounds/round-4-mixed-and-flash-ladders.md) 第 1.1 节、[round-8](../notes/3b-rounds/round-8-cpu-requests.md) 第 3 节。

## 这条告警在说什么

gateway 的限流有三个维度（按 IP 在进程里；按用户加接口、按接口在 Redis 里，状态放在缓存实例 `redis-cache`，不放在闸门所在的实例）。Redis 不可用时（调用超过 20 毫秒的时限也算）退回进程内的同一套规则，不放行也不拒绝。**2026-11-23 起（ADR-0035）：一次失败只让那一个请求退回，连续失败 5 次才在之后约 1 秒内不再尝试 Redis**；在那之前是一次失败就停 1 秒（ADR-0019 决定 5）。每次退回记一次 `gateway_ratelimit_fallbacks_total`。

规则（2026-11-23 起按比例）：退回的速率除以 gateway 全部 HTTP 请求的速率（都是 5 分钟窗口）超过 0.1，并且退回每秒 0.2 次以上，持续 5 分钟。一个请求最多退回一次，所以这个比例就是“有多大一部分请求的限额变宽了”。一成的来由：限流不再整段退回之后，过载最重的一档是 3.4%（第三轮过载压测，每一次退回只是超时的那个请求自己）；整段退回时是 21% 到 47%；Redis 真的不在是接近 100%。后一半是流量很小时的下限（几个请求里一个超时也能算出一成）。**所以现在它响，说明不是零星的超时**：要么 Redis 成片失败，要么“连续失败并持续 200 毫秒”的判据在反复打开。

改之前（到 2026-11-23）是按次数：`sum(rate(gateway_ratelimit_fallbacks_total[5m])) > 1`，过载时零星的超时就够它响，三轮过载压测每一轮都响了，第三轮响的时候限额实际只对 3.4% 的请求变宽。round-4 记到最高一档每秒 8.6 次、round-8 的 1907 请求/秒档每秒 12 次，都是节点 CPU 满了、问 Redis 超过时限（**这两个数是超时的次数；那时每次超时之后 1 秒的请求都退回，退回的次数是它的两百倍上下**，2026-11-23 更正）。出处：[round-4](../notes/3b-rounds/round-4-mixed-and-flash-ladders.md)、[round-8](../notes/3b-rounds/round-8-cpu-requests.md)、[ADR-0019](../adr/0019-gateway-rate-limiting.md)。

## 影响

退回期间每个 gateway 副本各算各的，总的限额变成配置的副本数倍（2 个副本是 2 倍；热点 SKU 的桶本来就在进程里，实际是 1000 而不是 500）。限流没有消失但变宽。系统最忙的时候恰好是限流最松的时候（round-4 第 1.1 节）。当天看；如果同时在被打，要马上看。没有数据风险。

## 先看什么

1. 退回的速率和占请求的比例：
   `scripts/cluster/monitoring.sh promql 'sum(rate(gateway_ratelimit_fallbacks_total{k8s_namespace_name="oppenheimer"}[5m]))'`
   `scripts/cluster/monitoring.sh promql 'sum(rate(http_server_request_duration_seconds_count{service_name="gateway",k8s_namespace_name="oppenheimer"}[5m]))'`
2. **节点的 CPU 是不是满了**（round-4 的原因就是它，不是 Redis 慢）：Grafana 的节点大盘，或 `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl top nodes`。
3. 限流的 Redis（缓存实例）好不好：`scripts/cluster/monitoring.sh alerts` 里有没有 `RedisDown`、`RedisClusterUnhealthy`、`RedisReplicaLinkDown`；`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis get pods`。
4. gateway 的日志和 Pod 分布：`{service_name="gateway"} | json | level=~"WARN|ERROR"`；`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer get pods -o wide | rg gateway`。
5. 是否在过拐点：`scripts/cluster/monitoring.sh promql 'sum by (outcome) (rate(order_admissions_total{k8s_namespace_name="oppenheimer"}[5m]))'`（`backlogged` 在涨说明在过载，见 [order-backlog.md](order-backlog.md)）。

## 常见的原因和各自的处理

- **节点 CPU 满了，限流 Redis 调用超时（真遇到过）。** round-4：最高一档限流的 Redis 每秒超时 8.6 次（`evalsha` 超过 20 毫秒的时限），原因是节点 CPU 到顶，不是 Redis 自己慢。round-8：1907 档每秒 12 次退回，0.6% 的请求。当时没有造成后果（被放宽的是按用户的限额）。处理：降低负载；这是过载的症状，不是限流的故障。保护机制自己的降级路径要在高负载下看，不能只在“Redis 停了”的故障注入里看。
- **（已改，ADR-0035；改之前的镜像上仍然是这样）一次超时就打开 1 秒，流量大时等于一直开着（2026-11-23 压测时看到的，机制读过代码，`pkg/ratelimit/fallback.go`）。** 改了之后退回的次数应当接近超时的次数；仍然是它的上百倍就说明跑的是旧镜像，或者 Redis 真的成片失败。 问 Redis 的时限是 20 毫秒，任何一个请求超时就让这个 Pod 之后 1 秒的请求全部走进程内；每个 Pod 每秒几百个请求时，节点一忙总有一个超过 20 毫秒，所以退回的比例不是“超时的比例”，而是接近一半。看退回占请求的比例（第 1 步），不要只看每秒的次数。处理同上：降低负载。
- **缓存 Redis 的主从切换或 Pod 重启。** 3a 记录过 Pod 重启换地址后 Redis 复制不会自己恢复（`redis-cache-5` 断了约 3 小时，业务没受影响）。限流走的是缓存集群，集群不健康时会退回。处理：`RedisClusterUnhealthy`、`RedisReplicaLinkDown` 的 runbook（另一份），修法是 `cluster replicate`。
- **Redis 的连接池或网络。** 推断，没有遇到过。
- **零星的退回不会让它响**（2026-11-23 起）：过载时个别请求问 Redis 超过 20 毫秒只让那一个请求退回，占比是百分之几，够不着一成。想看零星的超时有多少，看第 1 步的速率。

## 怎么确认恢复了

- 第 1 步的两个数相除低于 0.1；平时退回的速率接近 0。
- 限流的 Redis 健康：`RedisDown`、`RedisClusterUnhealthy` 不在响。
- 如果期间有大流量涌入，限额变宽期间的订单没有特殊要求，最后照例以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节没有专门列。待定，建议：暂停缓存 Redis 的一个主库 Pod 几分钟（1d 用暂停 Redis 的办法验证过熔断，没有专门验证限流的退回），按比例之后探针的流量就够（每秒约 5 个请求全部退回，比例接近 100%，单元测试 v 是这个情形），不用压测。过载的压测现在不该让它响（第三轮是 3.4%）；响了就说明又在整段退回。压测时本机不跑测试和构建，正式一轮之前先 `make down`。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

- 3b 的 round-4、round-8 量到过超过阈值的退回速率（每秒 8.6、12 次），那时还没有这条规则。
- 2026-11-23 02:39 UTC 第一次真的响（压测，设计 [3c](../design/3c-slo-alerting.md) 第 8 节“第 14 项”），是这一轮最早响的一条：普通下单 125 单/秒时是 0，156 单/秒时每秒 48 次（约 6% 的请求，节点 CPU 71% 和 82%，还没到顶），195 单/秒时 444 次（35%），280 单/秒时约 800 次（50%）；整轮 123 万次，占 gateway 289 万请求的 42%。缓存 Redis 自己只用 0.35 核，没有不正常的 Pod。日志里是“恢复”之后几十到几百毫秒又“失败”，每次打开 1 秒。**比 3b 记的“每秒 12 次”高两个量级，查清了：3b 记的是 Redis 超时的次数，不是退回的次数**（设计第 8 节“第 14 项的追查”第 1 条）；同一个场景 2026-11-21 那一轮的退回是每秒 0、6、150、792 次，行为没有变。停压后约 5 分钟恢复。
- 2026-11-23 的三轮过载压测每一轮都响了。第三轮（限流已经不整段退回）响的时候退回只占请求的 3.4%，是按次数的阈值造成的；当天把规则改成按比例（一成），加了单元测试。**改了之后还没有在集群上真实触发过**（要让限流的 Redis 真的不可用）。
