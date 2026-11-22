# 限流退回进程内计数

覆盖的告警：`RateLimitFallingBack`（warning）

规则：[business.yaml](../../deploy/cluster/monitoring/rules/business.yaml) 的 `business.protection` 组。出处：[ADR-0019](../adr/0019-gateway-rate-limiting.md)、[round-4](../notes/3b-rounds/round-4-mixed-and-flash-ladders.md) 第 1.1 节、[round-8](../notes/3b-rounds/round-8-cpu-requests.md) 第 3 节。

## 这条告警在说什么

gateway 的限流有三个维度（按 IP 在进程里；按用户加接口、按接口在 Redis 里，状态放在缓存实例 `redis-cache`，不放在闸门所在的实例）。Redis 不可用时（调用超过 20 毫秒的时限也算）退回进程内的同一套规则，不放行也不拒绝，失败后的一小段时间（约 1 秒）内不再尝试 Redis（ADR-0019 决定 5）。每次退回记一次 `gateway_ratelimit_fallbacks_total`。

规则：`sum(rate(gateway_ratelimit_fallbacks_total[5m])) > 1`，也就是每秒 1 次以上，持续 5 分钟，滤掉零星的超时。**Redis 没坏时它也会响**：round-4 记到最高一档每秒 8.6 次、round-8 的 1907 请求/秒档每秒 12 次（0.6% 的请求），都是节点 CPU 满了、问 Redis 超过时限。那时响是对的（限额确实变宽了），原因在节点而不在 Redis。出处：[round-4](../notes/3b-rounds/round-4-mixed-and-flash-ladders.md)、[round-8](../notes/3b-rounds/round-8-cpu-requests.md)、[ADR-0019](../adr/0019-gateway-rate-limiting.md)。

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
- **缓存 Redis 的主从切换或 Pod 重启。** 3a 记录过 Pod 重启换地址后 Redis 复制不会自己恢复（`redis-cache-5` 断了约 3 小时，业务没受影响）。限流走的是缓存集群，集群不健康时会退回。处理：`RedisClusterUnhealthy`、`RedisReplicaLinkDown` 的 runbook（另一份），修法是 `cluster replicate`。
- **Redis 的连接池或网络。** 推断，没有遇到过。
- **正常的零星退回。** 规则注释说平时是零星的；每秒 1 次的门槛要持续 5 分钟才报。
- **阈值太低的可能**（推断）：如果在 3b 那样的高负载档反复响而 Redis 实际健康，说明阈值需要和负载挂钩；记在“历史”里，放一周看误报时调（设计第 5 节第 3 层）。

## 怎么确认恢复了

- `scripts/cluster/monitoring.sh promql 'sum(rate(gateway_ratelimit_fallbacks_total{k8s_namespace_name="oppenheimer"}[5m]))'` 低于 1，平时接近 0。
- 限流的 Redis 健康：`RedisDown`、`RedisClusterUnhealthy` 不在响。
- 如果期间有大流量涌入，限额变宽期间的订单没有特殊要求，最后照例以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%s)`（[recon.md](recon.md)）。

## 怎么人为触发它

设计第 5 节没有专门列。待定，建议：暂停缓存 Redis 的一个主库 Pod 几分钟（1d 用暂停 Redis 的办法验证过熔断，没有专门验证限流的退回），每秒超过 1 次的退回要有流量：探针每秒约 3 到 5 个请求（一单加轮询加查询），不够，需要压测或 k6 的小流量。或者在节点 CPU 压满时复现（3b 的混合读写 1907 档，会烧真预算）。压测时本机不跑测试和构建，正式一轮之前先 `make down`。

故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)。

## 历史

（还没有真的响过。3b 的 round-4、round-8 量到过超过阈值的退回速率，那时还没有这条规则。）
