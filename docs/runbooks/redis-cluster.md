# 缓存 Redis 集群不正常

覆盖的告警：`RedisClusterUnhealthy`（critical）

## 这条告警在说什么

缓存集群 `redis-cache`（Redis Cluster，6 个节点，三主三从，`allkeys-lru`、128mb、不持久化）满足下面任一条，持续 2 分钟：

- `redis_cluster_state == 0`：这个节点看到的集群状态不是 ok；
- `redis_cluster_slots_ok < 16384`：16384 个槽没有全部是 ok，有槽处于 fail、pfail 或未分配。

每个节点各报一个值，所以会同时响好几个 `instance`，是同一件事。不覆盖闸门（`redis-gate` 是主从加 Sentinel，不是 Cluster）。规则本身没有“出处”，来自[设计 4.4](../design/3c-slo-alerting.md)；集群的行为来自[2b 笔记第 3 节和 15.1](../notes/2b-middleware.md)。

## 影响

- 落在失败槽位上的缓存读写报错，业务走降级（回源数据库、按地址的限流桶重新装满），不是下单失败。数据库读压力和延迟会涨。
- 数据风险：缓存和限流的状态可以丢（本来就不持久化），没有业务数据，不用对账。
- 多半先影响延迟和数据库负载；critical 是因为整块槽没了缓存。现在就看，但不是丢单级别。

## 先看什么

1. 哪些节点在报、值是多少：

   ```
   scripts/cluster/monitoring.sh promql 'redis_cluster_state'
   scripts/cluster/monitoring.sh promql 'redis_cluster_slots_ok'
   scripts/cluster/monitoring.sh promql 'redis_up{cluster="redis-cache"}'
   ```
2. 集群自己怎么说（`fail`、`pfail`、`disconnected` 标志，每个主库的槽）：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-cache-0 -c redis -- redis-cli cluster nodes
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-cache-0 -c redis -- redis-cli cluster info
   ```
3. Pod 状态、有没有刚重启或换了地址：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis get pods -o wide`。
4. 是不是两个主库在同一个节点，节点挂了：`scripts/cluster/redis-cache-pairing.sh check`；`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl get nodes`。
5. 日志：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis logs redis-cache-0 -c redis --tail=100`。

## 常见的原因和各自的处理

1. **所有 Pod 同时重建、地址全变。** 节点间互连用的是 `nodes.conf` 里的 IP；本项目的启动脚本在 `redis-server` 起来前把 `nodes.conf` 的地址按 hostname 重新解析，6 个 Pod 同时删掉后 8 秒内自己恢复（2b 笔记 3.2）。超过 2 分钟不恢复，先看这个脚本的日志和 DNS 是否就绪。
2. **同一节点上有两个主库，节点挂了。** 剩下的主库凑不出多数票，整个集群不能自动切换（`redis-cache-pairing.sh` 注释，2b 笔记 15.1）。2026-11-15 重建之后 `redis-cache-0` 和它的从库 `redis-cache-4` 就在同一节点。处理：节点恢复后让主库回来；两个主库同节点的情况脚本不自动改，要做主从切换，会短暂影响写入，先问主人。
3. **主库重启得比判定失败还快，带着空数据。** 集群没有切换，主库和它的从库一起变空（2b 笔记 3.1）。这不会让集群状态变成 fail，只是缓存冷了。
4. **`CLUSTER REPLICATE` 修配对时的瞬间。** `middleware.sh redis` 之后会调 `redis-cache-pairing.sh fix`，被改挂的从库在重新同步；只涉及从库，不动槽，不应该让状态变 fail。
5. **滚动重启中途**：6 个节点逐个重启时集群状态保持 ok（round-8 6.4 的记录）；某一步卡住就会超过 2 分钟，看卡在哪个 Pod。
6. **PDB 或驱逐同时带走一主一从**（推断，没有遇到过）：PDB 是 `maxUnavailable: 1`，只约束自愿驱逐，节点宕机不受约束。2b 笔记 15.2 里 PDB 曾经一直是坏的（`SyncFailed`），修好后七个都是 1。
7. **槽位未分配**（推断，没有遇到过）：组建 Job 没做完或被打断；“半成品状态”（已知节点数不是 6）Job 不自动处理、报错让人检查（2b 笔记 3.2）。

## 怎么确认恢复了

- `redis_cluster_state` 全是 1，`redis_cluster_slots_ok` 全是 16384，告警 2 分钟内消失。
- `KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-cache-0 -c redis -- redis-cli --cluster check redis-cache-0.redis-cache.redis.svc:6379` 报 `[OK] All 16384 slots covered`；`scripts/cluster/redis-cache-pairing.sh check` 退出 0。
- 缓存只有缓存，没有业务数据，不需要对账；确认数据库负载和下单延迟回到平时即可（大盘或 SLO 的告警不再响）。

## 怎么人为触发它

设计 5 节第 2 层没有为这条列办法。待定，建议：删掉一个**主库**的 Pod（`kubectl -n redis delete pod redis-cache-1`）会在 5 秒节点超时内回来、不切换，状态仍是 ok，触发不了（2b 笔记 3.1）。要让它响得停掉一对主从或让节点失联比较重，建议改成：把规则里的 `16384` 临时改成 `16385`、下发，2 分钟后每个节点都响，再改回，并说明这只验证了“路是通的”；真实触发（同时带走一主一从）影响缓存，故障注入时再登记。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
