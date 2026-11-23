# Redis 连不上

覆盖的告警：`RedisDown`（critical）

## 这条告警在说什么

`redis_up{instance!~"redis-gate-sentinel-.*"} == 0`，持续 2 分钟。redis_exporter 连不上这个 Redis 实例；exporter 自己活着，所以 `up` 是 1，要看 `redis_up`（规则文件开头同一条思路）。范围是 exporter 抓的数据节点：`redis-gate`（`redis-gate-0`、`-1`）、`redis-cache`（`redis-cache-0..5`），`cluster` 标签是 `redis-gate` 或 `redis-cache`。**不含三个 Sentinel**（`redis-gate-sentinel-0..2`，2026-11-23 改）：exporter 的目标里 Sentinel 和数据节点长得一样，原来不分，2026-11-22 的故障注入里 Sentinel 缩到 1，多响了两条 critical 的“Redis 连不上”；而少 Sentinel 的后果是“主库坏了切不了”，数据面不受影响。Sentinel 归 `RedisSentinelBelowQuorum` 和 `RedisSentinelDown`（[redis-sentinel.md](redis-sentinel.md)）。exporter 连接超时 3 秒，小于抓取超时，所以 Redis 挂了报的是 `redis_up 0` 而不是抓取失败。出处：[3a 第 5 步](../design/3a-observability.md)。

## 影响

- `redis-cache` 的一个节点：Cluster 内部有从库顶上；缓存和按地址的限流走降级（回源数据库、桶重新装满），用户基本无感。
- `redis-gate` 的主库：Sentinel 在约 7 秒内切换（实测 6.9 秒，[2b 笔记 2.1](../notes/2b-middleware.md)）；这期间库存预扣和幂等键写入失败，下单受理报错，闸门的熔断器会打开。切换后恢复。
- `redis-gate` 的从库：不影响受理，但少了一份副本，这时主库再坏就没有可切的了。Sentinel 不在这条告警里，见 [redis-sentinel.md](redis-sentinel.md)。
- 数据风险：闸门是异步复制，切换时可能丢最后一点没同步的写入（2b 笔记 2.3，预期由 MySQL 侧兜，没有实验）。critical，现在就看。

## 先看什么

1. 哪个实例：

   ```
   scripts/cluster/monitoring.sh promql 'redis_up{instance!~"redis-gate-sentinel-.*"} == 0'
   scripts/cluster/monitoring.sh promql 'redis_instance_info{cluster="redis-gate"}'
   ```
   后一条里的 `role` 标签告诉你谁是主库。
2. Pod 状态和重启：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis get pods -o wide
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis describe pod redis-gate-1 | rg -A8 'Last State|Events'
   ```
3. 日志：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis logs redis-gate-1 -c redis --tail=100`（Loki：`{k8s_namespace_name="redis", k8s_pod_name="redis-gate-1"}`）。
4. Sentinel 认为谁是主库：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-gate-sentinel-0 -c sentinel -- redis-cli -p 26379 sentinel get-master-addr-by-name gate`。
5. exporter 自己是不是有问题（只有一个实例是 0，别的都是 1，多半是实例本身）：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring logs deploy/redis-exporter --tail=50`。

## 常见的原因和各自的处理

1. **断电后 AOF 文件尾部是 0，从库起不来。** 2026-11-17 `redis-gate-1` 两次（[PROGRESS](../PROGRESS.md) 里搜 `redis-gate-1`）：宿主机断电，`appendonly.aof.N.incr.aof` 尾部几百字节全是 0，Redis 把它当格式错误而不是截断，`aof-load-truncated` 救不了，CrashLoopBackOff。修法：在 Pod 所在节点上把 `appendonlydir` 和 `dump.rdb` 挪走（不要删，先留着），删 Pod 重建，它作为从库向 Sentinel 问到主库后全量同步；核对三个 Sentinel 都报 `master-link-status ok`。**先确认另一个实例是好的主库**，它坏的时候如果是主库，断电前没同步出去的写入会丢（那次丢的是限流和租约这类会过期的键）。
2. **主库被切走、Pod 重启得比判定下线还快。** Sentinel 判定下线要 5 秒，Pod 两三秒就回来，有十几秒两个实例都自认为主库，之后旧主被改成从库（2b 笔记 2.2）。那次没有写入到达旧主，是因为客户端的旧连接随 Pod 重建断了。不用处理，看最后 Sentinel 的主库是谁。
3. **闸门内存写满，写入报 OOM。** 见 [redis-gate-memory.md](redis-gate-memory.md)。
4. **缓存集群的主库重启后是空的。** `redis-cache` 不持久化，主库快速重启会带着空数据继续当主库，从库随之变空（2b 笔记 3.1）。缓存和限流可以接受，不用处理。
5. **宿主机重启或虚拟机调内存。** 三台虚拟机逐台重启，集群自己恢复，Sentinel 会切一次（PROGRESS 里 ADR-0030 第 5 项那次）。等恢复，再核对主从。
6. **OOMKilled**（推断，没有遇到过）：闸门容器 1 Gi 而 `maxmemory` 768mb，缓存 128mb，正常不会；`describe` 的 `Last State` 能看到。
7. **只有 exporter 连不上**（推断，没有遇到过）：实例本身是好的，应用没报错。

## 怎么确认恢复了

- `scripts/cluster/monitoring.sh promql 'redis_up'` 全是 1（Sentinel 的也在里面，但它们不影响这条告警），告警在下一个求值周期消失。
- 闸门：`redis_instance_info{cluster="redis-gate"}` 里恰好一个 `master`；`redis_master_link_up` 是 1（[redis-replication.md](redis-replication.md)）；三个 Sentinel 都在（[redis-sentinel.md](redis-sentinel.md)）。
- 闸门出过事的，库存和订单要对：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)`，差异为 0 才算完。

## 怎么人为触发它

设计 5 节第 2 层没有为这条专门写办法。建议（待定，没有验证过）：缓存集群里删一个**从库**的 Pod（影响最小），`kubectl -n redis delete pod redis-cache-5`，Pod 重建期间 `redis_up` 是 0，但 2 分钟的 `for` 可能撑不满，Pod 通常几秒到十几秒就回来。要稳定触发可以把规则的 `for` 临时改成 `0m`，或 `kubectl -n redis scale` 之类让实例持续不在（会影响副本数，先登记）。不要对闸门的主库这样做。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
