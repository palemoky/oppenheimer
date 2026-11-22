# Redis 从库和主库的链路断了

覆盖的告警：`RedisReplicaLinkDown`（warning）

## 这条告警在说什么

`redis_master_link_up == 0`，持续 2 分钟。只有从库才有这个序列：某个从库和它的主库之间的复制链路断了。范围是闸门（`cluster="redis-gate"`，现在的从库是 `redis-gate-0` 或 `-1`，以 Sentinel 为准）和缓存集群（`cluster="redis-cache"`，六个节点里的三个从库）。

出处：3a 里 `redis-cache-5` 断了约 3 小时没人发现，是看大盘（`master_link_down_since_seconds` 约 10260）才看到的（[3a 笔记 15.1](../notes/3a-observability.md)，[PROGRESS](../PROGRESS.md) 里搜 `redis-cache-5`）。

## 影响

- 用户无感：从库不接业务读写（闸门读写走主库，缓存的从库只在主库故障时顶上）。
- 数据风险：从库不再同步，这时主库坏了，接手的从库拿到的是旧数据，或者根本没有可用的副本。闸门上这意味着库存占用可能丢（2b 笔记 2.3：异步复制本来就可能丢最后一点，链路断着则丢得更多）。
- warning，当天处理；断着的时间越长，缺的越多。

## 先看什么

1. 哪个从库、断了多久：

   ```
   scripts/cluster/monitoring.sh promql 'redis_master_link_up == 0'
   scripts/cluster/monitoring.sh promql 'redis_master_link_down_since_seconds'
   scripts/cluster/monitoring.sh promql 'redis_instance_info{cluster="redis-gate"}'
   ```
2. 从库自己看到的复制状态（`master_host` 指向的是不是当前主库，`master_link_status`）：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-cache-5 -c redis -- redis-cli info replication
   ```
3. 缓存集群：主从关系、有没有 `fail`、从库挂在谁下面：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-cache-0 -c redis -- redis-cli cluster nodes
   ```
4. 闸门：Sentinel 认为谁是主库、从库是否被 Sentinel 认为正常：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-gate-sentinel-0 -c sentinel -- redis-cli -p 26379 sentinel replicas gate
   ```
5. Pod 最近有没有重启、换了地址：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis get pods -o wide`，主库的日志里有没有同步被拒。

## 常见的原因和各自的处理

1. **缓存集群里主库的 Pod 重启换了地址，从库连着旧地址。** 3a 里 `redis-cache-5` 就是这样：连着 `redis-cache-1` 重启之前的旧 Pod 地址，断了约 3 小时。业务没受影响。修法是 `CLUSTER REPLICATE`：在从库上 `redis-cli cluster replicate <主库的 node id>`（node id 从第 3 步的输出里取），从库丢掉原有数据重新全量同步，不动槽，对缓存没有影响（2b 笔记 15.1）。那次是主人确认之后做的。**为什么集群总线学到了新地址而复制没有跟着换，没有查**；它记下的主库仍是一个 Pod 地址，主库下次换地址多半还会断（记给了阶段 4）。
2. **主从落在同一个节点。** `scripts/cluster/redis-cache-pairing.sh check` 核对，`fix` 用 `CLUSTER REPLICATE` 改挂；两个主库在同一节点的不自动改（2b 笔记 15.1）。这是配对问题，不是链路问题，但常和它一起出现。
3. **闸门的从库起不来或在重新同步。** `redis-gate-1` 在宿主机断电后 AOF 尾部是 0，反复崩溃，从库自然没有链路；处理见 [redis-down.md](redis-down.md) 的第一条（2026-11-17 两次，修法是挪走 `appendonlydir` 和 `dump.rdb`、删 Pod、全量同步）。
4. **主库在切换中。** Sentinel 切换后旧主变从库，短时间重新同步，几十秒内恢复；超过 2 分钟不恢复再查。
5. **主库内存满、复制缓冲区被撑爆**（推断，没有遇到过）：全量同步需要内存，闸门 `noeviction` 且接近上限时可能同步不下来。先看 [redis-gate-memory.md](redis-gate-memory.md)。
6. **网络或 NetworkPolicy**（推断，没有遇到过）：节点间的网络问题；主库侧日志里有连接超时。

## 怎么确认恢复了

- `scripts/cluster/monitoring.sh promql 'redis_master_link_up'` 全是 1，告警 2 分钟内消失。
- 从库上 `info replication` 里 `master_link_status:up`；闸门的三个 Sentinel 都报 `master-link-status ok`；缓存 `cluster nodes` 里每个主库恰好一个从库、没有同节点的主从（`redis-cache-pairing.sh check` 退出 0）。
- 闸门出过切换或丢写入的，以对账差异为 0 收尾：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)`。

## 怎么人为触发它

设计 5 节第 2 层没有为这条列办法。待定。建议：缓存集群里把一个主库的 Pod 删掉（地址会变），观察从库是否又连着旧地址（这就是 `redis-cache-5` 的成因，但**没有确认能复现**）；Pod 回来之后 2 分钟内多半自己恢复，这时告警不会响。更稳妥的验证“路是通的”：把规则的 `for` 临时改成 `0m`，在从库上 `redis-cli replicaof 127.0.0.1 1`（只对闸门的从库可用，Cluster 模式不支持 `REPLICAOF`；Sentinel 会在几十秒内把它改回来，要抢在那之前看）。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
