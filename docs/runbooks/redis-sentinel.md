# Sentinel 不够法定数

覆盖的告警：`RedisSentinelBelowQuorum`（warning）、`RedisSentinelDown`（warning）

## 这条告警在说什么

`redis_sentinel_master_ok_sentinels < on (instance, master_name) redis_sentinel_master_setting_ckquorum`，持续 2 分钟：某个 Sentinel 看到的（状态正常的）Sentinel 数，少于配置的法定数。闸门的 Sentinel 是 3 个（`redis-gate-sentinel-0..2`），`quorum: 2`、监控的主库名 `gate`（`deploy/cluster/redis/redis-gate.yaml`）。

所以它只在**只剩 1 个**时响：3 个掉到 2 个不报（设计第 8 节“第 4 步的另一半”里记了这一点，“要不要掉一个就报”没有定）。3 个掉到 2 个、三个全不在（没有还活着的 Sentinel 来报自己看到几个同伴，这条不响）都由下面的 `RedisSentinelDown` 报。以前这两种情况靠 `RedisDown` 报（Sentinel 也被 redis_exporter 抓），2026-11-23 起 `RedisDown` 不再包含 Sentinel（[redis-down.md](redis-down.md)）：少 Sentinel 不该和“闸门 Redis 连不上”一个级别。出处：[设计 4.4](../design/3c-slo-alerting.md)；Sentinel 掉线时两个指标的行为**没有实测**，只有单元测试。

- **`RedisSentinelDown`**（warning，2026-11-23 新增）：`redis_up{instance=~"redis-gate-sentinel-.*"} == 0`，持续 5 分钟：exporter 连不上这个 Sentinel。少一个就报（那时 `RedisSentinelBelowQuorum` 还不响，但已经没有余量）；三个都不在时三个实例各一条，合成一条通知。`for` 5 分钟是为了 Sentinel 的 Pod 重启一次不报。exporter 自己不在时这条不响（序列没有了），那是自带的 `TargetDown` 的事。

## 影响

- 用户无感，闸门照常读写。
- 少于法定数就选不出新主：这时闸门主库坏了不会自动切换，下单受理直到人工处理才恢复（critical 级别的损失，但此刻还没发生）。
- 数据风险：没有；风险在于“下一次主库故障没有保护”。warning，当天处理。
- `RedisSentinelDown`：少一个时没有影响，只是没有余量了；少两个以上时主库坏了不会自动切换（那时 `RedisSentinelBelowQuorum` 也在响，三个全不在时除外）。现在的主从不受影响，下单照常。

## 先看什么

1. 每个 Sentinel 看到几个，法定数多少：

   ```
   scripts/cluster/monitoring.sh promql 'redis_sentinel_master_ok_sentinels'
   scripts/cluster/monitoring.sh promql 'redis_sentinel_master_setting_ckquorum'
   scripts/cluster/monitoring.sh promql 'redis_up{cluster="redis-gate"}'
   ```
2. Sentinel 的 Pod：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis get pods -o wide | rg sentinel
   ```
3. 存活的那个 Sentinel 怎么看当前的局面（`num-other-sentinels`、`quorum`、`flags`）：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-gate-sentinel-0 -c sentinel -- redis-cli -p 26379 sentinel master gate
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis exec redis-gate-sentinel-0 -c sentinel -- redis-cli -p 26379 sentinel ckquorum gate
   ```
4. 日志（`+sdown`、`-sdown`、`+sentinel`）：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis logs redis-gate-sentinel-1 -c sentinel --tail=100`。
5. 主库现在还好吗：`redis_up{cluster="redis-gate"}`、`redis_instance_info{cluster="redis-gate"}`（谁是 `master`）；主库已经坏了的话先去 [redis-down.md](redis-down.md)，因为切不了。

## `RedisSentinelDown` 先看什么

1. Sentinel 的 StatefulSet 是不是被缩到 0，Pod 状态：

   ```
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis get pods -o wide | rg sentinel
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n redis get statefulset redis-gate-sentinel
   ```
2. Pod 不在或在重启的话，`describe pod` 的 `Last State` 和 Events，日志同上面第 4 步；`redis_up{cluster="redis-gate"}` 看 exporter 是不是只是连不上。
3. 主库现在还好吗：同上面第 5 步；主库已经坏了的话先去 [redis-down.md](redis-down.md)，因为切不了。

## 常见的原因和各自的处理

1. **节点重启或虚拟机调整，Sentinel 一起掉了。** 逐台重启时 Sentinel 会各掉一次，Sentinel 会切一次主（PROGRESS 里 ADR-0030 第 5 项那次、设计 2b 的发现 18）。等它们回来，Sentinel 靠重新连上互相发现。`for: 2m` 一般等得过去。
2. **多个 Sentinel 落在同一个节点，节点挂了。** Sentinel 的 StatefulSet 带 `podAntiAffinity`（`deploy/charts/redis-ha/templates/sentinel-statefulset.yaml`），是硬是软没有核实；三个 Sentinel 在三个节点才是预期的（推断，没有遇到过）；先看第 2 步的 `-o wide`。
3. **内存上限太小被 OOMKilled。** Sentinel 的容器上限是 64Mi（`redis-gate.yaml`）（推断，没有遇到过）；`describe pod` 的 `Last State`。
4. **Pod 换了地址后 Sentinel 之间互相找不到。** 2b 里 Redis 的复制在 Pod 换地址后断过（缓存集群的 `redis-cache-5`），Sentinel 之间是否有同类问题**没有验证**（推断）。看 Sentinel 日志里的 `+sentinel` 和 `-sdown`；一个 Sentinel 重启后可以用 `sentinel reset gate` 让它重新发现，但先确认不会影响当前的主库判断。
5. **exporter 读不到**（推断，没有遇到过）：如果三个 Sentinel 的 `redis_up` 都没有 1 而 Sentinel 实际都在，会表现为 `RedisSentinelDown`（2026-11-23 起 `RedisDown` 不看 Sentinel）。先用第 3 步的 `sentinel master gate` 确认 Sentinel 到底在不在。

## 怎么确认恢复了

- `redis_sentinel_master_ok_sentinels` 回到 3（至少 ≥ 2），告警 2 分钟内消失。
- `sentinel ckquorum gate` 输出 `OK 3 usable Sentinels. Quorum and failover authorization can be reached`；`sentinel get-master-addr-by-name gate` 指向的主库和 `redis_instance_info` 里的 `master` 一致。
- 主库在这期间出过事的，核对库存和订单：`KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n oppenheimer create job --from=cronjob/recon-hourly recon-manual-$(date +%H%M)`，差异为 0。

## 怎么人为触发它

设计 5 节第 2 层没有为这条列办法。待定，建议：把 `redis-gate-sentinel-1` 和 `-2` 缩掉（StatefulSet 缩到 1 个副本，`kubectl -n redis scale statefulset/redis-gate-sentinel --replicas=1`，再缩回 3），这时唯一的 Sentinel 看到 1 个，2 分钟后响；`RedisDown` 不会响（2026-11-23 起它不看 Sentinel，2026-11-22 这样做时它多响了两条）。`RedisSentinelDown`：缩掉任何一个，5 分钟后响（规则的单元测试覆盖了，没有在集群上验证）。这期间主库不要出事。也可以只做“把规则的比较改成 `<= 3`”的改阈值验证，并说明只验证了“路是通的”。故障注入动手前要先登记到 [phase-3-observability.md](../plan/phase-3-observability.md)；删 PVC、清数据这类破坏性操作要先问主人。

## 历史

（还没有真的响过）
