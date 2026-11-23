# 节点磁盘剩余空间低

覆盖的告警：`ServerDiskCritical`（critical）、`ServerDiskLow`（warning）、`AgentDiskLow`（warning）

## 这条告警在说什么

三条都看节点根分区（`mountpoint="/"`、`fstype="ext4"`）的 `node_filesystem_avail_bytes`，用 `kube_node_info` 把 `instance` 对应到节点名（不写死 IP）：

- `ServerDiskLow`：k3s-server 的剩余低于 15 GiB，持续 10 分钟。
- `ServerDiskCritical`：k3s-server 的剩余低于 8 GiB，持续 5 分钟。critical，发 Telegram；它在响时 Alertmanager 抑制同一节点的 `ServerDiskLow`。
- `AgentDiskLow`：k3s-agent-1 或 k3s-agent-2 的剩余低于 30 GiB，持续 10 分钟。

这是在 kube-prometheus-stack 自带的两条“按趋势预计多久写满”的规则之外补的绝对值：`local-path` 不限制卷的大小，PVC 上写的 10 Gi、20 Gi 只是标签，PVC 的用量指标没有意义，所以只能看节点根分区。阈值的来历：server 的系统盘只有 60 GB（现在剩 22 到 24 GiB），所以 15 和 8；agent 的盘大（150 GB），30 GiB 是按 MySQL、Kafka 的数据都在这块盘上定的。出处：[3a 笔记第 12 节](../notes/3a-observability.md)、[round-9 的第 3 节](../notes/3b-rounds/round-9-soak.md)、[设计 3c 的 4.5](../design/3c-slo-alerting.md)。

## 影响

**server**：etcd、Prometheus 的数据、Tempo、Loki 和镜像都在这块盘上。写满了 etcd 会停，整个集群的控制面不可用，监控也跟着没了（告警本身发不出来）。业务 Pod 已经在跑的会继续跑一阵，但不能调度、不能改任何东西（推断，没有遇到过）。`ServerDiskCritical` 是现在就要清理。

**agent**：MySQL 的数据和 binlog、Kafka 的数据都在 agent 上。写满了数据库或 broker 起不来、写不进去，订单落库停住，下单会被背压拒绝（503）。已经落盘的数据不会丢，但要在写满之前处理。

时限：warning 当天处理；按下面第 1 步算出的“多久写满”决定。压测期间盘掉得快（round-9 里 agent-2 每小时 4.4 GiB，server 每小时 1.5 GiB），要按小时算。

## 先看什么

1. 哪个节点、剩多少、掉得多快：

   ```sh
   scripts/cluster/monitoring.sh promql 'node_filesystem_avail_bytes{mountpoint="/",fstype="ext4"} / 1024^3'
   scripts/cluster/monitoring.sh promql 'predict_linear(node_filesystem_avail_bytes{mountpoint="/",fstype="ext4"}[6h], 6*3600) / 1024^3'
   ```

   第二条是按过去 6 小时的趋势预测 6 小时后还剩多少，负数就是写满。

2. 到节点上看谁占的（节点 IP：server 192.168.50.40，agent-1 .41，agent-2 .42，用户 `ubuntu`）。`local-path` 的卷在 k3s 的 storage 目录下（k3s 的默认位置，没有核对过这套集群里改没改）：

   ```sh
   ssh ubuntu@192.168.50.40 'sudo du -xh --max-depth=1 /var/lib/rancher/k3s/storage /var/lib/rancher/k3s/agent/containerd /var/log 2>/dev/null | sort -h | tail -12'
   ```

3. server 上的各块：Prometheus、Tempo、Loki 各占多少，保留期是多少（Prometheus 15 天加 15 GB 上限，Tempo 3 天，Loki 7 天）：

   ```sh
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n monitoring get pvc
   ```

   注意 PVC 的 `local-path` 容量不是实际用量。

4. agent 上的各块：MySQL 的 binlog（`mysql-main` 和四个订单库实例）和 Kafka 的段：

   ```sh
   scripts/cluster/middleware.sh mysql
   KUBECONFIG=~/.kube/oppenheimer.yaml kubectl -n kafka get pods -o wide
   ```

5. 有没有人在压测（盘是被压测写满的，不是被保留期）：看 Mac mini 上有没有 k6，和 `scripts/cluster/monitoring.sh alerts`。

## 常见的原因和各自的处理

1. **压测和长稳测试。** 记录：round-9 的 3.1：盘没有稳态，agent-2 每小时掉 4.4 GiB，agent-1 2.8，k3s-server 1.5（采样 10% 时）。server 剩 22 GiB 时，长稳测试约十个小时就会碰到 `ServerDiskLow`（15 GiB 约 5 小时、8 GiB 约 9 小时，按每小时 1.5 GiB 算）。处理：停压后看保留期能不能把它收回来；长稳测试前先看 server 的剩余。
2. **binlog 和 Kafka 没配保留期。** 记录：round-9 的 3.2：`mysql-main` 的 binlog 每小时 1.4 GiB、保留 30 天；Kafka 每个 broker 每小时 0.78 GiB、保留 7 天。2026-11-22 已改：binlog 1 天（`binlog-expire-logs-seconds=86400`，`deploy/charts/mysql/values.yaml`），Kafka 24 小时加每分区 4 GiB（`deploy/cluster/kafka/kafka.yaml`），死信 topic 7 天（[round-9 的 3.4](../notes/3b-rounds/round-9-soak.md)）。**改完之后没有再跑一次长稳去验证盘真的走平**，93 GiB 是估算。`cdc.order.orders` 是压缩保留，不受保留期管，每个 broker 上占 5.5 GiB。
3. **入口访问日志、链路、日志。** 记录：[3a 笔记第 12 节](../notes/3a-observability.md)：入口访问日志占 Loki 的 60%，已改成只留出错的；每个请求大约落 1.2 个 span 和 1.4 KB 日志，2000 请求/秒压一小时：Tempo 约 1.5 GB，Loki 约 1.4 GB；Tempo 的保留期 3 天、Loki 7 天，没有容量上限，**3b 不要关尾部采样压测**。**尾部采样在过载的压测里不起作用**（2026-11-23）：被拒绝的请求算“出错”、变慢的落库算“慢”，都全留，35 分钟写掉约 8 GB，比这里估的快一个量级；打过拐点的压测之前先看 server 的剩余，至少留出 10 GB。2026-11-23 起“出错”和“慢”两类各有每秒的上限（`deploy/cluster/monitoring/otel-gateway.yaml`，ADR-0035），没有在过载下验证过。
4. **etcd 和盘抢。** 记录：3a 12：etcd 的 fsync p99 一直高于 10 毫秒的参考线，没有查原因。盘快满时更糟。
5. **镜像和旧的 Job 留下的东西。** server 上镜像约 6.3 GiB，etcd 约 0.5 GB。清理未使用的镜像和压缩日志是推断的办法，没有在这套集群上做过。
6. **备份写 NFS 的页缓存。** 不是磁盘问题，是内存（见 [container-memory.md](container-memory.md)）。

处理的原则：不要先删 PVC 或数据目录。可以先做的：缩短 Tempo、Loki 的保留期（改 `deploy/cluster/monitoring/` 下对应清单再 `monitoring.sh` 下发）、清节点上的旧日志。**删 PVC、清数据目录是破坏性操作，先问主人。**

## 怎么确认恢复了

剩余空间回到阈值以上之后告警恢复（`for` 之后）。确认：

```sh
scripts/cluster/monitoring.sh promql 'node_filesystem_avail_bytes{mountpoint="/",fstype="ext4"} / 1024^3'
```

server 大于 15，agent 大于 30。再用 `predict_linear` 确认它不是在以每小时几 GiB 的速度继续掉。

## 怎么人为触发它

设计 3c 第 5 节第 2 层：磁盘这类不方便真做的，临时把规则的阈值改到当前值以下（比如把 `15 * 1024^3` 改成 `30 * 1024^3`，server 现在剩 22 到 24 GiB），响了再改回来。**这只验证了“路是通的”（规则求值、Alertmanager 路由、Telegram），不是真的验证了磁盘满的场景。**

不要真的往 server 的盘里填东西：会影响 etcd。agent 上用 `fallocate` 占空间是可行的办法，但没有验证过，待定。

故障注入动手前要先登记到 `docs/plan/phase-3-observability.md`；重置节点、清数据这类破坏性操作要先问主人。

## 历史

- 2026-11-23 03:13 UTC，`ServerDiskLow` 第一次真的响（压测，设计 [3c](../design/3c-slo-alerting.md) 第 8 节“第 14 项”）：普通下单阶梯 35 分钟里 k3s-server 少了约 8 GB，从约 22 GiB 掉到 14.1 GiB。写的人是 Tempo（收了 7.5 GB 链路，卷 13 G）和 Loki（2.6 GB），Kafka 在这个节点上只涨 1.3 GB。原因（Collector 的计数，设计第 8 节“第 14 项的追查”第 4 条）：324 万条链路留下了 84.5 万条，其中被拒绝的请求（429）被算成“出错”全留了 21.6 万条，落库的 handler 过载时平均 500 毫秒、被算成“慢”全留了 35.8 万条，后一类正是 span 最多的。Tempo 的保留期 3 天，这批数据到 11 月 26 日才过期。第 1 到 3 步的命令在现场都能跑（`ssh ubuntu@192.168.50.40` 的那条也能跑，卷的位置就是 k3s 的默认位置）。
