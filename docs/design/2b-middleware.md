# 2b 设计：中间件上集群

本文是阶段 2b 的实现规格：把 MySQL、Redis、Kafka 部署到 2a 建好的集群上，配好资源限额、排布、持久化和备份。业务服务上集群是 2c 的事，但 2c 要用的连接方式在这里定下来。

状态：主人已确认（2026-11-15），第 2 节的七项都按建议。其中第 1、3 项改了阶段计划原来的写法。

## 1. 动工前查到的事实（2026-11-15）

写之前读了本机 compose 的拓扑和相关的代码，查了两个 Operator 的现状。有四件事和阶段计划的写法对不上。

**1. 库存闸门的脚本不能在 Redis Cluster 上运行。** 计划写的是“Redis Cluster：3 主 3 从”。而 `pkg/stockgate` 的 Lua 脚本是这样访问键的：

- 键名在脚本里用前缀拼出来（`p .. 'gate:sku:' .. sku`），没有通过 `KEYS` 声明。Redis Cluster 要求脚本访问的键都在 `KEYS` 里，它靠这个判断请求该发给哪个节点。
- 一次下单受理（`holdScript`）要原子地改这些键：每个 SKU 的可售数、每个 SKU 的限购计数、这个订单的占用、全局的两个有序集合（`gate:holds`、`gate:claimed`）、幂等键。它们的名字各不相同，在 Cluster 里会落到不同的槽，而一个脚本只能访问同一个槽里的键。

要让它跑在 Cluster 上，只能给所有闸门的键加同一个 hash tag（`{gate}`），让它们全落在一个槽、也就是一个节点上，同时把调用方式改成经 `KEYS` 传键名（现在调用时 `KEYS` 是空的）。那样 Cluster 对闸门没有任何分摊作用，只多了一层间接。闸门的原子性来自“所有相关的键在同一个 Redis 上由一个脚本改”，这和分片是冲突的。

另外两处也受影响：gateway 的限流脚本一次改多个桶（按接口、按用户、按 IP，`#KEYS` 个），在 Cluster 里会跨槽；`pkg/redisx` 现在只会建单节点的客户端（`redis.NewClient`）。分布式锁（两个键）和查询缓存（一个键）问题不大。

**2. Strimzi 还不支持本机在用的 Kafka 版本。** Strimzi 最新版是 1.3.1，支持的 Kafka 是 4.2.0 到 4.3.2，测试过的 Kubernetes 是 1.32 到 1.37。本机 compose 用的是 Kafka 4.4.0。集群上只能用 4.3.2。

**3. Oracle 的 MySQL Operator 管的是 InnoDB Cluster。** 它的文档写的是“managing one or more MySQL InnoDB Clusters consisting of a group of MySQL Servers and MySQL Routers”，也就是组复制加 MySQL Router。本项目 1c 做的是 GTID 异步复制的一主一从，CDC 从从库读 binlog，扩容用 CLONE，这些都是按“普通的主从”设计和验证过的。它的最新版本号和支持的 MySQL 版本这次没有查到（发布页是空的，文档的介绍页不写版本）。

**4. 内存预算里漏了一个实例。** hardware.md 的预算列了“2 分片 × 一主一从”和“归档实例”，但本机还有一个共享的 `mysql` 实例，放着 inventory、payment 和商家索引的库。

集群的现状（2a 的结果）：server 可分配约 4.4 Gi、两个 agent 各约 9.8 Gi；空集群已经用了约 3.1 GiB；etcd 的 fsync 延迟 p99 在 8 到 16 毫秒，偏高。

## 2. 主人确认的七项（都按建议）

| # | 问题 | 建议 | 主要的另一种做法 |
|---|---|---|---|
| 1 | Redis 的拓扑（**改计划**） | 按用途分两套：闸门和锁用一主一从加 Sentinel；缓存和限流用 Redis Cluster 3 主 3 从（第 4 节） | 全部放进一个 Cluster，闸门的键加同一个 hash tag 挤在一个槽里 |
| 2 | MySQL 用 Operator 还是手写 | 手写 StatefulSet，做成一个 Helm chart，每个实例组装一次（第 3 节） | Oracle 的 MySQL Operator，拓扑变成每个分片 3 个实例的组复制加 Router |
| 3 | Kafka 的版本（**偏离版本规则**） | Strimzi 1.3.1 加 Kafka 4.3.2，本机 compose 仍是 4.4.0（第 5 节） | 等 Strimzi 支持 4.4；或者不用 Strimzi，手写 4.4.0 的 StatefulSet |
| 4 | 三个一组的中间件怎么排布 | Kafka 和 Redis Cluster 各有一个成员放在 server 节点上，三个节点各一个；MySQL 只放 agent（6.2） | 全部只放两个 agent，一个 agent 上有两个 broker |
| 5 | 密码怎么管 | 部署脚本在集群里生成随机密码存成 Secret，不存在时才生成；仓库里没有密码（6.4） | 用固定的开发密码写进 values |
| 6 | topic 由谁建 | 维持现状，服务启动时自己建（`kafkax`），不启用 Strimzi 的 Topic Operator（5.3） | 用 `KafkaTopic` 资源声明，服务不再建 |
| 7 | etcd 的恢复演练 | 做，放在中间件部署完、灌数据之前；它会重置控制面，是破坏性操作（7.2） | 只做快照和复制，不演练恢复 |

## 3. MySQL

决定和备选方案见 ADR-0027。

### 3.1 实例

和本机 compose 一一对应，只是从容器变成 Pod：

| 名字 | 内容 | 实例 | 排布 |
|---|---|---|---|
| `mysql-order-0` | `order_db_0`（slot 0、2、4、6） | 一主一从 | 主在 agent-1，从在 agent-2 |
| `mysql-order-1` | `order_db_1`（slot 1、3、5、7） | 一主一从 | 主在 agent-2，从在 agent-1 |
| `mysql-main` | inventory、payment、商家索引、订单冷库（`order_archive_db`） | 单实例 | 任一 agent |

两个分片的主库错开放：一个 agent 挂了，丢的是一个分片的主库和另一个分片的从库，而不是两个主库。`mysql-main` 没有从库，和本机一样，是已知的单点；给它加从库不在 2b 的范围。

版本用 9.7.4（和本机一致，LTS）。参数从 compose 搬过来（GTID、`binlog-row-image=FULL`、`binlog-row-metadata=FULL`、UTC、utf8mb4），内存相关的重新定：本机为了省内存把缓冲池压到 64 MB 还关了 `performance_schema`，集群上每个实例给 1 Gi（缓冲池 512 MB），`performance_schema` 打开，阶段 3 定位锁等待要用（1d 的 2.10 就是靠它找到的热点行）。

### 3.2 为什么不用 Operator

Operator 把“怎么搭复制、怎么故障切换、怎么备份”写成了控制器，生产上这是它的价值。这里不用，有三个理由：

- **拓扑不一样。** Oracle 的 Operator 管的是组复制，每个分片至少 3 个实例才有意义，还要加 Router。内存上是 6 个实例对 4 个；更要紧的是 1c 的读写分离（按 GTID 等从库追上）、CDC 的行版本号、扩容脚本都是对着异步主从验证的，换成组复制要重新验证一遍，而这些和 2b 的目标无关。
- **阶段 4 要练的正是 Operator 替你做掉的事。** 主库挂了，从库提升、其他服务改连接、CDC 换源、旧主回来怎么办，这是计划里的故障实验，也是 ADR-0014 里记着要验证的。自动切换会把这些盖住。
- **面试讲得清楚。** 手写一遍 StatefulSet 才讲得出稳定的网络标识、按序号启动、卷和 Pod 的绑定各自解决什么问题，也才讲得出 Operator 在它之上多做了什么。

代价是复制的搭建、切换、备份都要自己写脚本，出错的可能比用成熟的 Operator 高。Percona 的 Operator 支持异步复制，但它部署的是 Percona Server 而不是这里用的 MySQL 9.7，没有细查。

### 3.3 StatefulSet 怎么搭

一个 Helm chart `deploy/charts/mysql`，装三次（两个分片各一次，`mysql-main` 一次，后者副本数是 1）。

- 每个分片一个 StatefulSet，2 个副本。序号 0 是初始的主库，序号 1 是从库。`server-id` 由 chart 里给的基数加序号算出来（和本机的 11、12、21、22 对应）。
- 一个 headless Service 给每个 Pod 稳定的名字：`mysql-order-0-0.mysql-order-0.mysql.svc`。从库找主库、CDC 找从库都用这种名字。
- 另外两个普通的 Service：`mysql-order-0-primary` 和 `mysql-order-0-replica`，按 Pod 名选，主库是哪个序号写在 values 里（`primaryOrdinal`）。服务连的是这两个名字，所以故障切换时改的是这个值，不是服务的配置。**这个值是人工维护的**，这正是 Operator 会自动做的那一步。（最初的设计是给 Pod 打 `role` 标签，实现时发现不行，见第 10 节的发现 2。）
- 从库的初始化用 init 容器：数据目录为空时，用 CLONE 从主库拉全量，再按 GTID 自动定位建立复制。`super_read_only` 在复制建立之后才设（1c 的教训：写在启动参数里官方镜像的首次初始化会失败）。逻辑从本机的 `scripts/mysql-replication.sh` 搬过来。
- 存储用 local-path，每个 Pod 一个 PVC（`volumeClaimTemplates`）。卷建在哪个节点，这个 Pod 以后就只能在那个节点上；节点的磁盘没了，从库要重新 CLONE，主库就是数据丢失，只能提升从库。
- 探针：startup 探针给足时间（崩溃恢复可能要几分钟），readiness 对从库检查复制线程在跑。liveness 只检查进程能应答，不检查复制，否则复制断了会把从库反复重启。
- 关闭：`terminationGracePeriodSeconds` 给 120 秒，让 mysqld 正常刷盘退出。

### 3.4 CDC 和建库

CDC 伪装成从库从每个分片的从库读 binlog，源地址用从库 Pod 的稳定名字。库和账号（各服务的账号、`cdc` 账号）由一个 Helm 的安装后任务在主库上建，表仍然由各服务启动时的迁移建（现状）。

## 4. Redis

### 4.1 按用途分成两套

| | 闸门和锁（本机的 `redis`） | 缓存和限流（本机的 `redis-cache`） |
|---|---|---|
| 放什么 | 库存闸门、分布式锁、读己之写标记 | 查询缓存、gateway 限流的桶 |
| 丢了会怎样 | 闸门的占用丢了要靠回收和对账收拾，锁丢了可能两个持有者 | 缓存重新回源，桶重新装满 |
| 访问方式 | 多个键的原子脚本 | 一个键一次操作（限流除外，见 4.4） |
| 拓扑 | 一主一从加 3 个 Sentinel | Redis Cluster，3 主 3 从 |
| 配置 | `noeviction`，AOF | `allkeys-lru`，不持久化 |

这样分是顺着数据的性质走的：闸门要的是“所有键在一起、原子地改”，对它有意义的是主从切换；缓存的键彼此独立，正好能分片。两种高可用方式（Sentinel 和 Cluster）也都能练到，阶段 4 各有各的故障可做。

版本 8.10.3，和本机一致。

### 4.2 闸门这一套：主从加 Sentinel

- 一个 StatefulSet，2 个副本，主从分在两个 agent 上。3 个 Sentinel 是一个单独的 StatefulSet（带一个小的卷：它要把运行中学到的主库、纪元、其他 Sentinel 写回自己的配置文件），三个节点各一个：Sentinel 靠多数票判断主库是否下线，3 个里要 2 个同意，所以不能只放在两个 agent 上。
- 服务用 go-redis 的 Sentinel 客户端（`NewFailoverClient`）：向 Sentinel 问当前的主库是谁，切换后自己重连。
- **要想清楚的一件事：Redis 的复制是异步的。** 主库确认了一次受理、还没传给从库就挂了，Sentinel 提升从库之后，这次受理的占用就没了，而用户已经拿到了“受理成功”。现有的机制能不能兜住，2b 不验证，但要先把预期写下来：落库的消息是受理之后才发的，消息里带着订单的全部内容，落库时在 MySQL 里预占库存，MySQL 那一侧会发现超卖（ADR-0018）；闸门里的可售数会比实际多，靠 inventory 的 `reconcile` 纠正。这是阶段 4 的一个实验，实验前按这段推演写出预期。
- `min-replicas-to-write` 先不设。设成 1 可以缩小上面那个窗口（从库不在时主库拒绝写），代价是唯一的从库一挂闸门就不可写，按 ADR-0021 就是拒绝下单。等阶段 4 量过再定。

### 4.3 缓存这一套：Redis Cluster

- 一个 StatefulSet，6 个副本。集群的组建（分配槽、指定主从）由一个一次性的任务用 `redis-cli --cluster create` 完成，之后节点自己记着拓扑（`nodes.conf` 在卷上）。
- **Pod 重建后地址会变。** Redis Cluster 的节点之间用地址互相记，Pod 的地址变了，其他节点要靠 gossip 重新认识它。新版本支持按主机名通告（`cluster-announce-hostname` 加 `cluster-preferred-endpoint-type hostname`），客户端拿到的也是主机名。这一点要先在集群上验证：删掉一个 Pod，看它回来之后集群是否自己恢复、客户端是否不用重启。
- 3 个主库分在三个节点上，每个主库的从库不和它在同一个节点。
- 不持久化，每个主库 `maxmemory` 128 MB。节点重启后是空的，这对缓存和限流是可以接受的。

### 4.4 代码要改的地方

这些是 Go 代码的改动，放在 2b 是因为不改就没法验证；涉及库存和订单状态的部分由 Fable 写。

- `pkg/redisx`：`Config` 增加模式（单节点、Sentinel、Cluster），返回 `redis.UniversalClient`。本机和 CI 仍然用单节点。调用方的类型从 `*redis.Client` 改成接口，范围是 `stockgate`、`lock`、`cache`、`ratelimit`、`readmark` 和各服务的装配。
- `pkg/ratelimit` 的脚本一次改多个桶，在 Cluster 上会跨槽。有两种改法，**实现前要再细化并写进 ADR-0019 的补充**：
  - 每个桶单独调用一次脚本。失去的是“任何一个桶不够就一个都不扣”：前面的桶扣了、后面的桶拒绝时，前面的白扣一个令牌。可以先查最容易拒绝的桶来减少这种情况。
  - 限流不上 Cluster，和闸门共用主从那一套。这违背 1d 的 4.7（限流的流量不能压到闸门的 Redis 上）。

  建议第一种。
- `pkg/stockgate` 不改脚本，只换客户端的类型。它连的是主从那一套，脚本不声明 `KEYS` 在单个主库上没有问题。
- 测试：本机 compose 加一个可选的 profile 起一个最小的 Sentinel 和 Cluster，`redisx` 的三种模式各有集成测试；CI 里要不要常开，看它拖慢多少再定。

## 5. Kafka

### 5.1 Strimzi 和版本

Strimzi 1.3.1，Kafka 4.3.2，KRaft 模式。本机仍然是 4.4.0，两边差一个小版本；franz-go 客户端按协议协商，不受影响。这是对“选最新稳定版”的偏离，原因是 Operator 还不支持，按规则在 `versions.md` 的备注里写明。Strimzi 支持 4.4 之后再升，升级本身也是一次值得做的练习（滚动升级 broker）。

用 Operator 而不是手写，和 MySQL 的理由正相反：Kafka 的 broker 是对等的，没有“这个项目特有的拓扑”要保，Strimzi 做的滚动重启、配置变更、证书都是通用的；而且“Operator 模式”本身是阶段计划里的考点，三个中间件里至少有一个该用它。

### 5.2 拓扑

- 3 个节点，每个都同时是 controller 和 broker（一个 `KafkaNodePool`，两个角色）。分开部署要 6 个 Pod，内存不够。
- 三个节点各放一个（见 6.2）。
- 副本数 3，`min.insync.replicas` 2。生产者已经是 `acks=all`（`kafkax` 里的 `AllISRAcks`）。含义：挂一个 broker 照常读写；挂两个，剩下的那个拒绝写入，不会出现“写成功了但只有一份”的情况。本机是单 broker、副本数 1，这些行为到集群上才第一次出现。
- 每个 broker 堆 768 MB，容器上限 1.5 Gi。剩下的留给页缓存，Kafka 的读写主要靠它。
- 只开集群内部的监听。服务在集群里，不需要从外面连；要从 MacBook 看的话用端口转发。

### 5.3 topic

维持现状：服务启动时用 `kafkax` 的 `CreateTopics` 建自己用的 topic，分区数和配置在代码里。副本数要从 1 改成可配置（实现时先确认它现在从哪来）。不启用 Strimzi 的 Topic Operator：topic 有两个地方管，改分区数时就会不知道以谁为准，还省一个 Pod。

## 6. 公共的部分

### 6.1 资源和 QoS

有状态的中间件内存的 requests 等于 limits，另外用一个 PriorityClass（`middleware`）让它们在节点内存紧张时排在业务服务之后被驱逐；数据库被驱逐的代价比业务服务大得多。（这里原来写的是“Guaranteed”，不对：CPU 不设 limits 就是 Burstable，见第 10 节的发现 3。）CPU 只设 requests，不设 limits，避免被节流。

| 组件 | 实例 | 每个的内存 | 合计 |
|---|---|---|---|
| MySQL 订单分片 | 4 | 1 Gi | 4 Gi |
| MySQL `mysql-main` | 1 | 1.5 Gi | 1.5 Gi |
| Redis 闸门（主从） | 2 | 256 Mi | 0.5 Gi |
| Sentinel | 3 | 64 Mi | 0.2 Gi |
| Redis Cluster | 6 | 192 Mi | 1.2 Gi |
| Kafka | 3 | 1.5 Gi | 4.5 Gi |
| Strimzi 的 Operator | 1 | 约 400 Mi（估计，装好后实测） | 0.4 Gi |
| 合计 | | | 约 12.3 Gi |

hardware.md 的预算里这几项加起来是 11 GB，多出来的是 `mysql-main` 和 Sentinel。三个节点可分配合计约 24 Gi，装完中间件剩约 11.7 Gi，要放阶段 3 的监控（预算 5.5 GB）、业务服务（3 GB）、Chaos Mesh（0.5 GB）。够，但余量从 6 GB 降到约 2.7 GB。**这就是 ADR-0026 里 CNI 决定要用的数**：按这个预算，换 Cilium 多用的 1 到 1.5 GB 会把余量吃掉一半，装好实测后再下结论。

### 6.2 排布

约束有两条：同一组里的成员不在同一个节点（反亲和）；主从、多数派的成员分散。

问题在于只有两个 agent，而 Kafka、Redis Cluster 的主库、Sentinel 都是三个一组。全放在 agent 上，必然有一个 agent 上有两个：那个 agent 一挂，Kafka 就剩一个 broker、不可写，Sentinel 剩一个、选不出主。阶段 4 的“关掉一个节点，系统继续工作”就没法做。

所以建议三个一组的在三个节点上各放一个，server 节点上会有：一个 Kafka broker（1.5 Gi）、一个 Redis Cluster 的主库和一个别的主库的从库（各 192 Mi）、一个 Sentinel。合计约 2 Gi，server 可分配 4.4 Gi，现在用了约 1.5 GiB。

风险是 Kafka 的磁盘写入和 etcd 在同一块虚拟盘上，而 etcd 的 fsync 延迟本来就偏高。部署后盯着 `etcd_disk_wal_fsync_duration`，明显变差的话有两个退路：给 server 单独加一块盘放 Kafka 的数据；或者把三台虚拟机的规格调匀（重建集群只要 5 分半钟，现在没有数据）。MySQL 不放 server：它的刷盘比 Kafka 频繁得多。

PDB：每一组 `maxUnavailable: 1`，保证排空节点时不会同时带走一组里的两个。

### 6.3 命名空间和目录

命名空间 `mysql`、`redis`、`kafka`，一个系统一个。清单：

```
deploy/charts/mysql/          手写的 chart，装三次
deploy/charts/redis-ha/       主从加 Sentinel
deploy/charts/redis-cluster/  Cluster 和组建集群的任务
deploy/cluster/kafka/         Strimzi 的 values、Kafka 和 KafkaNodePool 资源
scripts/cluster/middleware.sh mysql | redis | kafka | all，和 addons.sh 同样的写法
```

`make cluster-up` 之后再运行 `make middleware-up`，两步分开：重建集群比重建中间件频繁。

### 6.4 密码

MySQL 的 root 密码、各服务的账号、复制账号、CDC 账号都存成 Secret。`middleware.sh` 在 Secret 不存在时用随机值生成，存在就不动。仓库里没有任何密码，values 里只引用 Secret 的名字。代价是重建集群后密码会变，这没有关系，服务从 Secret 读。Redis 在集群内部不设密码，和本机一致。

## 7. 备份和恢复

### 7.1 MySQL

- 每个实例一个 CronJob，每天一次（三个实例错开），从**从库**做 `mysqldump --single-transaction --source-data=2 --set-gtid-purged=ON`（`mysql-main` 没有从库，从它自己做），用 zstd 压缩后写到 NAS 的 `/volume1/k8s/backup/mysql/<实例>/<UTC 时间戳>/`，保留 7 份。实现和实测见第 10 节的发现 14。
- 逻辑备份的含义要讲得出来：`--single-transaction` 靠 InnoDB 的一致性快照，不锁表；它记下的 GTID 位置加上之后的 binlog 才能恢复到任意时刻，而 binlog 现在没有单独归档，所以**能恢复到的是备份的时刻，之后的数据会丢**。按时间点恢复不在 2b 做。
- 恢复演练：起一个临时的 MySQL，把一个分片最近的备份灌进去，用 1d 的对账工具（`recon`）和线上的从库比。不碰线上的实例，不是破坏性操作。演练在 2c 灌过数据之后才有意义，2b 先用少量手工写入的数据走通流程。

### 7.2 etcd

- 快照用 k3s 自带的定时任务（每 12 小时，本地留 5 份）。server 节点上加一个 systemd 定时器，把快照目录同步到 NAS 的 `backup/etcd`。NFS 只在同步时挂载，NAS 关机时不影响快照本身（2a 设计的 6.3）。
- 恢复演练（待确认项 7）：`k3s server --cluster-reset --cluster-reset-restore-path=<快照>`。它把控制面重置成快照时的状态，演练的做法是：拍快照，建一个做记号的资源，恢复，确认记号不见了而快照之前的东西都在。这是破坏性操作，放在中间件部署完、灌数据之前；最坏的情况是 `make cluster-up` 加 `make middleware-up` 重来。

## 8. 验证

2b 结束时业务服务还不在集群上，所以验证的是每个中间件自己：

| 对象 | 检查 |
|---|---|
| MySQL | 主库写入、从库读到，两边的 GTID 集合一致；删掉从库的 Pod，回来后自己接上复制；删掉从库的卷，重新 CLONE；两个分片的主库不在同一个节点 |
| Redis 主从 | 删掉主库的 Pod，Sentinel 完成切换，记下耗时；用 Sentinel 客户端的测试程序在切换前后持续写，记下失败了多久、丢了几条 |
| Redis Cluster | `redis-cli --cluster check` 通过；删掉一个主库的 Pod 后集群恢复正常（4.3 里要先验证的那一点；实际发生的不是切换，见第 10 节的发现 5） |
| Kafka | 建一个 3 副本的 topic，`acks=all` 压测一轮记下吞吐；删掉一个 broker 的 Pod，期间生产不中断，回来后没有落后的副本；停掉两个 broker 时写入被拒绝 |
| 代码 | `redisx` 三种模式的集成测试；`make lint test e2e` 在本机仍然通过（本机还是单节点） |
| 备份 | 7.1、7.2 的两次演练 |
| 整体 | 各节点的内存；etcd 的 fsync 分布和 2a 的对比；重启宿主机后全部中间件自己恢复 |

量完内存后回到 ADR-0026 改定 CNI，然后才是 2c。

## 9. 实现顺序和分工

| 步 | 内容 | 谁做 |
|---|---|---|
| 一 | ADR-0027；MySQL 的 chart、复制的搭建、Secret；MySQL 的验证 | chart 派 Opus（复制的初始化逻辑不简单），主会话评审并执行 |
| 二 | `redisx` 的三种模式、`ratelimit` 的改法（先细化设计） | Fable 写 `redisx` 和 `ratelimit`；调用方改类型派 Opus；测试派 Sonnet |
| 三 | Redis 的两个 chart 和验证 | chart 派 Sonnet，主会话执行 |
| 四 | Strimzi、Kafka、`kafkax` 的副本数；验证 | 清单派 Sonnet，主会话执行 |
| 五 | 量内存，改定 CNI（ADR-0026）；如果换 Cilium，在这里重建集群 | 主会话 |
| 六 | 备份的 CronJob、etcd 快照的同步、两次恢复演练 | 清单派 Sonnet，演练主会话做 |
| 七 | `middleware.sh`、Makefile、重启宿主机的检查、文档和笔记 | 主会话执行，笔记派 Sonnet |

第一步和第二步互不相干（一个是清单，一个是 Go 代码），可以并行；第三、四步的清单也可以并行写。集群上的执行都由主会话串行做，每装一个先看它实际创建了什么、占了多少内存。

## 10. 实现中的发现（2026-11-15，进行中）

**1. 节点上的 AppArmor 拦掉了容器里进程之间的全部信号。MySQL 因此起不来，这一项挡住了第一步，修法等主人决定。**

现象：官方的 `mysql:9.7.4` 镜像在这些节点上起不来，不管是不是经过 chart。以 root 运行时入口脚本卡在 `gosu`（它占着 CPU 空转）；以 mysql 用户运行时，首次初始化做完之后 `mysqld --initialize` 一直不退出。启动探针超时把容器杀掉后留下半初始化的数据目录，之后每次启动都报“数据文件损坏”，循环崩溃。

定位：直接起一个不带任何自定义配置的官方镜像，同样卡住，排除了 chart；给 Pod 加 `appArmorProfile: Unconfined` 后两种方式都正常，确定是 AppArmor；节点的内核日志里有对应的拒绝记录：

```
apparmor="DENIED" operation="signal" profile="cri-containerd.apparmor.d" comm="mysqld" signal=term peer="cri-containerd.apparmor.d//&unconfined"
apparmor="DENIED" operation="signal" profile="cri-containerd.apparmor.d" comm="gosu" signal=rtmin
```

原因：containerd 给容器套的默认配置（`cri-containerd.apparmor.d`）只允许向“标签是 `cri-containerd.apparmor.d`”的进程发信号。而这些节点上容器进程的实际标签是 `cri-containerd.apparmor.d//&unconfined`，是两个标签叠在一起的，和规则对不上，于是容器里的进程连给自己发信号都被拒绝。`mysqld` 初始化完靠给自己发 SIGTERM 来退出；`gosu` 是 Go 程序，运行时靠信号同步各线程。

**影响不止 MySQL。** 三个节点的内核日志里，CoreDNS、MetalLB、Envoy Gateway 的控制器、仓库、metrics-server 都有 `signal=urg` 被拒绝的记录：那是 Go 运行时用来抢占 goroutine 的信号。这些程序照常工作，但抢占退化成了协作式的，长时间不让出的 goroutine 会拖住垃圾回收，阶段 3 量延迟的长尾时会受影响。容器里用 `timeout` 的探针也杀不掉超时的子进程。

标签为什么会叠上 `unconfined`：Ubuntu 26.04 默认开着 `kernel.apparmor_restrict_unprivileged_unconfined=1`。k3s 自带的 runc 是不受限的进程，它在放弃权限之后才给容器切换配置，被这个开关当成“无特权的不受限进程换配置”，只能叠加不能替换。

验证（主人在 k3s-agent-2 上手工把开关设成 0）：之后新建的官方 MySQL 容器以 root、默认配置运行，13 秒完成初始化；`mysqld` 的标签是 `cri-containerd.apparmor.d`，不带叠加；内核日志里没有新的拒绝记录。

修法（主人确认）：把这个开关写进 `vm.sh` 的 cloud-init（节点上的 `/etc/sysctl.d/90-oppenheimer-apparmor.conf`），然后删掉三台虚拟机重建。重建后三个节点的开关都是 0，全部中间件部署完之后三个节点自开机以来信号被拒绝的记录都是 0 条。容器仍然受 `cri-containerd.apparmor.d` 约束，放宽的只是这一项系统级的限制。没有选的两种做法：在节点上维护一份改过的 AppArmor 配置；给每个 Pod 单独关掉 AppArmor。

顺带改的：MySQL 的 Pod 改成以镜像里的 mysql 用户（999）运行，不用 root，入口脚本因此不经过 `gosu`。改完之后从库的初始化脚本里对卷的根目录做 `chown` 会失败（根目录属于 root，靠 fsGroup 可写），已改成失败不算错。

教训：这个问题在 2a 就存在（Go 程序的抢占信号一直在被拒绝），但当时所有东西“看起来都正常”。2a 的检查只看了功能，没有看节点的内核日志；装完一个新环境，应当把内核日志里的拒绝和报错过一遍。

**2. 主从的角色不能记在 Pod 的标签上。** 原设计（3.3）用 `role` 标签让 Service 选主库。写 chart 的子 Agent 指出：标签在 Pod 对象上，Pod 一重建就没了，主库的 Service 随即没有端点；切换之后旧主的 Pod 重建，按序号重新打标签会把它标回主库。改成：Service 按 Pod 名选，主库是哪个序号写在 values 里（`primaryOrdinal`）。切换的最后一步是改 values 再 `helm upgrade`，只动 Service，不重启 Pod；主从关系在 git 里。部署脚本只读地核对“values 说的主库”和实例的实际状态是否一致，不一致就报异常并以非零退出，不自动改。3.3 里关于标签的描述以这里为准。

留下的两个要进切换手册的顺序问题：已经建立的连接不跟着 Service 走，所以要先把旧主设成只读再改 values；切换后旧主回来时是可写的，从库的 Service 会把它当从库，读到旧数据，要先重建它的复制。

**3. 6.1 里说的 QoS 等级不对。** CPU 不设 limits 的话是 Burstable，不是 Guaranteed（Guaranteed 要求 CPU 和内存的 requests 都等于 limits）。决定是接受 Burstable（不设 CPU 上限，避免节流），另外建一个 PriorityClass `middleware` 给有状态的中间件：节点内存紧张时排在业务服务之后被驱逐。MySQL 和 Kafka 已经用上，两个 Redis 的 chart 还没有加。

**4. Sentinel：旧主回来得比切换还快。** 删掉主库的 Pod，新的 Pod 两三秒就起来了，这时 Sentinel 还没判定它下线，启动脚本问到的主库仍然是它自己，于是它以主库的身份启动；同时 Sentinel 按旧的地址判定它下线并在 6.9 秒时完成了切换。结果有十几秒两个实例都自认为是主库，之后 Sentinel 把旧主改成了从库。期间没有写入到达旧主：Pod 重建后地址变了，客户端的旧连接都断了，重连时问 Sentinel 拿到的是新主。切换前写入并确认复制过的键没有丢。

**5. Redis Cluster 的主库重启得快，数据就没了。** 删掉一个主库的 Pod，它在节点超时（5 秒）之内就回来了，集群没有切换，它带着空的数据继续当主库，它的从库随即和它同步成空的。对缓存和限流可以接受（4.3 的前提），但这正是“不持久化的主库不能自动重启”这条经验说的事。6 个 Pod 同时删掉、地址全变之后，集群 8 秒内自己恢复（靠启动脚本按域名改写 `nodes.conf` 里的地址）。主从的配对这次恰好都不在同一个节点。

**6. Kafka。** `acks=all`、6 分区 3 副本、512 字节的消息：约 4.5 万条/秒（22 MB/s）。持续生产时删掉一个 broker：没有报错，最长的一条延迟 16 秒；恢复后没有落后的副本。“停掉两个 broker 时写入被拒绝”没有测：Strimzi 会立刻把删掉的 Pod 建回来，要用别的办法（阶段 4 的 Chaos Mesh）。

**7. 限流在 Cluster 下的测试。** `redisx` 的三种模式和 `ratelimit` 在 Cluster 下的行为有了集成测试，靠本机 compose 的一个可选 profile（`make redis-modes-up`）。钉住的行为：Sentinel 主动切换后同一个客户端继续可写；Cluster 上访问两个不同槽的脚本报 `CROSSSLOT`（这就是闸门不能上 Cluster 的原因）；限流被后面的桶拒绝时前面的桶白扣一个令牌。这些测试在环境没起来时跳过，CI 里现在不跑。

**8. MySQL 的验证（修好 AppArmor 并重建之后）。** 三个实例一次部署成功；两个分片的主库分别在 agent-1 和 agent-2 上，从库各在另一个节点；主从的 GTID 集合一致。

| 检查 | 结果 |
|---|---|
| 主库写、从库读 | 读到；直接往从库写被 `super_read_only` 拒绝 |
| 删掉从库的 Pod（卷还在） | 11 秒后就绪，init 容器识别出数据目录已初始化、什么都不做，停机期间主库上的写入已经追上，`super_read_only` 仍然是开的 |
| 删掉从库的卷 | 26 秒后就绪：init 容器从主库 CLONE（3 秒）、配好复制，克隆期间主库上的写入也在，GTID 和主库一致 |

每个实例空载时占约 480 Mi（上限 1 Gi，`mysql-main` 1.5 Gi），子 Agent 担心的“1 Gi 很紧”要到 2c 有了连接和数据之后再看。

**9. 建 topic 的副本数。** `kafkax.EnsureTopics` 原来在没指定副本数时写死成 1。改成交给 broker 的默认值（`default.replication.factor`）：本机的单个 broker 是 1，集群上是 3。副本数是部署环境的属性，服务的代码不用知道。

**10. Redis Cluster 的主从配对在重建之后有一对落在了同一个节点。** 给两个 Redis 加 `priorityClassName` 时顺手核对，发现 `redis-cache-0` 和它的从库 `redis-cache-4` 都在 k3s-agent-1 上：那个节点一挂，这三分之一的槽没有副本可以顶上。原因是配对由 `redis-cli --cluster create` 决定，它按地址避开“同一台主机”，而 Pod 的地址各不相同，它不知道哪两个 Pod 在同一个节点；Pod 落在哪个节点由调度器定。发现 5 里说的“恰好都不在同一个节点”是重建之前那一次的运气，`middleware.sh` 当时只把配对打印出来，没有人去对照节点。

改法：`scripts/cluster/redis-cache-pairing.sh`（`check`、`fix`），`middleware.sh redis` 结尾调用 `fix`。它取每个 Pod 所在的节点和 `cluster nodes` 的主从关系，有同节点的主从时找一个改动最少的配对，用 `CLUSTER REPLICATE` 把从库改挂过去（只动从库，不动槽；从库重新全量同步，对缓存没有影响）。这次改了两个从库，之后 `--cluster check` 通过。**没有自动处理的**：两个主库落在同一个节点（要做主从切换），脚本只报异常并以非零退出；这种情况下那个节点一挂就少两个主库，剩下一个凑不出多数票，整个集群不能自动切换。

教训和发现 2 是同一类：一条性质（“主从不同节点”）如果是靠碰运气成立的，就要有东西去核对它，只打印不核对等于没有。

**11. etcd 的快照同步到 NAS（7.2 的前一半）。** server 节点上一个 systemd 定时器（每天 00:20、12:20，在 k3s 自己的快照之后 20 分钟）运行 `/usr/local/sbin/etcd-snapshot-sync`：临时挂载 NAS 的 `backup/etcd`，把本地快照目录里的文件复制过去（先写临时名字，读回来比对 SHA-256 再改名），NAS 上保留 14 份。模板在 `deploy/cluster/k3s/etcd-snapshot-sync.*`，由 `scripts/cluster/k3s.sh etcd-backup install` 安装，已加进 `make cluster-up`；`make etcd-backup` 立刻拍一份并同步。三个决定：

- **放在节点上而不是做成集群里的 CronJob**：它备份的是控制面，控制面坏了的时候它也得能跑。
- **NFS 用 soft 挂载加短超时**，和别处的 hard 相反：NAS 不在时要的是这一次失败、下一次再试，不是永远挂住。soft 的风险（写到一半返回错误）由“临时名字加校验再改名”兜住。
- **token 和快照一起备份**（`/var/lib/rancher/k3s/server/token`）。k3s 把引导数据（各个 CA 的私钥等）用 token 加密后存在 etcd 里，换一台机器恢复时没有同一个 token，快照读得出来、集群起不来。快照里本来就有全部的 Secret（没有开静态加密），所以把 token 放在旁边没有扩大暴露面；NAS 上的目录属于 root、权限 700。

实测：一份快照 20 MB，拍加复制约 2 秒。本地一份快照都没有时脚本以失败退出（定时器在跑而 NAS 上永远是空的，要显出来）。

**12. 入口的 Envoy 改成 2 个副本。** `EnvoyProxy` 里 `replicas: 2`、按节点的分布约束、`envoyPDB`（`maxUnavailable: 1`）。两个副本现在在 k3s-server 和 k3s-agent-1 上，通路检查通过。要讲得清楚的一点：这不是负载分摊，MetalLB 二层模式加 `externalTrafficPolicy: Local`，流量同一时刻只进一个节点上的那一个 Envoy，另一个是备用的；节点挂了之后地址被另一个节点接管，接管要多久留到阶段 4 量。

**13. e2e 偶发失败的修法。** `TestCancelReturnsStockOnce` 在 CI 上失败过一次：轮询订单时收到 503 `STOCK_GATE_UNAVAILABLE`（订单还没落库，展示读又没占到读闸门占用的名额）。这个 503 是服务有意的答复（说不清状态时不猜 404），带 `Retry-After`，所以该改的是客户端：e2e 的 `getOrder` 对带 `Retry-After` 的 503 重试，最多 10 秒；别的非 200 仍然立刻失败。没有调大名额：那只是让测试碰不到这条路径。

**14. MySQL 的备份和恢复演练（7.1）。** chart 里加了 CronJob（`backup.enabled`）和脚本 `files/backup.sh`，子 Agent（Sonnet）写、主会话读过 diff 后在集群上执行。要点：

- **专门的 `backup` 账号，不用 root。** 权限是在 9.7.4 上逐个去掉试出来的：全局 `FLUSH_TABLES`（`--single-transaction` 对齐位点时要 `FLUSH TABLES WITH READ LOCK`，比 `RELOAD` 窄）、`REPLICATION CLIENT`（读位点和复制状态）、`SHOW_ROUTINE`；每个业务库 `SELECT, SHOW VIEW, TRIGGER, EVENT`。**缺 `SHOW_ROUTINE` 或 `TRIGGER` 时 dump 照常成功，只是存储过程、触发器悄悄不在里面**，所以演练脚本会比对这几类对象的个数。Secret 里多了 `backup-password`，`middleware.sh` 现在会给已存在的 Secret 补缺的键（服务端 apply，不留 last-applied 注解）。
- **位点用 `--source-data=2`，不用 `--dump-replica`。** 后者会先停从库的 SQL 线程（备份期间从库越落越后，CDC 跟着停），还要 `REPLICATION_SLAVE_ADMIN`。复制是 GTID 自动定位，续接靠 dump 开头的 `GTID_PURGED`，文件加位点只是给人看的记录。
- **只导业务库**，不导系统库（账号由 accounts hook 重建）。9.x 的 mysqldump 默认还会去读数据脱敏策略的系统表，备份账号没有权限，加了 `--skip-masking-policies`。
- **半截的备份不会被当成备份**：写到 `.tmp-<时间戳>`，整份解压一遍确认最后一行是 `-- Dump completed`、取出 GTID、算好 SHA-256 之后才改名。新的一份成功之后才删旧的，只删符合时间戳命名的目录。
- **从库上备份前先确认它确实是从库、复制在跑**：切换之后 `-replica` 可能指向别的东西，复制断了的从库备出来的是旧数据却不报错。
- 存储：一个 NFS 的 PV 加 PVC（`deploy/cluster/mysql/backup-pv.yaml`，RWX），三个 CronJob 共用，各写各的子目录。没有用 `subPath`：它的目录由 kubelet 以 root 建，以 999 运行的 Pod 写不进去。代价是三个 Job 互相看得到对方的目录，靠脚本约束而不是权限。
- **代价**：`backup.sh` 放在 `files/` 里，进了和 MySQL 共用的 ConfigMap，所以这次部署让三个实例（含主库）滚动重启了一次；以后改备份脚本也会。要避免得给备份单独一个 ConfigMap，现在没有做。

用法：`make middleware-backup`（立刻跑一次，可带 `RELEASE=`）、`make mysql-restore-drill RELEASE=mysql-order-0`。

实测（每个实例写了 1000 行记号数据）：三个实例手工备份一共 10 秒，每份约 4.5 KB。恢复演练（`scripts/cluster/mysql-restore-drill.sh`）：起一个临时的 MySQL Pod（数据在 emptyDir，只读挂备份），校验 SHA-256，灌入最新的一份，核对 `gtid_executed` 和备份记录一致，再逐表和线上从库比行数和 `CHECKSUM TABLE`。三个实例都通过。演练中间碰到一次“不一致”：当时切换演练的写入探针还在往线上写，恢复出来 1295 行、线上 1328 行，探针结束后重新备份再比就一致了。这就是 7.1 说的“能恢复到的是备份的时刻”，也说明这个比对只能在没有写入时用；有流量时要按备份的 GTID 用对账工具比，是 2c 的事。数据量是假的，恢复耗时（不到 1 秒）没有参考价值，2c 灌过数据之后要重量一次。

实现时的一个坑：手工触发的命令第一版一直等到超时，而备份 3 秒就做完了。Kubernetes 1.31 起 Job 成功时有**两个**为真的条件（`SuccessCriteriaMet`、`Complete`），脚本把所有为真的条件拼起来和 `Complete` 比，永远不相等。子 Agent 在本机没有集群可测，这一段是到集群上才跑出来的。

**15. 计划内的主从切换演练。** 按新写的手册 [infra/mysql-switchover.md](../infra/mysql-switchover.md) 在 `mysql-order-1` 上切了两趟（0 到 1，再切回 0），期间一个探针每 0.5 秒经 `-primary` Service 新建连接写一行：

| | 写入失败的时间 | 失败的次数 |
|---|---|---|
| 第一趟（主库 0 → 1） | 09:40:26.1 到 09:40:30.8，约 5.2 秒 | 10 |
| 第二趟（主库 1 → 0） | 09:41:08.4 到 09:41:13.1，约 5.2 秒 | 10 |

两趟一共 490 次写入得到了成功的答复，结束后主从都是 1490 行（1000 行记号加 490），没有丢也没有多。旧主库设成只读之后就绪探针让 `-primary` 没有端点，新的连接直接失败，没有一次写到错的实例上。这 5 秒是把各步骤连成脚本执行的结果，其中大部分花在 `helm upgrade` 上（`middleware.sh mysql` 按顺序处理三个实例，`mysql-order-1` 排第二）；人照着手册一步步敲会更久。`helm upgrade` 没有重启任何 Pod。演练完主库回到序号 0，values 没有留下改动。

没有验证的：服务的连接池碰到 1290 和断连之后自己恢复（手册第 7 步，2c）；CDC 换源（2c）；主库故障时的切换（手册第二节，阶段 4）。

**16. Redis Cluster 的 PDB 一直是坏的，允许的中断数是 0。** 核对各组件的 PDB 时发现的。组建集群的 hook Job 的 Pod 带着和 Redis 一样的标签，做完之后还留着（有意保留，方便看日志），被 PDB 的选择器选中。PDB 用 `maxUnavailable` 时要知道“应该有几个 Pod”，办法是找到每个 Pod 的控制器、读它的 scale 子资源；Job 没有这个子资源，于是 PDB 报 `SyncFailed`（`jobs.batch does not implement the scale subresource`），允许的中断数停在 0。后果是 `kubectl drain` 任何一个有 `redis-cache` 的节点都会卡住，而三个节点上都有。从部署那一刻起就是这样，之前的验证只看了“PDB 建出来了”。改法：Job 的 Pod 换一套自己的标签。改完之后七个 PDB 允许的中断数都是 1。

教训：PDB、Service、NetworkPolicy 都按标签选 Pod，一次性的 Job 和 hook 的 Pod 不能沿用被它们选中的那套标签。检查 PDB 要看 `ALLOWED DISRUPTIONS` 和它的状态，不是看它存不存在。

### 量到的数（全部中间件在运行，空载）

| 节点 | 内存（`free` 的 used） | 2a 结束时 | Pod 的内存 requests 合计（占可分配的比例） |
|---|---|---|---|
| k3s-server | 2194 MiB | 1483 MiB | 2188 Mi（48%） |
| k3s-agent-1 | 2692 MiB | 771 MiB | 4672 Mi（46%） |
| k3s-agent-2 | 3018 MiB | 828 MiB | 6240 Mi（62%） |

按命名空间（`kubectl top`）：MySQL 2432 Mi（5 个实例），Kafka 2067 Mi（3 个 broker 加 Operator，刚重建、没有压测过；压测之后到过 3.3 Gi，多出来的是页缓存），Redis 52 Mi。实际用了约 7.9 GiB，但调度看的是 requests：三个节点已经承诺出去 13.1 Gi，还剩约 10.9 Gi 可以承诺。

etcd 的 fsync（重建之后）：52% 在 2 毫秒以内，71% 在 8 毫秒以内，99.4% 在 16 毫秒以内。和 2a 相比没有变差，server 节点上多了一个 broker 没有带来可见的影响；p99 仍然高于 10 毫秒的参考线。

**CNI 的决定：保留 flannel**（ADR-0026 已改定）。还能承诺的约 10.9 Gi 要放阶段 3 的监控（预算 5.5 GB）、业务服务（3 GB）、Chaos Mesh（0.5 GB），剩约 1.9 Gi。Cilium 估计要多用 1 到 1.5 GB，装上之后基本没有余量，而且 agent-2 已经承诺了 62%。

### 还没做的

- **etcd 的恢复演练**（7.2 的后一半）。快照的同步已经有了（发现 11）；演练会重置控制面，等主人点头。
- 重启宿主机后全部中间件自己恢复的检查（重启前要和主人说一声）。
- “停掉两个 broker 时写入被拒绝”（阶段 4 用 Chaos Mesh）。
- 主库故障时的切换没有演练（手册第二节，阶段 4）。
- 备份脚本和 MySQL 共用一个 ConfigMap，改它会滚动重启 MySQL（发现 14）。
- 备份失败没有告警：失败的 Job 留在记录里，要有人去看。阶段 3 接监控时加（`kube_job_status_failed`，以及“最近一份备份有多旧”）。
- 笔记要补备份、切换和发现 10 到 15。

## 11. 不在 2b 做的

- 业务服务上集群、边车进 Pod、用集群上的中间件跑端到端测试（2c）。
- MySQL 的自动故障切换；`mysql-main` 的从库；按时间点恢复（binlog 归档）。
- 订单库从 2 个扩到 4 个：集群上从 2 个开始，扩容演练本机做过，集群上要不要再做一次等 2c 之后看。
- Redis 主从切换时闸门丢数据的实验、`min-replicas-to-write`（阶段 4）。
- Kafka 的机架感知、跨节点的副本迁移、静态成员（2c 评估）。
- 中间件的指标导出（mysqld_exporter、redis_exporter、Kafka 的 JMX），阶段 3 接监控时一起做。
- 网络策略（2d）。
