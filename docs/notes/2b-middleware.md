# 2b 笔记：把 MySQL、Redis、Kafka 部署到 k3s 上

这份笔记整理 2b 里值得在面试中讲的点，按主题组织，不按开发步骤。每条按“面试官会怎么问 → 要点 → 本项目里怎么做、量到什么数、踩过什么坑”来写。设计全文见 [design/2b-middleware.md](../design/2b-middleware.md)（下文简称“设计”，引用章节号），取舍见 [ADR-0027](../adr/0027-mysql-statefulset-not-operator.md)、[0019](../adr/0019-gateway-rate-limiting.md)（末尾的补充）、[0026](../adr/0026-k3s-datastore-and-bundled-components.md)（末尾的“CNI 改定”）。集群和 2a 的背景见 [2a 笔记](2a-cluster.md)。

2b 做的事：把三类中间件放到 2a 建好的集群上：MySQL（手写 StatefulSet）、Redis（两套：主从加 Sentinel、Redis Cluster）、Kafka（Strimzi Operator）；配好资源、排布、持久化；`pkg/redisx` 和 `pkg/ratelimit` 改成能连三种 Redis。**2b 还没有全部做完**：备份、恢复演练、切换手册、重启宿主机的检查都没做，见第 11 节。笔记只写已经做了的。

**关于“通用知识”**：文中标注“（通用知识）”的段落，是为了把原理讲清楚而补的 Redis、Kafka、Kubernetes、Linux 常识，设计文档和 ADR 里没有出处，面试前要对照官方文档核对。没有标注的内容，事实来自设计文档、ADR 或仓库里的代码和配置。设计里标了“预期”“推断”“没有测”的地方，这里保持同样的限定。数字都是 2026-11-15 在这个集群上量的，设计第 10 节。

## 1. Redis：为什么分成两套

### 1.1 库存闸门为什么不能上 Redis Cluster

**问**：Redis Cluster 能水平扩展，为什么库存闸门不放上去？

**要点（通用知识，先讲 Cluster 的规则）**：Redis Cluster 把键空间分成 16384 个槽，键按 CRC16 算槽，每个主节点负责一段槽。客户端按键所在的槽把命令发给对应的节点。一条命令、一个事务或一个 Lua 脚本如果访问了不在同一个槽的键，会被拒绝（`CROSSSLOT`）。想让几个键落在同一个槽，只能在键名里用 hash tag：键名里 `{}` 括起来的部分才参与算槽，`{gate}:a` 和 `{gate}:b` 落在同一个槽。

**本项目**（设计 1）：库存闸门的原子性来自“所有相关的键在同一个 Redis 上由一个脚本改”。`pkg/stockgate` 的 `holdScript` 一次下单受理要改：

- 每个 SKU 的可售数（`gate:sku:<sku>`）和限购计数（`gate:bought:<sku>`）；
- 这个订单的占用（`gate:hold:<订单号>`）；
- 全局的两个有序集合（`gate:holds`、`gate:claimed`）；
- 幂等键（`gate:idem:<用户>:<幂等键>`）。

这些键名各不相同，在 Cluster 里落在不同的槽，一个脚本跨槽就报错。此外还有一个实现层面的问题：脚本里的键名是用前缀拼出来的，调用时没有通过 `KEYS` 声明（代码里是 `holdScript.Run(ctx, rdb, nil, args...)`），而 Cluster 靠 `KEYS` 判断请求该发给哪个节点。

**hash tag 为什么救不了**：硬要在 Cluster 上跑，只能给所有闸门的键加同一个 hash tag（比如 `{gate}`），让它们全落在一个槽、也就是一个节点上。那样 Cluster 对闸门没有任何分摊作用，只多了一层间接，还要把键声明到 `KEYS` 里。分片的目的是把键分散，闸门要的是把键聚在一起，两者冲突。

测试把这一点钉住了（`pkg/redisx/modes_test.go`）：`TestClusterScriptCrossSlot` 让一个脚本访问两个不同槽的键，断言报 `CROSSSLOT`；同一个测试里换成相同 hash tag 的两个键，脚本可以执行。

另外两处也受 Cluster 影响（设计 1）：gateway 的限流脚本一次改多个桶（第 4 节）；`redisx` 原来只会建单节点客户端（第 5 节）。分布式锁（两个键）和查询缓存（一个键）问题不大。

### 1.2 Sentinel 和 Cluster 各解决什么问题

**问**：Sentinel 和 Cluster 有什么区别？各自怎么做故障切换？

**要点（通用知识）**：

| | Sentinel | Cluster |
|---|---|---|
| 解决什么 | 高可用：主库挂了自动提升从库；数据只有一份，容量受单机限制 | 分片加高可用：数据按槽分到多个主库，每个主库有从库 |
| 谁来发现故障 | 独立的 Sentinel 进程（一般 3 个或以上）互相通信，监视主从 | 节点之间用 gossip 互相探测，没有独立的监控进程 |
| 判定 | 单个 Sentinel 在 `down-after-milliseconds` 内连不上主库，标主观下线；够 quorum 个 Sentinel 同意，标客观下线；再选出一个 Sentinel 做这次切换的领头，由它挑一个从库提升 | 节点超过 `cluster-node-timeout` 联系不上另一个，标可疑；多数主节点都认为它不可达时标失败；它的从库发起选举，多数主节点投票后提升 |
| 客户端怎么跟 | 客户端问 Sentinel “主库是谁”，切换后重新问 | 客户端缓存槽到节点的映射，收到 `MOVED` 后更新 |
| 多键操作 | 单个主库，没有限制 | 跨槽不行 |

两种方式都是异步复制，切换时都可能丢掉主库已确认、还没传给从库的写入。两种方式的多数派都要求至少 3 个成员（Sentinel 3 个，Cluster 3 个主库），否则选不出多数。

**本项目**（设计 4.1、4.2、4.3）：Sentinel 部署为 3 个，分在三个节点上，`quorum` 是 2，`down-after-milliseconds` 是 5000，`failover-timeout` 是 30000（`deploy/charts/redis-ha/values.yaml`）。Sentinel 不能只放在两个 agent 上：放两个节点的话，一个节点挂了剩一个 Sentinel，凑不够 2 票，选不出主。Redis Cluster 6 个节点，3 主 3 从，`cluster-node-timeout` 是 5000 毫秒。

### 1.3 按用途分两套

**问**：为什么不用一个 Redis Cluster 把所有东西都放进去？

**要点**：顺着数据的性质分（设计 4.1）：

| | 闸门和锁 | 缓存和限流 |
|---|---|---|
| 放什么 | 库存闸门、分布式锁、读己之写标记 | 查询缓存、gateway 限流的桶 |
| 丢了会怎样 | 闸门的占用丢了要靠回收和对账收拾；锁丢了可能两个持有者 | 缓存重新回源，桶重新装满 |
| 访问方式 | 多个键的原子脚本 | 一个键一次操作（限流除外） |
| 拓扑 | 一主一从加 3 个 Sentinel | Redis Cluster，3 主 3 从 |
| 配置 | `noeviction`，AOF | `allkeys-lru`，不持久化，每个主库 `maxmemory` 128 MB |

闸门要的是“键在一起、原子地改”，对它有意义的是主从切换；缓存的键彼此独立，正好分片。这条决定**改了阶段计划原来的“Redis Cluster：3 主 3 从”**（设计 2 的第 1 项）。另一个理由是 1d 的约束：限流的流量不能压到闸门的 Redis 上，限流所以要放在缓存这一套（ADR-0019 的补充里把它作为不选“限流和闸门共用主从”的理由）。副产品：Sentinel 和 Cluster 两种高可用方式都能练到，阶段 4 各有各的故障可做。

面试里这是个好例子：**先看访问方式，再选拓扑**，不是看哪个技术“更高级”。

## 2. Sentinel 的实测

### 2.1 切换耗时

删掉主库的 Pod，Sentinel 在 6.9 秒时完成切换（设计 10 的发现 4）。`down-after-milliseconds` 是 5 秒，所以大头是判定下线的 5 秒，加上投票和提升。

### 2.2 旧主回来得比切换还快

**现象**：删掉主库 Pod 之后，新的 Pod 两三秒就起来了。这时 Sentinel 还没判定它下线（还没到 5 秒）。Redis 的启动脚本（`deploy/charts/redis-ha/templates/configmap.yaml` 的 `start-redis.sh`）起来先问 Sentinel “现在谁是主库”，问到的仍然是它自己，于是它以主库身份启动。同时 Sentinel 按旧地址判定它下线，并在 6.9 秒时完成切换。结果有十几秒两个实例都自认为是主库，之后 Sentinel 把旧主改成了从库。

**原因**：启动脚本的设计是“先问 Sentinel，Sentinel 说了算，不按序号自己决定”，目的是避免旧主重启后按序号自认为主库、而从库已被提升的脑裂。这条规则在“重启比判定下线还快”的时候失效：Sentinel 的答案本身是过期的。

**为什么没有写入到达旧主**：Pod 重建后地址变了，客户端的旧连接都断了；重连时客户端问 Sentinel，拿到的是新主。所以两个“主库”并存的十几秒里，客户端的写入只去了新主。切换前写入并确认复制过的键没有丢。

**怎么定位、怎么修**：这是实测时观察到的现象，设计里没有写修法。要说清楚的是：这次没有造成写入分叉，原因是“客户端地址随 Pod 重建而变”这个偶然的条件，而不是机制本身有保障；换成地址不变的部署方式（比如固定 IP）结论可能不同，这一点设计里没有展开，属于推断，不要说成已验证。

**（通用知识）补充**：Redis 自己的 `min-replicas-to-write` 可以让主库在没有足够从库时拒绝写入，间接缩小两个主库并存时的危害，但本项目**先没有设**（设计 4.2）：设成 1 的代价是唯一的从库一挂闸门就不可写，按 ADR-0021 等于拒绝下单。等阶段 4 量过再定。

### 2.3 异步复制下切换会丢什么，现有机制预期怎么兜

**问**：Redis 主从是异步复制，切换丢数据怎么办？

**要点**：（设计 4.2，**这是预期，没有实验**）主库确认了一次受理、还没传给从库就挂了，Sentinel 提升从库之后，这次受理的占用就没了，而用户已经拿到了“受理成功”。预期现有的机制这样兜：

- 落库的消息是受理之后才发的，消息里带着订单的全部内容；落库时在 MySQL 里预占库存，MySQL 那一侧会发现超卖（ADR-0018）。MySQL 是库存的唯一事实来源，闸门里的数只用来挡住注定买不到的请求。
- 闸门里的可售数会比实际多（占用丢了，数没有减），靠 inventory 的 `reconcile` 纠正。

这是阶段 4 的一个实验：实验前按这段推演写出预期，实验后对照。面试时要说清楚“我推演过、写下了预期，但没有验证”，不要说成已经证明。

## 3. Redis Cluster 的实测

### 3.1 主库重启得快，数据清空

**现象**（设计 10 的发现 5）：删掉一个主库的 Pod，它在节点超时（5 秒）之内就回来了，集群没有切换（没有人判定它失败），它带着空的数据继续当主库，它的从库随即和它同步成空的。

**原因**：缓存这一套不持久化（`appendonly no`、`save ""`），卷里只放 `nodes.conf`（`values.yaml` 里写着“卷只放 nodes.conf，不放数据”）。节点带着自己的身份（nodes.conf）回来，集群认为它还是原来那个主库，它的数据是空的。从库按复制规则从主库同步，于是一起变空。

**怎么办**：对缓存和限流可以接受（4.3 的前提：重新回源、桶重新装满）。但这正是“**不持久化的主库不能自动重启**”这条经验：如果让 Kubernetes 在主库挂掉后很快把它拉起来，比 Redis 自己的故障切换还快，结果是丢数据而不是切换。（通用知识）要避免的话，要么主库持久化，要么让故障判定先于重启发生；本项目选了前者之外的“接受”，没有做别的处理。

对照第 2.2 节：这是同一类现象的两种后果。Sentinel 那次是“两个主库并存”，Cluster 这次是“空主库覆盖了从库”，共同点都是 Pod 重启的速度快于故障检测的超时。

### 3.2 6 个 Pod 同时重建，怎么恢复

**问**：整个集群的 Pod 一起重启、IP 全变，Cluster 还能恢复吗？

**要点（通用知识）**：Redis Cluster 的节点把拓扑记在 `nodes.conf` 里（节点 ID、地址、槽、主从关系）。节点之间互相连接用的是 `nodes.conf` 里记的 IP；单个 Pod 重建时，别的节点能从连接来源学到新 IP；所有 IP 同时变，谁也找不到谁。

**本项目**（设计 4.3、10 的发现 5；`deploy/charts/redis-cluster/templates/configmap.yaml` 的 `start.sh`）：

- 每个节点用稳定的域名通告自己：`cluster-announce-hostname` 加 `cluster-preferred-endpoint-type hostname`，客户端拿到的 `MOVED` 和 `CLUSTER SLOTS` 里是域名，Pod 重建后客户端不用重启。
- 但域名只是元数据，节点之间互相连接仍然用 `nodes.conf` 里的 IP。所以启动脚本在 `redis-server` 起来之前，把 `nodes.conf` 里每一行的地址字段（`ip:port@cport,hostname`）按 hostname 重新解析，把 IP 换成最新的。自己的域名要等 DNS 就绪（试 10 次），别的节点试一次。
- 效果：6 个 Pod 同时删掉、地址全变之后，集群 8 秒内自己恢复。主从的配对这次恰好都不在同一个节点。

集群的组建是一次性的 Job（`bootstrap.sh`）：等 6 个节点都能 PING，已经组建好就什么都不做；“半成品状态”（已知节点数不是 6）不自动处理、报错让人检查；组建用 `redis-cli --cluster create`，它的 MEET 要用 IP，所以组建前先把域名解析成 IP。脚本最后输出主从对应，提醒要和 `kubectl get pod -o wide` 对照节点，主从同节点要用 `cluster replicate` 重配（反亲和没有强制这一点）。

## 4. 限流在 Cluster 下的取舍

**问**：限流脚本一次改几个桶，Cluster 下怎么办？

**背景**（ADR-0019 的补充、`pkg/ratelimit/redis.go` 里 `Redis` 类型的注释）：原来的做法是一次请求的几个桶（按接口、按用户、按 IP）在同一个脚本里处理，要么各扣一个令牌，要么一个都不扣。Cluster 下一个脚本只能访问同一个槽里的键，这些桶的键名不同，落在不同的槽。

**决定**：客户端是 Cluster 时，每个桶单独调用一次脚本，按调用方给的顺序，遇到第一个拒绝就停。按客户端的类型自动选择；单节点和主从下行为不变。

**放弃了什么**：

- **“一个都不扣”的保证。** 前面的桶放行并扣了令牌，后面的桶拒绝时，前面的令牌白扣。限流因此在被拒绝时略微偏严，不会偏松。调用方把最容易拒绝的桶放在前面，可以减少这种情况。
- **一次往返。** 变成最多“桶的个数”次，目前是 2 次。Redis 的时限（默认 20 毫秒）是整次判断的，不是每次调用的。

**为什么另外两种做法不行**：

- **hash tag 把几个桶绑到同一个槽**：做不到。同一个用户的桶要被他所有的请求共用，键名不能跟着某一次请求的接口变。比如“用户 A 的总桶”既要和“接口 X 的桶”绑在一起，又要和“接口 Y 的桶”绑在一起，两个组合要的槽不同。
- **先只读地检查所有桶，都够再逐个扣**：检查和扣之间不是原子的，并发下照样出现白扣，还多一倍往返。

**为什么白扣是可以接受的**：令牌桶偏严是安全的方向（多拒绝几个），偏松才危险。这是个把“精确”换成“能分片”的取舍，面试时要把代价说出口。

测试（`pkg/ratelimit/redis_cluster_test.go`）钉住了行为：`TestClusterRedisSecondBucketDenies`（第二个桶拒绝时请求被拒）、`TestClusterRedisEarlierBucketsStayCharged`（前面的桶确实白扣了一个令牌）、`TestClusterRedisSingleBucket`。这些测试要先 `make redis-modes-up` 起本机的 profile，环境没起来时跳过，**CI 里现在不跑**（设计 10 的发现 7）。

## 5. `redisx`：三种模式，一个地址字符串

**问**：怎么让同一份代码连单节点、Sentinel、Cluster？

**本项目**（`pkg/redisx/redisx.go` 的包注释、`ParseAddr`、`newClient`）：

```
127.0.0.1:6379                        单节点（本机、CI）
sentinel://gate@s0:26379,s1:26379     主从加 Sentinel
cluster://c0:6379,c1:6379             Redis Cluster
```

- **为什么一个字符串**：每个 Redis 仍然只对应一个环境变量（`REDIS_ADDR`、`CACHE_REDIS_ADDR`、`RATE_LIMIT_REDIS_ADDR`）。本机写主机加端口，集群上换成带模式的写法，服务的配置结构不用变。
- 有多个地址又没有前缀的写法直接报错（防止把逗号分隔的列表误当成单节点）。`Sentinel` 模式的地址是各个 Sentinel 的，不是 Redis 的；`Cluster` 的地址只是发现拓扑的种子，不必列全。
- 三种模式共用一份 `redis.UniversalOptions`（超时、连接池、重试、`ContextTimeoutEnabled`），再转成各自的类型（`opts.Failover()`、`opts.Cluster()`、`opts.Simple()`），返回统一的 `redis.UniversalClient`。Sentinel 和 Cluster 下连接池的上限是每个节点各自的，不是合计。

**为什么不用 `redis.NewUniversalClient`**（代码注释）：它按“有没有主库名、地址有几个”去猜模式，只给一个种子节点的 Cluster 会被当成单节点。这里的模式是地址里写明的，不猜。

**使用方的约束**（包注释）：换部署方式不是只换地址就行，要看使用方怎么访问键。库存闸门只能用单节点或 Sentinel；查询缓存三种都可以；限流对 Cluster 有单独的处理。所以接口统一了，语义没有统一，这一点面试时要主动说。

**Sentinel 模式的客户端行为**（代码注释，`newClient`）：返回的是指向当前主库的客户端，它订阅 Sentinel 的切换通知，主库换了之后新建的连接连到新的主库；**切换期间在途的命令会失败，由调用方按“Redis 不可用”处理**。测试 `TestSentinelFailover` 钉住了“主动切换后同一个客户端继续可写”。

## 6. MySQL

### 6.1 StatefulSet 提供了什么

**问**：为什么有状态服务用 StatefulSet 而不是 Deployment？

**要点（通用知识）**：StatefulSet 给每个 Pod 三样东西：

- **稳定的网络标识**：Pod 名是 `<名字>-<序号>`，重建后不变；配合 headless Service（`clusterIP: None`），每个 Pod 有稳定的 DNS 名。
- **按序号的创建和终止顺序**：默认 `OrderedReady`，序号 1 要等序号 0 就绪才创建。
- **卷和 Pod 的绑定**：`volumeClaimTemplates` 给每个 Pod 一个自己的 PVC，Pod 重建后挂回同一个卷。

**本项目**（设计 3、`deploy/charts/mysql`）：一个 chart 装三次（两个订单分片各一次，`mysql-main` 一次，副本数 1）。

- 每个分片一个 StatefulSet，2 个副本，序号 0 是初始的主库。`server-id` 由基数加序号算出来。
- headless Service 给稳定的名字，如 `mysql-order-0-0.mysql-order-0.mysql.svc`。从库找主库、CDC 找从库都用这种名字。`publishNotReadyAddresses: true`：Pod 没就绪名字也能解析，复制断了的从库不就绪，但它找主库、init 容器判断“别的 Pod 存不存在”都要这个名字。
- `persistentVolumeClaimRetentionPolicy` 的 `whenDeleted` 和 `whenScaled` 都是 `Retain`：卸载 release 或缩容都不删卷，删卷要人来做。
- `podManagementPolicy: OrderedReady` 的代价（chart 注释）：序号 0 不就绪时，序号 1 被删掉后不会重建，滚动更新也会停住。故障实验里要记得。
- 同一个实例的主从用 `podAntiAffinity` 要求不在同一个节点；序号 0 的节点靠 `preferredNode` 的偏好和 `OrderedReady` 的先后实现：序号 0 先调度拿到偏好的节点，序号 1 被反亲和排除在外只能去另一个。之后 local-path 的卷把 Pod 钉在第一次的节点上。
- 两个分片的主库错开放（`mysql-order-0` 主在 agent-1，`mysql-order-1` 主在 agent-2）：一个 agent 挂了丢的是一个分片的主库和另一个分片的从库，不是两个主库。`mysql-main` 没有从库，是已知的单点，和本机一样。
- 版本 9.7.4（和本机一致，LTS）。内存相关的参数重新定：集群上每个实例 1 Gi（缓冲池 512 MB），`mysql-main` 1.5 Gi，`performance_schema` 打开（阶段 3 定位锁等待要用）。

### 6.2 为什么不用 Operator

**问**：Operator 是什么？为什么 MySQL 不用？

**要点（通用知识）**：Operator 模式是“自定义资源加控制器”：用 CRD 描述“我要一个什么样的集群”，控制器持续把实际状态调到期望状态，把运维知识（搭复制、故障切换、备份、升级）写成代码。

**本项目的三个理由**（ADR-0027、设计 3.2）：

1. **拓扑不一样。** Oracle 的 MySQL Operator 管的是 InnoDB Cluster（组复制加 MySQL Router），每个分片至少 3 个实例，内存上是 6 个实例对 4 个。1c 做的是 GTID 异步复制的一主一从，围绕它已经有：按 GTID 等从库追上的读写分离（ADR-0013）、从从库读 binlog 的 CDC 和行版本号（ADR-0014）、用 CLONE 做的扩容（ADR-0015）。换成组复制，这些都要重新验证，而这和 2b 的目标无关。
2. **阶段 4 要练的正是 Operator 替你做掉的事。** 主库挂了，从库提升、其他服务改连接、CDC 换源、旧主回来怎么办，是计划内的故障实验。自动切换会把这些盖住。
3. **面试讲得清楚。** 手写一遍才讲得出 StatefulSet 的三样东西各解决什么，也才讲得出 Operator 在它之上多做了什么。

**代价**：复制的搭建、切换、备份都要自己写脚本，没有控制器持续纠正，出错的可能比成熟的 Operator 高。ADR-0027 里还列了没选的：Percona 的 Operator（支持异步复制，但部署的是 Percona Server 不是 MySQL 9.7，没有细查）、Vitess（等于把 1c 整个换掉）、不上集群（阶段 4 的故障注入 Chaos Mesh 作用在 Pod 上）。**Oracle Operator 的最新版本和支持的 MySQL 版本，做决定时没有查到**，不要在面试里报版本号。

### 6.3 从库怎么初始化

**问**：新从库怎么加入？

**本项目**（`deploy/charts/mysql/files/init-replica.sh`）：用 init 容器，判断依据是数据目录的状态，不是序号：

- 数据目录已初始化（`mysql.ibd` 在）且没有“初始化中”的记号：正常重启，什么都不做。复制的配置在系统表里，`mysqld` 起来自己接着复制；`super_read_only` 是 `SET PERSIST` 的，也还在。
- 有“初始化中”的记号：上次做到一半被杀了，清空重来，CLONE 不能续传。
- 空的：找到唯一的主库就从它 CLONE；找不到时，只有“序号 0、别的 Pod 都不存在”（第一次安装）才放行，让官方入口脚本初始化新的空库。**别的情况一律等**：比如主库的盘没了而从库还在，这时自己初始化一个空的主库，服务就会往空库里写，是最坏的结果。等着，由人决定先提升从库。

**CLONE**（通用知识）是 MySQL 的插件功能，从一个运行中的实例拷贝全量数据（物理拷贝），比 `mysqldump` 快；拷贝结束后新实例带着源的 `gtid_executed`。

**GTID 自动定位**：`CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION = 1`。位置来自自己的 `gtid_executed`（CLONE 带过来的），不需要记 binlog 文件名和位置；从库告诉主库“我执行过哪些事务”，主库补发缺的。

设计细节（脚本注释）：

- CLONE 用 `DATA DIRECTORY` 克隆到一个新目录，而不是克隆进正在运行的实例自己的数据目录：后者完成时 `mysqld` 要重启自己，容器里没有守护进程，它只能退出，要靠容器重启接着做后半段，每个中间状态都要能重入。克隆到新目录则临时实例一直在，做完再把目录挪过去，步骤是线性的。
- 整个过程不让 `mysqld` 监听对外的端口，复制和只读都配好之后主容器才启动，所以不存在“从库已经能被连上、但还可写”的窗口。
- `SOURCE_HOST` 用主库的稳定名字，不用 `-primary` 这个 Service：后者跟着 `primaryOrdinal` 走，改了 values 复制源不能被静悄悄地换掉（新主是否追平、有没有分叉的事务，要人先确认）。

**`super_read_only` 什么时候设**：在复制配好之后，用 `SET PERSIST read_only = ON; SET PERSIST super_read_only = ON`（写进数据目录里的 `mysqld-auto.cnf`，重启后仍然只读），脚本最后校验它确实是 1。**不能写进启动参数**：1c 的教训，官方镜像第一次初始化会失败；而且主从是同一份 Pod 模板，写进启动参数主库也只读了。复制线程不受 `super_read_only` 的限制（脚本注释，这里按它写）。

### 6.4 主从角色为什么不能记在 Pod 标签上

**问**：Service 怎么知道谁是主库？

**原设计**（设计 3.3 原文）：用 `role` 标签，由搭建复制的脚本打上，Service 按标签选 Pod。

**写 chart 的子 Agent 指出的问题**（设计 10 的发现 2）：

1. 标签在 Pod 对象上，Pod 一重建就没了，主库的 Service 随即没有端点；
2. 切换之后旧主的 Pod 重建，按序号重新打标签会把它标回主库。

**改成**：Service 按 Pod 名选（`statefulset.kubernetes.io/pod-name`，由 StatefulSet 控制器打，重建后还是它），主库是哪个序号写在 values 里（`primaryOrdinal`）。切换的最后一步是改 values 再 `helm upgrade`，**只动 Service，不重启 Pod**（`primaryOrdinal` 不进 Pod 模板，也不进 ConfigMap，所以不会引发滚动重启）；主从关系在 git 里。部署脚本只读地核对“values 说的主库”和实例的实际状态，不一致就报异常并以非零退出，不自动改。

限制：`-replica` Service 的 selector 只能写等于，写不出“除了主库以外的所有 Pod”，所以只支持 2 个副本，chart 里用 `fail` 校验。

### 6.5 切换时的两个顺序问题

**问**：手工切换主库要注意什么？

设计 10 的发现 2 留下两个要进切换手册的问题（**切换手册还没有写**）：

1. **已经建立的连接不跟着 Service 走。** 所以要**先把旧主设成只读，再改 values**。否则旧连接还连着旧主继续写。
2. **切换后旧主回来时是可写的**（没有 `super_read_only`），Service 不再指向它，但直接用稳定名字连它仍然能写进去，造成分叉；从库的 Service 还会把它当成从库，读到旧数据。所以要先重建它的复制（设只读、重建复制或重新 CLONE）才能让它回来。`middleware.sh` 和 `status.sh` 会把“可写但不是 `primaryOrdinal`”的实例报成异常。

ADR-0027 里写的“有意保留的失败方式”：标签或 Service 和实际主从不一致时（比如切换做了一半），服务会写到从库上并被 `super_read_only` 拒绝，宁可报错也不要写到错的地方。

### 6.6 探针：liveness 为什么不检查复制

**问**：三个探针各检查什么？

**要点（通用知识）**：startup 探针成功之前另外两个不生效；readiness 失败只是摘出 Service 端点，不重启；liveness 失败会杀掉容器重启。

**本项目**（`files/probe.sh`、`statefulset.yaml`）：

- startup：能用 root 登录并执行语句；周期 10 秒，失败阈值 60，也就是最长 10 分钟，给崩溃恢复。
- ready：能应答；配置了复制的，IO 和 SQL 两个线程都要在跑；没配复制的，要求没有 `super_read_only`（只读又没有复制的实例，也就是初始化或切换做了一半的，不就绪，免得被当成从库读到旧数据）。**按状态判断而不是按序号**：切换之后序号 1 是主库，按序号判断会让主库的 Service 没有端点。
- alive：`mysqladmin ping`，进程能应答就行，认证失败也算活着。**不检查复制**：复制断了，重启 `mysqld` 也修不好，只会让从库反复重启。

### 6.7 实测（设计 10 的发现 8，AppArmor 修好并重建之后）

三个实例一次部署成功；两个分片的主库分别在 agent-1 和 agent-2，从库各在另一个节点；主从的 GTID 集合一致。

| 检查 | 结果 |
|---|---|
| 主库写、从库读 | 读到；直接往从库写被 `super_read_only` 拒绝 |
| 删掉从库的 Pod（卷还在） | 11 秒后就绪；init 容器识别出已初始化、什么都不做；停机期间主库上的写入已经追上；`super_read_only` 仍然开着 |
| 删掉从库的卷 | 26 秒后就绪：init 容器从主库 CLONE（3 秒）、配好复制；克隆期间主库上的写入也在；GTID 和主库一致 |

每个实例空载约 480 Mi（上限 1 Gi，`mysql-main` 1.5 Gi）。写 chart 的子 Agent 担心“1 Gi 很紧”，要到 2c 有了连接和数据之后再看，**现在没有结论**。

## 7. Kafka

### 7.1 为什么 Kafka 用 Operator，MySQL 不用

**问**：同一个项目里，为什么一个用 Operator、一个手写？

**要点**（设计 5.1）：理由正相反。Kafka 的 broker 是对等的，没有“这个项目特有的拓扑”要保；Strimzi 做的滚动重启、配置变更、证书都是通用的。MySQL 恰恰是拓扑特殊、而且阶段 4 要练手工切换。另外“Operator 模式”本身是阶段计划里的考点，三个中间件里至少有一个该用它。

用法上：提交 `Kafka` 和 `KafkaNodePool` 两个资源（`deploy/cluster/kafka/kafka.yaml`），Operator 看到后创建 Pod、Service、配置。不要 `entityOperator`：topic 不用 `KafkaTopic` 资源管理（有两个地方管 topic，改分区数时就不知道以谁为准，还省一个 Pod），也没有用户和证书要管。

### 7.2 KRaft、controller 和 broker 合一

**要点（通用知识）**：KRaft 是 Kafka 用自己的 Raft 协议存放集群元数据，取代 ZooKeeper。运行 controller 角色的节点组成元数据的仲裁组；broker 角色的节点处理读写。两个角色可以合在一个节点上（合一模式），也可以分开。

**本项目**（`kafka.yaml`）：3 个节点，每个同时是 controller 和 broker（一个 `KafkaNodePool`，`roles: [controller, broker]`）。分开部署要 6 个 Pod，内存不够。每个 broker 堆 768 MB（`-Xms`、`-Xmx` 都是 768m），容器上限 1.5 Gi，剩下的留给页缓存，Kafka 的读写主要靠它。存储 20Gi 的 local-path，`deleteClaim: false`：删掉 Kafka 资源时不删卷，重新声明后 broker 带着原来的数据回来。只开集群内部的明文监听（`kafka-kafka-bootstrap.kafka.svc:9092`）。

### 7.3 副本数 3、`min.insync.replicas` 2、`acks=all`

**问**：这三个参数合起来是什么保证？

**要点**（设计 5.2、`kafka.yaml` 注释）：

- 副本数 3：每个分区有 3 份。
- `min.insync.replicas=2`：同步副本（ISR）少于 2 个时，分区拒绝写入。
- 生产者 `acks=all`（`kafkax` 里的 `AllISRAcks`）：等 ISR 里所有副本都确认才算成功。

合起来：挂一个 broker 照常读写（ISR 还有 2 个）；挂两个，剩下的那个拒绝写入，不会出现“写成功了但只有一份”的情况。偏移量 topic 和事务状态 topic 的副本数也设成 3、`transaction.state.log.min.isr` 是 2。本机是单 broker、副本数 1，这些行为到集群上才第一次出现。

**（通用知识）补充**：合一模式下，挂两个节点同时也让 controller 的仲裁组失去多数，元数据层面的影响设计里没有写；“停掉两个 broker 写入被拒绝”这项**没有测**（第 7.5 节）。

### 7.4 版本为什么是 4.3.2

**问**：为什么不用最新的 Kafka？

设计 1、5.1：Strimzi 最新版是 1.3.1，支持的 Kafka 是 4.2.0 到 4.3.2（测试过的 Kubernetes 是 1.32 到 1.37）。本机 compose 用的是 4.4.0，所以集群上只能用 4.3.2。这是对“选最新稳定版”规则的**偏离**，原因是 Operator 还不支持，在 `versions.md` 的备注里写明。两边差一个小版本，franz-go 客户端按协议协商，不受影响。Strimzi 支持 4.4 之后再升，升级本身也是一次值得做的练习（滚动升级 broker）。备选：等 Strimzi 支持 4.4，或不用 Strimzi 手写 4.4.0 的 StatefulSet。

### 7.5 建 topic 的副本数交给 broker 的默认值

设计 10 的发现 9：`kafkax.EnsureTopics` 原来在没指定副本数时写死成 1。改成不指定，交给 broker 的 `default.replication.factor`：本机的单个 broker 配的是 1，集群上是 3。**副本数是部署环境的属性，服务的代码不用知道。** 集群上 `auto.create.topics.enable: false`：topic 由服务启动时自己建，写错 topic 名时应该报错而不是悄悄建一个。

### 7.6 实测（设计 10 的发现 6）

- `acks=all`、6 分区 3 副本、512 字节的消息：约 4.5 万条/秒（22 MB/s）。
- 持续生产时删掉一个 broker：没有报错，最长的一条延迟 16 秒；恢复后没有落后的副本。
- “停掉两个 broker 时写入被拒绝”**没有测**：Strimzi 会立刻把删掉的 Pod 建回来，要用别的办法（阶段 4 的 Chaos Mesh）。这个现象本身就是 Operator 的“自愈”。

吞吐数字是空载、小消息、一个测试跑出来的，只能说明量级。面试时不要把它说成容量规划的结论。

## 8. AppArmor 那次排查（这一阶段最值得讲的故事）

设计 10 的发现 1，节点配置在 `scripts/cluster/vm.sh` 的 cloud-init 注释里。

### 8.1 现象

官方的 `mysql:9.7.4` 镜像在这些节点上起不来，不管是不是经过 chart：

- 以 root 运行：入口脚本卡在 `gosu`，它占着 CPU 空转；
- 以 mysql 用户运行：首次初始化做完之后 `mysqld --initialize` 一直不退出。

启动探针超时把容器杀掉后，留下半初始化的数据目录，之后每次启动都报“数据文件损坏”，循环崩溃。

### 8.2 怎么一步步定位

1. **排除 chart**：直接起一个不带任何自定义配置的官方镜像，同样卡住。问题不在我们写的 chart、脚本或参数上。
2. **关掉 AppArmor 做对照**：给 Pod 加 `appArmorProfile: Unconfined`，两种运行方式都正常。确定是 AppArmor。
3. **看内核日志**：节点上有对应的拒绝记录：
   ```
   apparmor="DENIED" operation="signal" profile="cri-containerd.apparmor.d" comm="mysqld" signal=term peer="cri-containerd.apparmor.d//&unconfined"
   apparmor="DENIED" operation="signal" profile="cri-containerd.apparmor.d" comm="gosu" signal=rtmin
   ```
4. **读标签**：规则只允许向“标签是 `cri-containerd.apparmor.d`”的进程发信号，而这些节点上容器进程的实际标签是 `cri-containerd.apparmor.d//&unconfined`，是两个标签叠在一起的，对不上。
5. **找为什么会叠上 `unconfined`**：Ubuntu 26.04 默认开着内核开关 `kernel.apparmor_restrict_unprivileged_unconfined=1`。k3s 自带的 runc 是不受限的进程，它在放弃权限之后才给容器切换配置，被这个开关当成“无特权的不受限进程换配置”，只能叠加不能替换。

### 8.2.1 为什么症状是这样的

容器里的进程连给自己发信号都被拒绝：`mysqld` 初始化完靠给自己发 SIGTERM 来退出，发不出去所以不退出；`gosu` 是 Go 程序，运行时靠信号同步各线程，信号被拒绝就空转。（通用知识）AppArmor 是 Linux 的强制访问控制，按“配置文件”限制进程能做的操作，信号是其中一类被管的操作。

### 8.3 怎么验证

主人在 k3s-agent-2 上手工把开关设成 0。之后新建的官方 MySQL 容器以 root、默认配置运行，13 秒完成初始化；`mysqld` 的标签是 `cri-containerd.apparmor.d`，不带叠加；内核日志里没有新的拒绝记录。

### 8.4 怎么修

把开关写进 `vm.sh` 的 cloud-init：节点上的 `/etc/sysctl.d/90-oppenheimer-apparmor.conf`，内容 `kernel.apparmor_restrict_unprivileged_unconfined = 0`，然后删掉三台虚拟机重建（2a 起集群本来就是脚本化、可重建的）。注意一个细节（`vm.sh` 注释）：文件在第一次开机时才写入，那时 systemd 已经加载过 sysctl，所以 `runcmd` 里再加载一次（`sysctl --system`），之后每次开机自动生效。

重建后三个节点的开关都是 0，全部中间件部署完之后三个节点自开机以来信号被拒绝的记录都是 0 条。容器仍然受 `cri-containerd.apparmor.d` 约束，**放宽的只是这一项系统级的限制**。

没有选的两种做法：在节点上维护一份改过的 AppArmor 配置；给每个 Pod 单独关掉 AppArmor。理由是前者要自己维护安全配置，后者要改每一个工作负载并且丢掉整个约束；而根因是节点上一个开关。

### 8.5 影响范围为什么不止 MySQL

三个节点的内核日志里，CoreDNS、MetalLB、Envoy Gateway 的控制器、仓库、metrics-server 都有 `signal=urg` 被拒绝的记录：那是 Go 运行时用来抢占 goroutine 的信号。这些程序照常工作，但抢占退化成了协作式的（（通用知识）Go 1.14 起默认用信号做异步抢占，信号发不出去就只能等 goroutine 自己让出）。长时间不让出的 goroutine 会拖住垃圾回收，阶段 3 量延迟的长尾时会受影响。容器里用 `timeout` 的探针也杀不掉超时的子进程。

### 8.6 教训

这个问题在 2a 就存在（Go 程序的抢占信号一直在被拒绝），但当时所有东西“看起来都正常”。**2a 的检查只看了功能，没有看节点的内核日志。** 装完一个新环境，应当把内核日志里的拒绝和报错过一遍。

可以延伸的几句：

- 对照法排查：先排除自己写的东西（直接用官方镜像），再逐个关掉可疑的层（AppArmor 设成 `Unconfined`），能把一个“MySQL 起不来”的问题缩到一个内核开关上。
- 症状在应用层（探针超时、数据文件损坏），根因在系统层（安全模块），中间隔了好几层。“数据文件损坏”是探针杀掉容器之后的**次生症状**，不是原因，按它去查数据会白费功夫。
- 对一个“看起来都正常”的集群，没有负面信号不等于没有问题，要主动找负面信号（内核日志）。

顺带改的（设计 10 的发现 1 末尾）：MySQL 的 Pod 改成以镜像里的 mysql 用户（999）运行，不用 root，入口脚本因此不经过 `gosu`。改完之后从库的初始化脚本里对卷的根目录做 `chown` 会失败（根目录属于 root，靠 `fsGroup` 可写），已改成失败不算错。

## 9. 资源和排布

### 9.1 QoS 的三个等级

**问**：Pod 的 QoS 有哪几级？怎么判定？

**要点（通用知识）**：

| 等级 | 条件 |
|---|---|
| Guaranteed | Pod 里每个容器（含 init 容器）的 CPU 和内存都设了 requests 和 limits，并且 requests 等于 limits |
| Burstable | 不满足 Guaranteed，但至少有一个容器设了 requests 或 limits |
| BestEffort | 所有容器都没设 requests 和 limits |

节点内存紧张时，kubelet 驱逐 Pod 的排序综合了：用量是否超过 requests、Pod 的优先级、超出 requests 的程度；QoS 等级影响的是 OOM 时内核的评分（`oom_score_adj`）。具体的排序细节面试前对照官方文档核对。

### 9.2 为什么这里是 Burstable，不是 Guaranteed

设计 6.1 原来写“有状态的中间件一律 requests 等于 limits（Guaranteed）……CPU 只设 requests，不设 limits”。**这两句是自相矛盾的**（设计 10 的发现 3）：CPU 不设 limits 的话是 Burstable，Guaranteed 要求 CPU 和内存的 requests 都等于 limits。

决定是**接受 Burstable**：不设 CPU 上限，避免被节流（CFS 的配额节流会让延迟出现毛刺，对数据库不友好——这一句是通用知识）。内存的 requests 等于 limits 保留，所以内存上不会被超卖。chart 里的注释还提到：init 容器也算在 Pod 的 QoS 里，所以 init 容器的内存也设成 requests 等于 limits。

### 9.3 PriorityClass 起什么作用

补偿 Burstable 的办法：建一个 PriorityClass `middleware`（`deploy/cluster/middleware/priorityclass.yaml`，值 1000000，远低于系统组件的 `system-cluster-critical` 的 2000000000，不会去抢 kube-system 里的东西）。文件注释里写的两个作用：

- 节点内存紧张时，kubelet 在用量都没超过 requests 的 Pod 里按优先级从低往高驱逐，业务服务先走；
- 调度不下时，中间件的 Pod 可以抢占业务服务的 Pod（`preemptionPolicy` 用默认的 `PreemptLowerPriority`）。

注意第二条是双刃的：中间件能把业务 Pod 挤走，不是单纯的好处。**现在 MySQL 和 Kafka 已经用上，两个 Redis 的 chart 还没有加 `priorityClassName`**（设计 10 的发现 3、“还没做的”）。

### 9.4 只有两个 agent，三个一组的中间件怎么放

**问**：Kafka、Sentinel、Redis Cluster 的主库都是三个一组，只有两个 agent，怎么放？

设计 6.2：约束有两条：同一组的成员不在同一个节点（反亲和）；主从、多数派的成员分散。全放在 agent 上，必然有一个 agent 上有两个：那个 agent 一挂，Kafka 就剩一个 broker、不可写，Sentinel 剩一个、选不出主。阶段 4 的“关掉一个节点，系统继续工作”就没法做。

所以三个一组的在三个节点上各放一个，包括 server 节点：一个 Kafka broker（1.5 Gi）、一个 Redis Cluster 的主库和一个别的主库的从库（各 192 Mi）、一个 Sentinel，合计约 2 Gi。MySQL 只放 agent：它的刷盘比 Kafka 频繁得多。

**风险**：Kafka 的磁盘写入和 etcd 在同一块虚拟盘上，而 etcd 的 fsync 延迟本来就偏高（2a 笔记 2.3）。设计留了两个退路：给 server 单独加一块盘放 Kafka 的数据；或者把三台虚拟机的规格调匀（重建集群只要 5 分半钟，现在没有数据）。

**实测结果**（设计 10）：重建之后 etcd 的 fsync：52% 在 2 毫秒以内、71% 在 8 毫秒以内、99.4% 在 16 毫秒以内，和 2a 相比没有变差，server 节点上多了一个 broker 没有带来可见的影响；p99 仍然高于 10 毫秒的参考线。（2a 的数是 43%、59%、99%。）这是空载下的结果，没有压测下的数据。

### 9.5 local-path 对有状态服务意味着什么

设计 3.3、2a 笔记 10：

- 卷建在哪个节点，这个 Pod 以后就只能在那个节点上（PV 带节点亲和）；Pod 不能随便漂移到别的节点重新挂载。
- 节点的磁盘没了：MySQL 从库要重新 CLONE，主库就是数据丢失，只能提升从库；Kafka 要靠别的副本重新同步。
- 所以副本分散和反亲和不是可选项：副本在同一个节点，就和数据在同一块盘上没区别。
- Redis Cluster 的卷里只放 `nodes.conf`，不放数据（3.1 节说的清空问题的背景）。

### 9.6 PDB

每一组 PodDisruptionBudget 设 `maxUnavailable: 1`（设计 6.2，三个 chart 里都有 `pdb.yaml`），保证排空节点（`kubectl drain`）时不会同时带走一组里的两个。（通用知识）PDB 只约束“自愿的驱逐”（drain、驱逐 API），不约束节点宕机这种非自愿的中断。

## 10. 内存的数和 CNI 的决定

### 10.1 预算和实测

设计 6.1 的预算：每个组件的 requests 合计约 12.3 Gi（MySQL 订单分片 4 个 4 Gi、`mysql-main` 1.5 Gi、Redis 主从 0.5 Gi、Sentinel 0.2 Gi、Redis Cluster 1.2 Gi、Kafka 4.5 Gi、Strimzi Operator 约 0.4 Gi，最后一项是估计值）。hardware.md 的预算是 11 GB，多出来的是 `mysql-main` 和 Sentinel：设计 1 的第 4 条发现，**预算里漏了一个共享的 MySQL 实例**。

实测（全部中间件在运行，空载，设计 10）：

| 节点 | 内存（`free` 的 used） | 2a 结束时 | Pod 的内存 requests 合计（占可分配的比例） |
|---|---|---|---|
| k3s-server | 2194 MiB | 1483 MiB | 2188 Mi（48%） |
| k3s-agent-1 | 2692 MiB | 771 MiB | 4672 Mi（46%） |
| k3s-agent-2 | 3018 MiB | 828 MiB | 6240 Mi（62%） |

按命名空间（`kubectl top`）：MySQL 2432 Mi（5 个实例），Kafka 2067 Mi（3 个 broker 加 Operator，刚重建、没有压测过；压测之后到过 3.3 Gi，多出来的是页缓存），Redis 52 Mi。实际用了约 7.9 GiB，但**调度看的是 requests**：三个节点已经承诺出去 13.1 Gi，还剩约 10.9 Gi 可以承诺。

要讲清的区别：requests 是调度的账，实际用量是 `kubectl top` 的数，两者不是一回事；预算要按 requests 算，不然调度器会因为“承诺满了”拒绝新 Pod，即使实际内存还很空。页缓存会让容器的内存读数虚高，Kafka 是典型例子（通用知识：页缓存算在容器的内存 cgroup 里，内存紧时可回收）。

### 10.2 CNI：保留 flannel

ADR-0026 的“CNI 改定”：

- 还能承诺的约 10.9 Gi 要放阶段 3 的监控（预算 5.5 GB）、业务服务（3 GB）、Chaos Mesh（0.5 GB），合计 9 GB，放进去之后剩约 1.9 Gi。
- Cilium 估计多用 1 到 1.5 GB（**仍然是公开资料的粗估，没有实测**），装上之后基本没有余量，而且 agent-2 已经承诺了 62%。
- 决定保留 flannel。放弃的是用 eBPF 取代 kube-proxy、Hubble 的按连接可观测，以及它和阶段 9 的衔接；NetworkPolicy 不受影响（k3s 内嵌的策略控制器）。
- **时间点是有讲究的**：换 CNI 等于重建集群，所以决定放在“中间件第一次部署完、量过内存之后、灌数据之前”。
- 重新评估的条件：宿主机加内存，或者监控的实际占用明显低于预算。

面试里这是一个“用数据做决定”的例子：2a 里是粗估，2b 里中间件部署完了才有真实的账。

## 11. 没做的和待办

诚实地说清楚（设计 10 的“还没做的”、设计 11）：

- 两个 Redis 的 chart 还没有加 `priorityClassName`。
- **备份**：MySQL 的 CronJob（设计 7.1 的方案：从从库做 `mysqldump --single-transaction --source-data`，写到 NAS，保留 7 份）和 etcd 快照同步到 NAS，**都没有做**。设计 7.1 已经写明的局限：binlog 没有单独归档，恢复到的是备份的时刻，之后的数据会丢，按时间点恢复不在 2b。
- **两次恢复演练**（MySQL 临时实例灌备份并用 `recon` 对账；etcd 的 `--cluster-reset` 恢复）没有做。
- 重启宿主机后全部中间件自己恢复的检查没有做。
- “停掉两个 broker 时写入被拒绝”没有测。
- **切换手册**没有写（第 6.5 节的两个顺序问题）。
- Redis 主从切换时闸门丢数据的实验、`min-replicas-to-write`：阶段 4。第 2.3 节的预期**没有实验验证**。
- Sentinel 的“旧主回来得比切换还快”：现象量到了，没有做处理，也没有验证换成别的方式后的行为。
- 不在 2b 做：MySQL 的自动故障切换、`mysql-main` 的从库、按时间点恢复、中间件的指标导出（阶段 3）、网络策略（2d）、业务服务上集群（2c）。
- `redisx` 的三种模式和限流的集成测试要手工起本机 profile，**CI 里现在不跑**。
- MySQL 每个实例 1 Gi 是否够，要到 2c 有数据之后再看。
- etcd 的 fsync p99 仍然高于 10 毫秒的参考线，原因没查（2a 留下的）。

## 12. 面试时怎么串起来讲

一个三分钟的讲法：

1. **先看访问方式再选拓扑**：库存闸门的脚本改一组名字各异的键，Cluster 上跨槽、hash tag 只能把它们挤在一个槽里，等于没分片，所以闸门用主从加 Sentinel，缓存和限流用 Cluster（第 1 节）。
2. **代价说出口**：限流在 Cluster 下“一个都不扣”变成“可能白扣一个”，偏严不偏松（第 4 节）；`redisx` 用一个地址字符串表达三种模式，但语义没有统一（第 5 节）。
3. **故障实测里的共同点**：Sentinel 旧主回来比切换快、Cluster 主库重启比超时快数据清空，都是“Pod 重启速度快于故障检测”（第 2、3 节）。
4. **MySQL 为什么手写**：拓扑和已验证的读写分离、CDC、扩容对得上；阶段 4 要练切换；StatefulSet 的三样东西。再讲角色不能记在标签上这个设计修正（第 6 节）。
5. **一次完整的排查**：AppArmor。排除自己写的东西、对照、内核日志、标签、内核开关、验证、写进 cloud-init 重建；影响不止 MySQL；教训是装完要看内核日志（第 8 节）。
6. **资源账**：Burstable 加 PriorityClass、三个一组的中间件三节点各一个、实测的内存和 CNI 的决定（第 9、10 节）。
7. **没做的**：备份、恢复演练、切换手册、两个 broker 的实验（第 11 节）。

## 13. 面试官可能追问的短问题

- **Sentinel 切换多久？** 实测 6.9 秒，其中 `down-after-milliseconds` 是 5 秒（第 2.1 节）。
- **切换时会不会丢数据？** 会有窗口，异步复制；预期靠落库时的 MySQL 预占和 `reconcile` 兜底，没有做实验（第 2.3 节）。
- **Cluster 里为什么不能用多键的 Lua 脚本？** 跨槽报 `CROSSSLOT`，要用 hash tag 把键绑到同一个槽（通用知识）；本项目的闸门脚本还没通过 `KEYS` 声明键（第 1.1 节）。
- **Pod 重建后 Cluster 的节点怎么找到彼此？** 域名通告加启动时按域名改写 `nodes.conf` 里的 IP（第 3.2 节）。
- **StatefulSet 的 Pod 重建后数据在哪？** 在同一个节点的 local-path 卷上，所以 Pod 不能漂移（第 9.5 节）。
- **为什么 liveness 不检查复制？** 重启修不好复制，只会让从库反复重启（第 6.6 节）。
- **`min.insync.replicas` 2 加 `acks=all` 是什么意思？** 挂一个照常，挂两个拒绝写（第 7.3 节）。
- **为什么 Kafka 是 4.3.2？** Strimzi 1.3.1 只支持到 4.3.2，本机是 4.4.0，是对“选最新稳定版”的偏离（第 7.4 节）。
- **Guaranteed 和 Burstable 的区别？** 看 CPU 和内存的 requests 是否都等于 limits；本项目内存相等、CPU 不设上限，所以是 Burstable，用 PriorityClass 补（第 9 节）。
- **为什么不上 Cilium？** 剩余内存约 1.9 Gi，Cilium 估计多用 1 到 1.5 GB（没实测），且决定时间点要在灌数据之前（第 10.2 节）。
- **PDB 能防住节点宕机吗？** 不能，只约束自愿驱逐（通用知识）。
