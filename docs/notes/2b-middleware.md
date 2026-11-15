# 2b 笔记：把 MySQL、Redis、Kafka 部署到 k3s 上

这份笔记整理 2b 里值得在面试中讲的点，按主题组织，不按开发步骤。每条按“面试官会怎么问 → 要点 → 本项目里怎么做、量到什么数、踩过什么坑”来写。设计全文见 [design/2b-middleware.md](../design/2b-middleware.md)（下文简称“设计”，引用章节号），取舍见 [ADR-0027](../adr/0027-mysql-statefulset-not-operator.md)、[0019](../adr/0019-gateway-rate-limiting.md)（末尾的补充）、[0026](../adr/0026-k3s-datastore-and-bundled-components.md)（末尾的“CNI 改定”）。集群和 2a 的背景见 [2a 笔记](2a-cluster.md)。

2b 做的事：把三类中间件放到 2a 建好的集群上：MySQL（手写 StatefulSet）、Redis（两套：主从加 Sentinel、Redis Cluster）、Kafka（Strimzi Operator）；配好资源、排布、持久化；`pkg/redisx` 和 `pkg/ratelimit` 改成能连三种 Redis；后半做了 MySQL 备份和恢复演练、计划内主从切换、etcd 快照和恢复演练、入口双副本、重启宿主机的检查（第 11 到 16 节）。没做的见第 17 节，笔记只写已经做了的。

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
- 效果：6 个 Pod 同时删掉、地址全变之后，集群 8 秒内自己恢复。（设计 10 的发现 5 原来说“主从的配对恰好都不在同一个节点”，那是运气：重建之后有一对落在了同一个节点，见第 15.1 节。）

集群的组建是一次性的 Job（`bootstrap.sh`）：等 6 个节点都能 PING，已经组建好就什么都不做；“半成品状态”（已知节点数不是 6）不自动处理、报错让人检查；组建用 `redis-cli --cluster create`，它的 MEET 要用 IP，所以组建前先把域名解析成 IP。脚本最后输出主从对应。反亲和管不了这一点（`create` 不知道哪两个 Pod 在同一个节点），所以 `middleware.sh redis` 之后会用 `redis-cache-pairing.sh fix` 核对并用 `CLUSTER REPLICATE` 修正（第 15.1 节）。

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

### 6.5 切换时的两个顺序问题（简述）

**问**：手工切换主库要注意什么？

设计 10 的发现 2 留下的两条，已经写进切换手册，完整的步骤、实测和故障场景见第 13 节：

1. **已经建立的连接不跟着 Service 走**，所以先把旧主设成只读，再改 values。
2. **故障切换后旧主回来是可写的**，会分叉，要先让它只读、再重建复制或重新 CLONE。`middleware.sh` 和 `status.sh` 会把“可写但不是 `primaryOrdinal`”的实例报成异常。

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

注意第二条是双刃的：中间件能把业务 Pod 挤走，不是单纯的好处。MySQL、Kafka 和两个 Redis 现在都用上了（`deploy/cluster/` 下各实例的 values 里都有 `priorityClassName: middleware`）。**设计 10 的发现 3 和“还没做的”里仍写着“两个 Redis 还没有加”，那是过时的**，发现 10 里“给两个 Redis 加 `priorityClassName` 时顺手核对”说明后来加了。

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

**但 Redis Cluster 的那一个 PDB 部署之后一直是坏的**，允许的中断数是 0，`drain` 会卡住；原因是它的选择器选中了组建集群的 Job 的 Pod。当时只验证了“PDB 建出来了”。见第 15.2 节。

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

## 11. MySQL 备份

设计 7.1、10 的发现 14；`deploy/charts/mysql/files/backup.sh`、`accounts.sh`。

### 11.1 逻辑备份的原理和局限

**问**：`mysqldump --single-transaction` 怎么得到一致的备份？只有它能恢复到什么时候？

**要点（通用知识）**：逻辑备份导出的是一堆 SQL 语句，不是数据文件。`--single-transaction` 开一个可重复读的事务，靠 InnoDB 的一致性快照读，不给表加锁，备份期间业务照常写；只对 InnoDB 一致。代价是大库导出和重放都慢（恢复要把 INSERT 重放一遍再建索引），物理备份（CLONE、xtrabackup）更快，但不能跨版本、不能挑库。

**本项目**：开头还要短暂持有一次全局读锁（`FLUSH TABLES WITH READ LOCK`），用来把“快照”和“binlog 位点、GTID”对齐到同一个时刻，对齐完就释放（`accounts.sh` 的注释）。不对齐的话，恢复后按位点续接会重一段或漏一段。

**局限要主动讲**（设计 7.1）：binlog 没有单独归档，所以**能恢复到的是备份的那一刻，之后的数据会丢**。每天一次，三个实例错开（UTC 19:00、19:30、20:00）。按时间点恢复要归档 binlog，不在 2b。

实测（设计 10 的发现 14）：三个实例手工备份一共 10 秒，每份约 4.5 KB。数据是每个实例 1000 行的记号数据，**这些数没有容量规划的意义**，2c 灌过数据要重量。

### 11.2 在从库上备份、位点和 GTID

- **为什么在从库上做**：不占主库。`mysql-main` 没有从库，从它自己做（`DUMP_MODE=single`）。
- **备份前先确认它真是从库、复制在跑**：`-replica` Service 切换之后可能指向别的东西；复制断了的从库备出来的是旧数据，却不报任何错。脚本里 `SHOW REPLICA STATUS` 为空或两个线程不是 `Yes` 就直接失败，宁可没有备份也不要一份假的新备份。
- **`--source-data` 和 `--dump-replica`**：前者记“这台服务器自己的” binlog 位点；后者在从库上记“它复制到了主库的哪里”，语义上更适合想接回主库的场景。选了前者（在 9.7.4 上实测），因为 `--dump-replica` 会先停从库的 SQL 线程（备份期间从库越落越后，读从库的 CDC 跟着停），还要 `REPLICATION_SLAVE_ADMIN`，等于让备份账号能改复制拓扑，和最小权限矛盾。取值 2 是把位点写成被注释掉的 `CHANGE REPLICATION SOURCE`，恢复时不会自动执行。
- **GTID 下靠什么续接**：`--set-gtid-purged=ON` 把和快照一致的 `gtid_executed` 写在 dump 开头（`SET @@GLOBAL.GTID_PURGED`）。恢复出来的实例的 `gtid_executed` 就停在快照那一刻，接回复制时 `SOURCE_AUTO_POSITION = 1` 从这个点要缺的事务，不丢不重。文件名加位点只是给人看的记录。`meta` 文件里也存了这个集合，演练用它核对。
- 只导业务库，不导系统库（账号由 accounts hook 重建）。9.x 的 mysqldump 默认还去读数据脱敏策略的系统表，备份账号没有权限，要加 `--skip-masking-policies`。

### 11.3 最小权限的备份账号，和“缺权限时悄悄少导”

**问**：备份用 root 不行吗？不用 root 怎么确定要哪些权限？

专门建一个 `backup` 账号。权限是在 9.7.4 上**逐个去掉、看哪一步出问题**试出来的：

| 范围 | 权限 | 为什么 |
|---|---|---|
| 全局 | `FLUSH_TABLES` | 对齐位点时的 `FLUSH TABLES WITH READ LOCK`；比 `RELOAD` 窄 |
| 全局 | `REPLICATION CLIENT` | `SHOW BINARY LOG STATUS`；脚本还要 `SHOW REPLICA STATUS` |
| 全局 | `SHOW_ROUTINE` | 导存储过程和函数 |
| 每个业务库 | `SELECT, SHOW VIEW, TRIGGER, EVENT` | 读数据、导视图、触发器、事件 |

没给的：`LOCK TABLES`（不锁表）、`PROCESS`（加了 `--no-tablespaces`）、`REPLICATION_SLAVE_ADMIN`（`--dump-replica` 才要）。

**要讲的重点：缺权限有两种后果。** 缺 `SHOW VIEW` 或 `EVENT`，dump 直接失败，容易发现；**缺 `SHOW_ROUTINE` 或 `TRIGGER`，dump 照常成功、退出码是 0，存储过程和触发器却悄悄不在里面**（实测）。所以光看“备份成功”不够，演练要把这几类对象的个数和线上比（第 12 节）。教训：退出码为 0 只说明命令没出错，不说明内容完整，要比内容。

权限会随库复制到从库，而且只加不减，以后要收回得手工 `REVOKE`。密码在 Secret 的 `backup-password` 里。

### 11.4 半截的备份怎么不被当成备份，保留策略

**问**：备份到一半被杀了、NAS 掉线了，怎么保证恢复时不会拿到半截的文件？

做法（`backup.sh` 的注释）：

1. 写到 `.tmp-<时间戳>`，不是最终目录。
2. 管道开 `pipefail`，mysqldump、zstd 任何一段失败都让 Job 失败。
3. 写完整份**解压一遍**：确认最后一行是 `-- Dump completed`（被截断的 dump 没有这一行），并取出 GTID；取不到就失败。
4. `sync`，算 SHA-256 并校验，写 `meta`。
5. `mv -T` 把临时目录改成最终名字。同一个目录里的 rename 是原子的，所以任何时刻看到的时间戳目录都是完整的。
6. 退出时（`trap`）没走到改名就删掉自己的临时目录；以前失败留下的 `.tmp-*`，只清比 `activeDeadlineSeconds` 更老的（手工触发的 Job 不受 `concurrencyPolicy` 约束，更新的可能是另一个正在跑的备份）。

**保留 7 份，先写新的、成功之后才删旧的**：先删后写的话，写失败就一份都没有了。只删符合时间戳命名的目录，手工放进去的东西不会被动。恢复和保留的逻辑都认时间戳命名，`.tmp-*` 对它们不存在。

一个脚本坑（代码注释）：不要写 `cmd | grep -q`，`grep -q` 找到就退出，上游被 SIGPIPE 终止，退出码 141，在 `pipefail` 下被当成失败；要取内容的地方都用 awk 读到底。

存储：一个 NFS 的 PV（RWX），三个 CronJob 共用，各写各的子目录。没有用 `subPath`，它的目录由 kubelet 以 root 建，以 999 运行的 Pod 写不进去。代价是三个 Job 互相看得到对方的目录，靠脚本约束而不是权限。

**已知的缺点**：`backup.sh` 在和 MySQL 共用的 ConfigMap 里，部署这次改动让三个实例（含主库）滚动重启了一次，以后改脚本也会；备份失败没有告警，失败的 Job 留在记录里要人去看，阶段 3 接监控时加。

## 12. 恢复演练

设计 10 的发现 14，`scripts/cluster/mysql-restore-drill.sh`。

**问**：为什么要演练？演练比对什么？

**为什么**：“备份 Job 成功”只证明文件写出来了。能不能灌进去、GTID 对不对、内容全不全、对象有没有悄悄少，只有真的恢复一次才知道。11.3 里“缺权限悄悄少导”这类问题，就只能靠演练里的比对发现。

**做法**：

1. 起一个临时 MySQL Pod：同一个镜像，数据在 `emptyDir`，备份的 PVC 只读挂载，只调度到 agent；不带 MySQL 的标签，不会被任何线上的 Service 或 PDB 选中；`trap` 里删掉，`activeDeadlineSeconds` 两小时兜底。
2. 校验 SHA-256，灌入最新一份，记耗时。
3. 核对恢复出来的 `gtid_executed` 和 `meta` 里记的一致。
4. 逐表和**线上从库**（只读查询）比行数和 `CHECKSUM TABLE`；再比存储过程、触发器、事件、视图的个数。

不碰线上实例，不是破坏性操作。三个实例都通过。

**有写入时为什么比不了**：备份是 T 时刻的快照，T 之后线上继续变，行数和校验和当然对不上。这次真的碰到了：切换演练的写入探针还在往线上写，恢复出来 1295 行、线上 1328 行；探针停了、重新备份再比就一致。这不是备份有问题，正是“能恢复到的是备份的时刻”的直接表现。有流量时要改成按 `meta` 里的 GTID 用对账工具（`recon`）比，这是 2c 的事。

**局限**：数据量是假的，恢复耗时不到 1 秒没有参考价值；`CHECKSUM TABLE` 对大表很慢（通用知识），数据多了要换比对方式。

## 13. MySQL 主从切换

设计 10 的发现 15，手册 [infra/mysql-switchover.md](../infra/mysql-switchover.md)，ADR-0027。

### 13.1 三个前提事实

顺序为什么是这样，全靠这三件事：

1. **“谁是主库”有两处**：实例自己的状态（可写且没配复制的是主库），和 values 里的 `primaryOrdinal`（决定两个 Service 指哪个 Pod）。两处要人让它们一致；`helm upgrade` 只改 Service，改实例的语句也不动 Service。`middleware.sh mysql` 结尾比这两处，不一致就报 `ANOMALY` 并以非零退出。
2. **Service 只管新建的连接**：已经建立的连接连的是某个 Pod 的地址，Service 改了指向，它也不断。服务连接池最长把一个连接留 30 分钟（`ConnMaxLifetime`）。所以改 Service 不能当“把写入挪走”的办法，必须先让旧主拒绝写入。
3. **复制是异步的**：主库说“提交成功”时，从库不一定有这个事务。计划内切换可以等追平；主库突然没了，从库上没有的事务就丢了。

### 13.2 计划内切换：每一步为什么在这个位置

| 步 | 做什么 | 为什么在这里 |
|---|---|---|
| 1 | 核对起点干净、从库延迟接近 0 | 第 3 步要等追平，延迟多大写入就停多久 |
| 2 | 旧主 `SET PERSIST super_read_only = ON` | 之后旧主不再产生新事务，要追的集合就固定了；用 `PERSIST` 是为了流程中途 Pod 重启，它起来仍然只读 |
| 3 | 取旧主的 `gtid_executed`，在从库上 `WAIT_FOR_EXECUTED_GTID_SET` 等到执行完，返回 0 才继续 | 这一步是“零丢失”的证明点；返回 1（超时）就停下查，旧主设回可写即可回到起点 |
| 4 | 从库 `STOP REPLICA; RESET REPLICA ALL`，放开写入 | 此时 `-primary` 还没有端点，没有流量 |
| 5 | 旧主 `CHANGE REPLICATION SOURCE`（自动定位），`START REPLICA` | 第 3 步确认过两边一致，所以不用补事务 |
| 6 | 改 `primaryOrdinal`，`middleware.sh mysql` | 写入从这里恢复；只有 Service 的 selector 变，Pod 不重启（前提是没夹带别的 chart 改动） |
| 7 | 在旧主上 `KILL` 业务账号的连接 | 老连接留在旧主上会一直报 1290，直到连接池换掉 |
| 8 | 提交 values | 主从关系在 git 里，不提交下次部署会指回旧主 |

**为什么先只读、再改指向**：反过来的话，旧连接还连着旧主继续写，切换之后这些写入落在“已经不是主库”的实例上。顺序保证了任何一步停下来，结果都是“写不进去”，不是“写到两个地方”：就绪探针对“只读又没配复制”的实例判不就绪（第 6.6 节），所以第 2 步之后 `-primary` 没有端点，新连接直接失败。

**步骤 5 在 6 之前**：改 Service 之后 `-replica` 就指向旧主，让它先已经在复制，读从库的请求才不会读到停在旧状态的数据。

第 6.5 节的两个顺序问题就是 13.1 的事实 2 和后面 13.5 的风险，那里只留简述。

### 13.3 实测，零丢失怎么证明

在 `mysql-order-1` 上切了两趟（0 到 1，再切回 0），探针每 0.5 秒经 `-primary` Service **新建连接**写一行：

| | 写入失败时间 | 失败次数 |
|---|---|---|
| 0 到 1 | 约 5.2 秒 | 10 |
| 1 到 0 | 约 5.2 秒 | 10 |

- 一共 490 次写入得到成功的答复；结束后主从都是 1490 行（1000 行记号加 490）：**没有丢也没有多**。
- 旧主设成只读之后就绪探针让 `-primary` 没有端点，新连接直接失败，**没有一次写到错的实例**。
- 5.2 秒是把各步连成脚本的结果，大部分花在 `helm upgrade`（`middleware.sh` 按顺序处理三个实例，`mysql-order-1` 排第二）。人照手册一步步敲会更久；`helm upgrade` 没有重启任何 Pod。

证明由两部分合起来：机制上，第 2 步之后没有新事务、第 3 步等到全部执行完才继续；实测上，成功答复的次数加起点正好等于两边的行数。**要说清的是后者是按行数核对的**，设计里没写探针的行是否带可逐行比对的编号。

没验证的：连接池遇到 1290 和断连之后自己恢复（2c）；CDC 换源（2c，行版本号的正确性依赖 CDC 的源，ADR-0014）。

### 13.4 主库故障：会丢什么，怎么知道丢了哪些

**没有演练，是按三个事实推出来的步骤，阶段 4 做实验**（手册第二节）。

- **先判断要不要切**：Pod 被删、进程崩溃，十几秒就回来，数据在本地卷上，什么都不用做；切换反而丢数据。只有短时间回不来（节点、卷坏了）才切。
- **会丢什么**：主库已确认、没传到从库的事务。对订单库是“用户拿到受理成功、订单落库后却不见了”，要靠对账（`recon`）和上游重试找回。要不丢得用半同步，ADR-0027 放弃了这条。
- **怎么知道丢了哪些**：记下新主的 `gtid_executed`；旧主起来后**先设只读**，在旧主上执行 `GTID_SUBTRACT(旧主的 gtid_executed, 新主的 gtid_executed)`。结果为空：没有多出来的事务，按计划内的第 5 步接成从库。不为空：这些就是没复制出去的，先用 `mysqlbinlog --include-gtids` 导出留证，再决定补写还是放弃；旧主这时已经和新主分叉，不能直接接成从库，要重新 CLONE。

### 13.5 旧主回来的风险

旧主的 `super_read_only` 是关的（故障切换时没机会设），它一回来就是一个可写的实例，而且就绪探针会让它通过。直接用稳定名字连它就能写进去，造成分叉；`-replica` Service 还会把它当从库，读到旧数据。所以手册第一步是 `cordon` 掉它所在的节点，让 Pod 回不来，由人决定它什么时候、以什么身份回来。

**怎么让它起来就是只读**：用 `SET PERSIST` 而不是 `SET GLOBAL`，写进数据目录的 `mysqld-auto.cnf`，重启后仍然只读。计划内切换第 2 步就是这么做的。故障场景里旧主没来得及设，只能靠 cordon 加“一回来立刻设只读”，中间有一个窗口，`-replica` 此时指向它。

这和第 2.2 节是同一类问题：旧主回来的时机不由自己说了算。`middleware.sh` 和 `status.sh` 会把“可写但不是 `primaryOrdinal`”报成异常，accounts hook 发现不止一个可写实例会拒绝往任何一个里写。

**提升之前要等从库把收到的事务应用完**（手册第二节第 2 步）：IO 线程断了之后，收到的事务在中继日志里（`RECEIVED_TRANSACTION_SET`），应用了的才进 `gtid_executed`。判据是 `GTID_SUBSET(RECEIVED_TRANSACTION_SET, @@global.gtid_executed)` 为 1；在这之前提升，会把“已经到了从库、只是还没应用”的事务也丢掉。这条语句在集群的从库上跑过，流程本身没有演练。

## 14. etcd 的快照和恢复

设计 7.2、10 的发现 11 和 17，`deploy/cluster/k3s/etcd-snapshot-sync.sh`。

### 14.1 快照和同步

k3s 自己每 12 小时拍一份，本地留 5 份。server 节点上的 systemd 定时器（每天 00:20、12:20，在 k3s 之后 20 分钟）把它们复制到 NAS，NAS 上留 14 份。一份快照 20 MB，拍加复制约 2 秒。

**为什么放在节点上，不做成集群里的 CronJob**：它备份的是控制面，控制面坏了它也得能跑；放在集群里就成了循环依赖。

**NFS 为什么用 soft**：这里的 NAS 可能关机，要的是“这一次失败、下一次再试”，不是永远挂住，所以 soft 加短超时（`timeo=100`、`retrans=2`）；只在复制那一会儿挂载，NAS 关机不影响 k3s 拍快照本身（不让 k3s 直接写 NFS：挂着 NFS 时 NAS 一关机，快照任务会卡在写上）。（通用知识）hard 是对端不在就无限重试，适合“写不能丢、对端一定会回来”的场景，进程会卡在不可中断的等待里；仓库里别处的 NFS 挂载用的是 hard。soft 的风险是写到一半返回 I/O 错误，由“先写临时名字、读回来比对 SHA-256、再改名”兜住，半截的文件不会被当成快照。本地一份快照都没有时脚本以失败退出：定时器在跑而 NAS 上永远是空的，这种安静的失败要让它显出来。

**token 为什么一起备份**：k3s 把引导数据（各个 CA 的私钥等）用 server 的 token 加密后存在 etcd 里。换一台机器恢复时没有同一个 token，设计的说法是“快照读得出来，集群起不来”。**这是代码注释和设计里的说法，换机器恢复没有演练过。** token 放在快照旁边没有扩大暴露面：快照里本来就有全部 Secret（没有开静态加密），NAS 上的目录是 root、权限 700。

### 14.2 恢复演练的做法和结果

做法（先对照了 k3s 文档，单 server）：

1. 建命名空间 `etcd-drill` 和记号 `before-snapshot`，记下 39 个 Pod 的名字和重启次数、Secret 个数、一个 Secret 的 `resourceVersion`。
2. `make etcd-backup` 拍快照并同步到 NAS，之后再建记号 `after-snapshot`。
3. 在 server 上：挂载 NAS，校验快照的 SHA-256，确认 NAS 上的 token 和本机一致，**把 NAS 上的那份复制回来用它恢复**（演练的是“备份能用”，不是“本地的快照能用”）；`systemctl stop k3s`，`k3s server --cluster-reset --cluster-reset-restore-path=<副本>`，`systemctl start k3s`。

结果：从停 k3s 到 API 可用 8 秒（恢复本身 3 秒）。`after-snapshot` 不见了，`before-snapshot` 在；39 个 Pod 的名字和重启次数不变，Secret 个数和 `resourceVersion` 不变；两个 agent 没有重启、自己接回来；MySQL 主从、两套 Redis、Kafka、入口通路、七个 PDB 都正常。

### 14.3 恢复的是“集群认为应该有什么”

**这是最要讲清的一点**：etcd 快照里只有 API 对象，不是工作负载的数据。

- 停控制面的那 8 秒里所有 Pod 照常运行（kubelet 和容器不依赖 API 才能继续跑），MySQL 的数据一行没动。
- 反过来，快照之后才建的对象，恢复后控制面不再认识。如果那是 Deployment，它的 Pod 还在节点上跑着，但没有“主”了。设计的说法是 kubelet 会把这些容器清掉；**这次的记号是 ConfigMap，没有看到这一层，是推断，没有测过。** 快照和现状差得越多，恢复后的“回退”越多（Secret 被轮换过、PVC 是快照之后建的）。
- 单 server 的恢复是原地的，token 没用上。

**没演练的**：server 的虚拟机整个没了，换一台从 NAS 的快照加 token 恢复；`make cluster-up` 现在不会从快照恢复，只会建一个新的空集群。

## 15. 三个小故事

### 15.1 Redis Cluster 的主从落在了同一个节点（发现 10）

- **现象**：给两个 Redis 加 `priorityClassName` 时顺手核对，发现 `redis-cache-0` 和它的从库 `redis-cache-4` 都在 k3s-agent-1。那个节点一挂，这三分之一的槽没有副本可以顶上。
- **原因**：配对是 `redis-cli --cluster create` 定的，它按地址避开“同一台主机”，而 Pod 的地址各不相同，它不知道哪两个在同一个节点；Pod 落在哪个节点是调度器定的。第 3.2 节原来的“恰好都不在同一个节点”是重建之前那一次的运气，`middleware.sh` 当时只把配对打印出来，没人去对照。
- **怎么修**：`scripts/cluster/redis-cache-pairing.sh`（`check`、`fix`），`middleware.sh redis` 结尾调用 `fix`。它取每个 Pod 的节点和 `cluster nodes` 的主从关系，找改动最少的配对，用 `CLUSTER REPLICATE` 把从库改挂过去。那次改了两个从库，之后 `--cluster check` 通过。
- **`CLUSTER REPLICATE` 和主从切换的区别**：前者只改“某个从库挂在哪个主库下”，从库丢掉原有数据重新全量同步，不动槽，对缓存没有影响；切换是把从库提升为主，会短暂影响写入。所以脚本只自动做前者。**两个主库在同一个节点的情况不自动处理**，报异常并以非零退出：那个节点一挂就同时少两个主库，剩下一个凑不出多数票，整个集群不能自动切换。
- **教训**：一条性质（“主从不同节点”）如果靠碰运气成立，就要有东西去核对它；只打印不核对等于没有。和第 6.4 节“标签会丢”是同一类。

### 15.2 PDB 一直是坏的（发现 16）

- **现象**：核对各组件 PDB 时发现 Redis Cluster 的 PDB 报 `SyncFailed`（`jobs.batch does not implement the scale subresource`），允许的中断数停在 0。后果是 `kubectl drain` 任何有 `redis-cache` 的节点都会卡住，而三个节点上都有。从部署那一刻起就是这样。
- **原因**：组建集群的 hook Job 的 Pod 带着和 Redis 一样的标签，做完还留着（有意保留，方便看日志），被 PDB 的选择器选中。（通用知识）PDB 用 `maxUnavailable` 或百分比时要知道“应该有几个 Pod”，办法是找到每个 Pod 的控制器、读它的 scale 子资源；Job 没有这个子资源，计算失败。第 9.6 节说“每组 PDB `maxUnavailable: 1`”，当时只验证到了“PDB 建出来了”。
- **怎么发现的**：看 PDB 的 `ALLOWED DISRUPTIONS` 和状态，不是看它存不存在。
- **修法和教训**：Job 的 Pod 换一套自己的标签；改完七个 PDB 允许的中断数都是 1。PDB、Service、NetworkPolicy 都按标签选 Pod，一次性的 Job、hook 的 Pod 不能沿用会被它们选中的标签。

### 15.3 Job 成功时有两个为真的条件（发现 14 末尾）

- **现象**：`make middleware-backup` 第一版一直等到超时，而备份 3 秒就做完了。
- **原因**：Kubernetes 1.31 起 Job 成功时有两个为真的条件：`SuccessCriteriaMet` 和 `Complete`；失败时是 `FailureTarget` 和 `Failed`（`middleware.sh` 注释，实测）。脚本把所有为真的条件拼起来和 `Complete` 比，永远不相等。
- **怎么发现的**：写脚本的子 Agent 在本机没有集群可测，到集群上才跑出来。
- **教训**：只挑终态条件（`Complete`、`Failed`）判断，现在的 go-template 就是这么写的；依赖真实 API 行为的代码，本机的假环境测不出来，必须在集群上跑。

## 16. 三件短的事

### 16.1 入口两个 Envoy 是主备，不是分摊（发现 12）

`replicas: 2`、分布在不同节点、`envoyPDB` 的 `maxUnavailable: 1`，现在在 k3s-server 和 k3s-agent-1 上。**不是负载分摊**：MetalLB 二层模式下，入口地址同一时刻只由一个节点应答 ARP；加上 `externalTrafficPolicy: Local`（保留真实的客户端地址，gateway 的按 IP 限流要用），流量只进那个节点上的那一个 Envoy，另一个是备用。持有地址的节点挂了，MetalLB 让另一个有就绪 Envoy 的节点接管地址（免费 ARP），这是故障切换。接管要多久**没有量**，留到阶段 4。

用 `topologySpreadConstraints` 而不是“必须不同节点”的反亲和（`gateway.yaml` 的注释）：滚动更新时多出来的 Pod 可以落到第三个节点，不会因为两个节点各有一个而卡住。

### 16.2 e2e 对带 `Retry-After` 的 503 怎么办（发现 13）

`TestCancelReturnsStockOnce` 在 CI 上失败过一次：轮询订单时收到 503 `STOCK_GATE_UNAVAILABLE`（订单还没落库，展示读又没占到读闸门的名额）。这个 503 是服务有意的答复，**说不清状态时不猜 404**，并带 `Retry-After`。所以改的是客户端：`getOrder` 对带 `Retry-After` 的 503 重试，最多 10 秒，别的非 200 仍然立刻失败。没有调大名额：那只是让测试碰不到这条路径。

和项目的重试规则一致：读是幂等的，重试放在调用链最外层的客户端，由服务端用 `Retry-After` 告诉它什么时候再来。

### 16.3 重启宿主机后全部自己恢复（发现 18）

直接 `systemctl reboot`，不做别的。API 第 32 秒断开，第 172 秒恢复；第 240 秒检查时三个节点 Ready、39 个 Pod 全部就绪，无人干预。核对：MySQL 主从关系不变、复制线程在跑；Redis Cluster `cluster_state:ok`、配对不变；Redis 主从经 Sentinel 切了一次（主库从 `redis-gate-1` 变成 `redis-gate-0`），复制正常；Kafka 无落后副本；七个 PDB、etcd 定时器、入口通路正常；重启后手工触发的 MySQL 备份成功；内核日志里没有信号被拒绝（AppArmor 的修法在重启后仍生效）。2a 的空集群是 104 秒，多出来的时间没有拆开量；“API 一回来 Pod 状态还是重启前的旧值”，第一版计时不可信，**只能说不超过 240 秒**。

## 17. 没做的和待办

诚实地说清楚（设计 10 的“还没做的”、设计 11）：

- **备份**：没有按时间点恢复（binlog 不归档，只能恢复到备份时刻）；备份失败没有告警，阶段 3 接监控时加；备份脚本和 MySQL 共用一个 ConfigMap，改它会滚动重启 MySQL；备份和演练的数据是假的，恢复耗时、备份耗时要等 2c 灌数据后重量；有流量时的比对要用 GTID 加对账工具（2c）。
- **etcd**：server 虚拟机整个没了、换机器用快照加 token 恢复，没有演练，`make cluster-up` 不会从快照恢复。
- **切换**：主库故障时的切换没有演练（阶段 4）；服务连接池遇到 1290 和断连后的恢复、CDC 换源（2c）；手册第二节追平判据的缺口（13.5）。
- 入口 Envoy 故障后地址接管要多久：没量（阶段 4）。
- “停掉两个 broker 时写入被拒绝”没有测。
- Redis 主从切换时闸门丢数据的实验、`min-replicas-to-write`：阶段 4。第 2.3 节的预期**没有实验验证**。
- Sentinel 的“旧主回来得比切换还快”：现象量到了，没有做处理。
- 不在 2b 做：MySQL 的自动故障切换、`mysql-main` 的从库、按时间点恢复、中间件的指标导出（阶段 3）、网络策略（2d）、业务服务上集群（2c）。
- `redisx` 的三种模式和限流的集成测试要手工起本机 profile，**CI 里现在不跑**。
- MySQL 每个实例 1 Gi 是否够，要到 2c 有数据之后再看。
- etcd 的 fsync p99 仍然高于 10 毫秒的参考线，原因没查（2a 留下的）。

## 18. 面试时怎么串起来讲

一个三分钟的讲法：

1. **先看访问方式再选拓扑**：库存闸门的脚本改一组名字各异的键，Cluster 上跨槽、hash tag 只能把它们挤在一个槽里，等于没分片，所以闸门用主从加 Sentinel，缓存和限流用 Cluster（第 1 节）。
2. **代价说出口**：限流在 Cluster 下“一个都不扣”变成“可能白扣一个”，偏严不偏松（第 4 节）；`redisx` 用一个地址字符串表达三种模式，但语义没有统一（第 5 节）。
3. **故障实测里的共同点**：Sentinel 旧主回来比切换快、Cluster 主库重启比超时快数据清空，都是“Pod 重启速度快于故障检测”（第 2、3 节）。
4. **MySQL 为什么手写**：拓扑和已验证的读写分离、CDC、扩容对得上；阶段 4 要练切换；StatefulSet 的三样东西。再讲角色不能记在标签上这个设计修正（第 6 节）。
5. **一次完整的排查**：AppArmor。排除自己写的东西、对照、内核日志、标签、内核开关、验证、写进 cloud-init 重建；教训是装完要看内核日志（第 8 节）。
6. **资源账**：Burstable 加 PriorityClass、三个一组的中间件三节点各一个、实测的内存和 CNI 的决定（第 9、10 节）。
7. **备份要演练，演练要比内容**：逻辑备份、从库上做、最小权限；缺权限时退出码是 0 但少了东西，演练里比对象个数才发现；半截的备份靠临时名字加校验加原子改名；有写入时比不了是演练的边界（第 11、12 节）。
8. **切换的顺序是被三个事实逼出来的**：旧主只读、等追平、提升、改指向；实测中断 5 秒、没有写到错的地方、行数对得上；故障切换会丢什么、用 `GTID_SUBTRACT` 找出来；旧主回来要先让它只读（第 13 节）。
9. **etcd 恢复的是集群认为应该有什么，不是数据**；token 一起备份、soft 挂载、从 NAS 的副本恢复才算演练了备份（第 14 节）。
10. **“碰运气成立的性质要有东西核对”**：Redis 主从同节点、PDB 一直允许 0 个中断、Job 的两个终态条件，三个故事都是“看起来都正常，其实有问题，用真实的检查才发现”，和 AppArmor 那次的教训同源（第 15 节）。
11. **没做的**：按时间点恢复、换机器恢复、主库故障切换、两个 broker 的实验、入口接管时间（第 17 节）。

## 19. 面试官可能追问的短问题

- **Sentinel 切换多久？** 实测 6.9 秒，其中 `down-after-milliseconds` 是 5 秒（第 2.1 节）。
- **切换时会不会丢数据？** 会有窗口，异步复制；预期靠落库时的 MySQL 预占和 `reconcile` 兜底，没有做实验（第 2.3 节）。
- **Cluster 里为什么不能用多键的 Lua 脚本？** 跨槽报 `CROSSSLOT`，要用 hash tag 把键绑到同一个槽（通用知识）；本项目的闸门脚本还没通过 `KEYS` 声明键（第 1.1 节）。
- **Pod 重建后 Cluster 的节点怎么找到彼此？** 域名通告加启动时按域名改写 `nodes.conf` 里的 IP（第 3.2 节）。
- **Redis Cluster 的主从为什么会在同一个节点？** `--cluster create` 只按地址避让，不知道节点；用 `CLUSTER REPLICATE` 只改从库挂谁，不是切换（第 15.1 节）。
- **StatefulSet 的 Pod 重建后数据在哪？** 在同一个节点的 local-path 卷上，所以 Pod 不能漂移（第 9.5 节）。
- **为什么 liveness 不检查复制？** 重启修不好复制，只会让从库反复重启（第 6.6 节）。
- **`min.insync.replicas` 2 加 `acks=all` 是什么意思？** 挂一个照常，挂两个拒绝写（第 7.3 节）。
- **为什么 Kafka 是 4.3.2？** Strimzi 1.3.1 只支持到 4.3.2，本机是 4.4.0，是对“选最新稳定版”的偏离（第 7.4 节）。
- **Guaranteed 和 Burstable 的区别？** 看 CPU 和内存的 requests 是否都等于 limits；本项目内存相等、CPU 不设上限，所以是 Burstable，用 PriorityClass 补（第 9 节）。
- **为什么不上 Cilium？** 剩余内存约 1.9 Gi，Cilium 估计多用 1 到 1.5 GB（没实测），且决定时间点要在灌数据之前（第 10.2 节）。
- **PDB 能防住节点宕机吗？** 不能，只约束自愿驱逐（通用知识）。PDB 选中了 Job 的 Pod 会怎样？算不出应有的 Pod 数，允许中断数停在 0，drain 卡住（第 15.2 节）。
- **`--single-transaction` 为什么还要全局读锁？** 对齐快照、位点和 GTID（第 11.1 节）。`--source-data` 和 `--dump-replica`？后者停 SQL 线程还要更大的权限（第 11.2 节）。
- **恢复出来的实例靠什么续接复制？** dump 开头的 `GTID_PURGED` 加 `SOURCE_AUTO_POSITION`（第 11.2 节）。
- **备份账号权限怎么定？缺了会怎样？** 逐个去掉试；缺 `SHOW_ROUTINE`、`TRIGGER` 悄悄少导，演练比对象个数发现（第 11.3 节）。
- **怎么保证半截的备份不被当成备份？为什么先写新的再删旧的？** 临时名字、解压校验、原子改名；先删后写失败就一份都没有（第 11.4 节）。
- **恢复演练比什么？有写入时为什么比不了？** 校验和、GTID、逐表行数和 `CHECKSUM TABLE`、对象个数；备份是过去的时刻（第 12 节）。
- **切换的顺序为什么是只读、追平、提升、改指向？Service 改了指向旧连接为什么不跟着走？** 第 13.1、13.2 节。
- **主库故障会丢什么、怎么知道丢了哪些？旧主回来为什么危险？怎么让它起来就只读？** `GTID_SUBTRACT`；可写会分叉；`SET PERSIST`（第 13.4、13.5 节）。
- **etcd 快照为什么连 token 一起备份？定时器为什么在节点上？NFS 用 hard 还是 soft？** 第 14.1 节。
- **从快照恢复之后，快照之后才建的对象怎么样？停控制面时 Pod 为什么不受影响？** 第 14.3 节。
- **两个入口副本是分摊吗？** 不是，主备；MetalLB 二层加 `Local`（第 16.1 节）。
- **客户端怎么对待带 `Retry-After` 的 503？** 重试、有上限，别的错误立刻失败（第 16.2 节）。
- **Job 的终态条件有哪些？** `Complete`、`Failed`；1.31 起成功时还有 `SuccessCriteriaMet`，失败时还有 `FailureTarget`（第 15.3 节）。
