# 阶段 2：Kubernetes 集群与部署

**目标**：把系统搬到多节点 K8s 集群上运行，过程中熟悉 K8s 核心机制和生态组件。
**设备**：Ubuntu 3900X（主集群）、DS218+（存储与备份）、MacBook（操作端）
**开工前**：请主人打开 Ubuntu 和 NAS；MacBook 安装 helm。

## 2a 集群搭建

- [x] 宿主机准备：装 KVM 和 libvirt，有线网卡接到网桥 `br0`（`scripts/cluster/host-setup.sh`）；换上 1TB 的盘放虚拟机磁盘
- [x] 设计文档 [design/2a-cluster.md](../design/2a-cluster.md)，主人已确认
- [x] Ubuntu 上用 KVM 创建 3 台虚拟机（`scripts/cluster/vm.sh`），资源分配写入 `docs/infra/hardware.md`
- [x] 安装 k3s：1 server + 2 agent，内嵌 etcd，CNI 先用 flannel（见 ADR-0001、ADR-0026）
- [x] MacBook 配置 kubeconfig，能远程操作集群
- [x] 镜像仓库：自建 registry（数据放 NAS），集群配置拉取
- [x] 镜像拉取代理经 R2S，验证常用镜像源可用
- [x] 存储：本地盘用 local-path，备份与共享数据用 NAS 的 NFS
- [x] 入口与负载均衡：Envoy Gateway（Gateway API）加 MetalLB，禁用 k3s 自带的 Traefik 和 ServiceLB（ADR-0025）
- [x] 建集群与重建集群的步骤脚本化（`make cluster-up`，重建演练 5 分 28 秒）
- [x] 重启宿主机后集群自己恢复（104 秒）
- [x] etcd 的 fsync 延迟偏高，2b 上中间件后复查：没有变差，p99 仍高于 10 毫秒的参考线，原因没有查（设计文档 2b 第 10 节“量到的数”）

## 2b 中间件上集群

- [x] 节点的 AppArmor 拦掉容器内的信号：`vm.sh` 里关掉 `apparmor_restrict_unprivileged_unconfined`，重建集群（设计文档 2b 第 10 节的发现 1）
- [x] MySQL：2 分片 × 一主一从加一个共享实例，手写 StatefulSet（ADR-0027，设计文档 [2b-middleware.md](../design/2b-middleware.md)）
- [x] Redis：闸门和锁用一主一从加 Sentinel，缓存和限流用 Redis Cluster 3 主 3 从；`pkg/redisx` 支持三种模式，`pkg/ratelimit` 适配 Cluster
- [x] Kafka：Strimzi Operator，KRaft 模式 3 broker（Kafka 4.3.2，Strimzi 还不支持 4.4）
- [x] 各中间件的资源限额、反亲和、PDB、持久化、PriorityClass；Redis Cluster 的主从配对由脚本核对并修正（设计文档 2b 第 10 节的发现 10）
- [x] 入口的 Envoy 改成 2 个副本并分散到两个节点，加 PDB
- [x] MySQL 定时备份到 NAS，并演练一次恢复（`make middleware-backup`、`make mysql-restore-drill`；数据量是假的，2c 灌数据后重量恢复耗时）
- [x] MySQL 主从切换手册（[infra/mysql-switchover.md](../infra/mysql-switchover.md)），计划内切换演练过两趟；主库故障的切换留到阶段 4
- [x] etcd 快照定时存到 NAS（server 节点上的 systemd 定时器，连同 token）
- [x] 演练一次从 etcd 快照恢复：用 NAS 上的副本原地恢复，控制面中断 8 秒，工作负载不受影响（设计文档 2b 第 10 节的发现 17）；换机器恢复没有演练
- [x] 重启宿主机后全部中间件自己恢复（240 秒内，没有人工干预，发现 18）
- [x] 中间件部署完、灌数据之前量各节点的内存，改定 CNI：保留 flannel（ADR-0026）

## 2c 服务上集群

- [x] 设计文档 [design/2c-services.md](../design/2c-services.md)，主人已确认（2026-11-15）
- [x] 节点号在多副本下的分配：没设 `WORKER_ID` 时从本服务的 MySQL 租（ADR-0028，`pkg/idgen` 的租约；本机 `make lint test e2e` 通过；集群上四个 Pod 各租到不同的号，滚动时新旧各占一个。租约所在的库不可用时受理被拒绝再恢复、`kill -9` 之后 30 秒才能被租走，这两种没有在集群上验证）
- [x] 多阶段 Dockerfile，构建 `linux/amd64` 镜像（`deploy/docker/Dockerfile`、`make images`；七个镜像已推到集群仓库并在节点上跑过）
- [x] 一个公共 chart 加每个服务一份 values（`deploy/charts/service`、`deploy/cluster/services/`）；六个服务已部署，从入口走通下单到支付到商家索引的整条链路
- [x] 探针：startup、liveness、readiness，与优雅启停配合（`DRAIN_DELAY` 15 秒，按实测的端点传播时间定的：最长 9.5 秒）
- [x] 配置与密钥：ConfigMap 经 `envFrom`，密码从 Secret 取并用变量展开拼 DSN，配置变了靠校验和注解触发滚动（设计文档 2c 的 7.2）
- [x] 资源 requests 与 limits、QoS 等级（Burstable）；requests 已按轻载下 `kubectl top` 的实际用量调低，阶段 3 压测时再校
- [x] HPA：基于 CPU（gateway、order，2 到 4 个副本，目标 70%）。约 810 请求/秒时两个服务都从 2 扩到 4，新 Pod 约 5 秒就绪并均分到流量；从发压到第一次扩容约 55 秒，主要是指标的滞后（设计文档 2c 第 13 节的第六步）。扩容瞬间有 14 个查订单的 503 没有修
- [ ] HPA：基于自定义指标（QPS），挪到阶段 3a，等有了 Prometheus
- [x] 滚动发布策略，验证发布过程零报错：六个服务整体滚动两次共 28500 个请求 0 失败，`DRAIN_DELAY=0` 的对照每删一个 Pod 失败 15 个左右（`scripts/cluster/probe.py`）
- [x] gRPC 在 K8s 里的负载均衡问题（长连接不均衡）及解决：直连 ClusterIP 时 gateway 的请求全部落在一个 inventory Pod 上，经边车时两个 Pod 各一半（设计文档 2c 第 13 节）
- [x] 在集群上重做 1b 的吞吐对比（`make cluster-bench`）：**没有数量级的提升**。1a 下单约 400 单/秒；1b 落库约 200 单/秒（1a 的一半），受理在默认背压下约 365 单/秒（被压回落库速度加一个队列的量），关掉背压约 1900 到 2500 单/秒（1a 的 5 到 6 倍，受集群大小限制）。短板是落库，原因没有查，留给阶段 3（设计文档 2c 第 13 节的第七步）
- [x] 评估 Kafka 消费者的静态成员（`group.instance.id`，用 Pod 名）：**结论是不做**。滚动 payment 时每个分区只停 0.5 到 3.2 秒；franz-go 的静态成员关闭时不离组，Deployment 的新 Pod 又是新名字，用了反而要等会话超时（设计文档 2c 第 13 节的第七步，ADR-0007 的补充）
- [x] 阶段 1 的端到端测试在集群上通过：`make cluster-e2e`，19 个用例，流量经入口的 Envoy（设计文档 2c 第 13 节的第五步）。一致性测试见下一条
- [x] 一致性测试在集群上通过：`make cluster-chaos`，压测期间每 5 秒随机删一个业务 Pod，静止后 `verify` 和 `recon` 无差异；正常删除（23 个 Pod，客户端 0 个 5xx）和强制删除（22 个 Pod，29 个 5xx）各通过一次。查出并修掉 `kafkax` 启动时查死信 topic 的缺陷（三个 broker 才出现）
- [x] 一键部署：`make cluster-up && make middleware-up && make deploy && make deploy-seed`，从删掉三台虚拟机到整套系统运行 17 分 34 秒，没有人工步骤；重建后没有容器重启，`make cluster-e2e` 19 个用例通过（设计文档 2c 第 13 节）。Argo CD 在 2d

## 2d 生态组件巡礼（按需）

- [ ] NetworkPolicy：限制服务间访问
- [ ] cert-manager：证书
- [ ] Argo CD：GitOps
- [ ] 灰度发布：Argo Rollouts 或 Ingress 金丝雀
- [ ] 服务网格：把服务之间手写的 Envoy 边车迁到 Istio，熔断、并发上限、路由超时逐项对照 1d 的实测重新验证（设计文档 [2d-mesh.md](../design/2d-mesh.md)，ADR-0029；主人 2026-11-15 定的，不是“按需”）。**已做完**：Istio 1.32.0 边车模式，六个服务不停机地切了过去（探针 18770 个请求 0 失败）；`make cluster-fault` 八个场景通过，数值和 1d 对得上；`make cluster-e2e`、`make cluster-chaos` 通过；mTLS 严格模式加按身份的访问控制。**还差**：端点传播时间和 `DRAIN_DELAY`、HPA 扩容点、延迟对比没有重量；chart 里手写边车的分支还没删；没有从空集群重建验证过（设计第 11 节）
- [x] 服务之间的访问控制：用网格的 mTLS 严格模式加 `AuthorizationPolicy` 做了（按身份，只管 gRPC 端口），代替了 NetworkPolicy 那一条想防的大部分；gateway 的 HTTP 入口仍然没有限制

## 验收标准

- 从空虚拟机到整套系统运行，按文档和脚本可重复完成。
- 阶段 1 的端到端测试和一致性测试对着集群入口全部通过。
- 滚动发布期间持续请求无失败。
- 删除任意一个业务 Pod，服务自动恢复且请求基本无感知。

## 面试考点

Pod 生命周期与探针；Service、kube-proxy、Ingress 的流量路径；调度（亲和、污点、优先级）；requests 与 limits 对调度和 OOM 的影响；StatefulSet 与 Deployment 的区别；Operator 模式；HPA 工作原理；滚动发布如何做到零中断；gRPC 负载均衡；etcd 与控制面组件职责。

2c 补充（设计文档 [2c-services.md](../design/2c-services.md)，ADR-0028）：

- 雪花算法的节点号在多副本下怎么分配；为什么“持有者在本地判断租约有效”不够（时钟、进程停顿），怎么用“每一任一段互不重叠的区间”把对时钟的依赖去掉；这和 fencing token 是什么关系。
- 为什么不用 StatefulSet 的序号当节点号：它的“同一身份最多一个”在节点失联时的代价。
- Pod 被删时 SIGTERM 和“从端点里摘掉”是同时开始的；摘流的等待该多长、怎么量；为什么不用 `preStop` 里 sleep。
- 原生 sidecar（`initContainers` 加 `restartPolicy: Always`）解决的启动和停止顺序问题。
- gRPC 长连接在 ClusterIP 后面为什么不均衡，三种解法（客户端按请求均衡、中间放七层代理、服务网格）；headless Service 是什么。
- 端点的变化经 DNS 传到调用方要多久，和经 xDS 下发比差在哪。
- `maxSurge`、`maxUnavailable`、`minReadySeconds`、PDB 各管什么；滚动发布期间新旧版本共存对接口和表结构的要求。
- 环境变量形式的配置为什么改了 ConfigMap 不生效，校验和注解的做法。
- requests 和 limits 怎么定，`GOMEMLIMIT` 为什么要设，CPU 为什么不设 limits。
- 多阶段构建、交叉编译对模拟、distroless 的取舍、tag 为什么不能重用。
- HPA 的算式和它看到的是什么：利用率对 requests 算、按整个 Pod（边车也在内）、指标滞后近一分钟所以是分几步扩上去的；为什么开了 HPA 就不能在 Deployment 里写 `replicas`，去掉这个字段的那一次发布会发生什么；缩容的稳定窗口。
- 扩出来的新 Pod 一就绪就拿到满额流量：冷连接池、没有慢启动；预热和 Envoy 慢启动各解决什么。
- 滚动发布时一个 Kafka 分区会停多久，由什么决定（增量再平衡只挪要挪的分区；成员主动离组后其他成员要到下一次心跳才知道）；静态成员帮得上和帮倒忙的情形。
- 带 `-race` 的测试二进制为什么不能放进 static 镜像，交叉编译 cgo 要什么。
- 集群里的 Pod 访问 LoadBalancer 的地址走的是哪条路（kube-proxy 就地转发，不经过外部的负载均衡器）。
- 两个 CDC 实例读同一个 MySQL 为什么 server id 必须不同。
- 新建的 Kafka topic 为什么“建成功了却查不到”：每个 broker 的元数据缓存各自异步更新，单 broker 的环境测不出来。
- 一致性测试的判定标准为什么是“数据对得上”而不是“零报错”；正常删除和强制删除在客户端看到的差别来自哪里。
- 异步削峰到底提升了什么：受理和落库是两个数；背压把持续过载时的受理压回落库速度，“快”只在突发时成立；压测时怎么认出自己量到的是保护机制的上限而不是系统的能力。
- 做对照实验：没有“去掉保护就失败”的对照，零失败说明不了保护起了作用；对照改过的配置要先恢复干净。

2d 补充（设计文档 [2d-mesh.md](../design/2d-mesh.md)，ADR-0029）：

- 服务网格解决什么：端点发现、按请求的负载均衡、超时、熔断、mTLS 从每个服务的代码或手写配置里拿出来，由控制面统一下发。控制面（istiod）和数据面（边车）各做什么，xDS 的几种资源（LDS、RDS、CDS、EDS）。
- 透明拦截怎么做到的：init 容器在 Pod 的网络命名空间里写 iptables 规则，出站重定向到 15001、入站到 15006；进程不知道边车存在。和“进程自己把地址指到代理”相比，排查时难在哪。
- Istio 的默认重试（2 次，条件里有 gRPC 的 `unavailable`）为什么危险；重试应该放在调用链的哪一层；怎么证明“重试关掉了”（要有打开时的对照）。
- 熔断在 Istio 里是两样东西：连接池上限（`connectionPool`）和异常检测（`outlierDetection`）。默认值哪些要改、为什么（`maxEjectionPercent` 10% 对两个副本是 0 个）；摘除为什么不是立刻生效的。
- 慢下游怎么被熔断：只有路由上的超时到期才算失败；调用方进程自己的超时必须比它长，否则代理永远看不到失败。
- `DestinationRule` 作用在调用方一侧；同一个下游对不同调用方用不同的额度怎么写（`workloadSelector`、`sourceLabels`）。
- Service 的端口名为什么会改变行为（协议识别）；认成 TCP 时会怎样、为什么不报错。
- 边车默认拿到全网格的配置，`Sidecar` 资源收窄可见范围为什么是大集群里必须做的。
- 已有的服务怎么不停机地进网格：`PERMISSIVE` 模式、自动 mTLS 按端点判断、为什么按调用关系的拓扑序“调用方先切”；回退为什么要单独演练。
- mTLS 的身份从哪来（ServiceAccount、SPIFFE）；`PeerAuthentication` 和 `AuthorizationPolicy` 各管什么；ALLOW 策略的语义（有一条 ALLOW 就默认拒绝其余的）和由此带来的应用顺序问题。
- 删掉 Pod 模板里的 `serviceAccountName` 为什么不生效（废弃的同义字段 `serviceAccount`）。
- 边车模式和 ambient 模式的区别，各自把七层的功能放在哪。
- istiod 挂了会怎样：已有的流量、端点变化、新 Pod 的创建（注入 webhook 的失败策略）。
- 原生 sidecar 之前，边车和 Job、和启动停止顺序的老问题是什么。

2b 补充（设计文档 [2b-middleware.md](../design/2b-middleware.md) 第 10 节，手册 [mysql-switchover.md](../infra/mysql-switchover.md)）：

- `mysqldump --single-transaction` 靠什么得到一致的备份，为什么还要短暂的全局读锁；只有逻辑备份、没有 binlog 归档时能恢复到哪个时刻。
- `--source-data` 和 `--dump-replica` 的区别；GTID 下恢复出来的实例靠什么续接复制（`GTID_PURGED`）。
- 备份账号的最小权限怎么定；缺权限时 mysqldump 会悄悄少导东西，怎么发现。
- 怎么保证“半截的备份不被当成备份”（临时名字、校验、改名）；为什么先写新的再删旧的。
- 备份不演练恢复等于没有备份：演练比对什么，有写入时为什么比不了。
- etcd 快照为什么要连 token 一起备份；为什么这个任务放在节点上而不是做成 CronJob；NFS 的 hard 和 soft 各适合什么。
- 主从切换的顺序为什么是“旧主只读、等追平、提升、改指向”；Service 改了指向为什么已有的连接不跟着走；异步复制下主库故障会丢什么、怎么知道丢了哪些（`GTID_SUBTRACT`）。
- 切换之后旧主库回来为什么危险（脑裂），怎么让它起来就是只读的（`SET PERSIST`）。
- Redis Cluster 的主从为什么可能落在同一个节点（`--cluster create` 只按地址避让），`CLUSTER REPLICATE` 和主从切换的区别。
- MetalLB 二层模式下两个入口副本是主备不是分摊；`externalTrafficPolicy: Local` 的影响。
- 从 etcd 快照恢复之后，快照之后才建的对象和它们正在跑的 Pod 会怎样；控制面停了的时候已经在跑的 Pod 为什么不受影响。
- PDB 的 `maxUnavailable` 怎么算出“应该有几个 Pod”（经控制器的 scale 子资源），选中了一个 Job 的 Pod 会怎样。
- Job 的终态条件（`Complete`、`Failed`，以及 1.31 起的 `SuccessCriteriaMet`、`FailureTarget`）。
- 客户端怎么对待带 `Retry-After` 的 503。

2a 补充（设计文档 [2a-cluster.md](../design/2a-cluster.md)）：

- 节点的可分配量怎么算（总量减 `system-reserved`、`kube-reserved`、驱逐阈值），不留预留会怎样。
- Gateway API 的三层资源各管什么，和 Ingress 的区别；ingress-nginx 为什么退役。
- MetalLB 二层模式的原理（ARP 应答、免费 ARP 接管），为什么说它是故障切换而不是负载均衡。
- `externalTrafficPolicy` 的 `Cluster` 和 `Local`：源地址保留、多一跳、对负载均衡器的影响。
- local-path 的 PV 为什么带节点亲和，`WaitForFirstConsumer` 解决什么问题。
- 数据库的数据为什么不放 NFS；NFS 的 squash（root 映射）是什么。
- 镜像仓库放在它所服务的集群里有什么循环依赖，节点上的镜像缓存起什么作用。
- 虚拟机桥接网络里 `br_netfilter` 的作用：为什么 Kubernetes 节点需要它，为什么宿主机上加载它会让虚拟机断网。
- k3s 和 kubeadm 搭出来的集群差在哪（单进程、内嵌组件、数据存储可选）。
