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
- [ ] 演练一次从 etcd 快照恢复（ADR-0026；会重置控制面，动手前和主人说）
- [ ] 重启宿主机后全部中间件自己恢复（重启前和主人说）
- [x] 中间件部署完、灌数据之前量各节点的内存，改定 CNI：保留 flannel（ADR-0026）

## 2c 服务上集群

- [ ] 多阶段 Dockerfile，构建 `linux/amd64` 镜像
- [ ] 每个服务一个 Helm chart（或一个公共 chart 加 values）
- [ ] 探针：startup、liveness、readiness，与优雅启停配合
- [ ] 配置与密钥：ConfigMap、Secret，配置变更如何生效
- [ ] 资源 requests 与 limits、QoS 等级
- [ ] HPA：基于 CPU，之后基于自定义指标（QPS）
- [ ] 滚动发布策略，验证发布过程零报错
- [ ] gRPC 在 K8s 里的负载均衡问题（长连接不均衡）及解决
- [ ] 在集群上重做 1b 的吞吐对比（`scripts/bench.sh`，1a 与 1b 各一遍），给出“下单吞吐相对 1a 是否有数量级提升”的结论。本机的测量疑似受 OrbStack 端口转发丢包干扰（约 23% 的提交卡在 200 毫秒，正好是 TCP 最小重传超时），见 1b 设计文档 7.3.4
- [ ] 评估 Kafka 消费者的静态成员（`group.instance.id`，用 Pod 名）：重启的实例直接接回原来的分区，不触发重平衡
- [ ] 一键部署：`make deploy` 或 Argo CD（可选）

## 2d 生态组件巡礼（按需）

- [ ] NetworkPolicy：限制服务间访问
- [ ] cert-manager：证书
- [ ] Argo CD：GitOps
- [ ] 灰度发布：Argo Rollouts 或 Ingress 金丝雀

## 验收标准

- 从空虚拟机到整套系统运行，按文档和脚本可重复完成。
- 阶段 1 的端到端测试和一致性测试对着集群入口全部通过。
- 滚动发布期间持续请求无失败。
- 删除任意一个业务 Pod，服务自动恢复且请求基本无感知。

## 面试考点

Pod 生命周期与探针；Service、kube-proxy、Ingress 的流量路径；调度（亲和、污点、优先级）；requests 与 limits 对调度和 OOM 的影响；StatefulSet 与 Deployment 的区别；Operator 模式；HPA 工作原理；滚动发布如何做到零中断；gRPC 负载均衡；etcd 与控制面组件职责。

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
