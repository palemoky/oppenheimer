# ADR-0027：集群上的 MySQL 用手写的 StatefulSet，不用 Operator

- 状态：已采纳
- 日期：2026-11-15

## 背景

阶段 2b 要把 MySQL 搬上集群，计划里写的是“对比 Operator 与 StatefulSet 手写，选一种并写 ADR”。

本机（compose）上的拓扑是 1c 定下来并验证过的：两个订单分片各一主一从，GTID 异步复制；一个共享实例放 inventory、payment、商家索引和订单冷库。围绕这个拓扑已经有的东西：按 GTID 等从库追上的读写分离（ADR-0013）、从从库读 binlog 的 CDC 和它的行版本号（ADR-0014）、用 CLONE 做的扩容（ADR-0015）。

## 决定

用一个手写的 Helm chart（`deploy/charts/mysql`）部署：每个分片一个 2 副本的 StatefulSet，序号 0 是初始的主库；从库用 init 容器经 CLONE 初始化并按 GTID 建立复制；主从各有一个 Service，按 Pod 名选：主库是哪个序号写在 values 里（`primaryOrdinal`）。故障切换是人工的，最后一步是改这个值再 `helm upgrade`。

## 理由

- **拓扑要和已经验证过的一致。** Oracle 的 MySQL Operator 管的是 InnoDB Cluster（组复制加 MySQL Router），每个分片至少 3 个实例。换成它，读写分离、CDC、扩容都要对着组复制重新验证，而这和 2b 的目标（熟悉 Kubernetes 上有状态服务的部署）无关；内存上是 6 个实例对 4 个。
- **阶段 4 要练的是 Operator 替你做掉的事。** 主库挂了之后提升从库、改连接、CDC 换源、旧主回来怎么处理，是计划内的故障实验（ADR-0014 的后果一节记着其中一项）。自动切换会把这些过程盖住。
- **学习价值。** 手写一遍才讲得清 StatefulSet 的稳定网络标识、按序号启动、卷和 Pod 的绑定各解决什么，也才讲得清 Operator 在这之上多做了什么。Operator 模式本身由 Kafka（Strimzi）来练。

## 备选方案

- **Oracle MySQL Operator**：见上。它的最新版本和支持的 MySQL 版本在做决定时没有查到。
- **Percona Operator for MySQL**：支持异步复制，但部署的是 Percona Server，不是这里用的 MySQL 9.7；没有细查。
- **Vitess**：分片、路由、切换都替你做。等于把 1c 整个换掉。
- **不上集群，MySQL 留在宿主机或虚拟机上**：生产里数据库不进 Kubernetes 是常见的做法。没选是因为阶段 4 的故障注入（Chaos Mesh）作用在 Pod 上。

## 后果

- 复制的搭建、备份、切换都是自己的脚本，没有控制器持续纠正：复制断了谁发现、切换做了一半怎么办，都要自己保证并测试。
- **主从关系记在 values 里，不记在 Pod 上。** 最初的设计是给 Pod 打 `role` 标签让 Service 去选，实现时发现不行：标签在 Pod 对象上，Pod 一重建就没了，主库的 Service 随即没有端点；切换之后旧主的 Pod 重建，按序号重新打标签会把它标回主库（设计文档 2b 第 10 节的发现 2）。现在 values 里的 `primaryOrdinal` 是事实来源，它和实例的实际状态不一致时（比如切换做了一半），服务会写到从库上并被 `super_read_only` 拒绝，部署脚本的只读核对也会报异常。这是有意保留的失败方式：宁可报错也不要写到错的地方。
- 没有自动故障切换，主库不可用时订单的写入一直失败到人工处理为止（受理端有落库健康检查，会拒绝新订单，ADR-0021）。
- 重新评估的条件：阶段 4 把手工切换练熟之后，可以换一个分片试 Operator 做对比。
