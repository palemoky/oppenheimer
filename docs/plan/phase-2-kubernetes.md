# 阶段 2：Kubernetes 集群与部署

**目标**：把系统搬到多节点 K8s 集群上运行，过程中熟悉 K8s 核心机制和生态组件。
**设备**：Ubuntu 3900X（主集群）、DS218+（存储与备份）、MacBook（操作端）
**开工前**：请主人打开 Ubuntu 和 NAS；MacBook 安装 helm。

## 2a 集群搭建

- [ ] Ubuntu 上用 KVM 创建 3 台虚拟机，资源分配写入 `docs/infra/hardware.md`
- [ ] 安装 k3s：1 server + 2 agent（见 ADR-0001）
- [ ] MacBook 配置 kubeconfig，能远程操作集群
- [ ] 镜像仓库：自建 registry（数据放 NAS），集群配置拉取
- [ ] 镜像拉取代理经 R2S，验证常用镜像源可用
- [ ] 存储：本地盘用 local-path，备份与共享数据用 NAS 的 NFS
- [ ] Ingress 与负载均衡：ingress-nginx + MetalLB（或 k3s 自带方案，选定后写 ADR）
- [ ] 建集群与重建集群的步骤脚本化

## 2b 中间件上集群

- [ ] MySQL：2 分片 × 一主一从（对比 Operator 与 StatefulSet 手写，选一种并写 ADR）
- [ ] Redis Cluster：3 主 3 从
- [ ] Kafka：Strimzi Operator，KRaft 模式 3 broker
- [ ] 各中间件的资源限额、反亲和、PDB、持久化
- [ ] MySQL 定时备份到 NAS，并演练一次恢复

## 2c 服务上集群

- [ ] 多阶段 Dockerfile，构建 `linux/amd64` 镜像
- [ ] 每个服务一个 Helm chart（或一个公共 chart 加 values）
- [ ] 探针：startup、liveness、readiness，与优雅启停配合
- [ ] 配置与密钥：ConfigMap、Secret，配置变更如何生效
- [ ] 资源 requests 与 limits、QoS 等级
- [ ] HPA：基于 CPU，之后基于自定义指标（QPS）
- [ ] 滚动发布策略，验证发布过程零报错
- [ ] gRPC 在 K8s 里的负载均衡问题（长连接不均衡）及解决
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
