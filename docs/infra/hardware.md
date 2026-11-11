# 设备与角色

## 清单

| 设备 | 配置 | 架构 | 角色 |
|---|---|---|---|
| MacBook Pro | M5，32GB，1TB | arm64 | 开发机；OrbStack 跑本地 compose 和单节点 K8s 做快速验证；集群操作端 |
| Ubuntu | AMD 3900X（12 核 24 线程），32GB，512GB，GTX1060 | x86_64 | 主集群宿主机：KVM 虚拟机跑 k3s |
| Mac mini | M4，16GB，256GB | arm64 | 压测发起机（k6）；诊断 Agent 运行机 |
| DS218+ | 12TB | x86_64 | NFS、镜像仓库存储、MySQL 备份、监控长期存储 |
| RT-AC5300 | — | — | 局域网路由 |
| R2S | — | — | 代理，解决 GitHub、Docker 镜像拉取 |

## 分配理由

- **集群放在 Ubuntu**：核心数最多，x86_64 镜像兼容性最好。用虚拟机而不是单机多容器节点，是为了能真实地关掉一个节点、切断节点间网络。
- **压测机与集群分离**：压测工具和被测系统抢资源会让数据失真，所以由 Mac mini 发起。
- **GTX1060 不用于模型推理**：6GB 显存跑不了对诊断有用的模型，Agent 使用 Claude API。
- **Mac mini 不加入集群**：混合架构会让每个镜像都要多架构构建，增加的麻烦与学习目标无关。

## Ubuntu 虚拟机规划（初稿，阶段 2a 按实际调整）

| 虚拟机 | vCPU | 内存 | 磁盘 | 角色 |
|---|---|---|---|---|
| k3s-server | 4 | 6GB | 60GB | 控制面，也承载部分工作负载 |
| k3s-agent-1 | 8 | 11GB | 150GB | 工作节点 |
| k3s-agent-2 | 8 | 11GB | 150GB | 工作节点 |
| 宿主机保留 | — | 4GB | — | 系统与 KVM 开销 |

## 内存预算（粗估）

32GB 是整个项目最紧的资源，总可分配约 28GB：

| 组件 | 预估 |
|---|---|
| K8s 系统组件 | 2GB |
| MySQL：2 分片 × 一主一从 | 4GB |
| Redis Cluster：6 节点 | 1.5GB |
| Kafka：3 broker | 4.5GB |
| Prometheus、Grafana、Alertmanager、Loki | 4GB |
| SkyWalking OAP + BanyanDB | 4GB |
| OpenTelemetry Collector | 0.5GB |
| Chaos Mesh | 0.5GB |
| 业务服务（多副本，含 CDC） | 3GB |
| 归档 MySQL 实例 | 1GB |
| 余量 | 约 3GB |

不够时的处理顺序：缩短监控数据保留期并下调采样率 → 把 SkyWalking 或 Loki 的存储挪到 NAS 或 Mac mini → 降低中间件副本规格。

## 网络

待阶段 2a 填写：各设备 IP、虚拟机网段、MetalLB 地址池、代理配置方式。

## 开机需求

| 阶段 | 需要开机的设备 |
|---|---|
| 0、1 | MacBook |
| 2 | MacBook、Ubuntu、NAS |
| 3 及以后 | 全部 |
