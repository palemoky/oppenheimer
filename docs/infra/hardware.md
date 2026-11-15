# 设备与角色

## 清单

| 设备 | 配置 | 架构 | 角色 |
|---|---|---|---|
| MacBook Pro | M5，32GB，1TB | arm64 | 开发机；OrbStack 跑本地 compose 和单节点 K8s 做快速验证；集群操作端 |
| Ubuntu | AMD 3900X（12 核 24 线程），32GB，1TB 系统盘（970 EVO Plus，2026-11-15 由 250GB 迁移而来）加 500GB 的 Windows 盘，两个 M.2 槽都占着，GTX 1660（6GB） | x86_64 | 主集群宿主机：KVM 虚拟机跑 k3s |
| Mac mini | M4，16GB，256GB | arm64 | 压测发起机（k6）；诊断 Agent 运行机 |
| DS218+ | 12TB | x86_64 | NFS、镜像仓库存储、MySQL 备份、监控长期存储 |
| RT-AC5300 | — | — | 局域网路由 |
| R2S | — | — | 代理，解决 GitHub、Docker 镜像拉取 |

## 分配理由

- **集群放在 Ubuntu**：核心数最多，x86_64 镜像兼容性最好。用虚拟机而不是单机多容器节点，是为了能真实地关掉一个节点、切断节点间网络。
- **压测机与集群分离**：压测工具和被测系统抢资源会让数据失真，所以由 Mac mini 发起。
- **GTX 1660 不用于模型推理**：6GB 显存跑不了对诊断有用的模型，Agent 使用 Claude API。
- **Mac mini 不加入集群**：混合架构会让每个镜像都要多架构构建，增加的麻烦与学习目标无关。

## Ubuntu 虚拟机（2026-11-15 建成，定义在 `scripts/cluster/cluster.env`）

| 虚拟机 | 地址 | vCPU | 内存 | 磁盘 | 角色 |
|---|---|---|---|---|---|
| k3s-server | 192.168.50.40 | 4 | 6GB | 60GB | 控制面，也承载部分工作负载 |
| k3s-agent-1 | 192.168.50.41 | 8 | 11GB | 150GB | 工作节点 |
| k3s-agent-2 | 192.168.50.42 | 8 | 11GB | 150GB | 工作节点 |
| 宿主机保留 | 192.168.50.19 | — | 约 3GB | — | 系统与 KVM 开销 |

系统是 Ubuntu 26.04.1 的 cloud image，用户 `ubuntu`（免密 sudo，只能用 MacBook 的 ssh 密钥登录）。磁盘是宿主机 `/var/lib/libvirt/images`（600G 的逻辑卷 `ubuntu-vg/vmpool`）下按需增长的 qcow2，三台刚建好合计占 7.5G。vCPU 合计 20，宿主机 24 线程，没有超分；KVM 不预占内存。

## 内存预算（粗估）

32GB 是整个项目最紧的资源，总可分配约 28GB：

| 组件 | 预估 |
|---|---|
| K8s 系统组件 | 2GB |
| MySQL：2 分片 × 一主一从 | 4GB |
| Redis Cluster：6 节点 | 1.5GB |
| Kafka：3 broker | 4.5GB |
| Prometheus、Grafana、Alertmanager、Loki | 4GB |
| Tempo（单体模式，链路；ADR-0024 之前这里是 SkyWalking OAP 加 BanyanDB 的 4GB） | 1GB（粗估，阶段 3a 实测） |
| OpenTelemetry Collector | 0.5GB |
| Chaos Mesh | 0.5GB |
| 业务服务（多副本，含 CDC） | 3GB |
| 归档 MySQL 实例 | 1GB |
| 余量 | 约 6GB |

不够时的处理顺序：缩短监控数据保留期并下调采样率 → 把 Tempo 或 Loki 的存储挪到 NAS 或 Mac mini → 降低中间件副本规格。

## 网络

局域网是 192.168.50.0/24。网关、DNS、DHCP 都在 R2S（192.168.50.2，OpenWrt），不在 RT-AC5300 上。DHCP 池是 .100 到 .249，集群用池外的地址（定义在 `scripts/cluster/cluster.env`）：

| 地址 | 用途 |
|---|---|
| 192.168.50.2 | R2S：网关、DNS、出网代理 |
| 192.168.50.3 | DS218+（`ssh nas`，端口 5022） |
| 192.168.50.14 | 3900X 的 Wi-Fi，管理用的备用通道（`ssh ubt`） |
| 192.168.50.19 | 3900X 的网桥 `br0`（有线；R2S 上按网卡 MAC 绑定了同一个地址） |
| 192.168.50.40 到 .42 | k3s-server、k3s-agent-1、k3s-agent-2 |
| 192.168.50.43、.44 | 已在代理白名单里，留给以后加节点 |
| 192.168.50.50 到 .59 | MetalLB 地址池（计划；只接收进来的访问，不需要进代理白名单） |

.1 到 .32 是 R2S 上给各设备的静态绑定，不要占用。

- **虚拟机用桥接**：网卡接在宿主机的 `br0` 上，直接拿局域网地址。Wi-Fi 网卡不能接进网桥，所以宿主机必须走有线。
- **出网要进 R2S 的白名单**：R2S 上的代理（nikki）是 fake-ip 模式，带按 IP 和 MAC 的局域网白名单。不在名单里的设备解析域名拿到的是 198.18.0.0/16 的假地址，但连接不被代理接管，表现是所有按域名的出网都连不上，而 ping 真实 IP 是通的。新加的设备（宿主机的有线网卡、每台虚拟机）都要进名单。
- 2026-11-15 实测有线链路只有 100 Mbit/s，换过一根网线后仍然如此。内核日志是 `Downshift occurred from negotiated speed 1Gbps to actual speed 100Mbps`：两端都同意千兆，但千兆要用的四对线里有不通的，网卡退到只用两对线的百兆。让网卡只通告千兆时链路起不来，别的电脑接同一个路由器口是千兆，所以是板载网口的硬件问题。准备加一块 PCIe 网卡（装好后改 `cluster.env` 的 `HOST_NIC`，重跑 `host-setup.sh`）；不挡建集群，阶段 3 压测之前要解决。

## 开机需求

| 阶段 | 需要开机的设备 |
|---|---|
| 0、1 | MacBook |
| 2 | MacBook、Ubuntu、NAS |
| 3 及以后 | 全部 |
