# 设备与角色

只写现状。怎么变成这样的（换盘、换网卡、加内存、挪插槽）在 `docs/PROGRESS.md` 的会话日志里。

## 清单

| 设备 | 配置 | 架构 | 角色 |
|---|---|---|---|
| MacBook Pro | M5，32GB，1TB | arm64 | 备用的开发机和操作端（2026-11-21 之前是主要的）；平时只用来远程连到 Mac mini |
| Ubuntu（3900X） | 12 核 24 线程，MSI MPG X570 GAMING PLUS，48GB，1TB 系统盘加 500GB 的 Windows 盘，GTX 1660 | x86_64 | 集群宿主机：KVM 虚拟机跑 k3s |
| Mac mini | M4，16GB，256GB（`ssh mini`） | arm64 | 开发机；OrbStack 跑本地 compose；集群操作端；压测发起机（k6）；诊断 Agent 运行机 |
| DS218+ | 12TB | x86_64 | NFS、镜像仓库存储、MySQL 备份 |
| R2S | OpenWrt | — | 网关、DNS、DHCP、出网代理 |
| RT-AC5300 | — | — | 交换和 Wi-Fi 接入（口是千兆） |

为什么这样分：

- **集群放在 Ubuntu**：核心数最多，x86_64 镜像兼容性最好。用虚拟机而不是单机多容器节点，是为了能真实地关掉一个节点、切断节点间网络。
- **压测机与集群分离**：压测工具和被测系统抢资源会让数据失真。
- **开发和发压在同一台 Mac mini 上**（2026-11-21 起，主人定的：它一直开着、接有线，可以远程连上去，长时间的压测不用 MacBook 开着盖子陪跑）。代价是开发的负载会和 k6 抢 CPU，所以有一条规矩：**k6 在跑的时候，这台机器上不跑 `make test`、`make e2e`、`make images`、`make lint`；正式的一轮之前先 `make down`**（compose 空闲时也占约 4GB 内存）。长稳测试的几个小时里只做读文档、看监控、写记录这类事。
- **Mac mini 不加入集群**：混合架构要求每个镜像都多架构构建，和学习目标无关。
- **GTX 1660 不用于模型推理**：6GB 显存跑不了对诊断有用的模型，Agent 使用 Claude API。

## 开机需求

| 阶段 | 需要开机的设备 |
|---|---|
| 0、1 | 开发机 |
| 2 | 开发机、Ubuntu、NAS |
| 3 及以后 | Mac mini、Ubuntu、NAS、R2S（MacBook 不需要） |

## 开发机和操作端（Mac mini）

仓库在 `~/code/personal/Oppenheimer`（和 MacBook 上同一个路径，Claude Code 的项目记忆按路径找）。这些东西在仓库之外，换一台机器要重新准备：

- **工具**：Homebrew 装 go、golangci-lint、helm、kubernetes-cli、mysql-client（要自己加进 `PATH`，见 `~/.zshrc`）、shellcheck、jq、gh、k6；`./bin` 用 `make tools` 生成。版本以 `docs/infra/versions.md` 为准。
- **OrbStack**：`~/.orbstack/config/docker.json` 的 `insecure-registries` 里要有集群的镜像仓库（`192.168.50.50:5000`，它是 HTTP 的），否则 `make images` 推不上去。内存上限 8GB（OrbStack 的默认，物理内存的一半），留着不调大：剩下的要给 k6。
- **ssh**：`~/.ssh/config` 里 `ubt` 指向宿主机（192.168.50.7）；这台机器的公钥在宿主机和三台虚拟机的 `authorized_keys` 里。**虚拟机的那一份是手工加的**：`vm.sh create` 只写入发起那台机器的公钥（`on-host.sh` 传过去的），从哪台机器重建集群，另一台就要重新加。
- **`mini` 这个别名在 Mac mini 上指向 `127.0.0.1`**（自己的公钥在自己的 `authorized_keys` 里）。`scripts/cluster/k6.sh` 固定经 `ssh mini` 跑 k6，这样在哪台机器上发起都是同一条代码路径，脚本不用区分。
- **kubeconfig**：`scripts/cluster/k3s.sh kubeconfig` 取到 `~/.kube/oppenheimer.yaml`。
- **`gh`**：要 `gh auth login`（令牌不随机器搬）。
- **`tmp/`** 不入库。历次压测、对账的原始数据从 MacBook 复制过来了（`tmp/k6`、`tmp/recon`、`tmp/cluster-bench` 等）；1d 的压测、本机 bench 和 chaos 的日志（共约 3.7GB）留在 MacBook 上没有搬。
- **本地 compose 的数据是新的**：MacBook 上那套扩成 4 个订单库、带 500 万归档订单的环境没有搬（`docs/PROGRESS.md` 的“本机开发环境”一节写的是 MacBook）。Mac mini 上是干净的 2 库环境，`make test`、`make e2e` 不受影响；要重做 1c 的扩容和归档演练，回 MacBook 或者重新灌数。
- **Mac mini 上有 Little Snitch**，新装或升级过的可执行文件第一次出站要主人在图形界面里放行。没放行时的样子：Go 程序报 `connect: bad file descriptor`，kubectl 报 `read tcp ...: connection reset`，而 `curl` 是通的。用 Homebrew 装或升级 go、kubectl、helm、golangci-lint、k6 之后，以及 `make build`、`make tools` 生成了新的二进制要连局域网或外网时，先想到它。不要换一个已放行的程序去绕。（2026-11-18 记的“macOS 本地网络权限挡住 k6”多半也是它，没有回头核对。）
- 经 ssh 的非交互命令不读 `~/.zshrc`，`PATH` 里没有 Homebrew。远程执行 make 时用登录的交互 shell，或者先导出 `PATH`。

## 宿主机

- **内存**：金士顿 16GB×2 加威刚 8GB×2，XMP 关闭，2400MHz。memtest86+ 完整跑过两轮没有错误。
- **磁盘**：1TB 上是 EFI、`/boot` 和 LVM（根分区；600G 的 `ubuntu-vg/vmpool` 挂在 `/var/lib/libvirt/images` 放虚拟机磁盘）。
- **双系统**：GRUB 菜单显示 5 秒，默认进 Ubuntu，第三项是 `Windows`。这一项是手工写在宿主机的 `/etc/grub.d/15_windows` 里的，不在仓库的脚本里，没有实际选它启动验证过。
- **三块网卡**，R2S 上按 MAC 各绑定了一个地址（MAC 不进仓库，要用时在宿主机上 `ethtool -P <接口名>` 读）：

| 网卡 | 接口名 | 地址 | 用途 |
|---|---|---|---|
| PCIe 有线（RTL8126，`PCI_E4`） | `enp41s0` | 192.168.50.7 | 网桥 `br0` 的上联口，地址在网桥上；宿主机出网用它 |
| Wi-Fi（`PCI_E3`） | `wlp35s0` | 192.168.50.8 | 管理通道，`ssh ubt` |
| 板载有线 | `enp39s0` | 192.168.50.6 | 兜底的管理通道：插上网线就能登录 |

三条路互不依赖：网桥的上联断了，从 .6 和 .8 照样能登录（按源地址的策略路由，原理在 `scripts/cluster/host-setup.sh` 的注释里）。

两个要知道的坑：

- **PCIe 网卡的接口名跟着插槽走**。挪插槽后有线和 Wi-Fi 都会改名，配置对不上，两条路同时断。这时把网线插到板载口（它的名字不变），从 192.168.50.6 登录，改三处：宿主机上 `/etc/netplan/00-installer-config-wifi.yaml` 里的接口名（安装器写的，不在仓库），`cluster.env` 的 `HOST_NIC` 和 `HOST_WIFI_NIC`，然后重跑 `host-setup.sh`。
- **板载口只能协商到百兆**，水晶头也卡不住（插座里有线对不通），只用来兜底。

## 虚拟机

定义在 `scripts/cluster/cluster.env`，改规格用 `scripts/cluster/on-host.sh vm.sh resize <名字>`，一次一台。

| 虚拟机 | 地址 | vCPU | 内存 | 磁盘 | 角色 |
|---|---|---|---|---|---|
| k3s-server | 192.168.50.40 | 4 | 14GB | 60GB | 控制面，监控的后端（ADR-0030） |
| k3s-agent-1 | 192.168.50.41 | 8 | 14GB | 150GB | 工作节点 |
| k3s-agent-2 | 192.168.50.42 | 8 | 14GB | 150GB | 工作节点 |

系统是 Ubuntu 26.04.1 的 cloud image，用户 `ubuntu`（免密 sudo，只能用 MacBook 的 ssh 密钥登录）。磁盘是按需增长的 qcow2。vCPU 合计 20，没有超分；内存合计 42GB，宿主机留约 5GB，KVM 不预占。

## 内存预算（粗估）

三台虚拟机合计 42GB：

| 组件 | 预估 |
|---|---|
| K8s 系统组件 | 2GB |
| MySQL：2 分片 × 一主一从 | 4GB |
| Redis Cluster：6 节点 | 1.5GB |
| Kafka：3 broker | 4.5GB |
| Prometheus、Grafana、Alertmanager、Loki | 4GB（3a 实测：空闲时约 2 GB，压测时的峰值约 2.4 GB） |
| Tempo（单体模式） | 1GB（3a 实测：空闲 0.2 GB，不采样压测时峰值 1.55 GB，上限 2 GB） |
| OpenTelemetry Collector | 0.5GB（3a 实测：gateway 加三个 agent 空闲 0.4 GB，压测时 0.6 GB） |
| KEDA | 0.1GB（3a 实测） |
| Chaos Mesh | 0.5GB |
| 业务服务（多副本，含 CDC） | 3GB |
| 归档 MySQL 实例 | 1GB |
| 余量 | 约 20GB |

监控各组件实测的内存、磁盘和保留期见设计文档 3a 第 8 节“第 8 步的前一半”。

中间件实际的 requests 见设计文档 2b 的 6.1（比这里多一个 `mysql-main` 和 Sentinel）。

## 网络

局域网是 192.168.50.0/24，网关、DNS、DHCP 都在 R2S（192.168.50.2）。DHCP 池是 .100 到 .249；.1 到 .32 是 R2S 上给各设备的静态绑定，不要占用。集群的地址定义在 `scripts/cluster/cluster.env`：

| 地址 | 用途 |
|---|---|
| 192.168.50.2 | R2S |
| 192.168.50.3 | DS218+（`ssh nas`，端口 5022） |
| 192.168.50.6、.7、.8 | 宿主机的板载口、网桥、Wi-Fi（见上） |
| 192.168.50.40 到 .42 | k3s-server、k3s-agent-1、k3s-agent-2 |
| 192.168.50.43、.44 | 已在代理白名单里，留给以后加节点 |
| 192.168.50.50 到 .59 | MetalLB 地址池。.50 是镜像仓库（端口 5000），.51 是对外入口（Envoy Gateway，端口 80），其余未分配 |

- **虚拟机用桥接**：网卡接在宿主机的 `br0` 上，直接拿局域网地址。Wi-Fi 网卡不能接进网桥，所以集群要靠有线。
- **出网要进 R2S 的白名单**：代理（nikki）是 fake-ip 模式，带按 IP 和 MAC 的白名单。不在名单里的设备解析域名拿到 198.18.0.0/16 的假地址，但连接不被代理接管：按域名的出网全都连不上，而 ping 真实 IP 是通的。现在放行的是 .7 和 .40 到 .44；MetalLB 的地址只接收进来的访问，不需要进名单。
- **节点不接受 IPv6 的路由通告**（`accept-ra: false`）：通告里带的 IPv6 DNS 不经过代理，返回被污染的地址（设计文档 2a 第 11 节）。集群只用 IPv4。
- **NAS 的 NFS**：共享文件夹 `k8s`（`/volume1/k8s`），只对 192.168.50.40/29 开放，Squash 是“无映射”。子目录由 `scripts/cluster/addons.sh nfs` 建，要直接挂子目录。
- **有线是千兆**：网卡最高 5G，但路由器的口只有千兆。实测从节点读 NAS 是 113 MB/s。
