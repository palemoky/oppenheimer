# 2a 笔记：在三台虚拟机上搭 k3s 集群

这份笔记整理 2a 里值得在面试中讲的点，按主题组织，不按开发步骤。每条按“面试官会怎么问 → 要点 → 本项目里怎么做、量到什么数、踩过什么坑”来写。设计全文见 [design/2a-cluster.md](../design/2a-cluster.md)（下文简称“设计”，引用章节号），取舍见 [ADR-0001](../adr/0001-k3s-on-kvm.md)、[0025](../adr/0025-envoy-gateway-and-metallb.md)、[0026](../adr/0026-k3s-datastore-and-bundled-components.md)。

2a 做的事：在宿主机的三台 KVM 虚拟机（1 个 server、2 个 agent）上装 k3s，配好镜像仓库、存储、入口（MetalLB 加 Envoy Gateway），并把“从空虚拟机到可用集群”做成可重复执行的脚本。

**关于“通用知识”**：文中标注“（通用知识）”的段落，是为了把原理讲清楚而补的 Kubernetes、Linux 常识，设计文档和 ADR 里没有出处，面试前要对照官方文档核对。没有标注的内容，事实来自设计文档、ADR 或仓库里的脚本和配置。数字都是 2026-11-15 在这个集群上量的，设计第 12 节。

## 1. 为什么是 KVM 虚拟机加 k3s

### 1.1 集群怎么选：kubeadm、k3s、kind

**问**：为什么在虚拟机上用 k3s，不用 kubeadm，也不用 kind？

**要点**：选型要从目标倒推。本项目要的是“能演练节点宕机和网络分区的多节点集群”，约束是一台 32 GB 的机器还要跑中间件和监控，内存最紧。

| 方案 | 为什么没选 |
|---|---|
| kind、k3d（容器当节点） | 最省资源，但节点是容器，关节点、断网络的故障不够真实 |
| 裸机单节点 | 没有节点级故障可练 |
| kubeadm | 更接近企业自建，能学到控制面各组件的手工部署；但控制面占用更多内存，日常使用层面和 k3s 没有区别 |
| 把 Mac mini 加进集群 | 变成 arm64 和 x86_64 混合集群，所有镜像要多架构构建，和学习目标无关 |

**本项目**（ADR-0001）：宿主机上用 KVM 建三台虚拟机，k3s 一个 server 加两个 agent。代价是缺少 kubeadm 手工搭建的经验，ADR 里写了需要的话以后单独练一次。

### 1.2 k3s 和 kubeadm 搭出来的集群差在哪

**要点（通用知识）**：k3s 是通过了一致性认证的 Kubernetes 发行版，API 和标准集群一致，差在打包方式：

- **单进程**：apiserver、controller-manager、scheduler、kubelet、kube-proxy 打进一个二进制，server 上是一个 `k3s` 进程；kubeadm 里它们是各自的静态 Pod 或进程。
- **数据存储可选**：默认 SQLite（经 kine 转成 etcd 接口），也可以内嵌 etcd 或外部数据库；kubeadm 固定是 etcd。
- **自带组件**：flannel、CoreDNS、Traefik、ServiceLB、local-path-provisioner、metrics-server 都随包装好，kubeadm 一个都不带，CNI 要自己选。
- **证书、安装**：一条安装脚本加一个配置文件，kubeadm 要分步骤 init 和 join。

**本项目**：k3s 自带的东西哪些留哪些换，是 ADR-0026 定的，见 2.3。实测上 server 节点上 k3s 进程自己占 638 MiB（apiserver、etcd、调度器等都在里面），containerd 约 190 MiB（设计 12）。进程是合在一起的，这解释了为什么在 server 上看不到独立的 apiserver 和 etcd 进程。

### 1.3 server 节点跑不跑工作负载

**问**：控制面节点要不要加污点？

**要点（通用知识）**：标准做法是控制面节点加 `NoSchedule` 污点，不让业务 Pod 上去，避免和控制面抢资源。小集群里这会让控制面节点的资源大半闲置。

**本项目**（设计 3.2、待确认项 3）：server 有 6 GB，不加污点，但约定：中间件用节点亲和放到两个 agent 上，server 上只放控制器一类的轻负载（MetalLB 和 Envoy Gateway 的控制器、镜像仓库）。风险是工作负载和 etcd 抢磁盘和内存。如果实测 etcd 的 fsync 延迟被拖高，再加污点。第 3 节里实测的 fsync 延迟已经偏高，这个条件是否触发要等 2b 上了中间件再看，**目前没有加**。

## 2. 控制面的数据存储

### 2.1 内嵌 etcd 还是 SQLite

**问**：k3s 单 server 默认是 SQLite，你为什么选内嵌 etcd？

**要点**：单个 server 用 SQLite 完全够用，选 etcd 是为了学习，不是为了可用性。理由（ADR-0026）：

- 标准控制面用的是 etcd。快照和恢复、碎片整理、空间配额写满后集群变只读、慢磁盘导致选主超时，这些是真实的运维问题，用 SQLite 一个都碰不到。
- 阶段 4 要对控制面做故障注入，etcd 有明确的可观测面（`etcdctl endpoint status`、自带的指标），SQLite 没有。
- 以后想加 server 做高可用时不用迁移数据存储。

代价是多占几百 MB 内存，并且对磁盘写延迟敏感。

**本项目**：server 的配置里 `cluster-init: true` 启用内嵌 etcd，`etcd-expose-metrics: true` 提前打开指标（阶段 3 要抓，不提前开以后要重启控制面）。

### 2.2 单成员 etcd 意味着什么

**问**：只有一个 server，etcd 是三副本吗？

**要点**：不是，是**单成员**，没有任何容错。（通用知识）etcd 基于 Raft，容错需要多数派，3 个成员容忍 1 个故障，1 个成员容忍 0 个。这个 server 挂了，控制面就没了：已经在跑的 Pod 不受影响，但不能调度、不能改配置。这和 SQLite 的情况一样，选 etcd 并没有让集群更可用（ADR-0026）。

**本项目没有做**三个 server 的 etcd 集群：三台虚拟机都跑控制面，留给工作负载的内存不够；而且三台虚拟机在同一台物理机上，物理机一挂全没，高可用的意义有限（ADR-0026 备选方案）。快照由 k3s 自带的任务做（默认每 12 小时一次、留 5 份），复制到 NAS 和演练恢复是 2b 的事，**2a 没有做**。设计 6.3 还特意说明：快照目录不直接挂 NFS，因为 NAS 关机时快照任务会卡住，所以先写本地再定时复制。

### 2.3 etcd 的 fsync 延迟为什么重要，这次量到什么

**问**：etcd 对磁盘有什么要求？怎么判断磁盘够不够？

**要点**：（通用知识）etcd 每次提交前要把 WAL 刷盘，写请求的延迟里包含这次 fsync；延迟高会让 Raft 的心跳和选主超时更容易触发，影响整个控制面。常见的判断线是指标 `etcd_disk_wal_fsync_duration_seconds` 的 p99 在 10 毫秒以内（设计 9.2 也用的这条线）。看分布要看直方图的各个桶，不能只看平均值。

**本项目**（设计 12）：装完全部组件后约 7400 次 WAL fsync 的分布：

| 区间 | 累计占比 |
|---|---|
| 2 毫秒以内 | 43% |
| 8 毫秒以内 | 59% |
| 16 毫秒以内 | 99% |

p99 落在 8 到 16 毫秒这一档，**超过了 10 毫秒的参考线**，而且分布是两头多、中间少。空集群上没有看到选主超时之类的后果。**原因没有查**：可能是 qcow2 加 ext4 加 LVM 这几层（虚拟机磁盘是 qcow2、`cache=none`），也可能是这块消费级 NVMe 本身的刷盘延迟。2b 上了中间件、磁盘有了竞争之后要再看一次；要改的话候选是给 server 的磁盘换成直接用逻辑卷。面试时如实说“量到了、偏高、没查出原因、有后续计划”，比说“没问题”可信。

### 2.4 自带组件哪些留哪些换

**问**：k3s 自带的组件你保留了哪些？

**本项目**（ADR-0026）：

| 组件 | 决定 | 理由一句话 |
|---|---|---|
| 数据存储 | 内嵌 etcd | 见 2.1 |
| CNI | 保留 flannel，**暂定** | Cilium 每节点 agent 加 operator，粗估多 1 到 1.5 GB（公开资料的估计，没实测），余量约 6 GB，先量再定 |
| 网络策略 | 保留内嵌的 kube-router 控制器 | flannel 加它已经能执行 NetworkPolicy，2d 不依赖换 CNI |
| CoreDNS、local-path、metrics-server | 保留 | HPA 和 `kubectl top` 要用 metrics-server |
| Traefik、ServiceLB | 禁用 | 换 Envoy Gateway 和 MetalLB，见第 7、8 节 |

CNI 的决定有时间点：**换 CNI 等于重建集群**，而 2b 的中间件数据在本地盘上，所以要在 2b 中间件部署完、量过内存之后、灌数据之前回来改定。**Cilium 没有装**，这是待定项，不是已经做的事。

## 3. 节点可分配量、预留和驱逐

### 3.1 可分配量怎么算

**问**：节点有 16 GB 内存，Pod 的 requests 能加到 16 GB 吗？

**要点**：不能。调度器只看节点的可分配量（allocatable）：

> 可分配量 = 总量 − `system-reserved` − `kube-reserved` − 驱逐阈值

不写预留的话可分配量约等于总量，Pod 的 requests 可以把节点排满，这时 kubelet、containerd、k3s 自己没有保障，内存紧时先出问题的可能是它们。（通用知识）`kube-reserved` 留给 Kubernetes 系统守护进程，`system-reserved` 留给操作系统的守护进程；预留只影响调度的账，是否真的强制执行（用 cgroup 限制）取决于另外的 `enforce-node-allocatable` 设置，这里没有展开，面试前核对。

**本项目**（设计 3.2、12）：server 的 `kubelet-arg` 是 `system-reserved=cpu=200m,memory=400Mi`、`kube-reserved=cpu=300m,memory=800Mi`、`eviction-hard=memory.available<200Mi,nodefs.available<5%,imagefs.available<5%`。agent 同样，只是 `kube-reserved` 的内存是 300Mi（agent 上没有控制面）。实测的可分配量，和上面的公式对得上：

| 节点 | 总量 | 可分配 |
|---|---|---|
| server | 4 核、5.8 Gi | 3500m、约 4.4 Gi |
| agent（各） | 8 核、10.7 Gi | 7500m、约 9.8 Gi |

例如 server 的内存：5.8 − 0.4 − 0.8 − 0.2 ≈ 4.4 Gi。预留的数是起点，要对照实测占用调整。

### 3.2 驱逐阈值是整体覆盖

**问**：配置 `eviction-hard` 有什么坑？

**要点**：这个参数是**整体覆盖**，不是合并。只写 `memory.available` 会把默认的磁盘阈值一起去掉。本项目一开始就发现了这点（设计 11 发现 5），所以把 `nodefs.available<5%`、`imagefs.available<5%` 一并写上。（通用知识）硬驱逐阈值到了 kubelet 会立即驱逐 Pod，没有宽限期，它从可分配量里扣掉，是为了让调度器不要把节点排到这个线上。

### 3.3 实测的内存占用

设计 12 的数（空集群，`free` 的 used）：

| 节点 | 只有 k3s | 加上 MetalLB、仓库、Envoy Gateway |
|---|---|---|
| k3s-server | 1052 MiB | 1483 MiB |
| k3s-agent-1 | 630 MiB | 771 MiB |
| k3s-agent-2 | 607 MiB | 828 MiB |

全部 Pod 加起来 239 Mi：Envoy Gateway 的控制器 41 Mi、Envoy 38 Mi、MetalLB 的控制器 35 Mi 加三个 speaker 各约 22 Mi、仓库 12 Mi、CoreDNS 13 Mi、metrics-server 21 Mi、local-path 8 Mi。三个节点合计约 3.1 GiB，比 hardware.md 里“K8s 系统组件 2 GB”的估计多约 1 GiB。这份数是 CNI 决定（见 2.4）的第一份输入。

## 4. 虚拟机的网络

### 4.1 桥接网络

**问**：虚拟机怎么接入局域网？为什么必须走有线？

**要点（通用知识）**：桥接是把虚拟机的网卡接到宿主机的网桥上，和宿主机的物理网卡在同一个二层网络，虚拟机像局域网里的一台独立设备，直接拿局域网地址。无线网卡通常不能加入网桥（它的二层模型不支持），所以宿主机必须走有线。

**本项目**（hardware.md）：虚拟机用桥接，网卡接在宿主机的 `br0` 上，直接拿局域网地址，用 DHCP 池之外的静态地址 192.168.50.40 到 .42。宿主机还有一条 Wi-Fi 作为管理用的备用通道。

**一个踩过的坑：网线拔掉后备用通道也连不上**（`host-setup.sh` 的注释，2026-11-15 实际发生）。网线被拔掉时 `br0` 没有载波，但它是静态地址，地址和路由都还在（标着 `linkdown`）。内核默认照样用这些路由，而且比 Wi-Fi 的路由优先：发往局域网的包全进了断掉的网桥，宿主机从 Wi-Fi 也连不上。修法是打开 `net.ipv4.conf.{all,default}.ignore_routes_with_linkdown = 1`，内核跳过 linkdown 的路由，Wi-Fi 才真的是备用通道。注意这个开关对已经存在的接口要逐个写 `/proc`，只写 `default` 对已有接口不生效，脚本里是这么处理的。

### 4.2 `br_netfilter` 的两面

**问**：Kubernetes 节点为什么要加载 `br_netfilter`？它在宿主机上为什么危险？

**要点**：

- **节点里需要。** （通用知识）Pod 之间经 Linux 网桥转发的流量，默认只在二层处理，不经过 iptables。`br_netfilter` 让桥上的帧也过 iptables 的链，kube-proxy 做 Service 转发（DNAT）的规则才对同一个节点内经网桥的流量生效。k3s 在虚拟机里面加载它，是它自己需要的（设计 3.5）。
- **宿主机上危险。** 宿主机的 `br0` 上，虚拟机之间、虚拟机和局域网之间的帧都要经过。现在这些帧不经过宿主机的 iptables，因为模块没有加载。一旦有东西把它加载起来（设计列的例子是 Docker 的某些网络模式、在宿主机上装 Kubernetes 相关的工具），桥上的帧就要过 `FORWARD` 链，而这条链的默认策略被 Docker 设成了 DROP：**三台虚拟机会同时断网**，原因很难联想到。

**本项目的处理**（`host-setup.sh`）：按 libvirt 文档的做法加 udev 规则，模块被加载的那一刻把 `net.bridge.bridge-nf-call-iptables`、`-ip6tables`、`-arptables` 设成 0。只写 `sysctl.d` 不够，因为这三个开关在模块加载之前不存在（`/proc/sys/net/bridge` 目录都没有）。

**实验结果**（设计 12）：在宿主机上手工 `modprobe br_netfilter` 后，三个开关立刻是 0，期间虚拟机之间 60 个 ping 没有丢。要说清楚的是：这个实验验证的是“规则生效、开关被置 0”，**没有**复现过“开关为 1 时三台虚拟机断网”这件事，那一半是推演。

## 5. DNS 和出网：fake-ip 代理下的一次排查

**问**：说一次你定位网络问题的经历。

这是 2a 最值得讲的一个故事（设计 11 发现 1），先交代背景，再讲过程。

**背景**：局域网的网关、DNS、出网代理都在 R2S（OpenWrt）上，代理是 fake-ip 模式并带按 IP 和 MAC 的白名单（hardware.md）。（通用知识）fake-ip 的意思是：代理对域名解析请求返回一个假的地址（这里是 198.18.0.0/16 里的），客户端连这个假地址，代理再按“假地址到域名”的映射接管连接并代为访问。所以它要求“解析和连接都经过代理”。不在白名单里的设备解析拿到假地址但连接不被接管，表现是所有按域名的出网都连不上，而 ping 真实 IP 是通的。节点已经加进了白名单。

**设计阶段的推演**（设计 3.4）：Pod 查域名走 CoreDNS，再到节点的上游 DNS；k3s 发现节点用 systemd-resolved 时改用 `/run/systemd/resolve/resolv.conf`，里面是 192.168.50.2 和一个 IPv6 地址。设计里已经标出“那个 IPv6 的上游是谁、返回什么不确定”，担心的是 Pod 里“同一个域名时通时不通”。

**现象**：担心的问题出现了，而且比预想的更早：k3s 装完后系统 Pod 全部卡在 `ContainerCreating`，原因是节点拉不到 `pause` 镜像。不是 Pod，是节点自己连不上镜像源。

**原因**：节点的 DNS 上游有两个。192.168.50.2 返回代理的假地址（198.18.x.x）；路由通告（RA）下发的 IPv6 地址的那个上游返回的是被污染的真实地址（`registry-1.docker.io` 解析成了一个不相干的地址）。systemd-resolved 选中后者时，所有按域名的出网都超时。建虚拟机时的检查之所以通过，是因为当时它恰好用的是前一个。**这是一个“时好时坏”的故障：上游有多个、其中一个有问题时，故障取决于选中谁。**

**怎么定位**：四步，每一步把范围缩小一层：

1. `kubectl describe pod` 看事件：`FailedCreatePodSandBox`，拉不到 `rancher/mirrored-pause`。说明问题在节点拉镜像，不在 Pod 里面，也不在 CNI。
2. 在节点上 `curl https://registry-1.docker.io/v2/`：10 秒超时。排除了 containerd，是节点自己出不了网。
3. `getent hosts registry-1.docker.io`：返回的是一个公网的 IPv6 地址，而经过代理应该拿到 198.18.x.x。问题在解析。
4. 用 `nslookup` 分别指定两个上游查同一个域名：问 192.168.50.2 得到 198.18.x.x，问那个 IPv6 的上游得到一个不相干的公网地址。再看 `/run/systemd/resolve/resolv.conf`，两个上游都在里面。

第 4 步是关键：`resolvectl query` 看到的是 systemd-resolved 已经选好上游之后的结果，分不出是谁答的；要绕过它，直接对每个上游发查询。

**怎么修**：R2S 不归本项目管，所以修在节点一侧：netplan 里 `accept-ra: false`、`dhcp6: false`，节点不再接受 IPv6 的路由通告，DNS 上游只剩 192.168.50.2。写进了 `vm.sh` 的 cloud-init `network-config`，而不是在节点上手工改，这样重建时不会丢。修完后三个节点上的 Pod 反复解析四个镜像源，结果全部落在 198.18.0.0/16。重建演练后也验证了节点的 DNS 上游只有 192.168.50.2，即修复来自脚本，不是手工补的。

**教训**：

- 多个 DNS 上游并存时，“检查通过”只说明当时选中的那个是好的。
- 设计阶段能推演出风险（它确实写进了 3.4），但要靠实测才知道后果在哪一层出现；推演担心的是 Pod，实际先坏的是节点。
- 集群只用 IPv4，是有意的取舍。

## 6. 容器网络：flannel、VXLAN 和 MTU

**问**：flannel 的 VXLAN 模式下，跨节点的 Pod 流量是怎么走的？MTU 为什么是 1450？

**要点（通用知识）**：VXLAN 是把 Pod 的二层帧封装进 UDP 包，从一个节点的 VTEP 发到另一个节点。封装多出外层的头，总共 50 字节，物理网卡的 MTU 是 1500，Pod 网卡的 MTU 就要降到 1450，否则大包会被分片或丢弃。症状上常见的是“小请求正常，大响应卡住”，排查时先查路径 MTU。

**本项目**：节点 eth0 的 MTU 是 1500（设计 1），ADR-0026 写明 flannel 的 VXLAN 让跨节点的包多 50 字节的头、MTU 是 1450；阶段 3 压测时如果看到跨节点吞吐明显低于同节点，先想到这里。**这一条是预判，2a 没有量跨节点和同节点的吞吐差**，实测的只有 Pod 跨节点互通、延迟约 0.4 毫秒（设计 12）。

**不改 Pod 和 Service 网段**（设计 3.2）：默认 10.42.0.0/16 和 10.43.0.0/16。要避开的有三个：局域网 192.168.50.0/24、R2S 代理的假地址 198.18.0.0/16、宿主机上 Docker 用的 172.17 和 172.31，默认值都不冲突。**选网段的原则：不和任何你要访问的真实网络重叠**，这里还多出一个特殊的“假地址网段”要避。

## 7. MetalLB 二层模式

### 7.1 原理

**问**：裸机上 `LoadBalancer` 类型的 Service 怎么拿到外部地址？二层模式怎么工作？

**要点**：云上的 `LoadBalancer` 由云厂商的控制器分配地址；家里的局域网没有，要靠 MetalLB 这类实现。二层模式（设计 5）：地址池里的某个地址由集群中的**一个节点的 speaker 持有**，这个节点对该地址的 ARP 请求作答，于是局域网里发往它的帧都到这个节点，再由 kube-proxy 的规则转给后端 Pod。持有地址的节点挂了，其他 speaker 发现后由新的节点发**免费 ARP**（gratuitous ARP）接管，让局域网里设备的 ARP 缓存更新。

**它是故障切换，不是负载均衡**：同一时刻只有一个节点接收这个地址的流量，所以入口的带宽上限是单个节点的，不是三个节点之和（ADR-0025 后果）。压测时入口成为瓶颈要先想到这一点。要真正分摊需要 BGP 模式加路由器的 ECMP（通用知识），本项目只用二层，不启用 BGP 的部分。

### 7.2 本项目的配置

- 版本 v0.16.2，用 Helm 装在 `metallb-system`。
- 一个 `IPAddressPool`（192.168.50.50 到 .59）和一个 `L2Advertisement`。地址池关掉自动分配，每个 Service 用注解写明要哪个地址：.50 给镜像仓库（5000），.51 给对外入口（Envoy Gateway，80），其余未分配。地址的用途登记在设计和 hardware.md。
- MetalLB 的地址不需要进代理白名单，因为它们只接收进来的访问（hardware.md）。
- 实测：仓库拿到 .50、入口拿到 .51，从 MacBook 能访问；**两个地址当时都由 k3s-agent-2 应答**。这说明一个细节：不同地址不保证落在不同节点上。

### 7.3 踩的坑：chart 默认装了 frr-k8s

MetalLB 的 chart 默认装 frr-k8s（设计 11 发现 3）：那是 BGP 的后端，每个节点一个 5 个容器的 Pod（各约 60 Mi）再加一个清理器，二层模式用不到。关的开关是 `frrk8s.enabled=false`；`speaker.frr.enabled` 是已弃用的旧后端，**只关它没有用**。教训：装 Helm chart 后要看它实际创建了什么，内存是最紧的资源，默认值未必适合你的场景。

### 7.4 没做的

- 持有入口地址的节点被关掉后入口多久恢复、漂移期间丢多少请求：**没有量**，阶段 4 做。ADR-0025 把这点当作选 MetalLB 的理由之一（ServiceLB 没有地址漂移可练）。
- 入口的 Envoy 目前只有 1 个副本，它所在的节点挂了入口就断到 Pod 被重建为止。阶段 4 之前要改成 2 个并分散到两个节点（设计 12）。

## 8. `externalTrafficPolicy`

**问**：`externalTrafficPolicy` 的 `Cluster` 和 `Local` 有什么区别？

**要点**（设计 5，细节（通用知识）部分需核对）：

| | `Cluster`（默认） | `Local` |
|---|---|---|
| 转发范围 | 流量可以从收到的节点转给别的节点上的 Pod | 只转给本节点上的 Pod |
| 源地址 | 转发时源地址被改成节点地址（SNAT），后端看到的客户端全是节点 | 保留客户端的真实源地址 |
| 多一跳 | 可能有 | 没有 |
| 对 LB 的影响 | 任何节点都能接 | MetalLB 只会让“有就绪后端的节点”持有地址；本节点没有后端的话该节点不应答 |

（通用知识）`Local` 的另一面是后端分布不均时流量也会不均，因为 LB 按节点而不是按 Pod 分流量。

**本项目**：入口的 Service 用 `Local`（`gateway.yaml` 里 `externalTrafficPolicy: Local` 加注释），因为 gateway 服务的按 IP 限流（ADR-0019）依赖真实的客户端地址，再由 Envoy 把客户端地址写进 `X-Forwarded-For`；gateway 怎么信任这个头是 2c 的事。仓库的 Service 不关心源地址，用默认值。

**实测**：回显服务看到的 `X-Forwarded-For` 是 MacBook 的地址，不是节点的（设计 12）。这一条要连到 1d：限流只能在拿到真实客户端地址时才有意义，所以一个看起来是存储或网络层的参数选择，实际上服务着上层的限流。

## 9. 入口：Gateway API 和 Envoy Gateway

### 9.1 三层资源，和 Ingress 的区别

**问**：Gateway API 的资源模型是什么？和 Ingress 有什么区别？

**要点**（设计 7）：

| 资源 | 回答什么 | 谁管 |
|---|---|---|
| `GatewayClass` | 这类入口由哪个控制器实现 | 基础设施提供方 |
| `Gateway` | 要一个入口，监听哪些端口 | 集群运维。Envoy Gateway 看到它才去创建 Envoy 的 Deployment 和 Service |
| `HTTPRoute` | 哪些请求转给哪个 Service | 应用开发者，由各个服务自己的 chart 带着 |

和 Ingress 的区别在于把“基础设施怎么提供”和“应用怎么路由”分给了不同的资源和不同的人；超出标准的能力（限流、重试、熔断）用策略资源挂上去，不靠注解。ADR-0025 指出 ingress-nginx 的问题之一正是大量行为藏在注解里。

**本项目**（`deploy/cluster/envoy-gateway/gateway.yaml`）：一个 `GatewayClass`（名字 `eg`，引用一个 `EnvoyProxy` 作参数）、一个 `Gateway`（名字 `public`，监听 80，`allowedRoutes` 允许所有命名空间挂路由，因为路由由各服务在自己的命名空间声明）、一个 `EnvoyProxy` 定制数据面（1 个副本、资源限额、Service 的 MetalLB 注解和 `externalTrafficPolicy: Local`）。2a 只验证通路：部署一个回显服务加一条 `HTTPRoute`，验证后删掉回显服务，`Gateway` 留给 2c。443 要等 2d 有了证书再加。

### 9.2 ingress-nginx 为什么退役

**问**：为什么不用 ingress-nginx？

**要点**（ADR-0025，查证于 2026-11-15）：Kubernetes 的 SIG Network 和安全响应委员会在 2025 年 11 月宣布它退役，尽力而为的维护到 2026 年 3 月为止，之后没有新版本也不修安全漏洞，最后一版是 controller-v1.15.1（2026-03-19）。官方建议迁到 Gateway API。Ingress 这套 API 本身也冻结了，新功能只进 Gateway API。

### 9.3 为什么选 Envoy Gateway

- 和 1d 是同一套东西：服务之间的熔断、并发上限、逐跳时限已经是 Envoy 边车在做，入口也用 Envoy，排查时看同一套指标名和访问日志。
- 是 Gateway API 的参考级实现，以它为唯一配置入口。
- 备选没选的理由：Traefik 是另一套代理、另一套指标；Cilium 的实现绑 CNI，而 CNI 还没定；Istio 内存开销大，而且等于把 1d 手写的边车配置重做。入口这一层先只做路由，不重复限流（ADR-0025）。
- MetalLB 对 ServiceLB：ServiceLB 是每个节点起转发 Pod、对外地址就是节点地址，没有独立的虚拟地址，没有漂移可练。

### 9.4 一次失败：CRD 通道冲突

**问**：装 Helm chart 时遇到过什么问题？（设计 11 发现 4）

**现象**：Envoy Gateway 的主 chart 安装被拒绝。

**原因**：k3s v1.37 自带一个 Helm release `gateway-api-crd`（v1.6.3，**标准通道**），禁用 Traefik 之后它还在。Envoy Gateway 的主 chart 自带的是**实验通道**的 CRD，被 Gateway API 的准入策略 `safe-upgrades` 拒绝：不允许用实验通道覆盖标准通道。（通用知识：Gateway API 的 CRD 分标准和实验两个通道，实验通道含更多、不稳定的资源，有防止误从标准降级的准入保护。）

**修法**：用 Envoy Gateway 单独的 `gateway-crds-helm` 只渲染它自己的 CRD，用服务端 apply 提交（整个作为 Helm release 安装会超过 Secret 的 1 MiB 上限，Helm 把 release 存在 Secret 里），主 chart 加 `--skip-crds`。

**后果**：Gateway API 的版本跟着 k3s 走（v1.6.3），而 Envoy Gateway v1.10 的兼容性表写的是 v1.6.1，只差补丁号；升级 k3s 时要对照兼容性表。顺带：版本是对照兼容性表选的，不是用上游最新的 v1.7.0。

**教训**：集群里已经有谁管着某类资源（CRD 的所有权），再装一个会冲突；改用“只提交我需要的部分”。

## 10. 存储

### 10.1 local-path 和 NFS 各管什么

**问**：这个集群的存储怎么分？

| | local-path（k3s 自带，默认 StorageClass） | NFS（NAS） |
|---|---|---|
| 数据在哪 | 节点本地盘 `/var/lib/rancher/k3s/storage` | NAS 的 `/volume1/k8s` |
| 给谁用 | 中间件的数据（MySQL、Redis、Kafka） | 镜像仓库、MySQL 备份、etcd 快照 |
| 节点挂了 | 数据跟着节点不可用，Pod 不能换节点 | 不受影响 |
| NAS 关机 | 不受影响 | 挂载的 Pod 卡在读写上 |

设计 6.1。中间件的使用在 2b，2a 只用了 NFS 放镜像仓库。

### 10.2 数据库为什么不放 NFS

**问**：MySQL 的数据目录能放在 NFS 上吗？

**要点**：数据库靠 fsync 保证提交过的事务落盘，靠文件锁保证只有一个实例在写；NFS 上这两样的语义都比本地盘弱，延迟还要加上一次网络往返（本项目现在还是百兆）。所以 NFS 只放“写完整个文件、之后只读”的东西（备份、镜像层）。（通用知识：NFS 服务端开了异步写时，服务端会在数据落盘前就答复成功，断电会丢已经确认的写入，这是 NFS 上放数据库最直接的危险。）

**本项目**：NAS 的共享文件夹明确**不勾“启用异步”**（设计 6.2），放的是备份，不能丢已确认的写入。另外 Prometheus 官方文档不支持把数据目录放 NFS，hardware.md 里“监控长期存储放 NAS”到阶段 3 要重新想（设计 6.3）。

### 10.3 local-path 的 PV 为什么带节点亲和，`WaitForFirstConsumer`

**问**：local-path 的卷为什么 Pod 不能换节点？`WaitForFirstConsumer` 解决什么问题？

**要点**（设计 6.1 是结论，机制解释为通用知识）：local-path 的卷就是节点本地的一个目录，别的节点看不到，所以 PV 带着节点亲和：卷建在哪个节点，Pod 以后就只能调度到那个节点。StorageClass 的绑定模式是 `WaitForFirstConsumer`：先让调度器决定 Pod 去哪个节点，再在那个节点上建卷。如果是立即绑定，卷会先在某个节点建好，而 Pod 的调度又有别的约束（亲和、资源）时，可能出现 Pod 无处可去。另外它不限制容量，PVC 里写的大小只是个声明。这三点 2b 排布中间件时都要用到。

### 10.4 手写 PV，不装动态供给

**本项目**（设计 6.3、待确认项 2）：2a 加 2b 只有三个 NFS 卷，各写一个 PV（`nfs.server`、`nfs.path`、`persistentVolumeReclaimPolicy: Retain`、挂载选项 `nfsvers=4.1,hard`）和一个按名字绑定的 PVC。动态供给（csi-driver-nfs）要多一个控制器加每节点一个 Pod，解决的是“很多人随时要卷”，这里没有。看 `registry.yaml` 能看到几个细节：PVC 的 `storageClassName: ""`（空字符串，不走默认的 local-path 动态供给）、`volumeName` 指定要绑的 PV、PV 里 `claimRef` 预先指定归属。`Retain` 让 PVC 删了数据不动，集群重建后重新声明同一个 PV 镜像还在，这被重建演练验证过（见第 12 节）。

### 10.5 NFS 的 squash，以及群晖 ACL 那个坑

**问**：NFS 的 squash 是什么？

**要点**（设计 6.2）：NFS 默认用客户端报上来的用户号做权限判断。服务端默认会把客户端的 root（uid 0）映射成匿名用户（`root_squash`，通用知识）。群晖的“Squash”选项里选“无映射”就是原样相信：节点上的 root 在这个共享文件夹里也是 root，可以建目录、改属主。**这样脚本能从节点上把子目录建好并交给对应的用户号，之后 NAS 上不再需要 root 或 sudo**（ssh 进去的用户没有免密 sudo，设计 1）。代价是这 8 个地址（192.168.50.40/29，覆盖 .40 到 .47）上任何拿到 root 的人对这个共享文件夹有完全权限，范围只限这一个文件夹。注意 Pod 挂 NFS 是节点上的 kubelet 去挂的，NAS 看到的来源是节点地址，不是 Pod 的。

**踩的坑**（设计 11 发现 2）：设计里写过“这一项没有实测过”，群晖的共享文件夹带自己的 ACL，实测结果是：

- 共享文件夹的根目录带群晖的 ACL，权限位显示 777，但非 root 的用户号列不了目录。
- root 新建的子目录继承这份 ACL，`chown` 给用户号 1000 之后，1000 仍然写不进去。
- 对目录做一次 `chmod` 之后它变成普通的权限位，属主才能读写，别的用户号被拒绝。
- 从共享文件夹的根挂载再走进子目录不行（根目录过不去），**要直接挂载子目录**。

**修法**：子目录由脚本（`addons.sh nfs`）以 root 建好，逐个 `chown` 加 `chmod`；PV 的路径写到子目录（`registry.yaml` 里是 `__NAS_EXPORT__/registry`，注释写着这个原因）。Squash 保持“无映射”，不用退到备选的“映射为 admin”（那样所有写入都记成 admin，改属主会失败）。

**教训**：权限位显示的不一定是实际的访问控制，NAS 上的文件系统可能叠加 ACL；这类东西要挂上实测，设计阶段留了验证项是对的。

NFS 的写入速度实测：200 MiB 用了 19 秒，11 MB/s，和百兆链路的预期一致（设计 12）。千兆口的问题是宿主机板载网口的硬件问题（协商下降到百兆，hardware.md），准备加 PCIe 网卡，不挡 2a，阶段 3 压测前要解决。

## 11. 镜像和仓库

### 11.1 公共镜像经代理直接拉

节点已经在代理白名单里，containerd 直接访问 `docker.io`、`ghcr.io`、`quay.io`、`registry.k8s.io`，不配国内镜像站。实测四个源在三个节点上都拉得到，小镜像每个 6 到 15 秒（设计 12）。已知风险：Docker Hub 对匿名拉取限速、按出口地址计数，而出口是代理的地址，可能和别人共用。真被限速时的办法是再起一个仓库实例做拉取缓存，**现在没做**（设计 4.1）。

### 11.2 仓库放在它所服务的集群里：循环依赖

**问**：镜像仓库跑在要用它的集群里，有什么问题？

**要点**：循环依赖：集群要从仓库拉镜像来启动 Pod，仓库自己又是集群里的 Pod。影响有限，因为：仓库自己的镜像来自 Docker Hub，不从它自己拉；仓库不可用时，**节点上已经缓存的镜像照常启动，只有拉新镜像会失败**。（通用知识）这是 kubelet 的拉取策略和 containerd 本地缓存起的作用，`imagePullPolicy` 是 `IfNotPresent` 时本地有就不去拉；用 `Always` 则每次要联系仓库，设了会放大这个依赖，核对一下。

**本项目**（设计 4.2、待确认项 1）：distribution v3.1.2，一个 Deployment（1 副本、更新策略 `Recreate`，因为滚动更新会有两个实例同时写同一个目录的时刻），数据卷是 NAS 上的 NFS 目录，固定调度到 server 节点（`nodeSelector` 选控制面，注释：两个 agent 留给中间件，阶段 4 关 agent 时仓库不受影响），Service 是 `LoadBalancer` 由 MetalLB 固定给 192.168.50.50:5000。放在集群里的好处：用清单声明入库，顺带把手写 PV 挂 NFS 和 MetalLB 固定地址各练一遍；集群重建后数据还在 NAS 上，镜像不用重新推。备选“放在 NAS 的 Container Manager 里”没有这个循环，但那是 DSM 界面里手工建的容器，仓库里留不下完整声明，而且 NAS 是 2 核的 J3355。

### 11.3 `registries.yaml` 和明文仓库

**问**：k3s 怎么让节点从一个 HTTP 仓库拉镜像？

**要点**：每个节点的 `/etc/rancher/k3s/registries.yaml`：

```yaml
mirrors:
  "192.168.50.50:5000":
    endpoint: ["http://192.168.50.50:5000"]
```

不写 `http://` 的话 containerd 默认用 HTTPS。**这个文件只在 k3s 启动时读，改了要重启 k3s**，所以脚本在装 k3s 之前就把它放好，仓库晚一点才起来也不要紧（设计 4.2）。

**为什么明文、不设登录**（待确认项 4）：局域网里任何设备都能推镜像，在家里的网络上可以接受。上 TLS 要给三个节点和 MacBook 都装证书，等 2d 有了 cert-manager 之后再做才顺。地址直接用 IP，因为加域名要改 R2S 上的 DNS，而 R2S 不由本项目管。

**验证时碰到的**：MacBook 的 Docker 往明文仓库推被拒绝（`server gave HTTP response to HTTPS client`），符合预期，**2c 构建镜像时再定**用哪种方式。

### 11.4 往仓库推多架构镜像的坑

（设计 11 发现 6）节点上只拉了一个架构的镜像时，`ctr images push` 会连着索引里其他架构的清单一起推，仓库以“blob 不存在”拒绝；`docker save` 同样不行。测试时改用 skopeo 直接从 Docker Hub 复制单个架构。要点：**镜像索引（manifest list）引用了多个架构的清单，而本地只有一个架构的内容**，推送就会缺 blob。这和“开发机是 Apple Silicon、集群是 x86_64、镜像要构建 linux/amd64 或多架构”（AGENTS.md）有关，2c 要用到。

### 11.5 带宽

MacBook 推镜像的路径：MacBook → 路由器 → 宿主机网口 → 节点 → 仓库 → 宿主机网口 → NAS，一次推送要过宿主机的百兆口两次，上限约 11 MB/s；Go 服务的镜像几十 MB，可以接受；节点之间的流量不出宿主机，不受影响。

## 12. 脚本化和重建

### 12.1 为什么重建而不是卸载

**问**：你怎么证明你的建集群脚本是可靠的？

**要点**：**重建 = 删虚拟机再从头跑一遍**，不用 `k3s-uninstall.sh`：从干净的系统盘开始才能证明脚本没有依赖手工留下的状态（设计 8）。重建一台虚拟机只要一两分钟。删除虚拟机不做成 make 目标，仍然要显式运行 `vm.sh destroy <名字>`，并且按 AGENTS.md 的规则要主人确认。2a 结束时集群里还没有数据，是做这次演练代价最小的时候。

**实测**（设计 12）：删掉三台虚拟机后 `make cluster-up` 一次跑通，**5 分 28 秒**（建虚拟机、等 cloud-init、装 k3s、装三个组件）。重建后：

- 节点全部 `Ready`，没有异常的 Pod；
- 之前推的镜像还在（NAS 上的数据加 `Retain` 的 PV）；
- 入口检查通过；
- 节点的 DNS 上游只有 192.168.50.2，即第 5 节的修复来自脚本，不是手工补的；
- 再跑一次 `install` 三个节点都是 unchanged。

### 12.2 幂等

脚本每一步都可重复执行（设计 8）：k3s 的配置文件由仓库里的模板渲染后放到节点，重新运行时文件没有变化就不重启服务（实测：第二次 `install` 三个节点都是 unchanged，不重启）；Helm 用 `helm upgrade --install`，chart 版本写在 `cluster.env`，不引入 helmfile；k3s 版本用 `INSTALL_K3S_VERSION` 固定，不走通道（通道会随时间前移，重建出来的集群版本就变了）。所有选项写在 `/etc/rancher/k3s/config.yaml` 里，不用命令行参数，重建时不会漏（ADR-0026）。

**加入集群用的 token 不入库**：脚本在加入时从 server 的 `/var/lib/rancher/k3s/server/node-token` 读出来，写进 agent 的配置文件（权限 600）。kubeconfig 是集群管理员权限，等同于密钥，存成 `~/.kube/oppenheimer.yaml`（权限 600，在仓库之外），**不并进默认的 `~/.kube/config`**，因为 MacBook 上还有 OrbStack 的集群，靠“当前 context”区分，迟早对着错的集群执行命令；仓库里的脚本一律显式带 `--kubeconfig`（设计 3.3）。

### 12.3 主机密钥和 cloud-init

**踩的坑**（设计 11 发现 7）：重建后新虚拟机的地址不变、主机密钥是新的，MacBook 的 `known_hosts` 里是旧的，ssh 拒绝连接。加了 `k3s.sh prepare`：连不上时删掉旧密钥、接受新的，再等 cloud-init 做完（它要装 `nfs-common`）。

两点要讲清楚（脚本行为是设计里的，机制是通用知识）：

- 主机密钥是 ssh 防中间人的机制，地址不变但密钥变，ssh 必须拒绝；这个场景里换密钥是预期的，所以脚本在“明确是自己刚重建”的位置自动处理。不应该全局关掉主机密钥检查。
- cloud-init 是 cloud image 首次启动时执行用户数据的机制：本项目用它写 `network-config`（静态地址、关闭 RA，见第 5 节）、装软件包、写入 ssh 公钥。“能连上 ssh”不等于 cloud-init 做完，后续步骤（比如挂 NFS）要先等它完成。

### 12.4 重启宿主机

重启宿主机后不做任何操作（设计 12）：33 秒时宿主机断开，85 秒时宿主机的 ssh 和 apiserver 恢复，104 秒时三个节点 `Ready`、所有 Pod `Running`、仓库应答。能自己恢复靠三件事：虚拟机设了自启；宿主机关机时 libvirt-guests 先把虚拟机正常关机（etcd 是正常停止，不是断电）；k3s 是 systemd 服务，开机自启。单成员的 etcd 重启后直接从本地数据恢复，不需要和别的成员达成多数。

## 13. 没做的和待办

诚实地说清楚哪些没做，比假装做完更重要（设计 12、13）：

- 持有入口地址的节点被关掉后入口多久恢复：没有量，阶段 4。
- 入口的 Envoy 只有 1 个副本，阶段 4 之前改成 2 个并分散到两个节点。
- etcd 的 fsync 偏高，原因没有查，2b 再看。
- 换 Cilium：2b 量过内存之后决定，没有做。
- NFS 动态供给、etcd 快照复制到 NAS 和 MySQL 备份（2b）、仓库的 TLS 和登录和垃圾回收、入口 TLS（2d）、入口层限流、Spegel 都不在 2a。
- DNS、镜像拉取、仓库推拉这几项验证是手工跑的，没有自动化；`addons.sh smoke`（`make cluster-smoke`）只自动化了入口的通路检查。
- 千兆网卡：宿主机板载网口协商下降到百兆，阶段 3 压测之前要解决。

## 14. 面试时怎么串起来讲

一个三分钟的讲法：

1. **选型**：为什么 KVM 加 k3s，不是 kubeadm，不是 kind（第 1 节）；内嵌 etcd 是为学习而不是为可用性，单成员没有容错（第 2 节）。
2. **资源账**：可分配量公式和实测的数，预留要从第一天就写，内存是最紧的资源，实测系统组件比估计多约 1 GiB（第 3 节）。
3. **一次完整的排查**：节点拉不到镜像，原因是 RA 下发的 IPv6 DNS 不经代理，修在 cloud-init 里并通过重建证明（第 5 节、12 节）。
4. **入口链路**：MetalLB 二层（ARP，故障切换）加 `externalTrafficPolicy: Local` 保留源地址，是为了 1d 的按 IP 限流；Gateway API 的三层资源；ingress-nginx 退役；CRD 通道冲突（第 7 到 9 节）。
5. **存储**：local-path 和 NFS 各管什么，为什么数据库不放 NFS，群晖 ACL 的坑（第 10 节）。
6. **可重复**：5 分 28 秒重建，幂等，没有做的事情有清单（第 12、13 节）。

## 15. 面试官可能追问的短问题

- **etcd 的 p99 超过 10 毫秒怎么办？** 先说量到了什么（第 2.3 节），再说可能的层（qcow2、ext4、LVM、NVMe 本身），候选改法是给 server 的磁盘直接用逻辑卷；2b 有了负载再看。
- **节点内存紧时谁先被驱逐？** （通用知识）按 Pod 的 QoS 类别和实际用量超出 requests 的程度排序；本项目设了硬驱逐阈值 `memory.available<200Mi`，没有做驱逐实验，面试前核对官方文档。
- **MetalLB 的地址漂移多久？** 没有量，阶段 4 做；只能说原理（免费 ARP）。
- **为什么不上 Cilium？** 内存是最紧的资源，粗估多 1 到 1.5 GB（没实测），先量后定，且决定要在灌数据之前做，因为换 CNI 等于重建集群（第 2.4 节）。
- **Gateway API 版本为什么是 v1.6.3 而不是 v1.6.1 或 v1.7.0？** 跟着 k3s 自带的走，Envoy Gateway 的兼容性表写 v1.6.1，只差补丁号，不选上游最新（第 9.4 节）。
