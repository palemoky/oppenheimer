# 2a 设计：集群搭建

本文是阶段 2a 的实现规格：在三台已经建好的虚拟机上装 k3s，配好镜像、存储、入口，并把“从空虚拟机到可用集群”做成可以重复执行的脚本。宿主机和虚拟机的准备已经做完（`scripts/cluster/host-setup.sh`、`vm.sh`，见 [infra/hardware.md](../infra/hardware.md)）。

已有的决定：k3s 跑在 KVM 虚拟机上（ADR-0001）；内嵌 etcd、保留 flannel（暂定）、禁用 Traefik 和 ServiceLB（ADR-0026）；入口用 Envoy Gateway 加 MetalLB（ADR-0025）。本文不重复它们的理由，只写怎么落地，以及这两篇 ADR 没有覆盖的选择。

状态：主人已确认（2026-11-15），第 2 节的六项都按建议。

## 1. 动工前查到的现状（2026-11-15）

| 项 | 现状 | 对设计的影响 |
|---|---|---|
| 三台虚拟机 | 在运行，内核 7.0.0-43，cgroup v2，没有 swap，ufw 没有启用，时间已同步，eth0 的 MTU 1500 | k3s 的前置条件都满足，不用再改系统 |
| 虚拟机的 DNS | systemd-resolved，上游是 192.168.50.2 和一个 IPv6 地址 `fd82:1609:af3::1` | 见 3.4：CoreDNS 会把这两个都当上游，IPv6 那个是谁、返回什么要先验证 |
| 虚拟机的地址 | 除了静态的 IPv4，还从路由通告里拿到了 IPv6 地址 | k3s 的 `node-ip` 要写明 IPv4，不让它自己挑 |
| 宿主机 | 有线仍是 100 Mbit/s（PCIe 网卡没装）；`br_netfilter` 没有加载；`FORWARD` 链默认 DROP（Docker 设的） | 见 3.5 和 4.3 |
| NAS | DSM 7.2.2，`/volume1` 是 Btrfs，剩 8.2 TB；**NFS 服务已经开着**（有一个给别的用途的导出，v3 和 v4 都在），装了 Container Manager（Docker 24.0.2）；ssh 进去的用户没有免密 sudo | 只缺一个共享文件夹和它的 NFS 权限，见 6.2 |
| k3s | 稳定通道是 v1.37.2+k3s1（当天从 `update.k3s.io` 核对） | 固定这个版本 |
| Envoy Gateway | v1.10 的兼容性表：Envoy v1.40.x，Gateway API **v1.6.1**，Kubernetes v1.34 到 v1.37 | Gateway API 用 v1.6.1（随 chart 装），不用上游最新的 v1.7.0；ADR-0025 已改 |

## 2. 主人确认的六项（都按建议）

| # | 问题 | 建议 | 主要的另一种做法 |
|---|---|---|---|
| 1 | 镜像仓库跑在哪 | 跑在集群里，数据经 NFS 放 NAS（4.2） | 跑在 NAS 的 Container Manager 里，集群只是它的客户端 |
| 2 | NFS 要不要动态供给 | 不装，2a 只用手写的 PV（6.3） | 装 csi-driver-nfs，多一套控制器和每节点一个 Pod |
| 3 | server 节点跑不跑工作负载 | 不加污点，可以调度；给系统留出资源（3.2） | 加 `NoSchedule` 污点，6 GB 里大半闲置 |
| 4 | 仓库的访问方式 | 局域网内明文 HTTP、不设登录，固定地址 192.168.50.50:5000（4.2） | 自签证书加登录，每个节点和 MacBook 都要装证书 |
| 5 | kubeconfig 放哪 | 单独的文件 `~/.kube/oppenheimer.yaml`，不并进 `~/.kube/config`（3.3） | 并进默认文件，和 OrbStack 的集群靠切换 context 区分 |
| 6 | 2a 结束前做一次完整的重建演练 | 做：删掉三台虚拟机，从头跑脚本，记下耗时（第 9 节） | 只在一台上验证脚本可重复 |

## 3. k3s

### 3.1 安装方式

用官方的安装脚本（`get.k3s.io`），用 `INSTALL_K3S_VERSION=v1.37.2+k3s1` 固定版本，不走通道：通道会随时间前移，重建出来的集群版本就变了。

所有选项写在 `/etc/rancher/k3s/config.yaml` 里，不用命令行参数（ADR-0026 的后果一节）。配置文件由仓库里的模板渲染后放到节点上，再运行安装脚本；重新运行时文件没有变化就不重启服务。

新脚本 `scripts/cluster/k3s.sh` 在 MacBook 上运行，直接 ssh 到三台虚拟机（MacBook 的密钥建虚拟机时已经写进去了），不经过宿主机。子命令：`install`（先 server 后 agent）、`kubeconfig`、`status`、`uninstall <节点>`。

### 3.2 配置

server（`deploy/cluster/k3s/server.yaml`）：

```yaml
cluster-init: true            # 内嵌 etcd（ADR-0026）
disable: [traefik, servicelb] # 换成 Envoy Gateway 和 MetalLB（ADR-0025）
node-ip: 192.168.50.40        # 节点还有 IPv6 地址，写明用哪个
tls-san: [192.168.50.40, k3s-server]
write-kubeconfig-mode: "0600"
etcd-expose-metrics: true     # 阶段 3 要抓 etcd 的指标，现在不开以后要重启控制面
kubelet-arg:
  - system-reserved=cpu=200m,memory=400Mi
  - kube-reserved=cpu=300m,memory=800Mi
  - eviction-hard=memory.available<200Mi,nodefs.available<5%,imagefs.available<5%
```

agent：`server: https://192.168.50.40:6443`、`token`、`node-ip`，以及同样三行 `kubelet-arg`（`kube-reserved` 的内存改成 300Mi，agent 上没有控制面）。

几个要理解的点：

- **不改 Pod 和 Service 的网段**（默认 10.42.0.0/16、10.43.0.0/16）。要避开的有三个：局域网 192.168.50.0/24、R2S 代理的假地址 198.18.0.0/16、宿主机上 Docker 的 172.17 和 172.31。默认值都不冲突。
- **预留资源为什么要写。** 节点的可分配量 = 总量 − `system-reserved` − `kube-reserved` − 驱逐阈值，调度器只看可分配量。不写预留的话可分配量约等于总量，Pod 的 requests 可以把节点排满，这时 kubelet、containerd、k3s 自己（server 上还包括 apiserver 和 etcd，都在 k3s 这一个进程里）没有保障，内存紧时先出问题的可能是它们。这个集群内存是最紧的资源，这几行值得从第一天就有。上面的数是起点，装好后对照实测的占用调整（9.2）。
- **server 不加污点**（待确认项 3）。它有 6 GB，控制面用不了那么多。风险是工作负载和 etcd 抢磁盘和内存：etcd 每次提交都要 fsync，延迟高了会影响整个控制面。所以约定：2b 的中间件用节点亲和放到两个 agent 上，server 上只放控制器一类的轻负载（MetalLB 和 Envoy Gateway 的控制器、镜像仓库）。如果实测 etcd 的 fsync 延迟被拖高，再加污点。
- **加入集群用的 token 不入库。** 脚本在加入时从 server 的 `/var/lib/rancher/k3s/server/node-token` 读出来，写进 agent 的配置文件（权限 600）。

### 3.3 kubeconfig

`k3s.sh kubeconfig` 把 server 上的 `/etc/rancher/k3s/k3s.yaml` 取回来，把地址从 `127.0.0.1` 换成 `192.168.50.40`，集群、用户、context 的名字从 `default` 改成 `oppenheimer`，存成 `~/.kube/oppenheimer.yaml`（权限 600，在仓库之外）。

不并进 `~/.kube/config`（待确认项 5）：MacBook 上还有 OrbStack 的单节点集群，两个集群靠“当前 context”区分的话，迟早会对着错的集群执行命令。仓库里的脚本和 Makefile 一律显式带 `--kubeconfig`；主人手工操作时可以 `export KUBECONFIG=~/.kube/oppenheimer.yaml`。

这份文件是集群管理员权限，等同于密钥。

### 3.4 Pod 出网和 DNS（要先验证）

R2S 的代理是 fake-ip 模式并且有白名单（hardware.md 的“网络”）。推演出来 Pod 出网应该是通的，但这条路上有三处是推断：

1. Pod 查域名 → CoreDNS → 节点的上游 DNS。k3s 发现节点用 systemd-resolved 时会改用 `/run/systemd/resolve/resolv.conf`，里面是 192.168.50.2 和 `fd82:1609:af3::1`。
2. R2S 返回 198.18.x.x 的假地址。Pod 往这个地址发包，出节点时源地址被改成节点地址（flannel 的 masquerade），节点在白名单里，R2S 接管连接。
3. **不确定的是那个 IPv6 的上游。** 如果它不是 R2S 的代理，CoreDNS 轮到它时返回的是真实地址，这条连接就不走代理，表现会是“同一个域名时通时不通”。

验证：装好后起一个临时 Pod，反复解析并访问 `ghcr.io`、`registry-1.docker.io`，看解析结果是不是都落在 198.18.0.0/16。如果不是，在节点上把 CoreDNS 的上游固定成 192.168.50.2（k3s 的 `resolv-conf` 选项指向一个只写这一行的文件）。

### 3.5 宿主机上的一个隐患：`br_netfilter`

虚拟机之间、虚拟机和局域网之间的帧都经过宿主机的 `br0`。现在这些帧不经过宿主机的 iptables，因为 `br_netfilter` 模块没有加载。一旦有东西把它加载起来（Docker 的某些网络模式、在宿主机上装 Kubernetes 相关的工具都会），桥上的帧就要过 `FORWARD` 链，而它的默认策略被 Docker 设成了 DROP：**三台虚拟机会同时断网**，而且原因很难联想到。

处理（加进 `host-setup.sh`）：按 libvirt 文档的做法加一条 udev 规则，模块被加载时立刻把 `net.bridge.bridge-nf-call-iptables`、`-ip6tables`、`-arptables` 设成 0。只写 sysctl 配置文件不够，因为这几个开关在模块加载之前不存在。

验证是一次计划内的小实验：在宿主机上手工 `modprobe br_netfilter`，确认三个开关是 0、虚拟机之间仍然互通，然后卸载模块。ssh 走 Wi-Fi，不受影响。

注意区分：k3s 会在**虚拟机里面**加载 `br_netfilter`，那是它需要的（Pod 之间经网桥的流量要过 iptables 才能做 Service 的转发），和宿主机无关。

## 4. 镜像

这里有两件独立的事：拉公共镜像，和放自己构建的镜像。

### 4.1 公共镜像：经 R2S 直接拉

节点已经在代理的白名单里，containerd 直接访问 `docker.io`、`ghcr.io`、`quay.io`、`registry.k8s.io`，不配国内镜像站。验证：在每个节点上用 `k3s crictl pull` 各拉一个小镜像，记下耗时。

已知的风险是 Docker Hub 对匿名拉取限速，按出口地址计数，而出口是代理的地址，可能和别人共用。真的被限速时的办法是再起一个仓库实例做 docker.io 的拉取缓存（distribution 的代理模式，一个实例只能代理一个上游，而且不能同时接受推送），在 `registries.yaml` 里把它配成 docker.io 的镜像。现在不做。

### 4.2 自己的镜像：集群里的仓库

- 软件：distribution（`registry`）v3.1.2。
- 位置：集群里的一个 Deployment（1 副本，更新策略 `Recreate`），数据卷是 NAS 上的 NFS 目录（6.3），固定调度到 server 节点。
- 地址：`LoadBalancer` 类型的 Service，由 MetalLB 固定分配 **192.168.50.50**，端口 5000。
- 每个节点的 `/etc/rancher/k3s/registries.yaml`：

```yaml
mirrors:
  "192.168.50.50:5000":
    endpoint: ["http://192.168.50.50:5000"]
```

不写 `http://` 的话 containerd 默认用 HTTPS。这个文件只在 k3s 启动时读，改了要重启 k3s，所以在装 k3s 之前就放好，仓库晚一点才起来也不要紧。

**为什么放在集群里（待确认项 1）。** 好处是它和其他东西一样用清单声明、入库，顺带把“手写 PV 挂 NFS”和“MetalLB 固定地址”各练一遍；集群重建后数据还在 NAS 上，镜像不用重新推。代价是循环依赖：仓库跑在它所服务的集群里。影响有限，因为仓库自己的镜像来自 Docker Hub，不从它自己拉；仓库不可用时，节点上已经缓存的镜像照常启动，只有拉新镜像会失败。放在 NAS 的 Container Manager 里没有这个循环，但那是 DSM 界面里手工建的容器，仓库里留不下完整的声明，而且 NAS 是 2 核的 J3355。

**为什么明文、不设登录（待确认项 4）。** 局域网里任何设备都能推镜像，这在家里的网络上可以接受。上 TLS 需要给三个节点和 MacBook 都装证书，2d 有了 cert-manager 之后再做才顺。地址直接用 IP，因为加域名要改 R2S 上的 DNS，而 R2S 不由本项目管理。Helm 的 values 里把仓库前缀做成一个变量，以后换地址只改一处。

MacBook 往明文仓库推镜像需要额外的设置（Docker 默认拒绝明文仓库），具体用哪种方式放到 2c 构建镜像时定。

### 4.3 带宽

MacBook 推镜像的路径是：MacBook → 路由器 → 宿主机网口 → 节点 → 仓库 → 宿主机网口 → NAS。宿主机的网口现在是百兆，一次推送要过它两次，上限约 11 MB/s。Go 服务的镜像几十 MB，可以接受；节点之间的流量不出宿主机，不受影响。PCIe 网卡装好后这个限制自然消失。

## 5. 负载均衡：MetalLB

- 版本 v0.16.2，用 Helm 装在 `metallb-system`。只用二层模式，不启用 BGP 的部分。
- 一个 `IPAddressPool`（192.168.50.50 到 .59）和一个 `L2Advertisement`。地址池关掉自动分配，每个 Service 用注解写明要哪个地址，地址的用途登记在下表和 hardware.md：

| 地址 | 用途 |
|---|---|
| 192.168.50.50 | 镜像仓库（5000） |
| 192.168.50.51 | 对外入口（Envoy Gateway，80；2d 加 443） |
| .52 到 .59 | 未分配（阶段 3 的 Grafana 等可以走入口，不必各占一个） |

二层模式的原理要讲得出来：某个节点上的 speaker 对这个地址的 ARP 请求作答，于是局域网里发往它的帧都到这个节点，再由 kube-proxy 的规则转给后端 Pod。它是故障切换，不是负载均衡：同一时刻只有一个节点接收流量。持有地址的节点挂了，其他 speaker 发现后由新的节点发免费 ARP 接管。

**`externalTrafficPolicy` 的选择会影响 1d 的限流。** 默认的 `Cluster` 下，流量可以从持有地址的节点转到别的节点上的 Pod，转发时源地址被改成节点地址，后端看到的客户端全是节点。`Local` 下只转给本节点上的 Pod，源地址保留，代价是 MetalLB 只会让“有就绪后端的节点”持有地址。gateway 的按 IP 限流（ADR-0019）依赖真实的客户端地址，所以入口的 Service 用 `Local`，再由 Envoy 把客户端地址写进 `X-Forwarded-For`；gateway 怎么信任这个头是 2c 的事（1d 设计文档 4.9）。仓库的 Service 不关心源地址，用默认值。

## 6. 存储

### 6.1 两类存储各管什么

| | local-path（k3s 自带，默认 StorageClass） | NFS（NAS） |
|---|---|---|
| 数据在哪 | 节点本地盘的 `/var/lib/rancher/k3s/storage` | NAS 的 `/volume1/k8s` |
| 给谁用 | 中间件的数据（MySQL、Redis、Kafka） | 镜像仓库、MySQL 备份、etcd 快照 |
| 节点挂了 | 数据跟着节点不可用，Pod 不能换节点 | 不受影响 |
| NAS 关机 | 不受影响 | 挂载的 Pod 卡在读写上 |

**数据库的数据不放 NFS。** 数据库靠 fsync 保证提交过的事务落盘，靠文件锁保证只有一个实例在写；NFS 上这两样的语义都比本地盘弱，延迟还要加上一次网络往返（现在还是百兆）。所以 NFS 只放“写完整个文件、之后只读”的东西。

local-path 的 PV 带着节点亲和：卷建在哪个节点，Pod 以后就只能调度到那个节点。它的 StorageClass 是 `WaitForFirstConsumer`，先定 Pod 去哪，再在那个节点上建卷。它不限制容量，PVC 里写的大小只是个声明。这三点 2b 排布中间件时都要用到。

### 6.2 NAS 上要建的东西（主人在 DSM 里操作）

只要**一个共享文件夹**，里面的子目录由脚本从节点上经 NFS 创建。

1. 控制面板 → 共享文件夹 → 新增：名称 `k8s`，位置“存储空间 1”（`/volume1`）。不加密，不启用回收站。
2. 编辑这个共享文件夹 → “NFS 权限” → 新增一条规则：

| 字段 | 值 | 说明 |
|---|---|---|
| 服务器名称或 IP 地址 | `192.168.50.40/29` | 覆盖 .40 到 .47，即三个节点和预留的 .43、.44。Pod 挂 NFS 是节点上的 kubelet 去挂的，NAS 看到的来源是节点地址，不是 Pod 的 |
| 权限 | 可读写 | |
| Squash | 无映射 | 见下 |
| 安全性 | sys | |
| 启用异步 | 不勾 | 勾上后 NAS 在数据落盘前就答复写入成功，断电会丢已经确认的写入；这里放的是备份 |
| 允许来自非特权端口的连接 | 不勾 | |
| 允许用户访问已装载的子文件夹 | 勾选 | |

3. 控制面板 → 文件服务 → NFS：确认“最大 NFS 协议”是 NFSv4.1（服务本身已经开着）。

**Squash 选“无映射”的含义。** NFS 默认用客户端报上来的用户号做权限判断。“无映射”就是原样相信：节点上的 root 在这个共享文件夹里也是 root，可以建目录、改属主。这样脚本能从节点上把子目录建好并交给对应的用户号，之后 NAS 上不再需要 root 或 sudo。代价是这 8 个地址上任何拿到 root 的人对这个共享文件夹有完全的权限，范围只限于这一个文件夹。

**这一项没有实测过**：群晖的共享文件夹带自己的 ACL，非 root 的用户号能不能写要挂上之后才知道。验证见 9.1；不行的话退到“映射所有用户为 admin”（所有写入都记成 admin，任何用户号都能写，但改属主会失败）。

目录规划（脚本创建）：

| 目录 | 用途 | 什么时候用 |
|---|---|---|
| `/volume1/k8s/registry` | 镜像仓库的数据 | 2a |
| `/volume1/k8s/backup/mysql` | MySQL 备份 | 2b |
| `/volume1/k8s/backup/etcd` | etcd 快照的副本 | 2b |

### 6.3 不装动态供给（待确认项 2）

2a 和 2b 加起来只有三个 NFS 卷，各写一个 PV（`nfs.server`、`nfs.path`，`persistentVolumeReclaimPolicy: Retain`，挂载选项 `nfsvers=4.1,hard`）和一个按名字绑定的 PVC 就够了。动态供给（csi-driver-nfs）要多一个控制器和每节点一个 Pod，解决的是“很多人随时要卷”的问题，这里没有。阶段 3 如果要把监控数据放 NAS 再评估；另外 Prometheus 的官方文档不支持把数据目录放在 NFS 上，hardware.md 里“监控长期存储放 NAS”那一项到时要重新想。

etcd 的快照不直接写 NFS：快照目录挂着 NFS 的话，NAS 关机时 k3s 的快照任务会卡住。做法是快照写本地（k3s 默认每 12 小时一次、留 5 份），再定时复制到 NAS，属于 2b。

## 7. 入口：Envoy Gateway

- 版本 v1.10.0，用它的 Helm chart 装在 `envoy-gateway-system`，Gateway API 的 CRD 用 k3s 自带的（v1.6.3），不随 chart 装（发现 4）。
- 资源：一个 `GatewayClass`；一个 `Gateway`（名字 `public`，监听 80）；一个 `EnvoyProxy` 用来定制数据面（副本数、资源限额、Service 的 MetalLB 注解和 `externalTrafficPolicy: Local`）。
- 2a 只验证通路：部署一个回显请求的小服务和一条 `HTTPRoute`，从 MacBook 访问 `http://192.168.50.51/`，确认返回里看到的客户端地址是 MacBook 的而不是节点的。验证完删掉回显服务，`Gateway` 留着给 2c。

三层资源的分工：`GatewayClass` 说“这类入口由哪个控制器实现”；`Gateway` 说“要一个入口，监听哪些端口”，Envoy Gateway 看到它才去创建 Envoy 的 Deployment 和 Service；`HTTPRoute` 说“哪些请求转给哪个 Service”，由各个服务自己的 chart 带着。和 Ingress 的区别在于把“基础设施怎么提供”和“应用怎么路由”分给了不同的资源和不同的人，超出标准的能力用策略资源挂上去，不靠注解。

流量路径（2c 之后）：客户端 → 192.168.50.51（MetalLB，ARP）→ 持有地址的节点 → Envoy（入口）→ gateway 服务 → gateway 的边车 → order 等。入口这一层只做路由，不限流（ADR-0025）。

## 8. 脚本和目录

```
scripts/cluster/
  cluster.env                 已有：地址和规格；新增版本号和固定的 LB 地址
  on-host.sh host-setup.sh vm.sh   已有；host-setup.sh 加 3.5 的 udev 规则
  k3s.sh                      新：install、kubeconfig、status、uninstall
  addons.sh                   新：按顺序装 MetalLB、NFS 目录和仓库、Envoy Gateway；smoke 子命令做入口的通路检查
deploy/cluster/
  k3s/                        server、agent 的配置模板，registries.yaml
  metallb/                    values、地址池
  registry/                   PV、PVC、Deployment、Service
  envoy-gateway/              values、GatewayClass、Gateway、EnvoyProxy
  smoke/                      回显服务和 HTTPRoute
```

Makefile 加 `cluster-up`（建虚拟机、`k3s.sh prepare`、装 k3s、取 kubeconfig、装组件，每一步都可重复执行）、`cluster-status`、`cluster-smoke`。删除虚拟机不做成 make 目标，仍然要显式运行 `vm.sh destroy <名字>`。

Helm 的 chart 版本写在 `cluster.env` 里，用 `helm upgrade --install` 装，不引入 helmfile；GitOps（Argo CD）是 2d 的事。

**重建 = 删虚拟机再从头跑一遍**，不用 `k3s-uninstall.sh`：从干净的系统盘开始才能证明脚本没有依赖手工留下的状态，而重建一台虚拟机只要一两分钟。重建后仓库的数据还在 NAS 上。

## 9. 验证

### 9.1 每一步做完要看到的

| 步骤 | 检查 |
|---|---|
| k3s | 三个节点 `Ready`；`kubectl get nodes -o wide` 的地址是 IPv4；Traefik 和 ServiceLB 的 Pod 不存在；各节点的可分配量符合预留 |
| DNS 和出网 | 3.4 的检查；Pod 之间跨节点互通 |
| 宿主机 | 3.5 的实验 |
| 公共镜像 | 四个镜像源各拉一个，三个节点都成功 |
| MetalLB | 一个测试 Service 拿到指定的地址，从 MacBook 能访问 |
| NFS | 在一个节点上手工挂载：root 能建目录、能 `chown`；用户号 1000 能写属于自己的目录、不能写别人的；用 `dd` 量一次写入速度 |
| 仓库 | 从 MacBook 推一个镜像，三个节点各起一个用它的 Pod；删掉仓库的 Pod，重建后镜像还在 |
| 入口 | 第 7 节的回显检查 |
| 重启 | 重启宿主机一次，不做任何操作，集群自己恢复到全部 `Ready` |
| 重建 | 删掉三台虚拟机，`make cluster-up` 一次跑通，记下总耗时；之前推的镜像还能拉 |

### 9.2 要量并记下来的数

- 每个节点装完 k3s、装完全部组件之后的内存占用，以及各组件的 Pod 占多少。这是 ADR-0026 里 CNI 决定的第一份输入，也用来校正 3.2 的预留值。
- etcd 的 `wal_fsync_duration` 的 p99（常见的参考线是 10 毫秒以内）。虚拟机磁盘是 qcow2、`cache=none`，够不够要看这个数。
- 公共镜像的拉取速度；NFS 的写入速度（预期被百兆卡在 11 MB/s 左右）。
- 持有入口地址的节点被关掉后，入口多久恢复。只粗看一次，细致的测量在阶段 4。

### 9.3 重建演练是破坏性操作

按 AGENTS.md 的规则，删虚拟机之前要主人确认（待确认项 6）。2a 结束时集群里还没有数据，这是做这次演练代价最小的时候。

## 10. 实现顺序和分工

| 步 | 内容 | 谁做 |
|---|---|---|
| 一 | `host-setup.sh` 的 udev 规则和实验；`k3s.sh`、k3s 的配置；装 k3s；kubeconfig；3.4 的验证 | 脚本派 Sonnet，主会话评审并在集群上执行 |
| 二 | 公共镜像的拉取验证 | 主会话 |
| 三 | MetalLB | 清单派 Sonnet |
| 四 | NFS（等主人建好共享文件夹）、仓库 | 清单派 Sonnet，NFS 权限的验证主会话做 |
| 五 | Envoy Gateway 和回显检查 | 清单派 Sonnet |
| 六 | `addons.sh`、Makefile、`smoke.sh`；重启和重建演练；把实测的数填回本文，登记 `versions.md`，写笔记 | 主会话执行，文档派 Sonnet |

所有在集群上的执行都由主会话做：1d 的教训是没用过的组件要先把行为跑出来，文档里的默认值靠不住。每装一个组件，先看它实际创建了什么、占了多少内存，再往下走。

第三步和第五步的清单互不相干，可以并行写；执行是串行的。

## 11. 实现中的发现（2026-11-15）

都是装的时候跑出来的，按碰到的顺序。

**1. 节点自己就连不上镜像源：路由通告里的 IPv6 DNS 不经过代理。** 3.4 担心的是 Pod，实际上节点先中招：k3s 装完后系统 Pod 全部卡在 `ContainerCreating`，原因是拉不到 `pause` 镜像。节点的 DNS 上游有两个，192.168.50.2 返回代理的假地址（198.18.x.x），而路由通告下发的 `fd82:1609:af3::1` 返回的是被污染的真实地址（`registry-1.docker.io` 解析成了一个不相干的地址）。systemd-resolved 选中后者时所有按域名的出网都超时。建虚拟机时的检查之所以通过，是因为当时它恰好用的是前一个。

排查的顺序：Pod 的事件说拉不到 `pause` 镜像；在节点上 `curl` 镜像源超时；`getent hosts` 返回的不是 198.18.x.x；用 `nslookup` 分别指定两个上游查同一个域名，一个返回假地址、一个返回公网地址。`resolvectl query` 分不出是哪个上游答的，要直接对每个上游发查询。

修法在节点一侧（R2S 不归本项目管）：netplan 里 `accept-ra: false`、`dhcp6: false`，节点不再接受 IPv6 的路由通告，DNS 上游只剩 192.168.50.2。已写进 `vm.sh` 的 network-config。之后三个节点上的 Pod 反复解析四个镜像源，结果全部落在 198.18.0.0/16。

**2. 群晖上经 NFS 新建的目录，属主自己也进不去。** 共享文件夹的根目录带群晖的 ACL，权限位显示 777，但非 root 的用户号列不了目录。root 新建的子目录继承这份 ACL，`chown` 给用户号 1000 之后，1000 仍然写不进去。对目录做一次 `chmod` 之后它变成普通的权限位，属主才能读写，别的用户号被拒绝。另外，从共享文件夹的根挂载再走进子目录不行（根目录过不去），**要直接挂载子目录**。所以规则是：子目录由脚本以 root 建好，逐个 `chown` 加 `chmod`；PV 的路径写到子目录。Squash 保持“无映射”，不用退到“映射为 admin”。

**3. MetalLB 的 chart 默认装 frr-k8s。** 那是 BGP 的后端，每个节点一个 5 个容器的 Pod（各约 60 Mi）再加一个清理器，二层模式用不到。关的开关是 `frrk8s.enabled=false`；`speaker.frr.enabled` 是已弃用的旧后端，只关它没有用。

**4. k3s 自己装了 Gateway API 的 CRD，Envoy Gateway 的 chart 不能再装一遍。** k3s v1.37 带一个 Helm release `gateway-api-crd`（v1.6.3，标准通道），禁用 Traefik 之后它还在。Envoy Gateway 的主 chart 自带实验通道的 CRD，安装时被 Gateway API 的准入策略 `safe-upgrades` 拒绝（不允许用实验通道覆盖标准通道）。做法：用它单独的 `gateway-crds-helm` 只渲染 Envoy Gateway 自己的 CRD 并用服务端 apply 提交（整个作为 Helm release 安装会超过 Secret 的 1 MiB 上限），主 chart 加 `--skip-crds`。结果是 Gateway API 的版本跟着 k3s 走（v1.6.3，兼容性表写的是 v1.6.1，只差补丁号）；升级 k3s 时要对照兼容性表。

**5. `eviction-hard` 是整体覆盖。** 只写 `memory.available` 会把默认的磁盘阈值一起去掉，所以把 `nodefs.available<5%`、`imagefs.available<5%` 一并写上了。

**6. 往仓库推多架构镜像，`ctr` 和 `docker save` 都不行。** 节点上只拉了一个架构，`ctr images push` 会连着索引里其他架构的清单一起推，仓库以“blob 不存在”拒绝。测试时改用 skopeo 直接从 Docker Hub 复制单个架构。MacBook 的 Docker 推明文仓库被拒绝（`server gave HTTP response to HTTPS client`），符合 4.2 的预期，2c 要定用哪种方式。

**7. 重建后主机密钥会变。** 新建的虚拟机地址不变、主机密钥是新的，MacBook 的 `known_hosts` 里是旧的，ssh 拒绝连接。加了 `k3s.sh prepare`：连不上时删掉旧密钥、接受新的，再等 cloud-init 做完（它要装 `nfs-common`）。

## 12. 结果（2026-11-15）

第 9 节的检查：

| 检查 | 结果 |
|---|---|
| k3s | 三个节点 `Ready`，v1.37.2+k3s1，containerd 2.3.7，地址是 IPv4；第二次 `install` 三个节点都是 unchanged，不重启 |
| 可分配量 | server 3500m、约 4.4 Gi（总量 5.8 Gi）；agent 各 7500m、约 9.8 Gi（总量 10.7 Gi） |
| DNS 和出网 | 发现 1 修掉之后通过；Pod 跨节点互通，延迟约 0.4 毫秒 |
| 宿主机的 `br_netfilter` | 加载模块后三个开关立刻是 0；期间虚拟机之间 60 个 ping 没有丢。“开关是 1 时虚拟机断网”没有复现过，是按 `FORWARD` 链的默认策略推出来的 |
| 公共镜像 | 四个镜像源在三个节点上都拉得到，小镜像每个 6 到 15 秒 |
| MetalLB | 仓库拿到 .50、入口拿到 .51，从 MacBook 能访问；两个地址当时都由 k3s-agent-2 应答。入口是 `Local` 而且 Envoy 只有 1 个副本，所以它的地址只能由 Envoy 所在的节点持有；仓库是 `Cluster`，哪个节点持有都行 |
| NFS | 见发现 2；写入 200 MiB 用了 19 秒，11 MB/s，和百兆链路的预期一致 |
| 仓库 | 推入的镜像三个节点都能拉；删掉仓库的 Pod 重建后镜像还在 |
| 入口 | 回显服务看到的 `X-Forwarded-For` 是 MacBook 的地址，不是节点的 |
| 重建 | 删掉三台虚拟机后 `make cluster-up` 一次跑通，**5 分 28 秒**（建虚拟机、等 cloud-init、装 k3s、装三个组件）。重建后节点全部 `Ready`、没有异常的 Pod、之前推的镜像还在、入口检查通过、节点的 DNS 上游只有 192.168.50.2（发现 1 的修复是从脚本来的，不是手工补的）、再跑一次 `install` 三个节点都是 unchanged。日志在 `tmp/cluster-up-rebuild.log` |
| 重启宿主机 | `systemctl reboot` 之后不做任何操作：33 秒时宿主机断开，85 秒时宿主机的 ssh 和 apiserver 都已恢复，104 秒时三个节点 `Ready`、所有 Pod `Running`、仓库应答。虚拟机是被正常关机的（libvirt-guests，k3s 的日志里是正常停止），各 Pod 重启 1 次；之后入口检查通过，仓库里的镜像还在，宿主机上原有的容器也自己起来了 |

内存（空集群，装完全部组件之后，`free` 的 used）：

| 节点 | 只有 k3s | 加上 MetalLB、仓库、Envoy Gateway |
|---|---|---|
| k3s-server | 1052 MiB | 1483 MiB |
| k3s-agent-1 | 630 MiB | 771 MiB |
| k3s-agent-2 | 607 MiB | 828 MiB |

server 上 k3s 进程自己占 638 MiB（apiserver、etcd、调度器等都在里面），containerd 约 190 MiB。全部 Pod 加起来 239 Mi：Envoy Gateway 的控制器 41 Mi、Envoy 38 Mi、MetalLB 的控制器 35 Mi 加三个 speaker 各约 22 Mi、仓库 12 Mi、CoreDNS 13 Mi、metrics-server 21 Mi、local-path 8 Mi。三个节点合计约 3.1 GiB，比 hardware.md 里“K8s 系统组件 2GB”的估计多 1 GiB 左右。

**etcd 的 fsync 延迟偏高。** 装完后约 7400 次 WAL fsync 的分布：43% 在 2 毫秒以内，59% 在 8 毫秒以内，99% 在 16 毫秒以内。p99 落在 8 到 16 毫秒这一档，超过了 10 毫秒的参考线，而且分布是两头多、中间少。空集群上没有看到选主超时之类的后果。原因没有查：可能是 qcow2 加 ext4 加 LVM 这几层，也可能是这块消费级 NVMe 本身的刷盘延迟。2b 上了中间件、磁盘有了竞争之后要再看一次；要改的话候选是给 server 的磁盘换成直接用逻辑卷。

没做的：

- `smoke.sh` 没有单独写，入口的检查在 `addons.sh smoke`（`make cluster-smoke`）里；DNS、镜像拉取、仓库推拉这几项是手工跑的，没有自动化。
- 持有入口地址的节点被关掉后多久恢复，没有量（阶段 4）。
- 入口的 Envoy 只有 1 个副本，它所在的节点挂了入口就断到 Pod 被重建为止。阶段 4 之前要改成 2 个并分散到两个节点。
- 版本登记、笔记见 PROGRESS。

## 13. 不在 2a 做的

- NFS 的动态供给（6.3）；etcd 快照复制到 NAS、MySQL 备份（2b）。
- 仓库的 TLS、登录、垃圾回收；Docker Hub 的拉取缓存（4.1）。
- k3s 内嵌的镜像分发（Spegel，节点之间互相提供已有的镜像）：能少走几次代理，但多一个要理解的东西，拉取慢到成为问题时再开。
- 入口的 TLS（2d，cert-manager）；入口层的限流（ADR-0025）。
- 控制面其他组件（调度器、控制器管理器、kube-proxy）的指标端口，阶段 3 接监控时再开。
- 换 Cilium（ADR-0026，2b 量过内存之后决定）。
- 千兆网卡：不挡 2a，阶段 3 压测之前要解决。
