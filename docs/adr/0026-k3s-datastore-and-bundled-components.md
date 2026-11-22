# ADR-0026：k3s 用内嵌 etcd；自带组件里保留 flannel、CoreDNS、local-path，禁用 Traefik 和 ServiceLB

- 状态：已采纳（CNI 已在 2b 改定为 flannel，见末尾）
- 日期：2026-11-15

## 背景

ADR-0001 选了 k3s，并留了一句：k3s 默认用 SQLite、自带 Traefik 和 ServiceLB，和标准发行版有差异，阶段 2a 要明确哪些保留、哪些替换。本篇把这件事定下来。

集群是 1 个 server 加 2 个 agent，跑在一台 32 GB 的宿主机的三台虚拟机里。

## 决定

| 组件 | k3s 的默认 | 决定 |
|---|---|---|
| 数据存储 | 单 server 时是 SQLite（经 kine 转成 etcd 接口） | **内嵌 etcd**（server 启动时加 `--cluster-init`） |
| CNI | flannel（VXLAN） | 保留 flannel，暂定 |
| 网络策略 | 内嵌的 kube-router 策略控制器 | 保留 |
| DNS | CoreDNS | 保留 |
| 入口 | Traefik | 禁用，换 Envoy Gateway（ADR-0025） |
| 负载均衡 | ServiceLB | 禁用，换 MetalLB（ADR-0025） |
| 本地存储 | local-path-provisioner | 保留 |
| 指标 | metrics-server | 保留（HPA 和 `kubectl top` 要用） |

## 理由

**内嵌 etcd。** 单个 server 用 SQLite 完全够用，选 etcd 是为了学习：

- 标准的 Kubernetes 控制面用的就是 etcd。快照和恢复、碎片整理、空间配额写满后集群变成只读、慢磁盘导致选主超时，这些是真实的运维问题，也是面试会问的，用 SQLite 一个都碰不到。
- 阶段 4 要对控制面做故障注入。etcd 有明确的可观测面（`etcdctl endpoint status`、自带的指标），SQLite 没有。
- 以后想加 server 做控制面高可用时不用迁移数据存储。

代价是 etcd 进程多占几百 MB 内存，并且对磁盘的写延迟敏感（它每次提交都要 fsync）。宿主机是 NVMe，虚拟机磁盘是其上的 qcow2，够不够用装好后看 etcd 的 `wal_fsync_duration`。

注意：只有一个 server 时 etcd 是单成员，**没有任何容错**，这个 server 挂了控制面就没了（已经在跑的 Pod 不受影响，但不能调度、不能改配置）。这和 SQLite 的情况一样，选 etcd 并没有让集群更可用。

**flannel 暂定。** 候选是 Cilium：基于 eBPF，可以取代 kube-proxy，自带按连接的可观测（Hubble），和可选的阶段 5（eBPF）也对得上。没有直接选它是因为内存：Cilium 每个节点一个 agent 加一个 operator，三个节点合计大约多用 1 到 1.5 GB（这是按公开资料的粗估，没有实测），而内存预算里的余量约 6 GB，还没有被阶段 3 的监控实际占用过。所以先用 flannel 把集群、中间件、服务都放上去，量出实际的内存占用再决定。

flannel 配上 k3s 内嵌的策略控制器已经能执行 NetworkPolicy，2d 的那一项不依赖换 CNI。

## 备选方案

- **SQLite**：最省内存。没选的原因见上。
- **外部数据库（MySQL、PostgreSQL）做数据存储**：k3s 支持，但这是 k3s 特有的用法，学到的东西带不走。
- **三个 server 的 etcd 集群**：才是真正的高可用控制面。三台虚拟机都跑控制面的话，留给工作负载的内存不够；而且三台虚拟机在同一台物理机上，物理机一挂全没，高可用的意义有限。
- **现在就上 Cilium**：见上。

## 后果

- 建集群的脚本要把这些选择写成 k3s 的配置文件（`/etc/rancher/k3s/config.yaml`），不靠命令行参数，重建时不会漏。
- 要有 etcd 快照的定时任务，并演练一次从快照恢复（k3s 自带 `k3s etcd-snapshot`，默认每 12 小时一次、留 5 份）。快照放到 NAS 上和 2b 的 MySQL 备份一起做。
- **换 CNI 等于重建集群**（要清掉每个节点上旧 CNI 的网卡和规则，所有 Pod 重建）。重建本来就是脚本化的（2a 的验收标准），但中间件的数据在本地盘上，重建前要先有备份和恢复。所以 CNI 的最终决定要在 2b 的数据变得“舍不得丢”之前做：**2b 的中间件第一次部署完、量过内存之后、灌数据之前，回来改定这一条。**
- flannel 的 VXLAN 封装让 Pod 之间跨节点的包多 50 字节的头，MTU 是 1450。阶段 3 压测时如果看到跨节点的吞吐明显低于同节点，先想到这里。

## 2a 的实测（2026-11-15）

- 空集群上 etcd 的 WAL fsync：43% 在 2 毫秒以内，59% 在 8 毫秒以内，99% 在 16 毫秒以内。p99 超过了常用的 10 毫秒参考线，原因没有查，没有看到后果。2b 上了中间件之后复查（设计文档 [2a-cluster.md](../design/2a-cluster.md) 第 12 节）。
- server 上 k3s 进程（apiserver、etcd、调度器等都在里面）占 638 MiB。空集群三个节点合计约 3.1 GiB，这是 CNI 决定的第一份输入。
- 重启宿主机后单成员的 etcd 自己恢复，集群 104 秒后全部就绪。
- flannel 的 MTU 和跨节点吞吐没有量。

## CNI 改定：保留 flannel（2026-11-15，阶段 2b）

按“后果”一节的约定，中间件第一次部署完、灌数据之前量了内存（设计文档 [2b-middleware.md](../design/2b-middleware.md) 第 10 节）。三个节点可分配合计约 24 Gi，空集群加全部中间件的 requests 已经承诺出去 13.1 Gi，剩约 10.9 Gi。还没上的监控、业务服务、Chaos Mesh 的预算合计 9 GB，放进去之后剩约 1.9 Gi。Cilium 估计多用 1 到 1.5 GB（仍然是公开资料的粗估），装上之后基本没有余量，agent-2 已经承诺了 62%。

决定保留 flannel。放弃的是用 eBPF 取代 kube-proxy、Hubble 的按连接可观测，以及它和阶段 5 的衔接；NetworkPolicy 不受影响（k3s 内嵌的策略控制器）。重新评估的条件：宿主机加内存，或者监控的实际占用明显低于预算。

