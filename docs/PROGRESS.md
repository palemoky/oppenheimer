# 进度与断点

每次会话结束时更新本文件。新会话先读这里。

## 当前状态

- **当前阶段**：2b（中间件上集群）进行中：三种中间件已部署并验证，剩备份、恢复演练和收尾。2a 已完成并合并。阶段 1 全部完成并合并进 `main`（PR #7、PR #8）。
- **当前分支**：`phase-2b`（从 `phase-2a` 开出；`phase-2a` 已经由 PR #9 合并进 `main`）。2b 已推送，draft PR #10，HEAD `993c5c6` 之后还有一个只改 PROGRESS 的提交。CI 27 项全部通过（2026-11-15；最长的是 `test (./services/order)` 4 分 09 秒、e2e 3 分 52 秒、`test (./pkg)` 3 分 24 秒，没有明显变慢的 job）。工作区是干净的。
- **最后更新**：2026-11-15

### 2a 做到哪（2026-11-15）

已完成：

- 摸清了三台设备的现状，地址规划写进了 `scripts/cluster/cluster.env` 和 [infra/hardware.md](infra/hardware.md) 的“网络”。
- 宿主机（`ssh ubt`，xinyu 已有免密 sudo）装好了 KVM 和 libvirt（qemu 10.2.1、libvirt 12.0.0、virtinst、cloud-image-utils、nfs-common），xinyu 已加进 `libvirt`、`kvm` 组。
- `scripts/cluster/host-setup.sh`（装包、建网桥 `br0`、登记 libvirt 网络 `host-bridge`、停掉默认的 NAT 网络）和 `on-host.sh`（从 MacBook 把脚本推到宿主机执行）已跑通，重复运行没有变化。宿主机的有线网卡现在接在 `br0` 上，地址是静态的（写在 `/etc/netplan/10-cluster-bridge.yaml`），默认路由走 `br0`；Wi-Fi 没有动，`ssh ubt` 仍然走它。
- Ubuntu 26.04 的 cloud image 已下到宿主机的 `/var/lib/libvirt/images/base/`，SHA256 核对过。
- 换盘方案写在 `tmp/disk-migration.md`（本地文档，不入库）。

- 宿主机整机备份在 NAS 的 `/volume1/homes/me/backups/ubt-20261115/`（2026-11-15）：`metadata.tar`（分区表、LVM 元数据、fstab、启动项、软件包清单）和 `rootfs.tar.zst`（`/`、`/boot`、`/boot/efi`，16.8 GB，两端 SHA256 一致；没有做过恢复演练）。
- **网线拔掉时宿主机从 IPv4 连不上的问题已修**：`br0` 是静态地址，没有载波时路由还在而且比 Wi-Fi 的优先。修法是 `ignore_routes_with_linkdown=1`（已写进 `host-setup.sh`）。再遇到 IPv4 连不上时可以走 Wi-Fi 网卡的 IPv6 链路本地地址（在 MacBook 上 `ping6 ff02::1%en0` 找邻居）。
- 板载网口确认是硬件坏了（只通告千兆时链路起不来，别的电脑接同一个路由器口是千兆）。主人已把网线拔掉，现在只走 Wi-Fi；长期方案是加一块 PCIe 网卡，显示器（U2723QX）的 USB 网口可以临时顶替。**网线没插时 `host-setup.sh` 会因为没有链路而退出**（`ALLOW_NO_LINK=1` 可以跳过）。
- 换盘定了走方案 A（没有硬盘盒）：主人拔 Windows 盘装 1TB，主会话经 ssh 做迁移，主人再换回。

- **换盘（方案 A）的软件步骤已做完并重启验证过**（2026-11-15）：1TB 分成 EFI 1G、`/boot` 2G、LVM 928G；根分区用 `pvmove` 在线搬到新盘，旧盘已移出卷组；`/boot`、EFI 复制过去，`/etc/fstab` 换了两个 UUID（原件在 `/etc/fstab.before-migration`）；GRUB 装到新盘，主板启动项 `Boot0001` 指向新盘的 EFI 分区。卷组里还有约 698G 没分配。250GB 的旧盘按主人要求已清空（分区表和签名都抹掉，整盘 discard），指向它的启动项已删。虚拟机磁盘的卷已建：`/dev/ubuntu-vg/vmpool`，600G，ext4，挂在 `/var/lib/libvirt/images`（fstab 里带 `nofail`），cloud image 已挪进去并重新核对过；卷组还剩约 98G 没分配。**剩下的**：主人关机拔掉 250GB、把 Windows 盘装回 `M2_1`，开机后主会话确认启动顺序和 Windows 的启动项。
- `br0` 在 netplan 里加了 `optional: true`：没插网线时开机不再等 2 分钟（等待期间 ssh 会以 pam_nologin 拒绝登录）。

- **换盘全部完成**：Windows 盘已装回 `M2_1`，开机自动进 Ubuntu，启动顺序是 Ubuntu、Windows Boot Manager（Windows 没有实际启动验证过）。
- **三台虚拟机已建好**（`scripts/cluster/vm.sh`，经 `scripts/cluster/on-host.sh vm.sh create` 调用；规格和地址见 [infra/hardware.md](infra/hardware.md)）。从 MacBook 用 `ubuntu` 用户直接 ssh 到节点地址能登录；三台都能经 R2S 访问 GitHub、Docker Hub、get.k3s.io，互相按名字能 ping 通。还没有装 k3s。

- **2a 的集群已建成并做过一次完整的重建演练**（2026-11-15）。设计文档 [design/2a-cluster.md](design/2a-cluster.md)（主人确认了第 2 节的六项，都按建议），实现中的七个发现在第 11 节，结果和实测的数在第 12 节。现状：
  - k3s v1.37.2+k3s1，1 server 加 2 agent，内嵌 etcd，flannel；MetalLB（二层模式）；镜像仓库（集群里，数据在 NAS 的 `/volume1/k8s/registry`，明文 HTTP）；Envoy Gateway v1.10.0，`Gateway` 叫 `public`，在 `envoy-gateway-system`，监听 80。固定地址见 hardware.md 的“网络”和 `cluster.env`。
  - 用法：`make cluster-up`（从空宿主机到可用集群，可重复执行，重建实测 5 分 28 秒）、`make cluster-status`、`make cluster-smoke`。kubeconfig 在 `~/.kube/oppenheimer.yaml`，**不在默认的 `~/.kube/config` 里**，手工操作要 `export KUBECONFIG=~/.kube/oppenheimer.yaml`。删虚拟机是 `scripts/cluster/on-host.sh vm.sh destroy <名字...>`。
  - NAS 的共享文件夹 `k8s` 主人已建好（NFS 权限按设计文档 6.2），NAS 不需要 root。子目录要直接挂载，并且建好后要 `chmod` 一次（设计文档第 11 节的发现 2）。
  - 宿主机加了 `br_netfilter` 的 udev 规则（`host-setup.sh`），做过实验。
  - 仓库里留着一个测试镜像 `test/busybox:1.37.0`，可以不管。

停在哪、在等什么：

1. 2a 没有在等的事。重启宿主机的检查已做（2026-11-15）：不做任何操作，104 秒后集群全部恢复。宿主机上有主人自己的一个 traefik 容器，以后重启宿主机前仍然先说一声。
2. **PCIe 网卡还没装，有线仍是 100 Mbit/s**。装好之后：确认设备在线，把 `scripts/cluster/cluster.env` 的 `HOST_NIC` 改成新网卡的名字（`ip -br link` 看，板载的是 `enp39s0`，建议插 `PCI_E5`），重跑 `scripts/cluster/on-host.sh host-setup.sh`，用 `ethtool` 确认协商到千兆。改网桥时 ssh 走 Wi-Fi（`ssh ubt`），不要走有线的地址。阶段 3 压测之前要解决；在那之前往仓库推镜像约 11 MB/s。
3. 出网的白名单由主人管，**不要去改 R2S**。R2S 经路由通告下发的 IPv6 DNS 不走代理、返回被污染的地址，节点一侧已经绕开（`accept-ra: false`）；局域网里其他在白名单内的设备可能有同样的问题，已告诉主人。

下一步：

1. **2b 的中间件已全部部署并验证，剩备份、演练和收尾。** 分支 `phase-2b`。PR #9（2a）已合并进 `main`（2026-11-15）；2b 到目前为止的改动已分提交推送，在 draft PR 里（见“当前状态”）。设计文档 [design/2b-middleware.md](design/2b-middleware.md)，实现中的九个发现、量到的数和“还没做的”都在它的第 10 节。
   - **集群在 2026-11-15 重建过一次**（主人确认）。原因是节点上的 AppArmor 拦掉了容器里进程之间的全部信号：官方的 MySQL 镜像起不来，所有 Go 程序的抢占信号被拒绝。根因是 Ubuntu 26.04 默认的 `kernel.apparmor_restrict_unprivileged_unconfined=1`，主人在一个节点上手工验证后同意写进 `vm.sh` 的 cloud-init 并重建。重建后三个节点没有任何信号被拒绝的记录。
   - 现在集群上有：两套 Redis（`redis-gate` 主从加 Sentinel、`redis-cache` Cluster）、Strimzi 1.3.1 加 Kafka 4.3.2（3 个 broker）、MySQL 9.7.4（`mysql-order-0`、`mysql-order-1` 各一主一从，`mysql-main` 单实例）。`make cluster-up` 之后 `make middleware-up`（或 `scripts/cluster/middleware.sh redis|kafka|mysql`），都可重复执行。MySQL 的密码是脚本在集群里随机生成的 Secret（`<release>-auth`），重建后会变。
   - MySQL 的主从由 values 里的 `primaryOrdinal` 决定（不是 Pod 的标签，发现 2）；`middleware.sh mysql` 最后只读地核对实际状态，不一致时非零退出。
   - Go 代码：`pkg/redisx` 三种模式（地址写法 `sentinel://gate@...`、`cluster://...`）、`pkg/ratelimit` 在 Cluster 下按桶调用（ADR-0019 的补充）、调用方改成 `redis.UniversalClient`、`kafkax` 建 topic 的副本数用 broker 的默认值。全部 11 个模块 `make lint` 0 issues、`make build` 通过；`pkg`、四个服务和 `test/load` 的 `make test` 通过；`make e2e` 通过（都是 2026-11-15）。第一次推送时漏改了 `test/load`（它也用旧的客户端类型），CI 有 3 项编译失败，已修。
   - CNI 已改定为保留 flannel（ADR-0026 末尾）。
   - 本机：OrbStack 配了 `insecure-registries`，可以直接 `docker push 192.168.50.50:5000/...`；多了一个可选的测试环境 `make redis-modes-up`（11 个容器，现在在运行，不用时 `make redis-modes-down`）。
2. **新会话从这里开始：etcd 的恢复演练**（设计文档 2b 的 7.2；主人 2026-11-15 说在新会话里做）。它会把控制面重置到快照时的状态，最坏情况是 `make cluster-up` 加 `make middleware-up` 重来（约 20 分钟）；主人确认过要做，动手前仍然说一声。演练之前先把 etcd 快照同步到 NAS 的定时器做出来。
3. **e2e 在 CI 上偶发失败过一次，原因查到了，没有修**（PR #10，`993c5c6` 的第一次运行；重跑通过）。`TestCancelReturnsStockOnce` 轮询订单是否落库时收到 503 `STOCK_GATE_UNAVAILABLE`。order 的日志里没有任何闸门或熔断的报错，所以走的是 `errReadBusy`：展示读读闸门占用的并发名额（`READ_GATE_MAX_CONCURRENCY`，默认 16，阶段 1 收尾时加的）满了，而订单还没落库，服务按设计回“说不清状态”的 503（`services/order/internal/service/service.go` 里 `errHoldPending` 那一段，这种情况有意不记日志）。用例的 `getOrder` 把任何非 200 当成致命错误（`test/e2e/e2e_test.go`）。和这次的 Redis 客户端改动无关。两种修法：轮询里把这个 503 当成“还没好，继续等”；或者 e2e 里把名额调大。涉及读路径的降级语义，先想清楚客户端该怎么对待这个 503 再改。
4. 2b 剩下的其他项（设计文档第 10 节“还没做的”）：给两个 Redis 的 chart 加 `priorityClassName`；MySQL 备份的 CronJob 和 etcd 快照同步到 NAS，以及两次恢复演练（etcd 的那次会重置控制面，动手前和主人说）；重启宿主机的检查；切换手册。面试笔记已写：`docs/notes/2b-middleware.md`（Sonnet 写，主会话抽查；只覆盖已经做了的部分，备份和演练做完后要补）。注意：`deploy/charts/mysql/files/init-replica.sh` 在部署之后改过一处无害的笔误，下次 `middleware.sh mysql` 会因为 ConfigMap 变了而滚动重启三个 MySQL 实例。没有查到的：Oracle MySQL Operator 的最新版本。
3. 2b 里要回头看的：etcd 的 fsync 延迟（空集群上 p99 在 8 到 16 毫秒，超过 10 毫秒的参考线，原因没有查）；装完中间件、灌数据之前量内存定 CNI（ADR-0026）；空集群三个节点已经用了约 3.1 GiB，比预算里的 2 GB 多。
4. 2c 要定的：MacBook 的 Docker 推明文仓库被拒绝，改 OrbStack 的 `insecure-registries` 要重启 Docker（会重启本机的 compose 依赖），或者用 skopeo、crane 一类的工具推。
5. 2a 的面试笔记已写：`docs/notes/2a-cluster.md`（Sonnet 写，主会话抽查并补了 DNS 排查的步骤和重启宿主机一节）。标了“通用知识”的段落面试前要对照官方文档核对。

2a 用到的版本都已登记到 `versions.md`。已查证、还没有用到的：Cilium v1.20.4。

主人已定的（2026-11-15；前三项已写成 ADR-0025、ADR-0026）：

- **入口控制器用 Envoy Gateway**（查到的最新版 v1.10.0，Gateway API 按它的兼容性表用 v1.6.1），取代已停止维护的 ingress-nginx；负载均衡用 MetalLB（地址池见 hardware.md）。k3s 自带的 Traefik 和 ServiceLB 要禁用（`--disable=traefik --disable=servicelb`）。
- **k3s 的数据存储用内嵌 etcd**（server 加 `--cluster-init`）。
- **CNI 由主会话按内存决定**：先用自带的 flannel，**2b 的中间件第一次部署完、量过内存之后、灌数据之前**回来改定（ADR-0026 的后果一节），够再换 Cilium（查到的最新版 v1.20.4）。换 CNI 等于重建集群。
- **换盘要同时保留 Windows 和 Ubuntu**（已按方案 A 做完，见上）。

## 上次做到哪

1c 分五步（设计文档 [design/1c-sharding.md](design/1c-sharding.md) 第 1 节）：分片、主从、CDC 索引表、跨分片查询与扩容、冷热归档。

**步骤一（分片）已完成**，阶段计划里前两个 checkbox 已勾：

- 布局（ADR-0012）：8 个 slot，`slot = user_id & 7`，`db = slot % 2`，表名带全局 slot 号（`orders_3`），每库一张 `outbox`。只拆 order。
- `pkg/sharding`（主会话写）：纯计算的路由。
- order 服务（Opus 子 Agent）：`mysqlx.MigrateTemplate` 按 slot 渲染迁移，迁移历史压缩成一个文件；store 的事务绑定 slot，跨 slot 返回 `ErrWrongShard`；每库一个中继（`order-0`、`order-1`）；sweeper 逐 slot 扫；落库消息基因不一致当毒消息。`MYSQL_DSNS` 取代 `MYSQL_DSN`（设了旧变量拒绝启动）。
- 脚本、Makefile、`test/load` 的 verify 和 wait-quiet（Sonnet 子 Agent）：建两个库，按 slot 汇总。
- 验证：`make lint test e2e` 通过，`make chaos` 两次 PASS。
- 实现中发现的问题记在设计文档 2.6（CHECK 约束名库级唯一、`payment_results` 主键只在 slot 内唯一、chaos 回收数偏高待复核）。

**Fable 评审（2026-11-13）**：1c 步骤一和 1b 遗留的背压代码没有一致性缺陷。发现并修掉了四个问题，细节在设计文档 1c 的 2.6.1 和 ADR-0011：

- 迁移没有锁（注释说有），也不可重入。`mysqlx` 用 `GET_LOCK` 实现了 goose 的会话锁，order 的建表改成 `IF NOT EXISTS`。
- order 两个连接池合计 100，加上其他服务超过 `max_connections`（151）。新增 `MYSQL_MAX_OPEN_CONNS`，每库默认 25。
- sweeper 没有按 slot 的时限，一个库挂起会拖住另一个库。新增 `SlotTimeout`，默认 10 秒。
- 背压在落库完全停住时周期性重新放行。`Claim` 记心跳，落库超过 `ADMIT_MAX_WAIT` 没有动静时只放进 `ADMIT_MAX_STALE + 1` 个探测订单。`Gate.Claim` 多了一个 `now` 参数。
- 分片布局改为只在 `pkg/sharding` 定义（`OrderSlots`、`OrderDBs`）。

前三项和布局由 Opus 子 Agent 实现，背压由 Opus 子 Agent 实现、Fable 逐行评审。合入后 `make lint test e2e` 通过，`make chaos` 两次 PASS（回收数都是 0）。

**chaos 回收数偏高与分片无关。** 旧日志显示分片前就有过 180 条（order 被杀 6 次）；回收只出现在 order 被杀之后，而且集中在其中一两次击杀上，和击杀次数的相关性弱。order 从启动到监听在分片前后都是冷启动约 1.4 秒、再启动 0.1 到 0.4 秒。推断的原因（没有直接量到）：`kill -9` 不会退出消费组，新实例要等旧成员的会话过期（10 秒）才分到分区，这段时间受理照常进行，而 chaos 把回收宽限期设成了 5 秒。chaos 的负载只有约 10 单/秒，队列到不了探测名额，所以这次的背压修复对它没有影响。chaos 里大量的 `claimed hold outlived twice the payment timeout` 是支付时限设成 5 秒造成的，对账通过。

## 下一步

**1c 步骤三（CDC 加商家索引表）已完成并推送（5 个提交，`400a97e` 到 `4b34bb7`，另有一个修 e2e 的提交）。** 设计在设计文档 1c 第 4 节，决定在 ADR-0014，实现中的发现在 4.13，结果在 4.14。`make lint test e2e` 通过，`make chaos` 两次 PASS。步骤一、二已推送，在 draft PR #6（`phase-1c`）里。

1. 下一步：
   - 步骤四已分 6 个提交推送（`baa80e3` 到 `e2a01c8`，在 draft PR #6 里），CI 的 24 项检查全部通过（2026-11-14）。
   - **CI 偶发失败，根因没有找到**：文档提交 `9a219ff` 的那次运行里，`test (./pkg)` 的 `TestMigrateConcurrentOnEmptyDB` 失败一次，8 个并发迁移中的一个报 `missing zero version migration`（goose 在版本表存在但没有 0 号记录时报这个错）。这次提交只改了文档，前一次运行（`e2a01c8`，代码相同）是通过的；本机带 `-race` 连跑 150 次没有复现。已经排除的：迁移锁的加锁、解锁和锁名（`pkg/mysqlx` 的 `getLockLocker`）读下来没有问题；goose v3.30.0 的 `up` 是先拿锁再建版本表、再读版本。一个没有证实的怀疑：MySQL 的建表会隐式提交，goose 把“建版本表”和“插入 0 号记录”放在一个事务里并带重试，如果插入那一步失败过一次，重试时看到表已存在就直接往下走，读到的是空表。再出现时先看失败的是不是第一个拿到锁的迁移。这关系到多实例同时在空库上启动，不只是测试问题。
   - 步骤四的结果在设计文档 5.11。最后一次 `make lint test e2e` 通过（2026-11-14，扩容之后）。`make chaos` 这次没有跑。
   - 商家索引这一半的造数报告已补（索引追平后量的，设计文档 5.11 的“商家索引”一段）。
   - **1c 步骤五（冷热归档）已完成并推送**（7 个提交，在 draft PR #6 里）。设计在设计文档 1c 第 6 节，决定在 ADR-0016，实现中的发现在 6.13，实测结果在 6.14。推送前 `make lint test e2e` 通过（2026-11-14），`make chaos` 没有跑。
   - 步骤五的要点：冷的定义是终态且订单号里的时间早于 90 天；读路径靠常量 `MinHotAge`（30 天）决定要不要查冷库，一律先热后冷；归档是读热、写冷、读回校验、在订单行锁里重读比对后删热；冷库是 `mysql` 实例上的 `order_archive_db`，压缩表按年分区。
   - **上线顺序是硬约束**：先给所有 order 实例配上 `ARCHIVE_DSN`，再第一次运行归档（设计文档 6.13）。`scripts/run-local.sh` 已经默认带上。
   - 步骤五留下、没有做的（设计文档 6.14 末尾）：
     - 大批量归档后冷库有碎片（多个 slot 并行、主键交错插入），这次手工 `REBUILD PARTITION` 了一遍，没有做进归档任务。
     - `GetOrder` 查冷库没有自己的时限，冷库不可用时没有降级（建议 1d）。
     - 归档任务没有进 `scripts/chaos.sh`；限速和等从库的路径没有自动化测试。
     - 用户列表落到冷库的比例没有量。
     - `load verify` 的第 4 到 15 项只读热库，长期运行的环境里会把已归档的真实订单当成不存在。
     - 面试笔记（`docs/notes/`）仍然没有写，考点已经补进阶段计划。
   - 步骤五推送后 CI 的 24 项检查全部通过（2026-11-14）。
   - 1c 的验收项都已满足。
   - **1d 步骤一（吞吐的欠账）进行中，改动都没有提交。** 设计在 [design/1d-protection.md](design/1d-protection.md) 第 2 节，决定在 ADR-0017（已采纳）。主人确认了：按步骤的顺序做、落库用分区内按 key 并发、商家索引先复用并发消费（达不到 1 万行/秒再做按批写）。
     - 主线已完成：`pkg/kafkax` 的 `Concurrency` 和 `MaxUncommitted`（`inflight.go`、`concurrent_test.go`）；order 的 `fetch` 改成先读占用再读订单行（`fetch_order_test.go`）。order 模块和 kafkax 的测试、lint 通过。
     - 三个子 Agent 的工作已验收合入：order 落库消费者开并发（`SUBMIT_CONCURRENCY`，默认 8）和并发落库测试，产品代码没有发现并发缺陷；orderindex 开并发（`INDEX_CONCURRENCY`，默认 16），测试库上约 7600 到 9000 条/秒见顶，瓶颈是每次提交的刷盘；压测工具的开环模式（`load run -mode open`）、`load poll` 子命令和 `scripts/bench.sh` 的 `BENCH_OPEN`、`BENCH_POLL`。
     - 共享的 `mysql` 实例的 `max_connections` 在 compose 里提到了 400（各服务的池加起来 164，超过默认的 151，服务开着时跑测试会报 1040）。
     - `make lint test e2e` 通过（2026-11-14）。以上改动已提交到本地的 `phase-1d`，**没有推送**。
     - 实测已做完，结果在设计文档 1d 的 2.10。`make chaos` 两次 PASS。要点：落库吞吐随并发度从 316 升到 686 单/秒就平了，`performance_schema` 显示瓶颈是 inventory 预占时 `sku_stock` 热门行的行锁（平均等 48 毫秒）加上每次提交约 7 毫秒；有查询压力时落库是没有时的 75%；商家索引在测试库上 7600 到 9000 行/秒见顶，瓶颈是提交刷盘。
     - 步骤一的前半已推送：draft PR #7（`phase-1d`），CI 的 29 项检查全部通过（2026-11-14，到提交 `9697914`）。
     - **步骤一的延续，进行中，都没有提交**（主人 2026-11-14 的决定）：
       - 商家索引按批写：设计在 1d 设计文档 2.11，ADR-0017 末尾有补充。要做 `kafkax.NewBatchConsumer`（Fable）和 orderindex 的改造（Opus）。
       - MySQL 侧库存扣减改成异步（方案 A：`stock_pending` 增量表加单个应用者合并更新）：设计在 1d 设计文档第 3 节，ADR-0018（已采纳）。主人接受了“MySQL 从阻止超卖变成发现超卖”的代价；方案 B（经 Kafka、order 不再调 Reserve）等 A 做完量过再定。产品代码由 Fable 写，测试铺开派 Opus，脚本和 verify 派 Sonnet（设计文档 3.6）。
       - 现状（2026-11-14）：两项都已实现、验收并实测，已提交并推送到 PR #7。`make lint test e2e` 通过，`make chaos` 两次 PASS（22 项）。
         - 商家索引按批写：测试库上批大小 500 时 4.8 万行/秒（带 `-race`），结果在设计文档 2.11。`INDEX_CONCURRENCY` 换成了 `INDEX_MAX_BATCH`。kafkax 的按批消费在暂时性错误时先整批重试 3 次再退回逐条。
         - 异步扣减：热点行的锁消失，落库吞吐在并发度 8 时从 642 到 789、16 时从 686 到 870；新的瓶颈是提交（每个订单三次，每次约 9 毫秒）。放宽刷盘后停在约 1050 单/秒，剩下的时间不在 MySQL 里，原因没有找到。结果在设计文档 3.9，测试铺开时发现并修掉的两个缺陷（超卖时预热失败、对账失效）在 3.8。
         - `load verify` 现在有 22 项，第 22 项是“库存增量都已应用”；wait-quiet 把 `stock_pending` 为空加进了静止条件。
         - 主人的决定（2026-11-14）：这一批提交并推送到 PR #7；**方案 B 先不做**，等有了埋点（步骤六）或上了集群再判断。步骤一的延续已分 6 个提交推送（`84b58d5` 到 `7b37be2`），PR #7 的 CI 30 项全部通过。
         - **1d 步骤二（网关限流）已实现、验收并实测，已提交并推送到 PR #7。** 设计在设计文档 1d 第 4 节，ADR-0019（已采纳），发现在 4.15、4.16，结果在 4.17。`make lint test e2e` 通过（e2e 18 个），`make chaos` 一次 PASS。
           - `pkg/ratelimit`（Fable）：进程内、Redis（Lua）、`Fallback`。网关的中间件、配置、客户端地址、规范（Opus，Fable 评审）。脚本、`bench.sh` 的 `BENCH_RATELIMIT`、e2e 用例（Sonnet）。
           - 各脚本的限流配置：`make run` 用默认值；e2e 关掉按 IP 的；chaos 和 bench 整体关闭。**从本机压网关而忘了关限流，现象是吞吐上不去或随机 429。**
           - 做限流时发现并修掉两个旧问题（设计文档 4.16）：受理的 `Hold` 被 Redis 客户端重试后不发落库消息、订单凭空消失（1b 起就有，`hold_retry_test.go` 钉住）；`redisx` 的客户端不遵守 ctx 的截止时间（已开 `ContextTimeoutEnabled`）。没改的：分布式锁的加锁脚本被重试时调用方以为没拿到锁。
           - 推送后 CI 失败两项，都已做根因分析并修掉（设计文档 3.10、4.16）：inventory 的应用者把“取到的增量先后不全”误报成超卖（设计漏洞：按 id 加锁扫描不是快照；改成被拒绝时用该 SKU 的全部增量重算，再用快照判断）；`pkg/lock` 的测试租期太短（`redisx` 的超时生效之后续期只有 100 毫秒）。应用者那个机制是推断的，本机没有复现。修复后的一轮 CI 里这两处都通过了，另有一个 kafkax 测试因为辅助函数查位点时遇到 `COORDINATOR_NOT_AVAILABLE` 而失败（测试辅助函数没有重试，不是被测逻辑），已改成重试到时限。
           - 步骤二最终的 CI：提交 `4db959f`，24 项检查全部通过。
         - **1d 步骤三（服务侧保护、超时与重试规范）已实现、验收并实测，改动没有提交。** 设计文档 1d 第 5 节，ADR-0020（已采纳），实测的 Envoy 行为和三个坑在 5.3，发现在 5.13，结果在 5.14。`make lint test e2e` 通过（e2e 18 个，经边车），`make fault` 五个场景全过，`make chaos` 一次 PASS。
           - 主人定的：不用 sentinel-go（停更五年）；服务之间的熔断和并发隔离用 **Envoy v1.40.2 边车，每个服务一个**；保护范围是全部下游调用；客户端断开时 gateway 返回 499。
           - 做了什么：五个边车的配置（`deploy/envoy/`）和 compose、`make envoy-validate`（进了 CI）；`grpcx.WithHopTimeout` 和 `grpcx.SidecarMargin`（进程里的时限 = 边车的路由超时 + 500 毫秒，三个服务各有测试核对）；`pkg/bulkhead`；order 的展示读并发上限（`READ_MAX_CONCURRENCY`，默认每库 16）、冷库上限和 1 秒时限、闸门调用 300 毫秒、从库读 500 毫秒；order 和 payment 的取消与超时映射；gateway 的热点参数限流（`RATE_LIMIT_HOT`、`RATE_LIMIT_HOT_ITEMS`）、499、按跳时限；`kafkax` 退避加抖动；`worker` 的 Guard 时限；`scripts/lib/sidecar.sh`（run-local、chaos、bench、fault 共用，`SIDECAR=off` 直连）；`scripts/fault.sh`（`make fault`）。
           - 实测：inventory 挂住时查库存前 5 次各约 3 秒、之后毫秒级 503，查订单和下单受理不受影响，解除后 5.5 秒恢复；同时 300 个请求时恰好 172 个被并发上限立刻拒绝；边车让受理延迟的 p50 多 0.7 毫秒、p99 多约 15 毫秒，落库吞吐不变。
           - 没做的（设计文档 5.13、5.14）：受理超时后清理和受理乱序留下的占用（要改闸门脚本）；`pkg/grpcx` 的日志拦截器对超时仍记 ERROR；预热被 1 秒封顶是否够用；按错误比例和慢调用比例的熔断（Envoy 单实例下没有）；`make fault` 没进 CI。
           - **本机现在多了五个 Envoy 容器**（`make up` 启动）。`make run`、e2e、chaos、bench 默认经过它们，它们没起来时脚本会提示先 `make up`。管理端口 19901 到 19905。
           - 已提交并推送（PR #7，HEAD `17cd38b`，五个提交），CI 25 项全部通过（新的 `envoy` job 17 秒；e2e 带五个边车 3 分 47 秒，是最长的 job）。第一次运行因私有仓库的 Actions 额度用完没有启动，主人把仓库改为公开后重跑通过。
         - **1d 步骤四（降级预案和优雅启停）已实现、验收并实测，已分 6 个提交推送到 PR #7（`5d30230` 到 `98ca954`），CI 25 项全部通过（2026-11-14；最长的是 e2e 3 分 42 秒，没有明显变慢的 job）。** 设计文档 1d 第 6 节，ADR-0021（已采纳），发现在 6.16，没做的在 6.17，结果在 6.18。`make lint test e2e` 通过（e2e 18 个），`make fault` 14 个场景全过，`make chaos` 一次 PASS（都是 2026-11-14）。
           - 主人定的：闸门的 Redis 不可用时**拒绝下单，不退回数据库扣减**（改了原计划）；开关在进程内、运行中可改；熔断器自己写；落库健康和列表的部分结果都做。
           - 做了什么：`pkg/breaker`、`pkg/degrade`、`pkg/app` 的 `/readyz`、`DRAIN_DELAY` 和第二次信号退出、`redisx.OpenLazy`、`mysqlx.OpenLazy`、`pkg/cache` 的熔断器和读时限、`lock.FailOpen` 的冷却、`errx.Degraded`；order 的闸门熔断器、落库健康（按订单库，受理时拒绝落不了库的订单）、回收任务在闸门恢复后等半个宽限期、从库健康探测、冷库的后台迁移和熔断、用户列表的部分结果（`archive_unavailable`）；order、inventory、payment 消费者对普通错误不设重试上限；inventory、payment 的启动和错误码；gateway 的降级错误映射（503 加 `Retry-After`）、三个开关和管理端口；`scripts/fault.sh` 的场景 F 到 N。
           - **新的约定（已写进 AGENTS.md）**：依赖坏了时返回 `errx.Degraded`（`FAILED_PRECONDITION` 加原因码），不返回 `UNAVAILABLE`，否则调用方的边车会把整个服务摘掉；消费者不设重试上限，handler 里重试无用的错误必须标 `Permanent`。
           - **本机的变化**：gateway 多了管理端口 `127.0.0.1:8089`（`ADMIN_ADDR`），降级开关在上面；其他服务的开关在各自的内部端口（8081 到 8084）的 `/degrade`。新的环境变量：`DEGRADE`（开关初始值，名字写错拒绝启动）、`DRAIN_DELAY`、`ORDER_READ_MAX_LAG`。`make fault` 会暂停本机共享的 Redis 和订单库的从库（结束时恢复，中途失败也会兜底恢复），跑它之前不要有别的东西在用。
           - 没做的见设计文档 6.17、6.18 末尾。要点：闸门脚本出错和 Redis 连不上分不开、payment 的扫描错误，这两种在数据损坏时会永久挡住分区；inventory 进程里的闸门没有熔断器；`Retry-After` 固定 5 秒；“Redis 暂停时还没被认领的订单恢复后落库”和回收任务的等待只有按模块的测试；`make fault` 没进 CI。
           - 一次没有解释的偶发失败：`pkg/cache` 的 `TestConcurrentColdKeyLoadsOnceInProcess` 在整包运行时失败过一次（回源 2 次，耗时 1.15 秒），之后重跑十几轮没有复现；耗时特征像本机容器网络的 1 秒停顿让一次读超时。order 的 `TestSubmitConcurrentWarmUpIsCoalesced` 也失败过一次，当时的输出没有留下，之后没有复现。
           - **1d 步骤五（对账）已实现、验收并实测，已分 4 个提交推送到 PR #7（`c8e8c9c` 到 `b09a74e`）**（2026-11-14）。设计文档 1d 第 7 节，ADR-0022（已采纳），实测结果在 7.12，发现在 7.15，没做的在 7.16。`make lint test e2e` 通过（e2e 18 个），`make chaos` 一次 PASS（`load verify` 和 `recon` 都通过）。
             - 主人定的（7.14）：摘要在数据库里算；节点摘要用行哈希的和，不是教科书式的 H(子节点)；闸门和 MySQL 的库存不纳入，维持 inventory 的 `reconcile`；只有商家索引可以自动修复。
             - 做了什么：`pkg/merkle`（`Digest`、规范编码和对应的 SQL 片段、订单号上的基数树、自上而下的 `Diff`）；新模块 `services/recon`（已进 `go.work`）：五对的投影（`replica`、`index`、`payment`、`stock`、`archive`，在 `internal/recon/pairs.go`）、数据源、比对、复查、报告、索引修复、命令行；`scripts/recon.sh`、`make recon`；`scripts/chaos.sh` 在 `load verify` 之后加跑 `recon -pairs payment,stock`，有差异时判失败。`pkg/merkle`、投影由 Fable 写，引擎由 Opus 写、Fable 评审后改了三处（修复改成按主键删、全量模式的分段、`archive` 改读主库）。
             - 用法：`ORDER_DBS=4 make recon`（默认比 `replica`、`index`、`payment`、`stock`，只读）；`RECON_ARGS="-pairs index -repair"` 修商家索引；归档的前后对比是三步：`RECON_ARGS="-pairs archive -snapshot-out F"`、归档、`RECON_ARGS="-pairs archive -against F"`，都要在归档任务不在跑的时候做。退出码 0 无差异、1 有差异、2 出错。
             - 实测要点（490 万行的 `index`）：无差异时树 4.8 秒、传约 1.7 万个摘要，全量归并 7.8 秒、545 MB；1 行差异多 0.3 秒；9612 行均匀散布的差异打中 95% 的节点，树比归并慢。交叉点取决于差异落在多少个节点里。
             - 推送后的 CI（`b09a74e`）：27 项里 26 项通过，新模块的 `lint (./services/recon)` 30 秒、`test (./services/recon)` 1 分 39 秒。失败的一项是 `envoy`（`make envoy-validate`）：前三个配置 OK，`deploy/envoy/orderindex.yaml` 那一次什么也没输出就失败了。这次推送没有改 `deploy/envoy/`，本机五个配置都通过。**根因没有找到**：原来的写法是 `docker run ... | tail -1 | grep OK`，失败时把校验器的输出丢掉了，日志里没有可看的东西。已把 `envoy-validate` 改成失败时打印 docker 的退出码和完整输出并重新推送；再出现时看那段输出（怀疑是 `docker run` 本身偶发失败，没有证据）。
             - 重新推送后（`1ac7c1c`）CI 27 项全部通过，`envoy` 这次也过了，所以那次失败的原因仍然不知道（最长的是 e2e 3 分 52 秒，其次 `test (./services/order)` 3 分 44 秒）。
             - **增量对账（窗口的下界）已加，已分 2 个提交推送到 PR #7**（2026-11-14，主人提出）：`recon -since 26h` 只比最近 26 小时的订单，和 `-settle` 合成一个窗口；`archive` 不支持。设计文档 7.17，ADR-0022 有补充。`make lint` 和 recon 模块的测试通过；本机 `index` 全量 4.7 秒、`-since 26h` 0.27 秒。改的文件：`services/recon/` 下的 `sql.go`、`source.go`、`rows.go`、`compare.go`、`report.go`、`cmd/recon/main.go` 和测试，`Makefile` 的帮助，三份文档。
             - 这次推送（`4e0f4ca`）的 CI 27 项全部通过（最长的是 `test (./services/order)` 4 分 1 秒和 e2e 4 分；`envoy` 没有再失败）。
             - **1d 步骤六（埋点）已实现、验收并实测，已分 6 个提交推送到 PR #7（`7826840` 到 `493e983`），CI 27 项全部通过**（2026-11-14；最长的是 `test (./services/order)` 4 分 27 秒、e2e 3 分 21 秒、`test (./services/gateway)` 3 分 13 秒，没有明显变慢的 job）。设计文档 1d 第 8 节，ADR-0023（已采纳），发现在 8.15，结果在 8.16，没做的在 8.17。`make lint`、`make test`（55 个包）、`make e2e` 通过，`make chaos` 一次 PASS（杀进程 18 次，`load verify` 和 `recon` 都通过）。
               - 主人定的（8.14，四项都按建议）：本机后端用 `grafana/otel-lgtm`（compose 的 `obs` profile）；`outbox` 加一列 `trace_context` 传递链路；异步一跳以父子为主，延迟关单和按批消费用 link；指标经 OTLP 推。
               - 做了什么：`pkg/otelx`（`Setup`、采样器、建指标和给 span 加属性的小工具）；`logx` 的日志带 `trace_id`、`span_id`；`grpcx`（`otelgrpc`）、`mysqlx`（`otelsql`）、`redisx`（`redisotel`）；`kafkax/telemetry.go`（生产和消费的 span、排队时长、重试和死信的计数）和分到分区的日志；`outbox` 的列加 order 的 `00004`、payment 的 `00003` 迁移（可重跑）；`breaker`、`degrade`、`cache` 的指标；gateway 入口的中间件（没有用 `otelgin`，原因在 8.4）、`X-Trace-Id`、限流和降级的指标；order、inventory、payment、orderindex、cdc 的业务指标和 span 属性；payment 的处理器在出结果处开一条链路（`payment.complete`）；e2e 的 `TestTraceSpansAllServices`（测试进程里起 OTLP 接收端）；`scripts/lib/obs.sh`、`make obs-up`、`make obs-down`。公共库和 payment 的那一处由 Fable 写；gateway、order、inventory 等四个服务派 Opus，order、inventory、payment 的 diff 由 Fable 逐行评审（只加观测，没有改行为）；公共库的测试、本机后端和脚本、e2e 用例派 Sonnet。
               - 实测要点：1% 采样的开销量不出来，全量采样落库吞吐低约 7%；一个订单是三条链路（下单到落库约 70 个 span、发起支付、支付结果），靠 span 上的订单号关联。**3.9 的悬案有了答案**：那 54 毫秒不在 order 进程里（子 span 没覆盖的时间中位数 0.15 毫秒），是“一次落库约 30 次往返乘以本机每次往返的代价”加上偶发的 200 毫秒 TCP 重传造成的长尾（中位数 27 毫秒、平均 55 毫秒）。
               - **新发现、没有查原因**：过载（开环每秒 2000 个请求）时，12 次运行里 5 次出现闸门的 Redis 调用成批超过 300 毫秒、熔断器打开、几百条消息重试，那一轮落库吞吐低 15% 到 25%，订单没有丢。和埋点无关（`always_off` 时也出现）。
               - **本机的变化**：`make obs-up` 启动 `otel-lgtm`（Grafana 在 http://127.0.0.1:3000 ，admin/admin；OTLP 4317/4318；内存上限 1.5 GiB，全量采样压测后到过 1.33 GiB）。它在运行时 `make run`、`bench.sh` 自动导出（`OBS=off` 关闭；`bench.sh` 默认 1% 采样）；e2e 把链路发给测试进程自己的接收端（14317），chaos、fault 不导出。**当前 `otel-lgtm` 容器在运行**，不用时 `make obs-down`。查链路和指标的命令在 8.10 之外另记在这里：Tempo 用 `curl -s -u admin:admin -G localhost:3000/api/datasources/proxy/uid/tempo/api/search --data-urlencode 'q={ resource.service.name = "order" }'`，Prometheus 用同样的前缀换成 `uid/prometheus/api/v1/query`。拆落库时间的脚本没有入库（在会话的临时目录里），压测的原始输出在 `tmp/bench/otel-overhead/`。
               - 新的约定（已写进 AGENTS.md 的“可观测”）：带 ctx 的日志；业务代码不手工建 span；指标的标签只能是有限的枚举；只有服务端故障标 Error；加指标不改行为、不新增查询。
               - 没做的见设计文档 8.17。要点：`sloglint` 没有打开；`order.gate.unclaimed_holds` 和 `inventory.stock_pending.backlog` 两个指标没有做；`make fault` 里指标对不对得上没有抽查；后端长时间不在时的表现没有跑过。
             - **阶段 1 收尾（2026-11-14，分支 `phase-1-wrapup`，从合并后的 `main` 开出）。已分 4 个提交推送（`a829857` 到 `7686015`），draft PR #8，CI 27 项全部通过（最长的是 `test (./services/order)` 4 分 20 秒、e2e 4 分 18 秒、`test (./pkg)` 3 分 14 秒；e2e 比 PR #7 的 3 分 21 秒慢了约 1 分钟，只看了这一次，没有查是波动还是变慢）。** PR #7 已合并进 `main`（合并提交 `d04e118`）。主人要求的四件事：
               - 合并 PR #7：已做。
               - 阶段计划里 1d 的四项旧条目（orderindex 按批写、落库性能、开环模式、1b 遗留的小问题）逐项在代码里核对过，已勾。1c 里有两项没勾的是有意留着的（读模式的定量对比等阶段 3；归档后自动重建分区没做）。
               - 面试笔记：`docs/notes/1c-sharding.md`（824 行）和 `docs/notes/1d-protection.md`（约 800 行，覆盖 104 条考点）由 Sonnet 写、主会话抽查。两份里按通用知识补充的段落都标了“通用知识”，设计文档里没有出处，面试前要对照官方文档核对。写笔记时查出的几处文档问题已改：**AGENTS.md 里边车时限的规则原来写反了**（“两处要一致”，实际是进程里的时限等于边车的数加 500 毫秒）；设计 1c 的 5.7、设计 1d 的 4.6 和 5.7、ADR-0014 里过时的内容加了注。
               - 8.17 里没做的，按收益做了三项：
                 - **过载时闸门调用成批超时，查到原因并修了**（设计文档 1d 的 8.18，ADR-0021 末尾的补充）。两个原因：查询把闸门的 Redis 连接池（原来 32）占满，排队的超时被算成闸门失败；一次约 300 毫秒的停顿让十几个在途调用同时失败，“连续 5 次”一下满足，熔断器打开 5 秒。修复：展示读读闸门占用要先占名额（`READ_GATE_MAX_CONCURRENCY`，默认 16），连接池可配置（`REDIS_POOL_SIZE`，默认 128）；`pkg/breaker` 加可选的 `MinFailureSpan`，闸门的熔断器设 1 秒。修后 6 轮压测熔断器没有再打开，重试从每轮 380 到 750 次降到 0 到 5 次；`make fault` 14 个场景全过。**那个约 300 毫秒的停顿本身来自容器网络还是 order 进程没有查到。**
                 - 两个积压量的指标（`order.gate.unclaimed_holds`、`inventory.stock_pending.backlog`），Opus 写、Fable 逐行评审。后者是全表扫描，见设计 8.17。
                 - `sloglint` 的 `context: scope` 已打开，作用域里有 ctx 的日志调用（约 66 处，九个模块）由 Sonnet 换成了带 ctx 的方法，主会话核对过 diff 里只有方法名和第一个参数的变化，没有加 `nolint`。
               - 一次没有解释的失败：`make e2e` 紧跟在 `make fault` 之后跑失败过一次，之后连跑 5 次通过。失败的输出被过滤掉了，不知道是哪个用例。再出现时把完整输出留下来。
               - 新的环境变量（order）：`REDIS_POOL_SIZE`、`READ_GATE_MAX_CONCURRENCY`。
               - 8.17 里仍然没做的：`make fault` 里抽查指标、后端长时间不在时的表现、压测工具用 `X-Trace-Id`、cdc 发 Kafka 没有 span、orderindex 的 `ListMerchantOrders` 错误映射。
               - 收尾的全部改动之后：`make lint` 各模块 0 issues，`make test` 全部通过，`make e2e` 通过，`make fault` 14 个场景全过（`make fault` 是在加指标和改日志之前跑的）。`make chaos` 这一轮没有跑。`pkg/lock` 的 `TestExperimentNaiveDelDeletesOthersLock` 在整仓库的 `make test` 里失败过一次，单独重跑通过，原因没有看。
               - 2026-11-15 进阶段 2 之前主人定的三件事（都只改了文档，见“计划变更记录”）：链路后端从 SkyWalking 换成 Tempo（ADR-0024）；Agent 的 Kubernetes 只读工具提前到 3d、pprof 工具提前到阶段 4；阶段 5 加 Gemini Decision 的对比实验。
               - 下一步：PR #8 已合并进 `main`（见“当前状态”）。阶段 1 的完成标准都满足了，可以进阶段 2（要先请主人开 Ubuntu 3900X 和 DS218+，装 helm）。
             - 动工前量过的数（本机冷库 482 万行）：行读到客户端 276 MB、7.3 秒；库里按 18.6 小时一个桶聚合 SHA-256 是 3115 个桶、8.5 秒；`CRC32` 5.0 秒；只 `COUNT(*)` 3.1 秒。MySQL 9.7 没有 `MD5()` 函数。
   - 步骤四留下、没有做的（都记在设计文档 5.11 末尾）：
     - 按用户的列表固定读主库，没有分流到从库（要在写路径上按用户再打一份读己之写标记）。
     - 造数的进度表不跟着 slot 走，扩容后不能用原参数续灌。
     - 多个 order 实例滚动切换没有验证（阶段 2）；CI 和 e2e 仍只跑 2 个库。
     - orderindex 消费者改成按批写，挂到了 1d。
     - 支付结果改成无限重试只有 `fence_test.go` 在 handler 层守着（返回可重试的错误），消费者配置本身（`MaxAttempts`）没有测试。
     - 面试笔记仍然没有写，考点已经补进阶段计划。
   - 步骤三留下、没有做的：
     - 把 CDC 和 orderindex 加进 `scripts/chaos.sh` 的击杀名单，并在对账前等索引追上（verify 的第 20 项现在在 chaos 里是跳过的）。“杀掉 CDC 再启动”目前只有 cdc 模块自己的集成测试覆盖。
     - 延迟的指标、告警和源库心跳，挪到阶段 3（见计划变更）。
     - `GET /orders/{id}` 不返回失败原因，多商家订单的 `MIXED_MERCHANTS` 只能从库里看到。
     - 多商家的订单在受理时不拒绝，落库预占时才失败（闸门里没有商家信息）。
     - 1c 的面试笔记（`docs/notes/`）还没有写，考点已经记在阶段计划里。
   - 步骤五（归档）动工前必须先定的两件事，记在设计文档 4.13：DELETE 在压缩的 topic 里怎么表示；行删除后同主键重插，版本从 0 重新开始。
   - 阶段 4 做故障切换时要验证：行版本号的正确性依赖 CDC 的源就是被提升的那个从库（ADR-0014 后果一节）。
2. 步骤一评审的遗留，没有做：
   - inventory 的 `00001_init.sql`（3 条建表）和 payment 的 `00002_outbox.sql`（建表后跟 `ALTER`）中途被杀后不可重入。并发启动已经由迁移锁解决。
   - 背压的第三项检查只管落库完全停住。消费者活着但落不了库（部分分区停住、极慢、认领后写库失败）时，原来的“触发、回收、重新放行”仍会出现，见 ADR-0011 的后果一节，建议 1d 处理。
   - `ListStaleCreating` 没有 `(status, updated_at)` 索引，决定不加：MySQL 没有 `ADD INDEX IF NOT EXISTS`，而 CREATING 的行很少。
   - 消费者没有“加入消费组、分配到分区”的日志，order 启动时也没有迁移耗时的日志，排查恢复耗时只能靠推断。
3. 步骤二（主从与读写分离）的结果见设计文档 3.10，决定见 ADR-0013。遗留：
   - CI 的按模块测试 job 改用 `make up-core`（只起 `mysql`、Redis、Kafka），只有 e2e job 用 `make up` 起主从。加主从后这些 job 各慢了 1 到 3 分钟，这是为了把时间降回去；本机 `make test` 同样只需要 `make up-core`。
   - 已知局限，建议 1d 和熔断降级一起做：从库整体落后时 `gtid` 模式的每次展示读先等满 100 毫秒再读主库；从库一直挂起时 sweeper 每轮约 1 分钟。
   - 定量的读模式对比挪到阶段 3（见计划变更）。

1b 步骤三遗留、仍未处理的两点（记在这里不丢）：

- `closed` 结局在正常时限下几乎不出现，CI 覆盖不到“支付对关单”的竞争，要稳定覆盖需要单独写一个用例。
- 支付时限从受理时刻起算，落库排队会挤占支付窗口。背压把排队限制在约 25 秒以内，影响变小了，但没有消除。

挂到后续阶段的（已写进对应计划）：落库串行与轮询拖垮落库、压测工具开环模式、几个小问题（1d）；集群上重测吞吐、Kafka 静态成员（阶段 2）。

## 本机开发环境

- `make tools` 安装 buf、proto 插件和 `oapi-codegen` 到 `./bin`（已装）。
- `make up-core` 只启动现有的 `mysql`、Redis、Kafka，够跑 `make test`。
- `make up` 启动依赖：现有的 `mysql`（inventory_db、payment_db、商家索引库和测试用的临时库）、order 的两主两从（端口见 `docs/infra/versions.md`）、Redis、Kafka，并建立复制。`make down` 停止并保留数据。
- **本机的订单库已经扩成 4 个**（2026-11-14 的演练）：`order_db_0` 到 `order_db_3`，各在自己的实例上，各带一个从库。500 万条造出来的历史订单（用户号从 2^40 起）**已经归档**：热库只剩约 16.7 万单（近 80 天的；1d 步骤五的演练又归档了 20717 单），其余 484 万单在 `mysql` 实例的 `order_archive_db` 里；热表已重建回收过空间。要重新得到“归档前”的状态只能清库重灌。多出来的 4 个实例属于 compose 的 `expand` profile，`make up` 不会启动它们：
  - 启动依赖要 `make up && make expand-up`。
  - 启动服务要 `ORDER_DBS=4 make run`。直接 `make run` 会因为 `shard_meta` 不符而拒绝启动，这是有意的。
  - 想回到干净的 2 库环境：`make down-volumes && make up`（会清掉全部数据，包括造的数）。
  - `make e2e`、`make test` 不受影响，它们用旧的两个实例上独立的库。
- 依赖在运行，六个服务**已停止**（2026-11-14 跑 1d 的压测前停的）。要起来用 `ORDER_DBS=4 make run`。停止时按进程号杀 `scripts/run-local.sh`，不要用 `pkill -f`（见会话日志的教训）。
- 共享的 `mysql` 实例的 `max_connections` 是 400（compose 里设的）。
- 对账：`ORDER_DBS=4 make recon`（只读，约 6 秒；增量用 `RECON_ARGS="-since 26h"`；不带 `ORDER_DBS=4` 会报 `orders_2` 不存在）。实测的原始输出和归档前的快照在 `tmp/recon/`。2026-11-14 最后一次运行四对加 `archive` 都没有差异。
- 归档：`ORDER_DBS=4 make archive`（本机是 4 个库，不带 `ORDER_DBS=4` 会被 `shard_meta` 校验拒绝），`make archive-dry-run` 只数不搬，`make archive-reclaim` 重建热表回收空间，参数用 `ARCHIVE_ARGS` 传。这次实跑的原始输出在 `tmp/archive/`。
- Kafka 的 Web 界面：`make kafka-ui` 启动 kafbat-ui（compose 的 `ui` profile，只读，http://127.0.0.1:8090），`make kafka-ui-down` 停止。看消费组的 lag 要点进具体的消费组。消息体是 protobuf，没有配置解码，看到的是原始字节。当前处于运行状态。
- 造数：`make seed-orders`（可中断续跑）、`make seed-report`。扩容：`scripts/expand.sh`（不带参数看用法），演练的状态文件在 `tmp/expand/`。
- order 的读模式由 `ORDER_READ_MODE` 控制（`primary`、`window`、`gtid`），`make e2e` 默认 `gtid`，`make run` 会连上从库但默认是 `primary`，要读从库时自己设这个变量。
- `make test` 的集成测试连接 `make up` 启动的 MySQL，每个测试自建临时库。
- `make run` 启动六个服务（订单库是 4 个时加 `ORDER_DBS=4`；gateway 8080、order 8081、inventory 8082、payment 8083、orderindex 8084、cdc 8085；gRPC 端口是 HTTP 端口加 1000），`make seed` 写入示例 SKU（1001 到 1003 属于商家 1，2001 属于商家 2），`make e2e` 在独立的 `e2e_*` 库上跑端到端测试。日志在 `./tmp/`。

## 阻塞项与待确认

- 见“当前状态”里的“停在哪、在等什么”：千兆网卡。

## 设备状态（上次已知）

| 设备 | 状态 |
|---|---|
| MacBook Pro | 在用 |
| Ubuntu 3900X | 开机（2026-11-15），k3s 集群在运行，Ubuntu 26.04.1，有线接在网桥 `br0` 上（百兆），Wi-Fi 是备用的管理通道；三台虚拟机在运行并设了自启 |
| Mac mini | 关机，阶段 3 开始需要 |
| DS218+ | 开机（2026-11-15） |

## 计划变更记录

| 日期 | 变更 | 原因 |
|---|---|---|
| 2026-11-11 | 监控与压测提前到故障注入之前 | 对应的 ADR 已由主人删除 |
| 2026-11-11 | 只保留 `AGENTS.md`，不建 `CLAUDE.md` | 主人决定 |
| 2026-11-11 | 最小诊断 Agent 提前到阶段 3d，阶段 4 同步迭代，阶段 5 改为评测与完善 | 让 Agent 尽早积累真实故障下的运行记录 |
| 2026-11-11 | 埋点改用 OpenTelemetry，SkyWalking 作为链路后端 | 见 ADR-0004 |
| 2026-11-11 | 1b 增加分布式锁；1c 增加 binlog CDC 和冷热分离归档；1d 对账改用 Merkle Tree | 主人要求 |
| 2026-11-11 | 阶段 4 增加 Go 代码缺陷类故障，细化 MySQL 和 Redis 故障 | 覆盖常见生产故障 |
| 2026-11-11 | 版本选择规则：LTS 优先，其次最新稳定版 | 见 `docs/infra/versions.md` |
| 2026-11-11 | 1a 就实现了支付与关单的并发处理（条件更新加退款答复）和 Release 的空回滚 | 这两点是主流程正确性的一部分，无法推迟到 1b；1b 的对应任务改为在引入 Kafka 后复核 |
| 2026-11-11 | 设计修正：Release 改用 `ON DUPLICATE KEY UPDATE` 加锁并增加死锁重试；Reserve 被业务拒绝后一律 Release | 原设计在并发下死锁、泄漏库存，由测试和子 Agent 的评审发现 |
| 2026-11-11 | `app.Run` 传给组件的 context 不再随退出信号取消 | 原行为与“优雅停止时做完在途工作”矛盾 |
| 2026-11-11 | 1a 与 1b 之间增加「1a 补充：对外接口契约（OpenAPI）」，gateway 改为规范先行 | 见 ADR-0005 |
| 2026-11-11 | ADR 重新编号：原 0005（OpenTelemetry）改为 0004，新增的 OpenAPI 为 0005、金额表示为 0006 | 主人删除了原 ADR-0004（可观测先于故障注入），序号补齐 |
| 2026-11-11 | 「1a 补充」的 gateway 重构由主会话完成，没有派给子 Agent | 验证 ADR-0005 的待验证细节需要先把重构做出来，改动很小 |
| 2026-11-11 | 「从 OpenAPI 规范生成客户端」从「1a 补充」移到阶段 3b | 和压测脚本一起评估，避免做两次 |
| 2026-11-12 | 步骤三设计修正：受理失败用 `ReturnIfUnclaimed`；认领的占用单独成列表；落库时已过期的订单直接失败；创建超时从落库时刻算起 | 都是实现时由子 Agent 的评审和测试发现的，见设计文档 6.3、6.4 |
| 2026-11-12 | 1b 验收项“吞吐相对 1a 有数量级提升”移到阶段 2，在集群上重测；落库性能问题和压测工具开环模式移到 1d | 本机实测只有 1.7 到 3.7 倍，测量疑似受 OrbStack 端口转发干扰；主人决定 1b 只修积压与回收的冲突 |
| 2026-11-12 | 1b 增加受理端背压（ADR-0011） | 压测发现积压超过回收宽限期会丢掉已受理的订单 |
| 2026-11-12 | 背压修复由 Opus 编写和自评审，没有经过 Fable | Fable 没有额度，主人决定先由 Opus 完成；Fable 可用时补一次评审 |
| 2026-11-13 | 1c 拆成五步，步骤一只拆 order；payment、inventory 不分库 | 见设计文档 1c 第 1 节、ADR-0012 |
| 2026-11-13 | 1c 步骤一由 Opus 主持并派子 Agent 实现，订单状态相关代码待 Fable 评审 | Fable 不可用，主人决定可用后一并评审 |
| 2026-11-13 | 背压增加第三项检查（落库心跳加探测名额），ADR-0011 补充 | 评审发现原来的第二项检查会被回收任务抹掉信号 |
| 2026-11-13 | 步骤二的读模式定量对比（读主库比例、落库吞吐）挪到阶段 3 | 现在没有指标能统计读走了哪里，本机吞吐测量不可靠；端到端用例已量到两种模式的行为差异 |
| 2026-11-13 | 步骤三：CDC 同步延迟的指标、告警和源库心跳挪到阶段 3；两个可选项（CDC 替换 outbox 中继、ES）不做 | 现在没有指标体系；主人确认 |
| 2026-11-13 | 步骤三的并行任务没有用 worktree，三个子 Agent 在主工作目录里各改一个目录 | 主线的共享改动没有提交，worktree 里看不到 |
| 2026-11-13 | `super_read_only` 改由复制脚本在复制建立后设置，不写在 compose 启动参数里 | 官方镜像首次初始化会失败 |
| 2026-11-14 | 步骤四新增“按用户列出订单”接口；订单号 Epoch 提前到 2020-01-01 并清空本机开发库；订单库数由 `MYSQL_DSNS` 个数决定（修改 ADR-0012 第 4 条） | 见设计文档 1c 第 5 节、ADR-0015；主人确认 |
| 2026-11-14 | order 消费支付结果改为无限重试 | 读代码发现默认 5 次重试（约 1.5 秒）后支付结果进死信，订单库短暂不可用就会丢；扩容的围栏窗口必然触发 |
| 2026-11-14 | 路线图增加阶段 9：eBPF（可选），计划文件只到里程碑粒度 | 主人要求 |
| 2026-11-14 | 步骤四的三个并行任务仍在主工作目录里各改各的目录，没有用 worktree | 主线的共享改动没有提交 |
| 2026-11-14 | 步骤五：冷热按订单号里的受理时间划分，不按完结时间；用 Merkle 工具校验归档挪到 1d | 订单号决定冷热时读路径是纯计算，本系统受理到终态只隔一个支付时限；Merkle 工具 1d 才有。见 ADR-0016，主人确认 |
| 2026-11-14 | order 落库拒绝订单号早于 30 天的受理消息；针对已归档订单的支付结果从冷库推出答复 | 归档引入的两个一致性问题：旧订单号重新插入热库会盖住冷库里的订单；重放的支付结果会对付清的订单要求退款 |
| 2026-11-14 | orderindex 对 DELETE 从“忽略”改为“把删除前的映像做带版本号的 upsert” | 归档在热库上是 DELETE；商家索引保留归档订单的行，索引重建时这条消息是它唯一的来源 |
| 2026-11-14 | 步骤五的五个并行任务仍在主工作目录里各改各的目录，没有用 worktree | 主线的共享改动没有提交 |
| 2026-11-14 | 1d 分六步，先还吞吐的欠账；步骤一增加“MySQL 侧库存扣减改成异步”和“商家索引按批写” | 限流和熔断的阈值要对着实际容量定；实测发现瓶颈是热门 SKU 的行锁。见设计文档 1d 第 1 到 3 节、ADR-0017、ADR-0018 |
| 2026-11-14 | 1d 的服务侧保护不用 sentinel-go，服务之间的熔断和并发隔离改用 Envoy | sentinel-go 最新正式版是 2021 年的；主人不想引入停更的库，并提出这类组件在生产里通常是独立的。见 ADR-0020 |
| 2026-11-14 | 1d 步骤四：闸门的 Redis 不可用时拒绝下单，不退回数据库扣减；增加落库健康（受理时拒绝落不了库的订单）和列表的部分结果；落库和订单事件的消费者不设重试上限 | 退回数据库扣减在多实例下会超卖、切回要重新装载闸门，而 ADR-0018 之后 MySQL 已不阻止超卖。见设计文档 1d 第 6 节、ADR-0021；主人确认 |
| 2026-11-14 | 步骤四的三个并行任务（order 读路径、inventory 和 payment、gateway）仍在主工作目录里各改各的目录，没有用 worktree；故障注入脚本在它们验收之后才派 | 主线的共享改动没有提交；故障注入要暂停共享的 Redis，不能和别的测试同时跑 |
| 2026-11-14 | 1d 步骤五：闸门（Redis）和 MySQL 的库存不纳入 Merkle 对账，维持 inventory 现有的 `reconcile`；树的节点摘要用行哈希的和，不是教科书式的 H(子节点) | SKU 很少、数一直在变，逐个比就是对的做法；两边分片方式不同，摘要要能跨分片相加。见设计文档 1d 第 7 节、ADR-0022；主人确认 |
| 2026-11-14 | 1d 步骤六：`outbox` 表加 `trace_context` 列；本机多一个可选的 `otel-lgtm` 容器（`obs` profile）；步骤六的五个并行任务仍在主工作目录里各改各的目录，没有用 worktree | 链路要穿过 outbox 只能跟着数据走；埋点要被真实的后端收过才算数。见设计文档 1d 第 8 节、ADR-0023，主人确认。主线的共享改动没有提交，worktree 里看不到 |
| 2026-11-15 | 链路后端从 SkyWalking 换成 Grafana Tempo，不部署 SkyWalking | 本机已经在用 Tempo；三种信号统一成 PromQL、LogQL、TraceQL，Agent 只对接一套；省约 3 GB 内存；去掉 ADR-0004 的待验证项。见 ADR-0024，主人提出并确认 |
| 2026-11-15 | 阶段 5 增加“封闭式判断交给决策模型（Gemini Decision 1）”的对比实验，挂在模型分层下面；3d 和阶段 4 不接，主循环保持 Claude | 它只做是否、多选、打分，不能做主循环；适合判分、裁剪工具输出、告警分诊，接不接由评测数据决定。主人提出并确认 |
| 2026-11-15 | 诊断 Agent 的 Kubernetes 只读工具从阶段 5 提前到 3d，pprof 工具提前到阶段 4（Go 代码缺陷实验之前） | 阶段 4 的 Pod 和节点类故障、Go 缺陷类故障没有这两个工具基本诊断不了，运行记录也就没有评测价值。主人提出并确认 |
| 2026-11-15 | 2b 的 Redis 从“Redis Cluster 3 主 3 从”改为按用途分两套；Kafka 用 4.3.2 | 闸门的脚本不能在 Cluster 上运行；Strimzi 还不支持 4.4。见设计文档 2b 第 1、2 节；主人确认 |
| 2026-11-15 | 2a 的入口控制器不再考虑 ingress-nginx；虚拟机等宿主机换上 1TB 的盘再建 | ingress-nginx 已于 2026 年 3 月停止维护；现有系统盘只剩 170 GB，放不下原规划的 360 GB，主人不想先建小规格再重建 |
| 2026-11-11 | 设计修正：支付服务退款的前置条件增加 `notified = 0`（第一次答复为准） | 原设计下矛盾的答复会把已付清订单的钱退掉，由子 Agent 发现 |
| 2026-11-11 | inventory 不建 outbox 表 | 步骤二里它只消费不发布，空表加没有中继容易误导；主人决定去掉 |
| 2026-11-11 | 设计修正：订单不存在且支付失败时答复 `ACCEPTED` 而不是 `REFUND_REQUIRED` | 没有收钱，不应要求退款 |

## 会话日志

### 2026-11-11

制定规划并落成文档。本机工具现状：Go 1.27.3、kubectl 1.33、Docker 29.8（OrbStack）已装；helm 未装，阶段 2 前安装。

同日完成阶段 0 并合并；开始 1a，完成契约、表结构、公共库和设计文档，四个服务派给子 Agent 并行实现；评审、合入、端到端测试和笔记全部完成，待推送。

同日讨论了金额用整数分的理由和对外接口是否用 OpenAPI。决定采用规范先行（`oapi-codegen`），写了 ADR-0005，在阶段 1 计划里加了「1a 补充」一节。另外补记了 ADR-0006（金额用最小货币单位的整数），这是 1a 已经执行的决定，不涉及代码和计划变更。本次只改文档。

同日完成 1a 收尾：文档改动作为独立提交进入 1a 分支，推送、开 PR #2、CI 全绿后合并到 `main`。

同日实施「1a 补充」：写了 OpenAPI 规范，引入 `oapi-codegen`，gateway 改为实现生成的接口，本机 lint、测试、端到端全部通过。随后提交、推送、开 PR 并合并到 `main`（git 操作派给 Haiku 子 Agent）。生成客户端的可选任务移到阶段 3b。

同日开始 1b：写了设计文档和 ADR-0007、ADR-0008，完成步骤一的公共库（三个子 Agent 并行实现，主会话评审合入，outbox 由主会话编写）。步骤二改了 proto 和脚本后暂停，服务改造尚未开始。

同日继续 1b：重新派出第二批四个子 Agent，全部完成。主会话评审合入，修了 outbox 的两处缺陷，`make lint test e2e` 通过，复核结论写入设计文档第 5.6 节。步骤二完成，已推送并开 draft PR #5；CI 的 e2e 首次失败（topic 创建后不可见），修复后全绿。

### 2026-11-12

完成 1b 步骤三。先细化设计文档第 6 节并写 ADR-0009，主会话写库存闸门和契约，order、inventory 派给子 Agent（中途因用量限制中断过一次，恢复后继续）。评审合入后修了两个设计问题，`make lint test e2e` 通过。

同日提交推送步骤三，CI 的 e2e 失败（支付与取消竞争的用例超时）。派 Opus 排查并修复：根因是测试造数耗尽了支付时限，产品代码没有一致性缺陷。主会话评审 diff 后推送修复，CI 全绿。随后细化了步骤四的设计（设计文档第 7 节、ADR-0010），主人确认后定稿（订单只缓存终态）。步骤四的代码尚未动工。

同日完成步骤四。主会话（Fable）写了 `lock.FailOpen`、`pkg/cache` 和 `redis-cache`；order、inventory、payment 的接入、压测工具、杀进程脚本、面试笔记、吞吐对比派给 Opus 和 Sonnet 子 Agent，主会话评审后合入。主人中途要求尽量把具体任务派给便宜的模型。杀进程测试发现消费者会话超时和下单重试两个问题，已修。吞吐没有达到数量级提升，主人决定挂到阶段 2。主人随后把会话切到 Opus，由 Opus 完成受理端背压（第一版只看排队时长，压测证明不够，改为按队列长度）。

同日按主人要求修了 e2e 的偶发失败（冷连接池遇上突发流量，新连接丢包要等 1 秒的初始重传超时），推送 `phase-1b` 并合并 PR #5。

### 2026-11-13

开始 1c。Opus 主持：写了设计文档 1c 和 ADR-0012、`pkg/sharding`；order 服务派给 Opus 子 Agent，脚本和对账工具派给 Sonnet 子 Agent，两者并行。合入后 `make lint test e2e` 通过，`make chaos` 两次 PASS。根据子 Agent 的报告补了设计文档 2.6，旧 `MYSQL_DSN` 改为拒绝启动。待 Fable 评审。

同日 Fable 评审步骤一和 1b 遗留代码，结论和修复见“上次做到哪”。修复派给两个 Opus 子 Agent 并行实现，e2e 和 chaos 的验证与回收数调查派给 Sonnet 子 Agent。提交和推送派给 Haiku 子 Agent，随后合入 `main` 并开 draft PR。

同日完成步骤二：设计（设计文档 1c 第 3 节、ADR-0013）经主人确认后，compose 拓扑和延迟用例派给 Sonnet 子 Agent，从库读和读模式派给 Opus 子 Agent，Fable 评审后返工一次。`make up` 现在起 9 个容器，本机依赖处于运行状态。

同日收尾：步骤二分三个提交推送，CI 全绿；加了 `make up-core`，CI 的按模块测试 job 改用它。下一个会话从步骤三的设计开始。

同日写了步骤三的设计（设计文档 1c 第 4 节、ADR-0014），主人确认后开始实施：主线完成了商家模型、行版本号和共享契约，修了容器重建后复制起不来的问题，三个子 Agent 并行实现 cdc、orderindex 和 gateway 的接口（中途因用量限制中断过一次），Fable 评审后合入；e2e 用例和对账项派给 Sonnet。步骤三完成并推送；CI 的 e2e 有一个竞争用例因取消窗口太窄失败，加宽后重新推送。

### 2026-11-14

确认 PR #6 的 CI 全绿。写了步骤四的设计（设计文档 1c 第 5 节，原第 5 节的步骤五要点挪到第 6 节）和 ADR-0015，主人确认了四项（Epoch 提前到 2020、改表名做围栏、散发加归并只放在造数工具里、默认 500 万单）。

主线（Fable）做了共享部分：库数由 DSN 个数决定加 `shard_meta` 校验、00003 迁移、`ListUserOrders` 的 proto、Epoch，清空了本机开发库。然后三个子 Agent 并行：按用户列出订单（Opus）、造数工具（Opus）、扩容脚本和 compose（Sonnet）。主线同时读代码确认围栏期间的行为，发现支付结果的消费者 1.5 秒后进死信并修掉，给 cdc 加了 `-print-checkpoint`。

评审后合入。扩容脚本改了三处（等待超时自动撤围栏、建从库移出窗口、校验不怕新写入）。灌了 500 万单并出报告，带流量演练了 2 库扩 4 库：窗口 9.3 秒，8181 个订单全部落库，对账和数据校验通过，结果在设计文档 5.11。做完后分 6 个提交推送（git 操作和看 CI 派给 Haiku），CI 全绿。

同日按主人要求在路线图里加了可选的阶段 9（eBPF），并回答了两个设计问题，要点记进了阶段计划的考点：为什么用 CHECK 而不是 UNSIGNED，`SKIP LOCKED` 适合什么场景。

同日商家索引追平，补了索引这一半的造数报告；按主人要求加了 kafbat-ui（Sonnet 子 Agent）。CI 在一次只改文档的提交上偶发失败过一次（并发迁移用例），重跑通过，根因未找到，记在“下一步”里。

同日写了步骤五的设计（设计文档 1c 第 6 节、ADR-0016），主人确认了四项（按订单号时间划分、压缩表加按年分区、列表冷热归并、商家索引保留）。主线（Fable）写了冷库迁移、冷库访问层、`MinHotAge`、落库拒绝和支付结果的冷库路径；五个子 Agent 并行：归档任务（Opus）、读路径（Opus）、orderindex（Opus）、脚本和 e2e（Sonnet）、对账和报告（Sonnet）。Fable 逐行评审了归档任务和读路径，补了归档任务的互斥锁和扫描语句的固定索引。

在本机 500 万单上实跑：482 万单 8 分 8 秒搬完，行数守恒，对账通过，从库延迟最大 1 秒；热库磁盘占用从 2273 MB 到 217 MB（要重建表才回来）；冷库刚归档完几乎没有压缩效果，重建分区后 906 MB。结果在设计文档 6.14。

同日主人同意提交，步骤五分 7 个提交推送，CI 全绿；随后合并 PR #6 进 `main`，开了 `phase-1d`。写了 1d 的分步方案和步骤一的设计（`docs/design/1d-protection.md`、ADR-0017），主人确认后开始实现：主线写了 kafkax 的分区内并发消费和 `fetch` 的新读顺序，三个子 Agent 并行做落库并发、orderindex 并发和开环压测。

同日继续 1d 步骤一的延续（主人定了 MySQL 异步扣减、商家索引按批写、推送并开 PR #7）。Fable 写了 kafkax 的按批消费和 inventory 的异步扣减（`stock_pending` 加应用者）；orderindex 按批写、inventory 测试的铺开派给 Opus，对账和脚本派给 Sonnet。测试铺开时发现超卖时预热和对账失效的两个缺陷，已修。实测见设计文档 2.11、3.9。

同日做 1d 步骤二（网关限流）：设计经主人确认后，Fable 写 `pkg/ratelimit`，网关接入派 Opus、脚本和 e2e 派 Sonnet。网关的子 Agent 提醒了 Redis 客户端的两个默认行为，核实后修掉了一个 1b 起就有的缺陷（受理脚本被重试后订单消失）。实测见设计文档 4.17。

同日做 1d 步骤三。查到 sentinel-go 停更五年，主人不想引入，并提出这类组件在生产里通常是独立的；最终定为每个服务一个 Envoy 边车。先在本机用可控的 gRPC 上游把 Envoy 的行为逐项验证（发现三个默认配置下不生效的坑），再写设计。Fable 写边车的配置和进程里的公共部分，gateway、order 和 payment 的接入派 Opus，脚本和故障注入派 Sonnet。实测见设计文档 5.14。

教训：引入一个没用过的组件，先用最小的探测把它的行为跑出来再写设计，文档里的默认值和“应该会怎样”靠不住（单实例下摘除不生效、慢下游不计入摘除，都是跑出来才知道的）；即使跑过，设计时也可能没把两个事实连起来（“调用方先放手不计入”和“两处时限相同”），子 Agent 的独立检查补上了这一环。

教训：并发测试本机通过不说明什么，CI 更慢、交错不同；论证“不会发生”时要分清一致性读（快照）和加锁读（不是快照）。

教训：客户端库的默认行为（会不会重试、时限从哪来）要读源码确认；子 Agent 报告里“待核实”的提醒值得逐条查，这次查出的问题比任务本身重要。

教训：一个值的取值范围变了（真实可售数可以为负），要把它所有的读者过一遍；改坏验证时测试没有失败，说明那条性质没有被断言（`SKIP LOCKED` 的害处是误报而不是算错，第一版测试只看了最终的数）。

教训：`pkill -f <脚本路径>` 会连带杀掉命令行里出现这个路径的其他 shell（这次杀掉了启动归档的外层 shell，归档进程本身没事）。要按进程号杀。MySQL 9.7 的客户端在 `-e` 里不认 `\G`，用 `-E`。

教训：直接在模块目录里跑 `go test` 时集成测试会因为没有环境变量而全部跳过，看起来是通过。要用 `make test MODULES=$PWD/<模块>`。

同日做 1d 步骤四。先读代码摸清每个依赖坏掉时的现状（发现闸门不可用超过约 26 秒时已受理的订单会进死信后消失、可选依赖会挡住启动等），写设计和 ADR-0021，主人确认四项都按建议（闸门不可用时拒绝下单，不退回数据库扣减）。Fable 写公共库和 order 的受理、落库、回收；order 读路径、inventory 和 payment、gateway 派 Opus 并行，Fable 评审；故障注入的场景派 Sonnet。实测见设计文档 6.18。

教训：一层保护的输出是另一层保护的输入。降级时返回的 `UNAVAILABLE` 会被调用方的边车当成服务故障、把整个服务摘掉；设计降级的错误时要把步骤三的熔断规则一起看。

教训：把测试输出过滤得太狠会把编译失败也滤掉。这次有两轮“通过”其实是测试没编译过，过滤条件里要留着 `build failed` 和每个包的 `ok`。

同日做 1d 步骤五（对账）。先在本机冷库上量了“行读到客户端”和“库里聚合摘要”的耗时，再写设计和 ADR-0022，主人确认四项都按建议。Fable 写 `pkg/merkle` 和五对的投影，引擎、修复、命令行派 Opus，Fable 评审后改了三处；脚本和 chaos 的接入很小，主会话直接写了。在 500 万单上实测并做了验收演练，结果在设计文档 7.12。

教训：`DELETE` 走不了索引时是带锁的全表扫描，会挡住这张表的所有写入；先用不加锁的读找到主键，再按主键删。评审时对每条写语句都要问它走哪个索引。

教训：性能对比的基线要先确认它自己没有被写慢（全量归并的第一版发了 2.9 万条大多读不到行的语句）；量之前先想清楚自变量是什么（这次是“差异落在多少个节点里”，不是“差异有多少行”）。

教训：给“什么算成功”下定义要想反例。落库健康第一版把“消息处理完了”当成功，而依赖坏着时被 sweeper 置为失败的订单也会“处理完”。

### 2026-11-15

阶段 2a 开工。摸清三台设备的现状后发现四件和计划不符的事：宿主机只连着 Wi-Fi；系统盘只剩 170 GB；R2S 的代理有白名单；ingress-nginx 已停止维护。主人插了网线、开了免密 sudo、给 NAS 加了公钥，决定换上 1TB 的盘再建虚拟机，并定了 Envoy Gateway、内嵌 etcd、CNI 按内存决定（ADR-0025、ADR-0026）。

同日做了：宿主机装 KVM 和 libvirt、建网桥（`host-setup.sh`）；整机备份到 NAS；换盘（方案 A：`pvmove` 在线迁移根分区，复制 `/boot` 和 EFI，重装 GRUB，中间重启验证，旧盘清空）；建 600G 的 `vmpool` 卷；写 `vm.sh` 并建好三台虚拟机。

同日新开一个会话：把上面的工作分两个提交存在本地（没有推送），写了 2a 的设计文档草稿。写之前对三台设备做了只读的探测并核对了版本，查出两处和之前记录不符的：Envoy Gateway v1.10 配的 Gateway API 是 v1.6.1；NAS 的 NFS 服务本来就开着。还没有装 k3s。

主人确认设计并建好 NAS 的共享文件夹之后，同一个会话里把 2a 做完：装 k3s、MetalLB、仓库、Envoy Gateway，最后删掉三台虚拟机用 `make cluster-up` 重建了一遍。`k3s.sh` 派给 Sonnet 写，其余清单和脚本很小，主会话直接写了；集群上的执行都是主会话做的。

教训：一项检查“通过了”要问它是不是碰巧通过。建虚拟机时验证过节点能访问镜像源，装 k3s 时却拉不到镜像：两个 DNS 上游一好一坏，第一次恰好用的是好的那个。有多个上游、多个副本、多条路径时，要逐个验证，不能只看整体的一次结果。

教训：Helm chart 的默认值要先 `helm show values` 看一遍再写 values。MetalLB 这次按记忆关了一个已经弃用的开关，真正的开关是另一个，结果多装了一套用不到的组件。

教训：本机的 `rm` 是 `trash`，不认 `-f`。在 `&&` 串起来的命令里它失败会让后面的步骤全部跳过；脚本和一次性命令里用 `command rm`。

教训：改公共库的类型之后要对**全部**模块跑 `make lint` 和 `make build`，不能只跑自己想到的那几个。这次按“谁在用 `*redis.Client`”搜了 `pkg` 和 `services`，漏了 `test/load`，推送后 CI 才发现。

教训：新环境装完要看内核日志，不能只看功能。2a 里所有 Go 程序的抢占信号一直在被 AppArmor 拒绝，但什么都“正常”，直到 2b 碰上一个靠信号退出的程序（mysqld）才暴露。排查的顺序是：先用不带任何自定义配置的官方镜像复现，排除自己写的 chart；再逐个去掉可疑的保护层（这里是 AppArmor）；确认之后去内核日志里找具体被拒绝的操作。

教训：子 Agent 报告里“我认为设计有问题”的那一条（主从角色记在 Pod 标签上，Pod 重建就丢）是对的，而且是主会话写设计时没有想到的。设计里凡是“把状态记在会被重建的对象上”的地方都要问一句：它重建之后谁来恢复这个状态。

教训：静态地址的网桥在没有载波时地址和路由都还在，而且比备用网卡的路由优先。说“留了一条备用通道”之前要想清楚主通道以各种方式坏掉时内核会怎么选路；这次是拔网线后整台机器从 IPv4 连不上，靠 IPv6 的链路本地地址才进去修。

教训：`set -o pipefail` 下不要写 `命令 | grep -q`，grep 匹配到就退出，前面的命令收到 SIGPIPE，整条管道算失败。这次让“停掉默认网络”那一步悄悄没有执行。

教训：链路只协商到百兆时，让网卡只通告千兆是个决定性的测试：起不来就是物理层少了线对，不用再猜驱动和设置。
