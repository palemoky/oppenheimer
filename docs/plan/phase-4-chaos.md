# 阶段 4：故障注入与复盘

**目标**：在可控条件下把系统弄坏，练习发现、定位、止血、修复的完整过程，并用结果反过来改进监控、预案和架构。
**设备**：Ubuntu（集群）、Mac mini（实验期间持续施压）

## 实验方法

每次实验遵循同一流程，记录用 [`docs/postmortems/TEMPLATE.md`](../postmortems/TEMPLATE.md)：

1. **假设**：写下预期，系统会怎样、哪条告警会响、多久恢复。
2. **稳态**：实验前后台持续压测，确认指标处于基线。
3. **注入**：尽量由脚本随机挑选场景，让主人在不知道故障内容的情况下排查（盲测）。
4. **排查**：只靠监控、日志、链路和 kubectl，记录时间线和走过的弯路。
5. **恢复**：止血与根治分开记录。
6. **复盘**：假设与实际的差距，产出改进项（补告警、补 runbook、改代码、改架构），并跟踪到完成。

每个实验的注入清单和真实根因要按固定格式存档，阶段 6 会用作 Agent 的评测集。

**Agent 同步参与**：阶段 3d 的最小诊断 Agent 在每次实验中与主人同时排查，各自独立得出结论。复盘时对比两者，记录 Agent 错在哪、缺什么工具或知识，当场或攒一批后改进。它的运行记录是阶段 6 评测的原始素材。

## 4a 准备

设计：[4a-chaos-prep.md](../design/4a-chaos-prep.md)（2026-11-24 写；同一天主人确认第 2 节的十项都按建议）。进度见 `PROGRESS.md`。

- [x] 安装 Chaos Mesh
  - **做完了（2026-11-24，主人同意后）**：`make chaos-mesh` 装、`make chaos-smoke` 冒烟，六类都生效并且到时恢复；目标命名空间里会留下 `PodNetworkChaos` / `PodIOChaos` 对象和事件（对盲测是泄漏，怎么处理在第 4、5 步里定）。结果在[设计第 6 节“第 1 步”](../design/4a-chaos-prep.md)。e2e 那一套删了，Chaos Mesh 留着，现在没有命名空间能被注入
  - 登记（2026-11-24，当时等主人对下面的具体做法说同意）：按[设计 3.2](../design/4a-chaos-prep.md)。
    1. 装：Helm chart `chaos-mesh` 2.8.8 到命名空间 `chaos-mesh`，`chaosDaemon.runtime=containerd`、`socketPath=/run/k3s/containerd/containerd.sock`、`enableFilterNamespace=true`、不装 dashboard。会多出一个特权的 DaemonSet（三个节点各一个 `chaos-daemon`）、controller-manager、`chaos-dns-server`，内存预算 0.5 GB。
    2. 冒烟：只在 `oppenheimer-e2e` 里（先 `make cluster-e2e E2E_KEEP=1` 起一套），给它加注入的注解，六类各一个最小的对象、各 30 秒：PodChaos（杀一个 Pod）、NetworkChaos（延迟、分区）、StressChaos（CPU、内存）、IOChaos、TimeChaos、DNSChaos。长期运行的 `oppenheimer`、`mysql`、`redis`、`kafka`、`monitoring` 不加注解，不会被注入。
    3. 做完删掉 e2e 那一套和注解；Chaos Mesh 留着。
    4. 不发压。影响：e2e 命名空间里的 Pod 会被杀、被限速；三个节点上各多一个特权 Pod。
- [x] 代码缺陷类故障的注入机制（`pkg/fault`）：这类故障无法从外部注入，需要在服务里预埋故障开关
  - 2026-11-24：`pkg/fault`、构建标签 `faults`、三道检查、第一个故障点 `f01` 写了（[设计第 6 节](../design/4a-chaos-prep.md)、[ADR-0041](../adr/0041-two-fault-injection-paths.md)）。**带标签的镜像没有构建和部署过，开关没有在集群上打开过**；其余的故障点在 4b 里一个个加
  - 通过管理接口或配置在运行时触发，默认关闭
  - 故障代码要写得像真实的 bug（例如忘记关闭的 goroutine、无上限的缓存、不一致的加锁顺序），而不是一眼能看出来的 `sleep`
  - 用构建标签隔离，正常构建的二进制里不包含故障代码
- [ ] 给诊断 Agent 加 pprof 工具（从阶段 6 提前，2026-11-15），在做下面“Go 代码缺陷”那一批实验之前完成：goroutine 泄漏、内存泄漏、死锁没有 pprof 基本诊断不了
  - 抓不抓、抓哪一种由 Agent 看了运行时指标之后决定，不做成“指标一超阈值就自动抓”：什么时候该抓 profile 本身是要评测的能力
  - 按症状选类型：CPU 高抓 CPU profile（默认 10 秒）；goroutine 数涨抓 goroutine（瞬时的，不用等）；内存涨抓 heap；延迟高而 CPU 不高抓 mutex 和 block
  - 喂给模型的是摘要，不是原始文件：top N 的函数和占比，goroutine 按调用栈聚合后的前几组和各自的数量
  - CPU profile 对被诊断的进程有开销，不算纯只读：每个 Pod 的抓取频率和时长设上限，pprof 只开在内部端口上，每次抓取都留痕
- [x] 实验清单目录 `chaos/`，每个场景一个可重复执行的清单
  - 2026-11-24：格式定了（`chaos/README.md`，`scripts/cluster/chaos-run.sh lint` 检查），三个场景 `s001` 到 `s003`。**主人在实验之前不读 `chaos/scenarios/`**
- [ ] 盲测脚本：随机选场景注入，并把答案密封到文件
  - 2026-11-24：`scripts/cluster/chaos-run.sh`（`make chaos-blind|chaos-inject|chaos-recover|chaos-reveal`）写了，主会话评审过。**连集群的路径一条都没有真的跑过**（只用假的 `kubectl` 走过逻辑），第 6 步走通一遍之后再勾
- [ ] 实验期间的稳态压测脚本与数据一致性检查脚本
  - 2026-11-24：`scripts/cluster/chaos-steady.sh start|stop|check` 写了。`check` 对着真的集群跑过（只读：告警、探针、五个 SLO 都读得到；没有发压所以“负载”一项不过，符合预期）。`start`、`stop`（k6）没有跑过。数据检查用已有的 `make recon-check`，由 `chaos-run.sh recover` 调
- [ ] `mysql-main` 的 `MALLOC_ARENA_MAX=4` 管不管用（2026-11-24 加的，主人定了第一次发压时一起看）：连接涨到 200 以上时常驻内存涨多少、之后回不回落（没有它时 +300 MiB、不回落），p99 和吞吐有没有变差。背景和对照的数在 `PROGRESS.md` 的“mysql-main 的内存”
- [ ] 3c 留下的告警验证（[设计 3c](../design/3c-slo-alerting.md) 第 8 节“第 16 项”），做对应的实验时顺带完成：
  - `OrderAdmitRateDropped` 在“一小时前没有流量”时不起作用，这个原因是推断的：先用数据核对（探针停一段之后的一小时里看表达式两边各自有没有值），再决定改不改规则
  - `OtelCollectorExportQueueFull` 单独响的情形：gateway 在而 agent 发不过去（比如挡住 agent 到 gateway 的 4317）
  - 3e 留下的（2026-11-24）：`DerivedDataCheckStalled` 没有真实触发过（要“核对停了”同时“订单库有写入”，e2e 里没有流量）；`recon-changes` 的从库目标没有造过不一致；有负载时三个常驻核对的开销、每天全量对账对线上的影响没有量
  - 只有单元测试的：超卖、闸门内存写满、Collector 导出失败、支付可用性的三档；`RateLimitFallingBack` 改成按比例之后没有真实触发过

## 4b 实验清单

**Go 代码缺陷（常见生产故障，通过故障开关注入）**

- [ ] goroutine 泄漏：阻塞在无人接收的 channel、未取消的 context、未关闭的 response body
- [ ] 内存泄漏：无上限的 map 或缓存、切片引用大数组、未停止的 ticker、全局变量持有
- [ ] 死锁：加锁顺序不一致、锁内调用会再次加锁的函数、channel 互相等待
- [ ] 锁竞争导致吞吐下降（不是死锁，但延迟飙升）
- [ ] CPU 飙高：死循环、正则回溯、大量小对象引发的 GC 压力
- [ ] 数据竞争导致的偶发错误数据
- [ ] 连接泄漏：数据库或 Redis 连接未归还，连接池耗尽
- [ ] panic 未恢复导致 Pod 反复重启

排查工具要练到：pprof（goroutine、heap、mutex、block、cpu）、`go tool trace`、运行时指标、core dump 与 delve。

**应用层**

- [ ] 随机杀业务 Pod
- [ ] Pod CPU 打满、内存压力直到 OOM
- [ ] 服务间调用注入延迟与错误（验证超时、重试、熔断）
- [ ] 下游变慢引发的级联故障（验证不会雪崩）

**网络**

- [ ] 网络延迟、丢包
- [ ] 网络分区：服务与数据库之间、节点之间
- [ ] DNS 故障

**MySQL**

- [ ] 主库宕机，主从切换，验证数据不丢、不重
- [ ] 从库延迟拉大
- [ ] 慢查询：缺索引、索引失效（隐式类型转换、函数包裹列、最左前缀不满足）、深分页、统计信息过期导致选错索引
- [ ] 死锁：两个事务以相反顺序更新行、间隙锁冲突；读懂 `SHOW ENGINE INNODB STATUS` 的死锁日志
- [ ] 锁等待与长事务：未提交事务阻塞写入、元数据锁阻塞 DDL
- [ ] 连接池耗尽
- [ ] 大事务造成主从延迟
- [ ] 磁盘写满

排查工具要练到：慢查询日志、`EXPLAIN` 与 `EXPLAIN ANALYZE`、`performance_schema`、`sys` 库的锁等待视图。

**Redis**

- [ ] 主节点宕机与故障转移，验证库存不超卖
- [ ] Redis 整体不可用，验证降级预案
- [ ] 大 key：超大的 hash、list、zset 导致读写阻塞、删除卡顿、集群迁移失败；练习发现（`--bigkeys`、`MEMORY USAGE`）与拆分、`UNLINK` 渐进删除
- [ ] 热 key：单个 key 流量集中压垮一个分片；练习发现与本地缓存、key 拆分
- [ ] 慢命令阻塞：`KEYS`、大范围 `HGETALL`、复杂 Lua 脚本；读 slowlog
- [ ] 内存打满与淘汰策略触发、大量 key 同时过期

**Kafka**

- [ ] broker 宕机，验证消息不丢
- [ ] 消费者频繁重平衡、消费积压
- [ ] 消息重复投递、乱序

**集群**

- [ ] 节点宕机（直接关虚拟机）
- [ ] 节点磁盘、内存压力触发驱逐
- [ ] 控制面不可用时业务是否受影响
- [ ] 时钟偏移（验证订单号生成）

**综合**

- [ ] 秒杀压测期间叠加故障
- [ ] 发布期间叠加故障
- [ ] 两个故障同时发生

## 验收标准

- 每个实验有完整复盘，改进项全部关闭或有明确结论。
- 所有实验后数据一致性检查通过（零超卖、对账无差异）；不通过的作为缺陷修复后重做。
- 盲测下，常见故障能在 15 分钟内定位。
- 形成一份故障速查手册：现象 → 可能原因 → 验证方法。

## 面试考点

4a 装 Chaos Mesh 补的（[设计 4a](../design/4a-chaos-prep.md) 第 6 节“第 1 步”）：Chaos Mesh 的几类故障各自在哪一层动手（NetworkChaos 是目标 Pod 网络命名空间里的 tc/netem 和 iptables，所以延迟加在出方向、一来一回是两倍；StressChaos 是往目标容器的 cgroup 里放 stress-ng；IOChaos 是把卷换成一层 FUSE；TimeChaos 是 ptrace 进去改 vDSO 里取时间的函数，所以只对注入时已经在跑的进程、经过 vDSO 取时间的进程有效；DNSChaos 是把 Pod 的 DNS 指到它自己的 DNS 服务）；`chaos-daemon` 为什么要特权、生产里这意味着什么（它能进任何 Pod 的命名空间，装它等于多了一个能动所有业务的入口，要配命名空间过滤、RBAC、审计）；为什么默认谁都不能被注入（`enableFilterNamespace` 加注解）；冒烟为什么要先验“没开的时候不生效”；验证一类故障要量“生效、到时恢复、留下什么痕迹”三件事。

4a 故障开关补的（[设计 4a](../design/4a-chaos-prep.md) 第 3.3 节和第 6 节、[ADR-0041](../adr/0041-two-fault-injection-paths.md)）：哪些故障从外面造不出来、只能预埋；Go 的构建标签怎么用（`//go:build` 写错了会被当成普通注释，文件就进了平时的构建）、怎么证明一段代码没有进二进制（`go tool nm`，以及这种检查为什么要“反过来确认一次”防止空转）；为什么不靠“常量为 false 的分支会被编译器删掉”；故障开关为什么必须自己会关（ttl）；带缓冲和不带缓冲的 channel 在“先返回、后台还在跑”这种写法里差在哪；shell 里 `pipefail` 加 `grep -q` 的 SIGPIPE 陷阱。

混沌工程的原则；Go 的 goroutine 泄漏、内存泄漏、死锁的成因与 pprof 排查方法；GMP 调度与 GC 原理；MySQL 慢查询优化、索引失效场景、InnoDB 锁（行锁、间隙锁、临键锁）与死锁分析；Redis 大 key 与热 key 的发现和治理；MySQL 主从切换的数据安全（半同步、GTID）；Redis 故障转移期间的数据丢失窗口与脑裂；Kafka 的 ISR、acks、不丢消息的完整条件；CAP 在这些实验里的具体表现；线上故障排查的思路与真实案例（这一阶段的复盘就是案例）。
