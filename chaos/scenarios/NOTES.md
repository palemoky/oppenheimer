# 场景池的内部笔记（主人不读）

和 `docs/design/4b-experiments.md` 配套：那一份主人能读，不写内容；这里写内容。接手的 Agent 先读这里。

## 现在的池（2026-11-24，B1 之后）

“盲”是 `scenario.yaml` 的 `blind`：`yes` 的才会被 `chaos-blind` 抽到。

| 编号 | 档 | 盲 | 方式 | 目标 | 是什么 | 跑过没有 |
|---|---|---|---|---|---|---|
| s001 | 2 | yes | 开关 `f01` | gateway | 查库存的时限用了无缓冲 channel，超时的请求泄漏一个 goroutine | 长期运行的那一套，带负载（4a） |
| s002 | 1 | yes | chaos-mesh | inventory | `pod-kill`、`gracePeriod: 45`：正常下线，“没出事”的对照 | e2e（B1）；带负载的是改之前的写法 |
| s003 | 1 | yes | chaos-mesh | payment | 出站延迟 100 毫秒 | e2e，只验了注入 |
| s004 | 1 | yes | chaos-mesh | order | `container-kill`：进程直接被 KILL，原地重启 | e2e |
| s005 | 1 | yes | chaos-mesh | order | 一个副本的容器里 3 个 CPU worker | e2e |
| s006 | 1 | yes | chaos-mesh | gateway | 一个副本要 640 MB，超过 limit，OOM 一次 | e2e |
| s007 | 1 | no | chaos-mesh | inventory | 出站丢包 10% | e2e，只验了注入。**第一次带负载时主会话带着做，恢复加 `WAIT_RECON=1`**（预占会大量超时，是对补偿的真考验） |
| s008 | 1 | no | chaos-mesh | orderindex | Pod 到集群 DNS 的包全丢，随连接到期逐渐显现，35 分钟 | e2e，只验了解析失败。带负载半小时以上的表现没有看过，第一次主会话带着做 |
| s009 | 2 | no | 开关 `f02` | gateway | 终态订单的详情缓存，没有上限、不淘汰（内存泄漏） | 没有。**太慢**：估计每个副本每小时 12 到 16 MB，3 小时看不出来。要调压测里“支付后再查订单”的比例，或者接受它是一个要看 heap 剖析的差值才发现的场景 |
| s010 | 2 | no | 开关 `f03` | inventory | 查库存的整段（含 Redis、读库）罩在一把全局互斥锁里（锁竞争） | 没有。稳态下估计 p99 只到两三倍；和“缓存 Redis 变慢 3 到 5 毫秒”叠加才飙升。要 mutex、block 采样开着 |
| s011 | 2 | no | 开关 `f04` | order | 订单列表逐行重新编译一个大正则（CPU 和 GC） | 没有。估计每个 Pod 多 0.15 到 0.3 核，可能偏弱；不要和 s005 连着做 |
| s012 | 2 | no | 开关 `f05` | orderindex | 商家列表首页缓存，两条路径加锁顺序相反（死锁） | 没有。估计每个副本约 3 分钟撞上一次，波动很大，30 分钟里可能一次都不发生 |
| s013 | 2 | no | 开关 `f06` | order | 兜底扫描读从库时，空结果的分支没有交还连接（连接泄漏） | 没有。只影响抢到主的那个 Pod 的从库连接池，2.5 到 5 分钟耗尽；用户无感，是个安静的场景 |
| s014 | 2 | no | 开关 `f07` | orderindex | 并行解码的 goroutine 里对 `paid_at` 的类型断言 panic（毒消息，反复重启） | 没有。**要重启后还在的那条路**（`persist: yes`，2026-11-26 写了，没有上集群）；只经管理端口开的话每个副本崩一次就好了 |
| s015 | 2 | no | 开关 `f08` | orderindex | 一批多于 32 条时并行反序列化并按 key 收拢，两个 goroutine 无同步地写同一个位置，旧映像盖掉新映像（数据竞争，商家索引停在旧版本） | 没有。**只在 e2e 里做**。要有积压才触发（开关打开后重启 cdc）；恢复要手工跑带 `-repair` 的对账 |

每个场景自己的“没有核实的”写在它的 `truth.md` 的备注里。第二档新加的六个的 `risk` 是任务说明里统一写的 2：s010、s011 关了开关就恢复、不用重启，按 README 该是 1，带负载试过之后再定。

## B1 试出来的（2026-11-24，`oppenheimer-e2e`，没有负载）

- `pod-kill` 的 `gracePeriod`：45 是正常下线（Terminating 约 10 秒，就绪探针先 503）；0 是 3 秒内 Pod 从 API 里消失，没有排空。
- `container-kill`：退出码 137、`reason: Error`、原地重启、没有 `Killing` 事件，不用写 `duration`。
- StressChaos：没有 CPU limit 的容器 3 个 worker 就是 3 个核；内存超过 limit 是整个容器 OOMKilled，**重启后不再注入**。
- **DNSChaos 对 distroless 的业务容器注入不了**（辅助程序在容器里报找不到文件）；对 `istio-proxy` 也不行（原生边车是 init 容器，选不中）。s008 改成了 NetworkChaos 挡 kube-dns 的地址（`externalTargets` 写 ClusterIP 是管用的：这也说明“服务到数据库的分区”可以用同样的写法，目标写数据库 Service 的地址；headless 的要写 Pod 的地址）。
- 盲测：`blind` 真的跑了一次，终端上没有编号、目标、方式。对账的闸（假的结果）拦得住、确认后放行。
- **Istio 的故障注入没有试成**：主会话从本机经端口转发打 gateway 的查库存接口，被鉴权挡在 401，请求没有走到 inventory，两种写法（另加一个 VirtualService、改现有的）都看不出效果。B4 开始之前重试：要带合法的令牌，或者直接在网格里的 Pod 之间发 gRPC。

## 还没有做的（2026-11-26 更新）

- （写了，2026-11-26，没有上集群）`f08` 数据竞争：s015。测试里走到有缺陷路径的用例带 `faults && !race`，`make test-faults` 不会执行它们，要不带 `-race` 另跑。
- （写了，2026-11-26，没有上集群）重启后还在的那条路：环境变量改名叫 `STARTUP_OVERRIDES_FILE`（名字在 Pod 规格里，Agent 读得到），Secret `startup-overrides` 的键 `enabled`，chart 的 `startupOverrides.enabled`（`GO_TAGS=faults` 部署时五个服务自动打开），`switch.yaml` 的 `persist: yes`（s014 标了）。设计文档里仍然叫 FAULT_FILE。知道的问题：
  - 走文件的开关没有 ttl：不跑 `recover` 它就一直在（总闸会删 Secret）。“脚本死了故障也会自己消失”对这一类不成立。
  - 写完 Secret 固定等 120 秒再开开关，没有办法核对文件真的同步进了 Pod（distroless 里没有 shell）。第一次在 e2e 里试时看：崩过一次重启之后 `GET /fault` 里这个开关是不是开着、没有 `until`。
  - `recover` 收尾的 `recon-check` 用 `APP_NAMESPACE`（默认 `oppenheimer`），不跟 `CHAOS_NS`：在 e2e 里做带对账的场景（s015）要显式设，或者改脚本。没有改。
- 集群上还是 `92d2426-p4`（`f01` 到 `f07`，没有 `f08`，没有挂那个卷）。要提交、重新构建、滚动一次。
- 第二档的七个（s009 到 s014 带负载，s015 在 e2e）都要试一次再决定标不标 `blind: yes`（s015 不盲）。
- s015 的子 Agent 提的：注入脚本不支持“打开开关之后重启另一个工作负载”这一步，s015 制造积压要手工重启 cdc。
