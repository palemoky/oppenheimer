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
| s007 | 1 | yes | chaos-mesh | inventory | 出站丢包 10% | e2e，只验了注入。**第一次带负载时主会话带着做，恢复加 `WAIT_RECON=1`**（预占会大量超时，是对补偿的真考验） |
| s008 | 1 | yes | chaos-mesh | orderindex | Pod 到集群 DNS 的包全丢，随连接到期逐渐显现，35 分钟 | e2e，只验了解析失败。带负载半小时以上的表现没有看过，第一次主会话带着做 |
| s009 | 2 | no | 开关 `f02` | gateway | 终态订单的详情缓存，没有上限、不淘汰（内存泄漏） | 没有。**太慢**：估计每个副本每小时 12 到 16 MB，3 小时看不出来。要调压测里“支付后再查订单”的比例，或者接受它是一个要看 heap 剖析的差值才发现的场景 |
| s010 | 2 | no | 开关 `f03` | inventory | 查库存的整段（含 Redis、读库）罩在一把全局互斥锁里（锁竞争） | 没有。稳态下估计 p99 只到两三倍；和“缓存 Redis 变慢 3 到 5 毫秒”叠加才飙升。要 mutex、block 采样开着 |
| s011 | 2 | no | 开关 `f04` | order | 订单列表逐行重新编译一个大正则（CPU 和 GC） | 没有。估计每个 Pod 多 0.15 到 0.3 核，可能偏弱；不要和 s005 连着做 |
| s012 | 2 | no | 开关 `f05` | orderindex | 商家列表首页缓存，两条路径加锁顺序相反（死锁） | 没有。估计每个副本约 3 分钟撞上一次，波动很大，30 分钟里可能一次都不发生 |
| s013 | 2 | yes | 开关 `f06` | order | 兜底扫描读从库时，空结果的分支没有交还连接（连接泄漏） | 没有。只影响抢到主的那个 Pod 的从库连接池，2.5 到 5 分钟耗尽；用户无感，是个安静的场景 |
| s014 | 2 | no | 开关 `f07` | orderindex | 并行解码的 goroutine 里对 `paid_at` 的类型断言 panic（毒消息，反复重启） | e2e（2026-11-26），带负载。走 `persist: yes`；只经管理端口开的话每个副本崩一次就好了 |
| s015 | 2 | no | 开关 `f08` | orderindex | 一批多于 32 条时并行反序列化并按 key 收拢，两个 goroutine 无同步地写同一个位置，旧映像盖掉新映像（数据竞争，商家索引停在旧版本） | e2e（2026-11-26）。**只在 e2e 里做**。要有积压才触发（试的时候是把 cdc 缩到 0 再恢复）；恢复要手工跑带 `-repair` 的对账 |

每个场景自己的“没有核实的”写在它的 `truth.md` 的备注里。第二档新加的六个的 `risk` 是任务说明里统一写的 2：s010、s011 关了开关就恢复、不用重启，按 README 该是 1，带负载试过之后再定。

## B1 试出来的（2026-11-24，`oppenheimer-e2e`，没有负载）

- `pod-kill` 的 `gracePeriod`：45 是正常下线（Terminating 约 10 秒，就绪探针先 503）；0 是 3 秒内 Pod 从 API 里消失，没有排空。
- `container-kill`：退出码 137、`reason: Error`、原地重启、没有 `Killing` 事件，不用写 `duration`。
- StressChaos：没有 CPU limit 的容器 3 个 worker 就是 3 个核；内存超过 limit 是整个容器 OOMKilled，**重启后不再注入**。
- **DNSChaos 对 distroless 的业务容器注入不了**（辅助程序在容器里报找不到文件）；对 `istio-proxy` 也不行（原生边车是 init 容器，选不中）。s008 改成了 NetworkChaos 挡 kube-dns 的地址（`externalTargets` 写 ClusterIP 是管用的：这也说明“服务到数据库的分区”可以用同样的写法，目标写数据库 Service 的地址；headless 的要写 Pod 的地址）。
- 盲测：`blind` 真的跑了一次，终端上没有编号、目标、方式。对账的闸（假的结果）拦得住、确认后放行。
- **Istio 的故障注入没有试成**：主会话从本机经端口转发打 gateway 的查库存接口，被鉴权挡在 401，请求没有走到 inventory，两种写法（另加一个 VirtualService、改现有的）都看不出效果。B4 开始之前重试：要带合法的令牌，或者直接在网格里的 Pod 之间发 gRPC。

## B3 在 e2e 里试出来的（2026-11-26，`oppenheimer-e2e`，带故障代码的构建）

环境是 `make cluster-chaos`（e2e 里自带发压的 Job 和 recon）：`GO_TAGS=faults E2E_KEEP=1 CHAOS_DELETE=0`，另加 `CHAOS_EXTRA_OVERLAY` 把 gateway、payment 降到 1 个副本（集群的 CPU requests 用到了 97%，不降的话第二个 gateway 排不上）。基线（开关全关）一轮压测加对账、verify 全过。留下的现场里再发压、再对账用的是照 `chaos.sh` 写的一个临时脚本（没有入库）：给新的 SKU 灌库存、`load warmup`、`load run`、`recon -pairs index`。

- **s014（`persist`）**：一段 8 分钟的压测跑着（e2e 里 `PAYMENT_TIMEOUT` 是 5 秒，CLOSED 的变更很多）。注入：写 Secret、等 120 秒、开开关。开关打开后 20 秒内两个副本都崩了并且重启后接着崩（一分半钟里重启 3 次，`CrashLoopBackOff`，退出码 2），`--previous` 的日志里是答案文件写的那个 panic 和调用栈（`rowDirect` ← `decodeChanges` 起的 goroutine）。**重启后还在崩就说明文件同步进去了、启动时读到了**；没有另外去读重启后的 `/fault`。恢复：Secret 被删、滚动重启出两个新副本、正常。压测跑完后对商家索引：20889 行两边一致，差异 0（位点没有提交过的那些变更重放了）。期间没有告警响（e2e 里规则不对着它）。
  - 试出一个误报并改了：崩溃循环里的 Pod 关不了开关，脚本把它算成“恢复失败”、退出码 1、提示跑总闸。现在 `persist` 的场景里关不掉只记一笔。第二轮（同一天）重做了一遍 s014 的注入和恢复：没有再误报，运行的状态是 `undo_ok: yes`，Secret 清了、新副本正常。
- **s015**：开 `f08`，把 e2e 的 cdc 缩到 0，发 90 秒压（约 3400 单），cdc 恢复到 1。一分钟后对商家索引：**875 处差异**，缺行 176、同一个订单版本低 699、多出来的行 0、重复 0，复查（隔 30 秒）一条都没有自己好。看到的例子和答案一致：订单库 `status=5 version=2`，索引 `status=2 version=1`。约四分之一的订单坏了，比答案里估的“几十到几百”多：这次所有订单的两次变更都压在同一段积压里。关开关后 `recon -repair`：875 条全部修好；再对一次，24289 行两边一致。orderindex 全程没有重启。
  - 用的是“缩到 0 再恢复”而不是答案里写的“滚动重启 cdc”：滚动重启的停顿只有几秒，攒不出多少积压。`scenario.yaml` 的 requires 还是原来的写法，没有改。
  - **变更核对（recon-changes）第二轮验了（2026-11-26 14:40 到 15:00 UTC，主人说“可以安装后在 e2e 验证一下”）**：把 `deploy/cluster/recon/changes.yaml` 的库名换成 `e2e_` 前缀、加上 `KAFKA_PREFIX=e2e-<运行号>.`，装进 `oppenheimer-e2e`（镜像 `recon:6b115e4`；长期运行的那一个没有动；清单是临时改的，没有入库）。重做一遍 s015：`recon_changes_checked_total{target="index",result="lagging"}` 是 **945**，`mismatch` 是 0，日志里每条是 `derived version 1 < change version 2`；同一时刻逐行对账的差异也是 **945**（缺行 173、版本低 772）。两边一条不差，和答案写的一致。`-repair` 修好 945 条，再对差异 0。注入之前（包括 s014 崩溃循环的那一分多钟）`lagging` 是 0：索引落后不到 2 分钟不报。

## 还没有做的（2026-11-26 更新）

- （写了，2026-11-26，没有上集群）`f08` 数据竞争：s015。测试里走到有缺陷路径的用例带 `faults && !race`，`make test-faults` 不会执行它们，要不带 `-race` 另跑。
- （写了，2026-11-26，没有上集群）重启后还在的那条路：环境变量改名叫 `STARTUP_OVERRIDES_FILE`（名字在 Pod 规格里，Agent 读得到），Secret `startup-overrides` 的键 `enabled`，chart 的 `startupOverrides.enabled`（`GO_TAGS=faults` 部署时五个服务自动打开），`switch.yaml` 的 `persist: yes`（s014 标了）。设计文档里仍然叫 FAULT_FILE。知道的问题：
  - 走文件的开关没有 ttl：不跑 `recover` 它就一直在（总闸会删 Secret）。“脚本死了故障也会自己消失”对这一类不成立。
  - 写完 Secret 固定等 120 秒再开开关，没有办法核对文件真的同步进了 Pod（distroless 里没有 shell）。第一次在 e2e 里试时看：崩过一次重启之后 `GET /fault` 里这个开关是不是开着、没有 `until`。
  - `recover` 收尾的 `recon-check` 用 `APP_NAMESPACE`（默认 `oppenheimer`），不跟 `CHAOS_NS`：在 e2e 里做带对账的场景（s015）要显式设，或者改脚本。没有改。
- （做了，2026-11-26）集群上是 `847bb0c-p4`，`f01` 到 `f08` 都在、全关，五个服务挂着那个可选的卷。
- 第二档的七个（s009 到 s014 带负载，s015 在 e2e）都要试一次再决定标不标 `blind: yes`（s015 不盲）。
- s015 的子 Agent 提的：注入脚本不支持“打开开关之后重启另一个工作负载”这一步，s015 制造积压要手工重启 cdc。

## 带负载试跑，第一次（2026-11-26 16:49 到 19:20 UTC，长期运行的那一套，稳态 570 请求/秒；主人同意后）

每个场景的数在各自 `truth.md` 的最后一段。一句话的结果：

| 编号 | 结果 | 进池 |
|---|---|---|
| s007 | 只有尾延迟变了（p99 到约 400 毫秒，p50 不变），9 分钟 1 次失败，没有告警、没有摘除。比预计的温和，“预占大面积超时”没有发生 | 标了 `blind: yes` |
| s008 | 约 4 分半开始报错，20 分钟起商家订单列表基本全挂，积压 7.5 万；`IngressGateway5xxHigh`（critical）响了，指标没有断。恢复后每小时对账把索引落后报成差异，`ReconFoundDifferences`（critical）又响了一次 | 标了 `blind: yes` |
| s013 | 3 分钟占满 leader 副本的从库池（80），用户无感，没有告警。和答案一致 | 标了 `blind: yes`，带一句 `report`（见下） |
| s012 | **29 分钟没有触发**。触发概率估错了，要改故障代码 | 不进 |

- 池里能盲测的以 `scenario.yaml` 为准：`grep -l 'blind: yes' chaos/scenarios/*/scenario.yaml`。
- s013 标不标：它完全安静（没有告警、用户无感），盲测时主人没有任何入口知道出了事。主会话的想法是标，但盲测时这一类要配一句“有人报了某个现象”的提示，或者只放进“第二档”的池。等主人定。
  - **2026-11-27 主人定了：进池，抽到时给一句提示。** 加了 `scenario.yaml` 的可选字段 `report`（`chaos/README.md` 的字段表）：注入之后脚本把这一句打印在终端上，盲测也打印。s013 的那一句只说“巡检时看到 order 一个副本的降级状态有一项不正常、另一个正常、没有用户报障”，不说哪一项、不说原因。副作用：现在只有 s013 有 `report`，所以盲测时终端上出现 `report:` 这一行本身就说明抽到的是安静的那一类；之后别的安静场景也照这样写。
- **s008 试出一个真的设计缺口**：orderindex 索引库连不上时返回 `INTERNAL`，没有熔断和降级（计划文件“4b 实验清单”里登记了）。
- **脚本和流程上的发现**：
  - `recover` 等稳态时会被“故障结束之后才响的告警”卡满 900 秒（s008 的 `ReconFoundDifferences`）。按 runbook 手工复跑 `recon-hourly` 成功后 critical 消了；失败的 Job 对象不删，`KubeJobFailed` 一直响，之后的注入和恢复用了 `CHAOS_STEADY_SKIP=alerts`，每次注入前手工看过只有它在响。主会话想删这个 Job，被权限检查拦了，没有删（`recon-hourly-29928607`，日志存在 `chaos/runs/r20261126-173553-0ef2/recon-hourly-29928607.log`）。
  - `loadtest.sh wait-quiet` 在探针开着时等不到安静（`order.order-close-delay` 常年约 900，主会话的推断：探针每秒一单、每单一条 15 分钟后到期的关单消息）。
  - `k3s-server` 的系统盘在稳态压测下约每小时少 1 GB（19.4 → 15.4 GB），`ServerDiskLow` 响了。第二次试跑因此没有接着做。
- 没有做的：第二次（mutex、block 采样的阶梯对照，s010、s011、s009）；s013 的 PROCESSLIST 和剖析没有核对。
