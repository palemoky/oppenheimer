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
| s009 | 2 | yes | 开关 `f02` | gateway | 终态订单的详情缓存，没有上限、不淘汰（内存泄漏） | 带负载（2026-11-27），和答案一致：每个副本约 9 到 10 MB/小时。30 分钟的盲测时限里看不出来，所以带 `soak: 60m`（注入后先放一小时）和一句 `report` |
| s016 | 2 | yes | 开关 `f03` 加 chaos-mesh | inventory | s010 的锁，叠上 inventory 到 `redis-cache` 出方向 5 毫秒：查库存 p50 0.6 → 16 毫秒、p99 7 → 97 毫秒，Reserve 不变，没有告警 | e2e 和带负载（2026-11-27），带一句 `report`。数在它的 `truth.md`“实测” |
| s010 | 2 | no | 开关 `f03` | inventory | 查库存的整段（含 Redis、读库）罩在一把全局互斥锁里（锁竞争） | 带负载（2026-11-27），**稳态下基本没有症状**（查库存 p99 7.8 → 8.3 毫秒），不进池；要和缓存 Redis 变慢叠加，或者改流量的配比 |
| s011 | 2 | yes | 开关 `f04` | order | 订单列表逐行重新编译一个大正则（CPU 和 GC） | 带负载（2026-11-27），症状清楚：每个 Pod 多 0.16 核、分配速率 11 倍、列表 p99 三倍，没有告警；带一句 `report` |
| s012 | 2 | yes | 开关 `f05` | orderindex | 商家列表首页缓存，两条路径加锁顺序相反（死锁） | 没有。估计每个副本约 3 分钟撞上一次，波动很大，30 分钟里可能一次都不发生 |
| s013 | 2 | yes | 开关 `f06` | order | 兜底扫描读从库时，空结果的分支没有交还连接（连接泄漏） | 没有。只影响抢到主的那个 Pod 的从库连接池，2.5 到 5 分钟耗尽；用户无感，是个安静的场景 |
| s014 | 2 | no | 开关 `f07` | orderindex | 并行解码的 goroutine 里对 `paid_at` 的类型断言 panic（毒消息，反复重启） | e2e（2026-11-26），带负载。走 `persist: yes`；只经管理端口开的话每个副本崩一次就好了 |
| s015 | 2 | no | 开关 `f08` | orderindex | 一批多于 32 条时并行反序列化并按 key 收拢，两个 goroutine 无同步地写同一个位置，旧映像盖掉新映像（数据竞争，商家索引停在旧版本） | e2e（2026-11-26）。**只在 e2e 里做**。要有积压才触发（试的时候是把 cdc 缩到 0 再恢复）；恢复要手工跑带 `-repair` 的对账 |
| s017 | 2 | yes | istio（EnvoyFilter） | gateway | gateway 到 payment 的出站调用 20% 被 gateway 自己的边车回 `UNAVAILABLE`（请求没有发出去），20 分钟 | e2e 和带负载（2026-11-27） |
| s018 | 1 | yes | chaos-mesh | order | 一个 order 副本到调用方（gateway、payment 的 Pod）的包慢 4 秒：副本 Ready、自己正常，调用方等不到，被边车反复摘除 | e2e 和带负载（2026-11-27）。会响 critical |
| s019 | 1 | yes | chaos-mesh | inventory | inventory 的所有副本到调用方（gateway、order）的包慢 4 秒：查库存全挂，落库熔断器打开、下单被拒绝；验不雪崩 | e2e 和带负载（2026-11-27）。会响 critical |
| s020 | 1 | yes | chaos-mesh | order | order 到订单库 1 主库的分区（挡 Service 的 ClusterIP，`__SVC_IP`）：一半用户受影响，库自己正常 | e2e 和带负载（2026-11-27）。会响 critical |
| s021 | 1 | yes | chaos-mesh | order | order 到订单库 0 主库的分区：节点号的租约在这个库上，约 20 秒后所有用户的下单被拒绝 | e2e 和带负载（2026-11-27）。会响 critical |
| s022 | 1 | no | script | inventory（mysql-main） | 一个没提交的事务锁住 `sku_stock` 正在卖的 20 行：应用库存增量的任务拿不到锁，`stock_pending` 积压；可能连带挡住预占的插入（要验） | 只在本机验了脚本 |
| s023 | 1 | no | script | order（订单库 1 主库） | `orders_1` 的元数据锁被堵：长事务占共享锁，后面排一个排他的请求，这张表上所有语句挂住；连接数可能顶到上限 | 只在本机验了脚本 |
| s024 | 1 | no | script | order（订单库 0 主库） | 三个并行的会话循环跑全表扫描加分组排序加深分页的统计语句，抢 CPU 和缓冲池 | 只在本机验了脚本；症状取决于表有多大 |
| s025 | 1 | no | script | inventory（mysql-main） | 外来会话按相反的顺序加锁（先 `sku_stock` 后 `stock_pending`），和应用增量的事务死锁；带 `report` | 只在本机验了脚本；撞不撞得上、谁被回滚要验 |
| s026 | 2 | no | script | order（订单库 1 从库） | 延迟复制 `SOURCE_DELAY = 180`：两个线程都在跑，从库稳定晚 3 分钟，CDC 和商家索引跟着晚 | 只在本机验了脚本 |
| s027 | 2 | no | script | order（订单库 0 主库） | 单独的库里两轮大事务（200 万行插入、再全删）：提交时别的事务排队，从库和 CDC 出现尖峰 | 只在本机验了脚本（2 万行） |
| s028 | 2 | no | script | inventory（mysql-main） | `SET GLOBAL max_connections = 30`：新连接被拒绝（1040），随连接到期逐渐显现 | 只在本机验了脚本；30 这个数要看了现有连接数再定 |
| s029 | 1 | no | script | gateway（缓存 Redis 一个主节点） | 每秒一次空转的 Lua，占主线程约 250 毫秒：三分之一的缓存键和限流桶时有时无地慢 | 只在本机验了脚本 |
| s030 | 2 | no | script | order（缓存 Redis） | 一个 30 MB 的 hash，每 2 秒被整个读一遍；带 `report` | 只在本机验了脚本（28.6 MB，一次读 48 毫秒） |
| s031 | 2 | no | script | order（Kafka） | 给客户端号 `order` 设拉取配额（每 broker 每秒 2048 字节）：落库、支付结果、关单三个消费者拿不到消息 | 只在本机验了脚本；数值要试 |
| s032 | 1 | no | script | orderindex（Kafka） | 外来的命令行消费者用同一个组名反复进组、退组：每 25 秒重平衡两次；带 `report` | 只在本机验了脚本 |

每个场景自己的“没有核实的”写在它的 `truth.md` 的备注里。第二档新加的六个的 `risk` 是任务说明里统一写的 2：s010、s011 关了开关就恢复、不用重启，按 README 该是 1，带负载试过之后再定。

## B1 试出来的（2026-11-24，`oppenheimer-e2e`，没有负载）

- `pod-kill` 的 `gracePeriod`：45 是正常下线（Terminating 约 10 秒，就绪探针先 503）；0 是 3 秒内 Pod 从 API 里消失，没有排空。
- `container-kill`：退出码 137、`reason: Error`、原地重启、没有 `Killing` 事件，不用写 `duration`。
- StressChaos：没有 CPU limit 的容器 3 个 worker 就是 3 个核；内存超过 limit 是整个容器 OOMKilled，**重启后不再注入**。
- **DNSChaos 对 distroless 的业务容器注入不了**（辅助程序在容器里报找不到文件）；对 `istio-proxy` 也不行（原生边车是 init 容器，选不中）。s008 改成了 NetworkChaos 挡 kube-dns 的地址（`externalTargets` 写 ClusterIP 是管用的：这也说明“服务到数据库的分区”可以用同样的写法，目标写数据库 Service 的地址；headless 的要写 Pod 的地址）。
- 盲测：`blind` 真的跑了一次，终端上没有编号、目标、方式。对账的闸（假的结果）拦得住、确认后放行。
- **Istio 的故障注入 2026-11-27 重试成了，定用 EnvoyFilter**（设计 4b 第 9 节；用 `method: istio`，清单放 `inject/`，`metadata.namespace` 写 `__TARGET_NS__`，带运行标签；注入的延迟不受路由超时的管、中断不触发摘除，写场景时别指望它带出摘除）。下面是第一次的记录。
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
- **2026-11-27**：s012 的故障代码改了（细节在它的 `truth.md`）。同一天换了镜像（`cf5b130-p4`）带负载重试：注入后约 1 分钟和 1 分半两个副本先后卡死，调用栈和答案一致，恢复后对账差异 0。标了 `blind: yes`（第二档的池 2 个：s012、s013）。“orderindex 索引库连不上时不降级”核对后不算缺口（计划文件那一项下面写了理由）：主库连不上按设计就是 `INTERNAL`，s008 的答案不用改。
- 没有做的：第二次（mutex、block 采样的阶梯对照，s010、s011、s009）；s013 的 PROCESSLIST 和剖析没有核对。

## 带负载试跑，第二次（2026-11-27 05:50 到 09:01 UTC，长期运行的那一套；主人同意后）

先做了 mutex、block 采样的阶梯对照（结果在设计 4b 第 8 节“2026-11-27 mutex、block 采样的开销”；采样现在常开，写在五个服务的 values 里），然后稳态 570 请求/秒（07:09:13 起），满 30 分钟后依次做第二档剩下的三个。每个场景的数在各自 `truth.md` 的最后一段。

| 编号 | 结果 | 进池 |
|---|---|---|
| s009 | 40 分钟里两个副本的存活堆各多 6 到 7 MB，heap 剖析的差值直接指到 `settledOrder`；用户无感，没有告警。和答案一致，比估的略慢 | 没有标：盲测的 30 分钟里只涨约 5 MB，等主人定要不要“先开一小时再叫人” |
| s010 | 机制一致，但稳态下锁的占用率只有 0.05 到 0.1，延迟上看不出来；只有 block 剖析做差值才看得到 | 不进。要叠加缓存 Redis 的延迟（没有试，没有登记过），或者调流量配比 |
| s011 | CPU +55%、分配速率 11 倍、列表 p99 6.7 → 19.7 毫秒，CPU 剖析 24% 在 `regexp.compile`；没有告警 | 标了 `blind: yes`，带 `report` |

- 第二档的池现在 3 个：s011、s012、s013。
- 三次注入、恢复前的告警检查都跳过了（`CHAOS_STEADY_SKIP=alerts`）：阶梯压测留下的 `SLOOrderCreateLatencyBurnSlow`（warning，慢档的长窗口）在响，每次手工确认过只有它；到 s011 时它已经消了。
- 剖析是手工抓的（端口转发到 6060 加 curl，文件在 `tmp/p4-trial/`，不入库），没有经过 Agent 的 `pprof_capture`。
- mutex、block 剖析是从进程启动起累计的：进程跑过大压力之后，累计里全是那时的争用（inventory 上 `runtime.unlock` 占 89%），看当前的问题要隔一段时间抓两次做 `-diff_base`。

## 登记了、还没有做的（2026-11-27，主人同意登记；计划文件“4b 实验清单”里两项新的 checkbox）

- **s009 的盲测（写了，2026-11-27：`blind: yes`、`soak: 60m`、`report`；脚本的改动见计划文件那一项）**：给 `scenario.yaml` 加 `soak`（注入后先放多久再让主人开始查）。s009 按实测每个副本约 9 到 10 MB/小时，`soak` 定 60 分钟：到时每个副本多约 10 MB，运行时指标上是一条看得出的斜线，两次 heap 剖析隔 15 分钟就有约 2.5 MB 的差值。`report` 暂定：“容量巡检时看到 gateway 两个副本的内存比昨天同一时段高，没有用户报障”。
- **s010 叠加缓存 Redis 的延迟**（新场景 `s016`，第二档；**场景文件和脚本的支持 2026-11-27 写了，没有上集群**，见计划文件那一项；熔断器的阈值读代码核对了：连续 5 次失败、读缓存时限 200 毫秒，5 毫秒不会打开）：先开 `f03`，再建 NetworkChaos：inventory 的 Pod 到 `redis-cache` 的出站延迟 5 毫秒（`redis-cache` 是 Redis Cluster，目标要写各节点 Pod 的地址，写法照 s008 的 `externalTargets`；延迟加在出方向，一来一回是 5 毫秒不是 10）。按 s010 `truth.md` 的推算，锁里一次变成约 6 到 8 毫秒，每个副本查库存的上限 125 到 170 次/秒，稳态约 100 次/秒，占用率 0.6 到 0.8，p99 到几百毫秒；这次实测的基线是锁里 p50 0.7 毫秒，所以先按 5 毫秒试，不够再加到 8。**不要一上来就 10 毫秒以上**：上限会低于到达速率，队列只涨不降，直接打到 gateway 的 3 秒时限和 128 个在途请求的上限。
  - 对照：只有延迟、不开 `f03` 时，查库存只是每次慢 5 毫秒、并行度不变，应当没有排队。e2e 里两种都看一眼，答案里“容易误判成 Redis 慢了”那一条靠这个对照才站得住。
  - 注意：缓存 Redis 的延迟也会让 `stock-cache` 的熔断器有机会打开（打开后查库存绕过缓存直接查库，锁里的时间反而变成一次查库）。熔断器的阈值没有核对，e2e 里先看。

## B4（2026-11-27 写了场景，没有连集群；上集群的三步登记在计划文件“B4：服务之间”，等主人同意）

五个场景 s017 到 s021，内容在上面的表和各自的 `truth.md`。写之前查到、决定了写法的几件事：

- **稳态压测的配比**（`test/k6/mixed.js`，570 请求/秒）：查库存 50%、查订单 25%、订单列表 10%、商家列表 5%、下单 8%（约每秒 46）、支付 2%（约每秒 11）。客户端遇到 5xx 不重试。
- **Istio 的故障注入只做了一个样例（s017，中断）**：注入的延迟不受路由超时的管、中断不算下游的失败（设计 4b 第 9 节），所以“超时、摘除、不雪崩”用它验不了，
  那三样靠真的让下游慢：s018、s019 用 NetworkChaos 的 delay 加 `target`（只对着调用方 Pod 的地址），探针和到中间件的流量不受影响，副本一直 Ready。
  `target` 加 `direction: to` 的 delay 这种写法没有试过，e2e 里第一件事就是看探针有没有受影响。
- **没有写“order 到从库的分区”**：集群上 `ORDER_READ_MODE` 是 `primary`（values 里没设，代码默认），展示读不走从库；从库不通只会让探测标成不健康、兜底扫描退回主库，没有症状。
- **到订单库的分区写了两个**（s020 库 1、s021 库 0）：清单一样，只差目标，但影响面不一样（节点号的租约在库 0，ADR-0028）。盲测时这两个要靠证据分开，是有意的。
- **可能的设计问题（s020 要验）**：一个分片的主库不通、连接挂住时，order 对这个分片的读返回 `UNAVAILABLE`（列表的 3 秒时限，`errDependencyTimeout`）或者被边车截成 504，
  gateway 的边车把它们算成 order 端点的 5xx；两个副本都连着同一个坏库，可能一起被摘除，另一个分片的用户跟着失败。展示读的并发上限（每库 16）可能先把大部分读挡掉、连不成 5 次，所以要实测。
  成立的话登记到计划文件、回到主人那里定（主人不读这里，登记时只写现象，不写是哪个场景）。
- **试的顺序**：e2e 里五个都只验注入和撤销；带负载时 s017 → s018 → s020 → s021 → s019（影响从小到大；s019、s020、s021 的恢复加 `WAIT_RECON=1`）。
  e2e 里 gateway、payment 被降到 1 个副本没关系，但 s018 要两个 order 副本，起之前看 e2e 的 order 是几个。
- 脚本：`__SVC_IP:<命名空间>:<Service 名>__`（`chaos/README.md`）。只用桩验过，第一次真的渲染是 s020 在 e2e 里。
- 没有做成场景的一件：`OtelCollectorExportQueueFull` 单独响（计划文件 B4 的第三步），目标在 `monitoring`，脚本拒绝这个命名空间，手工做。

## B4 带负载（2026-11-27 13:48 到 16:20 UTC，长期运行的那一套，稳态 570 请求/秒；主人回“三步都同意”之后）

先在 e2e 里五个各试了一次（13:10 到 13:50 UTC，结果在各自 `truth.md` 的“e2e 实测”）。带负载的顺序是 s017 → s018 → s020 → s021 → s019，每个恢复后三个常驻核对和逐行对账差异 0。

稳态的基线（这一轮量的）：570 请求/秒里约 170 是按地址限流的 429，前几轮也是这样。过了限流的：查库存每秒 200、查订单 100、订单列表 40、商家列表 21、下单受理 32（落库 33）、发起支付 8。

| 编号 | 运行 | 结果 | 进池 |
|---|---|---|---|
| s017 | `r20261127-141918-f12c` | 和答案一致：发起支付约两成 503，别的不变；边车的请求指标里完全看不到这些失败；`SLOOrderPayAvailabilityBurnFast` 3 到 5 分钟响 | 标了 |
| s018 | `r20261127-145236-6b85` | 比推的重：经过 order 的接口**稳定约 30% 失败**（等满 3 秒）直到恢复；每 15 秒摘除一次、不加长；三条 critical | 标了 |
| s020 | `r20261127-150843-96a8` | 库 1 的用户被落库熔断器拒绝（预期）；带负载时没有摘除；**库 0 的用户的下单也被背压拒绝，落库 33 → 2.5** | 标了 |
| s021 | `r20261127-152408-449d` | 和答案一致：全部下单被拒绝，只有 `landing-db-0` 打开；`ProbeFailing` 9 分钟内没响 | 标了 |
| s019 | `r20261127-154033-e67d` | 不雪崩成立：查库存全挂（多数等满 3 秒）、下单全被拒绝，其余接口和基线一样，gateway、order 没有被拖垮 | 标了 |

五个恢复后回稳态都是 336 到 338 秒（s017 那次 `recover` 等稳态超时，见下）。

## B4 试出来的三个问题（等主人定；计划文件里只写了现象，主人盲测前不读这里）

1. **摘除圈不住“活着但对调用方慢”的端点（s018、s019 都看到）。** `consecutive5xxErrors: 5`、`baseEjectionTime: 5s`。实测每次摘除都只有 5 秒，端点回来之后要等满 3 秒的路由超时才攒够下一轮连续 5 次，
   期间分到它的请求全部失败；15 秒一个周期，无限重复。`deploy/cluster/mesh/*.yaml` 的注释写“摘除时间随被摘除的次数加长（5、10、15 秒…）”，实测没有。
   猜的原因（没有核实，要读 Envoy 的文档或者源码）：摘除倍数在端点没被摘除的每个检查周期（`interval: 1s`）减一，端点每次回来挨几秒，倍数回到 1。
   可以考虑的方向（都没有试）：调大 `baseEjectionTime`（比如 30 秒）；调大 `interval`；给 gRPC 的端点加主动健康检查；路由超时和摘除之外再靠 HPA 或者人把坏副本换掉。
   还有一个没解释的现象：s018 期间慢副本上的受理多出 `order_queue_unavailable`、`stock_gate_unavailable`（每秒各约 0.5）。
2. **一个订单库不可用时，落库的消费者把另一个订单库的落库也拖住（s020）。** 落库消息的分区不按订单库分；坏库的消息无限重试（ADR-0021 定的：普通错误不设上限、让分区停住等依赖恢复），
   占着并发的名额和本地队列，同一个分区里好库的消息排在后面。结果好库的用户被背压拒绝（`BACKLOGGED`，429），下单总共只剩两成，落库不到一成。
   落库熔断器是按库的，受理时已经把坏库的新订单挡在外面了，但熔断器打开之前进了队列的那些（和半开时放进去的探测订单）一直在重试。
   可以考虑的方向（都没有试，都涉及订单的一致性，要 Fable 写、主人定）：落库的 topic 按订单库分（分区数是订单库数的整数倍、按库路由）；
   或者消费者遇到“这个库的落库熔断器开着”时把消息挪到一个按库的重试队列、让位点往前走；或者接受（一个订单库的主库不通本来就该尽快切换）。
3. **流量很低时，一个分片的主库不通会让调用方把整个 order 摘掉（s020 在 e2e 里，带负载时没有）。** 坏分片的读返回 `UNAVAILABLE`（列表自己的 3 秒时限）或者被边车截成超时，都算 order 端点的 5xx；
   没有别的成功请求混在中间时连续 5 次很容易凑够，两个副本一起被摘，另一个分片的用户拿到 `no healthy upstream`。带负载时成功的请求把“连续”打断了，摘除次数是 0。
   夜里、探针那种一秒一个的流量下会出现。方向：对“分片的主库不通”的读返回带原因码的降级错误（`errx.Degraded`，不算 5xx），这和 2026-11-27 给 orderindex 下的“主库不通就该是 5xx”的结论在有多个分片的 order 上不一样。

顺带：`ProbeFailing` 在 s021 的 9 分钟里没有响（s019 里响了），没有查两次差在哪。

## B5 的第一、二档（2026-11-28 写了场景和脚本，没有连集群；上集群的步骤登记在计划文件“B5：中间件”，等主人同意）

十一个场景 s022 到 s032，都是 `script` 方式（这种方式第一次有场景），都不带 `blind`。内容在上面的表和各自的 `truth.md`；答案都是按代码和配置推的。

- **事实是三个 Haiku 的检索子 Agent 查的**（MySQL、Redis、Kafka 各一个，带行号），主会话只核对了和场景的机制直接有关的几处：inventory 应用增量的加锁顺序（`services/inventory/internal/store/pending.go`、`apply/applier.go`）、各服务的 Kafka 客户端号（都是服务名）、订单表的列。
- **脚本的机制在本机 compose 上验过**（用一个把 `kubectl exec` 转成 `docker exec` 的桩）：十一个的注入、撤销、重复撤销；锁确实持有、元数据锁确实挡住别的会话、复制延迟设得上、配额查得到、外来的消费者进得了组。
  没有验的是集群特有的：`kubectl exec` 起的后台任务在 exec 结束之后是不是还活着（本机的 `docker exec` 是活着的）、Strimzi 的 Kafka 镜像里有没有 `timeout`、Redis Cluster 下 `redis-cli -c` 那几步、按 Service 找主库 Pod。
- **e2e 能隔离多少**：MySQL 的四个按库隔离（s022、s025 用 `e2e_inventory_db`，s023、s024 用 `e2e_order_db_N`），在 e2e 里试不影响长期运行的那一套的数据，但实例是同一个（s024 抢的是同一个实例的 CPU）。
  **隔离不了的五个**：s026（从库的复制延迟是实例一级的）、s027（binlog、提交排队是实例一级的）、s028（`max_connections` 是实例一级的）、s029（缓存 Redis 的主线程是共用的）、s031（配额按客户端号，两套的 order 都叫 `order`）。
  这五个在 e2e 里试等于在长期运行的那一套上试，所以放在“没有稳态压测、只有探针流量”的时候做，时间短。s030、s032 按键名和组名的前缀隔离。
- **试的顺序（建议）**：先一个最简单的验机制（s029：不碰数据、一个循环），再 MySQL 的 s022、s025、s023、s024（e2e 里要有流量：`make cluster-chaos` 自带一轮发压），然后隔离不了的五个不带负载各试一次（只验注入、撤销、到时自己恢复），最后带稳态压测逐个做。
  带负载的顺序从轻到重：s032、s030、s025、s029、s024、s026、s027、s028、s022、s031、s023。s022、s023、s031 的恢复加 `WAIT_RECON=1`。
- **每个场景试的时候先看什么**写在各自 `truth.md` 的备注里。最没有把握的三件：s022 会不会连带挡住预占（是安静的场景还是拖住整个下单）；s023 挂住的语句被客户端放弃后库里的线程留不留（连接数会不会顶到 200）；s025 死锁撞不撞得上、谁被回滚。
- **清单里这一批没有做成场景的，和理由**：
  - 缺索引、统计信息过期：要改业务表的结构或者往里塞数据，按设计 4b 只在 e2e 里做，另行登记。s024 用“函数包裹列、深分页”覆盖了“索引失效”里不改表的那一部分。
  - 主库宕机和切换、磁盘写满、Redis 主节点宕机、Redis 整体不可用、broker 宕机：第三档，逐个登记。
  - 闸门 Redis 内存写满（验 `OrderGateMemoryFull`、`RedisGateMemoryHigh`）：闸门是两套共用的、没有地方先试，容器上限 1 GiB、`maxmemory` 768 MB，写满的过程里 AOF 重写可能把它顶到 OOM（那就成了主节点宕机）。按接近第三档的办，和第三档一起登记。
  - 缓存 Redis 内存打满和淘汰：`allkeys-lru` 下淘汰的后果只是命中率下降，稳态下基本没有症状；s030 在分片接近上限时会顺带出现淘汰。
  - 热 key：稳态压测的 SKU 本来就是 Zipf 分布，要“压垮一个分片”得有比发压机给得出的大得多的流量（而且同一个地址的请求先被按地址限流挡掉）。留到 B6 的秒杀叠加。`redis-cli --hotkeys` 要 LFU 的淘汰策略，缓存集群是 LRU：这一点可以在 s030 的复盘里顺带讲。
  - 大量 key 同时过期：Redis 的主动过期每轮有时间上限，几十万个键同时过期只是几个周期的小抖动；应用一侧缓存的 TTL 带两成抖动。不值得单独做。
  - 消费积压：s031 是。原来想过让 handler 变慢（要加故障开关），配额的做法不用改代码，而且“消费者没有任何错误”更难查。
  - 消息乱序：从外面造不出来。重复投递由 s032 顺带覆盖（重平衡时在途的消息被重放）。
- **读代码顺带核对的两件（设计 4b 第 1.3 节留给 B5 的）**：
  - inventory 在 mysql-main 连不上时返回 `INTERNAL`（没有分类的错误走兜底），MySQL 上没有熔断器。这和 2026-11-27 对 orderindex 的结论一样：主库是它唯一的库，连不上就是整个不可用，被边车摘除是对的。“主库宕机”之前不用先修。
  - 连接池的预算（数是检索子 Agent 报的）：mysql-main 合计约 260 到 300，上限 400；订单库每个实例 order 2×40 加 CDC、对账，上限 200。够。`deploy/cluster/monitoring/rules/middleware.yaml` 里 `MySQLConnectionsHigh` 的注释还按 order 4 个副本算，过期了，没有改。
- **检索时看到的几处过期的注释，没有改**：`deploy/cluster/redis/redis-gate.yaml` 写幂等键 TTL 24 小时（代码是 1 小时）；`services/order/internal/service/protect.go` 的注释说 `order.submit` 集群上 8 个分区（`eventx` 是 4，集群上实际几个没有看）；`ConsumerNotConsuming` 的组名里没有 `recon.cdc-orders`、`order.order-close-delay`。
- **脚本恢复不了时手工兜底**（`make chaos-recover` 失败、或者运行目录丢了）：
  - MySQL 的会话类（s022、s023、s024、s025）：在对应实例的主库上看 `processlist` 里 root@127.0.0.1 的会话，`KILL`；Pod 里 `touch /tmp/.c-<运行号>.stop`。最坏等 `duration` 到（都不超过 15 分钟）。
  - s026：从库上 `STOP REPLICA SQL_THREAD; CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0; START REPLICA SQL_THREAD;`
  - s027：主库上 `DROP DATABASE IF EXISTS tmp_report;`
  - s028：`SET GLOBAL max_connections = 400;`（mysql-main；订单库是 200）。连不上的话重启 mysql-main 的 Pod 也会回到配置文件的值，但那是一次主库重启，先问主人。
  - s029、s030：Pod 里 `touch /tmp/.c-<运行号>.stop`；s030 再 `redis-cli -c unlink cache:export:orders:all`。
  - s031：`kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type clients --entity-name order --delete-config consumer_byte_rate`
  - s032：broker 的 Pod 里 `touch /tmp/.c-<运行号>.stop`，最多 15 秒后外来的成员退组。

