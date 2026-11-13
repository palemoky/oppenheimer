# 进度与断点

每次会话结束时更新本文件。新会话先读这里。

## 当前状态

- **当前阶段**：1c（分库分表与主从），步骤一（分片）、步骤二（主从与读写分离）已完成并推送
- **当前分支**：`phase-1c`（已推送，draft PR 已开）
- **最后更新**：2026-11-13

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

**从 1c 步骤三（非分片键查询：CDC 同步到按商家分片的索引表）开始。** 步骤一、二已完成并推送，都在 draft PR #6（`phase-1c`）里，1c 五步全部完成后再转为正式 PR。

1. 步骤三动工前先做设计，写进设计文档 1c 第 4 节并新增 ADR，主人确认后再实现：
   - CDC 用 Go 实现（go-mysql 库伪装成从库读 binlog），写入 Kafka 再由消费者落索引表；ADR 里与 Canal、Debezium 对比。阶段计划里列了要回答的问题：位点管理与断点续传、全量加增量的初始化、表结构变更、顺序与幂等、同步延迟的监控和应对。
   - **前置条件**：数据模型里要先有商家（SKU 所属商家），要改 inventory 的表和下单时的快照。这是共享部分的改动（表结构、可能还有 proto），先在主线串行完成，再分出去并行。
   - 现在的拓扑对 CDC 有用的事实：order 的两个主库和从库都开了 GTID 和 ROW 格式的 binlog（`binlog-row-image=FULL`）；从库关了 `performance_schema`。CDC 从主库还是从库读，是设计时要定的。
   - 引入 go-mysql 等新依赖时去官方发布页查版本，登记到 `docs/infra/versions.md`。
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
- `make up` 启动依赖：现有的 `mysql`（inventory_db、payment_db 和测试用的临时库）、order 的两主两从（`order_db_0`、`order_db_1`，端口见 `docs/infra/versions.md`）、Redis、Kafka，并建立复制。`make down` 停止并保留数据。当前依赖处于**运行**状态。
- order 的读模式由 `ORDER_READ_MODE` 控制（`primary`、`window`、`gtid`），`make e2e` 默认 `gtid`，`make run` 会连上从库但默认是 `primary`，要读从库时自己设这个变量。
- `make test` 的集成测试连接 `make up` 启动的 MySQL，每个测试自建临时库。
- `make run` 启动四个服务，`make seed` 写入示例 SKU，`make e2e` 在独立的 `e2e_*` 库上跑端到端测试。日志在 `./tmp/`。

## 阻塞项与待确认

- 无。阶段 1 只需要 MacBook，其他设备不用开机。

## 设备状态（上次已知）

| 设备 | 状态 |
|---|---|
| MacBook Pro | 在用 |
| Ubuntu 3900X | 关机，阶段 2 开始需要 |
| Mac mini | 关机，阶段 3 开始需要 |
| DS218+ | 关机，阶段 2 开始需要 |

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
| 2026-11-13 | `super_read_only` 改由复制脚本在复制建立后设置，不写在 compose 启动参数里 | 官方镜像首次初始化会失败 |
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
