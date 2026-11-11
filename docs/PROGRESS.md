# 进度与断点

每次会话结束时更新本文件。新会话先读这里。

## 当前状态

- **当前阶段**：1b（高并发改造），步骤一、二已完成，下一步是步骤三（削峰）
- **当前分支**：`phase-1b`，已推送，draft PR #5（base `main`），CI 全绿。步骤三、四做完再转正式 PR 合并
- **最后更新**：2026-11-11

## 上次做到哪

1b 分四步（见 [plan/phase-1-order-system.md](plan/phase-1-order-system.md) 和 [design/1b-high-concurrency.md](design/1b-high-concurrency.md)）。

**步骤一（公共库）、步骤二（事件化）都已完成**，本机 `make lint test e2e` 全部通过，端到端 7 个场景断言未改。

- 四个子 Agent 的提交已 cherry-pick 到 `phase-1b`：outbox 测试、inventory、payment、order 的事件化改造。三个服务的 diff 由主会话逐行评审，worktree 和分支都已清理。
- 评审结论和仍然存在的缺口写在设计文档第 5.6 节；实现与原设计不一致的地方已经改回设计文档（5.1、5.2）。
- 主会话另外修了三处：outbox 的两处缺陷（`Add` 不接受 nil 的 key 或消息体；`Flush` 在 Sender 只通过 errs 报失败时返回成功）；`kafkax.EnsureTopics` 创建后没等 topic 可见，CI 的 e2e 上 inventory 启动时查不到死信 topic（本机不出现）。
- inventory 的 outbox 表按主人的决定去掉了。
- `TestEnsureTopicsVisibleImmediately` 在本机对着未修复的代码也能通过，起不到回归保护作用；修复是靠 CI 的 e2e 转绿确认的。

## 下一步

1. 步骤三（Redis 预扣、异步下单、延迟关单）动工前先细化设计文档第 6 节。细化时必须回答的问题已经记在第 6 节里：异步下单后 `paid` 可能先于预占到达，库存服务对“预占不存在”的处置要改。延迟关单要对比时间轮、Redis ZSET、延迟 topic 并写 ADR。对外接口改为 202 加轮询，先改 OpenAPI 规范。
2. 之后是步骤四，动工前细化第 7 节。

已推迟的任务（记在对应阶段的计划里，不会丢）：

- 从 OpenAPI 规范生成客户端，替换 `test/e2e` 里手写的请求结构：移到阶段 3b，见 [plan/phase-3-observability.md](plan/phase-3-observability.md) 的第一项。

## 本机开发环境

- `make tools` 安装 buf、proto 插件和 `oapi-codegen` 到 `./bin`（已装）。
- `make up` 启动依赖并建好三个库，`make down` 停止并保留数据。当前依赖处于**运行**状态。
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
