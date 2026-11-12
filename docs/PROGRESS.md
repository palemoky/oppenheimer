# 进度与断点

每次会话结束时更新本文件。新会话先读这里。

## 当前状态

- **当前阶段**：1b（高并发改造），步骤一、二、三已完成；步骤四（加固）实施中：共享部分已完成并本地提交，四个服务侧任务已派给子 Agent
- **当前分支**：`phase-1b`（draft PR #5）。远端停在 `9f09ebf`，CI 全绿
- **最后更新**：2026-11-12
- **本地有多个提交未推送**（`0bafdfb` 起，步骤四的设计、共享部分、三个服务的接入、压测工具、笔记）。等主人说推送

## 步骤四的实施状态

已完成（主会话，提交 `932214d`，`pkg` 的 lint 和测试通过）：

- `pkg/lock` 的 `FailOpen`（`failopen.go`）。
- `pkg/cache`（`cache.go`、`codec.go`）。相对设计签名的调整已写回设计文档 7.2.5：`Client` 用 `redis.Cmdable`；新增 `DisableLock`（订单缓存用）；等待方每轮重新抢锁；拿到锁后复查缓存。已知缺口（Redis 拒绝连接时每个请求多等约半秒，需要熔断，放 1d）也记在那里。
- compose 增加 `redis-cache`（端口 6380，`allkeys-lru`，不持久化）；`scripts/run-local.sh` 导出 `CACHE_REDIS_ADDR`；CI 通过 `make up` 自动带上，不用单独改。

已合入主线（子 Agent 实现，主会话评审后 cherry-pick；主线 `make lint test e2e` 全部通过，e2e 8 个场景不改断言）：

- inventory 的库存查询缓存（`STOCK_CACHE_TTL`，默认 1 秒，0 关闭）。
- payment 连 Redis，中继和处理器各一把锁。
- order 三个周期任务的锁，`GetOrder` 的终态缓存（`ORDER_CACHE_TTL` 默认 10 分钟，0 关闭；`ORDER_CACHE_NULL_TTL` 默认 2 秒）。主会话逐行评审过。
- 子 Agent 评审时提出的缺口已记进设计文档 7.1 和 7.2.3 的末尾，都不影响正确性，没有改。

- `test/load` 的 `run` 子命令（账本字段见 `test/load/internal/ledger/ledger.go`；用法 `make load LOAD_ARGS="-addr http://127.0.0.1:8080 -c 100 -duration 30s -skus 1001 ..."`）。**还没有对真实服务跑过**，只有 `httptest` 的测试。
- 1b 面试笔记 `docs/notes/1b-high-concurrency.md`（Sonnet 写，主会话核对了 Redlock 一节）。压测数字待补。

仍在进行：

- `verify`、`wait-quiet` 子命令和 `scripts/chaos.sh`、`make chaos`（Opus，一个 worktree，基于 `f9d1a25`）。它会启动真实服务并至少跑 3 次，被要求**只报告产品缺陷、不修、不放宽检查**。回来后主会话评审 verify 的 18 条检查，再决定缺陷怎么修。

如果会话中断：用 `git worktree list` 和 `git branch` 找到子 Agent 的分支，读各自的 diff，按 AGENTS.md 的流程 cherry-pick 合入，再在主线跑 `make lint test e2e`。没有提交的 worktree 说明该任务没做完，重新派发即可。

## 上次做到哪

1b 分四步（见 [plan/phase-1-order-system.md](plan/phase-1-order-system.md) 和 [design/1b-high-concurrency.md](design/1b-high-concurrency.md)）。

**步骤三（削峰）已完成**，本机 `make lint test e2e` 全部通过。端到端 8 个场景（原有 7 个改为“提交后轮询到落库”，新增限购），四个死信 topic 都是空的，对账没有做过任何修正。

- 设计在设计文档第 6 节，延迟关单的选型在 ADR-0009。
- 主会话写的：库存闸门 `pkg/stockgate`（Lua 脚本）、proto 和 OpenAPI 的改动、gateway 的 202、端到端测试。
- 子 Agent 写、主会话逐行评审合入的：order（受理、落库、孤儿占用回收、延迟关单）和 inventory（预热、归还与结清、对账）。worktree 和分支已清理。
- 评审时按子 Agent 的意见改了设计，都已写回设计文档：
  - 受理方发消息失败后改用 `ReturnIfUnclaimed`（原设计的无条件归还在“发送失败但消息其实到了”时会让闸门多出一份）。
  - 认领时把占用移到单独的列表，回收任务不再被已认领的占用饿死。
  - 落库时支付时限已过的订单直接置为失败。
  - inventory 消费者的重试调到 60 次；对账顺带同步限购数量。
- 步骤三的四个提交（`4e659cc`、`76e79fa`、`5f16280`、`8afb981`）已推送。
- **CI 的 e2e 失败与修复**（run 43112680190）：`TestPayRacingCancelEndsConsistent` 等订单落定超时。
  - 根因是测试，不是产品缺陷：用例顺序创建 40 个订单并逐个等落库，异步下单后这一步在 CI 上要约 2.9 秒，几乎用光 `PAYMENT_TIMEOUT=3s`（时限从受理时刻起算）。开抢时最早的订单已被延迟消息正常关成 `closed`，而等待条件只认 `paid` 和 `cancelled`。
  - 与 `SWEEP_INTERVAL=1m` 无关：关单是延迟消息准时触发的。
  - 修复：造数改为并发提交（`submitOrder` + `waitLanded`）；等待条件和断言接受 `closed`（要求与 `cancelled` 相同：有支付单必须已退款，库存归还）。顺带更正了 `HandleOrderCloseDue` 的过时注释。
  - 本机验证：`make lint test` 通过，`make e2e` 连续 3 次通过，支付时限压到 2s（之前必现失败）5 次通过。

仍然存在的缺口记在设计文档 6.4 和 6.6 的末尾，主要是：对账只比较汇总量，发现不了“某一笔占用该还没还”；认领后卡住的占用只报警不自动处理；闸门只支持单实例 Redis。

## 下一步

1. e2e 修复的两个遗留点（CI run 43115796564 全绿，该用例 `paid=15 cancelled=25 closed=0`）：
   - 遗留：`closed` 结局在正常时限下几乎不出现，CI 覆盖不到“支付对关单”的竞争，要稳定覆盖需要单独写一个用例。
   - 遗留：支付时限从受理时刻起算，落库排队会挤占支付窗口；积压接近时限时订单落库后可能只剩几毫秒可付，库存白占一轮。可以加“剩余时间不足 X 就直接失败”的阈值，或在设计文档里写明取舍。步骤四细化设计时一并决定。
2. **步骤四（加固）的设计依据**。设计已定稿：设计文档第 7 节和 ADR-0010（已采纳），主人 2026-11-12 确认了四点：
   - 订单只缓存终态，非终态一律读库，没有任何写时失效逻辑。因此 `GetOrder` 的 proto 不用改，`pkg/cache` 没有 `Delete`。
   - 缓存用独立的 Redis 实例（compose 加 `redis-cache`，CI 的 e2e 也要加；配置 `CACHE_REDIS_ADDR`，不设时退回 `REDIS_ADDR`）。
   - 锁不可用时：对账停下；中继、sweeper、回收任务、支付处理器放行（`lock.FailOpen`）。
   - payment 连 Redis，配置方式和其他服务相同。
3. 步骤四剩余的工作（顺序）：
   - 验收并合入 `verify` 和 `scripts/chaos.sh`；修杀进程测试发现的缺陷（先记进设计文档 7.3.4 再修）。
   - 用同一个工具量 1a 的基线（`main` 上合并 PR #2 的提交，单独的 worktree）。验收项“吞吐相对 1a 有数量级提升”在这里完成，报告要同时给出 1a 下单吞吐、1b 受理吞吐、1b 落库吞吐。
4. 把压测结果补进 `docs/notes/1b-high-concurrency.md`（笔记里标了“压测结果待补”）。设计文档 5.6 的缺口列表里“中继单实例运行，步骤四处理”那一条还没标注已解决，顺手改掉。

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
| 2026-11-12 | 步骤三设计修正：受理失败用 `ReturnIfUnclaimed`；认领的占用单独成列表；落库时已过期的订单直接失败；创建超时从落库时刻算起 | 都是实现时由子 Agent 的评审和测试发现的，见设计文档 6.3、6.4 |
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
