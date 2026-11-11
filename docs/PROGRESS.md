# 进度与断点

每次会话结束时更新本文件。新会话先读这里。

## 当前状态

- **当前阶段**：1a（单库跑通下单主流程）
- **当前分支**：`phase-1a`（本地，未推送）
- **最后更新**：2026-11-11

## 上次做到哪

- 阶段 0 完成，PR #1 已合并到 `main`，CI 14 项检查通过。
- 1a 的基础由主会话完成并提交在 `phase-1a`：三个服务的 proto 契约、表结构、`pkg/{idgen,errx,mysqlx,grpcx,testdb}`、设计文档 [design/1a-order-flow.md](design/1a-order-flow.md)。CI 的测试任务已加上 MySQL 服务（尚未在 GitHub 上跑过）。
- 四个服务（inventory、order、payment、gateway）已派给子 Agent 在各自的 worktree 并行实现。**如果新会话看到这句话，说明派发后会话中断了**：用 `git worktree list` 和 `git branch -a` 找到它们的分支，检查各自是否已提交。

## 下一步

1. 逐个验收子 Agent 的结果：读 diff，对照设计文档逐条核对算法，跑 `make lint test`。inventory 和 order 涉及库存与订单状态，要逐行评审。
2. 合并到 `phase-1a`（`go.work.sum` 冲突直接重新生成），全量 `make lint test`。
3. 加 `make seed`（示例 SKU）和一键启动四个服务的方式。
4. 写端到端测试，见设计文档第 7 节。
5. 整理 1a 面试考点到 `docs/notes/`，推送并开 PR，CI 通过后合并。

任务清单见 [plan/phase-1-order-system.md](plan/phase-1-order-system.md) 的 1a 一节。

## 本机开发环境

- `make tools` 安装 buf 和 proto 插件到 `./bin`（已装）。
- `make up` 启动依赖并建好三个库，`make down` 停止并保留数据。当前依赖处于**运行**状态。
- `make test` 的集成测试连接 `make up` 启动的 MySQL，每个测试自建临时库。

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
| 2026-11-11 | 监控与压测提前到故障注入之前 | 见 ADR-0004 |
| 2026-11-11 | 只保留 `AGENTS.md`，不建 `CLAUDE.md` | 主人决定 |
| 2026-11-11 | 最小诊断 Agent 提前到阶段 3d，阶段 4 同步迭代，阶段 5 改为评测与完善 | 让 Agent 尽早积累真实故障下的运行记录 |
| 2026-11-11 | 埋点改用 OpenTelemetry，SkyWalking 作为链路后端 | 见 ADR-0005 |
| 2026-11-11 | 1b 增加分布式锁；1c 增加 binlog CDC 和冷热分离归档；1d 对账改用 Merkle Tree | 主人要求 |
| 2026-11-11 | 阶段 4 增加 Go 代码缺陷类故障，细化 MySQL 和 Redis 故障 | 覆盖常见生产故障 |
| 2026-11-11 | 版本选择规则：LTS 优先，其次最新稳定版 | 见 `docs/infra/versions.md` |
| 2026-11-11 | 1a 就实现了支付与关单的并发处理（条件更新加退款答复）和 Release 的空回滚 | 这两点是主流程正确性的一部分，无法推迟到 1b；1b 的对应任务改为在引入 Kafka 后复核 |

## 会话日志

### 2026-11-11

制定规划并落成文档。本机工具现状：Go 1.27.3、kubectl 1.33、Docker 29.8（OrbStack）已装；helm 未装，阶段 2 前安装。

同日完成阶段 0 并合并；开始 1a，完成契约、表结构、公共库和设计文档，四个服务派给子 Agent 并行实现。
