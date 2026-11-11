# 进度与断点

每次会话结束时更新本文件。新会话先读这里。

## 当前状态

- **当前阶段**：1a 补充（对外接口契约 OpenAPI），1a 已完成
- **当前分支**：`main`。1a 已通过 PR #2 合并；下一项工作从 `main` 新开分支
- **最后更新**：2026-11-11

## 上次做到哪

1a 全部完成并合并到 `main`（PR #2）。CI 全部通过，包括首次在 GitHub 上运行的 MySQL 集成测试任务和端到端任务。

- 四个服务由子 Agent 并行实现，主会话逐个评审后合入。评审和实现过程中发现并修复了两个设计缺陷（Release 的锁升级死锁、Reserve 被拒后的库存泄漏），详见 [notes/1a-consistency.md](notes/1a-consistency.md) 第 7 节。
- 合并前补了两篇 ADR：[ADR-0005](adr/0005-openapi-spec-first.md)（对外接口 OpenAPI 规范先行，**只有决定和计划，还没有任何代码**）和 [ADR-0006](adr/0006-money-as-integer-minor-units.md)（金额用最小货币单位的整数，补记 1a 的既有决定）。

## 下一步

1. 做「1a 补充：对外接口契约（OpenAPI）」，任务清单在 [plan/phase-1-order-system.md](plan/phase-1-order-system.md)。从 `main` 新开分支（建议 `phase-1a-openapi`）。先由主会话串行完成 `Makefile`（安装 `oapi-codegen` v2.8.2、`make openapi`）、规范文件 `api/openapi/gateway.yaml` 和 ADR-0005 里四个待验证细节，再把 gateway 重构派给子 Agent，主会话评审。
2. 进入 1b。先细化 [plan/phase-1-order-system.md](plan/phase-1-order-system.md) 的 1b 任务并写设计文档，顺序建议：Redis 预扣库存 → outbox 加 Kafka 替换轮询 → 异步下单 → 延迟关单 → 分布式锁 → 杀进程对账测试。

## 本机开发环境

- `make tools` 安装 buf 和 proto 插件到 `./bin`（已装）。
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

## 会话日志

### 2026-11-11

制定规划并落成文档。本机工具现状：Go 1.27.3、kubectl 1.33、Docker 29.8（OrbStack）已装；helm 未装，阶段 2 前安装。

同日完成阶段 0 并合并；开始 1a，完成契约、表结构、公共库和设计文档，四个服务派给子 Agent 并行实现；评审、合入、端到端测试和笔记全部完成，待推送。

同日讨论了金额用整数分的理由和对外接口是否用 OpenAPI。决定采用规范先行（`oapi-codegen`），写了 ADR-0005，在阶段 1 计划里加了「1a 补充」一节。另外补记了 ADR-0006（金额用最小货币单位的整数），这是 1a 已经执行的决定，不涉及代码和计划变更。本次只改文档。

同日完成 1a 收尾：文档改动作为独立提交进入 1a 分支，推送、开 PR #2、CI 全绿后合并到 `main`。
