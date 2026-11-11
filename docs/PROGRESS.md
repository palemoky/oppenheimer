# 进度与断点

每次会话结束时更新本文件。新会话先读这里。

## 当前状态

- **当前阶段**：0（文档与仓库骨架）
- **最后更新**：2026-11-11

## 上次做到哪

- 规划文档、`.gitignore`、CI 已提交在分支 `phase-0-bootstrap`（两次提交）。
- 阶段 0 的仓库骨架已写完并在本机全部验证通过，**尚未提交**：go workspace 与 6 个模块、`pkg/app` 和 `pkg/logx`、四个服务入口、proto 工具链与第一个 proto、Makefile、lint 配置、compose、CI 的 proto 任务。验证结果和实施中的决定记在 [plan/phase-0-bootstrap.md](plan/phase-0-bootstrap.md)。
- `AGENTS.md` 增加了「尽可能使用 Go 新版特性」的规则。

## 下一步

1. 主人确认后提交骨架代码，推送分支，确认 CI 在 GitHub 上跑通（阶段 0 唯一剩下的任务）。
2. 合并到 `main`，把 `ROADMAP.md` 里阶段 0 标为完成。
3. 进入阶段 1a，先细化 [plan/phase-1-order-system.md](plan/phase-1-order-system.md) 里 1a 的任务，从数据模型和订单状态机开始。

## 本机开发环境

- `make tools` 安装 buf 和 proto 插件到 `./bin`（已装）。
- `make up` 启动依赖，`make down` 停止并保留数据。当前依赖处于停止状态。

## 阻塞项与待确认

- 无。阶段 0 和阶段 1 只需要 MacBook，其他设备不用开机。

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

## 会话日志

### 2026-11-11

制定规划并落成文档。本机工具现状：Go 1.27.3、kubectl 1.33、Docker 29.8（OrbStack）已装；helm 未装，阶段 2 前安装。

同日完成阶段 0 的仓库骨架，本机验证通过，待提交和 CI 验证。
