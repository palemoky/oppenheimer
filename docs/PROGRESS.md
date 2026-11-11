# 进度与断点

每次会话结束时更新本文件。新会话先读这里。

## 当前状态

- **当前阶段**：0（文档与仓库骨架）
- **最后更新**：2026-11-11

## 上次做到哪

- 确定了整体规划、设备分配、技术选型和阶段顺序。
- 写完了全部规划文档：`AGENTS.md`、`docs/GOALS.md`、`docs/ROADMAP.md`、各阶段计划、4 篇初始 ADR、设备文档、复盘模板。
- 按主人的 11 条反馈修订了计划（见下方计划变更记录）。
- 已加 `.gitignore` 和 GitHub Actions CI（`.github/workflows/ci.yml`）。CI 还没有实际跑过：仓库里没有 Go 代码，任务会被跳过。
- 以上内容尚未提交到 git。

## 下一步

阶段 0 剩余任务，见 [plan/phase-0-bootstrap.md](plan/phase-0-bootstrap.md)：

1. 搭仓库骨架：go workspace、目录结构、Makefile、golangci-lint 配置。
2. 写本地 `docker-compose.yml`（MySQL、Redis、Kafka 单实例），确认在 OrbStack 上能起来。
   版本先按 `docs/infra/versions.md` 的规则查证。
3. 第一个 go module 入库并推送后，确认 CI 实际跑通。
4. 完成后进入阶段 1a。

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
