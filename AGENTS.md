# AGENTS.md

本文件是所有 AI Agent 在本仓库工作时的规则。开始任何工作前先读完本文件，再读 `docs/PROGRESS.md`。

## 项目是什么

Oppenheimer 是一个用于练习的高并发分布式订单系统，目的是为 Go 和 Agent 开发面试积累工业级场景的实战经验。它不是要上线的产品，**学习价值优先于交付速度**：

- 关键设计（一致性、分片、限流、故障恢复）要让仓库主人理解原理，不能只给能跑的代码。
- 每个重要取舍写进 `docs/adr/`，说明为什么选它、放弃了什么。
- 遇到有面试价值的点，记到对应阶段计划文件的「面试考点」一节。

完整目标见 `docs/GOALS.md`，阶段总览见 `docs/ROADMAP.md`。

## 每次会话的流程

**开始时**

1. 读 `docs/PROGRESS.md`，确认当前阶段、上次做到哪、下一步是什么、有没有阻塞项。
2. 读当前阶段的 `docs/plan/phase-N-*.md`，找到第一个未勾选的任务。
3. 如果任务需要 MacBook 以外的设备（见 `docs/infra/hardware.md`），先提醒主人开机，不要假设设备在线。

**进行中**

- 完成一个任务就在阶段计划文件里勾掉对应的 checkbox，不要攒到最后。
- 计划与实际情况不符时，先改计划文件再动手，并在 `PROGRESS.md` 的「计划变更」里记一笔。
- 做出架构层面的决定时新增一篇 ADR。

**结束时**

更新 `docs/PROGRESS.md`：做完了什么、停在哪、下一步具体做什么、未解决的问题。写到另一个没有任何上下文的 Agent 能直接接手的程度。

## 模型分工

主会话用 Fable。派发子任务时按复杂度选模型：

| 模型 | 用于 |
|---|---|
| Fable | 规划、架构决策、一致性和并发相关的核心代码、故障根因分析、代码评审 |
| Opus | 常规功能实现、较复杂的重构、调试 |
| Sonnet | 单元测试、YAML 和 Helm、脚本、文档 |
| Haiku | 代码和信息检索、git 操作、格式整理、简单查询 |

涉及资金、库存、订单状态的代码一律由 Fable 编写或评审，不下放。

## 给子 Agent 派任务

主会话负责规划、拆分和验收，具体任务按上表的分工派给子 Agent。为避免上下文污染和浪费 token，**只给子 Agent 当前任务需要的上下文**：

- 任务说明自成一体：要做什么、完成标准、能改哪些目录、必须遵守的约束（从本文件里摘出相关的几条，不要让它通读全部文档）。
- 给出它需要的接口和类型定义（proto、表结构、函数签名），或指明具体文件路径，不要让它自己在仓库里摸索。
- 不传与任务无关的历史讨论、其他阶段的计划、其他服务的实现细节。
- worktree 是从 `main` 建的。任务基于其他分支时，在任务说明里写明分支名，让它先把自己的分支快进到那个分支。
- worktree 里没有 `./bin`（不入库）。需要 `make fmt` 或 `make proto` 时让它先运行 `make tools`。
- 明确要求它报告“与设计不一致的地方”和“认为设计存在的问题”。1a 里两个真实的一致性缺陷都是这样被发现的。
- 要求它返回简短结果：改了哪些文件、测试是否通过、遇到的问题和没做完的部分。不要返回大段代码或日志。
- 主会话验收后再合并：读 diff、跑 `make lint test`。涉及资金、库存、订单状态的代码由 Fable 逐行评审。

## 并行与 worktree

互相没有依赖的任务可以派给多个子 Agent，各自在独立的 git worktree 里并行完成。

- **适合并行**：不同服务的独立功能、互不相干的 Helm chart、各自独立的测试或文档、不同的故障实验清单。
- **不要并行**：会改同一批文件的任务；有先后依赖的任务（例如先定 proto 再写实现）；`pkg/` 公共库、`api/proto/`、`go.work`、数据库表结构这类共享部分的改动，先在主线串行完成，再分出去并行。
- 每个 worktree 一个分支，任务说明里写清楚它能改的目录范围。
- 合并回主线前各自通过 `make lint test`，合并后在主线再跑一次。
- `docs/PROGRESS.md` 和阶段计划的 checkbox 只由主会话更新，子 Agent 把结果报回来，避免文档冲突。
- 合并时用 `git cherry-pick` 只取子 Agent 自己的提交。`go.work.sum` 冲突时保留任意一边再运行 `go work sync` 重新生成。合并完删除 worktree 和对应分支。

## 工程规范

**Go**

- Go 1.27，多模块用 go workspace。模块间的依赖要在 `go.mod` 里加指向本地目录的 `replace`（`go mod tidy` 不读 `go.work`）。
- 常用命令都在 `Makefile` 里，`make help` 查看。`make test` 和 `make e2e` 需要先 `make up`。proto 改动后运行 `make proto` 并把 `api/gen` 一起提交。
- 对外 HTTP 接口规范先行（ADR-0005）：先改 `api/openapi/gateway.yaml`，运行 `make openapi`，把生成的 `services/gateway/internal/httpapi/oas/oas.gen.go` 一起提交，再改 handler。
- 对外 HTTP（gin），服务间 gRPC。不引入 go-zero、Kratos 等重框架。
- 尽可能使用新版语言和标准库特性，例如 `sync.WaitGroup.Go`、`encoding/json/v2`、`for range n`、range-over-func 迭代器、`min`/`max`/`clear`、`slices` 和 `maps` 包、`log/slog`、`testing/synctest`、`b.Loop()`、`t.Context()`、`os.Root`。写之前用 `go doc` 确认该特性在当前 Go 版本可用，不凭记忆；有新写法时不用旧写法。
- 错误必须处理或显式向上返回，用 `fmt.Errorf("...: %w", err)` 包装。
- 所有跨进程调用带 `context` 和超时。
- 并发代码必须有 `-race` 测试；一致性逻辑（幂等、扣减、状态机）必须有并发场景的测试。
- 提交前通过 `make lint test`。CI（`.github/workflows/ci.yml`）会对每个 go module 跑 lint、build、vet 和带 `-race` 的测试。

**数据与一致性**

- 金额用整数分，不用浮点。
- 订单状态变更只能走状态机，SQL 更新带状态前置条件（`WHERE status = ?`）。
- 所有写接口幂等，消费者按至少一次投递来设计。
- 不使用跨库分布式事务（XA），用本地消息表加补偿。

**可观测**

- 埋点只依赖 OpenTelemetry SDK（链路、指标），不直接绑定具体后端，见 ADR-0004。
- 日志用 `slog` 输出 JSON，带 trace id。

**版本选择**

- 组件优先选最新的 LTS 版本；没有 LTS 的选最新的正式稳定版。
- 版本号在引入时去官方发布页查证，不凭记忆写；登记到 `docs/infra/versions.md`。
- 镜像和依赖固定到具体版本，不用 `latest`。

**基础设施**

- K8s 资源全部用 Helm 或清单文件声明并入库，不手工 `kubectl edit` 后不留痕迹。
- 密钥不入库。
- 对集群做破坏性操作（删 PVC、重置节点、清数据）前先向主人确认。故障注入实验属于计划内操作，但要先在阶段计划里登记。

**Git**

- 提交信息沿用现有风格：`type: Summary`（feat、fix、docs、refactor、test、chore）。
- 只在主人要求时提交和推送。

## 环境注意事项

- 开发机是 macOS Apple Silicon，集群在 Ubuntu x86_64 上。镜像需要构建 `linux/amd64`（或多架构）。
- 本机容器运行时是 OrbStack。
- 拉取 GitHub 和 Docker 镜像经过 R2S 代理，超时先怀疑网络。
