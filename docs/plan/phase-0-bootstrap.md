# 阶段 0：文档与仓库骨架

**目标**：规划落成文档，仓库具备开工条件。
**设备**：MacBook

## 任务

### 文档

- [x] `AGENTS.md`
- [x] `docs/GOALS.md`、`docs/ROADMAP.md`、`docs/PROGRESS.md`
- [x] 各阶段计划 `docs/plan/`
- [x] 初始 ADR、设备文档、复盘模板

### 仓库骨架

- [ ] 目录结构与 go workspace

  ```
  services/{gateway,order,inventory,payment}/   各自独立 go module
  pkg/                                          公共库（日志、错误码、ID 生成、分片路由）
  api/proto/                                    gRPC 定义
  deploy/{compose,helm}/                        部署清单
  test/{k6,e2e}/                                压测脚本与端到端测试
  agent/                                        阶段 5 的诊断 Agent
  ```

- [ ] `Makefile`：`build`、`test`（带 `-race`）、`lint`、`proto`、`up`、`down`
- [ ] golangci-lint 配置
- [ ] protobuf 工具链（buf）
- [ ] `deploy/compose/docker-compose.yml`：MySQL、Redis、Kafka（KRaft 单节点），版本按 `docs/infra/versions.md` 的规则查证后登记
- [x] `.gitignore`
- [ ] `.editorconfig`
- [x] GitHub Actions：`.github/workflows/ci.yml`，自动发现所有 go module，跑 lint、build、vet、`-race` 测试
- [ ] 第一个 go module 入库后，确认 CI 实际跑通（目前没有 Go 代码，CI 的任务会跳过）
- [ ] CI 后续扩展（到对应阶段再加）：依赖 MySQL、Redis、Kafka 的集成测试（阶段 1）、proto 检查（buf）、镜像构建与推送（阶段 2）

## 验收标准

- `make up` 能在 OrbStack 上启动依赖，`make lint test` 在空骨架上通过。
- 一个新会话只读 `AGENTS.md` 和 `docs/PROGRESS.md` 就能知道接下来做什么。
