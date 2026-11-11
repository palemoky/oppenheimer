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

- [x] 目录结构与 go workspace

  ```
  api/proto/                                    gRPC 与公共消息定义
  api/gen/                                      由 proto 生成的 Go 代码（入库）
  pkg/                                          公共库，目前有 app（生命周期）和 logx（日志）
  services/{gateway,order,inventory,payment}/   各自独立 go module，入口在 cmd/<服务名>/
  deploy/{compose,helm}/                        部署清单
  test/{k6,e2e}/                                压测脚本与端到端测试（阶段 1、3 建立）
  agent/                                        诊断 Agent（阶段 3d 建立）
  ```

- [x] `Makefile`：`tools`、`proto`、`proto-lint`、`fmt`、`lint`、`test`（带 `-race`）、`build`、`tidy`、`up`、`down`、`down-volumes`、`ps`、`logs`
- [x] golangci-lint 配置（`.golangci.yml`，含 `modernize` 等检查新写法的 linter）
- [x] protobuf 工具链：buf 加两个 Go 插件，`make tools` 安装到 `./bin`
- [x] `deploy/compose/docker-compose.yml`：MySQL 9.7.4（LTS）、Redis 8.10.3、Kafka 4.4.0（KRaft 单节点）
- [x] `.gitignore`、`.editorconfig`
- [x] GitHub Actions：`.github/workflows/ci.yml`，自动发现所有 go module，跑 lint、build、vet、`-race` 测试；另有 proto 检查任务
- [x] 推送后确认 CI 实际跑通（PR #1，14 项检查全部通过，已合并到 `main`）
- [ ] CI 后续扩展（到对应阶段再加）：依赖 MySQL、Redis、Kafka 的集成测试（阶段 1）、镜像构建与推送（阶段 2）

## 已验证

2026-11-11 在 MacBook 上：

- `make lint`：6 个模块 0 问题。
- `make test`：通过；`pkg/app` 的测试带 `-race` 连跑 20 次稳定。
- `make build`：四个服务编译成功；实际启动 order，`/healthz` 正常，收到 SIGTERM 后优雅退出，退出码 0。
- `make up`：三个容器均达到 healthy。MySQL 确认 GTID 开启、binlog 为 ROW、时区 UTC；Redis 读写正常；Kafka 能建 topic、收发消息。`make down` 再 `make up` 后数据仍在。

## 实施中的决定

- **模块间依赖用 `replace` 指向本地目录**：`go mod tidy` 不读 `go.work`，没有 `replace` 时会去网络上拉取兄弟模块而失败。新增服务或新增对 `api` 模块的依赖时，记得在 `go.mod` 里加对应的 `replace`。
- **proto 工具用 `go install` 固定版本装到 `./bin`**，没有用 `go.mod` 的 `tool` 指令：buf 的依赖树很大，放进 workspace 会影响业务模块的依赖版本选择。
- **`app.ListenHTTP` 在构造时就绑定端口**，而不是在 `Start` 里：端口冲突能在启动其他组件之前暴露，测试也能直接拿到系统分配的端口。
- **各服务目前只有健康检查接口**，用标准库 `net/http`。gin 和 gRPC 在阶段 1a 写业务时引入。
- **服务端口**：gateway 8080、order 8081、inventory 8082、payment 8083，可用环境变量 `HTTP_ADDR` 覆盖。

## 验收标准

- `make up` 能在 OrbStack 上启动依赖，`make lint test` 在空骨架上通过。
- 一个新会话只读 `AGENTS.md` 和 `docs/PROGRESS.md` 就能知道接下来做什么。
