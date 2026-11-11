# 组件版本

## 选型规则

1. 优先选最新的 **LTS** 版本。
2. 组件没有 LTS 概念时，选最新的**正式稳定版**（GA）。
3. 不用 RC、beta、nightly。
4. 版本号在引入组件时去官方发布页查证，不凭记忆填写。
5. 镜像和依赖固定到具体版本，不用 `latest` 标签。
6. 需要偏离规则时（例如某个 Operator 还不支持最新版），在备注里写明原因。

## 版本表

引入一个组件时在这里登记一行，升级时更新。

| 组件 | 版本 | 类型（LTS / 稳定版） | 查证日期 | 备注 |
|---|---|---|---|---|
| Go | 1.27 | 稳定版 | 2026-11-11 | 本机已装 1.27.3；Go 没有 LTS，只支持最近两个大版本 |
| actions/checkout | v7 | 稳定版 | 2026-11-11 | CI |
| actions/setup-go | v7 | 稳定版 | 2026-11-11 | CI |
| actions/upload-artifact | v7 | 稳定版 | 2026-11-11 | CI，端到端测试失败时上传服务日志 |
| golangci-lint-action | v9 | 稳定版 | 2026-11-11 | CI |
| golangci-lint | v2.15 | 稳定版 | 2026-11-11 | CI 与本地保持一致 |
| MySQL | 9.7.4 | LTS | 2026-11-11 | Docker Hub 的 `lts` 标签指向 9.7 系列；`latest` 是创新版 26.10，不用 |
| Redis | 8.10.3 | 稳定版 | 2026-11-11 | |
| Kafka | 4.4.0 | 稳定版 | 2026-11-11 | `apache/kafka` 镜像，KRaft 模式 |
| buf | v1.75.0 | 稳定版 | 2026-11-11 | 版本写在 `Makefile` 和 CI 里，升级时一起改 |
| protoc-gen-go | v1.36.12 | 稳定版 | 2026-11-11 | 同上 |
| protoc-gen-go-grpc | v1.6.2 | 稳定版 | 2026-11-11 | 同上 |
| bufbuild/buf-action | v1 | 稳定版 | 2026-11-11 | CI |
| google.golang.org/grpc | v1.85.0 | 稳定版 | 2026-11-11 | |
| go-sql-driver/mysql | v1.10.1 | 稳定版 | 2026-11-11 | |
| pressly/goose | v3.30.0 | 稳定版 | 2026-11-11 | 数据库迁移，作为库嵌入各服务 |
| gin | v1.12.0 | 稳定版 | 2026-11-11 | 仅 gateway 使用 |
| oapi-codegen | v2.8.2 | 稳定版 | 2026-11-11 | 由 OpenAPI 规范生成 gateway 的类型和服务端接口；版本写在 `Makefile` 里，CI 通过 `make openapi-tools` 安装同一版本 |
| oapi-codegen/runtime | v1.9.0 | 稳定版 | 2026-11-11 | 生成代码的运行时依赖（参数绑定），仅 gateway 使用 |

## 待引入（引入时查证并移到上表）

| 组件 | 阶段 | 是否有 LTS |
|---|---|---|
| Ubuntu（虚拟机镜像） | 2 | 有 |
| k3s / Kubernetes | 2 | 无，选当前受支持的最新小版本 |
| Strimzi、MySQL Operator、Helm | 2 | 无 |
| OpenTelemetry Collector 与 Go SDK | 1d / 3 | 无 |
| Prometheus | 3 | 有 |
| Grafana、Loki、Alertmanager | 3 | 无 |
| SkyWalking OAP、BanyanDB | 3 | 无 |
| k6 | 3 | 无 |
| Chaos Mesh | 4 | 无 |
| Rust | 8 | 无，选最新 stable |

「是否有 LTS」一栏是初步判断，引入时一并核实。
