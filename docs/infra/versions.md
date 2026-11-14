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
| Envoy | v1.40.2 | 稳定版 | 2026-11-14 | 服务之间的熔断和并发隔离（ADR-0020）。镜像 `envoyproxy/envoy:v1.40.2`。没有 LTS，同时维护最近四个小版本 |
| Redis | 8.10.3 | 稳定版 | 2026-11-11 | compose 里有两个实例，同一个镜像：`redis`（闸门和锁，`noeviction`，AOF）和 `redis-cache`（查询缓存和 gateway 的限流状态，`allkeys-lru`，不持久化；限流的桶丢了只是重新装满），升级时一起改 |
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
| twmb/franz-go | v1.22.3 | 稳定版 | 2026-11-11 | Kafka 客户端，见 ADR-0007；管理接口 `pkg/kadm` v1.20.0 |
| redis/go-redis | v9.23.0 | 稳定版 | 2026-11-11 | Redis 客户端，见 ADR-0007 |
| go-mysql-org/go-mysql | v1.16.0 | 稳定版 | 2026-11-13 | CDC 服务读 binlog 用它的 `replication` 包（ADR-0014）。GitHub 的 API 当时访问不了，版本是从仓库的标签列表和 Go 模块代理核对的；已实测能从 MySQL 9.7.4 读到列名、行映像和提交时间 |
| kafbat-ui | v1.5.0 | 稳定版 | 2026-11-14 | Kafka 的 Web 界面，镜像 `ghcr.io/kafbat/kafka-ui:v1.5.0`（tag 带 `v`），GitHub releases 上的最新正式版，发布于 2026-04-20。仅本机开发用，profile `ui`，只读，宿主机端口 8090（`KAFKA_UI_PORT`），见下 |
| grafana/otel-lgtm | 0.41.0 | 稳定版 | 2026-11-14 | 本机可观测后端，一个容器里有 OpenTelemetry Collector、Tempo、Prometheus、Loki、Grafana。Docker Hub 的最新正式 tag（2026-11-13 发布，有 amd64 和 arm64）。版本号是 0.x，没有 LTS。仅本机开发用，profile `obs`，宿主机端口 4317（OTLP gRPC）、4318（OTLP HTTP）、3000（Grafana，`GRAFANA_PORT`），数据不持久化，见 ADR-0023 |
| go.opentelemetry.io/otel | v1.47.0 | 稳定版 | 2026-11-14 | 链路和指标。同版本还有 `sdk`、`sdk/metric`、`trace`、`exporters/otlp/otlptrace/otlptracegrpc`、`exporters/otlp/otlpmetric/otlpmetricgrpc`（OTLP gRPC 导出器），版本一起升。装配在 `pkg/otelx`，见 ADR-0004、ADR-0023 |
| otel contrib：otelgrpc | v0.72.0 | 稳定版 | 2026-11-14 | `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc`，gRPC 的链路传播和指标 |
| otel contrib：runtime | v0.72.0 | 稳定版 | 2026-11-14 | `go.opentelemetry.io/contrib/instrumentation/runtime`，Go 运行时指标（GC、goroutine 等） |
| redis/go-redis/extra/redisotel | v9.23.0 | 稳定版 | 2026-11-14 | `github.com/redis/go-redis/extra/redisotel/v9`，Redis 调用的链路和指标，版本与 go-redis 一致 |
| XSAM/otelsql | v0.44.0 | 稳定版 | 2026-11-14 | `github.com/XSAM/otelsql`，`database/sql` 的链路和指标（MySQL） |
| oapi-codegen/runtime | v1.9.0 | 稳定版 | 2026-11-11 | 生成代码的运行时依赖（参数绑定），仅 gateway 使用 |

## 本机 compose 的 MySQL 实例（2026-11-13）

镜像都是上表的 MySQL 版本。order 的 4 个实例调小了内存（关 `performance_schema`、缓冲池 64 MB 等，见 compose 文件里的注释），空闲时主库约 140 MiB、从库约 180 MiB。

| compose 服务 | 用途 | 宿主机端口 | server-id |
|---|---|---|---|
| `mysql` | inventory、payment、`order_archive_db`（订单冷库，ADR-0016），`make test` 的临时库，chaos 和 bench 的库 | 3306 | 1 |
| `mysql-order-0` | `order_db_0` 主库 | 3311 | 11 |
| `mysql-order-0-replica` | `order_db_0` 从库 | 3312 | 12 |
| `mysql-order-1` | `order_db_1` 主库 | 3321 | 21 |
| `mysql-order-1-replica` | `order_db_1` 从库 | 3322 | 22 |
| `mysql-order-2` | 扩容演练（profile `expand`）：`order_db_2` 主库，由 `mysql-order-0` CLONE 而来 | 3331 | 31 |
| `mysql-order-2-replica` | `order_db_2` 从库 | 3332 | 32 |
| `mysql-order-3` | 扩容演练（profile `expand`）：`order_db_3` 主库，由 `mysql-order-1` CLONE 而来 | 3341 | 41 |
| `mysql-order-3-replica` | `order_db_3` 从库 | 3342 | 42 |

后四个实例属于 compose 的 `expand` profile，平时 `make up` 不启动；`make expand-up` 启动，`make expand-down` 连同数据卷删除，复制和提升由 `scripts/expand.sh` 完成（设计文档 1c 的 5.7）。镜像和参数与其他 order 实例一致，只多了 `restart: unless-stopped`：CLONE 完成后 mysqld 会退出，官方镜像里没有守护进程，靠容器的重启策略拉起。CLONE 插件在官方镜像里可用（`INSTALL PLUGIN clone SONAME 'mysql_clone.so'`，实测 9.7.4）。

复制由 `scripts/mysql-replication.sh` 建立（`make up` 会调用，可重复运行）。从库用 `--relay-log=relay-bin` 固定了中继日志的文件名（默认取主机名，容器重建后会找不到旧文件）；复制元数据损坏时脚本会按从库的 `gtid_executed` 重新配置。

五个实例都开了 `binlog_row_metadata=FULL`，binlog 里带列名，CDC 靠它得到列名。

CDC 伪装成从库读 binlog，要占用 server-id，不能和上表以及彼此重复：

| 流水线 | 源（默认） | server-id |
|---|---|---|
| `order-0` | `mysql-order-0-replica` | 1011 |
| `order-1` | `mysql-order-1-replica` | 1021 |
| `order-2` | `mysql-order-2-replica`（扩容后） | 1031 |
| `order-3` | `mysql-order-3-replica`（扩容后） | 1041 |

CDC 的集成测试每个用例用一个 10 万以上的随机 server-id，避免并行的用例互相踢掉连接。

CDC 的 MySQL 账号 `cdc` 由复制脚本在两个主库上创建（从库只读，账号靠复制到达）。

## 本机 compose 的 kafbat-ui（2026-11-14）

属于 compose 的 `ui` profile，平时 `make up` 和 CI 不启动；`make kafka-ui` 启动并打印地址，`make kafka-ui-down` 删除，`make down` 也会带上它。

| compose 服务 | 用途 | 宿主机端口 | 备注 |
|---|---|---|---|
| `kafka-ui` | 看 topic、消息、消费组 lag | 8090（只绑 127.0.0.1） | 连 `kafka:19092`；`KAFKA_CLUSTERS_0_READONLY=true` 只读；不设登录；堆上限 256 MB，容器上限 512 MB；未配置 protobuf serde |

## 本机 compose 的 Envoy 边车（2026-11-14）

每个服务一个边车（ADR-0020，配置在 `deploy/envoy/<服务>.yaml`），`make up` 启动，镜像 `envoyproxy/envoy:v1.40.2`。服务是宿主机上的进程，调用下游时连自己边车的出站端口；入站端口 15001 只在边车之间用（容器网络内），不发布到宿主机。`scripts/lib/sidecar.sh` 里是脚本用的同一份端口。

| compose 服务 | 管理端口（宿主机，`/stats`、`/ready`） | 出站端口（宿主机） | 备注 |
|---|---|---|---|
| `envoy-gateway` | 19901 | 15081 order、15082 inventory、15083 payment、15084 orderindex | gateway 没有入站边车，客户端直接访问 8080 |
| `envoy-order` | 19902 | 15182 inventory | 入站 15001 转给 order 的 gRPC 9081 |
| `envoy-payment` | 19903 | 15281 order | 入站 15001 转给 payment 的 gRPC 9083 |
| `envoy-inventory` | 19904 | 无 | 入站 15001 转给 inventory 的 gRPC 9082 |
| `envoy-orderindex` | 19905 | 无 | 入站 15001 转给 orderindex 的 gRPC 9084 |

统计项例子：`curl -s 127.0.0.1:19901/stats | grep to_inventory`。并发上限溢出看 `cluster.to_<下游>.upstream_rq_active_overflow`（HTTP/2 的在途请求上限），熔断看 `cluster.to_<下游>.outlier_detection.ejections_enforced_total` 和 `ejections_active`。

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
