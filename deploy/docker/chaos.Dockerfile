# 集群上删 Pod 对账测试（scripts/cluster/chaos.sh）的发压和对账镜像：test/load 的 load 加 services/recon 的 recon。
# 用法（构建上下文必须是仓库根目录）：
#
#   docker buildx build --platform linux/amd64 -f deploy/docker/chaos.Dockerfile .
#
# 一般不直接运行，用 scripts/cluster/images.sh（images.sh all chaos）。
#
# 为什么不复用 recon 的服务镜像、另做一个：发压、静止等待、对账要按顺序在同一个地方跑，中间隔着脚本在 MacBook 上
# 做的事（等 Deployment 恢复、把收集到的库存服务日志传进来），账本文件要从 load run 留到 load verify。
# 所以测试 Pod（一个 Job）常驻，脚本用 kubectl exec 分阶段启动命令（deploy/cluster/chaos/phase.sh），
# 这要一个 shell。服务镜像是 distroless static，没有 shell；这里的运行阶段换成同一系列的 debug 变体，
# 多了 busybox（/busybox 下的 sh、sleep、cat 等），仍然没有包管理器。两个二进制都是静态链接，不需要 libc。
# recon 的服务镜像照旧（它是长期运行那一套的定时任务用的），这里只是同一份源码再编一次。

# ---------- 构建阶段 ----------
# 版本和服务镜像一致，查证记录见 docs/infra/versions.md。
FROM --platform=$BUILDPLATFORM golang:1.27.4 AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

# 依赖层：和服务镜像同样的写法和理由（go.work 列出的目录必须都在，所以全部模块的 go.mod 都复制）。
COPY go.work go.work.sum ./
COPY api/go.mod api/go.sum ./api/
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY services/cdc/go.mod services/cdc/go.sum ./services/cdc/
COPY services/gateway/go.mod services/gateway/go.sum ./services/gateway/
COPY services/inventory/go.mod services/inventory/go.sum ./services/inventory/
COPY services/order/go.mod services/order/go.sum ./services/order/
COPY services/orderindex/go.mod services/orderindex/go.sum ./services/orderindex/
COPY services/payment/go.mod services/payment/go.sum ./services/payment/
COPY services/recon/go.mod services/recon/go.sum ./services/recon/
COPY test/e2e/go.mod test/e2e/go.sum ./test/e2e/
COPY test/load/go.mod test/load/go.sum ./test/load/

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download all

COPY . .

# 和服务镜像一样：静态链接、-trimpath、去掉符号表。
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" -o /out/ ./test/load ./services/recon/cmd/recon

# ---------- 运行阶段 ----------
# distroless static 的 debug-nonroot：static 加 busybox。用多架构清单的摘要固定，对应 Debian 13，
# 查证记录见 docs/infra/versions.md。nonroot 的默认用户是 65532。
FROM gcr.io/distroless/static-debian13:debug-nonroot@sha256:7df9fac2f5d3927260696af33fc2391da26366825838a47337088ed78f854a0c

COPY --from=build /out/ /app/
ENV PATH=/app:/busybox:/usr/local/bin:/usr/bin:/bin
USER nonroot
# 命令由 Job 的 command 给（deploy/cluster/chaos/load-job.yaml）。
ENTRYPOINT ["/busybox/sh"]
