# 端到端测试的镜像（设计文档 2c 的 10.1）：test/e2e 用 go test -c -race 编出来的测试二进制，加 archiver
#（归档用例会 exec 它，见 test/e2e/archive_test.go）。用法（构建上下文必须是仓库根目录）：
#
#   docker buildx build --platform linux/amd64 -f deploy/docker/e2e.Dockerfile .
#
# 一般不直接运行，用 scripts/cluster/images.sh（images.sh all e2e）。
#
# 和服务的镜像（deploy/docker/Dockerfile）不同的地方只有一个：-race 要 cgo。竞态检测的运行时（ThreadSanitizer）
# 是一段 C 写的代码，链接时要 C 编译器，产出的二进制动态链接 glibc。所以：
#   - 构建阶段要一个能产出 linux/amd64 代码的 C 编译器。MacBook 是 arm64，用 Debian 的交叉工具链
#     （gcc-x86-64-linux-gnu），构建阶段仍跑在原生架构上；不让这一阶段跑在模拟的 amd64 上，那样编译慢一个数量级。
#   - 运行阶段不能用 static（没有 libc），换成 distroless 的 base：多了 glibc，仍然没有 shell 和包管理器。
# 交叉工具链、构建镜像、运行镜像三者都是 Debian 13（trixie），glibc 是同一个版本：构建时链接的 glibc
# 不能比运行时的新，否则起不来（报 GLIBC_2.xx not found）。

# ---------- 构建阶段 ----------
# 版本和服务镜像一致，查证记录见 docs/infra/versions.md。golang:1.27.4 基于 Debian 13。
FROM --platform=$BUILDPLATFORM golang:1.27.4 AS build

ARG BUILDARCH
ARG TARGETARCH

# 只支持 amd64 的目标（集群是 x86_64）：交叉工具链按目标架构选，别的架构没有配。
# 构建机本身是 amd64 时（比如 CI）golang 镜像自带的 gcc 就是对的，不装交叉工具链。
# apt 的包没有固定版本：它们跟着 golang:1.27.4 这个镜像的 Debian 13 源走，只用来编译，不进运行镜像。
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    set -eu; \
    if [ "$TARGETARCH" != amd64 ]; then echo "only linux/amd64 is supported, got $TARGETARCH" >&2; exit 1; fi; \
    if [ "$BUILDARCH" != amd64 ]; then \
      rm -f /etc/apt/apt.conf.d/docker-clean; \
      apt-get update; \
      apt-get install -y --no-install-recommends gcc-x86-64-linux-gnu libc6-dev-amd64-cross; \
    fi

WORKDIR /src

# 依赖层：和服务镜像同样的写法和理由（只复制 go.work 和各模块的 go.mod、go.sum，下载依赖的层能被缓存）。
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

# 测试二进制：CGO_ENABLED=1 加交叉编译器。-trimpath 同服务镜像；不加 -s -w：竞态报告要符号才能给出文件和行号。
# archiver：和 order 镜像里的是同一个 main 包，静态链接（它不需要 -race，本机的 make e2e 用的也是 make build 编的普通二进制）。
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    if [ "$BUILDARCH" = amd64 ]; then cc=gcc; else cc=x86_64-linux-gnu-gcc; fi; \
    GOOS=linux GOARCH=amd64 CGO_ENABLED=1 CC="$cc" \
      go test -c -race -trimpath -o /out/e2e.test ./test/e2e; \
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
      go build -trimpath -ldflags="-s -w" -o /out/archiver ./services/order/cmd/archiver

# ---------- 运行阶段 ----------
# distroless 的 base：static 加上 glibc、libssl 等，没有 shell。nonroot 的默认用户是 65532。
# 用多架构清单的摘要固定（distroless 没有版本号标签），对应 Debian 13，查证记录见 docs/infra/versions.md。
FROM gcr.io/distroless/base-debian13:nonroot@sha256:a08d9054afbaf1af9b90fd02cdc7b32236319b5376d374505a41548d42b0c5db

COPY --from=build /out/ /app/
USER nonroot
# 参数（-test.v、-test.run 等）由 Job 的 args 给。测试从工作目录读不到任何文件，不用设 WORKDIR。
ENTRYPOINT ["/app/e2e.test"]
