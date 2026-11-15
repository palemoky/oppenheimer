# 业务服务的镜像，七个服务共用这一份。用法（构建上下文必须是仓库根目录）：
#
#   docker buildx build --platform linux/amd64 -f deploy/docker/Dockerfile --build-arg SERVICE=order .
#
# 一般不直接运行，用 scripts/cluster/images.sh（make images）。
#
# 分两个阶段：构建阶段有完整的 Go 工具链，运行阶段只有二进制，没有 shell、包管理器和 libc。
# 攻击面小、镜像小；代价是不能 exec 进去调试（需要时用 kubectl debug 挂临时容器）。

# ---------- 构建阶段 ----------
# --platform=$BUILDPLATFORM：构建阶段始终跑在构建机的原生架构上（MacBook 是 arm64），
# 再用 GOOS/GOARCH 交叉编译成目标架构（集群是 amd64）。Go 的交叉编译不需要模拟器，
# 如果让这一阶段也跑 amd64，就要经 QEMU 模拟，编译会慢一个数量级。
# 版本固定到补丁号，不用 latest；查证记录见 docs/infra/versions.md。
FROM --platform=$BUILDPLATFORM golang:1.27.4 AS build

# TARGETOS、TARGETARCH 由 buildx 按 --platform 自动注入，这里只需声明。
ARG TARGETOS
ARG TARGETARCH
# 要构建哪个服务：services/<SERVICE>/cmd/ 下的所有 main 包都会编出来。
# order 下有 order 和 archiver 两个，共用一个镜像，靠容器的 command 选择。
ARG SERVICE

WORKDIR /src

# 先只复制 go.work 和各模块的 go.mod、go.sum，再下载依赖。
# 这些文件很少变，下面这一层就能被缓存：改业务代码不会触发重新下载。
# 缺哪个文件 go 都会报错（go.work 列出的目录必须存在），所以七个服务的 go.mod 全部复制，
# 即便只构建其中一个；代价是任何一个服务加了依赖，所有服务的这一层都会失效。
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

# 模块缓存放在 BuildKit 的缓存挂载里：它不进镜像层，而且跨构建保留，
# 所以即使上面的层失效了，没变的依赖也不用重新下载。
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download all

# 再复制全部代码。（.dockerignore 已经排掉了 .git、docs、deploy 等。）
COPY . .

# CGO_ENABLED=0：静态链接，运行阶段的 static 镜像里没有 libc，动态链接的二进制起不来。
# -trimpath：去掉二进制里的构建机路径，同样的源码得到同样的二进制，也不泄露本机目录。
# -ldflags "-s -w"：去掉符号表和调试信息，镜像更小；堆栈信息不受影响。
# 编译缓存同样放缓存挂载，增量构建只重编改过的包。
# 目标写成 ./services/$SERVICE/cmd/...：编出该服务 cmd 下的所有二进制，输出到 /out/。
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" -o /out/ ./services/$SERVICE/cmd/... \
    && ln -s "$SERVICE" /out/entrypoint

# ---------- 运行阶段 ----------
# distroless 的 static：只有 CA 证书、时区数据、/etc/passwd 之类，没有 shell。
# 所以这一阶段不能有 RUN，只能 COPY。nonroot 标签的默认用户是 65532，不以 root 运行。
# 用摘要固定：distroless 没有版本号标签，标签会被覆盖；摘要是多架构清单的，
# buildx 会按 --platform 选对应架构。对应 Debian 13，查证记录见 docs/infra/versions.md。
FROM gcr.io/distroless/static-debian13:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931

COPY --from=build /out/ /app/
USER nonroot
# ENTRYPOINT 必须是 exec 形式（没有 shell 来展开变量），而它不能引用 ARG，
# 所以构建阶段建了一个符号链接 /app/entrypoint 指向 /app/$SERVICE。
# order 镜像里同时有 /app/order 和 /app/archiver，archiver 的 Pod 用 command 覆盖入口。
ENTRYPOINT ["/app/entrypoint"]
