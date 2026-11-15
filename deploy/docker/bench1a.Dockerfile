# 1a（main 上合并 PR #2 的提交 732f9ab）四个服务的镜像，只给集群上的吞吐对比用（scripts/cluster/bench.sh，
# 设计文档 2c 的 10.3、1b 的 7.3.1）。构建上下文是 732f9ab 的 worktree，不是本仓库的工作区：
#
#   git worktree add --detach /tmp/x 732f9ab
#   docker buildx build --platform linux/amd64 --build-arg SERVICE=order \
#     -f deploy/docker/bench1a.Dockerfile /tmp/x
#
# 一般不直接运行，用 scripts/cluster/bench.sh images-1a（它建临时 worktree、构建、推送、删 worktree）。
#
# 为什么不复用 deploy/docker/Dockerfile：那个文件按现在的模块布局逐个 COPY go.mod（cdc、orderindex、recon、test/load
# 在 1a 里都不存在），而 1a 的 go.work 只列了 api、pkg、四个服务和 test/e2e。写法和理由照抄现在的那份，
# 只是模块列表换成 1a 的。构建器和运行镜像与现在的服务镜像完全相同（同一个 Go 版本、同一个 distroless 摘要），
# 这样两边的差别只剩代码本身，编译器和运行时的版本不是变量。
# .dockerignore 用同目录下的 bench1a.Dockerfile.dockerignore（BuildKit 优先读“Dockerfile 名.dockerignore”）：
# 1a 的 worktree 根目录没有 .dockerignore。

# ---------- 构建阶段 ----------
# 版本和服务镜像一致，查证记录见 docs/infra/versions.md。
FROM --platform=$BUILDPLATFORM golang:1.27.4 AS build

ARG TARGETOS
ARG TARGETARCH
ARG SERVICE

WORKDIR /src

# 依赖层：1a 的 go.work 列出的目录都要在，所以 test/e2e 的 go.mod 也复制。
COPY go.work go.work.sum ./
COPY api/go.mod api/go.sum ./api/
COPY pkg/go.mod pkg/go.sum ./pkg/
COPY services/gateway/go.mod services/gateway/go.sum ./services/gateway/
COPY services/inventory/go.mod services/inventory/go.sum ./services/inventory/
COPY services/order/go.mod services/order/go.sum ./services/order/
COPY services/payment/go.mod services/payment/go.sum ./services/payment/
COPY test/e2e/go.mod test/e2e/go.sum ./test/e2e/

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download all

COPY . .

# 和服务镜像一样：静态链接、-trimpath、去掉符号表。1a 的每个服务只有一个 cmd。
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -n "$SERVICE" \
    && GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" -o /out/ ./services/$SERVICE/cmd/... \
    && ln -s "$SERVICE" /out/entrypoint

# ---------- 运行阶段 ----------
# 和服务镜像同一个摘要（distroless static，Debian 13，nonroot 的用户是 65532），见 docs/infra/versions.md。
FROM gcr.io/distroless/static-debian13:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931

COPY --from=build /out/ /app/
USER nonroot
ENTRYPOINT ["/app/entrypoint"]
