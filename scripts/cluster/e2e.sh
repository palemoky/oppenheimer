#!/usr/bin/env bash
# 在集群上跑端到端测试（设计文档 2c 的 10.1，make cluster-e2e）。本机的对应物是 scripts/e2e.sh（make e2e）。
#
#   scripts/cluster/e2e.sh [go test 的参数...]
#
# 例：scripts/cluster/e2e.sh -run 'TestRateLimit'   或   make cluster-e2e E2E_ARGS="-run TestRateLimit"
# 参数原样交给测试二进制；不带 test. 前缀的 -run、-v 这类会补成 -test.run、-test.v（编译出来的测试二进制只认带前缀的）。
#
# 做的事，按顺序：
#   1. 构建并推送六个服务和测试的镜像（同一个 tag，同 make deploy）。E2E_SKIP_IMAGES=1 跳过，这时必须给 IMAGE_TAG
#   2. 上一次保留下来的 e2e 命名空间还在，就先 services.sh e2e-down
#   3. 拉起两个订单从库的复制，用 root 在各主库上重建 e2e_* 库并给 app、cdc 授权，等订单库复制到从库
#   4. services.sh e2e-prepare（命名空间、Secret、OTLP 接收端的 Service），建测试 Job
#   5. services.sh e2e-up 部署六个服务；Job 在这期间等网关经入口就绪（E2E_WAIT_READY）
#   6. 把 Job 的日志流到终端（同时存一份到 tmp/），按 Job 的结果退出
# 结束时（无论成败）：对两个订单从库 START REPLICA；失败时把各 Pod 的日志、事件收到 tmp/cluster-e2e-logs/；
# 默认 services.sh e2e-down 删掉命名空间并清理中间件。E2E_KEEP=1 保留现场（命名空间和中间件里的东西都不删），
# 看完之后 scripts/cluster/services.sh e2e-down 手工清。
#
# 为什么 Job 先于服务：OTLP 接收端在测试进程里。服务先起而接收端还没监听的话，gRPC 导出器连不上就按指数退避
# 重连（最长 120 秒），部署要几分钟，测试开始时它可能正在等下一次重连，trace_test.go 要的 span 就丢了。
# 本机的 make e2e 里服务和测试只隔几秒，没有这个问题。
#
# 网关看到的客户端地址：Job 访问入口的 LoadBalancer 地址，kube-proxy 在本节点上直接转给入口 Envoy 的 Pod，
# 源地址是 Job 的 Pod 地址；入口 Envoy 把它写进 X-Forwarded-For，网关信任 Pod 网段（TRUSTED_PROXIES），
# 取到的客户端地址就是 Job 的 Pod 地址。所有用例同一个地址，和本机都从 127.0.0.1 来一样，所以按 IP 的限流照样关掉。
#
# 环境变量：IMAGE_TAG、E2E_SKIP_IMAGES、E2E_KEEP、E2E_RUN_ID（默认当前 Unix 时间）、E2E_NAMESPACE（默认 oppenheimer-e2e）。
#
# 主从延迟的用例会停共用的订单从库的复制线程：跑的时候长期运行那一套的展示读会退回主库、cdc 会落后几秒，
# 不要同时做压测和对账（设计文档 10.1）。
# 在 macOS 自带的 bash 3.2 上运行。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
ROOT="$(cd "$HERE/../.." && pwd)"

export KUBECONFIG="$KUBECONFIG_OUT"
if [[ ! -f "$KUBECONFIG" ]]; then
  echo "$KUBECONFIG not found; run scripts/cluster/k3s.sh kubeconfig first" >&2
  exit 1
fi

# shellcheck source=scripts/cluster/e2e-lib.sh
source "$HERE/e2e-lib.sh"

NS="${E2E_NAMESPACE:-oppenheimer-e2e}"
export E2E_NAMESPACE="$NS"
export E2E_RUN_ID="${E2E_RUN_ID:-$(date +%s)}"
JOB_TEMPLATE="$ROOT/deploy/cluster/e2e/test-job.yaml"
JOB="e2e"
LOG_DIR="$ROOT/tmp/cluster-e2e-logs"
# 服务的镜像都要；recon 不参与端到端测试。
IMAGES="gateway order inventory payment orderindex cdc e2e"

# 每个参数编码成 JSON 字符串（YAML 的流式序列认 JSON 的写法），只需要转义反斜杠和双引号。
json_str() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '"%s"' "$s"
}

# 测试二进制的参数：-test.v 和 15 分钟的总时限（go test 默认 10 分钟，集群上慢一些；TestMain 里等网关的时间不算在内），
# 再加调用者给的。-run=X、-v 这类补成 -test.run=X、-test.v。
job_args() {
  local out a
  out="[$(json_str -test.v), $(json_str -test.timeout=15m)"
  for a in "$@"; do
    case "$a" in
    -test.* | --test.*) ;;
    --*) a="--test.${a#--}" ;;
    -*) a="-test.${a#-}" ;;
    esac
    out="$out, $(json_str "$a")"
  done
  echo "$out]"
}

# 填模板。值都是脚本自己算出来的（镜像名、地址、运行号），不含特殊字符；参数那一行整行替换，不经过模式替换。
render_job() { # image 参数...
  local image="$1" args line
  shift
  args="$(job_args "$@")"
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" == *__ARGS__* ]]; then
      printf '%s%s\n' "${line%%__ARGS__*}" "$args"
      continue
    fi
    line="${line//__IMAGE__/$image}"
    line="${line//__LB_GATEWAY_IP__/$LB_GATEWAY_IP}"
    line="${line//__RUN_ID__/$E2E_RUN_ID}"
    printf '%s\n' "$line"
  done <"$JOB_TEMPLATE"
}

# Job 的 Pod 名（backoffLimit 0，只有一个）。
job_pod() {
  kubectl -n "$NS" get pods -l "job-name=$JOB" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

# Job 的结果：succeeded、failed 或空（还在跑）。
job_result() {
  local st
  st="$(kubectl -n "$NS" get job "$JOB" -o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null)" || st=""
  case "$st" in
  1/*) echo succeeded ;;
  */[1-9]*) echo failed ;;
  esac
}

# 失败时把现场收下来：每个 Pod 的日志（所有容器，含边车；重启过的再收上一次的）、Pod 列表、事件。
collect_logs() {
  local pod restarts
  mkdir -p "$LOG_DIR"
  echo "==> collecting logs into $LOG_DIR"
  kubectl -n "$NS" get pods -o wide >"$LOG_DIR/pods.txt" 2>&1 || true
  kubectl -n "$NS" get events --sort-by=.lastTimestamp >"$LOG_DIR/events.txt" 2>&1 || true
  for pod in $(kubectl -n "$NS" get pods -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}' 2>/dev/null || true); do
    kubectl -n "$NS" logs "$pod" --all-containers --prefix --timestamps >"$LOG_DIR/$pod.log" 2>&1 || true
    restarts="$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{range .status.containerStatuses[*]}{.restartCount}{" "}{end}' 2>/dev/null || true)"
    if [[ "$restarts" == *[1-9]* ]]; then
      kubectl -n "$NS" logs "$pod" --all-containers --prefix --timestamps --previous >"$LOG_DIR/$pod.previous.log" 2>&1 || true
    fi
  done
  echo "    $(command ls "$LOG_DIR" | wc -l | tr -d ' ') files"
}

finish() {
  local code=$?
  trap - EXIT INT TERM
  set +e
  echo "==> restart order replicas"
  e2e_start_replicas
  if [[ $code -ne 0 ]] && kubectl get namespace "$NS" >/dev/null 2>&1; then
    collect_logs
  fi
  if [[ "${E2E_KEEP:-}" == 1 ]]; then
    echo "==> E2E_KEEP=1: keeping namespace $NS and the e2e data in the middleware"
    echo "    clean up later with: scripts/cluster/services.sh e2e-down"
  else
    echo "==> e2e-down"
    "$HERE/services.sh" e2e-down
  fi
  if [[ $code -eq 0 ]]; then
    echo "==> cluster e2e PASSED (run $E2E_RUN_ID)"
  else
    echo "==> cluster e2e FAILED (run $E2E_RUN_ID, exit $code); logs in $LOG_DIR" >&2
  fi
  exit "$code"
}

# ---------- 1. 镜像 ----------
if [[ "${E2E_SKIP_IMAGES:-}" == 1 ]]; then
  if [[ -z "${IMAGE_TAG:-}" ]]; then
    echo "E2E_SKIP_IMAGES=1 needs IMAGE_TAG (the tag of images already in the registry)" >&2
    exit 2
  fi
else
  IMAGE_TAG="${IMAGE_TAG:-$("$HERE/images.sh" tag)}"
  export IMAGE_TAG
  # shellcheck disable=SC2086 # 镜像名按空格拆开是有意的
  "$HERE/images.sh" all $IMAGES
fi
export IMAGE_TAG
echo "==> cluster e2e: run $E2E_RUN_ID, image tag $IMAGE_TAG, namespace $NS"

# ---------- 2. 上一次留下的现场 ----------
if kubectl get namespace "$NS" >/dev/null 2>&1; then
  echo "==> namespace $NS exists (kept by an earlier E2E_KEEP=1 run?); tearing it down first"
  "$HERE/services.sh" e2e-down
fi
if [[ -d "$LOG_DIR" ]]; then command rm -rf "$LOG_DIR"; fi

# 从这里开始改了共用的中间件，退出时一定要收尾。
trap finish EXIT
trap 'exit 130' INT TERM

# ---------- 3. 库 ----------
echo "==> mysql: replicas, e2e databases and grants"
e2e_start_replicas
e2e_create_dbs
e2e_wait_replicated

# ---------- 4. 命名空间、Secret、Job ----------
"$HERE/services.sh" e2e-prepare
echo "==> test job"
render_job "$REGISTRY_ADDR/oppenheimer/e2e:$IMAGE_TAG" "$@" | kubectl -n "$NS" apply -f - >/dev/null
echo "    job $NS/$JOB created; it waits for the gateway before running the tests"

# ---------- 5. 服务 ----------
"$HERE/services.sh" e2e-up

# ---------- 6. 日志和结果 ----------
# 先等 Pod 不再是 Pending（拉镜像），kubectl logs -f 才连得上。
echo "==> test output"
for _ in $(seq 1 120); do
  pod="$(job_pod)"
  phase=""
  if [[ -n "$pod" ]]; then
    phase="$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  fi
  if [[ -n "$phase" && "$phase" != Pending ]]; then break; fi
  sleep 2
done
mkdir -p "$LOG_DIR"
# 日志流可能因为 API 的连接断开而提前结束，所以之后还要按 Job 的状态等结果，不以 logs 的退出为准。
kubectl -n "$NS" logs -f "job/$JOB" | tee "$LOG_DIR/e2e-test.log" || true

result=""
# Job 的 activeDeadlineSeconds 是 40 分钟，这里多等一点，Job 一定先有结果。
for _ in $(seq 1 500); do
  result="$(job_result)"
  if [[ -n "$result" ]]; then break; fi
  sleep 5
done
echo "==> job $NS/$JOB: ${result:-no result}"
if [[ "$result" != succeeded ]]; then
  kubectl -n "$NS" describe job "$JOB" | tail -n 20 || true
  exit 1
fi
