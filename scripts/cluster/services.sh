#!/usr/bin/env bash
# 在 MacBook 上给集群部署六个业务服务（设计文档 2c 第 11 节）。
#
#   services.sh deploy [服务...]   复制 Secret、渲染并校验边车配置、helm upgrade --install --wait，最后打印各 Pod 的状态
#   services.sh status             Pod、Service、HTTPRoute 的状态
#   services.sh render [服务]      只把渲染后的清单打印出来（helm template），不碰集群上的资源
#   services.sh envoy-validate     只渲染并校验五个边车的配置（需要 docker）
#   services.sh e2e-prepare        端到端测试那一套的准备：命名空间、Secret（含 root 密码）、OTLP 接收端的 Service
#   services.sh e2e-up             e2e-prepare 之后把六个服务部署到 e2e 的命名空间（values 叠上 deploy/cluster/services/e2e/）
#   services.sh e2e-down           删掉 e2e 的命名空间，再清理它留在中间件里的库、topic、消费者组和 Redis 键
#   services.sh e2e-render [服务]   只渲染 e2e 那一套的清单
#
# 不带服务名就是六个，按依赖顺序：inventory、order、payment、orderindex、cdc、gateway。
# 环境变量：
#   IMAGE_TAG      deploy、e2e-up 必须设置（make deploy、make cluster-e2e 会设好）：镜像的 tag。不猜，也不用 latest。
#   APP_NAMESPACE  部署到哪个命名空间，默认 oppenheimer。
#   E2E_NAMESPACE  e2e 那一套的命名空间，默认 oppenheimer-e2e。
#   E2E_RUN_ID     e2e-up 用它拼每次运行不同的 KAFKA_PREFIX（e2e-<号>.）和 REDIS_PREFIX（e2e-<号>:），
#                  没设就用当前的 Unix 时间（scripts/cluster/e2e.sh 会设好）。
#   E2E_OVERLAY_DIR e2e-up、e2e-render 在 e2e 那两层 values 之上再叠的一层（目录里的 common.yaml 和 <服务>.yaml），
#                  scripts/cluster/chaos.sh 设成 deploy/cluster/services/chaos。没设就没有这一层。
#                  可以用冒号隔开写几个目录，按顺序叠（scripts/cluster/bench.sh 用）。
# 端到端测试的完整流程（建库、部署、跑 Job、收日志）在 scripts/cluster/e2e.sh（make cluster-e2e）。
#
# 用 $KUBECONFIG_OUT 指向的 kubeconfig，不看当前的 context（这台机器上还有 OrbStack 的集群）。
# 在 macOS 自带的 bash 3.2 上运行：不用关联数组、mapfile 这些 bash 4 的特性。
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

NS="${APP_NAMESPACE:-oppenheimer}"
CHART="$ROOT/deploy/charts/service"
VALUES="$ROOT/deploy/cluster/services"
# e2e 那一套（设计文档 2c 的 10.1）：同一个 chart、同样的镜像，values 在 $VALUES/<服务>.yaml 之上再叠
# $E2E_VALUES/common.yaml 和 $E2E_VALUES/<服务>.yaml。E2E_MODE=1 时 helm_args 带上这两层和每次运行的前缀。
E2E_NS="${E2E_NAMESPACE:-oppenheimer-e2e}"
E2E_VALUES="$VALUES/e2e"
E2E_MANIFESTS="$ROOT/deploy/cluster/e2e"
E2E_MODE=0
E2E_ROOT_SECRET="oppenheimer-e2e-root"
ENVOY_DIR="$ROOT/deploy/envoy"
# 依赖顺序：被依赖的先部署。order 启动时要预热对 inventory 的调用，payment 调 order，gateway 调所有。
SERVICES=(inventory order payment orderindex cdc gateway)
# Envoy 的版本和 Makefile 的 envoy-validate、chart 里的边车镜像一致。
ENVOY_IMAGE="envoyproxy/envoy:v1.40.2"

# 应用用的 Secret（本命名空间里），键名带上实例。chart 的 secretName 默认值是同一个名字。
SECRET_NAME="oppenheimer-mysql"
MYSQL_NS="mysql"
# 要复制的 Secret：<mysql 命名空间里的 Secret>:<键名里的实例>
MYSQL_SECRETS=(mysql-order-0-auth:order-0 mysql-order-1-auth:order-1 mysql-main-auth:main)
MYSQL_SECRET_KEYS=(app-password cdc-password)
# 测试 Job 自己要的 root 连接（停从库的复制线程、读 GTID、直接读各库、跑 archiver）。只复制到 e2e 的命名空间，
# 长期运行的那一套不需要也不该有 root 的密码。
E2E_ROOT_SECRET_KEYS=(root-password)

TMP=""
cleanup() {
  if [[ -n "$TMP" ]]; then command rm -rf "$TMP"; fi
}
trap cleanup EXIT

usage() {
  sed -n '2,26p' "${BASH_SOURCE[0]}" >&2
  exit 2
}

check_services() {
  local s a ok
  for s in "$@"; do
    ok=0
    for a in "${SERVICES[@]}"; do
      if [[ "$s" == "$a" ]]; then ok=1; fi
    done
    if [[ $ok -eq 0 ]]; then
      echo "unknown service: $s (valid: ${SERVICES[*]})" >&2
      exit 2
    fi
  done
}

# 按依赖顺序排列用户给的服务名（不带就是全部）。
ordered() { # [服务...]
  local s a
  if [[ $# -eq 0 ]]; then
    printf '%s\n' "${SERVICES[@]}"
    return
  fi
  check_services "$@"
  for a in "${SERVICES[@]}"; do
    for s in "$@"; do
      if [[ "$s" == "$a" ]]; then echo "$a"; fi
    done
  done
}

# 已经存在就不动它。不用 create --dry-run | apply：e2e 的命名空间由 e2e-lib.sh 的 e2e_claim_ns 用 create 建
#（带占用的标签），对它 apply 会报缺 last-applied 注解的警告。
ensure_namespace() {
  kubectl get namespace "$1" >/dev/null 2>&1 || kubectl create namespace "$1" >/dev/null
}

require_tag() {
  if [[ -z "${IMAGE_TAG:-}" ]]; then
    echo "IMAGE_TAG is not set. Set it to the tag of the images in the registry (scripts/cluster/images.sh tag prints the one for the current tree; make deploy does both steps)." >&2
    exit 2
  fi
}

# 边车配置：deploy/envoy/<服务>.yaml 是唯一的来源，这里替换两处地址再交给 chart（设计文档 6.3）：
#   host.docker.internal           -> 127.0.0.1（同一个 Pod 里）
#   address: envoy-<服务>,          -> <服务>-headless.<命名空间>.svc.cluster.local（作为上游地址出现的地方）
render_envoy() { # 服务 输出文件
  sed -E \
    -e 's/host\.docker\.internal/127.0.0.1/g' \
    -e "s/address: envoy-([a-z]+),/address: \\1-headless.${NS}.svc.cluster.local,/g" \
    "$ENVOY_DIR/$1.yaml" >"$2"
}

# 和 make envoy-validate 同样的办法：Envoy 自己的校验模式，输出里有以 "' OK" 结尾的一行才算通过。
# 不用 grep -q：pipefail 下提前退出的 grep 会让上游 SIGPIPE，误判失败。
validate_envoy() { # 文件
  local out code=0
  out="$(docker run --rm -v "$1:/etc/envoy/envoy.yaml:ro" "$ENVOY_IMAGE" --mode validate -c /etc/envoy/envoy.yaml 2>&1)" || code=$?
  # "' OK" 那一行后面还有别的日志，所以要么在行尾、要么后面跟换行。
  if [[ $code -eq 0 && ("$out" == *"' OK" || "$out" == *"' OK"$'\n'*) ]]; then
    return 0
  fi
  printf 'validation failed (docker exit %s), full output:\n%s\n' "$code" "$out" >&2
  return 1
}

ensure_tmp() {
  if [[ -z "$TMP" ]]; then TMP="$(mktemp -d)"; fi
}

# 渲染某个服务的边车配置到临时目录；没有边车（deploy/envoy 下没有它的文件，比如 cdc）就什么都不做。
# 成功后 $TMP/<服务>-envoy.yaml 存在。
prepare_sidecar() { # 服务
  [[ -f "$ENVOY_DIR/$1.yaml" ]] || return 0
  ensure_tmp
  render_envoy "$1" "$TMP/$1-envoy.yaml"
}

# 所有 helm 子命令共用的参数：values、镜像、边车配置。
helm_args() { # 服务 tag
  local s="$1" tag="$2"
  HELM_ARGS=(-n "$NS" -f "$VALUES/$s.yaml"
    --set "image.repository=$REGISTRY_ADDR/oppenheimer/$s" --set "image.tag=$tag")
  if [[ -f "$TMP/$s-envoy.yaml" ]]; then
    HELM_ARGS+=(--set-file "sidecar.config=$TMP/$s-envoy.yaml")
  fi
  if [[ $E2E_MODE -eq 1 ]]; then
    HELM_ARGS+=(-f "$E2E_VALUES/common.yaml")
    if [[ -f "$E2E_VALUES/$s.yaml" ]]; then
      HELM_ARGS+=(-f "$E2E_VALUES/$s.yaml")
    fi
    # 再叠一层（E2E_OVERLAY_DIR，scripts/cluster/chaos.sh 用 deploy/cluster/services/chaos）：同一套 e2e 环境
    # 换一组配置，比如关限流、调短时限。没设就没有这一层。可以用冒号隔开写几个目录（scripts/cluster/bench.sh
    # 在 bench/ 之后再叠 bench-single/ 和临时生成的一层），按顺序叠，后面的覆盖前面的。
    local d dirs
    IFS=: read -r -a dirs <<<"${E2E_OVERLAY_DIR:-}"
    for d in ${dirs[@]+"${dirs[@]}"}; do
      [[ -n "$d" ]] || continue
      if [[ -f "$d/common.yaml" ]]; then
        HELM_ARGS+=(-f "$d/common.yaml")
      fi
      if [[ -f "$d/$s.yaml" ]]; then
        HELM_ARGS+=(-f "$d/$s.yaml")
      fi
    done
    # 每次运行不同的前缀（本机 scripts/e2e.sh 的 KAFKA_PREFIX、REDIS_PREFIX）。六个服务都给：用不到的服务不读它。
    # 值进 ConfigMap，变了校验和就变，重跑 e2e-up 会触发滚动。
    HELM_ARGS+=(--set-string "config.KAFKA_PREFIX=e2e-$E2E_RUN_ID."
      --set-string "config.REDIS_PREFIX=e2e-$E2E_RUN_ID:")
  fi
}

# 切到 e2e 那一套：命名空间换掉（边车配置里下游的地址跟着换），helm_args 叠上 e2e 的 values。
use_e2e() {
  NS="$E2E_NS"
  E2E_MODE=1
  if [[ -z "${E2E_RUN_ID:-}" ]]; then
    E2E_RUN_ID="$(date +%s)"
  fi
  case "$E2E_RUN_ID" in
  *[!a-z0-9-]*)
    echo "E2E_RUN_ID must be lowercase letters, digits and '-', got $E2E_RUN_ID" >&2
    exit 2
    ;;
  esac
  local d dirs
  IFS=: read -r -a dirs <<<"${E2E_OVERLAY_DIR:-}"
  for d in ${dirs[@]+"${dirs[@]}"}; do
    if [[ -n "$d" && ! -d "$d" ]]; then
      echo "E2E_OVERLAY_DIR: $d is not a directory" >&2
      exit 2
    fi
  done
}

# 把 mysql 命名空间里三个实例的 app-password、cdc-password 复制成本命名空间的一个 Secret（Secret 不能跨命名空间引用）。
# 密码不进命令行参数和日志：值用 kubectl 的 go-template 读出（已经是 base64，直接放进 data），
# 清单经标准输入交给 kubectl，printf 是 shell 的内建命令，不起新进程。
# 用 server-side apply：它不写 last-applied-configuration 注解，密码不会再存一份在注解里。
# 每次都以源为准整份覆盖，源里的密码换了，重跑就同步过来。
# 参数：本命名空间里的 Secret 名，以及要复制的键（每个实例都复制这几个，键名前面加上实例）。
copy_secret() { # secret 名 键...
  local name="$1" spec src inst key val manifest
  shift
  manifest="apiVersion: v1
kind: Secret
type: Opaque
metadata:
  name: $name
  namespace: $NS
data:"
  for spec in "${MYSQL_SECRETS[@]}"; do
    src="${spec%%:*}"
    inst="${spec##*:}"
    for key in "$@"; do
      val="$(kubectl -n "$MYSQL_NS" get secret "$src" -o go-template="{{index .data \"$key\"}}" 2>/dev/null)" || val=""
      if [[ -z "$val" || "$val" == "<no value>" ]]; then
        echo "secret $MYSQL_NS/$src has no key $key (is the mysql release deployed? make middleware-up)" >&2
        return 1
      fi
      manifest="$manifest
  $inst-$key: \"$val\""
    done
  done
  printf '%s\n' "$manifest" | kubectl apply --server-side --force-conflicts --field-manager=services-sh -f - >/dev/null
  echo "    secret $NS/$name synced from $MYSQL_NS"
}

cmd_deploy() { # [服务...]
  require_tag
  echo "==> namespace $NS"
  ensure_namespace "$NS"
  echo "==> secret"
  copy_secret "$SECRET_NAME" "${MYSQL_SECRET_KEYS[@]}"
  deploy_releases "$@"
}

# 渲染并校验边车配置，按依赖顺序逐个 helm upgrade --install --wait，最后打印 Pod。deploy 和 e2e-up 共用。
deploy_releases() { # [服务...]
  local s list
  list=()
  while IFS= read -r s; do list+=("$s"); done < <(ordered "$@")

  echo "==> envoy configs"
  for s in "${list[@]}"; do
    prepare_sidecar "$s"
    if [[ -f "$TMP/$s-envoy.yaml" ]]; then
      validate_envoy "$TMP/$s-envoy.yaml" || {
        echo "envoy config of $s did not validate" >&2
        return 1
      }
      echo "    $s: ok"
    fi
  done

  for s in "${list[@]}"; do
    echo "==> $s (image tag $IMAGE_TAG)"
    helm_args "$s" "$IMAGE_TAG"
    helm upgrade --install "$s" "$CHART" "${HELM_ARGS[@]}" --wait --timeout 10m >/dev/null
  done

  echo "==> pods"
  kubectl -n "$NS" get pods -o wide
}

cmd_status() {
  echo "==> pods"
  kubectl -n "$NS" get pods -o wide
  echo "==> services"
  kubectl -n "$NS" get services
  echo "==> httproutes"
  kubectl -n "$NS" get httproutes
}

# 只渲染：不碰集群上的资源（helm template 不连集群）。没设 IMAGE_TAG 时用一个占位的 tag，输出只用来检查。
cmd_render() { # [服务]
  local s list tag="${IMAGE_TAG:-render-only}"
  list=()
  while IFS= read -r s; do list+=("$s"); done < <(ordered "$@")
  for s in "${list[@]}"; do
    prepare_sidecar "$s"
    helm_args "$s" "$tag"
    echo "---"
    echo "# service: $s"
    helm template "$s" "$CHART" "${HELM_ARGS[@]}"
  done
}

cmd_envoy_validate() {
  local s
  for s in "${SERVICES[@]}"; do
    prepare_sidecar "$s"
    if [[ -f "$TMP/$s-envoy.yaml" ]]; then
      validate_envoy "$TMP/$s-envoy.yaml"
      echo "$s: ok"
    fi
  done
}

# ---------- 端到端测试那一套（设计文档 2c 的 10.1） ----------

# 命名空间、两个 Secret、OTLP 接收端的 Service。测试 Job 要在服务之前建（见 deploy/cluster/e2e/otlp-service.yaml），
# 它引用 root 的 Secret，所以这一步单独拿出来，scripts/cluster/e2e.sh 在建 Job 之前调用。可以重复执行。
cmd_e2e_prepare() {
  echo "==> namespace $NS"
  ensure_namespace "$NS"
  echo "==> secrets"
  copy_secret "$SECRET_NAME" "${MYSQL_SECRET_KEYS[@]}"
  copy_secret "$E2E_ROOT_SECRET" "${E2E_ROOT_SECRET_KEYS[@]}"
  echo "==> otlp receiver service"
  kubectl -n "$NS" apply -f "$E2E_MANIFESTS/otlp-service.yaml" >/dev/null
  echo "    service $NS/e2e-otlp"
}

cmd_e2e_up() { # [服务...]
  require_tag
  cmd_e2e_prepare
  echo "==> e2e run id $E2E_RUN_ID (KAFKA_PREFIX e2e-$E2E_RUN_ID., REDIS_PREFIX e2e-$E2E_RUN_ID:)"
  deploy_releases "$@"
}

# 先删命名空间并等它删完（服务都停了，不再写库、不再占着消费者组），再清中间件里的东西。
# 中间件的清理按 e2e- / e2e_ 前缀删，不区分运行号（scripts/cluster/e2e-lib.sh）。
# 订单库的 DROP 会复制到共用的从库；e2e_* 库不在 2b 的 MySQL chart 的 databases 里，备份不会导它们。
cmd_e2e_down() {
  echo "==> namespace $NS"
  if kubectl get namespace "$NS" >/dev/null 2>&1; then
    kubectl delete namespace "$NS" --wait --timeout 5m >/dev/null
    echo "    deleted"
  else
    echo "    not found"
  fi
  # shellcheck source=scripts/cluster/e2e-lib.sh
  source "$HERE/e2e-lib.sh"
  echo "==> mysql"
  e2e_drop_dbs
  echo "==> kafka"
  e2e_clean_kafka
  echo "==> redis"
  e2e_clean_redis
}

cmd="${1:-}"
[[ -n "$cmd" ]] || usage
shift
case "$cmd" in
deploy) cmd_deploy "$@" ;;
status) cmd_status ;;
render) cmd_render "$@" ;;
envoy-validate) cmd_envoy_validate ;;
e2e-prepare)
  use_e2e
  cmd_e2e_prepare
  ;;
e2e-up)
  use_e2e
  cmd_e2e_up "$@"
  ;;
e2e-down)
  use_e2e
  cmd_e2e_down
  ;;
e2e-render)
  use_e2e
  cmd_render "$@"
  ;;
*) usage ;;
esac
