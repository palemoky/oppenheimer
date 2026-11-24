#!/usr/bin/env bash
# 构建业务服务的镜像并推送到集群里的仓库（阶段 2c）。
#
#   images.sh build [服务...]   只构建，镜像留在本机
#   images.sh push  [服务...]   只推送（镜像要已经构建过，tag 要和现在算出来的一致）
#   images.sh all   [服务...]   构建并推送
#   images.sh tag               只打印这次会用的 tag
#
# 不带服务名就是全部七个。镜像名 $REGISTRY_ADDR/oppenheimer/<服务>:<tag>。
# 另有几个不在“全部”里的镜像，要显式写出来。agent：诊断 Agent（services/agent，用同一份 deploy/docker/Dockerfile，SERVICE=agent；
# 不属于 Helm chart 的业务服务，只有 monitoring.sh agent 用，images.sh all agent）。e2e：端到端测试的镜像（deploy/docker/e2e.Dockerfile，带 -race 的测试二进制加 archiver），
# 只有 scripts/cluster/e2e.sh 用，要显式写出来（images.sh all e2e）。chaos 同理：删 Pod 对账测试的发压和对账镜像
#（deploy/docker/chaos.Dockerfile，load、recon 加 busybox），只有 scripts/cluster/chaos.sh 用（images.sh all chaos）。
# tag 是短提交号；工作区有未提交的改动时是 <短提交号>-dirty-<UTC 时间>，
# 这样每次脏构建的 tag 都不同，集群不会因为 tag 没变而不拉新镜像。绝不用 latest。
# 环境变量 IMAGE_TAG 可以覆盖 tag。
# 平台固定 linux/amd64（集群是 x86_64），在 arm64 的 MacBook 上交叉编译，见 Dockerfile。
#
# 在 macOS 自带的 bash 3.2 上运行：不用关联数组、mapfile 这些 bash 4 的特性。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
ROOT="$(cd "$HERE/../.." && pwd)"

ALL_SERVICES="gateway order inventory payment orderindex cdc recon"
# 可以点名构建、但不在默认列表里的镜像。
EXTRA_IMAGES="agent e2e chaos"
PLATFORM="linux/amd64"

usage() {
  sed -n '2,17p' "${BASH_SOURCE[0]}" >&2
  exit 2
}

compute_tag() {
  if [[ -n "${IMAGE_TAG:-}" ]]; then
    echo "$IMAGE_TAG"
    return
  fi
  local sha status suffix=""
  # GO_TAGS=faults 构建的是带故障代码的镜像（阶段 4），tag 带后缀，不会和平时的镜像混（同一个提交号的两种构建
  # 不能共用一个 tag：后推的会盖掉先推的，也看不出集群上跑的是哪一种）。后缀故意不写 faults：tag 出现在 Pod 的
  # 事件和 describe 里，第一次走通时诊断 Agent 读到 -faults 就写进了报告（设计 4a 第 6 节“第 6 步”）。p4 是阶段 4。
  [[ " ${GO_TAGS:-} " == *" faults "* ]] && suffix="-p4"
  sha="$(git -C "$ROOT" rev-parse --short HEAD)"
  status="$(git -C "$ROOT" status --porcelain)"
  if [[ -n "$status" ]]; then
    echo "${sha}-dirty-$(date -u +%Y%m%d%H%M%S)${suffix}"
  else
    echo "${sha}${suffix}"
  fi
}

image_name() { # 服务 tag
  echo "$REGISTRY_ADDR/oppenheimer/$1:$2"
}

check_services() {
  local s a ok
  for s in "$@"; do
    ok=0
    for a in $ALL_SERVICES $EXTRA_IMAGES; do
      [[ "$s" == "$a" ]] && ok=1
    done
    if [[ $ok -eq 0 ]]; then
      echo "unknown service: $s (valid: $ALL_SERVICES $EXTRA_IMAGES)" >&2
      exit 2
    fi
  done
}

do_build() { # tag 服务...
  local tag="$1" s dockerfile
  shift
  for s in "$@"; do
    echo "==> build $(image_name "$s" "$tag")"
    dockerfile="$ROOT/deploy/docker/Dockerfile"
    case "$s" in
    e2e | chaos) dockerfile="$ROOT/deploy/docker/$s.Dockerfile" ;;
    esac
    # --load：结果放进本机的镜像库，后面 push 和 inspect 都用得上。
    # e2e.Dockerfile、chaos.Dockerfile 不用 SERVICE 这个参数，传了也没有影响。
    docker buildx build --platform "$PLATFORM" --load \
      -f "$dockerfile" \
      --build-arg "SERVICE=$s" \
      --build-arg "GO_TAGS=${GO_TAGS:-}" \
      -t "$(image_name "$s" "$tag")" \
      "$ROOT"
  done
}

do_push() { # tag 服务...
  local tag="$1" s
  shift
  for s in "$@"; do
    echo "==> push $(image_name "$s" "$tag")"
    docker push "$(image_name "$s" "$tag")"
  done
}

print_images() { # tag 服务...
  local tag="$1" s
  shift
  echo "==> images"
  for s in "$@"; do
    image_name "$s" "$tag"
  done
}

[[ $# -ge 1 ]] || usage
cmd="$1"
shift

if [[ "$cmd" == "tag" ]]; then
  compute_tag
  exit 0
fi

if [[ $# -gt 0 ]]; then
  services=("$@")
else
  # shellcheck disable=SC2206 # 服务名没有空白，按空格拆分是有意的
  services=($ALL_SERVICES)
fi
check_services "${services[@]}"
tag="$(compute_tag)"

case "$cmd" in
  build) do_build "$tag" "${services[@]}" ;;
  push)
    do_push "$tag" "${services[@]}"
    print_images "$tag" "${services[@]}"
    ;;
  all)
    do_build "$tag" "${services[@]}"
    do_push "$tag" "${services[@]}"
    print_images "$tag" "${services[@]}"
    ;;
  *) usage ;;
esac
