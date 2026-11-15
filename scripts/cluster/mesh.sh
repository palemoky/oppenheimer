#!/usr/bin/env bash
# 在 MacBook 上给集群装 Istio 的控制面（设计文档 2d）。up 可以重复运行。
#
#   mesh.sh fetch             下载 Istio 发布包到 bin/istio-<版本>/（核对 sha256；已经有了就跳过）
#   mesh.sh up                fetch，再装 istio-base 和 istiod，等 istiod 就绪，最后打印 status
#   mesh.sh status            只读：Pod、Helm release、istioctl 版本
#   mesh.sh check             在临时命名空间 mesh-check 里起一对注入了边车的 Pod，发一个请求，确认它经过了
#                             两边的边车并且是 mTLS；跑完删掉命名空间。装完或重建集群之后跑一次
#   mesh.sh istioctl [参数]   用 bin 里的 istioctl 加本脚本的 kubeconfig 执行，排查时用
#   mesh.sh down              卸载 istiod 和 istio-base；不删 CRD 和命名空间
#
# 用 $KUBECONFIG_OUT 指向的 kubeconfig，不看当前的 context：这台机器上还有 OrbStack 的集群。
# 版本来自 cluster.env。在 macOS 自带的 bash 3.2 上运行：不用关联数组、mapfile 这些 bash 4 的特性。
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

ISTIO_NS="istio-system"
ISTIO_DIR="$ROOT/bin/istio-$ISTIO_VERSION"
ISTIOCTL="$ISTIO_DIR/bin/istioctl"
ISTIOD_VALUES="$ROOT/deploy/cluster/istio/istiod.yaml"

# 官方 Helm 仓库里没有这个版本的 chart，所以用 GitHub 的发布包：里面有 chart 也有和它配套的 istioctl。
cmd_fetch() {
  if [[ -x "$ISTIOCTL" ]]; then
    echo "==> istio $ISTIO_VERSION already in $ISTIO_DIR, skipping"
    return 0
  fi

  local os arch platform
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os/$arch" in
  Darwin/arm64) platform="osx-arm64" ;;
  Darwin/x86_64) platform="osx-amd64" ;;
  Linux/x86_64) platform="linux-amd64" ;;
  Linux/aarch64 | Linux/arm64) platform="linux-arm64" ;;
  *)
    echo "unsupported platform: $os/$arch" >&2
    return 1
    ;;
  esac

  local name="istio-$ISTIO_VERSION-$platform.tar.gz"
  local url="https://github.com/istio/istio/releases/download/$ISTIO_VERSION/$name"
  echo "==> fetch $url"

  # 全部在临时目录里做：校验失败或中途断掉，bin/ 里什么都不会留下；最后一步才移动到位，
  # 所以半截的下载不会被上面的“已经装好”判断当成成功。临时目录建在 bin/ 下，保证 mv 在同一个文件系统上。
  mkdir -p "$ROOT/bin"
  local tmp
  tmp="$(mktemp -d "$ROOT/bin/.istio-fetch.XXXXXX")"
  # shellcheck disable=SC2064 # 要的就是现在展开
  trap "rm -rf '$tmp'" EXIT

  # 下载要经过 R2S 的代理，可能慢：给足超时并重试。
  curl -fsSL --connect-timeout 20 --max-time 600 --retry 3 --retry-delay 3 -o "$tmp/$name" "$url"
  curl -fsSL --connect-timeout 20 --max-time 60 --retry 3 --retry-delay 3 -o "$tmp/$name.sha256" "$url.sha256"

  # .sha256 的内容是“<哈希>  <文件名>”，中间一个或两个空格，只取第一列。
  local want got
  want="$(awk '{print $1; exit}' "$tmp/$name.sha256")"
  got="$(shasum -a 256 "$tmp/$name" | awk '{print $1}')"
  if [[ -z "$want" || "$want" != "$got" ]]; then
    echo "sha256 mismatch for $name: want '$want', got '$got'" >&2
    return 1
  fi

  mkdir "$tmp/x"
  tar -xzf "$tmp/$name" -C "$tmp/x"
  if [[ ! -x "$tmp/x/istio-$ISTIO_VERSION/bin/istioctl" ]]; then
    echo "istioctl not found in $name" >&2
    return 1
  fi
  # 目录可能是上次留下的半截（没有 istioctl）：先清掉再移动。
  rm -rf "$ISTIO_DIR"
  mv "$tmp/x/istio-$ISTIO_VERSION" "$ISTIO_DIR"
  echo "==> istio $ISTIO_VERSION ready in $ISTIO_DIR"
}

cmd_up() {
  if [[ ! -f "$ISTIOD_VALUES" ]]; then
    echo "$ISTIOD_VALUES not found" >&2
    return 1
  fi
  cmd_fetch
  echo "==> istio $ISTIO_VERSION: base and istiod in $ISTIO_NS"
  kubectl create namespace "$ISTIO_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  # CRD 在 base 里；istiod 依赖它们，所以先装 base。
  helm upgrade --install istio-base "$ISTIO_DIR/manifests/charts/base" \
    -n "$ISTIO_NS" --wait
  helm upgrade --install istiod "$ISTIO_DIR/manifests/charts/istio-control/istio-discovery" \
    -n "$ISTIO_NS" -f "$ISTIOD_VALUES" --wait --timeout 5m
  kubectl -n "$ISTIO_NS" rollout status deploy/istiod --timeout=300s
  cmd_status
}

cmd_status() {
  echo "==> pods in $ISTIO_NS"
  kubectl -n "$ISTIO_NS" get pods -o wide
  echo "==> helm releases in $ISTIO_NS"
  helm -n "$ISTIO_NS" list
  if [[ -x "$ISTIOCTL" ]]; then
    echo "==> istioctl version"
    "$ISTIOCTL" version
  else
    echo "==> istioctl not found in $ISTIO_DIR; run scripts/cluster/mesh.sh fetch first"
  fi
}

# 注入能不能用，取决于节点：init 容器要在 Pod 的网络命名空间里写重定向规则（这批节点的 iptables 是
# nf_tables 后端，还有 AppArmor）。判断的依据是服务端回显的请求头里有 X-Forwarded-Client-Cert：
# 这个头是服务端的边车在验过客户端证书之后加的，有它就说明出站被拦截、两个边车之间是 mTLS、入站也被拦截。
CHECK_NS="mesh-check"
cmd_check() {
  echo "==> injection check in namespace $CHECK_NS"
  kubectl create namespace "$CHECK_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl label namespace "$CHECK_NS" istio-injection=enabled --overwrite >/dev/null
  kubectl -n "$CHECK_NS" apply -f "$ROOT/deploy/cluster/istio/check.yaml" >/dev/null
  local code=0 out=""
  kubectl -n "$CHECK_NS" rollout status deploy/server deploy/client --timeout=300s >/dev/null || code=$?
  if [[ $code -eq 0 ]]; then
    out="$(kubectl -n "$CHECK_NS" exec deploy/client -c client -- wget -qO- -T 5 http://server:9898/headers 2>&1)" || code=$?
  fi
  if [[ $code -eq 0 && "$out" == *"X-Forwarded-Client-Cert"*"spiffe://"* ]]; then
    echo "    ok: the request went through both sidecars over mTLS"
    kubectl -n "$CHECK_NS" top pods --containers 2>/dev/null || true
    kubectl delete namespace "$CHECK_NS" --wait=false >/dev/null
    return 0
  fi
  # 失败时保留现场。
  echo "injection check failed (exit $code); namespace $CHECK_NS is left for inspection. Output:" >&2
  printf '%s\n' "$out" >&2
  kubectl -n "$CHECK_NS" get pods -o wide >&2 || true
  return 1
}

cmd_istioctl() {
  if [[ ! -x "$ISTIOCTL" ]]; then
    echo "$ISTIOCTL not found; run scripts/cluster/mesh.sh fetch first" >&2
    return 1
  fi
  "$ISTIOCTL" "$@"
}

cmd_down() {
  echo "==> uninstall istiod and istio-base"
  helm uninstall istiod -n "$ISTIO_NS" --ignore-not-found
  helm uninstall istio-base -n "$ISTIO_NS" --ignore-not-found
  echo "Istio CRDs and namespace $ISTIO_NS are left in place. To remove the CRDs by hand:"
  echo "  kubectl get crd -o name | grep '\\.istio\\.io' | xargs kubectl delete"
}

cmd="${1:-}"
[[ $# -gt 0 ]] && shift
case "$cmd" in
fetch) cmd_fetch ;;
up) cmd_up ;;
status) cmd_status ;;
check) cmd_check ;;
istioctl) cmd_istioctl "$@" ;;
down) cmd_down ;;
*)
  echo "usage: $0 fetch | up | status | check | istioctl [args...] | down" >&2
  exit 2
  ;;
esac
