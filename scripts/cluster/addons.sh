#!/usr/bin/env bash
# 在 MacBook 上给集群装基础组件。每个子命令都可以重复运行。
#
#   addons.sh metallb     MetalLB 和地址池
#   addons.sh nfs         在 NAS 的共享文件夹里建子目录（从 server 节点经 NFS）
#   addons.sh registry    镜像仓库（要先有 metallb 和 nfs）
#   addons.sh gateway     Envoy Gateway 和对外入口（要先有 metallb）
#   addons.sh smoke       入口的通路检查：部署回显服务，从本机访问入口地址，再删掉
#   addons.sh all         按上面的顺序全部执行（不含 smoke）
#
# 用 $KUBECONFIG_OUT 指向的 kubeconfig（scripts/cluster/k3s.sh kubeconfig 生成），不看当前的 context：
# 这台机器上还有 OrbStack 的集群。版本和地址都来自 cluster.env。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
DEPLOY="$HERE/../../deploy/cluster"

export KUBECONFIG="$KUBECONFIG_OUT"
if [[ ! -f "$KUBECONFIG" ]]; then
  echo "$KUBECONFIG not found; run scripts/cluster/k3s.sh kubeconfig first" >&2
  exit 1
fi

# 把模板里的 __名字__ 换成 cluster.env 里的值。这些值都不是密钥，可以用 sed。
render() {
  sed -e "s#__LB_POOL__#$LB_POOL#g" \
    -e "s#__LB_REGISTRY_IP__#$LB_REGISTRY_IP#g" \
    -e "s#__LB_GATEWAY_IP__#$LB_GATEWAY_IP#g" \
    -e "s#__NAS_IP__#$NAS_IP#g" \
    -e "s#__NAS_EXPORT__#$NAS_EXPORT#g" \
    -e "s#__REGISTRY_VERSION__#$REGISTRY_VERSION#g" \
    "$1"
}

server_ip() {
  local spec
  for spec in "${NODES[@]}"; do
    if [[ "${spec%%:*}" == "$K3S_SERVER_NODE" ]]; then
      cut -d: -f2 <<<"$spec"
      return 0
    fi
  done
  return 1
}

cmd_metallb() {
  echo "==> metallb $METALLB_VERSION"
  helm upgrade --install metallb metallb --repo https://metallb.github.io/metallb \
    --version "$METALLB_VERSION" --namespace metallb-system --create-namespace \
    -f "$DEPLOY/metallb/values.yaml" --wait --timeout 5m >/dev/null
  # 地址池是 MetalLB 的自定义资源，要等它的校验 webhook 就绪才能提交，--wait 已经等过控制器了。
  render "$DEPLOY/metallb/pool.yaml" | kubectl apply -f -
  kubectl -n metallb-system get pods -o wide
}

cmd_nfs() {
  local ip
  ip="$(server_ip)"
  echo "==> NFS directories under $NAS_IP:$NAS_EXPORT (via $K3S_SERVER_NODE)"
  # 共享文件夹的 Squash 是“无映射”，节点上的 root 在里面也是 root，所以目录从节点上建，NAS 上不需要 sudo。
  # 新建的目录会继承群晖的 ACL，这时即使 chown 给了某个用户号，它也进不去；chmod 一次之后目录
  # 变成普通的权限位，属主才能读写（2026-11-15 实测）。所以每个目录都要显式 chmod。
  ssh -o BatchMode=yes -o ConnectTimeout=10 "$VM_USER@$ip" "sudo bash -s" <<REMOTE
set -euo pipefail
m="\$(mktemp -d)"
mount -t nfs -o nfsvers=4.1,hard "$NAS_IP:$NAS_EXPORT" "\$m"
trap 'umount "\$m"; rmdir "\$m"' EXIT
mkdir -p "\$m/registry" "\$m/backup/mysql" "\$m/backup/etcd"
chown 1000:1000 "\$m/registry"   # 仓库的 Pod 以用户号 1000 运行
chmod 750 "\$m/registry"
chown 0:0 "\$m/backup" "\$m/backup/mysql" "\$m/backup/etcd"   # 2b 用到时再定属主
chmod 750 "\$m/backup" "\$m/backup/mysql" "\$m/backup/etcd"
ls -lan "\$m" "\$m/backup"
REMOTE
}

cmd_registry() {
  echo "==> registry $REGISTRY_VERSION at $REGISTRY_ADDR"
  render "$DEPLOY/registry/registry.yaml" | kubectl apply -f -
  kubectl -n registry rollout status deploy/registry --timeout=5m
  kubectl -n registry get svc,pods -o wide
}

cmd_gateway() {
  echo "==> envoy gateway $ENVOY_GATEWAY_VERSION"
  # Gateway API 的 CRD 已经由 k3s 自己装了（kube-system 里的 Helm release gateway-api-crd，v1.6.3，
  # 标准通道）。Envoy Gateway 的主 chart 自带一份实验通道的，装上去会被 Gateway API 的准入策略
  # safe-upgrades 拒绝（不允许用实验通道的 CRD 覆盖标准通道的；2026-11-15 实测）。所以分两步：
  # 用它单独的 CRD chart 只装 Envoy Gateway 自己的 CRD，主 chart 加 --skip-crds。
  # Gateway API 的版本因此跟着 k3s 走，升级 k3s 时要对照 Envoy Gateway 的兼容性表。
  # CRD chart 只渲染不安装：这些 CRD 加起来超过 1 MiB，Helm 把整个 release 存进一个 Secret 时会超限。
  # --server-side：同样因为太大，客户端 apply 要写的 last-applied 注解放不下。
  helm template eg-crds oci://docker.io/envoyproxy/gateway-crds-helm \
    --version "$ENVOY_GATEWAY_VERSION" \
    --set crds.gatewayAPI.enabled=false --set crds.envoyGateway.enabled=true |
    kubectl apply --server-side -f - >/dev/null
  helm upgrade --install eg oci://docker.io/envoyproxy/gateway-helm \
    --version "$ENVOY_GATEWAY_VERSION" --namespace envoy-gateway-system --create-namespace --skip-crds \
    --wait --timeout 5m >/dev/null
  render "$DEPLOY/envoy-gateway/gateway.yaml" | kubectl apply -f -
  kubectl -n envoy-gateway-system wait --for=condition=Programmed gateway/public --timeout=5m
  kubectl -n envoy-gateway-system get gateway,svc,pods -o wide
}

cmd_smoke() {
  echo "==> smoke: echo service behind http://$LB_GATEWAY_IP/"
  kubectl apply -f "$DEPLOY/smoke/echo.yaml" >/dev/null
  trap 'kubectl delete -f "$DEPLOY/smoke/echo.yaml" --wait=false >/dev/null' EXIT
  kubectl -n smoke rollout status deploy/echo --timeout=3m >/dev/null
  local i out=""
  for ((i = 0; i < 30; i++)); do
    if out="$(curl -fsS -m 5 -H 'Host: smoke.oppenheimer.test' "http://$LB_GATEWAY_IP/headers" 2>/dev/null)"; then
      break
    fi
    sleep 2
  done
  if [[ -z "$out" ]]; then
    echo "no answer from http://$LB_GATEWAY_IP/headers" >&2
    return 1
  fi
  # 回显的请求头里 X-Forwarded-For 是 Envoy 看到的客户端地址，应该是本机的局域网地址而不是节点的。
  printf '%s\n' "$out" | grep -i -A2 'x-forwarded-for' || true
  echo "==> smoke ok"
}

cmd="${1:-}"
case "$cmd" in
metallb) cmd_metallb ;;
nfs) cmd_nfs ;;
registry) cmd_registry ;;
gateway) cmd_gateway ;;
smoke) cmd_smoke ;;
all)
  cmd_metallb
  cmd_nfs
  cmd_registry
  cmd_gateway
  ;;
*)
  echo "usage: $0 metallb | nfs | registry | gateway | smoke | all" >&2
  exit 2
  ;;
esac
