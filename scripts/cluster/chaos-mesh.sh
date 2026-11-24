#!/usr/bin/env bash
# 在集群上装、卸、查看 Chaos Mesh（make chaos-mesh）。设计：docs/design/4a-chaos-prep.md 的 3.2。
#
#   chaos-mesh.sh install     装到命名空间 chaos-mesh（可重复运行）
#   chaos-mesh.sh status      Pod、能被注入的命名空间、现有的实验对象
#   chaos-mesh.sh uninstall   删掉全部实验对象、卸载、删 CRD 和命名空间
#
# 装上之后没有任何命名空间能被注入：要先给命名空间加注解 chaos-mesh.org/inject=enabled
# （注入脚本在实验开始时加、结束时去掉）。monitoring、kube-system、istio-system 永远不加。
#
# 用 $KUBECONFIG_OUT 指向的 kubeconfig，不看当前的 context。版本来自 cluster.env。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
DEPLOY="$HERE/../../deploy/cluster/chaos-mesh"
NS=chaos-mesh
REPO=https://charts.chaos-mesh.org

export KUBECONFIG="$KUBECONFIG_OUT"
if [[ ! -f "$KUBECONFIG" ]]; then
  echo "$KUBECONFIG not found; run scripts/cluster/k3s.sh kubeconfig first" >&2
  exit 1
fi

# Chaos Mesh 的自定义资源的名字（podchaos.chaos-mesh.org 这一类），没装时为空。
chaos_crds() {
  kubectl get crd -o name 2>/dev/null | sed -n 's|^customresourcedefinition.apiextensions.k8s.io/\(.*\.chaos-mesh\.org\)$|\1|p'
}

cmd_install() {
  echo "==> chaos-mesh $CHAOS_MESH_VERSION"
  helm upgrade --install chaos-mesh chaos-mesh --repo "$REPO" \
    --version "$CHAOS_MESH_VERSION" --namespace "$NS" --create-namespace \
    -f "$DEPLOY/values.yaml" --wait --timeout 10m >/dev/null
  cmd_status
}

cmd_status() {
  kubectl -n "$NS" get pods -o wide
  echo "==> namespaces open to injection (annotation chaos-mesh.org/inject=enabled)"
  kubectl get ns -o jsonpath='{range .items[?(@.metadata.annotations.chaos-mesh\.org/inject=="enabled")]}{.metadata.name}{"\n"}{end}'
  echo "==> chaos objects"
  local crd found=0
  for crd in $(chaos_crds); do
    if [[ -n "$(kubectl get "$crd" -A -o name 2>/dev/null)" ]]; then
      kubectl get "$crd" -A
      found=1
    fi
  done
  [[ $found -eq 1 ]] || echo "(none)"
}

cmd_uninstall() {
  local crd
  # 先删实验对象并等它们收回故障：先卸载的话，对象上的 finalizer 没有人处理，已经注入的故障也留在节点上。
  for crd in $(chaos_crds); do
    kubectl delete "$crd" -A --all --timeout=2m >/dev/null 2>&1 || echo "warning: could not delete all $crd objects" >&2
  done
  helm uninstall chaos-mesh --namespace "$NS" --wait --timeout 5m 2>/dev/null || true
  for crd in $(chaos_crds); do
    kubectl delete crd "$crd" --timeout=2m >/dev/null
  done
  kubectl delete namespace "$NS" --ignore-not-found --timeout=3m
}

case "${1:-}" in
  install) cmd_install ;;
  status) cmd_status ;;
  uninstall) cmd_uninstall ;;
  *)
    sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
