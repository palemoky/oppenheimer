#!/usr/bin/env bash
# 看或改集群上一个服务的降级开关（设计文档 2c 的 7.6，开关本身见 1d 的第 6 节、pkg/degrade）。
#
#   degrade.sh <服务>                    列出这个服务每个 Pod 的全部开关
#   degrade.sh <服务> <开关> off|auto    对每个 Pod 改一遍（off 强制降级，auto 恢复自动），再打印各 Pod 的当前值
#
# 服务：gateway order inventory payment orderindex cdc。环境变量 APP_NAMESPACE 默认 oppenheimer。
#
# 开关在进程内存里，每个 Pod 各管各的，所以要逐个 Pod 调；**之后新起来的 Pod（滚动、扩容、重启）不知道被改过**，
# 回到 DEGRADE 环境变量给的初始值。这个缺口是已知的，要补得让开关有共同的来源，阶段 4 再定。
# 改完看输出里是不是每个 Pod 都一样；HPA 管着的服务（gateway、order）尤其要留意副本数变了没有。
#
# 为什么走 kubectl port-forward：服务的镜像里没有 shell 也没有 curl，不能 kubectl exec；
# gateway 的开关在只听 127.0.0.1 的管理端口上，从别的 Pod 连不到，而 port-forward 是进到 Pod 的网络命名空间里再连的。
# 在 macOS 自带的 bash 3.2 上运行。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
export KUBECONFIG="$KUBECONFIG_OUT"
NS="${APP_NAMESPACE:-oppenheimer}"

usage() {
  sed -n '2,8p' "${BASH_SOURCE[0]}" >&2
  exit 2
}

svc="${1:-}"
name="${2:-}"
mode="${3:-}"
[[ -n "$svc" ]] || usage
# /degrade 所在的端口：gateway 在管理端口上，其余和 /healthz 同一个。
case "$svc" in
gateway) port=8089 ;;
order) port=8081 ;;
inventory) port=8082 ;;
payment) port=8083 ;;
orderindex) port=8084 ;;
cdc) port=8085 ;;
*) usage ;;
esac
if [[ -n "$name" ]]; then
  case "$mode" in
  off | auto) ;;
  *) usage ;;
  esac
fi

PF_PID=""
# shellcheck disable=SC2329 # 由下面的 trap 调用
cleanup() {
  if [[ -n "$PF_PID" ]]; then kill "$PF_PID" 2>/dev/null || true; fi
}
trap cleanup EXIT

pods="$(kubectl -n "$NS" get pods -l "app.kubernetes.io/name=$svc" \
  --field-selector=status.phase=Running -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}')"
if [[ -z "$pods" ]]; then
  echo "no running pods of $svc in namespace $NS" >&2
  exit 1
fi

# 把 /degrade 的 JSON 打成一行一个开关。
show() {
  python3 -I -c '
import json, sys
d = json.load(sys.stdin)
if "error" in d:
    print("    error:", d["error"]); sys.exit(1)
for s in d.get("switches", []):
    state = "DEGRADED" if s.get("degraded") else "ok"
    print("    %-24s mode=%-4s %-8s %s" % (s["name"], s["mode"], state, s.get("detail", "")))
'
}

failed=0
local_port=18900
for pod in $pods; do
  local_port=$((local_port + 1))
  echo "==> $pod"
  kubectl -n "$NS" port-forward "pod/$pod" "$local_port:$port" >/dev/null 2>&1 &
  PF_PID=$!
  # 等转发的端口能连上（最多 5 秒）。
  ok=0
  for _ in $(seq 1 25); do
    if curl -fsS -m 1 -o /dev/null "127.0.0.1:$local_port/degrade" 2>/dev/null; then
      ok=1
      break
    fi
    sleep 0.2
  done
  if [[ $ok -eq 0 ]]; then
    echo "    cannot reach /degrade on port $port (is the pod terminating?)" >&2
    failed=1
  elif [[ -n "$name" ]]; then
    # 4xx 时也要把服务给的错误打出来，所以不用 -f。
    curl -sS -m 5 -X PUT "127.0.0.1:$local_port/degrade/$name?mode=$mode" | show || failed=1
  else
    curl -sS -m 5 "127.0.0.1:$local_port/degrade" | show || failed=1
  fi
  kill "$PF_PID" 2>/dev/null || true
  wait "$PF_PID" 2>/dev/null || true
  PF_PID=""
done
exit "$failed"
