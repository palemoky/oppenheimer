#!/usr/bin/env bash
# 核对并修正 redis-cache（Redis Cluster）的主从配对：每个主库的从库不能和它在同一个节点（设计文档 2b 的 4.3）。
#
#   redis-cache-pairing.sh check   只打印现状，有同节点的主从时以 1 退出
#   redis-cache-pairing.sh fix     把从库改挂到别的主库下面，直到没有同节点的主从；改不了的情况以 1 退出
#
# 为什么需要它：主从的配对是组建集群时 redis-cli --cluster create 定的，它按地址避开“同一台主机”，
# 而 Pod 的地址各不相同，它看不出哪两个 Pod 在同一个节点上。6 个 Pod 落在哪个节点由调度器定
# （每个节点两个），所以配对对不对是碰运气：2026-11-15 重建之后 redis-cache-0 和它的从库 redis-cache-4
# 都在 k3s-agent-1 上，那个节点一挂，这一片的槽就没有副本可以顶上。
#
# 能自动改的只有从库：CLUSTER REPLICATE 把一个从库改挂到另一个主库，原来的数据丢掉、重新全量同步，
# 对缓存没有影响，也不动任何槽。两个主库落在同一个节点的情况这里不改（要做主从切换，会短暂影响写入），
# 只报出来：那个节点一挂就同时少两个主库，剩下的一个凑不出多数票，整个集群不能自动切换。
#
# 用 $KUBECONFIG_OUT 指向的 kubeconfig。在 macOS 自带的 bash 3.2 上运行，判断都在 awk 里做。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
export KUBECONFIG="$KUBECONFIG_OUT"

NS=redis
RELEASE=redis-cache
mode="${1:-check}"
case "$mode" in check | fix) ;; *)
  echo "usage: $0 check | fix" >&2
  exit 2
  ;;
esac

# 输出两类行给 awk：
#   P <Pod 名> <节点名>
#   N <Pod 名> <节点 id> <master|slave> <主库的节点 id 或 ->
snapshot() {
  kubectl -n "$NS" get pods -l "app.kubernetes.io/name=redis-cluster,app.kubernetes.io/instance=$RELEASE" \
    -o jsonpath='{range .items[*]}{"P "}{.metadata.name}{" "}{.spec.nodeName}{"\n"}{end}'
  # cluster nodes 的每一行：<id> <地址@端口,主机名> <标志> <主库 id> ...
  kubectl -n "$NS" exec "$RELEASE-0" -- redis-cli cluster nodes | tr -d '\r' | awk '{
    n = split($2, a, ","); host = a[n]; sub(/\..*/, "", host)
    role = ($3 ~ /master/) ? "master" : "slave"
    print "N", host, $1, role, $4 }'
}

# 读 snapshot 的输出，打印：
#   每个主库一行现状；
#   "BAD ..."      改不了的问题；
#   "MOVE <从库的 Pod> <新主库的 id> <新主库的 Pod>"  要做的改动（已经是改动最少的方案）。
# 主库数等于从库数、且不超过 3 时才会算方案（这个集群是 3 主 3 从）；穷举从库的全部排列。
plan() {
  awk '
    $1 == "P" { node[$2] = $3 }
    $1 == "N" { id[$2] = $3; role[$2] = $4; mid[$2] = $5; pod[$3] = $2 }
    END {
      nm = 0; nr = 0
      for (p in role) {
        if (!(p in node)) { print "BAD " p " is in the cluster but has no Pod"; bad = 1 }
        if (role[p] == "master") m[++nm] = p; else r[++nr] = p
      }
      for (i = 1; i <= nm; i++) for (j = i + 1; j <= nm; j++)
        if (node[m[i]] == node[m[j]]) { print "BAD masters " m[i] " and " m[j] " are both on " node[m[i]]; bad = 1 }
      same = 0
      for (i = 1; i <= nr; i++) {
        mp = pod[mid[r[i]]]
        flag = (node[r[i]] == node[mp]) ? "  <-- same node" : ""
        if (flag != "") same++
        printf "%s (%s) <- %s (%s)%s\n", mp, node[mp], r[i], node[r[i]], flag
      }
      if (bad || same == 0) exit
      if (nm != nr || nm > 3) { print "BAD cannot plan for " nm " masters and " nr " replicas"; exit }
      # 排列 perm[k]：第 k 个从库挂到第 perm[k] 个主库。3 个元素的 6 种排列直接列出来。
      np = split("1 2 3|1 3 2|2 1 3|2 3 1|3 1 2|3 2 1", perms, "|")
      best = -1; bestmoves = 99
      for (x = 1; x <= np; x++) {
        split(perms[x], q, " "); ok = 1; moves = 0
        for (k = 1; k <= nr; k++) {
          if (q[k] > nm) { ok = 0; break }
          if (node[r[k]] == node[m[q[k]]]) { ok = 0; break }
          if (mid[r[k]] != id[m[q[k]]]) moves++
        }
        if (ok && moves < bestmoves) { best = x; bestmoves = moves }
      }
      if (best < 0) { print "BAD no pairing keeps every replica off its master node"; exit }
      split(perms[best], q, " ")
      for (k = 1; k <= nr; k++)
        if (mid[r[k]] != id[m[q[k]]]) print "MOVE " r[k] " " id[m[q[k]]] " " m[q[k]]
    }' | sort
}

echo "==> $RELEASE: master (node) <- replica (node)"
out="$(snapshot | plan)"
printf '%s\n' "$out" | awk '$1 != "MOVE" && $1 != "BAD" { print "    " $0 }'
if printf '%s\n' "$out" | awk '$1 == "BAD" { found = 1; print "    ANOMALY: " substr($0, 5) > "/dev/stderr" } END { exit !found }'; then
  echo "$RELEASE: not fixing automatically, see above" >&2
  exit 1
fi
moves="$(printf '%s\n' "$out" | awk '$1 == "MOVE"')"
if [[ -z "$moves" ]]; then
  echo "    ok: no replica shares a node with its master"
  exit 0
fi
if [[ "$mode" == check ]]; then
  echo "$RELEASE: some replicas share a node with their master; run '$0 fix'" >&2
  exit 1
fi

# 一次改一个。改完等集群里所有从库的复制都连上，再回头核对一遍。
while read -r _ replica master_id master_pod; do
  echo "==> $replica: now replicating $master_pod"
  kubectl -n "$NS" exec "$replica" -- redis-cli cluster replicate "$master_id" >/dev/null
done <<<"$moves"

for _ in $(seq 1 30); do
  down="$(kubectl -n "$NS" exec "$RELEASE-0" -- redis-cli cluster nodes | tr -d '\r' | awk '$3 ~ /slave/ && $8 != "connected"' | wc -l | tr -d ' ')"
  [[ "$down" == 0 ]] && break
  sleep 2
done
out="$(snapshot | plan)"
printf '%s\n' "$out" | awk '{ print "    " $0 }'
if printf '%s\n' "$out" | awk '$1 == "MOVE" || $1 == "BAD" { found = 1 } END { exit !found }'; then
  echo "$RELEASE: pairing is still wrong after the fix" >&2
  exit 1
fi
kubectl -n "$NS" exec "$RELEASE-0" -- redis-cli --cluster check "$RELEASE-0.$RELEASE.$NS.svc.cluster.local:6379" | tr -d '\r' | tail -4
