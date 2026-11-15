#!/usr/bin/env bash
# 组建 Redis Cluster（已组建则跳过），并等 Sentinel 答得出主库、Cluster 的 cluster_state 为 ok。
# 由 make redis-modes-up 调用。容器是 compose 的 redis-modes profile 里的那些。
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f deploy/compose/docker-compose.yml --profile redis-modes)
cli() { "${COMPOSE[@]}" exec -T "$1" redis-cli "${@:2}"; }

nodes=""
for i in 1 2 3 4 5 6; do nodes+=" 172.30.0.$((30 + i)):6379"; done

if [ "$(cli redis-c1 cluster info | tr -d '\r' | sed -n 's/^cluster_known_nodes://p')" -le 1 ]; then
  # shellcheck disable=SC2086
  cli redis-c1 --cluster create $nodes --cluster-replicas 1 --cluster-yes >/dev/null
fi

deadline=$((SECONDS + 60))
until [ "$(cli redis-c1 cluster info | tr -d '\r' | sed -n 's/^cluster_state://p')" = ok ]; do
  [ $SECONDS -lt $deadline ] || { echo "cluster_state is not ok" >&2; exit 1; }
  sleep 1
done
# 3 个 Sentinel 要互相发现、并且发现了从库，之后切换才有候选。
until [ "$(cli redis-sentinel-1 -p 26379 sentinel master gate | tr -d '\r' | awk 'f{print;exit} $0=="num-other-sentinels"{f=1}')" = 2 ] \
   && [ "$(cli redis-sentinel-1 -p 26379 sentinel replicas gate | grep -c '^name$')" -ge 1 ]; do
  [ $SECONDS -lt $deadline ] || { echo "sentinel is not ready" >&2; exit 1; }
  sleep 1
done
echo "sentinel master: $(cli redis-sentinel-1 -p 26379 sentinel get-master-addr-by-name gate | tr -d '\r' | paste -sd: -)"
