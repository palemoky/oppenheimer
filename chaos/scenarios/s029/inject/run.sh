#!/usr/bin/env bash
# 缓存 Redis（Redis Cluster）的一个主节点上，每秒执行一次空转的 Lua 脚本，一次占住主线程约 250 毫秒。
# 脚本不读不写任何键。循环次数先在这个节点上量一次再定（不同机器差几倍）。到时或者看到停止文件就结束。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 600
RNS=redis

case "${1:-}" in
inject)
  pod=""
  for i in 0 1 2 3 4 5; do
    if [[ "$(kubectl -n "$RNS" exec "redis-cache-$i" -c redis -- redis-cli role 2>/dev/null | head -n 1)" == master ]]; then
      pod="redis-cache-$i"
      break
    fi
  done
  [[ -n "$pod" ]] || cx_die "no redis-cache master found"
  cx_remember pod "$pod"
  cx_bg "$RNS" "$pod" redis spin "BLOCK_MS=${CX_BLOCK_MS:-250}" <<'EOS'
lua='local s = 0 for i = 1, tonumber(ARGV[1]) do s = s + i end return s'
probe=2000000
a=$(date +%s%N)
redis-cli eval "$lua" 0 "$probe" >/dev/null || exit 1
b=$(date +%s%N)
ms=$(((b - a) / 1000000))
[ "$ms" -lt 1 ] && ms=1
n=$((probe * BLOCK_MS / ms))
echo "probe ${ms}ms, n=$n"
while [ ! -e "$STOP" ] && [ "$(date +%s)" -lt "$END" ]; do
  redis-cli eval "$lua" 0 "$n" >/dev/null 2>&1 || true
  sleep 0.75
done
EOS
  cx_bg_check "$RNS" "$pod" redis spin
  ;;
recover)
  pod="$(cx_recall pod)"
  [[ -n "$pod" ]] || exit 0
  cx_stop "$RNS" "$pod" redis
  cx_clean "$RNS" "$pod" redis
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
