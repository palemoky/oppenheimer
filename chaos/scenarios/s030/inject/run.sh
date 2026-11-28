#!/usr/bin/env bash
# 缓存 Redis 里造一个大 key（一个 hash，20 万个字段、每个值 100 字节，约 30 MB），然后每 2 秒把它整个读一遍（HGETALL）。
# key 落在哪个分片由它的名字决定；读它的循环跑在持有它的那个主节点的 Pod 里，直接连本机，读到的内容丢掉（不经过 redis-cli，免得在容器里攒出几十 MB 的应答）。
# key 带过期时间（时长加 5 分钟），恢复时用 UNLINK 删。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 900
RNS=redis
FIELDS="${CX_BIG_FIELDS:-200000}"

case "${1:-}" in
inject)
  key="$(cx_app_env order REDIS_PREFIX)cache:export:orders:all"
  cx_remember key "$key"
  # 分批写，每批 2 万个字段：一批一个脚本，占主线程十几毫秒。-c 跟着 MOVED 走到持有这个槽的节点。
  cx_pod_sh "$RNS" redis-cache-0 redis "KEY=$key" "FIELDS=$FIELDS" "TTL=$((CX_DURATION + 300))" <<'EOS'
lua='for i = tonumber(ARGV[1]), tonumber(ARGV[2]) do redis.call("HSET", KEYS[1], i, string.rep("x", 100)) end return 1'
i=1
while [ "$i" -le "$FIELDS" ]; do
  j=$((i + 19999))
  [ "$j" -gt "$FIELDS" ] && j=$FIELDS
  redis-cli -c eval "$lua" 1 "$KEY" "$i" "$j" >/dev/null || exit 1
  i=$((j + 1))
done
redis-cli -c expire "$KEY" "$TTL" >/dev/null
EOS
  pod=""
  for i in 0 1 2 3 4 5; do
    if [[ "$(kubectl -n "$RNS" exec "redis-cache-$i" -c redis -- redis-cli role 2>/dev/null | head -n 1)" == master &&
      "$(kubectl -n "$RNS" exec "redis-cache-$i" -c redis -- redis-cli exists "$key" 2>/dev/null)" == 1 ]]; then
      pod="redis-cache-$i"
      break
    fi
  done
  [[ -n "$pod" ]] || cx_die "the key was written but no master reports it"
  cx_remember pod "$pod"
  cx_bg "$RNS" "$pod" redis read "KEY=$key" <<'EOS'
while [ ! -e "$STOP" ] && [ "$(date +%s)" -lt "$END" ]; do
  exec 3<>/dev/tcp/127.0.0.1/6379 || exit 1
  printf 'HGETALL %s\r\nQUIT\r\n' "$KEY" >&3
  timeout 10 cat <&3 >/dev/null
  exec 3<&- 3>&-
  sleep 2
done
redis-cli -c unlink "$KEY" >/dev/null 2>&1
EOS
  cx_bg_check "$RNS" "$pod" redis read
  ;;
recover)
  key="$(cx_recall key)"
  pod="$(cx_recall pod)"
  if [[ -n "$pod" ]]; then
    cx_stop "$RNS" "$pod" redis
  fi
  if [[ -n "$key" ]]; then
    kubectl -n "$RNS" exec redis-cache-0 -c redis -- redis-cli -c unlink "$key" >/dev/null
  fi
  if [[ -n "$pod" ]]; then
    cx_clean "$RNS" "$pod" redis 4
  fi
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
