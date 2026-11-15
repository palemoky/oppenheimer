#!/bin/bash
# 给 middleware.sh 经 kubectl exec 调用，看这个实例自己的状态。只读，不改任何东西。
#
#   status.sh show [<主库的 Pod 名>]
#     打印摘要：主库给 gtid_executed，从库给复制源和复制线程的状态。
#     给了主库的 Pod 名（-primary Service 指向的那个）时，再和它核对，不一致的打一行 ANOMALY：
#       - 自己就是它，却不是“可写、没配复制”
#       - 自己不是它，却可写又没配复制（比如切换后回来的旧主库：Service 不指向它，但直接连还能写）
#       - 自己不是它，复制源却不是它
#     有 ANOMALY 时退出码为 3。
set -euo pipefail
# shellcheck source=lib.sh
. /scripts/lib.sh

case "${1:-}" in
show)
  want="${2:-}"
  role="$(instance_role 127.0.0.1)"
  common="$(rsql 127.0.0.1 -N -B -e "SELECT @@global.server_id, @@global.super_read_only, REPLACE(@@global.gtid_executed, '\n', '')")"
  read -r id ro gtid <<<"$common"
  echo "role=$role server_id=$id super_read_only=$ro gtid_executed=${gtid:-<empty>}"
  src=""
  if [ "$role" = replica ]; then
    src="$(replication_source 127.0.0.1)"
    # MySQL 9.7 的客户端在 -e 里不认 \G，用 --vertical。
    rsql 127.0.0.1 --vertical -e "SHOW REPLICA STATUS" |
      awk -F': ' '/^ *(Source_Host|Replica_IO_Running|Replica_SQL_Running|Seconds_Behind_Source|Last_IO_Error|Last_SQL_Error):/ {
        sub(/^ +/, "", $1); printf "  %s=%s\n", $1, $2 }'
  fi
  [ -n "$want" ] || exit 0
  anomaly=""
  if [ "$HOSTNAME" = "$want" ]; then
    [ "$role" = primary ] || anomaly="the -primary Service points here but this instance is $role, not writable-without-replication"
  elif [ "$role" = primary ]; then
    anomaly="WRITABLE but not the primary ($want): direct writes here diverge from the primary; set it read-only and rebuild replication by hand"
  elif [ "$role" != replica ]; then
    anomaly="neither primary nor replicating (read-only without replication)"
  else
    want_host="$(peer_host "${want##*-}")"
    [ "$src" = "$want_host" ] || anomaly="replicating from '$src', expected $want_host"
  fi
  if [ -n "$anomaly" ]; then
    echo "  ANOMALY: $anomaly"
    exit 3
  fi
  ;;
*)
  echo "usage: $0 show [<primary pod name>]" >&2
  exit 2
  ;;
esac
