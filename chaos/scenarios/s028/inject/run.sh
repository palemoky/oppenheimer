#!/usr/bin/env bash
# mysql-main 的主库上把 max_connections 调小（SET GLOBAL，不是 PERSIST）：已有的连接不受影响，新建连接超过上限就被拒绝（1040）。
# 原来的值记在运行目录里，恢复时改回去；另起一个后台任务到时自己改回去。Pod 重启也会回到配置文件里的值。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 1800
LIMIT="${CX_MAX_CONNECTIONS:-30}"

case "${1:-}" in
inject)
  pod="$(e2e_mysql_pod mysql-main primary)"
  cx_remember pod "$pod"
  orig="$(echo 'SELECT @@GLOBAL.max_connections' | e2e_root_sql "$pod" -N -B)"
  [[ "$orig" =~ ^[0-9]+$ ]] || cx_die "cannot read max_connections (got '$orig')"
  cx_remember orig "$orig"
  cx_bg "$E2E_MYSQL_NS" "$pod" mysql undo "ORIG=$orig" <<EOS
$CX_MYSQL_PRE
while [ ! -e "\$STOP" ] && [ "\$(date +%s)" -lt "\$END" ]; do sleep 5; done
[ -e "\$STOP" ] && exit 0
M -e "SET GLOBAL max_connections = \$ORIG"
EOS
  echo "SET GLOBAL max_connections = $LIMIT" | e2e_root_sql "$pod"
  ;;
recover)
  pod="$(cx_recall pod)"
  [[ -n "$pod" ]] || pod="$(e2e_mysql_pod mysql-main primary)"
  orig="$(cx_recall orig)"
  # 没有记录（注入在读到原值之前就失败了）时什么都没有改过，不动。
  if [[ "$orig" =~ ^[0-9]+$ ]]; then
    cx_stop "$E2E_MYSQL_NS" "$pod" mysql
    echo "SET GLOBAL max_connections = $orig" | e2e_root_sql "$pod"
  fi
  cx_clean "$E2E_MYSQL_NS" "$pod" mysql 6
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
