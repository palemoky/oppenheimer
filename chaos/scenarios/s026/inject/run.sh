#!/usr/bin/env bash
# 订单库 1 的从库上把复制设成延迟 180 秒（CHANGE REPLICATION SOURCE TO SOURCE_DELAY）：IO 线程照常收，SQL 线程故意晚 3 分钟才应用。
# 两个复制线程都在跑，没有报错。这个设置存在从库的复制元数据里，重启也还在，所以要明确地改回 0。
# 另起一个后台任务，到时自己改回去（脚本的人不在了故障也会消失）。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 1200

sql_delay() { # pod seconds
  e2e_root_sql "$1" <<EOS
STOP REPLICA SQL_THREAD;
CHANGE REPLICATION SOURCE TO SOURCE_DELAY = $2;
START REPLICA SQL_THREAD;
EOS
}

case "${1:-}" in
inject)
  pod="$(e2e_mysql_pod mysql-order-1 replica)"
  cx_remember pod "$pod"
  cx_bg "$E2E_MYSQL_NS" "$pod" mysql undo <<EOS
$CX_MYSQL_PRE
while [ ! -e "\$STOP" ] && [ "\$(date +%s)" -lt "\$END" ]; do sleep 5; done
[ -e "\$STOP" ] && exit 0
M -e "STOP REPLICA SQL_THREAD; CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0; START REPLICA SQL_THREAD;"
EOS
  sql_delay "$pod" 180
  ;;
recover)
  pod="$(cx_recall pod)"
  [[ -n "$pod" ]] || pod="$(e2e_mysql_pod mysql-order-1 replica)"
  cx_stop "$E2E_MYSQL_NS" "$pod" mysql
  sql_delay "$pod" 0
  cx_clean "$E2E_MYSQL_NS" "$pod" mysql 6
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
