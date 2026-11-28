#!/usr/bin/env bash
# 订单库 1 的主库上两个会话：
#   hold  在事务里读一次 orders_1（一行都不返回），然后不提交：占着这张表的元数据锁（共享读）；
#   ask   3 秒后对同一张表要排他的元数据锁（LOCK TABLES ... WRITE），拿不到，排队。
# 排他的请求在队里时，后来的读写都排在它后面：这张表上的所有语句挂住。不改表结构、不改数据。
# ask 的等待时限比 hold 短 15 秒：它先超时退出，永远拿不到那把锁。两个会话到时自己结束。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 600

case "${1:-}" in
inject)
  pod="$(e2e_mysql_pod mysql-order-1 primary)"
  cx_remember pod "$pod"
  cx_bg "$E2E_MYSQL_NS" "$pod" mysql hold "DB=${CX_DBP}order_db_1" <<EOS
$CX_MYSQL_PRE
left=\$((END - \$(date +%s)))
M -N -n "\$DB" -e "SELECT CONNECTION_ID(); BEGIN; SELECT COUNT(*) FROM orders_1 WHERE order_id = 0; SELECT SLEEP(\$left);" >"\$TAG.hold.cid"
EOS
  cx_bg_check "$E2E_MYSQL_NS" "$pod" mysql hold
  cx_bg "$E2E_MYSQL_NS" "$pod" mysql ask "DB=${CX_DBP}order_db_1" <<EOS
$CX_MYSQL_PRE
left=\$((END - \$(date +%s) - 15))
M -N -n "\$DB" -e "SELECT CONNECTION_ID(); SET SESSION lock_wait_timeout = \$left; LOCK TABLES orders_1 WRITE; UNLOCK TABLES;" >"\$TAG.ask.cid"
EOS
  cx_bg_check "$E2E_MYSQL_NS" "$pod" mysql ask
  ;;
recover)
  pod="$(cx_recall pod)"
  [[ -n "$pod" ]] || pod="$(e2e_mysql_pod mysql-order-1 primary)"
  # 文件名的顺序就是 KILL 的顺序：ask 在 hold 前面，排队的先走，那把排他锁不会被授予。
  cx_mysql_kill "$pod"
  cx_clean "$E2E_MYSQL_NS" "$pod" mysql
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
