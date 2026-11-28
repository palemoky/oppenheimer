#!/usr/bin/env bash
# 库存库的主库上，一个会话在事务里锁住 sku_stock 里编号最大的 20 行（SELECT ... FOR UPDATE），然后一直不提交。
# 只加锁，不改数据；会话结束（到时、或者被 KILL）事务回滚，锁放掉。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 600

case "${1:-}" in
inject)
  pod="$(e2e_mysql_pod mysql-main primary)"
  cx_remember pod "$pod"
  # 稳态压测每一轮用新的、编号更大的 SKU，所以编号最大的 20 个就是正在卖的那一批。
  cx_bg "$E2E_MYSQL_NS" "$pod" mysql hold "DB=${CX_DBP}inventory_db" <<EOS
$CX_MYSQL_PRE
left=\$((END - \$(date +%s)))
M -N -n "\$DB" -e "SELECT CONNECTION_ID(); BEGIN; SELECT sku_id FROM sku_stock ORDER BY sku_id DESC LIMIT 20 FOR UPDATE; SELECT SLEEP(\$left);" >"\$TAG.hold.cid"
EOS
  cx_bg_check "$E2E_MYSQL_NS" "$pod" mysql hold
  ;;
recover)
  pod="$(cx_recall pod)"
  [[ -n "$pod" ]] || pod="$(e2e_mysql_pod mysql-main primary)"
  cx_mysql_kill "$pod"
  cx_clean "$E2E_MYSQL_NS" "$pod" mysql
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
