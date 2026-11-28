#!/usr/bin/env bash
# 库存库的主库上一个循环，每一轮开一个事务：先锁 sku_stock 里编号最大的 20 行，停 0.3 秒，再锁 stock_pending 最前面的 200 行，提交。
# 加锁的顺序和 inventory 应用增量的事务相反（它先锁 stock_pending 的一批，再更新 sku_stock），两边会互相等：死锁。
# 全是 SELECT ... FOR UPDATE，不改数据。被选成牺牲者时这一轮的客户端报错退出，循环接着下一轮。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 900

case "${1:-}" in
inject)
  pod="$(e2e_mysql_pod mysql-main primary)"
  cx_remember pod "$pod"
  cx_bg "$E2E_MYSQL_NS" "$pod" mysql batch "DB=${CX_DBP}inventory_db" <<EOS
$CX_MYSQL_PRE
M -N "\$DB" -e "SELECT 1 FROM sku_stock LIMIT 1; SELECT 1 FROM stock_pending LIMIT 1" >/dev/null || exit 1
while [ ! -e "\$STOP" ] && [ "\$(date +%s)" -lt "\$END" ]; do
  M -N "\$DB" -e "BEGIN; SELECT sku_id FROM sku_stock ORDER BY sku_id DESC LIMIT 20 FOR UPDATE; DO SLEEP(0.3); SELECT id FROM stock_pending ORDER BY id LIMIT 200 FOR UPDATE; COMMIT;" >/dev/null 2>>"\$TAG.batch.err" || true
  sleep 0.5
done
EOS
  cx_bg_check "$E2E_MYSQL_NS" "$pod" mysql batch
  ;;
recover)
  pod="$(cx_recall pod)"
  [[ -n "$pod" ]] || pod="$(e2e_mysql_pod mysql-main primary)"
  cx_stop "$E2E_MYSQL_NS" "$pod" mysql
  cx_clean "$E2E_MYSQL_NS" "$pod" mysql
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
