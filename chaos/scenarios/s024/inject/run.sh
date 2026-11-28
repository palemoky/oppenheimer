#!/usr/bin/env bash
# 订单库 0 的主库上三个并行的循环，轮流对四张订单表跑同一条统计语句：
# 过滤条件把索引列包在函数里（走不了索引，全表扫描），按用户分组后排序，再翻到很深的一页。只读，不改数据。
# 每条语句最多跑 60 秒（MAX_EXECUTION_TIME），循环到时或者看到停止文件就结束。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 900

case "${1:-}" in
inject)
  pod="$(e2e_mysql_pod mysql-order-0 primary)"
  cx_remember pod "$pod"
  cx_bg "$E2E_MYSQL_NS" "$pod" mysql scan "DB=${CX_DBP}order_db_0" <<EOS
$CX_MYSQL_PRE
loop() {
  while [ ! -e "\$STOP" ] && [ "\$(date +%s)" -lt "\$END" ]; do
    for t in "\$@"; do
      [ -e "\$STOP" ] && break
      M -N "\$DB" -e "SELECT /*+ MAX_EXECUTION_TIME(60000) */ user_id, COUNT(*) AS c, SUM(total_amount) AS s FROM orders_\$t WHERE DATE(created_at) >= CURDATE() - INTERVAL 90 DAY GROUP BY user_id ORDER BY s DESC LIMIT 100000, 20" >/dev/null 2>>"\$TAG.scan.err" || true
    done
    sleep 1
  done
}
loop 0 2 4 6 &
loop 2 4 6 0 &
loop 4 6 0 2 &
wait
EOS
  cx_bg_check "$E2E_MYSQL_NS" "$pod" mysql scan
  ;;
recover)
  pod="$(cx_recall pod)"
  [[ -n "$pod" ]] || pod="$(e2e_mysql_pod mysql-order-0 primary)"
  cx_stop "$E2E_MYSQL_NS" "$pod" mysql
  # 正在跑的那几条语句不等它们自己跑完：按语句的特征找到并 KILL。
  cx_pod_sh "$E2E_MYSQL_NS" "$pod" mysql <<EOS
$CX_MYSQL_PRE
for id in \$(M -N -e "SELECT id FROM information_schema.processlist WHERE user = 'root' AND info LIKE 'SELECT /*+ MAX_EXECUTION_TIME(60000) */ user_id, COUNT(*) AS c%'"); do
  M -e "KILL \$id" 2>/dev/null || true
done
EOS
  cx_clean "$E2E_MYSQL_NS" "$pod" mysql
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
