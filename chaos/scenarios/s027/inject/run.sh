#!/usr/bin/env bash
# 订单库 0 的主库上，在一个单独建的库（tmp_report，不碰业务表）里做两轮大事务：
# 一条语句插入 200 万行（一个事务，binlog 约 450 MB），停 2 分钟，一条语句全删掉（又一个同样大的事务），停 3 分钟。
# 恢复时删掉这个库。主库和从库的 binlog 各多约 1.8 GB，一天后过期。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
# shellcheck source=scripts/cluster/chaos-lib.sh
source "$ROOT/scripts/cluster/chaos-lib.sh"
cx_init 900

case "${1:-}" in
inject)
  pod="$(e2e_mysql_pod mysql-order-0 primary)"
  cx_remember pod "$pod"
  cx_bg "$E2E_MYSQL_NS" "$pod" mysql job "ROWS=${CX_BIG_ROWS:-2000000}" <<EOS
$CX_MYSQL_PRE
nap() { t=\$1; while [ "\$t" -gt 0 ] && [ ! -e "\$STOP" ] && [ "\$(date +%s)" -lt "\$END" ]; do sleep 5; t=\$((t - 5)); done; }
M -e "CREATE DATABASE IF NOT EXISTS tmp_report; CREATE TABLE IF NOT EXISTS tmp_report.daily_orders (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, pad VARCHAR(255) NOT NULL) ENGINE = InnoDB" || exit 1
n=0
while [ "\$n" -lt 2 ] && [ ! -e "\$STOP" ] && [ "\$(date +%s)" -lt "\$((END - 150))" ]; do
  M -e "SET SESSION cte_max_recursion_depth = \$ROWS; INSERT INTO tmp_report.daily_orders (pad) WITH RECURSIVE s (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < \$ROWS) SELECT REPEAT('x', 200) FROM s" || exit 1
  nap 120
  M -e "DELETE FROM tmp_report.daily_orders" || exit 1
  nap 180
  n=\$((n + 1))
done
EOS
  cx_bg_check "$E2E_MYSQL_NS" "$pod" mysql job
  ;;
recover)
  pod="$(cx_recall pod)"
  [[ -n "$pod" ]] || pod="$(e2e_mysql_pod mysql-order-0 primary)"
  cx_stop "$E2E_MYSQL_NS" "$pod" mysql
  # 正在跑的那条大语句会挡住 DROP（元数据锁）：最多等 5 分钟。
  e2e_root_sql "$pod" <<'EOS'
SET SESSION lock_wait_timeout = 300;
DROP DATABASE IF EXISTS tmp_report;
EOS
  cx_clean "$E2E_MYSQL_NS" "$pod" mysql 6
  ;;
*)
  echo "usage: run.sh inject|recover" >&2
  exit 2
  ;;
esac
