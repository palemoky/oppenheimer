#!/usr/bin/env bash
# 给集群上长期运行的那一套写入示例 SKU（和本机 make seed 同样的四个，外加探针用的 3001）。
# 要在服务至少部署过一次、inventory 的迁移建好表之后运行；可以重复执行（已有的行只更新名字，不动库存）。
#
#   1001 到 1003 属于商家 1，2001 属于商家 2；3001（商家 3）是 scripts/cluster/probe.py 默认用的、库存很大的 SKU。
#
# SQL 在 mysql-main 的主库 Pod 里用 root 执行（密码取自容器自己的环境变量，不经过命令行），同 e2e-lib.sh 的做法。
# 只写 MySQL：闸门（Redis）里的库存由 inventory 在第一次用到这个 SKU 时从 MySQL 装载。
# 在 macOS 自带的 bash 3.2 上运行。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
export KUBECONFIG="$KUBECONFIG_OUT"
# shellcheck source=scripts/cluster/e2e-lib.sh
source "$HERE/e2e-lib.sh"

pod="$(e2e_mysql_pod mysql-main primary)"
printf '%s\n' "INSERT INTO inventory_db.sku_stock (sku_id, merchant_id, name, unit_price, available) VALUES
  (1001, 1, 'Mechanical Keyboard', 39900, 100), (1002, 1, 'Wireless Mouse', 12900, 500), (1003, 1, 'Limited Figure', 99900, 10),
  (2001, 2, 'Desk Lamp', 15900, 200), (3001, 3, 'Probe Item', 100, 100000000)
  ON DUPLICATE KEY UPDATE name = VALUES(name);
SELECT sku_id, merchant_id, name, unit_price, available FROM inventory_db.sku_stock WHERE sku_id IN (1001, 1002, 1003, 2001, 3001) ORDER BY sku_id;" |
  e2e_root_sql "$pod" -t
