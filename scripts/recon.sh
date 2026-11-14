#!/usr/bin/env bash
# 对账（ADR-0022）：运行 services/recon/cmd/recon，在系统运行中比对几份应当一致的数据。
#
# 用法：scripts/recon.sh [recon 的参数...]，例如 scripts/recon.sh -pairs index -settle 10m
# 通常经 make recon 调用。不带参数时比 replica、index、payment、stock 四对。
#
# 可用环境变量覆盖：DB_PREFIX（库名前缀，默认空）、MYSQL_ROOT_PASSWORD、MYSQL_PORT（冷库、索引库、
# 支付库、库存库所在的实例）、订单分片的端口（同 scripts/run-local.sh），以及 ORDER_DBS（2 或 4，默认 2）。
# 订单库 DSN 的个数和顺序与 order 服务的 MYSQL_DSNS、MYSQL_REPLICA_DSNS 完全一致。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DB_PREFIX="${DB_PREFIX:-}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"
MYSQL_PORT="${MYSQL_PORT:-3306}"

ORDER_DBS="${ORDER_DBS:-2}"
case "$ORDER_DBS" in
2 | 4) ;;
*)
  echo "ORDER_DBS must be 2 or 4, got $ORDER_DBS" >&2
  exit 2
  ;;
esac

dsn_at() { echo "root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:$1)/${DB_PREFIX}$2"; }
dsn() { dsn_at "$MYSQL_PORT" "$1"; }

# 端口规律：分片 i 的主库 3311、3321、3331、3341，即 3301 + 10*(i+1)；从库是主库端口加 1。
primaries=""
replicas=""
for ((i = 0; i < ORDER_DBS; i++)); do
  port_var="MYSQL_ORDER_${i}_PORT"
  replica_var="MYSQL_ORDER_${i}_REPLICA_PORT"
  port="${!port_var:-$((3301 + 10 * (i + 1)))}"
  replica_port="${!replica_var:-$((port + 1))}"
  sep=""
  [ -n "$primaries" ] && sep=","
  primaries="$primaries$sep$(dsn_at "$port" "order_db_$i")"
  replicas="$replicas$sep$(dsn_at "$replica_port" "order_db_$i")"
done

cd "$ROOT"
MYSQL_DSNS="$primaries" MYSQL_REPLICA_DSNS="$replicas" ARCHIVE_DSN="$(dsn order_archive_db)" \
  MERCHANT_INDEX_DSNS="$(dsn merchant_index_db_0),$(dsn merchant_index_db_1)" \
  PAYMENT_DSN="$(dsn payment_db)" INVENTORY_DSN="$(dsn inventory_db)" \
  exec ${RECON_BIN:-go run ./services/recon/cmd/recon} "$@"
