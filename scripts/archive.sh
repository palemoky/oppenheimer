#!/usr/bin/env bash
# 订单冷热归档（ADR-0016）：运行 services/order/cmd/archiver，把旧的终态订单从热库搬到冷库。
#
# 用法：scripts/archive.sh [archiver 的参数...]，例如 scripts/archive.sh -dry-run -older-than 744h
# 通常经 make archive、make archive-dry-run、make archive-reclaim 调用。
#
# 可用环境变量覆盖：DB_PREFIX（库名前缀，默认空）、MYSQL_ROOT_PASSWORD、MYSQL_PORT（冷库所在实例）、
# 订单分片的端口（同 scripts/run-local.sh），以及 ORDER_DBS（2 或 4，含义同 run-local.sh，默认 2）。
# DSN 的个数和顺序与 order 服务的 MYSQL_DSNS、MYSQL_REPLICA_DSNS 完全一致，对不上会搬错库。
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
MYSQL_DSNS="$primaries" MYSQL_REPLICA_DSNS="$replicas" ARCHIVE_DSN="$(dsn_at "$MYSQL_PORT" order_archive_db)" \
  exec go run ./services/order/cmd/archiver "$@"
