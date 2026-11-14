#!/usr/bin/env bash
# 端到端测试：在独立的一组库（e2e_*）上启动全部服务，对网关跑 test/e2e 的测试。
# 需要先 make up 和 make build。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
COMPOSE=(docker compose -f "$ROOT/deploy/compose/docker-compose.yml")

MYSQL_ORDER_0_PORT="${MYSQL_ORDER_0_PORT:-3311}"
MYSQL_ORDER_1_PORT="${MYSQL_ORDER_1_PORT:-3321}"
MYSQL_ORDER_0_REPLICA_PORT="${MYSQL_ORDER_0_REPLICA_PORT:-3312}"
MYSQL_ORDER_1_REPLICA_PORT="${MYSQL_ORDER_1_REPLICA_PORT:-3322}"

# 在指定实例上执行 SQL。参数：compose 服务名、宿主机端口、SQL、可选的额外 mysql 参数。
# CI 里用 runner 自带的 mysql 客户端连宿主机端口（compose 起的容器映射在 127.0.0.1），
# 本机则用 docker compose exec，不要求本机装 mysql 客户端。
run_sql() {
  local svc="$1" port="$2" stmt="$3"
  shift 3
  if [ -n "${CI:-}" ]; then
    mysql -h127.0.0.1 -P"$port" -uroot -p"$MYSQL_ROOT_PASSWORD" "$@" -e "$stmt" 2>/dev/null
  else
    "${COMPOSE[@]}" exec -T "$svc" mysql -uroot -p"$MYSQL_ROOT_PASSWORD" "$@" -e "$stmt" 2>/dev/null
  fi
}

# 每次从干净的库开始，结果不受上一次运行影响。
# inventory、payment 在单实例 mysql 上；order 的两个分片库建在各自的主库上，DDL 会复制到从库，
# 不能在从库上建（从库是只读的，也会造成 GTID 分叉）。
run_sql mysql "$MYSQL_PORT" "
  DROP DATABASE IF EXISTS e2e_inventory_db; CREATE DATABASE e2e_inventory_db;
  DROP DATABASE IF EXISTS e2e_payment_db; CREATE DATABASE e2e_payment_db;
  DROP DATABASE IF EXISTS e2e_merchant_index_db_0; CREATE DATABASE e2e_merchant_index_db_0;
  DROP DATABASE IF EXISTS e2e_merchant_index_db_1; CREATE DATABASE e2e_merchant_index_db_1;
  DROP DATABASE IF EXISTS e2e_order_archive_db; CREATE DATABASE e2e_order_archive_db;"
# CDC 的账号只有 order_db_N 的 SELECT 权限（复制脚本给的），e2e 的库要另外授权。授权也在主库上做，靠复制到从库。
for i in 0 1; do
  port_var="MYSQL_ORDER_${i}_PORT"
  run_sql "mysql-order-$i" "${!port_var}" "DROP DATABASE IF EXISTS e2e_order_db_$i; CREATE DATABASE e2e_order_db_$i;
    GRANT SELECT ON e2e_order_db_$i.* TO 'cdc'@'%';"
done

# 等库复制到从库，否则服务启动时连从库会报库不存在。有上限，超时就失败。
for i in 0 1; do
  port_var="MYSQL_ORDER_${i}_REPLICA_PORT"
  for _ in $(seq 1 60); do
    n="$(run_sql "mysql-order-$i-replica" "${!port_var}" \
      "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='e2e_order_db_$i'" -N -B || echo 0)"
    [ "$n" = 1 ] && break
    sleep 1
  done
  if [ "$n" != 1 ]; then
    echo "e2e_order_db_$i did not replicate to mysql-order-$i-replica within 60s" >&2
    exit 1
  fi
done

export DB_PREFIX=e2e_

# 读从库的模式默认用 gtid（覆盖最完整），标记时限缩到 1s，让主从延迟的用例不用等默认的 3s。
export ORDER_READ_MODE="${ORDER_READ_MODE:-gtid}"
export ORDER_READ_PIN_TTL="${ORDER_READ_PIN_TTL:-1s}"

# 主从延迟的用例会在从库上 STOP REPLICA SQL_THREAD。用例自己用 t.Cleanup 恢复，
# 但进程被杀、测试 panic 时 Cleanup 可能没机会跑，这里无条件再拉起一次：
# 已在运行时 START REPLICA 只产生警告；失败也不能盖掉测试本身的退出码。
restart_replicas() {
  local i port_var
  for i in 0 1; do
    port_var="MYSQL_ORDER_${i}_REPLICA_PORT"
    run_sql "mysql-order-$i-replica" "${!port_var}" "START REPLICA" || true
  done
}
trap restart_replicas EXIT

# 每次运行用不同的 topic 和消费者组前缀。数据库每次都重建，上一次运行留在 topic 里的消息
# 对不上新的数据，不能让这次的消费者读到。顺手删掉以前运行留下的 topic，失败不影响测试。
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --delete --topic 'e2e-.*' >/dev/null 2>&1 || true
export KAFKA_PREFIX="e2e-$(date +%s)."
export REDIS_PREFIX="e2e-$(date +%s):"
export LOG_DIR="$ROOT/tmp/e2e-logs"
# 缩短各时限，让超时关单这类场景能在几秒内测完。
export PAYMENT_TIMEOUT=3s
# sweeper 故意设得比测试的等待时间长：超时关单必须由延迟消息触发才能在时限内完成，
# 这样端到端测试覆盖的是延迟关单这条路径，而不是兜底的扫表。
export SWEEP_INTERVAL=1m
export GATE_RECONCILE_INTERVAL=1s
export LOG_LEVEL=warn

export E2E_GATEWAY_URL=http://127.0.0.1:8080
export E2E_INVENTORY_DSN="root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_PORT})/e2e_inventory_db"
export E2E_PAYMENT_TIMEOUT=3s
# 商家索引的用例：直接读两个索引库和 order 的两个主库上的 e2e 库，核对索引和订单表一致。
export E2E_MERCHANT_INDEX_DSNS="root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_PORT})/e2e_merchant_index_db_0,root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_PORT})/e2e_merchant_index_db_1"
export E2E_ORDER_DSNS="root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_ORDER_0_PORT})/e2e_order_db_0,root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_ORDER_1_PORT})/e2e_order_db_1"
# 主从延迟用例：两个分片库的主库和从库（下标即分片号），以及读模式和标记时限。
# 主库用来读 gtid_executed，从库用来停复制和等追上；不带库名，只用管理语句。
export E2E_ORDER_PRIMARY_DSNS="root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_ORDER_0_PORT})/,root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_ORDER_1_PORT})/"
export E2E_ORDER_REPLICA_DSNS="root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_ORDER_0_REPLICA_PORT})/,root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_ORDER_1_REPLICA_PORT})/"
export E2E_ORDER_READ_MODE="$ORDER_READ_MODE"
export E2E_ORDER_READ_PIN_TTL="$ORDER_READ_PIN_TTL"

# 归档用例（ADR-0016）：冷库 DSN 和 archiver 可执行文件（make build 产出）。订单主库的 DSN 复用 E2E_ORDER_DSNS。
export E2E_ARCHIVE_DSN="root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_PORT})/e2e_order_archive_db"
export E2E_ARCHIVER_BIN="$ROOT/bin/archiver"

cd "$ROOT/test/e2e"
"$ROOT/scripts/run-local.sh" -- go test -race -count=1 -v ./... "$@"
