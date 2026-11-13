#!/usr/bin/env bash
# 在本机启动四个服务。依赖用 make up 启动。
#
# 用法：
#   scripts/run-local.sh            前台运行，Ctrl-C 停止全部
#   scripts/run-local.sh -- <命令>   启动服务，执行命令，然后停止全部；退出码取自该命令
#
# 可用环境变量覆盖：DB_PREFIX（库名前缀，默认空，即 order_db_0 等）、MYSQL_ROOT_PASSWORD、MYSQL_PORT、
# 以及 order 分片的四个端口 MYSQL_ORDER_{0,1}_PORT、MYSQL_ORDER_{0,1}_REPLICA_PORT，
# 和各服务自己的配置（PAYMENT_TIMEOUT、SWEEP_INTERVAL 等）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin"
LOG_DIR="${LOG_DIR:-$ROOT/tmp/logs}"
DB_PREFIX="${DB_PREFIX:-}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"
MYSQL_PORT="${MYSQL_PORT:-3306}"

# order 的两个分片各有一主一从，端口与 compose 一致；inventory、payment 仍在单实例 mysql 上。
MYSQL_ORDER_0_PORT="${MYSQL_ORDER_0_PORT:-3311}"
MYSQL_ORDER_0_REPLICA_PORT="${MYSQL_ORDER_0_REPLICA_PORT:-3312}"
MYSQL_ORDER_1_PORT="${MYSQL_ORDER_1_PORT:-3321}"
MYSQL_ORDER_1_REPLICA_PORT="${MYSQL_ORDER_1_REPLICA_PORT:-3322}"

dsn_at() { echo "root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:$1)/${DB_PREFIX}$2"; }
dsn() { dsn_at "$MYSQL_PORT" "$1"; }

# Kafka 和 Redis 的地址对所有服务相同，直接导出，子进程会继承。
# KAFKA_PREFIX、REDIS_PREFIX 默认为空；端到端测试会设置它们，与手动运行的环境隔离。
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:${KAFKA_PORT:-9092}}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:${REDIS_PORT:-6379}}"
# 查询缓存用单独的实例（compose 里的 redis-cache）。服务在没有设置 CACHE_REDIS_ADDR 时退回 REDIS_ADDR。
export CACHE_REDIS_ADDR="${CACHE_REDIS_ADDR:-127.0.0.1:${CACHE_REDIS_PORT:-6380}}"
export KAFKA_PREFIX="${KAFKA_PREFIX:-}"
export REDIS_PREFIX="${REDIS_PREFIX:-}"

mkdir -p "$LOG_DIR"
pids=()

stop_all() {
  trap - EXIT INT TERM
  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] && kill -TERM "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap stop_all EXIT INT TERM

start() {
  local name="$1" http_port="$2"
  shift 2
  env "$@" "$BIN/$name" >"$LOG_DIR/$name.log" 2>&1 &
  pids+=($!)
  for _ in $(seq 1 100); do
    if curl -fsS "http://127.0.0.1:${http_port}/healthz" >/dev/null 2>&1; then
      echo "started $name (pid ${pids[${#pids[@]}-1]}), log: $LOG_DIR/$name.log"
      return 0
    fi
    if ! kill -0 "${pids[${#pids[@]}-1]}" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  echo "failed to start $name, last log lines:" >&2
  tail -n 20 "$LOG_DIR/$name.log" >&2
  exit 1
}

# 启动顺序按依赖方向：被依赖的先起。
start inventory 8082 MYSQL_DSN="$(dsn inventory_db)" WORKER_ID=1
# MYSQL_REPLICA_DSNS 的个数和顺序与 MYSQL_DSNS 一一对应。
start order     8081 MYSQL_DSNS="$(dsn_at "$MYSQL_ORDER_0_PORT" order_db_0),$(dsn_at "$MYSQL_ORDER_1_PORT" order_db_1)" \
  MYSQL_REPLICA_DSNS="$(dsn_at "$MYSQL_ORDER_0_REPLICA_PORT" order_db_0),$(dsn_at "$MYSQL_ORDER_1_REPLICA_PORT" order_db_1)" WORKER_ID=2
start payment   8083 MYSQL_DSN="$(dsn payment_db)"   WORKER_ID=3
start gateway   8080

if [ "${1:-}" = "--" ]; then
  shift
  set +e
  "$@"
  code=$?
  set -e
  exit "$code"
fi

echo "gateway listening on http://127.0.0.1:8080 — press Ctrl-C to stop"
wait
