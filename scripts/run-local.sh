#!/usr/bin/env bash
# 在本机启动四个服务。依赖用 make up 启动。
#
# 用法：
#   scripts/run-local.sh            前台运行，Ctrl-C 停止全部
#   scripts/run-local.sh -- <命令>   启动服务，执行命令，然后停止全部；退出码取自该命令
#
# 可用环境变量覆盖：DB_PREFIX（库名前缀，默认空，即 order_db 等）、MYSQL_ROOT_PASSWORD、MYSQL_PORT、
# 以及各服务自己的配置（PAYMENT_TIMEOUT、SWEEP_INTERVAL 等）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin"
LOG_DIR="${LOG_DIR:-$ROOT/tmp/logs}"
DB_PREFIX="${DB_PREFIX:-}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"
MYSQL_PORT="${MYSQL_PORT:-3306}"

dsn() { echo "root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_PORT})/${DB_PREFIX}$1"; }

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
start order     8081 MYSQL_DSN="$(dsn order_db)"     WORKER_ID=2
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
