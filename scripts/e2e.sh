#!/usr/bin/env bash
# 端到端测试：在独立的一组库（e2e_*）上启动四个服务，对网关跑 test/e2e 的测试。
# 需要先 make up 和 make build。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
COMPOSE=(docker compose -f "$ROOT/deploy/compose/docker-compose.yml")

# 每次从干净的库开始，结果不受上一次运行影响。
sql=""
for db in inventory_db order_db payment_db; do
  sql+="DROP DATABASE IF EXISTS e2e_${db}; CREATE DATABASE e2e_${db};"
done
if [ -n "${CI:-}" ]; then
  # CI 里 MySQL 是 service 容器，不是 compose 起的，用 runner 自带的客户端连。
  mysql -h127.0.0.1 -P"$MYSQL_PORT" -uroot -p"$MYSQL_ROOT_PASSWORD" -e "$sql" 2>/dev/null
else
  "${COMPOSE[@]}" exec -T mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -e "$sql" 2>/dev/null
fi

export DB_PREFIX=e2e_
export LOG_DIR="$ROOT/tmp/e2e-logs"
# 缩短各时限，让超时关单这类场景能在几秒内测完。
export PAYMENT_TIMEOUT=3s
export SWEEP_INTERVAL=200ms
export SETTLE_GRACE=500ms
export LOG_LEVEL=warn

export E2E_GATEWAY_URL=http://127.0.0.1:8080
export E2E_INVENTORY_DSN="root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_PORT})/e2e_inventory_db"
export E2E_PAYMENT_TIMEOUT=3s

cd "$ROOT/test/e2e"
"$ROOT/scripts/run-local.sh" -- go test -race -count=1 -v ./... "$@"
