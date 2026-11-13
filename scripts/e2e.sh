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
for db in inventory_db order_db_0 order_db_1 payment_db; do
  sql+="DROP DATABASE IF EXISTS e2e_${db}; CREATE DATABASE e2e_${db};"
done
if [ -n "${CI:-}" ]; then
  # CI 里 MySQL 是 service 容器，不是 compose 起的，用 runner 自带的客户端连。
  mysql -h127.0.0.1 -P"$MYSQL_PORT" -uroot -p"$MYSQL_ROOT_PASSWORD" -e "$sql" 2>/dev/null
else
  "${COMPOSE[@]}" exec -T mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -e "$sql" 2>/dev/null
fi

export DB_PREFIX=e2e_

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

cd "$ROOT/test/e2e"
"$ROOT/scripts/run-local.sh" -- go test -race -count=1 -v ./... "$@"
