#!/usr/bin/env bash
# 给 scripts/expand.sh 的 CDC_WAIT_CMD 用：两个旧 CDC 源（order-0、order-1）的位点都已越过围栏的 GTID 时返回 0。
#
# 环境里要有 FENCE_GTID_0、FENCE_GTID_1（expand.sh 的 wait 会导出）。位点用 cdc 二进制的 -print-checkpoint
# 读出（存在 Kafka 里），是否越过用 MySQL 的 GTID_SUBSET(围栏的集合, 位点的集合) 判断。
# 原因见设计文档 1c 的 5.8：变更 topic 按订单号压缩，旧源必须先发完围栏之前的变更，新库才能开始写。
#
# 可用环境变量：CDC_BIN（默认 ./bin/cdc）、KAFKA_BROKERS、KAFKA_PREFIX、MYSQL_ROOT_PASSWORD。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CDC_BIN="${CDC_BIN:-$ROOT/bin/cdc}"
COMPOSE_FILE="$ROOT/deploy/compose/docker-compose.yml"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"

for j in 0 1; do
  fence_var="FENCE_GTID_$j"
  fence="${!fence_var:-}"
  [ -n "$fence" ] || { echo "$fence_var 没有设置" >&2; exit 2; }
  # cdc 启动时总要校验数据源配置（-reset-checkpoint 也一样），读位点用不到它们，给占位值。
  cp="$(CDC_SOURCE_NAMES="order-$j" CDC_SOURCE_DSNS=unused CDC_SERVER_IDS=1 \
    "$CDC_BIN" -print-checkpoint "order-$j" 2>/dev/null)" || { echo "order-$j：读不到位点" >&2; exit 1; }
  ok="$(docker compose -f "$COMPOSE_FILE" exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" "mysql-order-$j" \
    mysql -uroot -N -B -e "SELECT GTID_SUBSET('$fence', '$cp')" 2>/dev/null)"
  [ "$ok" = 1 ] || { echo "order-$j：CDC 还没越过围栏" >&2; exit 1; }
done
