#!/usr/bin/env bash
# 在本机启动六个服务。依赖用 make up 启动。
#
# 用法：
#   scripts/run-local.sh            前台运行，Ctrl-C 停止全部
#   scripts/run-local.sh -- <命令>   启动服务，执行命令，然后停止全部；退出码取自该命令
#
# 可用环境变量覆盖：DB_PREFIX（库名前缀，默认空，即 order_db_0 等）、MYSQL_ROOT_PASSWORD、MYSQL_PORT、
# 以及 order 分片的四个端口 MYSQL_ORDER_{0,1}_PORT、MYSQL_ORDER_{0,1}_REPLICA_PORT，
# 和各服务自己的配置（PAYMENT_TIMEOUT、SWEEP_INTERVAL 等）。
# ORDER_DBS=4 表示订单库已经扩成 4 个（scripts/expand.sh 做完之后，ADR-0015）：order 连 4 主 4 从，
# cdc 读 4 个源。默认 2。
# order 带 ARCHIVE_DSN，指向单实例 mysql 上的 order_archive_db（冷库，ADR-0016）：启动时自己建表，热库查不到的旧订单去冷库查。
# gateway 的限流（ADR-0019）默认开启，按 IP（进程内）和按用户加接口（Redis）。按 IP 的限额是每秒 200，
# 从本机用很多用户号压测会被限住，现象是吞吐上不去或随机得到 429；压测和测试脚本会自己设置下面的变量。
# RATE_LIMIT（on/off）、RATE_LIMIT_IP、RATE_LIMIT_USER、RATE_LIMIT_ROUTE、TRUSTED_PROXIES：已经设置（并 export）就原样传给 gateway，
# 没设就由 gateway 用自己的默认值。
# RATE_LIMIT_REDIS_ADDR 默认指向缓存实例（和其他服务的 CACHE_REDIS_ADDR 同一个），RATE_LIMIT_REDIS_PREFIX 默认跟随 REDIS_PREFIX；
# 设为空字符串表示限流不用 Redis。
# 服务之间的 gRPC 调用经过各自的 Envoy 边车（ADR-0020，make up 会启动它们）：gateway、order、payment 的下游地址
# 指向自己边车的出站端口，熔断、并发上限、路由超时都在边车的配置里（deploy/envoy/）。SIDECAR=off 时直连
# （不经过边车，也就没有这些保护），用来对比或者在边车没起来的环境里跑。
# 冷库要先建好（make up 会建；e2e、chaos、bench 各自建带前缀的）。配置和数据对不上时 order 会因为 shard_meta 不符而拒绝启动。
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

ORDER_DBS="${ORDER_DBS:-2}"
MYSQL_ORDER_2_PORT="${MYSQL_ORDER_2_PORT:-3331}"
MYSQL_ORDER_2_REPLICA_PORT="${MYSQL_ORDER_2_REPLICA_PORT:-3332}"
MYSQL_ORDER_3_PORT="${MYSQL_ORDER_3_PORT:-3341}"
MYSQL_ORDER_3_REPLICA_PORT="${MYSQL_ORDER_3_REPLICA_PORT:-3342}"

dsn_at() { echo "root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:$1)/${DB_PREFIX}$2"; }
dsn() { dsn_at "$MYSQL_PORT" "$1"; }

# CDC 读 binlog 用的账号，由 scripts/mysql-replication.sh 在 order 的主库上创建。
MYSQL_CDC_PASSWORD="${MYSQL_CDC_PASSWORD:-oppenheimer-cdc}"
cdc_dsn() { echo "cdc:${MYSQL_CDC_PASSWORD}@tcp(127.0.0.1:$1)/${DB_PREFIX}$2"; }

# Kafka 和 Redis 的地址对所有服务相同，直接导出，子进程会继承。
# KAFKA_PREFIX、REDIS_PREFIX 默认为空；端到端测试会设置它们，与手动运行的环境隔离。
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:${KAFKA_PORT:-9092}}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:${REDIS_PORT:-6379}}"
# 查询缓存用单独的实例（compose 里的 redis-cache）。服务在没有设置 CACHE_REDIS_ADDR 时退回 REDIS_ADDR。
export CACHE_REDIS_ADDR="${CACHE_REDIS_ADDR:-127.0.0.1:${CACHE_REDIS_PORT:-6380}}"
export KAFKA_PREFIX="${KAFKA_PREFIX:-}"
export REDIS_PREFIX="${REDIS_PREFIX:-}"
# 限流状态放在缓存实例里（丢了只是桶重新装满，可以接受）。用 - 而不是 :-，让调用者能显式设为空来关掉 Redis。
export RATE_LIMIT_REDIS_ADDR="${RATE_LIMIT_REDIS_ADDR-$CACHE_REDIS_ADDR}"
export RATE_LIMIT_REDIS_PREFIX="${RATE_LIMIT_REDIS_PREFIX-$REDIS_PREFIX}"

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

# 每个服务调用下游时连自己边车的出站端口（scripts/lib/sidecar.sh：端口、边车健康检查、SIDECAR=off 直连）。
# shellcheck source=lib/sidecar.sh
. "$ROOT/scripts/lib/sidecar.sh"
sidecar_setup
order_to_inventory="$SC_ORDER_INVENTORY"
payment_to_order="$SC_PAYMENT_ORDER"
gateway_addrs="$SC_GATEWAY_ADDRS"

# 启动顺序按依赖方向：被依赖的先起。
start inventory 8082 MYSQL_DSN="$(dsn inventory_db)" WORKER_ID=1
# MYSQL_REPLICA_DSNS 的个数和顺序与 MYSQL_DSNS 一一对应。
order_dsns="$(dsn_at "$MYSQL_ORDER_0_PORT" order_db_0),$(dsn_at "$MYSQL_ORDER_1_PORT" order_db_1)"
order_replica_dsns="$(dsn_at "$MYSQL_ORDER_0_REPLICA_PORT" order_db_0),$(dsn_at "$MYSQL_ORDER_1_REPLICA_PORT" order_db_1)"
cdc_names=order-0,order-1
cdc_ids=1011,1021
cdc_dsns="$(cdc_dsn "$MYSQL_ORDER_0_REPLICA_PORT" order_db_0),$(cdc_dsn "$MYSQL_ORDER_1_REPLICA_PORT" order_db_1)"
case "$ORDER_DBS" in
2) ;;
4)
  order_dsns="$order_dsns,$(dsn_at "$MYSQL_ORDER_2_PORT" order_db_2),$(dsn_at "$MYSQL_ORDER_3_PORT" order_db_3)"
  order_replica_dsns="$order_replica_dsns,$(dsn_at "$MYSQL_ORDER_2_REPLICA_PORT" order_db_2),$(dsn_at "$MYSQL_ORDER_3_REPLICA_PORT" order_db_3)"
  cdc_names="$cdc_names,order-2,order-3"
  cdc_ids="$cdc_ids,1031,1041"
  cdc_dsns="$cdc_dsns,$(cdc_dsn "$MYSQL_ORDER_2_REPLICA_PORT" order_db_2),$(cdc_dsn "$MYSQL_ORDER_3_REPLICA_PORT" order_db_3)"
  ;;
*)
  echo "ORDER_DBS must be 2 or 4, got $ORDER_DBS" >&2
  exit 2
  ;;
esac
start order     8081 MYSQL_DSNS="$order_dsns" MYSQL_REPLICA_DSNS="$order_replica_dsns" ARCHIVE_DSN="$(dsn order_archive_db)" WORKER_ID=2 INVENTORY_ADDR="$order_to_inventory"
start payment   8083 MYSQL_DSN="$(dsn payment_db)"   WORKER_ID=3 ORDER_ADDR="$payment_to_order"
# 商家维度的订单索引（ADR-0014）。两个索引库在单实例 mysql 上。
start orderindex 8084 MYSQL_DSNS="$(dsn merchant_index_db_0),$(dsn merchant_index_db_1)"
# CDC 从 order 的两个从库读 binlog。排在 order 之后启动只是为了第一次全量时表已经建好；
# 表在它启动之后才建也没关系，建表之后的写入都在 binlog 里。server-id 登记在 docs/infra/versions.md。
start cdc 8085 CDC_SOURCE_NAMES="$cdc_names" CDC_SERVER_IDS="$cdc_ids" CDC_SOURCE_DSNS="$cdc_dsns"
# shellcheck disable=SC2086 # 有意按空格拆成几个 KEY=VALUE
start gateway   8080 $gateway_addrs

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
