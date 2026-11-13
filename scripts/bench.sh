#!/usr/bin/env bash
# 下单吞吐压测（设计文档 docs/design/1b-high-concurrency.md 的 7.3.1），用来对比 1a 和 1b。
#
# 每一轮（一个并发档的一次运行）：重建独立的库 → 启动四个服务 → 灌 SKU →（1b）预热闸门 →
# load run → 等落库停止 → 从库里统计落库吞吐 → 停服务。每一轮都从空库开始，互不影响。
# 汇总写到 $BENCH_OUT/summary.tsv，每轮的报告、服务日志、CPU 占用在 $BENCH_OUT/<轮次>/ 下。
#
# 同一个脚本既能打当前分支的二进制，也能打 1a 的二进制：
#   make build && scripts/bench.sh                                   # 当前分支（1b）
#   BENCH_ARCH=1a BENCH_BIN_DIR=/path/to/1a/bin scripts/bench.sh     # 1a 的二进制
# 1a 没有 Kafka 和 Redis，传给它的相关环境变量会被忽略；库表结构不同，默认用 bench1a_* 库。
#
# 需要先 make up。只用 macOS 自带 bash 3.2 能跑的写法。需要本机有 mysql 客户端、jq、python3。
#
# 可用环境变量调整：
#   BENCH_ARCH            1a 或 1b，默认 1b（1b 才预热闸门、清理 Kafka 和 Redis 前缀）
#   BENCH_BIN_DIR         四个服务二进制所在目录，默认 ./bin
#   BENCH_DB_PREFIX       库名前缀，默认 1b 为 bench_，1a 为 bench1a_
#   BENCH_CONCURRENCY     并发档，空格分隔，默认 "50 200 800"
#   BENCH_REPEAT          每档运行次数，默认 2
#   BENCH_DURATION        每轮的提交窗口，默认 30s
#   BENCH_SKU_COUNT       SKU 个数（id 从 5001 起），默认 10
#   BENCH_STOCK           每个 SKU 的库存，默认 100000000（足够大，不会卖完）
#   BENCH_USERS           用户 id 范围，默认 100000（不设限购）
#   BENCH_QTY             每单数量，默认 1
#   BENCH_MODE            closed（默认）：虚拟用户提交后轮询到落库再下一单，是用户视角；
#                         fire：轮询时限设为 1ms，提交后只查一次就开始下一单，近似开环，
#                         测网关能受理多快，落库吞吐和 drain 改从库里统计。1a 返回 201 不轮询，两种模式相同。
#   BENCH_POLL_INTERVAL   轮询间隔，默认 50ms（fire 模式固定 1ms）
#   BENCH_QUIET_TIMEOUT   压测结束后等落库停止的上限（秒），默认 closed 180、fire 1200
#   BENCH_LOAD_ARGS       追加给 load run 的参数
#   BENCH_OUT             输出目录，默认 tmp/bench/<时间戳>-<arch>-<mode>
#   LOG_LEVEL             服务和压测工具的日志级别，默认 warn
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ARCH="${BENCH_ARCH:-1b}"
BIN="${BENCH_BIN_DIR:-$ROOT/bin}"
case "$BIN" in /*) ;; *) BIN="$ROOT/$BIN" ;; esac
case "$ARCH" in
  1a) DB_PREFIX="${BENCH_DB_PREFIX:-bench1a_}" ;;
  1b) DB_PREFIX="${BENCH_DB_PREFIX:-bench_}" ;;
  *) echo "BENCH_ARCH must be 1a or 1b" >&2; exit 2 ;;
esac
case "$DB_PREFIX" in
  bench*) ;;
  *) echo "BENCH_DB_PREFIX must start with bench (the script drops these databases)" >&2; exit 2 ;;
esac

MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
COMPOSE="docker compose -f $ROOT/deploy/compose/docker-compose.yml"

CONCURRENCY_LIST="${BENCH_CONCURRENCY:-50 200 800}"
REPEAT="${BENCH_REPEAT:-2}"
DURATION="${BENCH_DURATION:-30s}"
SKU_COUNT="${BENCH_SKU_COUNT:-10}"
STOCK="${BENCH_STOCK:-100000000}"
USERS="${BENCH_USERS:-100000}"
QTY="${BENCH_QTY:-1}"
MODE="${BENCH_MODE:-closed}"
POLL_INTERVAL="${BENCH_POLL_INTERVAL:-50ms}"
POLL_TIMEOUT=30s
QUIET_TIMEOUT="${BENCH_QUIET_TIMEOUT:-}"
case "$MODE" in
  closed) QUIET_TIMEOUT="${QUIET_TIMEOUT:-180}" ;;
  fire) POLL_INTERVAL=1ms POLL_TIMEOUT=1ms QUIET_TIMEOUT="${QUIET_TIMEOUT:-1200}" ;;
  *) echo "BENCH_MODE must be closed or fire" >&2; exit 2 ;;
esac

TS="$(date +%Y%m%d-%H%M%S)"
OUT="${BENCH_OUT:-$ROOT/tmp/bench/$TS-$ARCH-$MODE}"
mkdir -p "$OUT"
SUMMARY="$OUT/summary.tsv"

export LOG_LEVEL="${LOG_LEVEL:-warn}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:6379}"
export CACHE_REDIS_ADDR="${CACHE_REDIS_ADDR:-127.0.0.1:6380}"
# 支付时限设长，压测期间不会有订单超时关单。
export PAYMENT_TIMEOUT="${PAYMENT_TIMEOUT:-1h}"
# 1b：占用多久没被消费就回收，默认 1 分钟。fire 模式的积压会排上几分钟，默认值会把已受理的订单回收掉，
# 测出来的落库吞吐就不是消费能力了，所以压测时调长（要观察回收时显式设回 1m）。
export HOLD_REAP_GRACE="${HOLD_REAP_GRACE:-1h}"
export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"

log() { echo "[bench $(date +%H:%M:%S)] $*" | tee -a "$OUT/bench.log" >&2; }
sql() { mysql -h127.0.0.1 -P"$MYSQL_PORT" -uroot -N -B "$@"; }
dsn() { echo "root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_PORT})/${DB_PREFIX}$1"; }
now() { python3 -I -c 'import time; print("%.3f" % time.time())'; }

SERVICES="inventory order payment gateway"
for s in $SERVICES; do eval "pid_$s=0"; done
getv() { eval "echo \"\${$1}\""; }

port_of() {
  case "$1" in
    gateway) echo 8080 ;;
    order) echo 8081 ;;
    inventory) echo 8082 ;;
    payment) echo 8083 ;;
  esac
}

stop_services() {
  local s pid
  for s in $SERVICES; do
    pid="$(getv "pid_$s")"
    if [ "$pid" != 0 ]; then kill -TERM "$pid" 2>/dev/null || true; fi
  done
  for s in $SERVICES; do
    pid="$(getv "pid_$s")"
    if [ "$pid" != 0 ]; then
      for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
      kill -KILL "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      eval "pid_$s=0"
    fi
  done
}

cleanup_prefixes() {
  if [ "$ARCH" != 1b ]; then return 0; fi
  # 只删本脚本创建的 bench- 前缀的 topic 和 key，失败不影响压测。
  $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
    --delete --topic 'bench-.*' >/dev/null 2>&1 || true
  for r in redis redis-cache; do
    $COMPOSE exec -T "$r" sh -c "redis-cli --scan --pattern 'bench-*' | xargs -r -n 500 redis-cli DEL >/dev/null" \
      >/dev/null 2>&1 || true
  done
}

on_exit() {
  trap - EXIT INT TERM
  stop_services
  cleanup_prefixes
}
trap on_exit EXIT
trap 'log interrupted; exit 130' INT TERM

start_svc() {
  local s="$1" dir="$2" i pid
  case "$s" in
    inventory) env MYSQL_DSN="$(dsn inventory_db)" WORKER_ID=1 "$BIN/inventory" >"$dir/inventory.log" 2>&1 & ;;
    order) env MYSQL_DSNS="$(dsn order_db_0),$(dsn order_db_1)" WORKER_ID=2 "$BIN/order" >"$dir/order.log" 2>&1 & ;;
    payment) env MYSQL_DSN="$(dsn payment_db)" WORKER_ID=3 "$BIN/payment" >"$dir/payment.log" 2>&1 & ;;
    gateway) "$BIN/gateway" >"$dir/gateway.log" 2>&1 & ;;
  esac
  pid=$!
  eval "pid_$s=$pid"
  for i in $(seq 1 300); do
    if curl -fsS -m 1 "http://127.0.0.1:$(port_of "$s")/healthz" >/dev/null 2>&1; then return 0; fi
    if ! kill -0 "$pid" 2>/dev/null; then break; fi
    sleep 0.1
  done
  log "$s failed to start, last log lines:"
  tail -n 20 "$dir/$s.log" >&2
  return 1
}

# cpu_seconds 打印一个进程累计用掉的 CPU 秒数（ps 的 time 格式是 [hh:]mm:ss.cc）。
cpu_seconds() {
  ps -o time= -p "$1" 2>/dev/null | awk '{
    n = split($1, a, ":"); s = a[n]; if (n >= 2) s += a[n - 1] * 60; if (n >= 3) s += a[n - 2] * 3600
    printf "%.2f", s }'
}

# landed_count 打印进入 pending_payment 的订单数（不支付不取消，所以落库的订单都停在这个状态）。
# 订单分在两个库的 8 张分片表里，汇总时全部扫一遍。
all_orders_sql() {
  local cols="$1" tail="${2:-}" i out="" sep=""
  for i in 0 1 2 3 4 5 6 7; do
    out="$out${sep}SELECT $cols FROM ${DB_PREFIX}order_db_$((i % 2)).orders_$i $tail"
    sep=" UNION ALL "
  done
  echo "$out"
}
landed_count() { sql -e "SELECT COUNT(*) FROM ($(all_orders_sql status "WHERE status = 2")) o"; }

# ---------- 准备 ----------

for p in 8080 8081 8082 8083; do
  if curl -fsS -m 1 "http://127.0.0.1:$p/healthz" >/dev/null 2>&1; then
    echo "port $p is already serving; stop the other services first" >&2
    exit 2
  fi
done
for s in $SERVICES; do
  if [ ! -x "$BIN/$s" ]; then echo "missing $BIN/$s (run make build)" >&2; exit 2; fi
done
for t in mysql jq python3; do
  command -v "$t" >/dev/null || { echo "$t is required" >&2; exit 2; }
done
(cd "$ROOT" && go build -o "$OUT/load" ./test/load)
LOAD="$OUT/load"

SKU_LIST=""
VALUES=""
for i in $(seq 1 "$SKU_COUNT"); do
  sku=$((5000 + i))
  SKU_LIST="$SKU_LIST${SKU_LIST:+,}$sku"
  VALUES="$VALUES${VALUES:+,}($sku, 'bench $sku', 1990, $STOCK)"
done

{
  echo "# date $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "# machine $(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) cores, $(($(sysctl -n hw.memsize) / 1073741824)) GiB"
  echo "# arch $ARCH, bin $BIN, mode $MODE, duration $DURATION, skus $SKU_COUNT, stock $STOCK, users $USERS, qty $QTY, poll $POLL_INTERVAL/$POLL_TIMEOUT, payment timeout $PAYMENT_TIMEOUT, reap grace $HOLD_REAP_GRACE"
  printf 'arch\tmode\tc\trun\taccepted\taccept_per_s\taccept_p50_ms\taccept_p99_ms\t'
  printf 'landed_client\tland_p50_ms\tland_p99_ms\tdrain_client_s\t'
  printf 'landed_db\tland_db_per_s\tdrain_db_s\tpolls\terrors\tcpu_load\tcpu_gateway\tcpu_order\tcpu_inventory\tcpu_payment\n'
} >"$SUMMARY"
log "output $OUT"

# ---------- 每一轮 ----------

run_one() {
  local c="$1" n="$2" name dir run_ts s pid t0 t1 last stable deadline count prev
  name="c${c}-r${n}"
  dir="$OUT/$name"
  mkdir -p "$dir"
  run_ts="$(date +%s)"
  export KAFKA_PREFIX="bench-$run_ts-$c-$n."
  export REDIS_PREFIX="bench-$run_ts-$c-$n:"

  local q=""
  for db in inventory_db order_db_0 order_db_1 payment_db; do
    q="$q DROP DATABASE IF EXISTS ${DB_PREFIX}${db}; CREATE DATABASE ${DB_PREFIX}${db};"
  done
  sql -e "$q"

  # inventory 先起（它建表），灌入 SKU 后再起其他服务。
  start_svc inventory "$dir"
  sql "${DB_PREFIX}inventory_db" -e \
    "INSERT INTO sku_stock (sku_id, name, unit_price, available) VALUES $VALUES;"
  for s in order payment gateway; do start_svc "$s" "$dir"; done
  if [ "$ARCH" = 1b ]; then
    "$LOAD" warmup -skus "$SKU_LIST" -redis-addr "$REDIS_ADDR" -redis-prefix "$REDIS_PREFIX" >"$dir/warmup.txt"
  fi

  for s in $SERVICES; do eval "cpu0_$s=$(cpu_seconds "$(getv "pid_$s")")"; done
  log "$name: load run c=$c duration $DURATION"
  ( sleep 15; docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' >"$dir/docker-stats.txt" 2>&1 ) &
  local stats_pid=$!
  t0="$(now)"
  set +e
  # shellcheck disable=SC2086 # BENCH_LOAD_ARGS 故意按空格拆分
  /usr/bin/time -p "$LOAD" run -c "$c" -duration "$DURATION" -skus "$SKU_LIST" -users "$USERS" -qty "$QTY" \
    -pay 0 -cancel 0 -poll-interval "$POLL_INTERVAL" -poll-timeout "$POLL_TIMEOUT" \
    -log-level "$LOG_LEVEL" -progress 0 -run-id "b$run_ts$c$n" \
    -ledger "$dir/ledger.jsonl" -json "$dir/report.json" ${BENCH_LOAD_ARGS:-} \
    >"$dir/report.txt" 2>"$dir/load.log"
  local code=$?
  set -e
  t1="$(now)"
  for s in $SERVICES; do eval "cpu1_$s=$(cpu_seconds "$(getv "pid_$s")")"; done
  wait "$stats_pid" 2>/dev/null || true
  if [ "$code" != 0 ]; then log "$name: load run exited with $code (see $dir/load.log)"; fi

  # 等落库停止：pending_payment 的数量连续 3 秒不变（1a 立即满足）。
  deadline=$((SECONDS + QUIET_TIMEOUT))
  prev=-1
  stable=0
  while [ "$SECONDS" -lt "$deadline" ]; do
    count="$(landed_count)"
    if [ "$count" = "$prev" ]; then stable=$((stable + 1)); else stable=0; fi
    if [ "$stable" -ge 3 ]; then break; fi
    prev="$count"
    sleep 1
  done
  sql -e "SELECT COUNT(*), UNIX_TIMESTAMP(MIN(updated_at)), UNIX_TIMESTAMP(MAX(updated_at))
    FROM ($(all_orders_sql "status, updated_at" "WHERE status = 2")) o" >"$dir/db-landed.txt"
  sql -e "SELECT status, COUNT(*) FROM ($(all_orders_sql status)) o GROUP BY status" >"$dir/db-status.txt"
  stop_services

  local wall load_user load_sys
  wall="$(awk '/^real/ {print $2}' "$dir/load.log")"
  load_user="$(awk '/^user/ {print $2}' "$dir/load.log")"
  load_sys="$(awk '/^sys/ {print $2}' "$dir/load.log")"
  {
    for s in $SERVICES; do
      echo "$s $(getv "cpu0_$s") $(getv "cpu1_$s")"
    done
    echo "load 0 $(awk -v u="$load_user" -v s="$load_sys" 'BEGIN { printf "%.2f", u + s }')"
  } >"$dir/cpu.txt"

  # 汇总：CPU 是压测期间平均占用的核数；落库吞吐和 drain 以库里的 updated_at 为准。
  python3 -I - "$dir" "$ARCH" "$MODE" "$c" "$n" "$t0" "$t1" "$wall" >>"$SUMMARY" <<'PY'
import json, sys, datetime
d, arch, mode, c, n, t0, t1, wall = sys.argv[1:]
t0, t1, wall = float(t0), float(t1), float(wall or 0) or (float(t1) - float(t0))
try:
    r = json.load(open(d + "/report.json"))
except Exception:
    r = {"accepted": {"latency": {}}, "landed": {"latency": {}}, "orders": {}}
cpu = {}
for line in open(d + "/cpu.txt"):
    name, a, b = line.split()
    cpu[name] = (float(b) - float(a)) / wall if wall > 0 else 0
cnt, lo, hi = (open(d + "/db-landed.txt").read().split() + ["NULL"] * 3)[:3]
cnt = int(cnt)
acc = r.get("accepted", {})
start = r.get("started_at")
last_accept = None
if start:
    st = datetime.datetime.fromisoformat(start.replace("Z", "+00:00")).timestamp()
    last_accept = st + acc.get("window_seconds", 0)
else:
    st = t0
rate = drain = 0.0
if cnt and hi != "NULL":
    rate = cnt / (float(hi) - st) if float(hi) > st else 0
    drain = max(0.0, float(hi) - last_accept) if last_accept else 0
errs = sum(v for op in r.get("requests", {}).values() for k, v in op.items() if k not in ("ok", "rejected"))
land = r.get("landed", {})
row = [arch, mode, c, n, acc.get("count", 0), acc.get("per_second", 0),
       acc.get("latency", {}).get("p50_ms", 0), acc.get("latency", {}).get("p99_ms", 0),
       land.get("count", 0), land.get("latency", {}).get("p50_ms", 0), land.get("latency", {}).get("p99_ms", 0),
       round(r.get("drain_seconds", 0), 3), cnt, round(rate, 1), round(drain, 3),
       r.get("poll_requests", 0), errs, round(cpu.get("load", 0), 2)] + \
      [round(cpu.get(s, 0), 2) for s in ("gateway", "order", "inventory", "payment")]
print("\t".join(str(x) for x in row))
PY
  log "$name: $(tail -n 1 "$SUMMARY")"
  cleanup_prefixes
}

cleanup_prefixes
for c in $CONCURRENCY_LIST; do
  for n in $(seq 1 "$REPEAT"); do
    run_one "$c" "$n"
  done
done
log "done, summary $SUMMARY"
grep -v '^#' "$SUMMARY" | column -t -s "$(printf '\t')" >&2 || true
