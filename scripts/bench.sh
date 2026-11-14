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
#
# 两组可选的对比，默认都不跑；不设下面这些变量时，脚本的行为和输出与加它们之前完全相同。
#   BENCH_SWEEP           默认 1。设为 0 跳过上面按并发档的压测，只跑下面开启的对比
#   BENCH_OPEN            设为 1 加一组开环档位：load run -mode open 按固定的到达速率发下单请求，
#                         不管上一个有没有返回。每档输出计划速率、受理速率、被背压拒绝（429 BACKLOGGED）的比例、
#                         受理延迟（从计划发出的时刻算）的 p50/p99、未发出数（在途请求到上限而没发出的），
#                         看受理端在多大的到达速率下开始拒绝、延迟是否稳住。汇总在 $BENCH_OUT/summary-open.tsv
#   BENCH_OPEN_RATES      开环的速率档（请求/秒），空格分隔，默认 "200 500 1000 2000 4000"
#   BENCH_OPEN_REPEAT     每档运行次数，默认 1
#   BENCH_OPEN_MAX_IN_FLIGHT  开环在途请求上限，默认 2000
#   BENCH_OPEN_DRAIN      开环压测结束后轮询受理订单最终状态的时间上限，默认 10s（落库吞吐另从库里统计）
#   BENCH_POLL            设为 1 加一个“轮询压力下的落库吞吐”对比：同样的开环下单压力跑两次，
#                         一次没有额外查询压力，一次同时用 load poll 对最近受理的订单高频查询。
#                         落库吞吐沿用上面的办法（库里 pending_payment 的数量除以第一个请求到最后一次落库的时间），
#                         输出两次的落库吞吐和它们的比值。汇总在 $BENCH_OUT/summary-poll.tsv
#   BENCH_POLL_RATE       对比里的开环下单速率，默认 2000
#   BENCH_POLL_DURATION   对比里每次的下单时长，默认同 BENCH_DURATION
#   BENCH_POLL_CONCURRENCY  load poll 的 worker 数，默认 64
#   BENCH_POLL_INTERVAL_REQ  load poll 每个 worker 两次查询的间隔，默认 10ms（约 c/interval 次查询每秒）
#   BENCH_POLL_RECENT     load poll 从最近受理的多少个订单里随机选，默认 2000
#   BENCH_POLL_REPEAT     对比重复次数（每次含无查询和有查询各一轮），默认 1
#   BENCH_OPEN_QUIET_TIMEOUT  开环和对比各轮结束后等落库停止的上限（秒），默认 1200
#
# gateway 的限流（ADR-0019）：上面所有的轮次都以 RATE_LIMIT=off 启动 gateway，量的是下单和落库的吞吐，
# 限流会把结果盖住。下面的对比单独量限流本身：
#   BENCH_RATELIMIT       设为 1 加一组限流对比，默认不跑。同样的开环下单压力跑两轮：
#                         RATE_LIMIT=on（默认的按用户限额，按 IP 关掉，限流的 Redis 是缓存实例）和 RATE_LIMIT=off 作对照。
#                         压力只用很少的用户号，速率远超这些用户的配额总和（默认 20 个用户 x 每秒 5 单 = 100 单/秒，速率 1000），
#                         预期开着限流时受理速率稳定在配额附近、其余被限流（429 RATE_LIMITED）、受理延迟不升高。
#                         每轮输出计划速率、受理速率、被限流和被背压拒绝（429 BACKLOGGED）的比例、受理延迟 p50/p99、落库吞吐。
#                         汇总在 $BENCH_OUT/summary-ratelimit.tsv
#   BENCH_RATELIMIT_RATE      对比里的开环下单速率，默认 1000
#   BENCH_RATELIMIT_USERS     对比里的用户数（用户号取 [1, N]），默认 20
#   BENCH_RATELIMIT_DURATION  对比里每轮的下单时长，默认同 BENCH_DURATION
#   BENCH_RATELIMIT_REPEAT    对比重复次数（每次含限流开和关各一轮），默认 1
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
# 开环和轮询对比的参数（只在 BENCH_OPEN=1 或 BENCH_POLL=1 时用到）。
SWEEP="${BENCH_SWEEP:-1}"
OPEN="${BENCH_OPEN:-0}"
POLL="${BENCH_POLL:-0}"
OPEN_RATES="${BENCH_OPEN_RATES:-200 500 1000 2000 4000}"
OPEN_REPEAT="${BENCH_OPEN_REPEAT:-1}"
OPEN_MAX_IN_FLIGHT="${BENCH_OPEN_MAX_IN_FLIGHT:-2000}"
OPEN_DRAIN="${BENCH_OPEN_DRAIN:-10s}"
OPEN_POLL_INTERVAL="${BENCH_POLL_INTERVAL:-50ms}"
OPEN_QUIET_TIMEOUT="${BENCH_OPEN_QUIET_TIMEOUT:-1200}"
POLL_RATE="${BENCH_POLL_RATE:-2000}"
POLL_DURATION="${BENCH_POLL_DURATION:-}"
POLL_CONC="${BENCH_POLL_CONCURRENCY:-64}"
POLL_REQ_INTERVAL="${BENCH_POLL_INTERVAL_REQ:-10ms}"
POLL_RECENT="${BENCH_POLL_RECENT:-2000}"
POLL_REPEAT="${BENCH_POLL_REPEAT:-1}"
RATELIMIT="${BENCH_RATELIMIT:-0}"
RL_RATE="${BENCH_RATELIMIT_RATE:-1000}"
RL_USERS="${BENCH_RATELIMIT_USERS:-20}"
RL_DURATION="${BENCH_RATELIMIT_DURATION:-}"
RL_REPEAT="${BENCH_RATELIMIT_REPEAT:-1}"
case "$MODE" in
  closed) QUIET_TIMEOUT="${QUIET_TIMEOUT:-180}" ;;
  fire) POLL_INTERVAL=1ms POLL_TIMEOUT=1ms QUIET_TIMEOUT="${QUIET_TIMEOUT:-1200}" ;;
  *) echo "BENCH_MODE must be closed or fire" >&2; exit 2 ;;
esac

for v in "$SWEEP" "$OPEN" "$POLL"; do
  case "$v" in 0|1) ;; *) echo "BENCH_SWEEP, BENCH_OPEN and BENCH_POLL must be 0 or 1" >&2; exit 2 ;; esac
done
case "$RATELIMIT" in 0|1) ;; *) echo "BENCH_RATELIMIT must be 0 or 1" >&2; exit 2 ;; esac
POLL_DURATION="${POLL_DURATION:-$DURATION}"
RL_DURATION="${RL_DURATION:-$DURATION}"

TS="$(date +%Y%m%d-%H%M%S)"
OUT="${BENCH_OUT:-$ROOT/tmp/bench/$TS-$ARCH-$MODE}"
mkdir -p "$OUT"
SUMMARY="$OUT/summary.tsv"
SUMMARY_OPEN="$OUT/summary-open.tsv"
SUMMARY_POLL="$OUT/summary-poll.tsv"
SUMMARY_RL="$OUT/summary-ratelimit.tsv"

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

# gateway 的限流开关，默认关；BENCH_RATELIMIT 的对比轮次在启动前把它设为 on，轮次结束后设回 off。
GW_RATE_LIMIT=off

start_svc() {
  local s="$1" dir="$2" i pid
  case "$s" in
    inventory) env MYSQL_DSN="$(dsn inventory_db)" WORKER_ID=1 "$BIN/inventory" >"$dir/inventory.log" 2>&1 & ;;
    order) env MYSQL_DSNS="$(dsn order_db_0),$(dsn order_db_1)" ARCHIVE_DSN="$(dsn order_archive_db)" WORKER_ID=2 "$BIN/order" >"$dir/order.log" 2>&1 & ;;
    payment) env MYSQL_DSN="$(dsn payment_db)" WORKER_ID=3 "$BIN/payment" >"$dir/payment.log" 2>&1 & ;;
    # gateway 默认以 RATE_LIMIT=off 启动：这个脚本量的是下单和落库的吞吐，默认的限流（按 IP 每秒 200）
    # 会把结果盖住，看起来像“吞吐上不去”。只有 BENCH_RATELIMIT 的对比轮次（GW_RATE_LIMIT=on）才开限流，
    # 那时按 IP 的限额仍然关掉（压力全部来自 127.0.0.1，开着就只剩按 IP 的效果），按用户的用默认值，
    # 状态放在缓存实例上（和服务里缓存用的是同一个 Redis，前缀跟本轮的 REDIS_PREFIX，轮次结束时一并清理）。
    # 调用者 shell 里残留的 RATE_LIMIT_USER、RATE_LIMIT_ROUTE 会改变限额，这里清掉。
    gateway) env -u RATE_LIMIT_USER -u RATE_LIMIT_ROUTE \
      RATE_LIMIT="$GW_RATE_LIMIT" RATE_LIMIT_IP=off \
      RATE_LIMIT_REDIS_ADDR="$CACHE_REDIS_ADDR" RATE_LIMIT_REDIS_PREFIX="$REDIS_PREFIX" \
      "$BIN/gateway" >"$dir/gateway.log" 2>&1 & ;;
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

# summarize_open 把开环或轮询对比的一轮汇总成一行：开环写 summary-open.tsv，对比写 summary-poll.tsv。
# 落库吞吐的算法和上面按并发档的一样：库里 pending_payment 的数量，除以从压测开始到最后一次落库（updated_at）的时间。
summarize_open() {
  local dir="$1" n="$2" t0="$3" out="$SUMMARY_OPEN" label="$ROUND_RATE"
  if [ "$ROUND_KIND" = poll ]; then
    out="$SUMMARY_POLL"
    if [ "$ROUND_POLLER" = 1 ]; then label=polled; else label=baseline; fi
  elif [ "$ROUND_KIND" = rl ]; then
    out="$SUMMARY_RL"
    label="$GW_RATE_LIMIT"
  fi
  python3 -I - "$ROUND_KIND" "$dir" "$label" "$n" "$t0" >>"$out" <<'PY'
import json, sys, datetime
kind, d, label, n, t0 = sys.argv[1:]
try:
    r = json.load(open(d + "/report.json"))
except Exception:
    r = {}
o = r.get("open") or {}
oc = o.get("outcomes", {})
sent = o.get("sent", 0)
win = o.get("send_window_seconds", 0)
acc = oc.get("accepted", 0)
acc_rate = round(acc / win, 1) if win else 0
back_pct = round(100.0 * oc.get("backlogged", 0) / sent, 2) if sent else 0
lat = o.get("accepted_from_schedule", {})
cnt, lo, hi = (open(d + "/db-landed.txt").read().split() + ["NULL"] * 3)[:3]
cnt = int(cnt)
start = r.get("started_at")
st = datetime.datetime.fromisoformat(start.replace("Z", "+00:00")).timestamp() if start else float(t0)
land = round(cnt / (float(hi) - st), 1) if cnt and hi != "NULL" and float(hi) > st else 0
if kind == "rl":
    lim_pct = round(100.0 * oc.get("rate_limited", 0) / sent, 2) if sent else 0
    row = [label, n, o.get("rate", 0), o.get("planned", 0), sent, acc, acc_rate, lim_pct, back_pct,
           lat.get("p50_ms", 0), lat.get("p99_ms", 0), cnt, land]
elif kind == "open":
    row = [label, n, o.get("planned", 0), sent, o.get("dropped", 0), acc, acc_rate, back_pct,
           oc.get("rate_limited", 0), oc.get("timeout", 0), lat.get("p50_ms", 0), lat.get("p99_ms", 0), cnt, land]
else:
    try:
        p = json.load(open(d + "/poll.json"))
    except Exception:
        p = {}
    pl = p.get("latency", {})
    row = [label, n, o.get("planned", 0), acc, acc_rate, cnt, land,
           p.get("requests", 0), p.get("per_second", 0), pl.get("p50_ms", 0), pl.get("p99_ms", 0)]
print("\t".join(str(x) for x in row))
PY
  log "$(basename "$dir"): $(tail -n 1 "$out")"
}


# run_one 跑一轮。c 是并发数（BENCH_OPEN、BENCH_POLL 的轮次里只是个标签，用来区分 topic 前缀和目录名）。
# 调用方可以在调用前设置 ROUND_KIND（sweep、open、poll，默认 sweep）、ROUND_NAME、ROUND_RATE、ROUND_POLLER，
# 不设时就是原来按并发档的一轮。
ROUND_KIND=sweep
ROUND_NAME=""
ROUND_RATE=""
ROUND_POLLER=0
ROUND_USERS=""
run_one() {
  local c="$1" n="$2" name dir run_ts s pid t0 t1 last stable deadline count prev quiet="$QUIET_TIMEOUT" poll_pid=""
  name="${ROUND_NAME:-c${c}-r${n}}"
  if [ "$ROUND_KIND" != sweep ]; then quiet="$OPEN_QUIET_TIMEOUT"; fi
  local users="${ROUND_USERS:-$USERS}"
  dir="$OUT/$name"
  mkdir -p "$dir"
  run_ts="$(date +%s)"
  export KAFKA_PREFIX="bench-$run_ts-$c-$n."
  export REDIS_PREFIX="bench-$run_ts-$c-$n:"

  local q=""
  for db in inventory_db order_db_0 order_db_1 order_archive_db payment_db; do
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
  case "$ROUND_KIND" in
    sweep) log "$name: load run c=$c duration $DURATION" ;;
    open) log "$name: load run -mode open rate $ROUND_RATE duration $DURATION" ;;
    poll) log "$name: load run -mode open rate $ROUND_RATE duration $POLL_DURATION, poller $ROUND_POLLER" ;;
    rl) log "$name: load run -mode open rate $ROUND_RATE duration $RL_DURATION, users $users, rate limit $GW_RATE_LIMIT" ;;
  esac
  ( sleep 15; docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' >"$dir/docker-stats.txt" 2>&1 ) &
  local stats_pid=$!
  t0="$(now)"
  set +e
  case "$ROUND_KIND" in
    sweep)
      # shellcheck disable=SC2086 # BENCH_LOAD_ARGS 故意按空格拆分
      /usr/bin/time -p "$LOAD" run -c "$c" -duration "$DURATION" -skus "$SKU_LIST" -users "$USERS" -qty "$QTY" \
        -pay 0 -cancel 0 -poll-interval "$POLL_INTERVAL" -poll-timeout "$POLL_TIMEOUT" \
        -log-level "$LOG_LEVEL" -progress 0 -run-id "b$run_ts$c$n" \
        -ledger "$dir/ledger.jsonl" -json "$dir/report.json" ${BENCH_LOAD_ARGS:-} \
        >"$dir/report.txt" 2>"$dir/load.log"
      ;;
    open|poll|rl)
      local dur="$DURATION" drain="$OPEN_DRAIN"
      if [ "$ROUND_KIND" = poll ]; then
        # 对比的两轮都只在下单结束后查一下（drain 1s），避免收尾阶段的轮询变成额外的查询压力。
        dur="$POLL_DURATION" drain=1s
      fi
      if [ "$ROUND_KIND" = rl ]; then
        # 开限流时用户号很少，收尾轮询自己的 getOrder 也会被限流，drain 没有意义，查一下就结束。
        dur="$RL_DURATION" drain=1s
      fi
      if [ "$ROUND_POLLER" = 1 ]; then
        # 先于下单起动：load poll 会等账本出现，并随账本的增长追最近受理的订单。
        "$LOAD" poll -orders-from "$dir/ledger.jsonl" -c "$POLL_CONC" -interval "$POLL_REQ_INTERVAL" \
          -recent "$POLL_RECENT" -duration "$dur" -log-level "$LOG_LEVEL" -json "$dir/poll.json" \
          >"$dir/poll.txt" 2>"$dir/poll.log" &
        poll_pid=$!
      fi
      # shellcheck disable=SC2086 # BENCH_LOAD_ARGS 故意按空格拆分
      /usr/bin/time -p "$LOAD" run -mode open -rate "$ROUND_RATE" -max-in-flight "$OPEN_MAX_IN_FLIGHT" \
        -duration "$dur" -drain "$drain" -skus "$SKU_LIST" -users "$users" -qty "$QTY" \
        -poll-interval "$OPEN_POLL_INTERVAL" \
        -log-level "$LOG_LEVEL" -progress 0 -run-id "b$run_ts$c$n" \
        -ledger "$dir/ledger.jsonl" -json "$dir/report.json" ${BENCH_LOAD_ARGS:-} \
        >"$dir/report.txt" 2>"$dir/load.log"
      ;;
  esac
  local code=$?
  if [ -n "$poll_pid" ]; then wait "$poll_pid" || log "$name: load poll failed (see $dir/poll.log)"; fi
  set -e
  t1="$(now)"
  for s in $SERVICES; do eval "cpu1_$s=$(cpu_seconds "$(getv "pid_$s")")"; done
  wait "$stats_pid" 2>/dev/null || true
  if [ "$code" != 0 ]; then log "$name: load run exited with $code (see $dir/load.log)"; fi

  # 等落库停止：pending_payment 的数量连续 3 秒不变（1a 立即满足）。
  deadline=$((SECONDS + quiet))
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

  case "$ROUND_KIND" in
  sweep)
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
    ;;
  open|poll|rl)
    summarize_open "$dir" "$n" "$t0"
    ;;
  esac
  cleanup_prefixes
}

cleanup_prefixes
if [ "$SWEEP" = 1 ]; then
  for c in $CONCURRENCY_LIST; do
    for n in $(seq 1 "$REPEAT"); do
      run_one "$c" "$n"
    done
  done
  log "done, summary $SUMMARY"
  grep -v '^#' "$SUMMARY" | column -t -s "$(printf '\t')" >&2 || true
fi

# 开环档位：每档固定时长、固定到达速率。
if [ "$OPEN" = 1 ]; then
  {
    echo "# open loop: duration $DURATION, max in flight $OPEN_MAX_IN_FLIGHT, drain $OPEN_DRAIN; latency is from the planned send time"
    printf 'rate\trun\tplanned\tsent\tdropped\taccepted\taccept_per_s\tbacklogged_pct\trate_limited\ttimeout\taccept_p50_ms\taccept_p99_ms\tlanded_db\tland_db_per_s\n'
  } >"$SUMMARY_OPEN"
  ROUND_KIND=open
  for r in $OPEN_RATES; do
    for n in $(seq 1 "$OPEN_REPEAT"); do
      ROUND_NAME="open-r${r}-n${n}" ROUND_RATE="$r"
      run_one "o$r" "$n"
    done
  done
  ROUND_KIND=sweep ROUND_NAME="" ROUND_RATE=""
  log "open loop done, summary $SUMMARY_OPEN"
  grep -v '^#' "$SUMMARY_OPEN" | column -t -s "$(printf '\t')" >&2 || true
fi

# 轮询压力对落库吞吐的影响：同样的开环下单压力，一次没有查询压力，一次叠加 load poll。
if [ "$POLL" = 1 ]; then
  {
    echo "# poll pressure: open rate $POLL_RATE for $POLL_DURATION; poller c=$POLL_CONC interval=$POLL_REQ_INTERVAL recent=$POLL_RECENT"
    printf 'variant\trun\tplanned\taccepted\taccept_per_s\tlanded_db\tland_db_per_s\tpoll_requests\tpoll_per_s\tpoll_p50_ms\tpoll_p99_ms\n'
  } >"$SUMMARY_POLL"
  ROUND_KIND=poll ROUND_RATE="$POLL_RATE"
  for n in $(seq 1 "$POLL_REPEAT"); do
    ROUND_NAME="poll-base-n${n}" ROUND_POLLER=0
    run_one pb "$n"
    ROUND_NAME="poll-with-n${n}" ROUND_POLLER=1
    run_one pw "$n"
  done
  ROUND_KIND=sweep ROUND_NAME="" ROUND_RATE="" ROUND_POLLER=0
  log "poll comparison done, summary $SUMMARY_POLL"
  grep -v '^#' "$SUMMARY_POLL" | column -t -s "$(printf '\t')" >&2 || true
  python3 -I - "$SUMMARY_POLL" >&2 <<'PY'
import sys
rows = [l.rstrip("\n").split("\t") for l in open(sys.argv[1]) if not l.startswith("#")][1:]
def avg(variant):
    v = [float(r[6]) for r in rows if r[0] == variant]
    return sum(v) / len(v) if v else 0.0
base, polled = avg("baseline"), avg("polled")
print("landing throughput without poll pressure: %.1f orders/s" % base)
print("landing throughput with poll pressure:    %.1f orders/s" % polled)
print("ratio (with / without): %s" % ("%.3f" % (polled / base) if base else "n/a"))
PY
fi

# 限流对比：同样的开环下单压力（很少的用户号、速率远超他们的配额），gateway 的限流开和关各一轮。
if [ "$RATELIMIT" = 1 ]; then
  {
    echo "# rate limit: open rate $RL_RATE for $RL_DURATION, users $RL_USERS (per-user create quota 5/s, burst 10), per-IP limit off; variant on = RATE_LIMIT=on, off = RATE_LIMIT=off"
    printf 'rate_limit\trun\trate\tplanned\tsent\taccepted\taccept_per_s\trate_limited_pct\tbacklogged_pct\taccept_p50_ms\taccept_p99_ms\tlanded_db\tland_db_per_s\n'
  } >"$SUMMARY_RL"
  ROUND_KIND=rl ROUND_RATE="$RL_RATE" ROUND_USERS="$RL_USERS"
  for n in $(seq 1 "$RL_REPEAT"); do
    for v in on off; do
      GW_RATE_LIMIT="$v" ROUND_NAME="ratelimit-$v-n${n}"
      run_one "rl$v" "$n"
    done
  done
  GW_RATE_LIMIT=off ROUND_KIND=sweep ROUND_NAME="" ROUND_RATE="" ROUND_USERS=""
  log "rate limit comparison done, summary $SUMMARY_RL"
  grep -v '^#' "$SUMMARY_RL" | column -t -s "$(printf '\t')" >&2 || true
fi
