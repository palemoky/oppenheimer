#!/usr/bin/env bash
# 杀进程对账测试（设计文档 docs/design/1b-high-concurrency.md 的 7.3.2、7.3.3）。
#
# 在独立的 chaos_* 库和 chaos-<时间戳> 的 Kafka、Redis 前缀上启动四个服务（各一个实例），
# 灌入 SKU、预热闸门后压测；压测期间每隔 2–5 秒随机 kill -9 一个服务，1–3 秒后重启。
# 压测结束后停止杀进程、确保四个服务都在，等系统静止（load wait-quiet），再对账（load verify）。
# 退出码：0 表示静止且对账通过，1 表示没有静止或对账不通过，其他表示脚本本身出错。
#
# 需要先 make up 和 make build。只能用 macOS 自带的 bash 3.2 能跑的写法：
# 不用关联数组、wait -n、${var,,} 等。
#
# 可用环境变量调整：
#   CHAOS_DURATION        压测时长（提交窗口），默认 60s
#   CHAOS_CONCURRENCY     虚拟用户数，默认 50
#   CHAOS_USERS           用户 id 的范围，默认 500（小一些才会触发限购）
#   CHAOS_STOCK           每个 SKU 的初始库存，默认 60（小于压测的总需求，覆盖卖完）
#   CHAOS_LIMIT           SKU 3003 的每人限购，默认 2（其余 SKU 不限购）
#   CHAOS_QUIET_TIMEOUT   等静止的总时限，默认 2m
#   CHAOS_SEED            压测的随机种子，默认随机
#   CHAOS_KILL            设为 0 时不杀进程（用来对比）
#   CHAOS_REJECT_BACKOFF  提交被拒绝（卖完、限购）后虚拟用户等多久再下一单，默认 200ms
#   CHAOS_LOAD_ARGS       追加给 load run 的参数
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
COMPOSE="docker compose -f $ROOT/deploy/compose/docker-compose.yml"

DURATION="${CHAOS_DURATION:-60s}"
CONCURRENCY="${CHAOS_CONCURRENCY:-50}"
USERS="${CHAOS_USERS:-500}"
STOCK="${CHAOS_STOCK:-60}"
LIMIT="${CHAOS_LIMIT:-2}"
QUIET_TIMEOUT="${CHAOS_QUIET_TIMEOUT:-2m}"
SEED="${CHAOS_SEED:-0}"
KILL="${CHAOS_KILL:-1}"
SKUS="3001 3002 3003"

TS="$(date +%s)"
RUN_DIR="$ROOT/tmp/chaos-logs/$TS"
mkdir -p "$RUN_DIR"
ln -sfn "$TS" "$ROOT/tmp/chaos-logs/latest"
LEDGER="$RUN_DIR/ledger.jsonl"
EVENTS="$RUN_DIR/chaos-events.log"

log() { echo "[chaos $(date +%H:%M:%S)] $*" | tee -a "$EVENTS"; }

mysql_exec() {
  $COMPOSE exec -T mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" "$@" 2>/dev/null
}

dsn() { echo "root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:${MYSQL_PORT})/chaos_$1"; }

# ---------- 环境 ----------

export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:6379}"
export CACHE_REDIS_ADDR="${CACHE_REDIS_ADDR:-127.0.0.1:6380}"
export KAFKA_PREFIX="chaos-$TS."
export REDIS_PREFIX="chaos-$TS:"
# 时限调短，让系统能在几十秒内静止（7.3.2）。CREATE_TIMEOUT 必须大于订单服务处理一条消息的时限
# （service.DefaultEventTimeout = 10s，见 services/order/cmd/order/main.go），设计里的 5s 不被允许。
export PAYMENT_TIMEOUT="${PAYMENT_TIMEOUT:-5s}"
export HOLD_REAP_GRACE="${HOLD_REAP_GRACE:-5s}"
export HOLD_REAP_INTERVAL="${HOLD_REAP_INTERVAL:-1s}"
export SWEEP_INTERVAL="${SWEEP_INTERVAL:-2s}"
export CREATE_TIMEOUT="${CREATE_TIMEOUT:-11s}"
export GATE_RECONCILE_INTERVAL="${GATE_RECONCILE_INTERVAL:-2s}"
export LOG_LEVEL="${LOG_LEVEL:-info}"

SRC_FLAGS="-inventory-dsn $(dsn inventory_db) -order-dsn $(dsn order_db) -payment-dsn $(dsn payment_db)
  -redis-addr $REDIS_ADDR -redis-prefix $REDIS_PREFIX -kafka-brokers $KAFKA_BROKERS -kafka-prefix $KAFKA_PREFIX"

# ---------- 进程管理 ----------
# 每个服务一组变量：pid_<svc>（0 表示停着）、restart_<svc>（计划重启的时刻，SECONDS）、
# kills_<svc>（被杀次数）、starts_<svc>（启动次数）。bash 3.2 没有关联数组，用 eval 读写。

SERVICES="inventory order payment gateway"
for s in $SERVICES; do
  eval "pid_$s=0 restart_$s=0 kills_$s=0 starts_$s=0"
done
LOAD_PID=0

getv() { eval "echo \"\${$1}\""; }
setv() { eval "$1=\"$2\""; }

port_of() {
  case "$1" in
    gateway) echo 8080 ;;
    order) echo 8081 ;;
    inventory) echo 8082 ;;
    payment) echo 8083 ;;
  esac
}

start_svc() {
  local s="$1" pid
  case "$s" in
    inventory) env MYSQL_DSN="$(dsn inventory_db)" WORKER_ID=1 "$BIN/inventory" >>"$RUN_DIR/inventory.log" 2>&1 & ;;
    order) env MYSQL_DSN="$(dsn order_db)" WORKER_ID=2 "$BIN/order" >>"$RUN_DIR/order.log" 2>&1 & ;;
    payment) env MYSQL_DSN="$(dsn payment_db)" WORKER_ID=3 "$BIN/payment" >>"$RUN_DIR/payment.log" 2>&1 & ;;
    gateway) "$BIN/gateway" >>"$RUN_DIR/gateway.log" 2>&1 & ;;
  esac
  pid=$!
  setv "pid_$s" "$pid"
  setv "restart_$s" 0
  setv "starts_$s" $(($(getv "starts_$s") + 1))
  log "start $s pid $pid"
}

healthy() { curl -fsS -m 1 "http://127.0.0.1:$(port_of "$1")/healthz" >/dev/null 2>&1; }

# wait_healthy 等一个服务的 /healthz 通过，最多 30 秒。
wait_healthy() {
  local s="$1" i pid
  for i in $(seq 1 300); do
    if healthy "$s"; then return 0; fi
    pid="$(getv "pid_$s")"
    if [ "$pid" != 0 ] && ! kill -0 "$pid" 2>/dev/null; then
      log "$s (pid $pid) exited while starting"
      return 1
    fi
    sleep 0.1
  done
  log "$s not healthy after 30s"
  return 1
}

stop_all() {
  local s pid
  trap - EXIT INT TERM
  if [ "$LOAD_PID" != 0 ]; then kill -TERM "$LOAD_PID" 2>/dev/null || true; fi
  for s in $SERVICES; do
    pid="$(getv "pid_$s")"
    if [ "$pid" != 0 ]; then kill -TERM "$pid" 2>/dev/null || true; fi
  done
  sleep 1
  for s in $SERVICES; do
    pid="$(getv "pid_$s")"
    if [ "$pid" != 0 ]; then kill -KILL "$pid" 2>/dev/null || true; fi
  done
  wait 2>/dev/null || true
}
on_signal() {
  log "interrupted"
  stop_all
  exit 130
}
trap stop_all EXIT
trap on_signal INT TERM

# ---------- 准备 ----------

for p in 8080 8081 8082 8083; do
  if curl -fsS -m 1 "http://127.0.0.1:$p/healthz" >/dev/null 2>&1; then
    echo "port $p is already serving; stop the other services first" >&2
    exit 2
  fi
done

log "building load tool"
(cd "$ROOT" && go build -o "$BIN/load" ./test/load)

log "run dir $RUN_DIR, kafka prefix $KAFKA_PREFIX, redis prefix $REDIS_PREFIX"
sql=""
for db in inventory_db order_db payment_db; do
  sql="$sql DROP DATABASE IF EXISTS chaos_${db}; CREATE DATABASE chaos_${db};"
done
mysql_exec -e "$sql"
# 删掉以前的 chaos 运行留下的 topic 和 Redis key（只动 chaos- 前缀），失败不影响测试。
$COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --delete --topic 'chaos-.*' >/dev/null 2>&1 || true
for r in redis redis-cache; do
  $COMPOSE exec -T "$r" sh -c "redis-cli --scan --pattern 'chaos-*' | xargs -r -n 500 redis-cli DEL >/dev/null" \
    >/dev/null 2>&1 || true
done

# 启动顺序按依赖方向：inventory 先起（它建表），然后灌入 SKU。
start_svc inventory
wait_healthy inventory || exit 2
values=""
for sku in $SKUS; do
  lim=0
  if [ "$sku" = 3003 ]; then lim="$LIMIT"; fi
  values="$values${values:+,}($sku, 'chaos $sku', 1990, $STOCK, $lim)"
done
mysql_exec "chaos_inventory_db" -e \
  "INSERT INTO sku_stock (sku_id, name, unit_price, available, purchase_limit) VALUES $values;"
for s in order payment gateway; do
  start_svc "$s"
  wait_healthy "$s" || exit 2
done

INITIAL=""
SKU_LIST=""
for sku in $SKUS; do
  INITIAL="$INITIAL${INITIAL:+,}$sku=$STOCK"
  SKU_LIST="$SKU_LIST${SKU_LIST:+,}$sku"
done
"$BIN/load" warmup -skus "$SKU_LIST" -redis-addr "$REDIS_ADDR" -redis-prefix "$REDIS_PREFIX" | tee -a "$EVENTS"

# ---------- 压测与杀进程 ----------

log "load run: duration $DURATION, concurrency $CONCURRENCY, users $USERS, skus $SKU_LIST, stock $STOCK each"
# shellcheck disable=SC2086 # CHAOS_LOAD_ARGS 故意按空格拆分
"$BIN/load" run -c "$CONCURRENCY" -duration "$DURATION" -skus "$SKU_LIST" -users "$USERS" -seed "$SEED" \
  -reject-backoff "${CHAOS_REJECT_BACKOFF:-200ms}" \
  -run-id "chaos$TS" -ledger "$LEDGER" -json "$RUN_DIR/load-report.json" ${CHAOS_LOAD_ARGS:-} \
  >"$RUN_DIR/load-report.txt" 2>"$RUN_DIR/load.log" &
LOAD_PID=$!

dur_seconds() {
  case "$1" in
    *m) echo $((${1%m} * 60)) ;;
    *s) echo "${1%s}" ;;
    *) echo "$1" ;;
  esac
}
KILL_UNTIL=$((SECONDS + $(dur_seconds "$DURATION")))
# 前四次按随机排列各杀一个，保证四个服务都被杀到；之后完全随机。
ORDERED="$(for s in $SERVICES; do echo "$RANDOM $s"; done | sort -n | awk '{print $2}' | tr '\n' ' ')"
next_kill=$((SECONDS + 2 + RANDOM % 4))
kill_n=0

pick_victim() {
  local cand="" s n i=0
  if [ "$kill_n" -lt 4 ]; then
    set -- $ORDERED
    shift "$kill_n"
    s="$1"
    if [ "$(getv "pid_$s")" != 0 ]; then echo "$s"; fi
    return 0
  fi
  for s in $SERVICES; do
    if [ "$(getv "pid_$s")" != 0 ]; then cand="$cand $s"; fi
  done
  set -- $cand
  n=$#
  if [ "$n" = 0 ]; then return 0; fi
  i=$((RANDOM % n + 1))
  eval "echo \"\${$i}\""
}

# tick 处理到期的重启，并发现不是被脚本杀掉的退出（服务自己崩了，这是要报告的问题）。
tick() {
  local s pid r
  for s in $SERVICES; do
    pid="$(getv "pid_$s")"
    r="$(getv "restart_$s")"
    if [ "$pid" = 0 ]; then
      if [ "$r" != 0 ] && [ "$SECONDS" -ge "$r" ]; then start_svc "$s"; fi
    elif ! kill -0 "$pid" 2>/dev/null; then
      log "UNEXPECTED EXIT: $s pid $pid exited on its own, restarting"
      echo "unexpected_exit $s" >>"$RUN_DIR/unexpected-exits"
      start_svc "$s"
    fi
  done
}

while kill -0 "$LOAD_PID" 2>/dev/null; do
  tick
  if [ "$KILL" = 1 ] && [ "$SECONDS" -lt "$KILL_UNTIL" ] && [ "$SECONDS" -ge "$next_kill" ]; then
    victim="$(pick_victim)"
    if [ -n "$victim" ]; then
      pid="$(getv "pid_$victim")"
      kill -KILL "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      delay=$((1 + RANDOM % 3))
      setv "pid_$victim" 0
      setv "restart_$victim" $((SECONDS + delay))
      setv "kills_$victim" $(($(getv "kills_$victim") + 1))
      kill_n=$((kill_n + 1))
      log "kill -9 $victim (pid $pid), restart in ${delay}s"
    fi
    next_kill=$((SECONDS + 2 + RANDOM % 4))
  fi
  sleep 0.2
done
set +e
wait "$LOAD_PID"
load_code=$?
set -e
LOAD_PID=0
log "load run exited with $load_code"

# ---------- 静止与对账 ----------

# 确保四个服务都在运行（停着的立即重启，不再等计划的时刻）。
for s in $SERVICES; do
  pid="$(getv "pid_$s")"
  if [ "$pid" = 0 ] || ! kill -0 "$pid" 2>/dev/null; then start_svc "$s"; fi
done
for s in $SERVICES; do wait_healthy "$s" || exit 2; done
log "all services up; waiting for quiet (timeout $QUIET_TIMEOUT)"

set +e
# shellcheck disable=SC2086
"$BIN/load" wait-quiet $SRC_FLAGS -timeout "$QUIET_TIMEOUT" 2>"$RUN_DIR/wait-quiet.log" | tee "$RUN_DIR/wait-quiet.txt"
quiet_code=${PIPESTATUS[0]}
# 静止等待期间服务也不能自己退出。
tick
# shellcheck disable=SC2086
"$BIN/load" verify $SRC_FLAGS -ledger "$LEDGER" -initial "$INITIAL" -inventory-log "$RUN_DIR/inventory.log" \
  2>"$RUN_DIR/verify.log" | tee "$RUN_DIR/verify.txt"
verify_code=${PIPESTATUS[0]}
set -e

{
  echo "== chaos summary ($RUN_DIR) =="
  kills=""
  for s in $SERVICES; do kills="$kills $s=$(getv "kills_$s")"; done
  echo "kills:$kills (total $kill_n)"
  if [ -f "$RUN_DIR/unexpected-exits" ]; then
    echo "unexpected exits: $(wc -l <"$RUN_DIR/unexpected-exits" | tr -d ' ')"
  else
    echo "unexpected exits: 0"
  fi
  grep -E '^(accepted|landed|drain) ' "$RUN_DIR/load-report.txt" 2>/dev/null || true
  echo "load exit $load_code, wait-quiet exit $quiet_code, verify exit $verify_code"
} | tee -a "$EVENTS"

if [ "$quiet_code" = 0 ] && [ "$verify_code" = 0 ] && [ ! -f "$RUN_DIR/unexpected-exits" ]; then
  log "CHAOS PASS"
  exit 0
fi
log "CHAOS FAIL"
exit 1
