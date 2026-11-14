#!/usr/bin/env bash
# 故障注入：验证 Envoy 边车（ADR-0020，deploy/envoy/）的熔断和并发隔离真的起作用。
#
# 在独立的 fault_* 库和 fault-<时间戳> 的 Kafka、Redis 前缀上启动六个服务（经边车），灌入 SKU，
# 然后依次注入故障，每个场景打印通过/失败和关键数字，结束时打印结果表：
#   A 基线      查库存 50 次全 200；下一单并落库，整条链路是通的
#   B 下游挂住  kill -STOP inventory（进程在、不响应）：前几次约 3 秒后 503/504（路由超时），
#               之后被摘除，毫秒内 503；查订单和下单（不涉及 inventory）不受影响；恢复后 15 秒内查库存成功，
#               挂住期间受理的订单最终落库
#   C 进程被杀  kill -9 inventory：连不上，快速 503（先是连接失败，然后是摘除）；重启后 20 秒内恢复
#   D 并发上限  kill -STOP inventory 后同时发 300 个查库存请求：超过 gateway -> inventory 的在途上限（128）的
#               请求立刻被边车拒绝（upstream_rq_active_overflow 增长），其余等路由超时；查订单不受影响
#   E 业务错误  查不存在的 SKU（404）30 次不触发熔断（ejections_enforced_total 不增）
# 任何一个场景失败，脚本以非零退出。退出码：0 全部通过，1 有场景失败，2 脚本本身出错（环境不满足等）。
#
# 需要先 make up（五个边车和依赖都在）和 make build（bin/ 下有六个服务的二进制）。8080 到 8085 端口要空闲，
# 被占用时报错退出，不会去杀别的进程。边车是长期运行的容器，统计项的计数器不从 0 开始，每个场景只看增量。
# 只用 macOS 自带 bash 3.2 能跑的写法。
#
# 可用环境变量：
#   FAULT_KEEP_LOGS   设为 1 时全部通过也保留日志目录（默认只在失败时保留）
#   FAULT_ONLY        只跑指定的场景，空格分隔，例如 "A B"（A 总是先跑，后面的场景要用它建的订单）
#   LOG_LEVEL         服务的日志级别，默认 info
#   MYSQL_ROOT_PASSWORD、KAFKA_BROKERS、REDIS_ADDR、CACHE_REDIS_ADDR 同其他脚本
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin"
COMPOSE=(docker compose -f "$ROOT/deploy/compose/docker-compose.yml")
# shellcheck source=lib/sidecar.sh
. "$ROOT/scripts/lib/sidecar.sh"
SIDECAR=on # 这个脚本验证的就是边车
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
ONLY="${FAULT_ONLY:-A B C D E}"

TS="$(date +%s)"
RUN_DIR="$ROOT/tmp/fault-logs/$TS"
mkdir -p "$RUN_DIR"
ln -sfn "$TS" "$ROOT/tmp/fault-logs/latest"
EVENTS="$RUN_DIR/fault-events.log"
GW=http://127.0.0.1:8080
# 五个边车的管理端口（compose 里发布的），清场时要看它们全部的摘除状态。
ADMIN_PORTS="19901 19902 19903 19904 19905"
GW_ADMIN=19901
USER_ID=7001
SKU=3001
MISSING_SKU=999999

log() { echo "[fault $(date +%H:%M:%S)] $*" | tee -a "$EVENTS" >&2; }

mysql_on() {
  local svc="$1" stmt="$2"
  "${COMPOSE[@]}" exec -T "$svc" mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -e "$stmt" 2>/dev/null
}
dsn_at() { echo "root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:$1)/fault_$2"; }
cdc_dsn() { echo "cdc:${MYSQL_CDC_PASSWORD:-oppenheimer-cdc}@tcp(127.0.0.1:$1)/fault_$2"; }

# ---------- 环境 ----------

export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:6379}"
export CACHE_REDIS_ADDR="${CACHE_REDIS_ADDR:-127.0.0.1:6380}"
export KAFKA_PREFIX="fault-$TS."
export REDIS_PREFIX="fault-$TS:"
export LOG_LEVEL="${LOG_LEVEL:-info}"
# 占用回收的宽限调长：场景 B 里 inventory 挂住期间受理的订单要一直等到恢复，不能被当成没人消费而回收。
export HOLD_REAP_GRACE="${HOLD_REAP_GRACE:-10m}"

# ---------- 进程管理 ----------
# 每个服务一个 pid_<svc>（0 表示没有）；bash 3.2 没有关联数组，用 eval 读写。

SERVICES="inventory order payment orderindex cdc gateway"
for s in $SERVICES; do eval "pid_$s=0"; done
getv() { eval "echo \"\${$1}\""; }

port_of() {
  case "$1" in
    gateway) echo 8080 ;;
    order) echo 8081 ;;
    inventory) echo 8082 ;;
    payment) echo 8083 ;;
    orderindex) echo 8084 ;;
    cdc) echo 8085 ;;
  esac
}

start_svc() {
  local s="$1" pid
  case "$s" in
    inventory) env MYSQL_DSN="$(dsn_at "$MYSQL_PORT" inventory_db)" WORKER_ID=1 "$BIN/inventory" >>"$RUN_DIR/inventory.log" 2>&1 & ;;
    order) env MYSQL_DSNS="$(dsn_at 3311 order_db_0),$(dsn_at 3321 order_db_1)" \
      MYSQL_REPLICA_DSNS="$(dsn_at 3312 order_db_0),$(dsn_at 3322 order_db_1)" \
      ARCHIVE_DSN="$(dsn_at "$MYSQL_PORT" order_archive_db)" WORKER_ID=2 INVENTORY_ADDR="$SC_ORDER_INVENTORY" \
      "$BIN/order" >>"$RUN_DIR/order.log" 2>&1 & ;;
    payment) env MYSQL_DSN="$(dsn_at "$MYSQL_PORT" payment_db)" WORKER_ID=3 ORDER_ADDR="$SC_PAYMENT_ORDER" \
      "$BIN/payment" >>"$RUN_DIR/payment.log" 2>&1 & ;;
    orderindex) env MYSQL_DSNS="$(dsn_at "$MYSQL_PORT" merchant_index_db_0),$(dsn_at "$MYSQL_PORT" merchant_index_db_1)" \
      "$BIN/orderindex" >>"$RUN_DIR/orderindex.log" 2>&1 & ;;
    cdc) env CDC_SOURCE_NAMES=order-0,order-1 CDC_SERVER_IDS=1011,1021 \
      CDC_SOURCE_DSNS="$(cdc_dsn 3312 order_db_0),$(cdc_dsn 3322 order_db_1)" \
      "$BIN/cdc" >>"$RUN_DIR/cdc.log" 2>&1 & ;;
    # 限流关掉：场景里从一个地址用少数几个用户高频请求，不关会先被限流（429）拦住，验证不到边车。
    # shellcheck disable=SC2086 # SC_GATEWAY_ADDRS 故意按空格拆成几个 KEY=VALUE
    gateway) env RATE_LIMIT=off $SC_GATEWAY_ADDRS "$BIN/gateway" >>"$RUN_DIR/gateway.log" 2>&1 & ;;
  esac
  pid=$!
  eval "pid_$s=$pid"
  wait_healthy "$s" || { tail -n 20 "$RUN_DIR/$s.log" >&2; exit 2; }
  log "started $s (pid $pid)"
}

wait_healthy() {
  local s="$1" i pid
  pid="$(getv "pid_$s")"
  for i in $(seq 1 300); do
    if curl -fsS -m 1 "http://127.0.0.1:$(port_of "$s")/healthz" >/dev/null 2>&1; then return 0; fi
    if ! kill -0 "$pid" 2>/dev/null; then echo "$s exited while starting" >&2; return 1; fi
    sleep 0.1
  done
  echo "$s not healthy after 30s" >&2
  return 1
}

stop_all() {
  local s pid
  trap - EXIT INT TERM
  # 被 SIGSTOP 的进程收不到 SIGTERM，先 CONT。
  for s in $SERVICES; do
    pid="$(getv "pid_$s")"
    if [ "$pid" != 0 ]; then kill -CONT "$pid" 2>/dev/null || true; kill -TERM "$pid" 2>/dev/null || true; fi
  done
  sleep 1
  for s in $SERVICES; do
    pid="$(getv "pid_$s")"
    if [ "$pid" != 0 ]; then kill -KILL "$pid" 2>/dev/null || true; fi
  done
  wait 2>/dev/null || true
}
on_exit() {
  local code=$?
  stop_all
  if [ "$code" != 0 ] || [ "$FAILED" != 0 ]; then
    echo "logs kept in $RUN_DIR" >&2
  elif [ "${FAULT_KEEP_LOGS:-0}" != 1 ]; then
    rm -rf "$RUN_DIR" "$ROOT/tmp/fault-logs/latest"
  fi
}
FAILED=0
trap on_exit EXIT
trap 'log interrupted; exit 130' INT TERM

# ---------- 辅助 ----------

# stat <管理端口> <统计项全名>：取一个数，没有这一项时为 0。
stat() {
  curl -fsS -m 3 "127.0.0.1:$1/stats" | awk -F': ' -v k="$2" '$1 == k { print $2; found = 1 } END { if (!found) print 0 }'
}
GW_INV=cluster.to_inventory

# http_code <方法> <路径> [用户号]：对 gateway 发一个请求，打印“状态码 耗时毫秒”。响应体在 $RUN_DIR/last-body。
http_code() {
  local out
  out="$(curl -s -m 10 -o "$RUN_DIR/last-body" -w '%{http_code} %{time_total}' -X "$1" \
    -H "X-User-Id: ${3:-$USER_ID}" "$GW$2" || true)"
  echo "$out" | awk '{ printf "%s %d\n", $1, $2 * 1000 + 0.5 }'
}

# 把“状态码 耗时”拆到 HC_CODE、HC_MS。
split_hc() { HC_CODE="${1%% *}"; HC_MS="${1##* }"; }

stock_path() { echo "/api/v1/skus/$1/stock"; }

# percentile <p> 数字...：打印第 p 百分位（最近秩法）。
percentile() {
  local p="$1"
  shift
  printf '%s\n' "$@" | sort -n | awk -v p="$p" '{ a[NR] = $1 } END { i = int((p * NR + 99) / 100); if (i < 1) i = 1; print a[i] }'
}

# wait_clear 等所有边车的摘除期满（ejections_active 都回到 0）并且一个探测请求成功。
wait_clear() {
  local i total p
  for i in $(seq 1 60); do
    total=0
    for p in $ADMIN_PORTS; do
      total=$((total + $(curl -fsS -m 3 "127.0.0.1:$p/stats" | awk -F': ' '/outlier_detection.ejections_active:/ { n += $2 } END { print n + 0 }')))
    done
    if [ "$total" = 0 ]; then
      split_hc "$(http_code GET "$(stock_path $SKU)")"
      if [ "$HC_CODE" = 200 ]; then return 0; fi
    fi
    sleep 0.5
  done
  log "wait_clear: ejections still active or probe failing after 30s"
  return 1
}

# 场景记账。expect <说明> <命令...>：命令成功即通过。
SC_FAIL=0
RESULTS=""
expect() {
  local desc="$1"
  shift
  if "$@"; then
    log "  ok    $desc"
  else
    log "  FAIL  $desc"
    SC_FAIL=1
  fi
}
begin() { SC_FAIL=0; SC_NOTE=""; log "== scenario $1: $2"; }
finish() {
  local name="$1" title="$2" res=PASS
  if [ "$SC_FAIL" != 0 ]; then res=FAIL; FAILED=1; fi
  log "== scenario $name $res: $SC_NOTE"
  RESULTS="$RESULTS$name|$title|$res|$SC_NOTE
"
}
wanted() { case " $ONLY " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

# 在 gateway 上下一单，打印订单号（失败时打印空）。
submit_order() {
  curl -s -m 10 -X POST -H "X-User-Id: $USER_ID" -H "Idempotency-Key: fault-$TS-$RANDOM-$RANDOM" \
    -H 'Content-Type: application/json' -w '\n%{http_code}' \
    -d "{\"items\":[{\"sku_id\":\"$SKU\",\"quantity\":1}]}" "$GW/api/v1/orders" |
    awk 'NR == 1 { body = $0 } END { code = $0; print code " " body }'
}
# 把“状态码 响应体”拆到 SUB_CODE、SUB_ID。
split_submit() {
  SUB_CODE="${1%% *}"
  SUB_ID="$(echo "${1#* }" | sed -n 's/.*"order_id" *: *"\([0-9]*\)".*/\1/p')"
}

# order_status <订单号>：打印订单状态（取不到时为空）。
order_status() {
  curl -s -m 10 -H "X-User-Id: $USER_ID" "$GW/api/v1/orders/$1" | sed -n 's/.*"status" *: *"\([a-z_]*\)".*/\1/p' | head -n 1
}

# wait_left_creating <订单号> <秒数>：等订单离开 creating，状态放在 ORDER_STATE。
wait_left_creating() {
  local deadline=$((SECONDS + $2))
  ORDER_STATE=""
  while [ "$SECONDS" -lt "$deadline" ]; do
    ORDER_STATE="$(order_status "$1")"
    if [ -n "$ORDER_STATE" ] && [ "$ORDER_STATE" != creating ]; then return 0; fi
    sleep 0.5
  done
  return 1
}

# 恢复 inventory 之后轮询查库存直到成功，恢复用时（毫秒）放在 RECOVER_MS；超过 $1 秒返回失败。
poll_stock_until_ok() {
  local limit="$1" t0 now
  t0="$(python3 -I -c 'import time; print(int(time.time()*1000))')"
  RECOVER_MS=-1
  while :; do
    split_hc "$(http_code GET "$(stock_path $SKU)")"
    now="$(python3 -I -c 'import time; print(int(time.time()*1000))')"
    if [ "$HC_CODE" = 200 ]; then
      RECOVER_MS=$((now - t0))
      log "  first success: ejections_active $(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_active)"
      return 0
    fi
    if [ $((now - t0)) -gt $((limit * 1000)) ]; then RECOVER_MS=$((now - t0)); return 1; fi
    sleep 0.3
  done
}

# ---------- 准备 ----------

sidecar_setup
for p in 8080 8081 8082 8083 8084 8085; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    echo "port $p is already in use; stop the other services first" >&2
    exit 2
  fi
done
for s in $SERVICES; do
  if [ ! -x "$BIN/$s" ]; then echo "missing $BIN/$s (run make build)" >&2; exit 2; fi
done
for t in python3 curl; do command -v "$t" >/dev/null || { echo "$t is required" >&2; exit 2; }; done

log "run dir $RUN_DIR, kafka prefix $KAFKA_PREFIX, redis prefix $REDIS_PREFIX"
(cd "$ROOT" && go build -o "$RUN_DIR/load" ./test/load)

mysql_on mysql "
  DROP DATABASE IF EXISTS fault_inventory_db; CREATE DATABASE fault_inventory_db;
  DROP DATABASE IF EXISTS fault_payment_db; CREATE DATABASE fault_payment_db;
  DROP DATABASE IF EXISTS fault_merchant_index_db_0; CREATE DATABASE fault_merchant_index_db_0;
  DROP DATABASE IF EXISTS fault_merchant_index_db_1; CREATE DATABASE fault_merchant_index_db_1;
  DROP DATABASE IF EXISTS fault_order_archive_db; CREATE DATABASE fault_order_archive_db;"
# 分片库建在各自的主库上（DDL 复制到从库），CDC 的账号另外授权。
for i in 0 1; do
  mysql_on "mysql-order-$i" "DROP DATABASE IF EXISTS fault_order_db_$i; CREATE DATABASE fault_order_db_$i;
    GRANT SELECT ON fault_order_db_$i.* TO 'cdc'@'%';"
done
for i in 0 1; do
  n=0
  for _ in $(seq 1 60); do
    n="$(mysql_on "mysql-order-$i-replica" "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='fault_order_db_$i'" | tail -n 1 || echo 0)"
    [ "$n" = 1 ] && break
    sleep 1
  done
  if [ "$n" != 1 ]; then echo "fault_order_db_$i did not replicate within 60s" >&2; exit 2; fi
done
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --delete --topic 'fault-.*' >/dev/null 2>&1 || true
for r in redis redis-cache; do
  "${COMPOSE[@]}" exec -T "$r" sh -c "redis-cli --scan --pattern 'fault-*' | xargs -r -n 500 redis-cli DEL >/dev/null" \
    >/dev/null 2>&1 || true
done

start_svc inventory
mysql_on mysql "INSERT INTO fault_inventory_db.sku_stock (sku_id, name, unit_price, available, purchase_limit) VALUES ($SKU, 'fault $SKU', 1990, 100000, 0);"
for s in order payment orderindex cdc gateway; do start_svc "$s"; done
"$RUN_DIR/load" warmup -skus "$SKU" -redis-addr "$REDIS_ADDR" -redis-prefix "$REDIS_PREFIX" >>"$EVENTS" 2>&1

BASE_ORDER=""
BASE_P50=1

# ---------- 场景 A：基线 ----------

# A 总是要跑：B、D 要用它建的订单和基线延迟。
begin A "baseline"
lat=""
bad=0
for i in $(seq 1 50); do
  split_hc "$(http_code GET "$(stock_path $SKU)" "$((8000 + i))")"
  if [ "$HC_CODE" != 200 ]; then bad=$((bad + 1)); fi
  lat="$lat $HC_MS"
done
# shellcheck disable=SC2086
BASE_P50="$(percentile 50 $lat)"
[ "$BASE_P50" -ge 1 ] || BASE_P50=1
expect "50 stock queries all 200 (non-200: $bad)" [ "$bad" = 0 ]
split_submit "$(submit_order)"
BASE_ORDER="$SUB_ID"
expect "create order accepted (202), id ${BASE_ORDER:-none}" [ "$SUB_CODE" = 202 -a -n "$BASE_ORDER" ]
if wait_left_creating "$BASE_ORDER" 30; then :; fi
expect "order landed as pending_payment (got ${ORDER_STATE:-none})" [ "$ORDER_STATE" = pending_payment ]
SC_NOTE="stock p50 ${BASE_P50}ms, order $BASE_ORDER -> $ORDER_STATE"
finish A "baseline"

# 估计一次查订单的基线延迟（B、D 用来比较）。
lat=""
for i in $(seq 1 10); do
  split_hc "$(http_code GET "/api/v1/orders/$BASE_ORDER")"
  lat="$lat $HC_MS"
done
# shellcheck disable=SC2086
ORDER_P50="$(percentile 50 $lat)"
[ "$ORDER_P50" -ge 1 ] || ORDER_P50=1
ORDER_LIMIT=$((ORDER_P50 * 10 > 150 ? ORDER_P50 * 10 : 150))

# order_isolation_check <次数>：查 BASE_ORDER 若干次，全 200 且 p50 在 ORDER_LIMIT 以内；结果放在 ISO_NOTE。
order_isolation_check() {
  local n="$1" i bad=0 lat="" p50
  for i in $(seq 1 "$n"); do
    split_hc "$(http_code GET "/api/v1/orders/$BASE_ORDER")"
    if [ "$HC_CODE" != 200 ]; then bad=$((bad + 1)); fi
    lat="$lat $HC_MS"
  done
  # shellcheck disable=SC2086
  p50="$(percentile 50 $lat)"
  ISO_NOTE="order queries $n, non-200 $bad, p50 ${p50}ms (baseline ${ORDER_P50}ms, limit ${ORDER_LIMIT}ms)"
  expect "$ISO_NOTE" [ "$bad" = 0 -a "$p50" -le "$ORDER_LIMIT" ]
}

# ---------- 场景 B：下游挂住 ----------

if wanted B; then
  begin B "downstream hung (SIGSTOP inventory)"
  wait_clear || SC_FAIL=1
  ej0="$(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_enforced_total)"
  kill -STOP "$pid_inventory"
  codes="" slow=0 fast=0 slow_bad=0 fast_bad=0 lat_list=""
  for i in $(seq 1 12); do
    split_hc "$(http_code GET "$(stock_path $SKU)" "$((8100 + i))")"
    codes="$codes $HC_CODE/${HC_MS}ms"
    if [ "$HC_MS" -ge 1000 ]; then
      slow=$((slow + 1))
      # gateway 自己的按跳时限和边车的路由超时都是 3 秒，谁先到都可能；耗时不能是 gateway 的总时限 5 秒。
      if { [ "$HC_CODE" != 503 ] && [ "$HC_CODE" != 504 ]; } || [ "$HC_MS" -lt 2500 ] || [ "$HC_MS" -gt 4500 ]; then
        slow_bad=$((slow_bad + 1))
      fi
    elif [ "$HC_CODE" = 503 ] && [ "$HC_MS" -lt 200 ]; then
      fast=$((fast + 1))
    else
      fast_bad=$((fast_bad + 1))
    fi
  done
  log "  stock queries (code/latency):$codes"
  ej1="$(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_enforced_total)"
  expect "slow responses ($slow) are 503/504 within 2.5-4.5s (violations: $slow_bad)" [ "$slow" -ge 1 -a "$slow_bad" = 0 ]
  expect "fast failures: $fast are 503 under 200ms (need >= 3; other responses: $fast_bad)" [ "$fast" -ge 3 ]
  expect "gateway sidecar ejections_enforced_total +$((ej1 - ej0)) (need >= 1)" [ "$((ej1 - ej0))" -ge 1 ]
  # 隔离：inventory 挂住期间，不涉及它的请求不受影响。
  order_isolation_check 20
  split_submit "$(submit_order)"
  HUNG_ORDER="$SUB_ID"
  expect "create order accepted while inventory hung (got $SUB_CODE, id ${HUNG_ORDER:-none})" [ "$SUB_CODE" = 202 -a -n "$HUNG_ORDER" ]
  kill -CONT "$pid_inventory"
  poll_stock_until_ok 15 || true
  expect "stock query recovered ${RECOVER_MS}ms after SIGCONT (limit 15000ms)" [ "$RECOVER_MS" -ge 0 -a "$RECOVER_MS" -le 15000 ]
  rec_ms="$RECOVER_MS"
  t0=$SECONDS
  wait_left_creating "$HUNG_ORDER" 60 || true
  expect "order accepted during the hang left creating within 60s (state ${ORDER_STATE:-none}, $((SECONDS - t0))s)" [ -n "$ORDER_STATE" -a "$ORDER_STATE" != creating ]
  SC_NOTE="slow $slow, fast $fast, ejections +$((ej1 - ej0)), recovery ${rec_ms}ms, hung order -> ${ORDER_STATE:-none}"
  finish B "downstream hung"
  wait_clear || log "warning: wait_clear after B timed out"
fi

# ---------- 场景 C：下游进程被杀 ----------

if wanted C; then
  begin C "downstream killed (SIGKILL inventory)"
  wait_clear || SC_FAIL=1
  ej0="$(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_enforced_total)"
  kill -KILL "$pid_inventory"
  wait "$pid_inventory" 2>/dev/null || true
  pid_inventory=0
  codes="" bad=0 maxms=0
  for i in $(seq 1 12); do
    split_hc "$(http_code GET "$(stock_path $SKU)" "$((8200 + i))")"
    codes="$codes $HC_CODE/${HC_MS}ms"
    if [ "$HC_CODE" != 503 ] || [ "$HC_MS" -ge 1000 ]; then bad=$((bad + 1)); fi
    if [ "$HC_MS" -gt "$maxms" ]; then maxms="$HC_MS"; fi
  done
  log "  stock queries (code/latency):$codes"
  ej1="$(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_enforced_total)"
  log "  after queries: ejections_active $(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_active)"
  expect "all 12 queries 503 under 1s (violations: $bad, max ${maxms}ms)" [ "$bad" = 0 ]
  expect "gateway sidecar ejections_enforced_total +$((ej1 - ej0)) (need >= 1)" [ "$((ej1 - ej0))" -ge 1 ]
  start_svc inventory
  log "  after restart: ejections_active $(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_active)"
  poll_stock_until_ok 20 || true
  expect "stock query recovered ${RECOVER_MS}ms after restart (limit 20000ms)" [ "$RECOVER_MS" -ge 0 -a "$RECOVER_MS" -le 20000 ]
  SC_NOTE="12 queries max ${maxms}ms, ejections +$((ej1 - ej0)), recovery ${RECOVER_MS}ms"
  finish C "downstream killed"
  wait_clear || log "warning: wait_clear after C timed out"
fi

# ---------- 场景 D：并发上限 ----------

# 在干净状态（没有被摘除）下 kill -STOP inventory，同时发 300 个请求：前 128 个占满 gateway -> inventory 的在途名额，
# 一直挂到 3 秒的路由超时；其余的在名额满的那一刻被边车立刻拒绝，UNAVAILABLE -> 503。
# 统计项是 upstream_rq_active_overflow（max_requests 对应 HTTP/2 的在途请求；upstream_rq_pending_overflow 对应
# max_pending_requests，只在 HTTP/1 连接池排队时增长，这里实测不增长）。
# 超时的那批请求到期后才会让连续失败达到 5 次触发摘除，所以 300 个请求发出的瞬间还没有摘除，溢出一定能观察到。
if wanted D; then
  begin D "concurrency limit (300 concurrent against hung inventory)"
  wait_clear || SC_FAIL=1
  of0="$(stat $GW_ADMIN $GW_INV.upstream_rq_active_overflow)"
  ov0="$(stat $GW_ADMIN $GW_INV.circuit_breakers.default.rq_open)"
  ej0="$(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_enforced_total)"
  kill -STOP "$pid_inventory"
  OUT_D="$RUN_DIR/scenario-d.txt"
  : >"$OUT_D"
  d0="$SECONDS"
  # 每个请求用不同的用户号；输出一行“状态码 耗时毫秒”。curl 的总时限 10 秒，比 gateway 的 5 秒总时限长。
  seq 1 300 | xargs -P 300 -I{} sh -c \
    "curl -s -m 10 -o /dev/null -w '%{http_code} %{time_total}\n' -H 'X-User-Id: 9{}' '$GW$(stock_path $SKU)' >>'$OUT_D'" &
  XARGS_PID=$!
  sleep 1
  peak="$(stat $GW_ADMIN $GW_INV.upstream_rq_active)"
  rq_open_peak="$(stat $GW_ADMIN $GW_INV.circuit_breakers.default.rq_open)"
  order_isolation_check 20
  wait "$XARGS_PID" || true
  elapsed=$((SECONDS - d0))
  of1="$(stat $GW_ADMIN $GW_INV.upstream_rq_active_overflow)"
  ej1="$(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_enforced_total)"
  total="$(wc -l <"$OUT_D" | tr -d ' ')"
  fast503="$(awk '$1 == 503 && $2 < 0.5' "$OUT_D" | wc -l | tr -d ' ')"
  slow_all="$(awk '$2 >= 5.0' "$OUT_D" | wc -l | tr -d ' ')"
  other="$(awk '$1 != 503 && $1 != 504' "$OUT_D" | wc -l | tr -d ' ')"
  log "  responses $total, fast 503 (<500ms) $fast503, >=5s $slow_all, non-503/504 $other, elapsed ${elapsed}s, active at 1s $peak, rq_open $rq_open_peak"
  expect "300 responses received (got $total)" [ "$total" = 300 ]
  expect "upstream_rq_active_overflow +$((of1 - of0)) (need >= 1)" [ "$((of1 - of0))" -ge 1 ]
  expect "$fast503 requests rejected immediately (<500ms, 503) (need >= 100)" [ "$fast503" -ge 100 ]
  expect "no request waited the gateway's 5s total deadline ($slow_all)" [ "$slow_all" = 0 ]
  expect "all responses 503/504 (other: $other)" [ "$other" = 0 ]
  SC_NOTE="overflow +$((of1 - of0)), fast 503 $fast503/$total, active at 1s $peak, ejections +$((ej1 - ej0)), order p50 within limit"
  kill -CONT "$pid_inventory"
  finish D "concurrency limit"
  wait_clear || log "warning: wait_clear after D timed out"
fi

# ---------- 场景 E：业务错误不触发熔断 ----------

if wanted E; then
  begin E "business errors do not trip the breaker"
  wait_clear || SC_FAIL=1
  ej0="$(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_enforced_total)"
  not404=0
  for i in $(seq 1 30); do
    split_hc "$(http_code GET "$(stock_path $MISSING_SKU)" "$((8300 + i))")"
    if [ "$HC_CODE" != 404 ]; then not404=$((not404 + 1)); fi
  done
  split_hc "$(http_code GET "$(stock_path $SKU)")"
  ej1="$(stat $GW_ADMIN $GW_INV.outlier_detection.ejections_enforced_total)"
  expect "30 queries for a missing SKU all 404 (other: $not404)" [ "$not404" = 0 ]
  expect "existing SKU still 200 (got $HC_CODE)" [ "$HC_CODE" = 200 ]
  expect "ejections_enforced_total +$((ej1 - ej0)) (need 0)" [ "$((ej1 - ej0))" = 0 ]
  SC_NOTE="404 x30, ejections +$((ej1 - ej0))"
  finish E "business errors"
fi

# ---------- 结果 ----------

echo
echo "== fault injection results =="
printf '%s' "$RESULTS" | awk -F'|' '{ printf "  %s  %-22s %s  %s\n", $1, $2, $3, $4 }'
if [ "$FAILED" != 0 ]; then
  echo "FAULT FAIL" >&2
  exit 1
fi
echo "FAULT PASS"
