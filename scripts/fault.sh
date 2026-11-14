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
# 下面是“依赖坏了，系统按预案降级”的场景（设计文档 1d 的 6.4 到 6.12），验证 503 加原因码、/degrade 的状态、
# 已受理的订单不丢、恢复后自动回来。会暂停或停掉本机共享的 Redis 和 MySQL 从库容器，每个场景结束和脚本退出时都会恢复：
#   F 暂停闸门的 Redis   docker pause redis 30 秒：下单毫秒级 503 STOCK_GATE_UNAVAILABLE，已落库订单的查询照常，
#                        gateway 边车不摘除 order；恢复后下单恢复，暂停前受理而未落库的订单全部落库，死信为空
#   G 停掉闸门的 Redis   docker stop 再 start（连接被拒绝；redis 开了 AOF，数据还在，所以同样断言受理的订单落库）
#   H 停掉缓存的 Redis   pause redis-cache 20 秒：查订单（终态走缓存）和查库存全 200，延迟回到基线附近，
#                        order、inventory 的 cache 开关 degraded，恢复后回到正常
#   I 从库停止回放       ORDER_READ_MODE=gtid，STOP REPLICA SQL_THREAD：3 秒内 replica_read degraded，之后读到的是最新状态，
#                        START REPLICA SQL_THREAD 后 10 秒内恢复
#   J 暂停一个从库       pause 从库容器 10 秒：查询全 200，延迟回到基线附近，恢复后 replica_read 回到正常
#   K 冷库不通           ARCHIVE_DSN 指向不通的端口重启 order：能启动、下单落库正常、旧订单号 503 ARCHIVE_UNAVAILABLE、
#                        用户订单列表 200 并带 archive_unavailable
#   L inventory 挂住且持续下单  kill -STOP inventory 20 秒：头几单之后 503 ORDER_LANDING_UNAVAILABLE，挂住期间受理的单数有限，
#                        恢复后 30 秒内下单恢复
#   M 带流量 SIGTERM order  20 秒内退出、退出码 0，重启后正常，没有孤儿占用，停止前受理的订单全部离开 creating
#   N Redis 停着时启动   停 payment 和 order，pause redis，启动两者：都能就绪，查询 200、下单 503，恢复后下单成功
# 任何一个场景失败，脚本以非零退出。退出码：0 全部通过，1 有场景失败，2 脚本本身出错（环境不满足等）。
#
# 需要先 make up（五个边车和依赖都在）和 make build（bin/ 下有六个服务的二进制）。8080 到 8085 和 8089（gateway 管理端口）要空闲，
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
ONLY="${FAULT_ONLY:-A B C D E F G H I J K L M N}"

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

# ORDER_ENV 是 order 进程额外的 KEY=VALUE（空格分隔，值里不能有空格），排在默认值之后，同名的以它为准。
ORDER_ENV=""
start_svc() {
  local s="$1" pid
  # shellcheck disable=SC2086 # ORDER_ENV 故意按空格拆开
  case "$s" in
    inventory) env MYSQL_DSN="$(dsn_at "$MYSQL_PORT" inventory_db)" WORKER_ID=1 "$BIN/inventory" >>"$RUN_DIR/inventory.log" 2>&1 & ;;
    order) env MYSQL_DSNS="$(dsn_at 3311 order_db_0),$(dsn_at 3321 order_db_1)" \
      MYSQL_REPLICA_DSNS="$(dsn_at 3312 order_db_0),$(dsn_at 3322 order_db_1)" \
      ARCHIVE_DSN="$(dsn_at "$MYSQL_PORT" order_archive_db)" WORKER_ID=2 INVENTORY_ADDR="$SC_ORDER_INVENTORY" \
      $ORDER_ENV "$BIN/order" >>"$RUN_DIR/order.log" 2>&1 & ;;
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
# restore_env 把场景可能留下的破坏全部恢复：暂停的容器、停掉的 Redis、停着的复制、后台下单的循环。
# 场景正常结束时已经恢复过一遍，这里是脚本中途失败或被中断时的兜底；恢复动作都容忍“本来就没坏”。
# ENV_DIRTY 在第一次注入故障前置 1，没注入过就不碰容器。
ENV_DIRTY=0
LOADER_PID=0
restore_env() {
  [ "$ENV_DIRTY" = 1 ] || return 0
  local c i
  [ "$LOADER_PID" = 0 ] || kill "$LOADER_PID" 2>/dev/null || true
  for c in redis redis-cache mysql-order-0-replica mysql-order-1-replica; do
    "${COMPOSE[@]}" unpause "$c" >/dev/null 2>&1 || true
  done
  "${COMPOSE[@]}" start redis redis-cache >/dev/null 2>&1 || true
  for i in 0 1; do
    "${COMPOSE[@]}" exec -T "mysql-order-$i-replica" mysql -uroot -p"$MYSQL_ROOT_PASSWORD" \
      -e "START REPLICA SQL_THREAD; START REPLICA IO_THREAD" >/dev/null 2>&1 || true
  done
}
on_exit() {
  local code=$?
  restore_env
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

# ---------- 降级场景的辅助和阈值 ----------

# 阈值。断言宁可宽松也不要对时序太敏感；关键数字都会打印进结果表的备注。
WARM_CALLS=8        # 熔断器要连续失败 5 次才打开，每个依赖类的断言都跳过最前面的这几次调用
FAST_MS=100         # 熔断器打开后，降级响应（503）的耗时上限
PROBE_SLOW_MAX=1500 # 半开探测放行的那一次调用允许慢（order 对闸门 Redis 的一次调用时限 300ms），但不能超过它
GW_ORD=cluster.to_order

now_ms() { python3 -I -c 'import time; print(int(time.time()*1000))'; }

# 各服务 /degrade 所在的端口：gateway 的开关在管理端口 8089（不在对外的 8080 上），其他在各自的内部端口。
dg_port() { case "$1" in gateway) echo 8089 ;; *) port_of "$1" ;; esac; }

# dg_field <服务> <开关名> <字段>：打印开关的一个字段（degraded 打印 true/false）；取不到时为空。
dg_field() {
  curl -fsS -m 3 "127.0.0.1:$(dg_port "$1")/degrade" 2>/dev/null | python3 -I -c '
import json, sys
name, field = sys.argv[1], sys.argv[2]
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for s in d.get("switches", []):
    if s.get("name") == name:
        v = s.get(field)
        print(str(v).lower() if isinstance(v, bool) else v)
' "$2" "$3" || true
}

# dg_names <服务>：打印当前 degraded 的开关名，空格分隔。
dg_names() {
  curl -fsS -m 3 "127.0.0.1:$(dg_port "$1")/degrade" 2>/dev/null | python3 -I -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
print(" ".join(s["name"] for s in d.get("switches", []) if s.get("degraded")))
' || true
}

# 查询类请求的“驱动”：熔断器和探测要靠请求推动恢复。wait_dg 每轮调用 $NUDGE_CMD。
nudge_reads() {
  http_code GET "/api/v1/orders/$BASE_ORDER" >/dev/null
  http_code GET "$(stock_path $SKU)" >/dev/null
}
nudge_submit() { submit_full; }
NUDGE_CMD=nudge_reads

# wait_dg <服务> <开关名> <true|false> <秒数>：等开关的 degraded 变成指定值，用时（毫秒）放在 DG_MS。
wait_dg() {
  local t0 now
  t0="$(now_ms)"
  DG_MS=-1
  while :; do
    now="$(now_ms)"
    if [ "$(dg_field "$1" "$2" degraded)" = "$3" ]; then DG_MS=$((now - t0)); return 0; fi
    if [ $((now - t0)) -gt $(($4 * 1000)) ]; then DG_MS=$((now - t0)); return 1; fi
    "$NUDGE_CMD" >/dev/null 2>&1 || true
    sleep 0.25
  done
}

ejections_active_total() {
  local p total=0
  for p in $ADMIN_PORTS; do
    total=$((total + $(curl -fsS -m 3 "127.0.0.1:$p/stats" | awk -F': ' '/outlier_detection.ejections_active:/ { n += $2 } END { print n + 0 }')))
  done
  echo "$total"
}

# env_clean：场景开始前确认环境干净：六个服务都健康、各 /degrade 都不 degraded、边车没有摘除、查库存成功。
# 受理的恢复（落库健康、闸门熔断器）要靠请求驱动，所以等待期间 admission 还在 degraded 时下一单。
env_clean() {
  local i s names bad="" ej
  for i in $(seq 1 80); do
    bad=""
    for s in inventory order payment orderindex gateway; do
      if ! curl -fsS -m 2 "http://127.0.0.1:$(port_of "$s")/healthz" >/dev/null 2>&1; then bad="$bad $s:down"; fi
      names="$(dg_names "$s")"
      if [ -n "$names" ]; then bad="$bad $s:degraded($names)"; fi
    done
    ej="$(ejections_active_total)"
    if [ "$ej" != 0 ]; then bad="$bad ejections_active:$ej"; fi
    if [ -z "$bad" ]; then
      split_hc "$(http_code GET "$(stock_path $SKU)")"
      if [ "$HC_CODE" = 200 ]; then return 0; fi
      bad="stock probe $HC_CODE"
    fi
    if [ "$(dg_field order admission degraded)" = true ]; then submit_full; fi
    nudge_reads
    sleep 0.5
  done
  log "env_clean: environment not clean after 40s:$bad"
  return 1
}

# submit_full：下一单，结果放在 SUB_CODE、SUB_MS、SUB_REASON（响应体 error.code）、SUB_RETRY（Retry-After 头）、SUB_ID。
# 在当前 shell 里调用（不要放进 $(...)）。SUB_TAG 区分同时下单的几个进程用的临时文件。
SUB_TAG=main
submit_full() {
  local out body="$RUN_DIR/sub-body.$SUB_TAG" hdr="$RUN_DIR/sub-hdr.$SUB_TAG"
  out="$(curl -s -m 10 -o "$body" -D "$hdr" -w '%{http_code} %{time_total}' -X POST -H "X-User-Id: $USER_ID" \
    -H "Idempotency-Key: fault-$TS-$RANDOM-$RANDOM" -H 'Content-Type: application/json' \
    -d "{\"items\":[{\"sku_id\":\"$SKU\",\"quantity\":1}]}" "$GW/api/v1/orders" || true)"
  SUB_CODE="${out%% *}"
  SUB_MS="$(echo "${out#* }" | awk '{ printf "%d", $1 * 1000 + 0.5 }')"
  SUB_ID="$(sed -n 's/.*"order_id" *: *"\([0-9]*\)".*/\1/p' "$body" 2>/dev/null | head -n 1 || true)"
  SUB_REASON="$(sed -n 's/.*"code" *: *"\([A-Z_]*\)".*/\1/p' "$body" 2>/dev/null | head -n 1 || true)"
  SUB_RETRY="$(awk -F': *' 'tolower($1) == "retry-after" { gsub(/\r/, "", $2); print $2 }' "$hdr" 2>/dev/null | head -n 1 || true)"
}

# poll_submit_until_ok <秒数>：反复下单直到 202，用时（毫秒）放在 RECOVER_MS，成功的订单号在 SUB_ID。
poll_submit_until_ok() {
  local t0 now
  t0="$(now_ms)"
  RECOVER_MS=-1
  while :; do
    submit_full
    now="$(now_ms)"
    if [ "$SUB_CODE" = 202 ] && [ -n "$SUB_ID" ]; then RECOVER_MS=$((now - t0)); return 0; fi
    if [ $((now - t0)) -gt $(($1 * 1000)) ]; then RECOVER_MS=$((now - t0)); return 1; fi
    sleep 0.3
  done
}

# all_landed <总秒数> <订单号...>：等这些订单全部离开 creating；不是 pending_payment（比如 404、failed）的个数放在 LAND_BAD，
# 各状态的计数放在 LAND_STATES。
all_landed() {
  local deadline=$((SECONDS + $1)) id left states=""
  shift
  LAND_BAD=0
  for id in "$@"; do
    left=$((deadline - SECONDS))
    [ "$left" -ge 1 ] || left=1
    wait_left_creating "$id" "$left" || true
    if [ "$ORDER_STATE" != pending_payment ]; then
      LAND_BAD=$((LAND_BAD + 1))
      states="$states ${id}:${ORDER_STATE:-none}"
    fi
  done
  LAND_STATES="${states:-all pending_payment}"
  [ "$LAND_BAD" = 0 ]
}

cancel_order() {
  curl -s -m 10 -o /dev/null -w '%{http_code}' -X POST -H "X-User-Id: $USER_ID" "$GW/api/v1/orders/$1/cancel"
}

# log_count <服务> <模式>：服务日志里匹配的行数（日志在整个脚本期间累积，场景里只看增量）。
log_count() { grep -c -- "$2" "$RUN_DIR/$1.log" 2>/dev/null || true; }

# dlq_total：所有 fault-<时间戳> 前缀的死信 topic 里的消息总数（场景里只看增量）。
KAFKA_BIN=/opt/kafka/bin
dlq_total() {
  local topics t n=0
  topics="$("${COMPOSE[@]}" exec -T kafka "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server localhost:19092 --list 2>/dev/null |
    tr -d '\r' | grep "^${KAFKA_PREFIX}.*\.dlq$" || true)"
  for t in $topics; do
    n=$((n + $("${COMPOSE[@]}" exec -T kafka "$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server localhost:19092 --topic "$t" 2>/dev/null |
      awk -F: '{ s += $3 } END { print s + 0 }')))
  done
  echo "$n"
}

# stop_svc <服务>：SIGTERM 并等它退出（最多 25 秒），退出码放在 STOP_CODE，耗时（秒）放在 STOP_SECS。
stop_svc() {
  local s="$1" pid t0=$SECONDS
  pid="$(getv "pid_$s")"
  STOP_CODE=-1
  kill -CONT "$pid" 2>/dev/null || true
  kill -TERM "$pid" 2>/dev/null || true
  while kill -0 "$pid" 2>/dev/null && [ $((SECONDS - t0)) -lt 25 ]; do sleep 0.2; done
  STOP_SECS=$((SECONDS - t0))
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
  STOP_CODE=0
  wait "$pid" 2>/dev/null || STOP_CODE=$?
  eval "pid_$s=0"
  log "stopped $s (exit $STOP_CODE after ${STOP_SECS}s)"
}

# restart_order [KEY=VALUE...]：用额外的环境变量重启 order（不带参数就是默认配置）。
restart_order() {
  stop_svc order
  ORDER_ENV="$*"
  start_svc order
}

wait_ready() {
  local i
  for i in $(seq 1 100); do
    if [ "$(curl -s -m 1 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$(port_of "$1")/readyz")" = 200 ]; then return 0; fi
    sleep 0.2
  done
  return 1
}

# 注入故障前先标记环境脏了（on_exit 据此兜底恢复）。
pause_c() { ENV_DIRTY=1; "${COMPOSE[@]}" pause "$1" >/dev/null; log "  paused $1"; }
unpause_c() { "${COMPOSE[@]}" unpause "$1" >/dev/null; log "  unpaused $1"; }
redis_wait() {
  local i
  for i in $(seq 1 60); do
    if "${COMPOSE[@]}" exec -T "$1" redis-cli ping 2>/dev/null | grep -q PONG; then return 0; fi
    sleep 0.5
  done
  return 1
}

# order 的订单所在库号：订单按 user_id 的基因落 slot（slot 数 8），库号 = slot % 库数（2）。
user_db() { echo $(((USER_ID & 7) % 2)); }

# 在查询类请求里按“跳过前 LAT_WARM 次”收集延迟和最大值：lat_add <列表名> <计数名> <毫秒>，最大值放在 <列表名>_max。
LAT_WARM=$WARM_CALLS
lat_add() {
  local n
  n="$(getv "$2")"
  n=$((n + 1))
  eval "$2=$n"
  if [ "$n" -gt "$LAT_WARM" ]; then
    eval "$1=\"\$$1 $3\""
    if [ "$3" -gt "$(getv "$1_max")" ]; then eval "$1_max=$3"; fi
  fi
}

# ---------- 准备 ----------

sidecar_setup
for p in 8080 8081 8082 8083 8084 8085 8089; do
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

# ---------- 场景 F、G：闸门的 Redis 不可用 ----------

# scn_redis_outage <场景名> <pause|stop> <持续秒数> <标题>
# F 用 docker pause（进程冻住，调用超时），G 用 docker stop 再 start（连接被拒绝）。
# 1. 下 3 单并等落库（暂停期间拿它们验证“已落库订单的查询”）。
# 2. 造“已受理未落库”的订单：kill -STOP inventory（落库要同步调 inventory.Reserve，订单停在 creating），
#    受理 3 单，注入故障；故障进行到一半再 kill -CONT。只下 3 单：落库持续失败的熔断要攒够次数才会打开。
# 3. 故障期间持续下单和查订单；结束后恢复，下单要在 15 秒内成功，受理过的订单全部落库，死信为空，没有孤儿占用。
# G 能断言“受理的订单最终落库”，是因为 compose 里闸门的 Redis 开了 AOF（redis-data 卷），stop 之后数据还在。
scn_redis_outage() {
  local name="$1" mode="$2" secs="$3" title="$4"
  local pre="" acc="" id i calls=0 early="" bad=0 okc=0 slow=0 maxms=0 lat="" qn=0 qbad=0 seen_dg=0 t_end p50
  local ej0 ej1 dlq0 dlq1 orph0 orph1 rec_ms pre_bad=0 acc_bad=0
  begin "$name" "$title"
  ENV_DIRTY=1
  env_clean || SC_FAIL=1
  ej0="$(stat $GW_ADMIN $GW_ORD.outlier_detection.ejections_enforced_total)"
  dlq0="$(dlq_total)"
  orph0="$(log_count order 'orphan hold returned')"
  for i in 1 2 3; do
    submit_full
    if [ "$SUB_CODE" = 202 ] && [ -n "$SUB_ID" ]; then pre="$pre $SUB_ID"; else pre_bad=$((pre_bad + 1)); fi
  done
  # shellcheck disable=SC2086
  all_landed 30 $pre || pre_bad=$((pre_bad + 1))
  expect "3 pre-orders accepted and landed before the outage (problems: $pre_bad, $LAND_STATES)" [ "$pre_bad" = 0 ]

  kill -STOP "$pid_inventory"
  for i in 1 2 3; do
    submit_full
    if [ "$SUB_CODE" = 202 ] && [ -n "$SUB_ID" ]; then acc="$acc $SUB_ID"; else acc_bad=$((acc_bad + 1)); fi
  done
  expect "3 orders accepted while inventory is frozen, to be left unlanded (rejected: $acc_bad)" [ "$acc_bad" = 0 ]
  if [ "$mode" = pause ]; then pause_c redis; else ENV_DIRTY=1; "${COMPOSE[@]}" stop redis >/dev/null; log "  stopped redis"; fi
  # inventory 一直冻到故障进行到一半再恢复：受理的订单在整个前半段都停在 creating，恢复之后的落库发生在 Redis 仍然
  # 不可用的时候；落库用到的闸门操作（失败时归还占用等）要靠重试撑到 Redis 恢复。
  t_cont=$((SECONDS + secs / 2))
  thawed=0

  t_end=$((SECONDS + secs))
  while [ "$SECONDS" -lt "$t_end" ]; do
    if [ "$thawed" = 0 ] && [ "$SECONDS" -ge "$t_cont" ]; then
      # 记一下解冻时这几单的状态（希望看到 creating：已受理、未落库），只打印，不做断言。
      st=""
      for id in $acc; do st="$st $(order_status "$id")"; done
      log "  states of the 3 orders accepted before the injection, just before inventory is thawed:${st:- (none)}"
      kill -CONT "$pid_inventory"
      thawed=1
    fi
    submit_full
    calls=$((calls + 1))
    if [ "$calls" -le "$WARM_CALLS" ]; then
      early="$early $SUB_CODE/${SUB_MS}ms"
    else
      if [ "$SUB_CODE" = 503 ] && [ "$SUB_REASON" = STOCK_GATE_UNAVAILABLE ] && [ -n "$SUB_RETRY" ]; then
        okc=$((okc + 1))
      else
        bad=$((bad + 1))
        [ "$bad" -gt 3 ] || log "  unexpected submit response: $SUB_CODE reason='$SUB_REASON' retry-after='$SUB_RETRY' ${SUB_MS}ms"
      fi
      lat="$lat $SUB_MS"
      if [ "$SUB_MS" -gt "$FAST_MS" ]; then slow=$((slow + 1)); fi
      if [ "$SUB_MS" -gt "$maxms" ]; then maxms="$SUB_MS"; fi
    fi
    for id in $pre; do
      split_hc "$(http_code GET "/api/v1/orders/$id")"
      qn=$((qn + 1))
      if [ "$HC_CODE" != 200 ]; then qbad=$((qbad + 1)); fi
    done
    if [ "$(dg_field order admission degraded)" = true ]; then seen_dg=$((seen_dg + 1)); fi
    sleep 0.4
  done
  if [ "$thawed" = 0 ]; then kill -CONT "$pid_inventory"; fi
  log "  first $WARM_CALLS submits (code/latency):$early"
  log "  admission detail: $(dg_field order admission detail)"
  ej1="$(stat $GW_ADMIN $GW_ORD.outlier_detection.ejections_enforced_total)"
  # shellcheck disable=SC2086
  p50="$(percentile 50 ${lat:-0})"
  expect "after the first $WARM_CALLS submits: $okc x 503 STOCK_GATE_UNAVAILABLE with Retry-After (other: $bad)" [ "$okc" -ge 5 -a "$bad" = 0 ]
  expect "submit latency p50 ${p50}ms <= ${FAST_MS}ms; slow (half-open probes) $slow <= $((secs / 5 + 2)), max ${maxms}ms <= ${PROBE_SLOW_MAX}ms" \
    [ "$p50" -le "$FAST_MS" -a "$slow" -le $((secs / 5 + 2)) -a "$maxms" -le "$PROBE_SLOW_MAX" ]
  expect "$qn queries of landed orders during the outage, non-200: $qbad" [ "$qn" -gt 0 -a "$qbad" = 0 ]
  expect "order admission switch seen degraded in $seen_dg polls" [ "$seen_dg" -ge 1 ]
  expect "gateway sidecar to_order ejections_enforced_total +$((ej1 - ej0)) (need 0)" [ "$((ej1 - ej0))" = 0 ]

  if [ "$mode" = pause ]; then
    unpause_c redis
  else
    "${COMPOSE[@]}" start redis >/dev/null
    redis_wait redis || log "  warning: redis not answering PING 30s after start"
    log "  started redis"
  fi
  poll_submit_until_ok 15 || true
  rec_ms="$RECOVER_MS"
  expect "submit recovered ${rec_ms}ms after the Redis came back (limit 15000ms)" [ "$rec_ms" -ge 0 -a "$rec_ms" -le 15000 ]
  if [ -n "$SUB_ID" ] && [ "$SUB_CODE" = 202 ]; then acc="$acc $SUB_ID"; fi
  # shellcheck disable=SC2086
  all_landed 90 $acc || true
  expect "orders accepted before/after the outage all landed (pending_payment): $LAND_STATES" [ "$LAND_BAD" = 0 ]
  dlq1="$(dlq_total)"
  orph1="$(log_count order 'orphan hold returned')"
  expect "dead letter messages +$((dlq1 - dlq0)) (need 0)" [ "$dlq1" = "$dlq0" ]
  expect "order log 'orphan hold returned' +$((orph1 - orph0)) (need 0)" [ "$orph1" = "$orph0" ]
  SC_NOTE="submits $calls, 503 ok $okc/bad $bad, p50 ${p50}ms max ${maxms}ms slow $slow, queries $qn bad $qbad, ejections +$((ej1 - ej0)), recovery ${rec_ms}ms, dlq +$((dlq1 - dlq0)), orphans +$((orph1 - orph0))"
  finish "$name" "$title"
}

if wanted F; then scn_redis_outage F pause 30 "gate Redis paused 30s"; fi
if wanted G; then scn_redis_outage G stop 20 "gate Redis stopped and restarted"; fi

# ---------- 场景 H：缓存的 Redis 不可用 ----------

if wanted H; then
  begin H "cache Redis paused 20s"
  ENV_DIRTY=1
  env_clean || SC_FAIL=1
  # 造一个终态订单（已取消）：终态订单走缓存。先读几次把缓存填上。
  submit_full
  HID="$SUB_ID"
  wait_left_creating "$HID" 30 || true
  cc="$(cancel_order "$HID")"
  expect "order $HID landed and cancelled (cancel $cc, state before ${ORDER_STATE:-none}, after $(order_status "$HID"))" [ "$cc" = 200 -a "$(order_status "$HID")" = cancelled ]
  lo="" ls=""
  for i in $(seq 1 12); do
    split_hc "$(http_code GET "/api/v1/orders/$HID")"
    lo="$lo $HC_MS"
    split_hc "$(http_code GET "$(stock_path $SKU)")"
    ls="$ls $HC_MS"
  done
  # shellcheck disable=SC2086
  bo="$(percentile 50 $lo)"
  # shellcheck disable=SC2086
  bs="$(percentile 50 $ls)"
  [ "$bo" -ge 1 ] || bo=1
  [ "$bs" -ge 1 ] || bs=1
  # 上限：基线 p50 的 10 倍，至少 150ms。
  lim_o=$((bo * 10 > 150 ? bo * 10 : 150))
  lim_s=$((bs * 10 > 150 ? bs * 10 : 150))
  pause_c redis-cache
  # 缓存的 Redis 每次调用要等 1 秒的读超时，一次请求（读缓存、回填）最多慢 2 秒，熔断器攒够 5 次失败才打开，
  # 所以头几个请求慢是预期的（跳过前 6 次）；窗口给到 20 秒，熔断器打开之后还有足够的样本。
  LAT_WARM=6
  t_end=$((SECONDS + 20))
  no=0 ns=0 lo="" ls="" bad=0 total=0 lo_max=0 ls_max=0 dgo=0 dgi=0
  while [ "$SECONDS" -lt "$t_end" ]; do
    split_hc "$(http_code GET "/api/v1/orders/$HID")"
    total=$((total + 1))
    [ "$HC_CODE" = 200 ] || bad=$((bad + 1))
    lat_add lo no "$HC_MS"
    split_hc "$(http_code GET "$(stock_path $SKU)")"
    total=$((total + 1))
    [ "$HC_CODE" = 200 ] || bad=$((bad + 1))
    lat_add ls ns "$HC_MS"
    if [ "$(dg_field order cache degraded)" = true ]; then dgo=$((dgo + 1)); fi
    if [ "$(dg_field inventory cache degraded)" = true ]; then dgi=$((dgi + 1)); fi
    sleep 0.2
  done
  LAT_WARM=$WARM_CALLS
  maxo=$lo_max
  maxs=$ls_max
  # shellcheck disable=SC2086
  po="$(percentile 50 ${lo:-0})"
  # shellcheck disable=SC2086
  ps="$(percentile 50 ${ls:-0})"
  expect "$total queries (order + stock) during the cache outage, non-200: $bad" [ "$total" -gt 0 -a "$bad" = 0 ]
  expect "order query p50 ${po}ms <= ${lim_o}ms (baseline ${bo}ms), max ${maxo}ms <= ${PROBE_SLOW_MAX}ms" [ "$po" -le "$lim_o" -a "$maxo" -le "$PROBE_SLOW_MAX" ]
  expect "stock query p50 ${ps}ms <= ${lim_s}ms (baseline ${bs}ms), max ${maxs}ms <= ${PROBE_SLOW_MAX}ms" [ "$ps" -le "$lim_s" -a "$maxs" -le "$PROBE_SLOW_MAX" ]
  expect "order cache switch seen degraded in $dgo polls" [ "$dgo" -ge 1 ]
  expect "inventory cache switch seen degraded in $dgi polls" [ "$dgi" -ge 1 ]
  unpause_c redis-cache
  wait_dg order cache false 20 || true
  r1="$DG_MS"
  wait_dg inventory cache false 20 || true
  r2="$DG_MS"
  expect "order cache back to normal ${r1}ms after unpause (limit 20000ms)" [ "$r1" -ge 0 -a "$r1" -le 20000 ]
  expect "inventory cache back to normal ${r2}ms after unpause (limit 20000ms)" [ "$r2" -ge 0 -a "$r2" -le 20000 ]
  SC_NOTE="queries $total bad $bad, order p50 ${po}ms/max ${maxo}ms (base ${bo}), stock p50 ${ps}ms/max ${maxs}ms (base ${bs}), degraded seen order $dgo inventory $dgi, recovery ${r1}/${r2}ms"
  finish H "cache Redis down"
fi

# ---------- 场景 I：从库停止回放 ----------

# 服务要以 ORDER_READ_MODE=gtid 启动（读从库前要确认从库已经执行到写入的位点）。从库的 SQL 线程停了以后，
# 探测发现它落后，replica_read 降级，读走主库，所以刚落库的订单读到的是最新状态。
if wanted I; then
  begin I "replica SQL thread stopped"
  ENV_DIRTY=1
  restart_order ORDER_READ_MODE=gtid
  env_clean || SC_FAIL=1
  DB="$(user_db)"
  REPLICA="mysql-order-$DB-replica"
  # 从库的健康探测比的是“从库有没有追上主库的 GTID 集合”（WAIT_FOR_EXECUTED_GTID_SET）：主库上没有新写入时，
  # SQL 线程停了的从库和主库集合相同，读它是对的，不算落后。所以停了之后要在主库上写一笔（下一单），才有落后可探测。
  t_stop="$(now_ms)"
  mysql_on "$REPLICA" "STOP REPLICA SQL_THREAD" || log "  warning: STOP REPLICA SQL_THREAD failed"
  submit_full
  expect "a write on the primary after the stop (submit got $SUB_CODE)" [ "$SUB_CODE" = 202 ]
  wait_dg order replica_read true 3 || true
  d1=$(($(now_ms) - t_stop))
  det="$(dg_field order replica_read detail)"
  expect "replica_read degraded ${d1}ms after STOP REPLICA SQL_THREAD (limit 3000ms)" [ "$DG_MS" -ge 0 -a "$d1" -le 3000 ]
  expect "detail shows db_$DB unhealthy: $det" sh -c "echo '$det' | grep -q 'db_$DB=unhealthy'"
  submit_full
  NID="$SUB_ID"
  expect "order accepted while the replica is stopped (got $SUB_CODE)" [ "$SUB_CODE" = 202 -a -n "$NID" ]
  wait_left_creating "$NID" 30 || true
  st="$(order_status "$NID")"
  expect "freshly landed order read back as pending_payment, not stale (got ${st:-none})" [ "$st" = pending_payment ]
  cc="$(cancel_order "$NID")"
  st="$(order_status "$NID")"
  expect "order read right after cancel is cancelled (cancel $cc, got ${st:-none})" [ "$cc" = 200 -a "$st" = cancelled ]
  mysql_on "$REPLICA" "START REPLICA SQL_THREAD" || log "  warning: START REPLICA SQL_THREAD failed"
  wait_dg order replica_read false 10 || true
  r1="$DG_MS"
  expect "replica_read healthy ${r1}ms after START REPLICA SQL_THREAD (limit 10000ms)" [ "$r1" -ge 0 -a "$r1" -le 10000 ]
  restart_order
  SC_NOTE="degraded after ${d1}ms, db_$DB unhealthy, fresh reads ok, healthy again after ${r1}ms"
  finish I "replica stopped replaying"
fi

# ---------- 场景 J：暂停一个从库 ----------

if wanted J; then
  begin J "replica container paused 10s"
  ENV_DIRTY=1
  restart_order ORDER_READ_MODE=gtid
  env_clean || SC_FAIL=1
  DB="$(user_db)"
  pause_c "mysql-order-$DB-replica"
  t_end=$((SECONDS + 10))
  nq=0 lo="" total=0 bad=0 lo_max=0 dgs=0
  while [ "$SECONDS" -lt "$t_end" ]; do
    split_hc "$(http_code GET "/api/v1/orders/$BASE_ORDER")"
    total=$((total + 1))
    [ "$HC_CODE" = 200 ] || bad=$((bad + 1))
    lat_add lo nq "$HC_MS"
    if [ "$(dg_field order replica_read degraded)" = true ]; then dgs=$((dgs + 1)); fi
    sleep 0.2
  done
  maxo=$lo_max
  # shellcheck disable=SC2086
  po="$(percentile 50 ${lo:-0})"
  log "  replica_read detail: $(dg_field order replica_read detail)"
  expect "$total order queries during the pause, non-200: $bad" [ "$total" -gt 0 -a "$bad" = 0 ]
  expect "after the first $WARM_CALLS: p50 ${po}ms <= ${ORDER_LIMIT}ms (baseline ${ORDER_P50}ms), max ${maxo}ms" [ "$po" -le "$ORDER_LIMIT" ]
  expect "replica_read seen degraded in $dgs polls" [ "$dgs" -ge 1 ]
  unpause_c "mysql-order-$DB-replica"
  wait_dg order replica_read false 15 || true
  r1="$DG_MS"
  expect "replica_read healthy ${r1}ms after unpause (limit 15000ms)" [ "$r1" -ge 0 -a "$r1" -le 15000 ]
  restart_order
  SC_NOTE="queries $total bad $bad, p50 ${po}ms max ${maxo}ms (base ${ORDER_P50}ms), degraded seen $dgs, healthy again after ${r1}ms"
  finish J "replica paused"
fi

# ---------- 场景 K：冷库不通 ----------

if wanted K; then
  begin K "archive DB unreachable at startup"
  ENV_DIRTY=1
  restart_order "ARCHIVE_DSN=root:x@tcp(127.0.0.1:1)/x"
  expect "order healthy with a dead archive DSN" [ "$(curl -s -m 2 -o /dev/null -w '%{http_code}' http://127.0.0.1:8081/healthz)" = 200 ]
  expect "order ready with a dead archive DSN" wait_ready order
  submit_full
  NID="$SUB_ID"
  expect "create order accepted (got $SUB_CODE)" [ "$SUB_CODE" = 202 -a -n "$NID" ]
  wait_left_creating "$NID" 30 || true
  expect "order landed as pending_payment (got ${ORDER_STATE:-none})" [ "$ORDER_STATE" = pending_payment ]
  wait_dg order archive_read true 10 || true
  expect "archive_read degraded: $(dg_field order archive_read detail)" [ "$DG_MS" -ge 0 ]
  # 一年多以前的订单号：时间戳在 Epoch 之后 ((now - 400 天 - Epoch) 毫秒) 左移 22 位，低 6 位是基因（按 user_id）。
  OLD_ID="$(python3 -I -c 'import sys, time; ms = int((time.time() - 400 * 86400 - 1577836800) * 1000); print((ms << 22) | (int(sys.argv[1]) & 63))' "$USER_ID")"
  split_hc "$(http_code GET "/api/v1/orders/$OLD_ID")"
  OLD_REASON="$(sed -n 's/.*"code" *: *"\([A-Z_]*\)".*/\1/p' "$RUN_DIR/last-body" | head -n 1)"
  expect "old order $OLD_ID: 503 ARCHIVE_UNAVAILABLE within 500ms (got $HC_CODE $OLD_REASON ${HC_MS}ms)" \
    [ "$HC_CODE" = 503 -a "$OLD_REASON" = ARCHIVE_UNAVAILABLE -a "$HC_MS" -le 500 ]
  old_ms="$HC_MS"
  lcode="$(curl -s -m 10 -o "$RUN_DIR/list-body" -w '%{http_code}' -H "X-User-Id: $USER_ID" "$GW/api/v1/orders")"
  expect "user order list 200 (got $lcode)" [ "$lcode" = 200 ]
  expect "list contains the new order $NID" grep -q "\"$NID\"" "$RUN_DIR/list-body"
  expect "list carries archive_unavailable=true" grep -Eq '"archive_unavailable" *: *true' "$RUN_DIR/list-body"
  restart_order
  wait_dg order archive_read false 15 || true
  expect "archive_read back to normal ${DG_MS}ms after restart with the real DSN" [ "$DG_MS" -ge 0 ]
  SC_NOTE="order up, landed; old order 503 ARCHIVE_UNAVAILABLE in ${old_ms}ms; list 200 with archive_unavailable"
  finish K "archive DB down"
fi

# ---------- 场景 L：inventory 挂住且持续下单 ----------

# 落库要同步调 inventory.Reserve。inventory 挂住后落库持续失败，受理在失败攒够之后拒绝新单（ORDER_LANDING_UNAVAILABLE），
# 不再出现“202 之后失败”。挂住期间拿到 202 的订单数要有限（这里上限 30，实测个位数到十几个）。
LAND_ACCEPT_MAX=30 # 挂住期间允许受理的订单数上限
if wanted L; then
  begin L "inventory hung while submitting"
  env_clean || SC_FAIL=1
  submit_full
  expect "first order accepted before the hang (got $SUB_CODE)" [ "$SUB_CODE" = 202 -a -n "$SUB_ID" ]
  wait_left_creating "$SUB_ID" 30 || true
  expect "first order landed (got ${ORDER_STATE:-none})" [ "$ORDER_STATE" = pending_payment ]
  kill -STOP "$pid_inventory"
  t_end=$((SECONDS + 20))
  calls=0 acc="" n202=0 n503=0 other=0 early="" first503=0 late202=0 maxms=0
  while [ "$SECONDS" -lt "$t_end" ]; do
    submit_full
    calls=$((calls + 1))
    if [ "$SUB_CODE" = 202 ]; then
      n202=$((n202 + 1))
      acc="$acc $SUB_ID"
      [ "$calls" -le "$WARM_CALLS" ] || late202=$((late202 + 1))
    elif [ "$SUB_CODE" = 503 ] && [ "$SUB_REASON" = ORDER_LANDING_UNAVAILABLE ] && [ -n "$SUB_RETRY" ]; then
      n503=$((n503 + 1))
    else
      other=$((other + 1))
      log "  unexpected submit response: $SUB_CODE reason='$SUB_REASON' ${SUB_MS}ms"
    fi
    if [ "$calls" -le "$WARM_CALLS" ]; then early="$early $SUB_CODE/${SUB_MS}ms"; fi
    if [ "$SUB_MS" -gt "$maxms" ]; then maxms="$SUB_MS"; fi
    sleep 0.3
  done
  log "  first $WARM_CALLS submits (code/latency):$early"
  log "  admission detail: $(dg_field order admission detail)"
  kill -CONT "$pid_inventory"
  expect "submits: $n202 accepted (202), $n503 x 503 ORDER_LANDING_UNAVAILABLE with Retry-After, other $other" [ "$n503" -ge 1 -a "$other" = 0 ]
  expect "orders accepted during the hang: $n202 (limit $LAND_ACCEPT_MAX)" [ "$n202" -le "$LAND_ACCEPT_MAX" ]
  poll_submit_until_ok 30 || true
  rec_ms="$RECOVER_MS"
  expect "submit recovered ${rec_ms}ms after SIGCONT (limit 30000ms)" [ "$rec_ms" -ge 0 -a "$rec_ms" -le 30000 ]
  if [ -n "$SUB_ID" ] && [ "$SUB_CODE" = 202 ]; then acc="$acc $SUB_ID"; fi
  # shellcheck disable=SC2086
  all_landed 90 $acc || true
  expect "orders accepted during the hang all landed (pending_payment): $LAND_STATES" [ "$LAND_BAD" = 0 ]
  SC_NOTE="$calls submits in 20s: 202 x $n202 (after the first $WARM_CALLS: $late202), 503 x $n503, max ${maxms}ms, recovery ${rec_ms}ms"
  finish L "inventory hung, submitting"
fi

# ---------- 场景 M：带流量 SIGTERM order ----------

if wanted M; then
  begin M "SIGTERM order under load"
  env_clean || SC_FAIL=1
  FLAG="$RUN_DIR/loader.run"
  ACC_M="$RUN_DIR/scenario-m-accepted.txt"
  : >"$FLAG"
  : >"$ACC_M"
  # 后台持续下单：只记 202 的订单号；用自己的临时文件，不和前台的 submit_full 互相覆盖。
  (
    set +e
    SUB_TAG=bg
    while [ -f "$FLAG" ]; do
      submit_full
      if [ "$SUB_CODE" = 202 ] && [ -n "$SUB_ID" ]; then echo "$SUB_ID" >>"$ACC_M"; fi
      sleep 0.1
    done
  ) &
  LOADER_PID=$!
  sleep 3
  orph0="$(log_count order 'orphan hold returned')"
  before="$(wc -l <"$ACC_M" | tr -d ' ')"
  kill -TERM "$pid_order"
  t0=$SECONDS
  while kill -0 "$pid_order" 2>/dev/null && [ $((SECONDS - t0)) -lt 25 ]; do sleep 0.1; done
  exit_s=$((SECONDS - t0))
  if kill -0 "$pid_order" 2>/dev/null; then kill -KILL "$pid_order" 2>/dev/null || true; fi
  ocode=0
  wait "$pid_order" 2>/dev/null || ocode=$?
  pid_order=0
  rm -f "$FLAG"
  wait "$LOADER_PID" 2>/dev/null || true
  LOADER_PID=0
  accepted="$(wc -l <"$ACC_M" | tr -d ' ')"
  log "  accepted before SIGTERM ~$before, total accepted $accepted"
  expect "order exited ${exit_s}s after SIGTERM (limit 20s)" [ "$exit_s" -le 20 ]
  expect "order exit code $ocode (need 0)" [ "$ocode" = 0 ]
  start_svc order
  split_submit "$(submit_order)"
  expect "submit works after the restart (got $SUB_CODE)" [ "$SUB_CODE" = 202 -a -n "$SUB_ID" ]
  post_id="$SUB_ID"
  # shellcheck disable=SC2046
  all_landed 90 $(cat "$ACC_M") "$post_id" || true
  expect "$accepted orders accepted under load + 1 after the restart all landed: $LAND_STATES" [ "$LAND_BAD" = 0 ]
  orph1="$(log_count order 'orphan hold returned')"
  expect "order log 'orphan hold returned' +$((orph1 - orph0)) (need 0)" [ "$orph1" = "$orph0" ]
  SC_NOTE="exit ${exit_s}s code $ocode, accepted $accepted, not landed $LAND_BAD, orphans +$((orph1 - orph0))"
  finish M "SIGTERM order under load"
fi

# ---------- 场景 N：Redis 停着时启动 ----------

if wanted N; then
  begin N "start payment and order while Redis is paused"
  ENV_DIRTY=1
  env_clean || SC_FAIL=1
  stop_svc payment
  stop_svc order
  pause_c redis
  start_svc payment
  start_svc order
  expect "payment ready with Redis paused" wait_ready payment
  expect "order ready with Redis paused" wait_ready order
  # 重启期间 gateway 的边车可能暂时摘除了 order（连接被拒绝），等摘除期满，免得把摘除当成降级。
  for i in $(seq 1 60); do
    [ "$(ejections_active_total)" = 0 ] && break
    sleep 0.5
  done
  qbad=0 qn=0 sub_bad=0 sn=0
  for i in 1 2 3 4 5 6; do
    split_hc "$(http_code GET "/api/v1/orders/$BASE_ORDER")"
    qn=$((qn + 1))
    [ "$HC_CODE" = 200 ] || qbad=$((qbad + 1))
    submit_full
    sn=$((sn + 1))
    if [ "$SUB_CODE" != 503 ] || [ "$SUB_REASON" != STOCK_GATE_UNAVAILABLE ]; then
      sub_bad=$((sub_bad + 1))
      log "  unexpected submit response: $SUB_CODE reason='$SUB_REASON' ${SUB_MS}ms"
    fi
  done
  expect "$qn queries of a landed order with Redis paused, non-200: $qbad" [ "$qbad" = 0 ]
  expect "$sn submits with Redis paused all 503 STOCK_GATE_UNAVAILABLE (other: $sub_bad)" [ "$sub_bad" = 0 ]
  unpause_c redis
  poll_submit_until_ok 15 || true
  expect "submit recovered ${RECOVER_MS}ms after unpause (limit 15000ms)" [ "$RECOVER_MS" -ge 0 -a "$RECOVER_MS" -le 15000 ]
  SC_NOTE="payment+order ready with Redis paused, queries $qn bad $qbad, submits $sn bad $sub_bad, recovery ${RECOVER_MS}ms"
  finish N "start with Redis down"
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
