#!/usr/bin/env bash
# 订单库 2 库扩 4 库的演练脚本（设计文档 1c 的 5.4 到 5.8，ADR-0015）。
#
# 只管 MySQL 这一层：不启动、停止、重启 order、cdc 等应用服务，切路由由人来做（见 `env` 子命令）。
# 每个阶段是一个子命令，可单独重跑（幂等）。所有 SQL 在执行前打印（密码打码）。
#
# 阶段顺序：  prepare -> check -> fence -> wait -> promote -> verify -> cleanup
# 其他：      status  env  rollback（只能在 promote 之前）
#
# 布局（常量，与设计文档一致）：8 个 slot，2 个库扩成 4 个库，db = slot % 库数。
#   旧库 j（0、1）搬走 slot % 4 == j+2 的 slot：库 0 搬 2、6 到库 2，库 1 搬 3、7 到库 3。
#   新库 k = j+2，在新实例 mysql-order-k 上，从旧主库 mysql-order-j 拷贝并复制而来。
#
# 环境变量：
#   DB_PREFIX             库名前缀，默认空（order_db_0）；演练用 drill_
#   DRY_RUN=1             只打印，不执行会改变状态的语句（只读查询仍会执行，用来判断幂等）
#   MYSQL_ROOT_PASSWORD   默认 oppenheimer；MYSQL_REPL_PASSWORD 默认 oppenheimer-repl（同 mysql-replication.sh）
#   MAX_LAG_SECONDS       check 允许的复制延迟，默认 5
#   LONG_TRX_SECONDS      check 里“长事务”的阈值，默认 5
#   WAIT_GTID_TIMEOUT     wait (a) 新实例追上围栏 GTID 的超时秒数，默认 60（必须为正数，0 在 MySQL 里是无限等）
#   OUTBOX_TIMEOUT        wait (b) 旧库 outbox 发完的超时秒数，默认 60
#   CDC_WAIT_CMD          wait (c) 的钩子：每秒执行一次，返回 0 表示 CDC 已追上；环境里带
#                         FENCE_GTID_0、FENCE_GTID_1。没设时打印警告并固定等 CDC_FALLBACK_SECONDS（默认 5）
#   CDC_TIMEOUT           CDC_WAIT_CMD 的超时秒数，默认 60
#   CUTOVER_STEP_TIMEOUT  cutover 里 wait 的三项各自的超时秒数，默认 5；任何一项超时就撤掉围栏
#   CONFIRM=yes           cleanup 必需
#
# 另有三个子命令：
#   cutover   依次 fence、wait、promote；wait 超出预算时自动 unfence
#   unfence   promote 之前撤掉围栏（把 moved_* 改回原名），新实例保持从库身份
#   replicas  promote 之后给两个新主库各建一个从库（不在不可用窗口内）
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$ROOT/deploy/compose/docker-compose.yml"
STATE_DIR="$ROOT/tmp/expand"
MIGRATION="$ROOT/services/order/migrations/00003_user_index_shard_meta.sql"

DB_PREFIX="${DB_PREFIX:-}"
DRY_RUN="${DRY_RUN:-0}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"
MYSQL_REPL_PASSWORD="${MYSQL_REPL_PASSWORD:-oppenheimer-repl}"
MYSQL_CDC_PASSWORD="${MYSQL_CDC_PASSWORD:-oppenheimer-cdc}"
REPL_USER=repl
MAX_LAG_SECONDS="${MAX_LAG_SECONDS:-5}"
LONG_TRX_SECONDS="${LONG_TRX_SECONDS:-5}"
WAIT_GTID_TIMEOUT="${WAIT_GTID_TIMEOUT:-60}"
OUTBOX_TIMEOUT="${OUTBOX_TIMEOUT:-60}"
CDC_TIMEOUT="${CDC_TIMEOUT:-60}"
CDC_FALLBACK_SECONDS="${CDC_FALLBACK_SECONDS:-5}"
CUTOVER_STEP_TIMEOUT="${CUTOVER_STEP_TIMEOUT:-5}"
FENCE_LOCK_TIMEOUT=5
FENCE_RETRIES=3

SLOT_COUNT=8
OLD_N=2
NEW_N=4
OLD_IDX=(0 1) # 旧库序号 j；对应的新库序号是 j + OLD_N

# 宿主机端口，与 compose、run-local.sh 一致，只用于 env 子命令。
PORT_ORDER_0="${MYSQL_ORDER_0_PORT:-3311}"
PORT_ORDER_0_R="${MYSQL_ORDER_0_REPLICA_PORT:-3312}"
PORT_ORDER_1="${MYSQL_ORDER_1_PORT:-3321}"
PORT_ORDER_1_R="${MYSQL_ORDER_1_REPLICA_PORT:-3322}"
PORT_ORDER_2="${MYSQL_ORDER_2_PORT:-3331}"
PORT_ORDER_2_R="${MYSQL_ORDER_2_REPLICA_PORT:-3332}"
PORT_ORDER_3="${MYSQL_ORDER_3_PORT:-3341}"
PORT_ORDER_3_R="${MYSQL_ORDER_3_REPLICA_PORT:-3342}"

case "$DB_PREFIX" in
*[!A-Za-z0-9_]*)
  echo "DB_PREFIX 只能包含字母、数字、下划线: $DB_PREFIX" >&2
  exit 2
  ;;
esac

STATE_FILE="$STATE_DIR/state.${DB_PREFIX:-default}.env"
ERRF="$(mktemp "${TMPDIR:-/tmp}/expand.err.XXXXXX")"
trap 'command rm -f "$ERRF"' EXIT

# ---------------------------------------------------------------- 基础函数

die() {
  echo "错误: $*" >&2
  exit 1
}
say() { echo "==> $*"; }
warn() { echo "!!! $*" >&2; }

dc() { docker compose -f "$COMPOSE_FILE" --profile expand "$@"; }

# 当前时间（秒，小数），macOS 的 date 没有 %N。
hr_now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }

old_db() { echo "${DB_PREFIX}order_db_$1"; }   # 旧库 j 的库名，同样是新实例上那份拷贝的库名
new_db() { echo "${DB_PREFIX}order_db_$1"; }   # 新库 k 的库名
old_svc() { echo "mysql-order-$1"; }
new_svc() { echo "mysql-order-$1"; }
new_replica_svc() { echo "mysql-order-$1-replica"; }

# 把 SQL 压成一行并给密码打码后打印。
log_sql() {
  local svc="$1" sql="$2" one
  one="$(printf '%s' "$sql" | tr '\n' ' ' | sed -e 's/  */ /g' \
    -e "s/\(SOURCE_PASSWORD=\)'[^']*'/\1'***'/g" -e "s/\(IDENTIFIED BY \)'[^']*'/\1'***'/g")"
  printf '  [sql] %s> %s\n' "$svc" "$one" >&2
}

# 在容器里跑 mysql 客户端。成功时丢弃 stderr；失败时 stderr 留在 $ERRF 里，由调用方决定是否显示。
run_mysql() {
  local svc="$1"
  shift
  dc exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" "$svc" mysql -uroot --default-character-set=utf8mb4 "$@" 2>"$ERRF"
}

show_err() { grep -v 'Using a password' "$ERRF" >&2 || true; }

# 只读查询：总是执行，输出无表头的结果。失败时终止脚本。
rd() {
  local svc="$1" sql="$2"
  log_sql "$svc" "$sql"
  run_mysql "$svc" -N -B -e "$sql" || {
    show_err
    die "查询失败: $svc"
  }
}

# 同 rd，但失败时只返回非零，不终止（调用方自己处理）。
rd_try() {
  local svc="$1" sql="$2"
  log_sql "$svc" "$sql"
  run_mysql "$svc" -N -B -e "$sql"
}

# 会改变状态的语句：DRY_RUN=1 时只打印。
wr() {
  local svc="$1" sql="$2"
  log_sql "$svc" "$sql"
  if [ "$DRY_RUN" = 1 ]; then
    echo "        (DRY_RUN，未执行)" >&2
    return 0
  fi
  run_mysql "$svc" -e "$sql" || {
    show_err
    die "语句失败: $svc"
  }
}

# 不打印 SQL 的静默探测（轮询用），失败返回非零。
probe() { run_mysql "$1" -N -B -e "$2"; }

svc_running() { [ -n "$(dc ps --status running -q "$1" 2>/dev/null)" ]; }

need_running() {
  local s
  for s in "$@"; do
    svc_running "$s" || die "容器 $s 没有运行。先 make up（旧实例）或 make expand-up（新实例）"
  done
}

wait_mysql_up() { # <svc> <秒数>
  local svc="$1" limit="$2" deadline=$((SECONDS + $2))
  until probe "$svc" "SELECT 1" >/dev/null 2>&1; do
    [ "$SECONDS" -lt "$deadline" ] || die "$svc 在 ${limit}s 内没有恢复"
    sleep 1
  done
}

# ---------------------------------------------------------------- 状态文件

state_get() { # <key>
  [ -f "$STATE_FILE" ] || return 0
  grep "^$1=" "$STATE_FILE" | tail -1 | cut -d= -f2- || true
}

state_set() { # <key> <value>
  if [ "$DRY_RUN" = 1 ]; then
    echo "  [state] $1=$2 (DRY_RUN，未写入)" >&2
    return 0
  fi
  mkdir -p "$STATE_DIR"
  touch "$STATE_FILE"
  { grep -v "^$1=" "$STATE_FILE" || true; } >"$STATE_FILE.tmp"
  echo "$1=$2" >>"$STATE_FILE.tmp"
  mv "$STATE_FILE.tmp" "$STATE_FILE"
}

# ---------------------------------------------------------------- 布局计算

# 旧库 j 要搬走的 slot（空格分隔）：slot % NEW_N == j + OLD_N。
moved_slots() {
  local j="$1" s out=""
  for ((s = 0; s < SLOT_COUNT; s++)); do
    [ $((s % NEW_N)) -eq $((j + OLD_N)) ] && out="$out $s"
  done
  echo "${out# }"
}

# 库序号 d（共 n 个库）持有的 slot，逗号分隔升序，即 shard_meta.slots。
slots_csv() { # <d> <n>
  local d="$1" n="$2" s out=""
  for ((s = 0; s < SLOT_COUNT; s++)); do
    [ $((s % n)) -eq "$d" ] && out="$out,$s"
  done
  echo "${out#,}"
}

# 旧库 j 上要搬走的所有表名（空格分隔）。
moved_tables() {
  local s t out=""
  for s in $(moved_slots "$1"); do
    for t in orders order_items payment_results; do out="$out ${t}_$s"; done
  done
  echo "${out# }"
}

# 单条 RENAME TABLE 的主体：旧库 j 内原名 -> moved_ 名（reverse=1 时反过来）。
fence_rename_sql() { # <j> [reverse]
  local j="$1" rev="${2:-0}" db t out=""
  db="$(old_db "$j")"
  for t in $(moved_tables "$j"); do
    if [ "$rev" = 1 ]; then
      out="$out, $db.moved_$t TO $db.$t"
    else
      out="$out, $db.$t TO $db.moved_$t"
    fi
  done
  echo "RENAME TABLE ${out#, }"
}

count_tables() { # <svc> <db> <表名列表...>：这些表里实际存在几张
  local svc="$1" db="$2" list="" t
  shift 2
  for t in "$@"; do list="$list,'$t'"; done
  rd "$svc" "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$db' AND table_name IN (${list#,})"
}

prefixed_names() { # <前缀> <表名...>
  local p="$1" t out=""
  shift
  for t in "$@"; do out="$out $p$t"; done
  echo "${out# }"
}

# ---------------------------------------------------------------- 复制相关

replica_status() { # <svc>：打印关心的字段 key=value，没配置复制则为空
  run_mysql "$1" --vertical -e "SHOW REPLICA STATUS" 2>/dev/null |
    awk -F': ' '/^ *(Replica_IO_Running|Replica_SQL_Running|Last_IO_Error|Last_SQL_Error|Seconds_Behind_Source|Source_Host):/ {
      sub(/^ +/, "", $1); print $1 "=" $2 }' || true
}

field() { awk -F= -v k="$2" '$1 == k { print $2 }' <<<"$1"; }

replica_running() { # <svc>
  local st
  st="$(replica_status "$1")"
  [ "$(field "$st" Replica_IO_Running)" = Yes ] && [ "$(field "$st" Replica_SQL_Running)" = Yes ]
}

# 实例的 gtid_executed 里有没有别的 UUID 的事务：全新实例只有自己初始化时的几个事务，
# CLONE 之后 gtid_executed 被替换成捐赠方的，所以“有外来 UUID”等于“已经拷过数据”。
is_cloned() { # <svc>
  local n
  n="$(probe "$1" "SELECT GTID_SUBTRACT(@@GLOBAL.gtid_executed, CONCAT(@@GLOBAL.server_uuid, ':1-1000000000')) <> ''" 2>/dev/null || echo 0)"
  [ "$n" = 1 ]
}

ensure_clone_plugin() { # <svc>
  local n
  n="$(rd "$1" "SELECT COUNT(*) FROM information_schema.plugins WHERE plugin_name='clone' AND plugin_status='ACTIVE'")"
  [ "$n" = 1 ] || wr "$1" "INSTALL PLUGIN clone SONAME 'mysql_clone.so'"
}

# CLONE：把 donor 的全部数据（所有库、GTID 状态）拷到 recipient。完成后 recipient 的 mysqld 会退出，
# 官方镜像里没有守护进程，所以返回 3707（Restart server failed），靠容器的 restart: unless-stopped 拉起来。
do_clone() { # <donor svc> <recipient svc>
  local donor="$1" rec="$2"
  say "CLONE $donor -> $rec"
  ensure_clone_plugin "$donor"
  ensure_clone_plugin "$rec"
  wr "$rec" "SET GLOBAL clone_valid_donor_list='$donor:3306'"
  log_sql "$rec" "CLONE INSTANCE FROM 'root'@'$donor':3306 IDENTIFIED BY '$MYSQL_ROOT_PASSWORD'"
  if [ "$DRY_RUN" = 1 ]; then
    echo "        (DRY_RUN，未执行)" >&2
    return 0
  fi
  if ! run_mysql "$rec" -e "CLONE INSTANCE FROM 'root'@'$donor':3306 IDENTIFIED BY '$MYSQL_ROOT_PASSWORD'"; then
    if grep -qE 'ERROR (3707|2013|2006)' "$ERRF"; then
      echo "  CLONE 已完成，$rec 正在由容器重启策略拉起（3707/连接断开是预期的）"
    else
      show_err
      die "CLONE 失败: $donor -> $rec"
    fi
  fi
  sleep 5 # 等 mysqld 真正退出，再轮询它回来
  wait_mysql_up "$rec" 180
  is_cloned "$rec" || die "CLONE 后 $rec 的 gtid_executed 里没有捐赠方的事务，拷贝可能没有生效"
  echo "  $rec 已就绪，gtid_executed = $(probe "$rec" "SELECT REPLACE(@@GLOBAL.gtid_executed, CHAR(10), '')")"
}

# 让 recipient 成为 source 的从库（GTID 自动定位）。
# GET_SOURCE_PUBLIC_KEY=1 的原因见 scripts/mysql-replication.sh（仅限本地开发）。
configure_replica() { # <recipient svc> <source svc>
  wr "$1" "CHANGE REPLICATION SOURCE TO
      SOURCE_HOST='$2', SOURCE_PORT=3306,
      SOURCE_USER='$REPL_USER', SOURCE_PASSWORD='$MYSQL_REPL_PASSWORD',
      SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1"
  wr "$1" "START REPLICA"
}

wait_replica_running() { # <svc> <秒数>
  local svc="$1" limit="$2" deadline=$((SECONDS + $2))
  [ "$DRY_RUN" = 1 ] && return 0
  until replica_running "$svc"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      replica_status "$svc" >&2
      die "$svc 的复制线程在 ${limit}s 内没有跑起来"
    fi
    sleep 1
  done
}

# 建一个从库：拷贝（已拷过则跳过）、配置复制（已在复制则跳过）、设只读。
build_replica() { # <source svc> <recipient svc>
  local src="$1" rec="$2"
  say "建立从库 $rec <- $src"
  if replica_running "$rec"; then
    echo "  $rec 已经在复制，跳过拷贝和配置"
  else
    if is_cloned "$rec"; then
      echo "  $rec 已经有数据（gtid_executed 含外来事务），跳过 CLONE"
    else
      do_clone "$src" "$rec"
    fi
    if [ -z "$(replica_status "$rec")" ]; then
      configure_replica "$rec" "$src"
    else
      wr "$rec" "START REPLICA"
    fi
    wait_replica_running "$rec" 60
  fi
  # SET PERSIST：重启后依然生效；复制线程不受 super_read_only 限制。
  wr "$rec" "SET PERSIST read_only = ON; SET PERSIST super_read_only = ON"
}

# ---------------------------------------------------------------- 守卫

require_state() { # <key> <提示>
  [ "$(state_get "$1")" = 1 ] || die "$2"
}

refuse_if_promoted() { # <动作>
  if [ "$(state_get PROMOTED)" = 1 ]; then
    die "$1 被拒绝：promote 已经开始或完成（新库已可写）。这一步之后只能向前修"
  fi
}

need_old_masters() {
  local j
  need_running "$(old_svc 0)" "$(old_svc 1)"
  for j in "${OLD_IDX[@]}"; do
    [ "$(rd "$(old_svc "$j")" "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='$(old_db "$j")'")" = 1 ] ||
      die "旧主库 $(old_svc "$j") 上没有库 $(old_db "$j")（DB_PREFIX='$DB_PREFIX'）"
    [ "$(rd "$(old_svc "$j")" "SELECT @@GLOBAL.gtid_mode")" = ON ] || die "$(old_svc "$j") 没有开 GTID"
  done
}

need_new_instances() {
  local j k
  for j in "${OLD_IDX[@]}"; do
    k=$((j + OLD_N))
    need_running "$(new_svc "$k")" "$(new_replica_svc "$k")"
  done
}

# ---------------------------------------------------------------- 子命令

cmd_prepare() {
  refuse_if_promoted prepare
  say "前置检查"
  need_old_masters
  local j k
  say "启动新实例（profile expand）"
  if [ "$DRY_RUN" = 1 ]; then
    echo "  [docker] compose --profile expand up -d --wait mysql-order-2 mysql-order-3 (DRY_RUN，未执行)" >&2
  else
    dc up -d --wait mysql-order-2 mysql-order-3
  fi
  for j in "${OLD_IDX[@]}"; do
    k=$((j + OLD_N))
    build_replica "$(old_svc "$j")" "$(new_svc "$k")"
  done
  state_set PREPARED 1
  say "prepare 完成：两个新实例已经是旧主库的只读从库。可提前很久做，之后跑 check"
}

cmd_check() {
  need_old_masters
  need_new_instances
  local j k bad=0 st lag n old new t tries
  for j in "${OLD_IDX[@]}"; do
    k=$((j + OLD_N))
    say "库 $j -> 库 $k"
    if [ "$(count_tables "$(old_svc "$j")" "$(old_db "$j")" $(prefixed_names moved_ $(moved_tables "$j")))" != 0 ]; then
      echo "  旧库 $j 上已经有 moved_* 表：围栏已立，check 不再适用（用 verify）"
      continue
    fi
    # 1. 复制状态
    st="$(replica_status "$(new_svc "$k")")"
    if replica_running "$(new_svc "$k")"; then
      lag="$(field "$st" Seconds_Behind_Source)"
      echo "  复制线程运行中，Seconds_Behind_Source=$lag"
      if [ "$lag" = NULL ] || [ "$lag" -gt "$MAX_LAG_SECONDS" ]; then
        warn "延迟 $lag 秒超过 ${MAX_LAG_SECONDS}s"
        bad=1
      fi
      n="$(rd "$(new_svc "$k")" "SELECT GTID_SUBTRACT('$(rd "$(old_svc "$j")" "SELECT REPLACE(@@GLOBAL.gtid_executed, CHAR(10), '')")', @@GLOBAL.gtid_executed)")"
      echo "  新库落后的 GTID: ${n:-（无）}"
    else
      warn "$(new_svc "$k") 没有在复制（先跑 prepare）"
      echo "$st" >&2
      bad=1
    fi
    # 2. 长事务（会让 RENAME TABLE 一直等元数据锁）
    n="$(rd "$(old_svc "$j")" "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > $LONG_TRX_SECONDS")"
    if [ "$n" != 0 ]; then
      warn "旧主库上有 $n 个运行超过 ${LONG_TRX_SECONDS}s 的事务："
      rd "$(old_svc "$j")" "SELECT trx_id, trx_started, trx_state, LEFT(IFNULL(trx_query,''), 80) FROM information_schema.innodb_trx WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > $LONG_TRX_SECONDS" >&2
      bad=1
    else
      echo "  旧主库上没有超过 ${LONG_TRX_SECONDS}s 的事务"
    fi
    # 3. 行数。旧库在线写入时新库可能略落后，重试几次再下结论。
    for t in $(moved_tables "$j"); do
      tries=0
      while :; do
        old="$(rd "$(old_svc "$j")" "SELECT COUNT(*) FROM $(old_db "$j").$t")"
        new="$(rd_try "$(new_svc "$k")" "SELECT COUNT(*) FROM $(old_db "$j").$t" || echo MISSING)"
        [ "$old" = "$new" ] && break
        tries=$((tries + 1))
        [ "$tries" -lt 3 ] || break
        sleep 2
      done
      if [ "$old" = "$new" ]; then
        echo "  $t: 旧 $old = 新 $new"
      else
        warn "$t: 旧 $old != 新 $new"
        bad=1
      fi
    done
    # 4. shard_meta 应当还是 2 库布局
    n="$(rd_try "$(old_svc "$j")" "SELECT CONCAT(db_index, ',', db_count, ',', slot_count, ',', slots) FROM $(old_db "$j").shard_meta WHERE id = 1" || echo MISSING)"
    if [ "$n" = "$j,$OLD_N,$SLOT_COUNT,$(slots_csv "$j" "$OLD_N")" ]; then
      echo "  shard_meta = ($n)"
    else
      warn "shard_meta 不符预期：得到 '$n'，期望 '$j,$OLD_N,$SLOT_COUNT,$(slots_csv "$j" "$OLD_N")'"
      bad=1
    fi
  done
  [ "$bad" = 0 ] || die "check 未通过"
  state_set CHECKED 1
  say "check 通过"
}

# 在一个会话里立围栏，成功返回 0；锁等待超时返回 2；其他错误返回 1。
# 输出两行：围栏之后的 gtid_executed、outbox 的最大 id。
fence_once() { # <j> <rename sql> <结果文件>
  local j="$1" sql="$2" out="$3"
  local full="SET SESSION lock_wait_timeout = $FENCE_LOCK_TIMEOUT; $sql;
    SELECT REPLACE(@@GLOBAL.gtid_executed, CHAR(10), '');
    SELECT IFNULL(MAX(id), 0) FROM $(old_db "$j").outbox;
    SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%d %H:%i:%s.%f');"
  log_sql "$(old_svc "$j")" "$full"
  if [ "$DRY_RUN" = 1 ]; then
    echo "        (DRY_RUN，未执行)" >&2
    return 0
  fi
  if run_mysql "$(old_svc "$j")" -N -B -e "$full" >"$out"; then
    return 0
  fi
  show_err
  grep -qE 'ERROR 1205|Lock wait timeout' "$ERRF" && return 2
  return 1
}

cmd_fence() {
  refuse_if_promoted fence
  need_old_masters
  need_new_instances
  local j k attempt rc tmp gtid mark have want ts_try
  for j in "${OLD_IDX[@]}"; do
    k=$((j + OLD_N))
    replica_running "$(new_svc "$k")" || die "$(new_svc "$k") 没有在复制，先跑 prepare。围栏之后没有从库接着追就没有意义"
  done
  [ "$(state_get CHECKED)" = 1 ] || warn "没有记录到 check 通过，建议先跑 check"
  tmp="$(mktemp "${TMPDIR:-/tmp}/expand.fence.XXXXXX")"
  for j in "${OLD_IDX[@]}"; do
    k=$((j + OLD_N))
    say "旧库 ${j}：围栏 slot $(moved_slots "$j")"
    want=$(($(echo "$(moved_tables "$j")" | wc -w)))
    have="$(count_tables "$(old_svc "$j")" "$(old_db "$j")" $(prefixed_names moved_ $(moved_tables "$j")))"
    if [ "$have" = "$want" ]; then
      echo "  moved_* 表已经存在，围栏已立，跳过改名"
      if [ -z "$(state_get FENCE_GTID_$j)" ]; then
        warn "状态文件里没有这个库的围栏 GTID（可能被删过）：用当前的 gtid_executed 代替，只会更保守"
        state_set "FENCE_GTID_$j" "$(rd "$(old_svc "$j")" "SELECT REPLACE(@@GLOBAL.gtid_executed, CHAR(10), '')")"
        state_set "OUTBOX_MARK_$j" "$(rd "$(old_svc "$j")" "SELECT IFNULL(MAX(id), 0) FROM $(old_db "$j").outbox")"
        state_set "FENCE_UTC_$j" "$(rd "$(old_svc "$j")" "SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%d %H:%i:%s.%f')")"
      fi
      continue
    fi
    [ "$have" = 0 ] || die "旧库 $j 上只有部分 moved_* 表（$have/${want}），状态不一致，需要人工处理"
    [ "$(count_tables "$(old_svc "$j")" "$(old_db "$j")" $(moved_tables "$j"))" = "$want" ] ||
      die "旧库 $j 上缺少要搬走的表（需要 $(moved_tables "$j")）；订单库还没建表？"

    attempt=1
    ts_try="$(hr_now)"
    while :; do
      rc=0
      fence_once "$j" "$(fence_rename_sql "$j")" "$tmp" || rc=$?
      [ "$rc" = 0 ] && break
      [ "$rc" = 2 ] || die "围栏失败（非锁超时），不重试"
      warn "第 $attempt 次：等元数据锁超过 ${FENCE_LOCK_TIMEOUT}s，说明有事务占着这些表"
      [ "$attempt" -lt "$FENCE_RETRIES" ] || die "围栏重试 $FENCE_RETRIES 次仍超时，放弃。查 information_schema.innodb_trx 里的长事务后再来"
      attempt=$((attempt + 1))
      sleep 2
    done
    if [ "$DRY_RUN" != 1 ]; then
      gtid="$(sed -n 1p "$tmp")"
      mark="$(sed -n 2p "$tmp")"
      [ -n "$gtid" ] || die "没有读到围栏后的 gtid_executed"
      state_set "FENCE_GTID_$j" "$gtid"
      state_set "OUTBOX_MARK_$j" "$mark"
      state_set "FENCE_UTC_$j" "$(sed -n 3p "$tmp")"
      [ -n "$(state_get FENCE_TS)" ] || state_set FENCE_TS "$ts_try"
      echo "  围栏已立。gtid_executed = ${gtid}；outbox 最大 id = $mark"
    fi
  done
  command rm -f "$tmp"
  state_set FENCED 1
  say "fence 完成。从现在起 slot $(moved_slots 0) $(moved_slots 1) 不可读写，其余 slot 不受影响。尽快 wait、promote"
}

cmd_wait() {
  require_state FENCED "还没有 fence（状态文件 $STATE_FILE 里没有 FENCED=1）"
  refuse_if_promoted wait
  need_old_masters
  need_new_instances
  local j k gtid mark r deadline n
  say "(a) 新实例执行到围栏的 GTID（超时 ${WAIT_GTID_TIMEOUT}s）"
  [ "$WAIT_GTID_TIMEOUT" -gt 0 ] 2>/dev/null || die "WAIT_GTID_TIMEOUT 必须是正整数（0 表示无限等）"
  for j in "${OLD_IDX[@]}"; do
    k=$((j + OLD_N))
    gtid="$(state_get FENCE_GTID_$j)"
    [ -n "$gtid" ] || die "状态文件里没有 FENCE_GTID_$j"
    r="$(rd "$(new_svc "$k")" "SELECT WAIT_FOR_EXECUTED_GTID_SET('$gtid', $WAIT_GTID_TIMEOUT)")"
    [ "$r" = 0 ] || die "新库 $k 在 ${WAIT_GTID_TIMEOUT}s 内没有追上围栏 GTID（返回 ${r}）。看 SHOW REPLICA STATUS"
    echo "  库 $k 已追上"
  done

  say "(b) 旧库 outbox 发完（围栏之前写入的行，id <= 围栏时的最大 id；超时 ${OUTBOX_TIMEOUT}s）"
  for j in "${OLD_IDX[@]}"; do
    mark="$(state_get OUTBOX_MARK_$j)"
    [ -n "$mark" ] || die "状态文件里没有 OUTBOX_MARK_$j"
    deadline=$((SECONDS + OUTBOX_TIMEOUT))
    while :; do
      n="$(rd "$(old_svc "$j")" "SELECT COUNT(*) FROM $(old_db "$j").outbox WHERE published_at IS NULL AND id <= $mark")"
      [ "$n" = 0 ] && break
      [ "$SECONDS" -lt "$deadline" ] || die "旧库 $j 的 outbox 还有 $n 条围栏之前的行没发布。outbox 中继在运行吗？"
      echo "  旧库 $j 还有 $n 条未发布，等待..."
      sleep 1
    done
    echo "  旧库 ${j}：id <= $mark 的行都已发布"
  done

  say "(c) CDC 追上围栏"
  export FENCE_GTID_0 FENCE_GTID_1
  FENCE_GTID_0="$(state_get FENCE_GTID_0)"
  FENCE_GTID_1="$(state_get FENCE_GTID_1)"
  if [ -n "${CDC_WAIT_CMD:-}" ]; then
    deadline=$((SECONDS + CDC_TIMEOUT))
    until bash -c "$CDC_WAIT_CMD"; do
      [ "$SECONDS" -lt "$deadline" ] || die "CDC_WAIT_CMD 在 ${CDC_TIMEOUT}s 内没有返回 0"
      sleep 1
    done
    echo "  CDC_WAIT_CMD 返回 0"
  else
    warn "############################################################################"
    warn "# 没有设置 CDC_WAIT_CMD：无法确认 CDC 是否越过了围栏的 GTID。                #"
    warn "# 现在只是固定等 ${CDC_FALLBACK_SECONDS}s，这不是保证。请人工确认 CDC 源 order-0、order-1 已追上。 #"
    warn "############################################################################"
    sleep "$CDC_FALLBACK_SECONDS"
  fi
  state_set WAIT_DONE 1
  say "wait 完成，可以 promote"
}

# 新实例上建 shard_meta 的语句：从迁移文件里照抄（本来就带 IF NOT EXISTS）。
shard_meta_ddl() {
  [ -f "$MIGRATION" ] || die "找不到迁移文件 $MIGRATION"
  local ddl
  ddl="$(sed -n '/^CREATE TABLE IF NOT EXISTS shard_meta/,/^) ENGINE/p' "$MIGRATION")"
  [ -n "$ddl" ] || die "在 $MIGRATION 里没有找到 shard_meta 的建表语句"
  printf '%s' "$ddl"
}

cmd_promote() {
  require_state FENCED "还没有 fence"
  require_state WAIT_DONE "还没有 wait 成功（不能在没确认复制追平、outbox 发完、CDC 追上时提升）"
  need_old_masters
  need_new_instances
  local j k nsvc odb ndb moved have want rename="" t slots started
  started="$(hr_now)"
  # 先停复制并保持只读，确认每个新实例都能走到“改名”这一步，再放开写入。
  for j in "${OLD_IDX[@]}"; do
    k=$((j + OLD_N))
    nsvc="$(new_svc "$k")"
    odb="$(old_db "$j")" # 新实例上带来的拷贝的库名
    ndb="$(new_db "$k")"
    say "提升 ${nsvc}：${odb}（拷贝）-> $ndb"
    if [ -n "$(replica_status "$nsvc")" ]; then
      wr "$nsvc" "STOP REPLICA"
      wr "$nsvc" "RESET REPLICA ALL"
    else
      echo "  复制已经移除，跳过"
    fi
    # 从这里开始新库可以有写入，rollback 不再安全。
    state_set PROMOTED 1
    wr "$nsvc" "SET PERSIST super_read_only = OFF; SET PERSIST read_only = OFF"
    wr "$nsvc" "CREATE DATABASE IF NOT EXISTS $ndb"
    # 跨库改名：只改元数据，同实例内瞬间完成。
    want=$(($(echo "$(moved_tables "$j")" | wc -w)))
    have="$(count_tables "$nsvc" "$odb" $(prefixed_names moved_ $(moved_tables "$j")))"
    if [ "$have" = "$want" ]; then
      rename=""
      for t in $(moved_tables "$j"); do rename="$rename, $odb.moved_$t TO $ndb.$t"; done
      wr "$nsvc" "RENAME TABLE ${rename#, }"
    elif [ "$have" = 0 ] && [ "$(count_tables "$nsvc" "$ndb" $(moved_tables "$j"))" = "$want" ]; then
      echo "  表已经在 $ndb 里，跳过改名"
    else
      die "$nsvc 上 $odb 里有 $have/$want 张 moved_* 表且 $ndb 里没有全部到位，状态不一致，需要人工处理"
    fi
    wr "$nsvc" "USE $ndb; $(shard_meta_ddl);"
    slots="$(slots_csv "$k" "$NEW_N")"
    wr "$nsvc" "REPLACE INTO $ndb.shard_meta (id, db_index, db_count, slot_count, slots) VALUES (1, $k, $NEW_N, $SLOT_COUNT, '$slots')"
    # 拷贝来的 cdc 账号只有旧库名上的 SELECT；新库名要单独授权（授权会复制到之后建的从库）。
    wr "$nsvc" "GRANT SELECT ON $ndb.* TO 'cdc'@'%'"
  done
  # 旧库的 shard_meta 改成 4 库布局。
  for j in "${OLD_IDX[@]}"; do
    say "旧主库 $(old_svc "$j")：shard_meta 改成 $NEW_N 库布局"
    wr "$(old_svc "$j")" "UPDATE $(old_db "$j").shard_meta SET db_count = $NEW_N, slots = '$(slots_csv "$j" "$NEW_N")' WHERE id = 1"
  done
  # 到这里新主库已经可写、布局已改，数据库层面的不可用窗口结束（建从库不在窗口内）。
  local wts fts0
  wts="$(hr_now)"
  state_set WRITABLE_TS "$wts"
  fts0="$(state_get FENCE_TS)"
  if [ -n "$fts0" ] && [ "$DRY_RUN" != 1 ]; then
    say "围栏到新主库可写：$(perl -e "printf '%.1f', $wts - $fts0") 秒（被搬的 slot 在数据库层面的不可用窗口）"
  fi
  state_set PROMOTE_TS "$wts"
  say "promote 完成。现在用 'scripts/expand.sh env' 的输出切路由（新主库的从库还没建，先不带从库启动 order），再跑 replicas 和 verify"
}

# 给两个新主库各建一个从库。不在不可用窗口内：CLONE 要重启 recipient，每个约 5 到 10 秒。
cmd_replicas() {
  require_state PROMOTED "还没有 promote"
  need_new_instances
  local j k
  for j in "${OLD_IDX[@]}"; do
    k=$((j + OLD_N))
    build_replica "$(new_svc "$k")" "$(new_replica_svc "$k")"
  done
  state_set REPLICAS 1
  say "replicas 完成。可以带上 4 个从库重启 order，并给 CDC 加上两个新源"
}

# 撤掉围栏：把 moved_* 改回原名。新实例保持从库身份继续追（改回的 RENAME 也会复制过去），之后可以重新 fence。
# 只在 promote 之前允许。与 rollback 的区别：rollback 还会删掉新实例。
cmd_unfence() {
  refuse_if_promoted unfence
  need_running "$(old_svc 0)" "$(old_svc 1)"
  local j
  for j in "${OLD_IDX[@]}"; do
    if [ "$(count_tables "$(old_svc "$j")" "$(old_db "$j")" $(prefixed_names moved_ $(moved_tables "$j")))" = 0 ]; then
      echo "  旧库 ${j}：没有 moved_* 表，无需改回"
      continue
    fi
    say "旧库 ${j}：撤掉围栏"
    wr "$(old_svc "$j")" "SET SESSION lock_wait_timeout = $FENCE_LOCK_TIMEOUT; $(fence_rename_sql "$j" 1)"
  done
  state_set FENCED 0
  state_set WAIT_DONE 0
  state_set FENCE_TS ""
  for j in "${OLD_IDX[@]}"; do
    state_set "FENCE_GTID_$j" ""
    state_set "OUTBOX_MARK_$j" ""
    state_set "FENCE_UTC_$j" ""
  done
  say "unfence 完成，被搬的 slot 恢复读写"
}

# 一口气做 fence、wait、promote，并给等待设预算：被围住的 slot 上，下单消息的重试合计只有约 21 秒
# （order 的 submitMaxAttempts），超过就进死信。wait 的三项各自最多等 CUTOVER_STEP_TIMEOUT 秒，
# 任何一项超时就撤掉围栏（promote 之前撤围栏始终安全），而不是让窗口无限拖下去。
cmd_cutover() {
  refuse_if_promoted cutover
  [ "$DRY_RUN" = 1 ] && die "cutover 不支持 DRY_RUN，分别对 fence、wait、promote 用"
  cmd_fence
  WAIT_GTID_TIMEOUT="$CUTOVER_STEP_TIMEOUT"
  OUTBOX_TIMEOUT="$CUTOVER_STEP_TIMEOUT"
  CDC_TIMEOUT="$CUTOVER_STEP_TIMEOUT"
  # 子 shell：wait 里的 die 只结束子 shell，这里接着撤围栏。
  if ! (cmd_wait); then
    warn "wait 没有在预算内完成，撤掉围栏"
    cmd_unfence
    die "cutover 已回退：围栏撤掉，新实例仍是从库。排查原因后可以重试"
  fi
  cmd_promote
}

# 一张表所有列拼成的行摘要表达式。NULL 用占位符表示（CONCAT_WS 会跳过 NULL，不占位的话
# (NULL, 'a') 和 ('a', NULL) 分不清）；二进制列转成十六进制。
row_digest_expr() { # <svc> <db> <表名>
  rd "$1" "SELECT CONCAT('CRC32(CONCAT_WS(''#'', ', GROUP_CONCAT(
      IF(data_type IN ('binary', 'varbinary', 'blob'),
         CONCAT('IFNULL(HEX(\`', column_name, '\`), ''~'')'),
         CONCAT('IFNULL(\`', column_name, '\`, ''~'')'))
      ORDER BY ordinal_position SEPARATOR ', '), '))')
    FROM information_schema.columns WHERE table_schema = '$2' AND table_name = '$3'"
}

# 校验一对表，返回 0 一致。
#
# 旧库的 moved_* 从围栏起不再变化，新库的表在切路由之后会继续被写。直接比行数和 CHECKSUM TABLE
# 只在切路由之前成立，所以改成只比“围栏之后没有动过的行”，两边用同一个条件：
#   - orders_N：行会被更新。先在新库上取出围栏之后（留 5 秒余量）更新过的订单号，两边都排除这些订单。
#   - order_items_N：用同一份订单号名单排除。不能按明细自己的 created_at 划线：它取的是订单的受理时间，
#     围栏之前受理、切换之后才落库的订单，明细的 created_at 早于围栏，却是切换之后才写进新库的
#     （第一次演练就是这样报了三张表“不一致”）。这类订单落库时订单行的 updated_at 一定晚于围栏，在名单里。
#   - payment_results_N：行写入后不再改，created_at 是处理支付结果时的时间，两边都只取早于围栏时刻的行。
# 然后比较行数、行摘要的异或与求和。被排除的行数会打印出来：它应当和切换之后的流量相称，
# 如果大得离谱（比如把整张表都排除了），“一致”就没有意义。
compare_table() { # <j> <表名>
  local j="$1" t="$2" k=$(($1 + OLD_N)) fence expr where ids="" excluded=0 old new
  local osvc nsvc odb ndb
  osvc="$(old_svc "$j")"
  nsvc="$(new_svc "$k")"
  odb="$(old_db "$j")"
  ndb="$(new_db "$k")"
  fence="$(state_get "FENCE_UTC_$j")"
  [ -n "$fence" ] || die "状态文件里没有 FENCE_UTC_$j"
  case "$t" in
  orders_* | order_items_*)
    # 明细跟着订单走：用同一个 slot 的订单表算排除名单。
    ids="$(rd "$nsvc" "SET SESSION group_concat_max_len = 64000000;
      SELECT IFNULL(GROUP_CONCAT(order_id), '') FROM $ndb.orders_${t##*_} WHERE updated_at >= '$fence' - INTERVAL 5 SECOND")"
    where="1 = 1"
    if [ -n "$ids" ]; then
      where="order_id NOT IN ($ids)"
      excluded="$(run_mysql "$nsvc" -N -B -e "SELECT COUNT(*) FROM $ndb.$t WHERE NOT ($where)")"
    fi
    ;;
  *)
    where="created_at < '$fence'"
    excluded="$(rd "$nsvc" "SELECT COUNT(*) FROM $ndb.$t WHERE NOT ($where)")"
    ;;
  esac
  expr="$(row_digest_expr "$osvc" "$odb" "moved_$t")"
  old="$(run_mysql "$osvc" -N -B -e "SELECT COUNT(*), IFNULL(BIT_XOR($expr), 0), IFNULL(SUM($expr), 0) FROM $odb.moved_$t WHERE $where")" ||
    { show_err; die "校验查询失败: $osvc moved_$t"; }
  new="$(run_mysql "$nsvc" -N -B -e "SELECT COUNT(*), IFNULL(BIT_XOR($expr), 0), IFNULL(SUM($expr), 0) FROM $ndb.$t WHERE $where")" ||
    { show_err; die "校验查询失败: $nsvc $t"; }
  if [ "$old" = "$new" ] && [ -n "$old" ]; then
    printf '  %-22s 一致：比较 %s 行（摘要 %s / %s），围栏后有变动而排除 %s 行\n' "$t" \
      "$(echo "$old" | awk '{print $1}')" "$(echo "$old" | awk '{print $2}')" "$(echo "$old" | awk '{print $3}')" "$excluded"
    return 0
  fi
  warn "$t 不一致：旧库 (行数 异或 求和) = ${old}，新库 = ${new}，排除 $excluded 行"
  return 1
}

print_shard_meta() { # 返回 0 表示 4 个库的 shard_meta 都符合预期
  local d svc db got want bad=0
  for ((d = 0; d < NEW_N; d++)); do
    svc="$(old_svc "$d")"
    db="$(old_db "$d")"
    [ "$d" -ge "$OLD_N" ] && {
      svc="$(new_svc "$d")"
      db="$(new_db "$d")"
    }
    got="$(rd_try "$svc" "SELECT CONCAT('(', db_index, ', ', db_count, ', ', slot_count, ', ''', slots, ''')') FROM $db.shard_meta WHERE id = 1" 2>/dev/null || echo MISSING)"
    want="($d, $NEW_N, $SLOT_COUNT, '$(slots_csv "$d" "$NEW_N")')"
    if [ "$got" = "$want" ]; then
      printf '  %-9s shard_meta = %s\n' "$db" "$got"
    else
      warn "$db shard_meta = ${got:-空}，期望 $want"
      bad=1
    fi
  done
  return $bad
}

cmd_verify() {
  require_state PROMOTED "还没有 promote"
  need_old_masters
  need_new_instances
  local j t bad=0
  for j in "${OLD_IDX[@]}"; do
    say "旧库 $j 的 moved_* 对新库 $((j + OLD_N))"
    for t in $(moved_tables "$j"); do
      compare_table "$j" "$t" || bad=1
    done
  done
  say "4 个库的 shard_meta"
  print_shard_meta || bad=1
  [ "$bad" = 0 ] || die "verify 未通过"
  say "verify 通过"
}

cmd_cleanup() {
  require_state PROMOTED "还没有 promote"
  [ "${CONFIRM:-}" = yes ] || die "cleanup 不可回退，需要 CONFIRM=yes"
  if [ "$(state_get CLEANED)" = 1 ]; then
    echo "已经清理过，跳过"
    return 0
  fi
  say "先重新跑一遍 verify"
  cmd_verify
  local j k t t0 t1
  for j in "${OLD_IDX[@]}"; do
    k=$((j + OLD_N))
    say "旧主库 $(old_svc "$j")：删除 moved_* 表"
    for t in $(moved_tables "$j"); do
      t0="$(hr_now)"
      wr "$(old_svc "$j")" "DROP TABLE IF EXISTS $(old_db "$j").moved_$t"
      t1="$(hr_now)"
      printf '    DROP TABLE moved_%s 耗时 %.3f 秒\n' "$t" "$(perl -e "print $t1 - $t0")"
    done
    say "新实例 $(new_svc "$k")：删除带过来的旧库拷贝 $(old_db "$j")"
    t0="$(hr_now)"
    wr "$(new_svc "$k")" "DROP DATABASE IF EXISTS $(old_db "$j")"
    t1="$(hr_now)"
    printf '    DROP DATABASE %s 耗时 %.3f 秒\n' "$(old_db "$j")" "$(perl -e "print $t1 - $t0")"
  done
  state_set CLEANED 1
  say "cleanup 完成"
}

cmd_rollback() {
  refuse_if_promoted rollback
  # 以实例上的实际情况为准再确认一次：新库名下已经有库，说明 promote 做过事。
  local j k t lock_sql rc
  for j in "${OLD_IDX[@]}"; do
    k=$((j + OLD_N))
    if svc_running "$(new_svc "$k")" &&
      [ "$(probe "$(new_svc "$k")" "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='$(new_db "$k")'" 2>/dev/null || echo 0)" != 0 ]; then
      die "rollback 被拒绝：$(new_svc "$k") 上已有库 $(new_db "$k")，promote 做过，新库上可能已有新写入，改回去会丢数据"
    fi
  done
  need_running "$(old_svc 0)" "$(old_svc 1)"
  for j in "${OLD_IDX[@]}"; do
    say "旧库 ${j}：把 moved_* 改回原名"
    if [ "$(count_tables "$(old_svc "$j")" "$(old_db "$j")" $(prefixed_names moved_ $(moved_tables "$j")))" = 0 ]; then
      echo "  没有 moved_* 表，无需改回"
      continue
    fi
    lock_sql="SET SESSION lock_wait_timeout = $FENCE_LOCK_TIMEOUT; $(fence_rename_sql "$j" 1)"
    wr "$(old_svc "$j")" "$lock_sql"
  done
  say "停止并删除新实例的容器和数据卷（会丢掉新实例上所有前缀的拷贝）"
  if [ "$DRY_RUN" = 1 ]; then
    echo "  [docker] compose rm -s -v + docker volume rm (DRY_RUN，未执行)" >&2
  else
    dc rm -f -s -v mysql-order-2 mysql-order-2-replica mysql-order-3 mysql-order-3-replica >/dev/null 2>&1 || true
    for k in 2 3; do
      docker volume rm "oppenheimer_mysql-order-$k-data" "oppenheimer_mysql-order-$k-replica-data" >/dev/null 2>&1 || true
    done
    command rm -f "$STATE_FILE"
  fi
  say "rollback 完成，旧库恢复原状"
}

cmd_status() {
  local st="未开始" j k svc s
  [ "$(state_get PREPARED)" = 1 ] && st="prepare 完成"
  [ "$(state_get FENCED)" = 1 ] && st="已围栏（slot 不可用中）"
  [ "$(state_get WAIT_DONE)" = 1 ] && st="wait 完成，待 promote"
  [ "$(state_get PROMOTED)" = 1 ] && st="已 promote（不可回退）"
  [ "$(state_get CLEANED)" = 1 ] && st="已 cleanup"
  echo "DB_PREFIX='$DB_PREFIX'  状态文件: $STATE_FILE"
  echo "当前阶段: $st"
  for svc in mysql-order-0 mysql-order-0-replica mysql-order-1 mysql-order-1-replica \
    mysql-order-2 mysql-order-2-replica mysql-order-3 mysql-order-3-replica; do
    if ! svc_running "$svc"; then
      printf '%-24s 未运行\n' "$svc"
      continue
    fi
    s="$(replica_status "$svc")"
    if [ -n "$s" ]; then
      printf '%-24s 从库 <- %s  IO=%s SQL=%s lag=%s  read_only=%s\n' "$svc" "$(field "$s" Source_Host)" \
        "$(field "$s" Replica_IO_Running)" "$(field "$s" Replica_SQL_Running)" "$(field "$s" Seconds_Behind_Source)" \
        "$(probe "$svc" "SELECT @@GLOBAL.super_read_only")"
    else
      printf '%-24s 主库  read_only=%s\n' "$svc" "$(probe "$svc" "SELECT @@GLOBAL.read_only")"
    fi
    probe "$svc" "SELECT CONCAT('    ', table_schema, ': ', COUNT(*), ' 张表，其中 moved_ ', SUM(table_name LIKE 'moved\\_%')) FROM information_schema.tables WHERE table_schema LIKE '${DB_PREFIX}order\\_db\\_%' GROUP BY table_schema ORDER BY table_schema" 2>/dev/null || true
    for j in 0 1 2 3; do
      probe "$svc" "SELECT CONCAT('    ${DB_PREFIX}order_db_$j.shard_meta: ', db_index, ',', db_count, ',', slot_count, ',', slots) FROM ${DB_PREFIX}order_db_$j.shard_meta" 2>/dev/null || true
    done
  done
}

cmd_env() {
  local dsn_m="" dsn_r="" cdc="" k p pr
  for k in 0 1 2 3; do
    eval "p=\$PORT_ORDER_$k; pr=\$PORT_ORDER_${k}_R"
    dsn_m="$dsn_m,root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:$p)/${DB_PREFIX}order_db_$k"
    dsn_r="$dsn_r,root:${MYSQL_ROOT_PASSWORD}@tcp(127.0.0.1:$pr)/${DB_PREFIX}order_db_$k"
    cdc="$cdc,cdc:${MYSQL_CDC_PASSWORD}@tcp(127.0.0.1:$pr)/${DB_PREFIX}order_db_$k"
  done
  echo "# 4 库布局下启动服务要用的环境变量（与 scripts/run-local.sh 里的变量一致）。"
  echo "# order 用 MYSQL_DSNS / MYSQL_REPLICA_DSNS（个数和顺序一一对应）；cdc 用后三个，新源的 server-id 登记在 docs/infra/versions.md。"
  echo "export MYSQL_DSNS=\"${dsn_m#,}\""
  echo "export MYSQL_REPLICA_DSNS=\"${dsn_r#,}\""
  echo "export CDC_SOURCE_NAMES=\"order-0,order-1,order-2,order-3\""
  echo "export CDC_SERVER_IDS=\"1011,1021,1031,1041\""
  echo "export CDC_SOURCE_DSNS=\"${cdc#,}\""
}

usage() {
  sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'
}

main() {
  local cmd="${1:-}"
  [ -n "$cmd" ] || {
    usage
    exit 2
  }
  command -v docker >/dev/null || die "没有 docker"
  case "$cmd" in
  prepare | check | fence | wait | promote | cutover | unfence | replicas | verify | cleanup | rollback | status | env)
    [ "$DRY_RUN" = 1 ] && warn "DRY_RUN=1：只打印会改变状态的语句"
    "cmd_$cmd"
    ;;
  -h | --help | help) usage ;;
  *)
    echo "未知子命令: $cmd" >&2
    usage >&2
    exit 2
    ;;
  esac
}

main "$@"
