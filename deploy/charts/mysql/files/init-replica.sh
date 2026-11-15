#!/bin/bash
# init 容器：数据目录为空时，用 CLONE 从当前的主库拉全量，配好复制和只读，再交给主容器。
#
# 判断的依据是数据目录的状态，不是序号：
#   - 有数据（mysql.ibd 在）且没有“初始化中”的记号：正常的重启，什么都不做。复制的配置在系统表里，
#     mysqld 起来会自己接着复制；super_read_only 是 SET PERSIST 的，也还在。
#   - 有“初始化中”的记号：上次做到一半被杀了。清空重来，CLONE 本身不能续传。
#   - 空的：找到唯一的主库就从它 CLONE；找不到时，只有“序号 0、而且别的 Pod 都不存在”（第一次安装）
#     才放行，让官方入口脚本初始化一个新的空库。别的情况一律等：比如主库的盘没了，而从库还在，
#     这时自己初始化一个空的主库，服务就会往空库里写，是最坏的结果。等着，由人决定先提升从库。
#
# 整个过程不让 mysqld 监听对外的端口，复制和 super_read_only 都配好之后主容器才启动，
# 所以不存在“从库已经能被连上、但还可写”的窗口。
#
# CLONE 用 DATA DIRECTORY 克隆到一个新目录，而不是克隆进正在运行的实例自己的数据目录：
# 后者完成时 mysqld 要重启自己，容器里没有守护进程，它只能退出，要靠容器重启接着做后半段，
# 每个中间状态都要能重入。克隆到新目录则临时实例一直在，做完再把目录挪过去，步骤是线性的。
set -euo pipefail
# shellcheck source=lib.sh
. /scripts/lib.sh

D=/var/lib/mysql             # 数据卷
W=/work                      # emptyDir，临时实例的数据和套接字
FLAG="$D/.oppenheimer-provisioning"
CLONE_DIR="$D/.clone"
ord="$(ordinal)"
sid="$(server_id "$ord")"

if ! command -v getent >/dev/null; then
  log "getent not found in image; cannot tell whether peers exist"
  exit 1
fi

resumed=0
if [ -e "$FLAG" ]; then
  log "previous provisioning did not finish; starting over"
  resumed=1
elif [ -e "$D/mysql.ibd" ]; then
  log "data directory already initialized; nothing to do"
  exit 0
fi

# 找主库。连不上、还没建好 repl 账号都在这里等；init 容器没有超时，卡住时看它的日志。
donor=""
last=""
while :; do
  rc=0
  donor="$(find_primary "$ord")" || rc=$?
  if [ "$rc" = 0 ]; then
    # repl 账号由 hook Job 建，第一次安装时可能比主库就绪晚一点。先确认能登录，免得 CLONE 失败后整个重来。
    if MYSQL_PWD="$REPL_PASSWORD" mysql --no-defaults -h "$donor" -P 3306 -urepl --connect-timeout=5 \
      -N -B -e "SELECT 1" >/dev/null 2>&1; then
      break
    fi
    rc=3
  fi
  if [ "$rc" = 1 ] && [ "$ord" = 0 ] && [ "$resumed" = 0 ] && ! any_peer_exists "$ord"; then
    log "ordinal 0, empty data directory, no other pods: leaving bootstrap to the image entrypoint"
    exit 0
  fi
  msg="waiting for exactly one primary among the other pods (rc=$rc)"
  [ "$msg" != "$last" ] && log "$msg"
  last="$msg"
  sleep 5
done
log "donor: $donor"

# 从这里开始任何一步被杀，下次都会看到记号并从头来。
# 容器以 mysql 用户运行（statefulset.yaml 里的 securityContext），这些文件本来就是它建的；卷的根目录属于 root、
# 靠 fsGroup 可写，chown 它会失败，所以失败不算错。保留 chown 是为了以 root 运行时行为不变。
chown mysql:mysql "$D" 2>/dev/null || true 2>/dev/null || true
touch "$FLAG"
shopt -s dotglob nullglob
for f in "$D"/*; do
  [ "$f" = "$FLAG" ] || rm -rf "$f"
done
rm -rf "${W:?}"/*
mkdir -p "$W/tmp"
chown -R mysql:mysql "$W" 2>/dev/null || true

# 启动一个临时实例，参数和主容器一致，只是不对外监听正式的端口、缓冲池压小（它只是个执行语句的壳）。
temp_pid=""
start_temp() { # <datadir> <套接字> [额外参数...]
  local dir="$1" sock="$2"
  shift 2
  mysqld --defaults-extra-file="$CNF" --user=mysql --datadir="$dir" --socket="$sock" \
    --pid-file="$sock.pid" --log-error="$sock.err" --tmpdir="$W/tmp" --mysqlx=OFF \
    --innodb-buffer-pool-size=64M --server-id="$sid" --skip-replica-start "$@" &
  temp_pid=$!
  local i
  for ((i = 0; i < 300; i++)); do
    if mysqladmin --no-defaults -S "$sock" -uroot ping >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$temp_pid" 2>/dev/null; then
      log "temporary mysqld exited; error log:"
      cat "$sock.err" >&2 || true
      return 1
    fi
    sleep 1
  done
  log "temporary mysqld did not come up in 300s"
  return 1
}
stop_temp() { # <套接字> [root 的密码，空表示没有密码]
  if [ -n "${2:-}" ]; then
    MYSQL_PWD="$2" mysqladmin --no-defaults -S "$1" -uroot shutdown
  else
    mysqladmin --no-defaults -S "$1" -uroot shutdown
  fi
  wait "$temp_pid" || true
  temp_pid=""
}

# 1. 空的临时实例，只用来执行 CLONE。root 没有密码，只能经套接字登录。
log "initializing a scratch instance"
mysqld --defaults-extra-file="$CNF" --user=mysql --datadir="$W/scratch" --initialize-insecure \
  --log-error="$W/initialize.err" || {
  cat "$W/initialize.err" >&2
  exit 1
}
start_temp "$W/scratch" "$W/scratch.sock" --port=3307 --bind-address=127.0.0.1

# 2. CLONE。主库一侧的 repl 账号要 BACKUP_ADMIN；这一侧执行语句的是临时实例的 root，自带 CLONE_ADMIN。
# 语句经标准输入传，密码不进命令行。
log "cloning from $donor into $CLONE_DIR"
mysql --no-defaults -S "$W/scratch.sock" -uroot <<SQL
SET GLOBAL clone_valid_donor_list = '$donor:3306';
CLONE INSTANCE FROM 'repl'@'$donor':3306 IDENTIFIED BY '$REPL_PASSWORD' DATA DIRECTORY = '$CLONE_DIR';
SQL
stop_temp "$W/scratch.sock"

# 3. 把克隆出来的目录挪到数据卷的根上。
for f in "$CLONE_DIR"/*; do
  mv "$f" "$D/"
done
rmdir "$CLONE_DIR"
# server_uuid 必须和主库不同，否则复制会把对方当成自己；持久化的变量也不要沿用主库的。
# 两个文件按说都不会被克隆过来，删掉是为了不依赖这一点。
rm -f "$D/auto.cnf" "$D/mysqld-auto.cnf"
# 官方入口脚本靠 $D/mysql 目录是否存在判断“已经初始化过”，没有它会去重新初始化并因目录非空而失败。
mkdir -p "$D/mysql"
chown -R mysql:mysql "$D" 2>/dev/null || true

# 4. 在克隆来的数据上起临时实例，配复制和只读。用户表是从主库克隆来的，所以 root 密码和主库一样。
# --skip-networking：这一步谁都连不上它。
log "configuring replication"
start_temp "$D" "$W/data.sock" --skip-networking
# SOURCE_HOST 用主库的稳定名字，不用 -primary 这个 Service：后者跟着 primaryOrdinal 走，改了 values
# 复制源不能被静悄悄地换掉（新主是否已经追平、有没有分叉的事务，要人先确认）。
# SOURCE_AUTO_POSITION=1：位置来自自己的 gtid_executed（CLONE 带过来的），不需要 binlog 文件名和位置。
# GET_SOURCE_PUBLIC_KEY=1：caching_sha2_password 在非 TLS 连接上要拿主库的公钥，和本机一致（不校验主库身份，集群内部可接受）。
# SET PERSIST 写进数据目录里的 mysqld-auto.cnf，重启后仍然只读；不能写进启动参数，官方镜像第一次初始化会失败，
# 而且主从是同一份 Pod 模板。复制线程不受 super_read_only 的限制。
MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql --no-defaults -S "$W/data.sock" -uroot <<SQL
RESET REPLICA ALL;
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST = '$donor', SOURCE_PORT = 3306,
  SOURCE_USER = 'repl', SOURCE_PASSWORD = '$REPL_PASSWORD',
  SOURCE_AUTO_POSITION = 1, GET_SOURCE_PUBLIC_KEY = 1,
  SOURCE_CONNECT_RETRY = 10, SOURCE_RETRY_COUNT = 1000000;
SET PERSIST read_only = ON;
SET PERSIST super_read_only = ON;
SQL
state="$(MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql --no-defaults -S "$W/data.sock" -uroot -N -B \
  -e "SELECT @@global.super_read_only, @@global.server_uuid, REPLACE(@@global.gtid_executed, '\n', '')")"
log "cloned state (super_read_only, server_uuid, gtid_executed): $state"
if [ "$(cut -f1 <<<"$state")" != 1 ]; then
  log "super_read_only is not on after configuration"
  exit 1
fi
stop_temp "$W/data.sock" "$MYSQL_ROOT_PASSWORD"

rm -f "$FLAG"
rm -rf "${W:?}"/*
log "replica provisioned from $donor"
