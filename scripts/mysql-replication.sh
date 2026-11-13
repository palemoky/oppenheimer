#!/usr/bin/env bash
# 建立 order 两个分片的主从复制。由 make up 在容器就绪后调用，可重复运行。
#
# 做的事：在两个主库上建复制账号和 order_db_i，在两个从库上配置并启动复制，
# 最后确认两个复制线程都在运行、库已复制到从库。已经在复制的从库不会被重置。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/deploy/compose/docker-compose.yml")
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-oppenheimer}"
# 复制账号的密码，仅用于本地开发；不要在任何共享环境里使用这个默认值。
MYSQL_REPL_PASSWORD="${MYSQL_REPL_PASSWORD:-oppenheimer-repl}"
REPL_USER=repl
WAIT_SECONDS="${REPL_WAIT_SECONDS:-60}"
SHARDS=(0 1)

# 用 MYSQL_PWD 传密码，避免命令行上出现密码的警告。需要解析单个值的地方自己加 -N -B（无表头）；
# SHOW REPLICA STATUS 要字段名，所以不能加 -N。
sql() {
  local svc="$1"
  shift
  "${COMPOSE[@]}" exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" "$svc" mysql -uroot "$@"
}

# 打印 SHOW REPLICA STATUS 中我们关心的字段（key=value），没配置复制时输出为空。
replica_status() {
  sql "$1" --vertical -e "SHOW REPLICA STATUS" 2>/dev/null |
    awk -F': ' '/^ *(Replica_IO_Running|Replica_SQL_Running|Last_IO_Error|Last_SQL_Error|Replica_IO_State):/ {
      sub(/^ +/, "", $1); print $1 "=" $2 }'
}

status_field() { # <状态文本> <字段名>
  awk -F= -v k="$2" '$1 == k { print $2 }' <<<"$1"
}

# 1. 主库：复制账号和分片库。
# MySQL 9 移除了 mysql_native_password，账号只能用默认的 caching_sha2_password。
# 这种账号在没有 TLS 的连接上做完整认证时，客户端要拿到服务端的 RSA 公钥才能加密密码。
# CHANGE REPLICATION SOURCE TO 的 GET_SOURCE_PUBLIC_KEY=1 让从库在连接时向主库请求公钥，
# 不需要配置证书或 SOURCE_PUBLIC_KEY_PATH。代价是请求公钥时没有校验主库身份，
# 存在中间人风险，所以只适合本地开发；生产应该用 SOURCE_SSL=1 加证书校验。
for i in "${SHARDS[@]}"; do
  sql "mysql-order-$i" -e "
    CREATE USER IF NOT EXISTS '$REPL_USER'@'%' IDENTIFIED BY '$MYSQL_REPL_PASSWORD';
    GRANT REPLICATION SLAVE ON *.* TO '$REPL_USER'@'%';
    CREATE DATABASE IF NOT EXISTS order_db_$i;"
done

# 2. 从库：没有配置复制才配置；已经配置的不碰（重新 CHANGE SOURCE 要先 STOP，会打断复制）。
for i in "${SHARDS[@]}"; do
  replica="mysql-order-$i-replica"
  st="$(replica_status "$replica")"
  if [ -z "$st" ]; then
    echo "configuring replication: $replica <- mysql-order-$i"
    # SOURCE_AUTO_POSITION=1：用 GTID 自动定位，不需要记 binlog 文件和位置，主从切换时也不用重算。
    sql "$replica" -e "
      CHANGE REPLICATION SOURCE TO
        SOURCE_HOST='mysql-order-$i', SOURCE_PORT=3306,
        SOURCE_USER='$REPL_USER', SOURCE_PASSWORD='$MYSQL_REPL_PASSWORD',
        SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1;
      START REPLICA;"
  elif [ "$(status_field "$st" Replica_IO_Running)" != Yes ] || [ "$(status_field "$st" Replica_SQL_Running)" != Yes ]; then
    # 配置过但线程停了（比如上次出错）：只重新启动，不改配置。
    echo "restarting replication threads: $replica"
    sql "$replica" -e "START REPLICA;"
  else
    echo "replication already running: $replica"
  fi
done

# 3. 等两个线程都运行，且主库上的库已出现在从库。有上限，超时就打印出错字段并失败。
fail=0
for i in "${SHARDS[@]}"; do
  replica="mysql-order-$i-replica"
  deadline=$((SECONDS + WAIT_SECONDS))
  while :; do
    st="$(replica_status "$replica")"
    ok=0
    if [ "$(status_field "$st" Replica_IO_Running)" = Yes ] && [ "$(status_field "$st" Replica_SQL_Running)" = Yes ]; then
      n="$(sql "$replica" -N -B -e "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='order_db_$i'" 2>/dev/null || echo 0)"
      [ "$n" = 1 ] && ok=1
    fi
    if [ "$ok" = 1 ]; then
      echo "replica ok: $replica (order_db_$i replicated)"
      break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "replica NOT ready after ${WAIT_SECONDS}s: $replica" >&2
      echo "$st" >&2
      fail=1
      break
    fi
    sleep 1
  done
done
[ "$fail" = 0 ] || exit 1

# 4. 复制建立后再把从库设为只读。官方镜像首次启动时要写系统表和 root 密码，
# 所以不能在 compose 的 command 里直接开 super_read_only。
# SET PERSIST 写入 mysqld-auto.cnf，重启后依然生效；复制线程不受 super_read_only 限制。
for i in "${SHARDS[@]}"; do
  sql "mysql-order-$i-replica" -e "SET PERSIST read_only = ON; SET PERSIST super_read_only = ON;"
done
echo "mysql replication ready"
