#!/bin/bash
# hook Job：在主库上建库、建账号。可以重复执行：库和账号都带 IF NOT EXISTS，权限 GRANT 多次没有副作用，
# 密码每次用 ALTER USER 对齐到 Secret（Secret 是密码的唯一来源，重建 Secret 后重跑一次就一致了）。
# 从库只读，账号和库靠复制到达。
#
# 主库按状态找（可写、没有复制的那一个），不看 primaryOrdinal：主库是谁以实例自己的状态为准，
# 而且 primaryOrdinal 不进 Pod 模板和 ConfigMap（改它不能触发滚动重启），这里拿不到它。
# 找到不止一个可写的实例（切换后旧主回来）就失败，不往任何一个里写。
#
# 环境变量：DATABASES（空格分隔）、CDC_ENABLED、WAIT_SECONDS 和各账号的密码（REPL、APP、CDC、BACKUP）。
set -euo pipefail
# shellcheck source=lib.sh
. /scripts/lib.sh

deadline=$((SECONDS + WAIT_SECONDS))
last=""
while :; do
  rc=0
  primary="$(find_primary -1)" || rc=$?
  [ "$rc" = 0 ] && break
  if [ "$SECONDS" -ge "$deadline" ]; then
    log "no unique primary in $RELEASE after ${WAIT_SECONDS}s (rc=$rc)"
    exit 1
  fi
  msg="waiting for exactly one primary in $RELEASE (rc=$rc)"
  [ "$msg" != "$last" ] && log "$msg"
  last="$msg"
  sleep 5
done
log "primary: $primary"

# 库名进的是 SQL 标识符，只允许字母、数字和下划线。GRANT 的库名里下划线是通配符，要转义成 \_，
# 否则 order_db_0 也会匹配 orderXdbY0 这样的库。
sql=""
backup_sql=""
dbs=""
for db in $DATABASES; do
  case "$db" in
  *[!A-Za-z0-9_]*)
    log "invalid database name: $db"
    exit 1
    ;;
  esac
  sql="$sql
CREATE DATABASE IF NOT EXISTS \`$db\`;
GRANT ALL PRIVILEGES ON \`${db//_/\\_}\`.* TO 'app'@'%';"
  # backup：见下面 backup_global 的说明。库级的四个权限，每个都是 mysqldump 缺了就丢东西或报错的（在 9.7.4 上逐个去掉实测过）：
  #   SELECT      读表数据
  #   SHOW VIEW   导视图（SHOW CREATE VIEW）；缺了 dump 直接失败
  #   TRIGGER     导触发器；缺了 dump 照常成功，但触发器悄悄不见了
  #   EVENT       导事件（SHOW EVENTS）；缺了 dump 直接失败
  backup_sql="$backup_sql
GRANT SELECT, SHOW VIEW, TRIGGER, EVENT ON \`${db//_/\\_}\`.* TO 'backup'@'%';"
  dbs="$dbs $db"
done

cdc_sql=""
if [ "$CDC_ENABLED" = true ]; then
  # CDC 照本机的 scripts/mysql-replication.sh：REPLICATION SLAVE 请求 binlog，REPLICATION CLIENT 读
  # gtid_executed、gtid_purged，对业务库的 SELECT 用于全量阶段分批读表。
  cdc_sql="
CREATE USER IF NOT EXISTS 'cdc'@'%' IDENTIFIED BY '$CDC_PASSWORD';
ALTER USER 'cdc'@'%' IDENTIFIED BY '$CDC_PASSWORD';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';"
  for db in $DATABASES; do
    cdc_sql="$cdc_sql
GRANT SELECT ON \`${db//_/\\_}\`.* TO 'cdc'@'%';"
  done
fi

# backup：每天的 mysqldump 备份（CronJob，见 backup.sh），不用 root。只给 mysqldump --single-transaction
# --source-data=2 --routines --events --triggers 用得上的权限，全局的三个：
#   FLUSH_TABLES        --single-transaction 加位点时 mysqldump 要执行 FLUSH TABLES WITH READ LOCK，短暂持有
#                       来对齐快照、位点和 GTID。这条语句要 RELOAD 或 FLUSH_TABLES 之一，选窄的那个
#                       （RELOAD 还能 FLUSH 日志、权限表等，用不着）
#   REPLICATION CLIENT  --source-data 要 SHOW BINARY LOG STATUS；backup.sh 在从库上还要 SHOW REPLICA STATUS
#                       确认复制在跑。两者都认这个权限
#   SHOW_ROUTINE        导存储过程和函数；缺了 dump 照常成功，但存储过程悄悄不见了（实测）
# 没有给的：LOCK TABLES（--single-transaction 不锁表）、PROCESS（dump 加了 --no-tablespaces）、
# REPLICATION_SLAVE_ADMIN（--dump-replica 才要，它还会停从库的 SQL 线程，所以不用它，原因见 backup.sh）。
# 这些权限随库一起复制到从库，权限只加不减：以后要收回得手工 REVOKE，这里不会替你收。
backup_global="GRANT FLUSH_TABLES, REPLICATION CLIENT, SHOW_ROUTINE ON *.* TO 'backup'@'%';"

# repl：REPLICATION SLAVE 给从库的 IO 线程；BACKUP_ADMIN 是 CLONE 时主库（donor）一侧要的。
# 接收方一侧执行 CLONE 的是从库 init 容器里临时实例的 root，不需要给 repl 加 CLONE_ADMIN。
# 账号要先于授权建出来，所以 app 放在库的 GRANT 之前。语句经标准输入传，密码不进命令行。
rsql "$primary" <<SQL
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY '$REPL_PASSWORD';
ALTER USER 'repl'@'%' IDENTIFIED BY '$REPL_PASSWORD';
GRANT REPLICATION SLAVE, BACKUP_ADMIN ON *.* TO 'repl'@'%';
CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED BY '$APP_PASSWORD';
ALTER USER 'app'@'%' IDENTIFIED BY '$APP_PASSWORD';
CREATE USER IF NOT EXISTS 'backup'@'%' IDENTIFIED BY '$BACKUP_PASSWORD';
ALTER USER 'backup'@'%' IDENTIFIED BY '$BACKUP_PASSWORD';
$backup_global
$sql
$backup_sql
$cdc_sql
SQL
log "databases ready on $primary:$dbs (cdc: $CDC_ENABLED)"
