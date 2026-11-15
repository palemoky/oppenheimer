#!/bin/bash
# CronJob：把一个实例的业务库逻辑备份（mysqldump）到 NAS。
#
# 产物：$BACKUP_ROOT/$RELEASE/<UTC 时间戳>/ 里三个文件
#   dump.sql.zst   压缩的 dump（zstd：镜像里现成有，比 gzip 快、比例也更好）
#   sha256         dump.sql.zst 的 sha256（sha256sum -c 可直接校验）
#   meta           实例名、连的主机、gtid_executed、起止时间、字节数等，key=value 一行一个
#
# 半截的备份不能被当成备份：先写到 .tmp-<时间戳>，完整写完、解压校验、算好 sha256 之后才改名成最终目录。
# 同一个目录里的 rename 是原子的，所以任何时刻看到的最终目录都是完整的。.tmp-* 不符合时间戳命名，
# 恢复和保留的逻辑都不会碰它。
#
# 保留最近 KEEP 份：新的一份成功之后才删旧的（先删后写，写失败就一份都没有了）。只删符合时间戳命名的目录，
# 手工放进去的东西（比如 keep-before-upgrade 这样的目录）不会被动。
#
# 环境变量（chart 注入）：RELEASE、BACKUP_HOST、BACKUP_PASSWORD、BACKUP_ROOT、DATABASES（空格分隔）、
# DUMP_MODE（replica 或 single）、KEEP、ACTIVE_DEADLINE_SECONDS。
#
# 管道里任何一段失败都要让 Job 失败：set -o pipefail。注意不要写 `cmd | grep -q`：grep -q 找到就退出，
# 上游会因为 SIGPIPE 以 141 结束，pipefail 下被当成失败。下面要从输出里取内容的地方都用 awk 读到底。
set -euo pipefail
# shellcheck source=lib.sh
. /scripts/lib.sh

export LC_ALL=C # 目录名的排序按字节，时间戳格式下字典序就是时间序
umask 027       # 目录 750、文件 640

: "${RELEASE:?}" "${BACKUP_HOST:?}" "${BACKUP_PASSWORD:?}" "${BACKUP_ROOT:?}" "${DATABASES:?}"
: "${DUMP_MODE:?}" "${KEEP:?}" "${ACTIVE_DEADLINE_SECONDS:?}"

case "$KEEP" in '' | *[!0-9]* | 0)
  log "KEEP must be a positive integer, got '$KEEP'"
  exit 1
  ;;
esac
case "$DUMP_MODE" in replica | single) ;; *)
  log "DUMP_MODE must be replica or single, got '$DUMP_MODE'"
  exit 1
  ;;
esac
# 库名进命令行，和 accounts.sh 一样只允许字母、数字和下划线。
for db in $DATABASES; do
  case "$db" in *[!A-Za-z0-9_]*)
    log "invalid database name: $db"
    exit 1
    ;;
  esac
done

# 密码只经环境变量 MYSQL_PWD 交给客户端，不出现在命令行参数里。
export MYSQL_PWD="$BACKUP_PASSWORD"
client_args=(--no-defaults -h "$BACKUP_HOST" -P 3306 -ubackup)

ts_re='^[0-9]{8}T[0-9]{6}Z$'
dir="$BACKUP_ROOT/$RELEASE"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
tmp="$dir/.tmp-$ts"
final="$dir/$ts"
started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# 备份根目录（NFS 上的 backup/mysql）的属主是 999，每个实例的子目录由这里自己建，
# 不用 volumeMount 的 subPath：subPath 的目录由 kubelet 以 root 建，属主 999 的 Pod 写不进去。
# 群晖上新建的目录可能继承 ACL，属主也进不去，所以建完显式 chmod 一次（和 addons.sh 的 cmd_nfs 同一个原因）。
mkdir -p "$dir"
chmod 750 "$dir"

# 清掉以前失败（被杀、超时）留下的临时目录。只清比 activeDeadlineSeconds 更老的：Job 最长就跑这么久，
# 更老的一定是死的；更新的可能是另一个正在跑的备份（手工触发的 Job 不受 concurrencyPolicy 约束）。
for t in "$dir"/.tmp-*; do
  [ -d "$t" ] && [ ! -L "$t" ] || continue
  if [ -n "$(find "$t" -maxdepth 0 -mmin "+$((ACTIVE_DEADLINE_SECONDS / 60 + 5))")" ]; then
    log "removing stale temp dir $t"
    rm -rf -- "$t"
  fi
done

done_ok=0
cleanup() {
  # 没走到改名就退出（任何原因）：删掉自己的临时目录，不留半截的东西。
  if [ "$done_ok" != 1 ]; then
    rm -rf -- "$tmp"
  fi
}
trap cleanup EXIT

[ ! -e "$final" ] || {
  log "$final already exists"
  exit 1
}
mkdir "$tmp"

# 从库上备份前先确认它是从库、复制在跑。-replica Service 在切换之后可能指向别的东西，
# 复制断了的从库备出来的是过时的数据却不报错。这里宁可失败也不默默备一份旧的。
lag=""
if [ "$DUMP_MODE" = replica ]; then
  status="$(mysql "${client_args[@]}" --connect-timeout=10 --vertical -e 'SHOW REPLICA STATUS')"
  if [ -z "$status" ]; then
    log "$BACKUP_HOST is not a replica (SHOW REPLICA STATUS is empty); refusing to back up"
    exit 1
  fi
  io="$(awk -F': ' '/^ *Replica_IO_Running:/ {print $2}' <<<"$status")"
  sql="$(awk -F': ' '/^ *Replica_SQL_Running:/ {print $2}' <<<"$status")"
  lag="$(awk -F': ' '/^ *Seconds_Behind_Source:/ {print $2}' <<<"$status")"
  if [ "$io" != Yes ] || [ "$sql" != Yes ]; then
    log "replication on $BACKUP_HOST is not running (io=$io sql=$sql); the backup would be stale"
    exit 1
  fi
  log "replica ok: lag=${lag}s"
fi

# 位点用 --source-data=2（9.x 里 --master-data 已经是废弃名字，用新名字），在从库上也一样，不用 --dump-replica。
# 两者的区别：
#   --source-data    记的是“这个服务器自己的” binlog 文件和位点；在从库上是从库自己的 binlog。
#   --dump-replica   在从库上记的是“它从主库复制到哪了”（主库的 binlog 文件和位点，来自 SHOW REPLICA STATUS），
#                    语义上更适合“恢复出来的实例想接回主库”。
# 选前者的原因（在 mysql:9.7.4 上实测）：--dump-replica 会先 STOP REPLICA SQL_THREAD（备份期间从库停止应用，
# 越落越后，读从库的 CDC 也跟着停），而且这要 REPLICATION_SLAVE_ADMIN 权限，等于让备份账号能改复制拓扑，
# 和最小权限矛盾。我们的复制是 GTID 自动定位（SOURCE_AUTO_POSITION=1），恢复后续接靠的是下面的
# GTID_PURGED，文件加位点只是给人看的记录，不是必需的。取值 2：写成被注释掉的 CHANGE REPLICATION SOURCE，
# 恢复时不会自动执行。
# 单实例（mysql-main）和从库用同一个参数，记它自己的位点。
position_flag=--source-data=2

# 参数说明：
#   --single-transaction  一致性快照，不锁表（只在开头短暂持有全局读锁来对齐位点和 GTID）
#   --databases           只导 values 里列的库，并带 CREATE DATABASE；不导系统库，账号由 accounts hook 重建
#   --routines --events --triggers  存储过程、事件、触发器（triggers 默认开，写出来是为了意图明确）
#   --set-gtid-purged=ON  把和快照一致的 gtid_executed 写进 dump 开头（恢复时 SET @@GLOBAL.GTID_PURGED），
#                         恢复出来的实例按它续接才不丢不重
#   --skip-masking-policies  9.x 的 mysqldump 默认还去读 mysql.column_masking_policy 导数据脱敏策略，
#                         备份账号没有这张系统表的 SELECT（也不该有）会报错；我们不用脱敏策略
#   --no-tablespaces      不导表空间信息，省掉 PROCESS 权限（InnoDB 的表空间在逻辑备份里没有意义）
#   --hex-blob            二进制列用十六进制，不受字符集和换行的影响
#   --default-character-set=utf8mb4  和 server.cnf 一致
log "dumping databases [$DATABASES] from $BACKUP_HOST ($DUMP_MODE, $position_flag)"
# shellcheck disable=SC2086 # DATABASES 按空格拆成多个库名，上面校验过只含安全字符
mysqldump "${client_args[@]}" \
  --single-transaction "$position_flag" --set-gtid-purged=ON \
  --routines --events --triggers --no-tablespaces --skip-masking-policies --hex-blob \
  --default-character-set=utf8mb4 \
  --databases $DATABASES |
  zstd -q -T1 -3 -o "$tmp/dump.sql.zst"

# 校验：整份解压一遍（zstd 的校验和 + 管道里任何一段失败都会让这里失败），
# 同时用 awk 读到底取三样东西：gtid_executed、位点记录、最后一行。
# mysqldump 正常结束时最后会写 "-- Dump completed"；被截断的 dump 没有这一行。
# GTID 集合里有多个 UUID 时会拆成多行，所以从 GTID_PURGED 那行一直收集到以 ';' 结尾的行。
info="$(zstd -dcq "$tmp/dump.sql.zst" | awk '
  /^SET @@GLOBAL.GTID_PURGED=/ { g = 1 }
  g { buf = buf $0; if ($0 ~ /;$/) { g = 0 } }
  /^-- CHANGE REPLICATION SOURCE TO/ { pos = $0 }
  { last = $0 }
  END { print "gtid\t" buf; print "pos\t" pos; print "last\t" last }')"
last="$(awk -F'\t' '$1 == "last" {print $2}' <<<"$info")"
case "$last" in "-- Dump completed"*) ;; *)
  log "dump is incomplete: last line is '$last'"
  exit 1
  ;;
esac
gtid="$(awk -F'\t' '$1 == "gtid" {print $2}' <<<"$info")"
# SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ 'uuid:1-100,uuid2:1-5';  取最后一对引号里的内容
gtid="$(sed -E "s/^.*'\+'\*\/ '//; s/';\$//" <<<"$gtid")"
position="$(awk -F'\t' '$1 == "pos" {print substr($0, 5)}' <<<"$info")"
position="${position#-- }"
if [ -z "$gtid" ]; then
  log "no GTID_PURGED found in the dump (is gtid_mode on?)"
  exit 1
fi

sync "$tmp/dump.sql.zst"
(cd "$tmp" && sha256sum dump.sql.zst >sha256 && sha256sum -c --quiet sha256)
bytes="$(stat -c %s "$tmp/dump.sql.zst")"
{
  echo "release=$RELEASE"
  echo "host=$BACKUP_HOST"
  echo "mode=$DUMP_MODE"
  echo "databases=$DATABASES"
  echo "gtid_executed=$gtid"
  echo "position=$position"
  echo "replica_lag_seconds=$lag"
  echo "started_at=$started"
  echo "finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "bytes=$bytes"
} >"$tmp/meta"
sync "$tmp/sha256" "$tmp/meta"

# 到这里才算完整：改名。
mv -T "$tmp" "$final"
done_ok=1
log "backup ok: $final ($bytes bytes, gtid_executed=$gtid)"

# 新的一份成功了，再按数量删旧的。
all=()
for d in "$dir"/*; do
  [ -d "$d" ] && [ ! -L "$d" ] || continue
  [[ "$(basename "$d")" =~ $ts_re ]] || continue
  all+=("$d")
done
extra=$((${#all[@]} - KEEP))
if [ "$extra" -gt 0 ]; then
  for ((i = 0; i < extra; i++)); do
    log "retention: removing ${all[$i]} (keeping the newest $KEEP)"
    rm -rf -- "${all[$i]}"
  done
fi
