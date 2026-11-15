# shellcheck shell=bash
# Pod 里各脚本共用的函数。环境变量由 chart 注入：RELEASE、POD_NAMESPACE、CLUSTER_DOMAIN、REPLICAS、
# SERVER_ID_BASE、MYSQL_ROOT_PASSWORD（以及按需的 REPL_PASSWORD 等）。
#
# 密码一律经环境变量 MYSQL_PWD 或标准输入交给 mysql 客户端，不出现在任何进程的命令行参数里。

CNF=/etc/mysql-oppenheimer/server.cnf

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" >&2; }

# StatefulSet 的 Pod 名是 <release>-<序号>，序号就是最后一段。
ordinal() {
  local o="${HOSTNAME##*-}"
  case "$o" in
  '' | *[!0-9]*)
    log "cannot parse ordinal from HOSTNAME=$HOSTNAME"
    return 1
    ;;
  esac
  echo "$o"
}

server_id() { echo $((SERVER_ID_BASE + $1)); }

# 序号 i 的稳定名字，headless Service 给的。
peer_host() { echo "${RELEASE}-$1.${RELEASE}.${POD_NAMESPACE}.svc.${CLUSTER_DOMAIN}"; }

# 用 root 经 TCP 连某个实例。--no-defaults：不读镜像里 /etc/my.cnf 的 [client]，行为只由这里的参数决定。
rsql() {
  local host="$1"
  shift
  MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql --no-defaults -h "$host" -P 3306 -uroot --connect-timeout=5 "$@"
}

# 一个实例当前的角色，只看它自己的状态，不看标签：
#   primary  可写，且没有配置复制
#   replica  配置了复制（不管线程是否在跑）
#   unknown  只读但没有复制（初始化做了一半，或切换做了一半），连不上也算
# replication_connection_configuration 每个复制通道一行，RESET REPLICA ALL 之后就没有了。
instance_role() { # <host>
  local out ro ch
  out="$(rsql "$1" -N -B -e "SELECT @@global.super_read_only, (SELECT COUNT(*) FROM performance_schema.replication_connection_configuration)" 2>/dev/null)" || {
    echo unknown
    return 0
  }
  ro="$(cut -f1 <<<"$out")"
  ch="$(cut -f2 <<<"$out")"
  if [ "$ch" != 0 ]; then
    echo replica
  elif [ "$ro" = 0 ]; then
    echo primary
  else
    echo unknown
  fi
}

# 复制源的主机名（只看第一个通道），没配复制时为空。
replication_source() { # <host>
  rsql "$1" -N -B -e "SELECT HOST FROM performance_schema.replication_connection_configuration LIMIT 1"
}

# 在除 <self> 以外的序号里找主库，打印它的稳定名字。<self> 传 -1 表示所有序号都看。
# 返回 0：恰好一个；1：一个都没有；2：不止一个（双主，必须人工处理，绝不自己挑一个）。
find_primary() { # <self>
  local i found="" n=0 h
  for ((i = 0; i < REPLICAS; i++)); do
    [ "$i" = "$1" ] && continue
    h="$(peer_host "$i")"
    if [ "$(instance_role "$h")" = primary ]; then
      found="$h"
      n=$((n + 1))
    fi
  done
  if [ "$n" -gt 1 ]; then
    log "more than one writable instance without replication in $RELEASE; refusing to pick one"
    return 2
  fi
  [ "$n" = 1 ] || return 1
  echo "$found"
}

# 除 <self> 以外有没有任何一个 Pod 存在。headless Service 开了 publishNotReadyAddresses，
# Pod 一建出来名字就能解析，不管就没就绪；解析不到说明这个 Pod 不存在。
any_peer_exists() { # <self>
  local i
  for ((i = 0; i < REPLICAS; i++)); do
    [ "$i" = "$1" ] && continue
    if getent hosts "$(peer_host "$i")" >/dev/null 2>&1; then
      return 0
    fi
  done
  return 1
}
