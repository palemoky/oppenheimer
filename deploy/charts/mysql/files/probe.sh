#!/bin/bash
# 探针。连 127.0.0.1 而不是套接字：官方镜像第一次初始化时会起一个只听套接字的临时实例，
# 那时探针不应该算它成功。
#
#   probe.sh startup   能用 root 登录并执行语句（崩溃恢复、首次初始化都完成了）
#   probe.sh alive     进程能应答。不检查复制：复制断了重启 mysqld 也修不好，只会让从库反复重启
#   probe.sh ready     能应答；配置了复制的，IO 和 SQL 两个线程都要在跑
set -uo pipefail
# shellcheck source=lib.sh
. /scripts/lib.sh

case "${1:-}" in
startup)
  rsql 127.0.0.1 -N -B -e "SELECT 1" >/dev/null
  ;;
alive)
  # mysqladmin ping 只要服务端在应答就返回 0，即使认证失败也一样；这里要的正是这个。
  MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysqladmin --no-defaults -h 127.0.0.1 -P 3306 -uroot --connect-timeout=3 ping >/dev/null 2>&1
  ;;
ready)
  # 按状态判断而不是按序号：切换之后序号 1 是主库，按序号判断会让主库的 Service 没有端点。
  # 只读又没有复制的实例（初始化或切换做了一半）不就绪，免得被当成从库读到旧数据。
  out="$(rsql 127.0.0.1 -N -B -e "
    SELECT @@global.super_read_only,
      (SELECT COUNT(*) FROM performance_schema.replication_connection_configuration),
      (SELECT COUNT(*) FROM performance_schema.replication_connection_status WHERE SERVICE_STATE <> 'ON'),
      (SELECT COUNT(*) FROM performance_schema.replication_applier_status WHERE SERVICE_STATE <> 'ON')")" || exit 1
  read -r ro channels io_down sql_down <<<"$out"
  if [ "$channels" != 0 ]; then
    [ "$io_down" = 0 ] && [ "$sql_down" = 0 ]
  else
    [ "$ro" = 0 ]
  fi
  ;;
*)
  echo "usage: $0 startup | alive | ready" >&2
  exit 2
  ;;
esac
