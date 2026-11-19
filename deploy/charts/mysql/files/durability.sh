#!/bin/bash
# 给 middleware.sh 经 kubectl exec 调用：把这个实例的落盘方式设成它当前的角色应有的样子（lib.sh 的 durability_want）。
#
#   durability.sh apply
#
# 为什么是运行时的 SET PERSIST 而不是启动参数：主从是同一份 Pod 模板，而且角色会在不重启的情况下变
# （切换手册里提升从库）。和 super_read_only 是同一个道理、同一种做法：写进数据目录的 mysqld-auto.cnf，
# 重启后还在。两个变量都是动态的，改了立刻生效，不用重启。
#
# 可以重复执行；已经是想要的值时不动。super_read_only 不拦 SET PERSIST（它拦的是改表）。
#
# 提升从库时不能指望事后再来跑这个脚本：切换手册在放开写入之前就把它设回 1 1。
# 这里是“平时让实例和 values 一致”的那一步，middleware.sh mysql 每次都会对每个 Pod 执行。
set -euo pipefail
# shellcheck source=lib.sh
. /scripts/lib.sh

case "${1:-}" in
apply)
  role="$(instance_role 127.0.0.1)"
  want="$(durability_want "$role")"
  have="$(durability_have 127.0.0.1)"
  if [ "$have" = "$want" ]; then
    echo "durability: role=$role flush_log_at_trx_commit/sync_binlog=${want/ //} (unchanged)"
    exit 0
  fi
  rsql 127.0.0.1 -e "SET PERSIST innodb_flush_log_at_trx_commit = ${want% *}; SET PERSIST sync_binlog = ${want#* };"
  echo "durability: role=$role flush_log_at_trx_commit/sync_binlog=${want/ //} (was ${have/ //})"
  ;;
*)
  echo "usage: $0 apply" >&2
  exit 2
  ;;
esac
