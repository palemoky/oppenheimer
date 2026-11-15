#!/bin/bash
# 主容器的入口：从 Pod 名算出 server-id，再交给官方镜像的入口脚本。
# StatefulSet 的 Pod 模板对所有序号是同一份，按序号不同的参数只能在容器里算。
set -euo pipefail
# shellcheck source=lib.sh
. /scripts/lib.sh

ord="$(ordinal)"
# report-host 让主库的 SHOW REPLICAS 显示从库的稳定名字，而不是 Pod 的地址。
# --defaults-extra-file 必须是 mysqld 的第一个参数；官方入口脚本初始化、起临时实例时都会原样带上它。
exec docker-entrypoint.sh mysqld \
  --defaults-extra-file="$CNF" \
  --server-id="$(server_id "$ord")" \
  --report-host="$(peer_host "$ord")"
