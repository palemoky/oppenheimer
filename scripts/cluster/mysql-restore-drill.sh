#!/usr/bin/env bash
# MySQL 备份的恢复演练：在 MacBook 上运行，对集群里一个实例的最新备份做一次真实的恢复，再和线上比对。
#
#   mysql-restore-drill.sh <release>      release：mysql-order-0、mysql-order-1、mysql-main
#
# 做什么：
#   1. 在 mysql 命名空间起一个临时的 MySQL Pod（和线上同一个镜像，数据放 emptyDir，只读挂备份的 PVC）
#   2. 校验最新一份备份的 sha256，把它灌进去，记恢复耗时
#   3. 逐表比对“恢复出来的”和“线上从库的”：行数和 CHECKSUM TABLE；再比较存储过程、触发器、事件、视图的个数；
#      再核对恢复出来的 gtid_executed 和备份记录的一致
#   4. 删掉临时 Pod（trap：失败、被中断也删）
#
# 不碰线上的实例：对线上从库（没有从库的 mysql-main 是它自己）只做只读查询（SELECT 和 CHECKSUM TABLE）。
# 临时 Pod 不带 app.kubernetes.io/name=mysql 这类标签，不会被任何线上 Service 或 PDB 选中；
# root 只允许从容器内的 localhost 登录且没有密码，Pod 不暴露在任何 Service 后面，所以不需要密码。
#
# 注意：这个比对只在“备份之后线上没有新写入”时才成立。线上有流量时，行数和校验和对不上是正常的，
# 不代表备份有问题；有流量时要改用对账工具，按备份时刻的 GTID（meta 里的 gtid_executed）去比，
# 那是阶段 2c 的事。所以这个演练适合在没有流量的时候跑（比如刚备份完，或者备份之后停了写入）。
#
# 在 macOS 自带的 bash 3.2 上运行：不用关联数组、mapfile 这些 bash 4 的特性。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
NS="mysql"
KNOWN_RELEASES="mysql-order-0 mysql-order-1 mysql-main"

POD=""
TMPD=""
cleanup() {
  if [[ -n "$TMPD" ]]; then rm -rf "$TMPD"; fi
  if [[ -n "$POD" ]]; then
    echo "==> deleting $POD"
    kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
}

# 下面两个函数从标准输入读 SQL，在标准输出给出 -N -B 的结果（制表符分隔）。
# 单独拆出来：本地测试时可以 source 这个脚本再把它们换成 docker exec 的版本。

# 临时 Pod 里的 mysql，root 无密码，只经容器内的 socket。
q_restored() {
  kubectl -n "$NS" exec -i "$POD" -c mysql -- mysql --no-defaults -uroot -N -B
}

# 线上从库里的 mysql，用 Pod 里 chart 带的 lib.sh 的 rsql（密码是 Pod 里的环境变量，不经过这台机器）。
# 这里只会收到 SELECT 和 CHECKSUM TABLE。
q_online() {
  kubectl -n "$NS" exec -i "$ONLINE_POD" -c mysql -- bash -c '. /scripts/lib.sh; rsql 127.0.0.1 -N -B'
}

# 生成并执行某一侧的报告，打印 "<键>\t<行数>\t<校验和>" 的行，按键排序。
#   库里每张基表：键 db.table
#   每个库的存储过程/函数、触发器、事件、视图的个数：键 obj:db:kind，第三列是 -
# $1：q_restored 或 q_online
side_report() {
  local runner="$1" in_list="" db tables sql t
  for db in $DATABASES; do in_list="$in_list,'$db'"; done
  in_list="${in_list#,}"
  tables="$(
    "$runner" <<SQL
SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES
WHERE TABLE_SCHEMA IN ($in_list) AND TABLE_TYPE = 'BASE TABLE' ORDER BY 1, 2;
SQL
  )"
  sql="
SELECT 'obj', CONCAT('obj:', ROUTINE_SCHEMA, ':routines'), COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA IN ($in_list) GROUP BY ROUTINE_SCHEMA;
SELECT 'obj', CONCAT('obj:', TRIGGER_SCHEMA, ':triggers'), COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA IN ($in_list) GROUP BY TRIGGER_SCHEMA;
SELECT 'obj', CONCAT('obj:', EVENT_SCHEMA, ':events'), COUNT(*) FROM information_schema.EVENTS WHERE EVENT_SCHEMA IN ($in_list) GROUP BY EVENT_SCHEMA;
SELECT 'obj', CONCAT('obj:', TABLE_SCHEMA, ':views'), COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA IN ($in_list) GROUP BY TABLE_SCHEMA;
"
  while IFS=$'\t' read -r db t; do
    [[ -n "$db" ]] || continue
    # 库名、表名放进反引号：表名里的反引号这里没有处理，我们的表名是字母数字下划线。
    sql="$sql
SELECT 'rows', '$db.$t', COUNT(*) FROM \`$db\`.\`$t\`;
CHECKSUM TABLE \`$db\`.\`$t\`;"
  done <<<"$tables"
  "$runner" <<<"$sql" | awk -F'\t' '
    $1 == "obj"  && NF == 3 { print $2 "\t" $3 "\t-"; next }
    $1 == "rows" && NF == 3 { rows[$2] = $3; keys[$2] = 1; next }
    NF == 2 { ck[$1] = $2; keys[$1] = 1 }
    END { for (k in keys) print k "\t" (k in rows ? rows[k] : "?") "\t" (k in ck ? ck[k] : "?") }
  ' | LC_ALL=C sort
}

main() {
  local rel="${1:-}"
  if [[ -z "$rel" ]] || [[ " $KNOWN_RELEASES " != *" $rel "* ]]; then
    echo "usage: $0 <release>   (one of: $KNOWN_RELEASES)" >&2
    exit 2
  fi

  # shellcheck source=scripts/cluster/cluster.env
  source "$HERE/cluster.env"
  export KUBECONFIG="$KUBECONFIG_OUT"
  [[ -f "$KUBECONFIG" ]] || {
    echo "$KUBECONFIG not found; run scripts/cluster/k3s.sh kubeconfig first" >&2
    exit 1
  }

  # 镜像、库列表都从这个实例的 CronJob 里取：备份和演练用同一个镜像、同一份库列表，不会各写各的。
  local cj="cronjob/$rel-backup" image svc
  image="$(kubectl -n "$NS" get "$cj" -o jsonpath='{.spec.jobTemplate.spec.template.spec.containers[0].image}')"
  DATABASES="$(kubectl -n "$NS" get "$cj" -o jsonpath='{.spec.jobTemplate.spec.template.spec.containers[0].env[?(@.name=="DATABASES")].value}')"
  [[ -n "$image" && -n "$DATABASES" ]] || {
    echo "cannot read image or DATABASES from $cj" >&2
    exit 1
  }

  # 线上比对的对象：有 -replica Service（两个副本）就是从库，否则是 -primary 指向的唯一实例。和 CronJob 备份的是同一个。
  if kubectl -n "$NS" get service "$rel-replica" >/dev/null 2>&1; then svc="$rel-replica"; else svc="$rel-primary"; fi
  ONLINE_POD="$(kubectl -n "$NS" get service "$svc" -o jsonpath='{.spec.selector.statefulset\.kubernetes\.io/pod-name}')"
  [[ -n "$ONLINE_POD" ]] || {
    echo "service $svc has no pod-name selector" >&2
    exit 1
  }
  echo "==> restore drill for $rel: image $image, databases [$DATABASES], comparing against $ONLINE_POD (via $svc, read-only)"

  POD="restore-drill-$rel-$(date +%H%M%S)"
  trap cleanup EXIT
  trap 'exit 130' INT TERM

  # 数据放 emptyDir，不用 PVC：演练完 Pod 一删就什么都没有了。备份的 PVC 只读挂载（volume 和 volumeMount 都标 readOnly）。
  # 只调度到 agent（和线上的 MySQL 一样不上 server 节点）；内存上限 1Gi，buffer pool 给 256M。
  # 参数和线上 server.cnf 一致的是 GTID、字符集、时区；关掉 binlog 的刷盘等待只是为了让恢复快一点，演练的数据不要求持久。
  # activeDeadlineSeconds：脚本被杀、trap 没机会跑时，Pod 自己也会在两小时后结束，不会一直占着内存。
  kubectl -n "$NS" create -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  labels:
    app.kubernetes.io/name: mysql-restore-drill
    app.kubernetes.io/instance: $rel
spec:
  restartPolicy: Never
  activeDeadlineSeconds: 7200
  securityContext:
    runAsNonRoot: true
    runAsUser: 999
    runAsGroup: 999
    fsGroup: 999
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: node-role.kubernetes.io/control-plane
                operator: DoesNotExist
  containers:
    - name: mysql
      image: $image
      args:
        - --gtid-mode=ON
        - --enforce-gtid-consistency=ON
        - --log-bin=binlog
        - --server-id=9999
        - --character-set-server=utf8mb4
        - --default-time-zone=+00:00
        - --innodb-buffer-pool-size=256M
        - --innodb-flush-log-at-trx-commit=2
        - --sync-binlog=0
      env:
        - name: MYSQL_ALLOW_EMPTY_PASSWORD
          value: "yes"
      readinessProbe:
        exec:
          command: ["mysqladmin", "--protocol=tcp", "-h127.0.0.1", "-uroot", "ping"]
        periodSeconds: 3
        failureThreshold: 200
      resources:
        requests: { cpu: 100m, memory: 512Mi }
        limits: { memory: 1Gi }
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
      volumeMounts:
        - { name: data, mountPath: /var/lib/mysql }
        - { name: backup, mountPath: /backup, readOnly: true }
  volumes:
    - name: data
      emptyDir: { sizeLimit: 10Gi }
    - name: backup
      persistentVolumeClaim:
        claimName: mysql-backup
        readOnly: true
EOF
  echo "==> waiting for $POD (pull image, initialize the data directory)"
  kubectl -n "$NS" wait --for=condition=Ready "pod/$POD" --timeout=10m >/dev/null

  # 这个实例最新的一份备份：只认符合时间戳命名的目录（.tmp-* 不算），按名字排序取最后一个。
  local dir
  dir="$(kubectl -n "$NS" exec "$POD" -c mysql -- bash -c \
    "ls -1d /backup/$rel/[0-9]*Z 2>/dev/null | grep -E '/[0-9]{8}T[0-9]{6}Z\$' | sort | tail -n 1" || true)"
  if [[ -z "$dir" ]]; then
    echo "no backup found under /backup/$rel (run: make middleware-backup)" >&2
    exit 1
  fi
  echo "==> latest backup: $dir"
  kubectl -n "$NS" exec "$POD" -c mysql -- cat "$dir/meta" | sed 's/^/    /'
  local meta_gtid
  meta_gtid="$(kubectl -n "$NS" exec "$POD" -c mysql -- sed -n 's/^gtid_executed=//p' "$dir/meta")"
  # 后面要把它拼进 SQL，只允许 GTID 集合会有的字符。
  case "$meta_gtid" in '' | *[!0-9a-fA-F:,-]*)
    echo "unexpected gtid_executed in meta: '$meta_gtid'" >&2
    exit 1
    ;;
  esac

  echo "==> verifying sha256"
  kubectl -n "$NS" exec "$POD" -c mysql -- bash -c "cd '$dir' && sha256sum -c sha256"

  # 灌进去。先 RESET BINARY LOGS AND GTIDS：镜像的 entrypoint 初始化时已经用 root 执行了一些语句，
  # 开着 GTID 和 binlog 它们占了 gtid_executed，而 dump 开头的 SET @@GLOBAL.GTID_PURGED 要求它是空的。
  # pipefail：解压或 mysql 任何一段失败都让这里失败。
  echo "==> restoring"
  local t0=$SECONDS restore_s
  kubectl -n "$NS" exec "$POD" -c mysql -- bash -c \
    "set -o pipefail; { echo 'RESET BINARY LOGS AND GTIDS;'; zstd -dcq '$dir/dump.sql.zst'; } | mysql --no-defaults -uroot"
  restore_s=$((SECONDS - t0))

  local bad=0

  echo "==> gtid_executed"
  local same
  same="$(q_restored <<<"SELECT GTID_SUBTRACT(@@global.gtid_executed, '$meta_gtid') = '' AND GTID_SUBTRACT('$meta_gtid', @@global.gtid_executed) = '';")"
  if [[ "$same" == 1 ]]; then
    echo "    restored gtid_executed matches the backup meta"
  else
    echo "    MISMATCH: restored gtid_executed differs from the meta ($meta_gtid)" >&2
    bad=1
  fi

  echo "==> comparing restored vs online $ONLINE_POD"
  TMPD="$(mktemp -d)"
  side_report q_restored >"$TMPD/restored"
  side_report q_online >"$TMPD/online"
  local tab=$'\t' key rr rc lr lc status
  printf '    %-40s %12s %12s  %-14s %-14s  %s\n' "table / object" "rows(rest)" "rows(live)" "cksum(rest)" "cksum(live)" "result"
  while IFS="$tab" read -r key rr rc lr lc; do
    status=OK
    if [[ "$rr" != "$lr" || "$rc" != "$lc" ]]; then
      status=MISMATCH
      bad=1
    fi
    printf '    %-40s %12s %12s  %-14s %-14s  %s\n' "$key" "$rr" "$lr" "$rc" "$lc" "$status"
  done < <(LC_ALL=C join -t "$tab" -a 1 -a 2 -e MISSING -o 0,1.2,1.3,2.2,2.3 "$TMPD/restored" "$TMPD/online")
  # 一侧没有的库对象个数（比如线上没有触发器、恢复出来也没有）两边都不出现，不算不一致；
  # 只有一侧有（另一侧 MISSING）就会被上面标成 MISMATCH。

  echo "==> restore took ${restore_s}s"
  if [[ "$bad" != 0 ]]; then
    echo "DRILL FAILED: the restored data does not match $ONLINE_POD (if the live instance took writes after the backup this is expected; see the header of this script)" >&2
    exit 1
  fi
  echo "==> DRILL OK: $rel backup ${dir##*/} restores and matches $ONLINE_POD"
}

# 被 source 时（本地测试）不执行 main。
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
