#!/usr/bin/env bash
# 在 MacBook 上给集群部署中间件（设计文档 2b）。每个子命令都可以重复运行。
#
#   middleware.sh mysql   三个 MySQL 实例：Secret、chart，最后只读地核对并打印主从状态
#   middleware.sh redis   两套 Redis：redis-gate（主从加 Sentinel）、redis-cache（Cluster）
#   middleware.sh kafka   Strimzi 的 Operator 和 3 节点的 Kafka
#   middleware.sh all     redis、kafka、mysql 依次执行
#   middleware.sh mysql-backup [release...]   立刻手工跑一次 MySQL 备份并等它完成（不带参数是三个实例）
#
# 每个子命令之前先执行 common（建 PriorityClass）。
#
# 用 $KUBECONFIG_OUT 指向的 kubeconfig，不看当前的 context（这台机器上还有 OrbStack 的集群）。
# 在 macOS 自带的 bash 3.2 上运行：不用关联数组、mapfile 这些 bash 4 的特性。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
ROOT="$(cd "$HERE/../.." && pwd)"

export KUBECONFIG="$KUBECONFIG_OUT"
if [[ ! -f "$KUBECONFIG" ]]; then
  echo "$KUBECONFIG not found; run scripts/cluster/k3s.sh kubeconfig first" >&2
  exit 1
fi

ensure_namespace() {
  kubectl create namespace "$1" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

# 所有子命令共用的前置步骤：中间件的 PriorityClass（说明在清单里）。各 chart 的 Pod 引用它，要先于它们存在。
common() {
  echo "==> priorityclass middleware"
  kubectl apply -f "$ROOT/deploy/cluster/middleware/priorityclass.yaml" >/dev/null
}

# ---------- MySQL ----------

MYSQL_NS="mysql"
MYSQL_CHART="$ROOT/deploy/charts/mysql"
MYSQL_VALUES="$ROOT/deploy/cluster/mysql"
MYSQL_RELEASES=(mysql-order-0 mysql-order-1 mysql-main)

# 每个实例的 Secret 里应有的键。以后加账号就在这里加一个名字：已经存在的 Secret 会被补上缺的键。
MYSQL_SECRET_KEYS=(root-password repl-password app-password cdc-password backup-password)

# Secret 不存在时用随机值创建；存在就只补上缺的键，已有的键不动：重跑脚本不能换掉正在用的密码
# （数据目录里的账号还是旧密码）。
# 密码不打印，也不出现在命令行参数里：清单经标准输入交给 kubectl，printf 是 shell 的内建命令。
# 创建用 create 而不是 apply：client-side apply 会把整份清单（含密码）存进 last-applied-configuration 注解。
# 补键用 server-side apply：它不写 last-applied 注解，只在 managedFields 里记“这个管理者拥有哪些字段”
# （字段路径，不含值），而且只带这一个键的话，别的键（别的管理者拥有的）不会被动。值用 data（base64）。
mysql_secret() { # <release>
  local name="$1-auth" have k missing=()
  # 只取键名，不取值。
  # shellcheck disable=SC2016 # go-template 的 $k 不是 shell 变量
  have="$(kubectl -n "$MYSQL_NS" get secret "$name" --ignore-not-found -o go-template='{{range $k, $_ := .data}}{{$k}} {{end}}')"
  # 存在的 Secret 至少有一个键，所以“一个键都没取到”就是不存在。
  if [[ -z "$have" ]]; then
    echo "    creating secret $name with random passwords"
    {
      printf 'apiVersion: v1\nkind: Secret\ntype: Opaque\nmetadata:\n  name: %s\n  namespace: %s\nstringData:\n' "$name" "$MYSQL_NS"
      for k in "${MYSQL_SECRET_KEYS[@]}"; do
        printf '  %s: "%s"\n' "$k" "$(openssl rand -hex 16)"
      done
    } | kubectl create -f - >/dev/null
    return 0
  fi
  for k in "${MYSQL_SECRET_KEYS[@]}"; do
    case " $have " in
    *" $k "*) ;;
    *) missing+=("$k") ;;
    esac
  done
  if [[ ${#missing[@]} -eq 0 ]]; then
    echo "    secret $name exists, keeping it"
    return 0
  fi
  echo "    secret $name exists; adding missing keys: ${missing[*]}"
  {
    printf 'apiVersion: v1\nkind: Secret\nmetadata:\n  name: %s\n  namespace: %s\ndata:\n' "$name" "$MYSQL_NS"
    for k in "${missing[@]}"; do
      printf '  %s: "%s"\n' "$k" "$(openssl rand -hex 16 | base64 | tr -d '\n')"
    done
  } | kubectl apply --server-side --field-manager=middleware-sh -f - >/dev/null
}

# 备份用的 NAS 存储（PV 加 PVC），chart 里的 CronJob 引用这个 PVC，要先于 chart 存在。
# 清单里的 __NAS_IP__、__NAS_EXPORT__ 不是密钥，可以用 sed 填。
mysql_backup_storage() {
  echo "==> mysql backup storage (NFS $NAS_IP:$NAS_EXPORT/backup/mysql)"
  sed -e "s#__NAS_IP__#$NAS_IP#g" -e "s#__NAS_EXPORT__#$NAS_EXPORT#g" "$MYSQL_VALUES/backup-pv.yaml" | kubectl apply -f - >/dev/null
}

# 主从关系的核对，只读：不改任何东西，不一致就报出来、以非零退出，处理交给人。
# 主库以 -primary Service 实际指向的 Pod 为准（也就是集群里生效的 primaryOrdinal），再到每个实例里看它自己的状态：
# 被指向的要可写且没配复制，另一个要在复制它；“可写但不是主库”的（切换后回来的旧主库）报成异常。
# 具体的判断在 chart 带的 status.sh 里，密码只在 Pod 里从环境变量取。
mysql_check() { # <release>，不一致时返回 1
  local rel="$1" n i primary rc=0
  primary="$(kubectl -n "$MYSQL_NS" get service "$rel-primary" -o jsonpath='{.spec.selector.statefulset\.kubernetes\.io/pod-name}')"
  n="$(kubectl -n "$MYSQL_NS" get statefulset "$rel" -o jsonpath='{.spec.replicas}')"
  echo "  $rel (primary per Service: ${primary:-<none>})"
  if [[ -z "$primary" ]]; then
    echo "    ANOMALY: $rel-primary has no pod-name selector" >&2
    return 1
  fi
  for ((i = 0; i < n; i++)); do
    echo "    $rel-$i"
    if ! kubectl -n "$MYSQL_NS" exec "$rel-$i" -c mysql -- /scripts/status.sh show "$primary" | sed 's/^/      /'; then
      rc=1
    fi
  done
  return "$rc"
}

cmd_mysql() {
  local rel failed=0
  echo "==> mysql (namespace $MYSQL_NS)"
  ensure_namespace "$MYSQL_NS"
  mysql_backup_storage
  for rel in "${MYSQL_RELEASES[@]}"; do
    echo "==> $rel"
    mysql_secret "$rel"
    # 不加 --wait：Helm 4 默认只等 hook。建库建账号的 post hook 要先跑，从库的 CLONE 等着它建的 repl 账号；
    # 加了 --wait 就是先等从库就绪再跑 hook，互相等住。--timeout 管的是 hook Job，第一次要拉镜像、初始化。
    helm upgrade --install "$rel" "$MYSQL_CHART" --namespace "$MYSQL_NS" \
      -f "$MYSQL_VALUES/$rel.yaml" --timeout 20m >/dev/null
    # 从库要 CLONE 全量，数据多的时候会久。
    kubectl -n "$MYSQL_NS" rollout status "statefulset/$rel" --timeout=30m
  done

  echo "==> mysql status"
  for rel in "${MYSQL_RELEASES[@]}"; do
    mysql_check "$rel" || failed=1
  done
  kubectl -n "$MYSQL_NS" get pods -o wide
  kubectl -n "$MYSQL_NS" get endpointslices -o wide
  if [[ "$failed" != 0 ]]; then
    echo "mysql: primary/replica state does not match the Services; see ANOMALY lines above (nothing was changed)" >&2
    return 1
  fi
}

# 立刻手工跑一次备份：从 CronJob 建一个 Job，等它完成，打印产物。不带参数就是三个实例都备。
# 产物的路径和大小取自 Job 日志里 backup.sh 打的 "backup ok" 那一行（MacBook 上没有挂 NAS）。
cmd_mysql_backup() { # [release...]
  local rels=("$@") rel known job active found out path
  if [[ ${#rels[@]} -eq 0 ]]; then rels=("${MYSQL_RELEASES[@]}"); fi
  local failed=0
  for rel in "${rels[@]}"; do
    found=0
    for known in "${MYSQL_RELEASES[@]}"; do [[ "$known" == "$rel" ]] && found=1; done
    if [[ "$found" == 0 ]]; then
      echo "unknown mysql release: $rel (known: ${MYSQL_RELEASES[*]})" >&2
      return 2
    fi
  done
  for rel in "${rels[@]}"; do
    echo "==> backup $rel"
    # 手工建的 Job 不受 concurrencyPolicy 约束，所以先自己看有没有正在跑的：两个备份同时写同一个实例的目录、
    # 同时做保留清理会互相踩。
    active="$(kubectl -n "$MYSQL_NS" get jobs -l "app.kubernetes.io/name=mysql-backup,app.kubernetes.io/instance=$rel" \
      -o go-template='{{range .items}}{{if .status.active}}{{.metadata.name}} {{end}}{{end}}')"
    if [[ -n "$active" ]]; then
      echo "    a backup of $rel is already running: $active" >&2
      failed=1
      continue
    fi
    job="$rel-backup-manual-$(date -u +%y%m%d%H%M%S)"
    if ! kubectl -n "$MYSQL_NS" create job "$job" --from="cronjob/$rel-backup" >/dev/null; then
      echo "    cannot create a job from cronjob/$rel-backup (is backup.enabled and the chart installed?)" >&2
      failed=1
      continue
    fi
    # 等 Job 完成或失败。kubectl wait 一次只能等一个条件，等成功会把失败拖到超时，所以轮询状态。
    local deadline=$((SECONDS + 3900)) state=""
    while ((SECONDS < deadline)); do
      # 只挑 Complete 和 Failed 这两个终态的条件。不能把所有为真的条件拼起来比：Kubernetes 1.31 起 Job 成功时
      # 有两个为真的条件（SuccessCriteriaMet、Complete），失败时是 FailureTarget、Failed（2026-11-15 实测，
      # 第一版因此一直等到超时，而备份 3 秒就做完了）。
      state="$(kubectl -n "$MYSQL_NS" get job "$job" -o go-template='{{range .status.conditions}}{{if and (eq .status "True") (or (eq .type "Complete") (eq .type "Failed"))}}{{.type}}{{end}}{{end}}')"
      [[ "$state" == Complete || "$state" == Failed ]] && break
      sleep 3
    done
    if [[ "$state" != Complete ]]; then
      echo "    FAILED ($job, state: ${state:-timeout}); last log lines:" >&2
      kubectl -n "$MYSQL_NS" logs "job/$job" --tail=15 2>&1 | sed 's/^/      /' >&2 || true
      failed=1
      continue
    fi
    # 日志的最后一行就是产物的路径和大小（backup.sh 的 "backup ok: <路径> (<字节> bytes, ...)"）。
    out="$(kubectl -n "$MYSQL_NS" logs "job/$job" --tail=50 2>&1 | grep 'backup ok:' || true)"
    if [[ -z "$out" ]]; then
      echo "    job $job completed but the log has no 'backup ok' line" >&2
      failed=1
      continue
    fi
    path="${out#*backup ok: }"
    echo "    ${path}  (job $job)"
    path="${path%% *}"
    echo "    on the NAS: $NAS_EXPORT/backup/mysql/${path#/backup/}"
  done
  return "$failed"
}

cmd="${1:-}"
# 新的子命令（redis、kafka）照 mysql 的样子加一个 cmd_xxx，在这里各加一行，并加进 all。
# Redis：两套（设计文档 2b 第 4 节）。redis-gate 是一主一从加 3 个 Sentinel，给库存闸门和锁用；
# redis-cache 是 Redis Cluster 3 主 3 从，给查询缓存和限流用。集群的组建在 chart 的 hook 任务里，可重复执行。
cmd_redis() {
  echo "==> redis (namespace redis)"
  ensure_namespace redis
  helm upgrade --install redis-gate "$ROOT/deploy/charts/redis-ha" -n redis \
    -f "$ROOT/deploy/cluster/redis/redis-gate.yaml" --wait --timeout 10m >/dev/null
  helm upgrade --install redis-cache "$ROOT/deploy/charts/redis-cluster" -n redis \
    -f "$ROOT/deploy/cluster/redis/redis-cache.yaml" --wait --timeout 15m >/dev/null
  kubectl -n redis get pods -o wide
  echo "==> redis-gate: master according to sentinel"
  kubectl -n redis exec redis-gate-sentinel-0 -- redis-cli -p 26379 sentinel get-master-addr-by-name gate
  echo "==> redis-cache: cluster state"
  local info
  info="$(kubectl -n redis exec redis-cache-0 -- redis-cli cluster info)"
  printf '%s\n' "$info" | tr -d '\r' | awk -F: '/^cluster_state|^cluster_known_nodes|^cluster_size/ { print "    " $0 }'
  # 主从配对由 redis-cli --cluster create 决定，它不知道 Pod 在哪个节点上。主从落在同一个节点时
  # 那个节点一挂这一片就没了。原来这里只把配对打印出来，结果重建之后有一对同节点的没人发现
  # （设计文档 2b 第 10 节的发现 10），现在交给这个脚本核对并改挂从库；它改不了的情况会以非零退出。
  "$HERE/redis-cache-pairing.sh" fix
}

# Kafka：Strimzi 的 Operator 加一个 3 节点的 KRaft 集群（设计文档 2b 第 5 节）。
# Operator 用 Helm 装；Kafka 和 KafkaNodePool 是它的自定义资源，提交之后由 Operator 去创建 Pod。
cmd_kafka() {
  echo "==> kafka (namespace kafka): strimzi $STRIMZI_VERSION"
  ensure_namespace kafka
  helm upgrade --install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
    --version "$STRIMZI_VERSION" -n kafka --wait --timeout 10m >/dev/null
  kubectl apply -f "$ROOT/deploy/cluster/kafka/kafka.yaml"
  kubectl -n kafka wait kafka/kafka --for=condition=Ready --timeout=15m
  kubectl -n kafka get pods -o wide
  kubectl -n kafka get kafka kafka \
    -o jsonpath='{"    version "}{.status.kafkaVersion}{", bootstrap "}{.status.listeners[0].bootstrapServers}{"\n"}'
}

case "$cmd" in
mysql)
  common
  cmd_mysql
  ;;
redis)
  common
  cmd_redis
  ;;
kafka)
  common
  cmd_kafka
  ;;
mysql-backup)
  shift
  cmd_mysql_backup "$@"
  ;;
all)
  common
  cmd_redis
  cmd_kafka
  cmd_mysql
  ;;
*)
  echo "usage: $0 mysql | redis | kafka | all | mysql-backup [release...]" >&2
  exit 2
  ;;
esac
