#!/usr/bin/env bash
# 在集群上做删 Pod 对账测试（make cluster-chaos）。本机的对应物是 scripts/chaos.sh（make chaos）：
# 压测期间随机删业务 Pod（本机是 kill -9 进程），压测结束后等 Deployment 恢复、系统静止，再对账，有差异判失败。
# 判定标准是数据一致，不是零报错：被删的 Pod 上的请求会失败，压测工具用同一个幂等键重试提交，
# 对账查的是账本、库存、订单状态、支付单、商家索引对不对得上。完整的故障实验是后面阶段的事，这里只是最小版本。
#
# 做的事，按顺序：
#   1. 构建并推送六个服务和 chaos 镜像（deploy/docker/chaos.Dockerfile：load、recon）。E2E_SKIP_IMAGES=1 跳过，这时必须给 IMAGE_TAG
#   2. 占用命名空间 oppenheimer-e2e（和 e2e 共用，见下面的说明）：正有一次 e2e 或 chaos 在跑就拒绝（退出码 3），
#      上一次 E2E_KEEP=1 留下的现场先清掉
#   3. 建 e2e_* 库并授权（和 e2e 同一套，scripts/cluster/e2e-lib.sh）
#   4. services.sh e2e-prepare，建 ConfigMap chaos-phase（deploy/cluster/chaos/phase.sh）和发压 Job（deploy/cluster/chaos/load-job.yaml）
#   5. services.sh e2e-up 部署六个服务，values 在 e2e 那两层上再叠 deploy/cluster/services/chaos/（关限流、调短时限、副本数）
#   6. 灌 SKU（3001、3002、3003，3003 每人限购），load warmup 装进闸门，等网关经入口就绪
#   7. load run 发压；同时每隔 CHAOS_INTERVAL 秒随机删一个名单里服务的一个就绪 Pod，直到 CHAOS_DURATION 结束
#      （头几次按随机排列把名单里每个服务各删一次，之后完全随机，同本机）
#   8. 发压结束后等所有 Deployment 恢复（副本都就绪、没有正在终止的 Pod），再 load wait-quiet、recon、load verify
#   9. 打印删 Pod 的记录、发压统计、三步的结论，按结果退出
# 结束时（无论成败）：失败时把各 Pod 的日志、事件收到日志目录的 cluster/ 下；默认 services.sh e2e-down 删掉命名空间
# 并清理中间件；E2E_KEEP=1 保留现场（命名空间标成 kept，下一次 e2e 或 chaos 会先清掉它）。
# 退出码：0 静止且对账通过；1 没有静止、对账不通过、发压失败或有意外重启（脚本中途出错被 set -e 中止的也多半是 1，
# 看输出区分）；2 参数或环境不对（部署之后的准备步骤失败也是 2）；3 命名空间被占用。
#
# 和 e2e 共用命名空间、库、清理（理由）：两者用同一个 chart 和 e2e 那两层 values（入口主机名 e2e.oppenheimer.test、
# cdc 的 server-id 1012/1022、e2e_* 库、e2e- 前缀），差别只在一层覆盖。另起一个命名空间就要另一套库名、主机名、
# server-id 和清理逻辑，换来的“能同时跑”并不需要：两者同时跑会互相干扰对方的计时和资源，结果都不可信。
# 所以共用，并用命名空间上的标签互斥（e2e-lib.sh 的“命名空间的占用”），scripts/cluster/e2e.sh 也遵守同一规则。
#
# 测试 Pod 是一个常驻的 Job，各阶段用 kubectl exec 启动、和 exec 的会话脱开（phase.sh），账本留在 Pod 的 /work 里。
# 库存服务的日志（verify 的第 19 条要找闸门修正）随被删的 Pod 一起消失，所以脚本在发压期间对每个就绪的 inventory Pod
# 跑一个 kubectl logs -f，存在 MacBook 上，verify 之前合成一个文件传进 Pod。
#
# 环境变量（CHAOS_ 开头的对应本机 scripts/chaos.sh 的同名变量，默认值不同的已注明）：
#   CHAOS_DURATION        压测时长（提交窗口），也是删 Pod 的时长，默认 120s（本机 60s：Pod 重建比进程重启慢得多）
#   CHAOS_CONCURRENCY     虚拟用户数，默认 50
#   CHAOS_USERS           用户 id 的范围，默认 500
#   CHAOS_STOCK           每个 SKU 的初始库存，默认 60
#   CHAOS_LIMIT           SKU 3003 的每人限购，默认 2
#   CHAOS_QUIET_TIMEOUT   等静止的时限，默认 3m（本机 2m）
#   CHAOS_SEED            压测的随机种子，默认随机
#   CHAOS_REJECT_BACKOFF  提交被拒绝后虚拟用户等多久再下一单，默认 200ms
#   CHAOS_LOAD_ARGS       追加给 load run 的参数
#   CHAOS_DELETE          设为 0 时不删 Pod（用来对比；本机叫 CHAOS_KILL）
#   CHAOS_INTERVAL        删 Pod 的间隔（秒），默认 5（本机是 2 到 5 秒随机）
#   CHAOS_VICTIMS         在名单里的服务，空格分隔，默认 "gateway order inventory payment"（可加 orderindex、cdc）
#   CHAOS_FORCE           设为 1 时 --grace-period=0 --force（相当于 kill -9），默认 0：正常的优雅期（摘流 15 秒、停止 20 秒）
#   CHAOS_RECON_PAIRS     recon 比哪几对，默认 payment,stock,replica,index（本机只有 payment,stock：没有从库和商家索引）
#   CHAOS_RECON_RECHECK   recon 发现差异后隔多久复查，默认 30s（本机 1s：商家索引经 cdc 异步追上，wait-quiet 不等它）
#   IMAGE_TAG、E2E_SKIP_IMAGES、E2E_KEEP、E2E_RUN_ID（默认当前 Unix 时间）、E2E_NAMESPACE（默认 oppenheimer-e2e）：同 e2e.sh
#
# 和长期运行的那一套共用 MySQL、Kafka、Redis：跑的时候它们多了一份压力，不要同时做压测。
# 在 macOS 自带的 bash 3.2 上运行。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
ROOT="$(cd "$HERE/../.." && pwd)"

export KUBECONFIG="$KUBECONFIG_OUT"
if [[ ! -f "$KUBECONFIG" ]]; then
  echo "$KUBECONFIG not found; run scripts/cluster/k3s.sh kubeconfig first" >&2
  exit 2
fi

# shellcheck source=scripts/cluster/e2e-lib.sh
source "$HERE/e2e-lib.sh"

NS="${E2E_NAMESPACE:-oppenheimer-e2e}"
export E2E_NAMESPACE="$NS"
export E2E_RUN_ID="${E2E_RUN_ID:-$(date +%s)}"
# services.sh 在 e2e 那两层 values 上再叠这一层。
export E2E_OVERLAY_DIR="$ROOT/deploy/cluster/services/chaos"
JOB_TEMPLATE="$ROOT/deploy/cluster/chaos/load-job.yaml"
PHASE_SCRIPT="$ROOT/deploy/cluster/chaos/phase.sh"
JOB="chaos"
IMAGES="gateway order inventory payment orderindex cdc chaos"
DEPLOYS="inventory order payment orderindex cdc gateway"

DURATION="${CHAOS_DURATION:-120s}"
CONCURRENCY="${CHAOS_CONCURRENCY:-50}"
USERS="${CHAOS_USERS:-500}"
STOCK="${CHAOS_STOCK:-60}"
LIMIT="${CHAOS_LIMIT:-2}"
QUIET_TIMEOUT="${CHAOS_QUIET_TIMEOUT:-3m}"
SEED="${CHAOS_SEED:-0}"
DELETE="${CHAOS_DELETE:-1}"
INTERVAL="${CHAOS_INTERVAL:-5}"
VICTIMS="${CHAOS_VICTIMS:-gateway order inventory payment}"
FORCE="${CHAOS_FORCE:-0}"
RECON_PAIRS="${CHAOS_RECON_PAIRS:-payment,stock,replica,index}"
RECON_RECHECK="${CHAOS_RECON_RECHECK:-30s}"
SKUS="3001 3002 3003"

# 测试 Pod 里连中间件和服务的地址。都是不含密码的部分，可以出现在命令行上；DSN 在 Job 的环境变量里。
# 闸门在 redis-gate（Sentinel），写法和服务一样（deploy/cluster/services/inventory.yaml 的 REDIS_ADDR）。
GATE_REDIS="sentinel://gate@redis-gate-sentinel.redis.svc:26379"
KAFKA_BROKERS="kafka-kafka-bootstrap.kafka.svc:9092"
KAFKA_PREFIX="e2e-$E2E_RUN_ID."
REDIS_PREFIX="e2e-$E2E_RUN_ID:"
GATEWAY_URL="http://e2e.oppenheimer.test"
# warmup 直接调 inventory 的 gRPC：手写边车形态下经它的 headless Service 到边车的入站端口（和别的服务的边车调它的路径一样），
# Istio 形态（MESH_ISTIO 含 inventory）下是普通 Service 的 gRPC 端口（e2e-lib.sh 的 e2e_inventory_addr）。
INVENTORY_ADDR="$(e2e_inventory_addr)"

LOG_DIR="$ROOT/tmp/cluster-chaos-logs/$E2E_RUN_ID"
EVENTS="$LOG_DIR/chaos-events.log"
DELETIONS="$LOG_DIR/deletions.txt"
RESTARTS="$LOG_DIR/unexpected-restarts.txt"
INV_LOG_DIR="$LOG_DIR/inventory-logs"

log() { echo "[chaos $(date +%H:%M:%S)] $*" | tee -a "$EVENTS"; }
die() { # 退出码 消息
  local code="$1"
  shift
  echo "$*" >&2
  exit "$code"
}

in_list() { # 词 列表
  case " $2 " in
  *" $1 "*) return 0 ;;
  esac
  return 1
}

# "60s"、"2m"、"90" 换成秒数。
dur_seconds() {
  local v="$1"
  case "$v" in
  *m) v=$((${v%m} * 60)) ;;
  *s) v="${v%s}" ;;
  esac
  case "$v" in
  '' | *[!0-9]*) return 1 ;;
  esac
  echo "$v"
}

# ---------- 参数检查 ----------
DUR_S="$(dur_seconds "$DURATION")" || die 2 "CHAOS_DURATION must look like 120s or 2m, got $DURATION"
case "$INTERVAL" in
'' | *[!0-9]* | 0) die 2 "CHAOS_INTERVAL must be a positive number of seconds, got $INTERVAL" ;;
esac
for s in $VICTIMS; do
  in_list "$s" "$DEPLOYS" || die 2 "CHAOS_VICTIMS: unknown service $s (valid: $DEPLOYS)"
done
[[ -n "${VICTIMS// /}" ]] || die 2 "CHAOS_VICTIMS is empty (use CHAOS_DELETE=0 to run without deleting pods)"

# 和 e2e 共用命名空间：正有一次在跑就拒绝。先查一次，免得白构建镜像。
e2e_check_free "$NS" || exit 3

mkdir -p "$LOG_DIR" "$INV_LOG_DIR"
ln -sfn "$E2E_RUN_ID" "$ROOT/tmp/cluster-chaos-logs/latest"
: >"$DELETIONS"

# ---------- 测试 Pod 里的阶段 ----------
POD=""

pexec() { kubectl -n "$NS" exec "$POD" -c chaos -- "$@"; }

# 填 Job 模板。值都是脚本自己算出来的（镜像名、地址、运行号），不含特殊字符。
render_job() { # image
  local line
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line//__IMAGE__/$1}"
    line="${line//__LB_GATEWAY_IP__/$LB_GATEWAY_IP}"
    line="${line//__RUN_ID__/$E2E_RUN_ID}"
    printf '%s\n' "$line"
  done <"$JOB_TEMPLATE"
}

# 阶段的退出码：结束了就输出，没结束（或者这一次 exec 失败了）输出空。
phase_status() { pexec sh /phase/phase.sh status "$1" 2>/dev/null || true; }

# 把阶段的输出取回日志目录。
phase_fetch() { # 名字
  pexec cat "/work/$1.out" >"$LOG_DIR/$1.out" 2>/dev/null || true
  pexec cat "/work/$1.err" >"$LOG_DIR/$1.err" 2>/dev/null || true
}

# 启动一个阶段并等它结束，期间每 2 秒调一次 tick。把标准输出打到终端，退出码放在 PHASE_CODE
#（超时是 timeout）。阶段自己失败不让脚本退出，由调用方判断。
PHASE_CODE=""
run_phase() { # 名字 时限秒数 命令...
  local name="$1" limit="$2" deadline
  shift 2
  pexec sh /phase/phase.sh start "$name" "$@"
  deadline=$((SECONDS + limit))
  PHASE_CODE=""
  while [[ -z "$PHASE_CODE" ]]; do
    if [[ $SECONDS -ge $deadline ]]; then
      PHASE_CODE=timeout
      break
    fi
    sleep 2
    tick
    PHASE_CODE="$(phase_status "$name")"
  done
  phase_fetch "$name"
  cat "$LOG_DIR/$name.out"
  log "$name exited with $PHASE_CODE"
}

# ---------- Pod 的观察：删谁、意外重启、库存服务日志 ----------
# tick 读一次全部 Pod，存在 PODS 里（每行 名字|删除时间|就绪|各容器的重启次数|所属服务），
# 顺带做两件事：发现意外的重启（不是脚本删的，是容器自己退出了：崩溃、OOM、存活探针失败），
# 给每个就绪的 inventory Pod 起一个 kubectl logs -f。
PODS=""
FOLLOWED=""
FOLLOW_PIDS=""
RESTARTED=""

tick() {
  local out name del ready restarts app r n
  out="$(kubectl -n "$NS" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.metadata.deletionTimestamp}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"|"}{.status.containerStatuses[*].restartCount}{" "}{.status.initContainerStatuses[*].restartCount}{"|"}{.metadata.labels.app\.kubernetes\.io/name}{"\n"}{end}' 2>/dev/null)" || return 0
  PODS="$out"
  while IFS='|' read -r name del ready restarts app; do
    [[ -n "$name" ]] || continue
    n=0
    for r in $restarts; do n=$((n + r)); done
    if [[ $n -gt 0 ]] && ! in_list "$name" "$RESTARTED"; then
      RESTARTED="$RESTARTED $name"
      echo "$name $n" >>"$RESTARTS"
      log "UNEXPECTED RESTART: pod $name restarted $n time(s); saving its previous logs"
      # 不用 --all-containers：有一个容器没重启过（通常是边车）时整条命令报错，什么也拿不到。
      # 容器名和服务名相同（Pod 名去掉末尾两段）。
      kubectl -n "$NS" logs "$name" -c "${name%-*-*}" --timestamps --previous \
        </dev/null >"$LOG_DIR/$name.previous.log" 2>&1 || true
    fi
    # 就绪了才跟：还在创建的容器 logs -f 会立刻失败。logs -f 从容器启动时的第一行开始给，就绪之前的日志不会漏。
    if [[ "$app" == inventory && "$ready" == True ]] && ! in_list "$name" "$FOLLOWED"; then
      FOLLOWED="$FOLLOWED $name"
      kubectl -n "$NS" logs -f "$name" -c inventory </dev/null >"$INV_LOG_DIR/$name.log" 2>"$INV_LOG_DIR/$name.err" &
      FOLLOW_PIDS="$FOLLOW_PIDS $!"
    fi
  done <<<"$PODS"
}

# shellcheck disable=SC2329 # 由 finish（EXIT 的 trap）调用
stop_followers() {
  local p
  for p in $FOLLOW_PIDS; do kill "$p" 2>/dev/null || true; done
  FOLLOW_PIDS=""
}

# 某个服务里可以删的 Pod：就绪、没有在终止。随机挑一个输出，没有就不输出。
pick_pod() { # 服务
  local name del ready restarts app cands=""
  while IFS='|' read -r name del ready restarts app; do
    if [[ "$app" == "$1" && -z "$del" && "$ready" == True ]]; then cands="$cands $name"; fi
  done <<<"$PODS"
  # shellcheck disable=SC2086 # 按空格拆成位置参数是有意的
  set -- $cands
  [[ $# -gt 0 ]] || return 0
  eval "echo \"\${$((RANDOM % $# + 1))}\""
}

# 头几次按随机排列把名单里每个服务各删一次，之后完全随机（同本机）。
ORDERED="$(for s in $VICTIMS; do echo "$RANDOM $s"; done | sort -n | awk '{print $2}' | tr '\n' ' ')"
DELETE_N=0
LOAD_T0=0

# shellcheck disable=SC2086 # 按空格数个数
N_VICTIMS="$(set -- $VICTIMS && echo $#)"
FORCE_NOTE=""
if [[ "$FORCE" == 1 ]]; then FORCE_NOTE=", force"; fi

pick_service() {
  if [[ $DELETE_N -lt $N_VICTIMS ]]; then
    # shellcheck disable=SC2086
    set -- $ORDERED
    shift "$DELETE_N"
    echo "$1"
    return 0
  fi
  # shellcheck disable=SC2086
  set -- $VICTIMS
  eval "echo \"\${$((RANDOM % $# + 1))}\""
}

delete_one() {
  local svc pod args
  svc="$(pick_service)"
  pod="$(pick_pod "$svc")"
  if [[ -z "$pod" ]]; then
    log "skip: no ready pod of $svc to delete (+$((SECONDS - LOAD_T0))s)"
    return 0
  fi
  args=(--wait=false)
  if [[ "$FORCE" == 1 ]]; then args+=(--grace-period=0 --force); fi
  if kubectl -n "$NS" delete pod "$pod" "${args[@]}" >/dev/null 2>&1; then
    DELETE_N=$((DELETE_N + 1))
    echo "$(date +%H:%M:%S) +$((SECONDS - LOAD_T0))s $svc $pod" >>"$DELETIONS"
    log "delete pod $pod ($svc) at +$((SECONDS - LOAD_T0))s$FORCE_NOTE"
  else
    log "WARNING: deleting pod $pod failed"
  fi
}

# 所有 Deployment 恢复：期望的副本数都可用、都是当前模板，名下没有正在终止的 Pod。
deploys_settled() {
  local out name want avail updated total del ready restarts app
  out="$(kubectl -n "$NS" get deploy -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.spec.replicas}{"|"}{.status.availableReplicas}{"|"}{.status.updatedReplicas}{"|"}{.status.replicas}{"\n"}{end}' 2>/dev/null)" || return 1
  [[ -n "$out" ]] || return 1
  while IFS='|' read -r name want avail updated total; do
    [[ -n "$name" ]] || continue
    [[ "${avail:-0}" == "$want" ]] || return 1
    [[ "${updated:-0}" == "$want" ]] || return 1
    [[ "${total:-0}" == "$want" ]] || return 1
  done <<<"$out"
  while IFS='|' read -r name del ready restarts app; do
    if [[ -n "$del" && "$app" != chaos-load ]]; then return 1; fi
  done <<<"$PODS"
  return 0
}

# ---------- 收尾 ----------
# shellcheck disable=SC2329 # 由 EXIT 的 trap 调用
finish() {
  local code=$?
  trap - EXIT INT TERM
  set +e
  stop_followers
  if [[ $code -ne 0 ]] && kubectl get namespace "$NS" >/dev/null 2>&1; then
    e2e_collect_logs "$NS" "$LOG_DIR/cluster"
  fi
  if [[ "${E2E_KEEP:-}" == 1 ]]; then
    e2e_mark_kept "$NS"
    echo "==> E2E_KEEP=1: keeping namespace $NS and the e2e data in the middleware"
    echo "    clean up later with: scripts/cluster/services.sh e2e-down"
  else
    echo "==> e2e-down"
    "$HERE/services.sh" e2e-down
  fi
  if [[ $code -eq 0 ]]; then
    echo "==> cluster chaos PASSED (run $E2E_RUN_ID); logs in $LOG_DIR"
  else
    echo "==> cluster chaos FAILED (run $E2E_RUN_ID, exit $code); logs in $LOG_DIR" >&2
  fi
  exit "$code"
}

# ---------- 1. 镜像 ----------
if [[ "${E2E_SKIP_IMAGES:-}" == 1 ]]; then
  [[ -n "${IMAGE_TAG:-}" ]] || die 2 "E2E_SKIP_IMAGES=1 needs IMAGE_TAG (the tag of images already in the registry)"
else
  IMAGE_TAG="${IMAGE_TAG:-$("$HERE/images.sh" tag)}"
  export IMAGE_TAG
  # shellcheck disable=SC2086 # 镜像名按空格拆开是有意的
  "$HERE/images.sh" all $IMAGES
fi
export IMAGE_TAG
log "cluster chaos: run $E2E_RUN_ID, image tag $IMAGE_TAG, namespace $NS, logs $LOG_DIR"
log "duration $DURATION, interval ${INTERVAL}s, victims: $VICTIMS, force $FORCE, delete $DELETE"

# ---------- 2. 命名空间 ----------
if [[ -n "$(e2e_ns_state "$NS")" ]]; then
  e2e_check_free "$NS" || exit 3
  echo "==> namespace $NS exists (kept by an earlier E2E_KEEP=1 run?); tearing it down first"
  "$HERE/services.sh" e2e-down
fi
# 另一次运行恰好同时建了它就失败退出，不进下面的 trap（那会清掉别人的现场）。
e2e_claim_ns "$NS" chaos "$E2E_RUN_ID" || exit 3

# 从这里开始改了共用的中间件，退出时一定要收尾。
trap finish EXIT
trap 'exit 130' INT TERM

# ---------- 3. 库 ----------
echo "==> mysql: replicas, e2e databases and grants"
e2e_start_replicas
e2e_create_dbs
e2e_wait_replicated

# ---------- 4. 命名空间里的准备、发压 Job ----------
"$HERE/services.sh" e2e-prepare
kubectl -n "$NS" create configmap chaos-phase --from-file=phase.sh="$PHASE_SCRIPT" \
  --dry-run=client -o yaml | kubectl -n "$NS" apply -f - >/dev/null
render_job "$REGISTRY_ADDR/oppenheimer/chaos:$IMAGE_TAG" | kubectl -n "$NS" apply -f - >/dev/null
echo "    job $NS/$JOB created"

# ---------- 5. 服务 ----------
"$HERE/services.sh" e2e-up

# 测试 Pod 跑起来（服务部署的几分钟里它早就拉完镜像了）。
for _ in $(seq 1 120); do
  POD="$(kubectl -n "$NS" get pods -l "job-name=$JOB" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "$POD" && "$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)" == Running ]]; then
    break
  fi
  POD=""
  sleep 2
done
[[ -n "$POD" ]] || die 2 "job pod of $NS/$JOB is not running after 4 minutes"
log "test pod $POD"
tick

# ---------- 6. SKU、闸门、入口 ----------
# inventory 已经就绪，表已经迁移好了。SQL 在 mysql 的 Pod 里用 root 执行（e2e-lib.sh），密码不出那个容器。
values=""
initial=""
sku_list=""
for sku in $SKUS; do
  lim=0
  if [[ "$sku" == 3003 ]]; then lim="$LIMIT"; fi
  values="$values${values:+,}($sku, 'chaos $sku', 1990, $STOCK, $lim)"
  initial="$initial${initial:+,}$sku=$STOCK"
  sku_list="$sku_list${sku_list:+,}$sku"
done
printf '%s\n' "INSERT INTO e2e_inventory_db.sku_stock (sku_id, name, unit_price, available, purchase_limit) VALUES $values;" |
  e2e_root_sql "$(e2e_mysql_pod mysql-main primary)"
log "skus $sku_list, stock $STOCK each, limit $LIMIT on 3003"

run_phase warmup 90 load warmup -inventory-addr "$INVENTORY_ADDR" -skus "$sku_list" \
  -redis-addr "$GATE_REDIS" -redis-prefix "$REDIS_PREFIX"
[[ "$PHASE_CODE" == 0 ]] || die 2 "warmup failed (see $LOG_DIR/warmup.err)"

# 网关经入口连续 3 次就绪（同 test/e2e 的 waitGatewayReady：入口刚挂上路由时可能时好时坏）。
ok=0
for _ in $(seq 1 90); do
  if pexec wget -q -O /dev/null -T 3 "$GATEWAY_URL/readyz" >/dev/null 2>&1; then
    ok=$((ok + 1))
    [[ $ok -ge 3 ]] && break
  else
    ok=0
  fi
  sleep 2
done
[[ $ok -ge 3 ]] || die 2 "gateway not ready through the ingress ($GATEWAY_URL/readyz) after 3 minutes"
log "gateway ready through the ingress"

# ---------- 7. 发压与删 Pod ----------
log "load run: duration $DURATION, concurrency $CONCURRENCY, users $USERS"
# shellcheck disable=SC2086 # CHAOS_LOAD_ARGS 故意按空格拆分
pexec sh /phase/phase.sh start run load run -addr "$GATEWAY_URL" -c "$CONCURRENCY" -duration "$DURATION" \
  -skus "$sku_list" -users "$USERS" -seed "$SEED" -reject-backoff "${CHAOS_REJECT_BACKOFF:-200ms}" \
  -run-id "chaos$E2E_RUN_ID" -ledger /work/ledger.jsonl -json /work/load-report.json ${CHAOS_LOAD_ARGS:-}
LOAD_T0=$SECONDS
DELETE_UNTIL=$((SECONDS + DUR_S))
# 提交窗口之后，虚拟用户还要把手上的订单轮询到终态（每段最多 30 秒），留足 10 分钟。
LOAD_DEADLINE=$((SECONDS + DUR_S + 600))
next_delete=$((SECONDS + INTERVAL))
load_code=""
i=0
while :; do
  tick
  if [[ "$DELETE" == 1 && $SECONDS -lt $DELETE_UNTIL && $SECONDS -ge $next_delete ]]; then
    delete_one
    next_delete=$((SECONDS + INTERVAL))
  fi
  # 每 3 轮看一次发压结束了没有（exec 比 get 贵）。
  i=$((i + 1))
  if [[ $((i % 3)) -eq 0 ]]; then
    load_code="$(phase_status run)"
    [[ -z "$load_code" ]] || break
  fi
  if [[ $SECONDS -ge $LOAD_DEADLINE ]]; then
    load_code=timeout
    break
  fi
  sleep 1
done
phase_fetch run
pexec cat /work/load-report.json >"$LOG_DIR/load-report.json" 2>/dev/null || true
pexec cat /work/ledger.jsonl >"$LOG_DIR/ledger.jsonl" 2>/dev/null || true
log "load run exited with $load_code after $((SECONDS - LOAD_T0))s; $DELETE_N pods deleted"
cat "$LOG_DIR/run.out"

# ---------- 8. 恢复、静止、对账 ----------
log "waiting for all deployments to recover"
settled=0
for _ in $(seq 1 150); do
  tick
  if deploys_settled; then
    settled=1
    break
  fi
  sleep 2
done
if [[ $settled -eq 1 ]]; then
  log "all deployments recovered"
else
  log "deployments did not recover within 5 minutes"
  kubectl -n "$NS" get deploy,pods -o wide | tee -a "$EVENTS" || true
fi

quiet_s="$(dur_seconds "$QUIET_TIMEOUT")" || die 2 "CHAOS_QUIET_TIMEOUT must look like 3m or 180s, got $QUIET_TIMEOUT"
run_phase quiet $((quiet_s + 60)) load wait-quiet -redis-addr "$GATE_REDIS" -redis-prefix "$REDIS_PREFIX" \
  -kafka-brokers "$KAFKA_BROKERS" -kafka-prefix "$KAFKA_PREFIX" -timeout "$QUIET_TIMEOUT"
quiet_code="$PHASE_CODE"

# recon 排在 verify 前面：wait-quiet 不等 cdc 和 orderindex，商家索引可能还差几条。recon 发现差异后隔
# CHAOS_RECON_RECHECK 复查，复查通过就说明索引追上了（静止之后订单不再变），之后 verify 的第 20 条才是确定的。
# 系统已经静止，不设水位线（-settle 0），同本机。
run_phase recon 600 recon -pairs "$RECON_PAIRS" -settle 0 -recheck "$RECON_RECHECK" -json /work/recon.json
recon_code="$PHASE_CODE"

# 库存服务的日志：所有跟过的 inventory Pod 的日志合成一个文件，传进测试 Pod。
cat "$INV_LOG_DIR"/*.log >"$LOG_DIR/inventory.log" 2>/dev/null || : >"$LOG_DIR/inventory.log"
kubectl -n "$NS" exec -i "$POD" -c chaos -- sh -c 'cat >/work/inventory.log' <"$LOG_DIR/inventory.log"
run_phase verify 300 load verify -redis-addr "$GATE_REDIS" -redis-prefix "$REDIS_PREFIX" \
  -kafka-brokers "$KAFKA_BROKERS" -kafka-prefix "$KAFKA_PREFIX" \
  -ledger /work/ledger.jsonl -initial "$initial" -inventory-log /work/inventory.log
verify_code="$PHASE_CODE"
tick

# ---------- 9. 结论 ----------
# 受理数为 0 时 verify 也会通过（账本是空的），那不算通过：多半是入口或网关整段不可用，测试没测到东西。
# 输出里有两行以 accepted 开头（带延迟分位数的那行和后面缩进的汇总），只取第一行。
accepted="$(awk '$1 == "accepted" {print $2; exit}' "$LOG_DIR/run.out" 2>/dev/null || true)"
{
  echo "== chaos summary (run $E2E_RUN_ID, $LOG_DIR) =="
  if [[ "$FORCE" == 1 ]]; then
    echo "pods deleted: $DELETE_N (force, grace period 0)"
  else
    echo "pods deleted: $DELETE_N (normal grace period)"
  fi
  awk '{n[$3]++} END {for (s in n) printf "  %s=%d\n", s, n[s]}' "$DELETIONS"
  echo "deletions (time, offset from load start, service, pod):"
  sed 's/^/  /' "$DELETIONS"
  if [[ -s "$RESTARTS" ]]; then
    echo "unexpected restarts: $(wc -l <"$RESTARTS" | tr -d ' ')"
    sed 's/^/  /' "$RESTARTS"
  else
    echo "unexpected restarts: 0"
  fi
  echo "inventory pods whose logs were followed:$FOLLOWED"
  grep -E '^(accepted|landed|drain) ' "$LOG_DIR/run.out" 2>/dev/null || true
  echo "deployments recovered: $settled (1 = yes)"
  echo "load exit $load_code, wait-quiet exit $quiet_code, recon exit $recon_code, verify exit $verify_code"
} | tee -a "$EVENTS"

if [[ "$load_code" == 0 && "${accepted:-0}" -gt 0 && $settled -eq 1 && "$quiet_code" == 0 &&
  "$recon_code" == 0 && "$verify_code" == 0 && ! -s "$RESTARTS" ]]; then
  log "CHAOS PASS"
  exit 0
fi
log "CHAOS FAIL"
exit 1
