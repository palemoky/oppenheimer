#!/usr/bin/env bash
# 对集群上长期运行的那一套（命名空间 oppenheimer）做压测时，发压以外的辅助动作（设计文档 3b 的 3.1、3.2、3.7）。
# 发压本身（k6）不在这里。每个子命令都可以重复执行。
#
#   loadtest.sh preflight              开跑前的检查，任何一条不满足就以非零退出并说明是哪一条：
#                                      没有不正常的 Pod、Kafka 消费延迟为 0、没有 e2e/chaos 在跑（命名空间 oppenheimer-e2e 不存在）、
#                                      k3s-server 的系统盘够用；并打印这一轮的条件（RATE_LIMIT_IP、副本数、ScaledObject 是否暂停）
#   loadtest.sh conditions [文件]      把这一轮的条件写成文本（默认标准输出）：提交号、镜像 tag、副本数、限流、采样、各库的行数（估计值）
#   loadtest.sh skus <起始号> <个数> [每个的库存] [商家号]
#                                      准备一批新的 SKU（只写 MySQL 的 inventory_db.sku_stock）。这批号里有一个已存在就整批拒绝
#   loadtest.sh pin <服务>=<副本数> ...  钉死副本数（gateway、order 用 KEDA 的暂停注解，别的服务直接 scale）；等副本数到位再返回
#   loadtest.sh unpin <服务> ...         放开（KEDA 的去掉注解；别的服务恢复成 deploy/cluster/services/<服务>.yaml 里的 replicas）
#   loadtest.sh wait-quiet             对长期运行的那一套跑 load wait-quiet（outbox 清空、消费延迟为 0、没有未完成的订单）
#   loadtest.sh recon [recon 的参数]    跑 recon，默认 -pairs payment,stock,replica,index -settle 0 -recheck 30s
#   loadtest.sh verify <SKU 列表> <orders.jsonl>
#                                      跑 load verify 的限定范围模式（-scope-skus、-orders-jsonl）
#   loadtest.sh seed-orders <load seed 的参数>
#                                      直接向两个订单主库灌历史订单（load seed，设计文档 1c 的 5.3）。可中断续跑，但必须用同一组参数
#                                      （参数指纹记在 seed_progress 里，换了参数会被拒绝），所以 -orders、-from、-to 每次都要写全
#   loadtest.sh image                  只构建并推送 chaos 镜像（load、recon），打印 tag
#   loadtest.sh empty-up               空表对照：在三个 MySQL 实例上重建 e2e_* 空库，把长期运行的那一套指过去
#                                      （叠 deploy/cluster/services/loadtest-empty/，镜像 tag 不变），Kafka、Redis 带 e2e-empty 前缀。
#                                      正式库里的数据一行不动。切之前先 wait-quiet：切走之后正式库里没关的订单没有人管
#   loadtest.sh empty-down             切回正式的库，再删掉 e2e_* 库和 e2e- 前缀的 topic、消费者组、Redis 键。同样先 wait-quiet、对完账
#
# 空表对照期间上面的子命令照常用：每个子命令都先看 order 的 Deployment 实际连的是哪套库（dataset），
# SKU 建在那一套里，wait-quiet、recon、verify 对的也是那一套。conditions 的第一段写明是哪一套。
#
# 环境变量：
#   PREFLIGHT_MIN_DISK_GB   k3s-server 系统盘剩余空间的下限，默认 15（盘 58 GB，kubelet 的驱逐线约 6 GB；2026-11-21 以前是 30）
#   EXPECT_RATE_LIMIT_IP    期望的 gateway RATE_LIMIT_IP（例如 off 或 200/400）；没给就只打印不核对
#   EXPECT_REPLICAS         期望的副本数，例如 "gateway=2,order=2"；没给就只打印不核对
#   EXPECT_PAUSED           期望处于暂停的 ScaledObject，例如 "gateway,order"；没给就只打印不核对
#   ALLOW_LOW_SKU=1         skus 允许起始号低于 7001（7001 以前是示例和探针用的，只给冒烟用）
#   RECON_FULL=1            recon 不带 -since 时必须给它：全量对账会读全部订单（库里以后有上千万单）
#   RECON_SETTLE            recon 的 -settle，默认 0（压完、系统静止之后跑；recon 自己的默认 30 分钟会把这一轮排除在外）
#   RECON_TIMEOUT           等 recon 跑完的时限（秒），默认 3600
#   QUIET_TIMEOUT           wait-quiet 的 -timeout，默认 20m（没付款的订单要等 15 分钟的支付超时才关，在那之前不算静止）
#   VERIFY_INITIAL          verify 的 -initial（"7001=1000,7002=1000"）；没给就读 skus 记下的 tmp/loadtest/sku-initial.txt
#   LOADTEST_IMAGE_TAG      chaos 镜像的 tag；没给就用 images.sh tag（工作区干净时是短提交号，不干净时每次都不同）。
#                           仓库里已经有这个 tag 就不再构建
#   SEED_TIMEOUT            等 seed-orders 跑完的时限（秒），默认 21600。超时或 MacBook 这边中断时 Pod 被删，重跑同样的命令接着灌
#   LOADTEST_OUT            输出目录，默认 tmp/loadtest/<时间>
#
# wait-quiet、recon、verify 在集群里起一个一次性的 Pod（oppenheimer 命名空间，chaos 镜像，不进网格），
# DSN 来自已有的 Secret oppenheimer-mysql，Redis、Kafka 的地址是和服务一样的固定值；密码不进命令行和文件。
# 命令在 Pod 里和 exec 的会话脱开（deploy/cluster/chaos/phase.sh），MacBook 隔几秒来看一次退出码。
# 退出码：0 通过；1 检查没通过；2 参数或环境不对。完整输出在 tmp/loadtest/<时间>/ 下。
#
# 钉死副本数是不入库的临时改动，说明见 pin 子命令的注释。
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

NS="oppenheimer"
E2E_NS="oppenheimer-e2e"
SERVICES="gateway order inventory payment orderindex cdc"
KEDA_SERVICES="gateway order"
PAUSE_ANN="autoscaling.keda.sh/paused-replicas"
PAUSE_ANN2="autoscaling.keda.sh/paused"
MIN_DISK_GB="${PREFLIGHT_MIN_DISK_GB:-15}"
SKU_INITIAL_FILE="$ROOT/tmp/loadtest/sku-initial.txt"
# 和服务一样的固定地址（deploy/cluster/services/*.yaml）。长期运行的那一套没有 REDIS_PREFIX、KAFKA_PREFIX，前缀是空的。
GATE_REDIS="sentinel://gate@redis-gate-sentinel.redis.svc:26379"
KAFKA_BROKERS="kafka-kafka-bootstrap.kafka.svc:9092"
# 空表对照期间（empty-up 之后）三个前缀不是空的。以集群上实际的配置为准，不靠环境变量：
# 免得 SKU 建在一套库里、服务读的是另一套。由 detect_dataset 填。
DB_PREFIX=""
REDIS_PREFIX=""
KAFKA_PREFIX=""

die() { # 退出码 消息
  local code="$1"
  shift
  echo "$*" >&2
  exit "$code"
}

is_uint() { case "${1:-}" in '' | *[!0-9]*) return 1 ;; esac; }

in_list() { # 词 列表
  case " $2 " in
  *" $1 "*) return 0 ;;
  esac
  return 1
}

# ---------- Prometheus ----------
# 一条即时查询，输出所有结果的“标签 值”，每行一条（标签是 instance 或 {}）。查询失败或没有结果时输出为空。
prom_values() { # 查询
  "$HERE/monitoring.sh" promql "$1" 2>/dev/null | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["data"]["result"]
except Exception:
    sys.exit(0)
for s in r:
    print(s["metric"].get("instance", "{}"), s["value"][1])'
}

server_ip() {
  local spec
  for spec in "${NODES[@]}"; do
    if [[ "${spec%%:*}" == "$K3S_SERVER_NODE" ]]; then
      echo "$spec" | cut -d: -f2
      return
    fi
  done
  return 1
}

# ---------- 配置读取 ----------
# 某个服务 ConfigMap 里的一个键，没设输出空。chart 把 values 的 config 渲染成同名 ConfigMap，Pod 用 envFrom 注入
#（deploy/charts/service/templates/configmap.yaml）。
cm_value() { # 服务 键
  local v
  v="$(kubectl -n "$NS" get configmap "$1" -o go-template="{{index .data \"$2\"}}" 2>/dev/null)" || v=""
  if [[ "$v" == "<no value>" ]]; then v=""; fi
  echo "$v"
}

deploy_replicas() { # 服务：spec 副本数
  kubectl -n "$NS" get deployment "$1" -o jsonpath='{.spec.replicas}'
}

deploy_ready() { # 服务：就绪副本数
  local n
  n="$(kubectl -n "$NS" get deployment "$1" -o jsonpath='{.status.readyReplicas}')"
  echo "${n:-0}"
}

# ScaledObject 的暂停情况：没有这个对象输出 none；有就输出 paused-replicas 注解的值，或 paused，或 no。
so_state() { # 服务
  local out rep p
  if ! kubectl -n "$NS" get scaledobject "$1" >/dev/null 2>&1; then
    echo none
    return
  fi
  out="$(kubectl -n "$NS" get scaledobject "$1" -o go-template="{{with .metadata.annotations}}{{index . \"$PAUSE_ANN\"}}|{{index . \"$PAUSE_ANN2\"}}{{end}}")"
  rep="${out%%|*}"
  p="${out##*|}"
  rep="${rep//<no value>/}"
  p="${p//<no value>/}"
  if [[ -n "$rep" ]]; then
    echo "paused-replicas=$rep"
  elif [[ "$p" == true ]]; then
    echo paused
  else
    echo no
  fi
}

image_tag_of() { # 服务
  kubectl -n "$NS" get deployment "$1" -o jsonpath="{.spec.template.spec.containers[?(@.name==\"$1\")].image}" | sed 's/.*://'
}

# 现在连的是哪套库。库名的前缀看 order 的 Deployment 里的 DSN，Kafka、Redis 的前缀看它的 ConfigMap；
# 两边对不上（一个有前缀一个没有）说明切到一半，拒绝继续。
detect_dataset() {
  DB_PREFIX=""
  if e2e_longrunning_on_e2e_dbs; then DB_PREFIX="e2e_"; fi
  REDIS_PREFIX="$(cm_value order REDIS_PREFIX)"
  KAFKA_PREFIX="$(cm_value order KAFKA_PREFIX)"
  if [[ -n "$DB_PREFIX" && ( -z "$REDIS_PREFIX" || -z "$KAFKA_PREFIX" ) ]] ||
    [[ -z "$DB_PREFIX" && -n "$REDIS_PREFIX$KAFKA_PREFIX" ]]; then
    die 2 "order is half switched (db prefix '$DB_PREFIX', REDIS_PREFIX '$REDIS_PREFIX', KAFKA_PREFIX '$KAFKA_PREFIX'); finish with empty-up or empty-down"
  fi
}

dataset_name() {
  if [[ -n "$DB_PREFIX" ]]; then
    echo "EMPTY (databases ${DB_PREFIX}*, kafka prefix $KAFKA_PREFIX, redis prefix $REDIS_PREFIX)"
  else
    echo "production (10M seeded orders plus earlier load test orders)"
  fi
}

# ---------- preflight ----------
FAILS=0
fail() {
  echo "    FAIL: $*" >&2
  FAILS=$((FAILS + 1))
}

cmd_preflight() {
  local bad ip gb line lag exp rl have svc want n state
  echo "==> 1. pods in all namespaces"
  # NAMESPACE NAME READY STATUS ...：不是 Running/Completed，或者 Running 但容器没全部就绪
  bad="$(kubectl get pods -A --no-headers 2>/dev/null | awk '
    { split($3, r, "/") }
    $4 == "Completed" { next }
    $4 != "Running" || r[1] != r[2] { print "    " $1 "/" $2 " " $3 " " $4 }')" || bad="cannot list pods"
  if [[ -n "$bad" ]]; then
    echo "$bad" >&2
    fail "there are pods that are not Running/Completed or not fully ready"
  else
    echo "    ok"
  fi

  echo "==> 2. kafka consumer lag (all groups except order.order-close-delay)"
  # order.order-close-delay 的延迟是延迟关单的机制本身（消息到期前不提交位点，ADR-0009）：每个还没付款的订单在那里有 15 分钟，
  # 它不是积压，也不会在压测前清零（上一轮留下的未付款订单要 15 分钟后才关掉）。所以排除它，单独打印出来。
  lag="$(prom_values 'sum(kafka_consumergroup_lag{consumergroup!~"(.+[.])?order[.]order-close-delay"})' | awk '{print $2}')"
  if [[ -z "$lag" ]]; then
    fail "no data for kafka_consumergroup_lag in Prometheus (cannot confirm the lag is 0)"
  elif [[ "${lag%%.*}" != 0 ]]; then
    fail "kafka consumer lag is $lag, want 0"
  else
    echo "    lag 0 (order.order-close-delay, the close timer of unpaid orders, has $(prom_values 'sum(kafka_consumergroup_lag{consumergroup=~"(.+[.])?order[.]order-close-delay"})' | awk '{print $2}') pending; not counted)"
  fi

  echo "==> 3. no other load test, e2e or chaos running"
  if kubectl get namespace "$E2E_NS" >/dev/null 2>&1; then
    fail "namespace $E2E_NS exists (state: $(e2e_ns_state "$E2E_NS"))"
  else
    echo "    namespace $E2E_NS does not exist"
  fi

  echo "==> 4. $K3S_SERVER_NODE root disk (want >= ${MIN_DISK_GB} GB free)"
  ip="$(server_ip)" || die 2 "$K3S_SERVER_NODE not found in NODES"
  line="$(prom_values "node_filesystem_avail_bytes{mountpoint=\"/\",instance=~\"$ip:.*\"}" | head -n 1)"
  if [[ -z "$line" ]]; then
    fail "no node_filesystem_avail_bytes{mountpoint=\"/\"} for $ip in Prometheus"
  else
    gb="$(awk -v b="${line##* }" 'BEGIN { printf "%d", b / 1073741824 }')"
    if [[ "$gb" -lt "$MIN_DISK_GB" ]]; then
      fail "$K3S_SERVER_NODE has $gb GB free on /, want at least $MIN_DISK_GB"
    else
      echo "    $gb GB free"
    fi
  fi

  echo "==> 5. conditions of this round"
  echo "    dataset: $(dataset_name)"
  rl="$(cm_value gateway RATE_LIMIT_IP)"
  have="${rl:-<unset, default 200/400>}"
  echo "    gateway RATE_LIMIT_IP (configmap $NS/gateway): $have; RATE_LIMIT: $(cm_value gateway RATE_LIMIT)"
  exp="${EXPECT_RATE_LIMIT_IP:-}"
  if [[ -n "$exp" && "$rl" != "$exp" ]]; then
    fail "RATE_LIMIT_IP is '${rl:-<unset>}', expected '$exp'"
  fi
  # ConfigMap 改了但 Deployment 没滚动时，Pod 里还是旧值：比对 Deployment 的校验和注解和各 Pod 的创建时间太绕，
  # 这里只核对 ConfigMap；改了 values 之后要 make deploy 并等滚动完（预检的第 1 条会挡住滚动中的 Pod）。
  echo "    replicas (ready/spec) and ScaledObject:"
  for svc in $SERVICES; do
    state="$(so_state "$svc")"
    echo "      $svc: $(deploy_ready "$svc")/$(deploy_replicas "$svc")  scaledobject: $state"
  done
  if [[ -n "${EXPECT_REPLICAS:-}" ]]; then
    for n in ${EXPECT_REPLICAS//,/ }; do
      svc="${n%%=*}"
      want="${n##*=}"
      if ! in_list "$svc" "$SERVICES" || ! is_uint "$want"; then
        die 2 "EXPECT_REPLICAS: bad item '$n' (want <service>=<count>)"
      fi
      if [[ "$(deploy_replicas "$svc")" != "$want" || "$(deploy_ready "$svc")" != "$want" ]]; then
        fail "$svc has $(deploy_ready "$svc")/$(deploy_replicas "$svc") ready/spec replicas, expected $want"
      fi
    done
  fi
  if [[ -n "${EXPECT_PAUSED:-}" ]]; then
    for svc in ${EXPECT_PAUSED//,/ }; do
      state="$(so_state "$svc")"
      case "$state" in
      none | no) fail "scaledobject $svc is not paused (state: $state), expected paused" ;;
      esac
    done
  fi
  # 两个 ScaledObject 没有暂停、又给了期望的副本数时，KEDA 还会伸缩，核对的数一会儿就可能变：提醒一句。
  if [[ -n "${EXPECT_REPLICAS:-}" ]]; then
    for svc in $KEDA_SERVICES; do
      case "$(so_state "$svc")" in
      paused*) ;;
      *) in_list "$svc" "$(echo "${EXPECT_REPLICAS}" | tr ',=' '  ')" &&
        echo "    NOTE: $svc is not paused; KEDA may change its replicas during the run (use pin)" ;;
      esac
    done
  fi

  if [[ $FAILS -gt 0 ]]; then
    echo "==> preflight FAILED ($FAILS)" >&2
    return 1
  fi
  echo "==> preflight passed"
}

# ---------- conditions ----------
cmd_conditions() { # [文件]
  local out="${1:-}"
  if [[ -n "$out" ]]; then
    mkdir -p "$(dirname "$out")"
    cmd_conditions_body >"$out"
    echo "conditions written to $out" >&2
  else
    cmd_conditions_body
  fi
}

cmd_conditions_body() {
  local svc k v pod i dirty
  dirty=""
  if [[ -n "$(git -C "$ROOT" status --porcelain)" ]]; then dirty=" (working tree dirty)"; fi
  echo "time: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git commit: $(git -C "$ROOT" rev-parse HEAD)$dirty"
  echo "dataset: $(dataset_name)"
  echo
  echo "services (image tag, ready/spec replicas, scaledobject):"
  for svc in $SERVICES; do
    echo "  $svc: tag $(image_tag_of "$svc"), replicas $(deploy_ready "$svc")/$(deploy_replicas "$svc"), scaledobject $(so_state "$svc")"
  done
  echo
  echo "rate limit (gateway configmap; unset means the default in services/gateway/cmd/gateway/ratelimit.go: RATE_LIMIT on, RATE_LIMIT_IP 200/400):"
  for k in RATE_LIMIT RATE_LIMIT_IP RATE_LIMIT_USER RATE_LIMIT_ROUTE RATE_LIMIT_HOT RATE_LIMIT_HOT_ITEMS; do
    v="$(cm_value gateway "$k")"
    echo "  $k: ${v:-<unset>}"
  done
  echo
  echo "sampling (service configmaps; unset means the default parentbased_always_on):"
  for svc in $SERVICES; do
    echo "  $svc: OTEL_TRACES_SAMPLER=$(cm_value "$svc" OTEL_TRACES_SAMPLER) OTEL_TRACES_SAMPLER_ARG=$(cm_value "$svc" OTEL_TRACES_SAMPLER_ARG)  (empty = unset)"
  done
  echo
  echo "table rows (ESTIMATES from information_schema.TABLES.TABLE_ROWS, not COUNT(*)):"
  for i in 0 1; do
    pod="$(e2e_mysql_pod "mysql-order-$i" primary)"
    table_rows "$pod" "${DB_PREFIX}order_db_$i" "mysql-order-$i"
  done
  pod="$(e2e_mysql_pod mysql-main primary)"
  table_rows "$pod" "${DB_PREFIX}payment_db" "mysql-main"
  table_rows "$pod" "${DB_PREFIX}inventory_db" "mysql-main"
}

table_rows() { # pod 库 实例名
  echo "  $3 / $2:"
  # stats_expiry=0：不用缓存了一天的统计信息。TABLE_ROWS 对 InnoDB 仍然只是估计值。
  printf '%s\n' "SET SESSION information_schema_stats_expiry = 0;
SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$2' AND TABLE_TYPE = 'BASE TABLE'
  AND TABLE_NAME NOT LIKE 'goose%' ORDER BY TABLE_NAME;" |
    e2e_root_sql "$1" -N -B | awk -F'\t' '{printf "    %-32s ~%s\n", $1, $2}'
}

# ---------- skus ----------
cmd_skus() { # 起始号 个数 [库存] [商家号]
  local start="${1:-}" count="${2:-}" stock="${3:-1000000000}" merchant="${4:-7}"
  if ! { is_uint "$start" && is_uint "$count" && is_uint "$stock" && is_uint "$merchant"; }; then
    die 2 "usage: loadtest.sh skus <start sku> <count> [stock each] [merchant id]  (all non-negative integers)"
  fi
  [[ "$count" -ge 1 && "$count" -le 10000 ]] || die 2 "count must be between 1 and 10000"
  [[ "$stock" -ge 1 ]] || die 2 "stock must be positive"
  if [[ "$start" -lt 7001 && "${ALLOW_LOW_SKU:-}" != 1 ]]; then
    die 2 "start sku $start is below 7001 (examples and probes live there); set ALLOW_LOW_SKU=1 for smoke tests"
  fi
  local end=$((start + count - 1)) pod existing sku values=""
  pod="$(e2e_mysql_pod mysql-main primary)"
  existing="$(printf '%s\n' "SELECT sku_id FROM ${DB_PREFIX}inventory_db.sku_stock WHERE sku_id BETWEEN $start AND $end ORDER BY sku_id;" |
    e2e_root_sql "$pod" -N -B | tr '\n' ' ')"
  if [[ -n "${existing// /}" ]]; then
    echo "refusing the whole batch $start..$end: these skus already exist (every round must use new skus): $existing" >&2
    return 1
  fi
  for ((sku = start; sku <= end; sku++)); do
    values="$values${values:+,}($sku, $merchant, 'loadtest $sku', 1990, $stock)"
  done
  # 不用 ON DUPLICATE KEY UPDATE：上面查过之后、插入之前有人抢先建了同一个号时，整条 INSERT 失败，什么都不写。
  printf '%s\n' "INSERT INTO ${DB_PREFIX}inventory_db.sku_stock (sku_id, merchant_id, name, unit_price, available) VALUES $values;
SELECT sku_id, merchant_id, name, unit_price, available, reserved, sold FROM ${DB_PREFIX}inventory_db.sku_stock WHERE sku_id BETWEEN $start AND $end ORDER BY sku_id;" |
    e2e_root_sql "$pod" -t
  # verify 要每个 SKU 的初始库存（可售加预占加已售），卖了之后库里已经没有这个数，所以建的时候记下来。
  mkdir -p "$(dirname "$SKU_INITIAL_FILE")"
  for ((sku = start; sku <= end; sku++)); do
    echo "$sku=$stock" >>"$SKU_INITIAL_FILE"
  done
  echo "initial stock recorded in $SKU_INITIAL_FILE (used by verify); dataset: $(dataset_name)"
}

# ---------- pin / unpin ----------
# 钉死副本数。gateway、order 由 KEDA 的 ScaledObject 管：用注解 autoscaling.keda.sh/paused-replicas: "<N>"，
# KEDA 2.21 的文档（https://keda.sh/docs/2.21/concepts/scaling-deployments/ 的 Pausing autoscaling 一节）写的是
# “scale your current workload to specified amount of replicas and pause autoscaling”：先把目标扩缩到 N 再暂停伸缩。
# （autoscaling.keda.sh/paused 只是“冻结在当前副本数”，不能指定数。）
# 这是不入库的临时改动。下一次 make deploy（helm upgrade）时：
#   - 注解不在 chart 的模板里，helm 的三方合并不会去掉别人加上的注解，暂停应当保留；
#   - 但 Deployment 模板写了 replicas: {{ .Values.replicas }}，helm 会把 spec.replicas 改回 values 里的数
#     （gateway、order 是 2），和钉死的数不一样时，会和 KEDA 的暂停数打架。
# 所以钉死之后不要 deploy；deploy 之后重新 pin 一次，并用 preflight 的 EXPECT_REPLICAS 核对。
# 别的服务是普通的 Deployment，scale 之后下一次 deploy 一定改回 values 里的数。
cmd_pin() { # 服务=副本数...
  local item svc n
  [[ $# -ge 1 ]] || die 2 "usage: loadtest.sh pin <service>=<replicas> ..."
  for item in "$@"; do
    svc="${item%%=*}"
    n="${item##*=}"
    in_list "$svc" "$SERVICES" || die 2 "unknown service $svc (valid: $SERVICES)"
    is_uint "$n" && [[ "$n" -ge 1 ]] || die 2 "bad replica count in '$item'"
  done
  for item in "$@"; do
    svc="${item%%=*}"
    n="${item##*=}"
    if in_list "$svc" "$KEDA_SERVICES"; then
      kubectl -n "$NS" annotate scaledobject "$svc" "$PAUSE_ANN=$n" --overwrite >/dev/null
      echo "    scaledobject $svc: $PAUSE_ANN=$n"
    else
      kubectl -n "$NS" scale deployment "$svc" --replicas="$n" >/dev/null
      echo "    deployment $svc: scaled to $n"
    fi
  done
  for item in "$@"; do
    wait_replicas "${item%%=*}" "${item##*=}"
  done
}

wait_replicas() { # 服务 数量
  local deadline=$((SECONDS + 300)) st
  while [[ $SECONDS -lt $deadline ]]; do
    st="$(kubectl -n "$NS" get deployment "$1" -o jsonpath='{.spec.replicas}/{.status.replicas}/{.status.readyReplicas}/{.status.updatedReplicas}')"
    # spec/status/ready/updated 都等于目标，没有多出来的（正在终止的）副本
    if [[ "$st" == "$2/$2/$2/$2" ]]; then
      echo "    $1: $2 replicas ready"
      return 0
    fi
    sleep 3
  done
  die 1 "$1 did not reach $2 ready replicas in 5 minutes (spec/status/ready/updated: $st)"
}

cmd_unpin() { # 服务...
  local svc want
  [[ $# -ge 1 ]] || die 2 "usage: loadtest.sh unpin <service> ..."
  for svc in "$@"; do
    in_list "$svc" "$SERVICES" || die 2 "unknown service $svc (valid: $SERVICES)"
  done
  for svc in "$@"; do
    if in_list "$svc" "$KEDA_SERVICES"; then
      kubectl -n "$NS" annotate scaledobject "$svc" "$PAUSE_ANN-" "$PAUSE_ANN2-" >/dev/null
      echo "    scaledobject $svc: pause annotations removed"
    else
      want="$(awk '/^replicas:/ {print $2; exit}' "$ROOT/deploy/cluster/services/$svc.yaml")"
      is_uint "$want" || die 2 "cannot read replicas from deploy/cluster/services/$svc.yaml"
      kubectl -n "$NS" scale deployment "$svc" --replicas="$want" >/dev/null
      echo "    deployment $svc: scaled back to $want (values)"
      wait_replicas "$svc" "$want"
    fi
  done
}

# ---------- 镜像和一次性 Pod ----------
OUT=""
POD=""
IMAGE=""

ensure_out() {
  if [[ -z "$OUT" ]]; then
    OUT="${LOADTEST_OUT:-$ROOT/tmp/loadtest/$(date +%Y%m%d-%H%M%S)}"
    mkdir -p "$OUT"
  fi
}

chaos_tag() {
  if [[ -n "${LOADTEST_IMAGE_TAG:-}" ]]; then
    echo "$LOADTEST_IMAGE_TAG"
  else
    "$HERE/images.sh" tag
  fi
}

# chaos 镜像：仓库里没有这个 tag 才构建并推送。工作区不干净时 tag 带时间，每次都会重建。
ensure_image() {
  local tag
  tag="$(chaos_tag)"
  if curl -fsI -m 10 -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json' \
    "http://$REGISTRY_ADDR/v2/oppenheimer/chaos/manifests/$tag" >/dev/null 2>&1; then
    echo "==> chaos image $tag already in the registry" >&2
  else
    echo "==> building chaos image $tag" >&2
    IMAGE_TAG="$tag" "$HERE/images.sh" all chaos >&2
  fi
  IMAGE="$REGISTRY_ADDR/oppenheimer/chaos:$tag"
}

cmd_image() {
  ensure_image
  echo "$IMAGE"
}

# 一次性 Pod：sleep 着等 exec。DSN 在环境变量里（来自 Secret oppenheimer-mysql 的 app 账号），库名跟着 detect_dataset：
# 平时是正式的库，空表对照期间是 e2e_*。
# 不进网格：这些命令只连 MySQL、Redis、Kafka（网格排除这些端口），不调服务。
start_pod() {
  POD="loadtest-$(date +%s)"
  # 下面是写给 Kubernetes 的 $(VAR) 引用（Pod 里展开），不是 shell 的命令替换
  # shellcheck disable=SC2016
  local m0='$(MYSQL_ORDER_0_PASSWORD)' m1='$(MYSQL_ORDER_1_PASSWORD)' mm='$(MYSQL_MAIN_PASSWORD)'
  local h0="mysql-order-0-primary.mysql.svc:3306" h1="mysql-order-1-primary.mysql.svc:3306" hm="mysql-main-primary.mysql.svc:3306"
  local r0="mysql-order-0-replica.mysql.svc:3306" r1="mysql-order-1-replica.mysql.svc:3306"
  local p="$DB_PREFIX"
  local o0="app:$m0@tcp($h0)/${p}order_db_0" o1="app:$m1@tcp($h1)/${p}order_db_1"
  local mi0="app:$mm@tcp($hm)/${p}merchant_index_db_0" mi1="app:$mm@tcp($hm)/${p}merchant_index_db_1"
  kubectl -n "$NS" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  labels:
    app.kubernetes.io/name: loadtest-tool
    app.kubernetes.io/part-of: oppenheimer
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  terminationGracePeriodSeconds: 1
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: node-role.kubernetes.io/control-plane
                operator: DoesNotExist
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: tool
      image: "$IMAGE"
      imagePullPolicy: IfNotPresent
      command: ["/busybox/sh", "-c", "exec sleep 14400"]
      env:
        - {name: MYSQL_ORDER_0_PASSWORD, valueFrom: {secretKeyRef: {name: oppenheimer-mysql, key: order-0-app-password}}}
        - {name: MYSQL_ORDER_1_PASSWORD, valueFrom: {secretKeyRef: {name: oppenheimer-mysql, key: order-1-app-password}}}
        - {name: MYSQL_MAIN_PASSWORD, valueFrom: {secretKeyRef: {name: oppenheimer-mysql, key: main-app-password}}}
        # load wait-quiet、load verify（test/load/verify_cmd.go 的 dsnEnv）
        - {name: LOAD_INVENTORY_DSN, value: "app:$mm@tcp($hm)/${p}inventory_db"}
        - {name: LOAD_ORDER_DSNS, value: "$o0,$o1"}
        - {name: LOAD_PAYMENT_DSN, value: "app:$mm@tcp($hm)/${p}payment_db"}
        - {name: LOAD_MERCHANT_INDEX_DSNS, value: "$mi0,$mi1"}
        # recon
        - {name: MYSQL_DSNS, value: "$o0,$o1"}
        - {name: MYSQL_REPLICA_DSNS, value: "app:$m0@tcp($r0)/${p}order_db_0,app:$m1@tcp($r1)/${p}order_db_1"}
        - {name: ARCHIVE_DSN, value: "app:$mm@tcp($hm)/${p}order_archive_db"}
        - {name: MERCHANT_INDEX_DSNS, value: "$mi0,$mi1"}
        - {name: PAYMENT_DSN, value: "app:$mm@tcp($hm)/${p}payment_db"}
        - {name: INVENTORY_DSN, value: "app:$mm@tcp($hm)/${p}inventory_db"}
      resources:
        requests: {cpu: 200m, memory: 128Mi}
        limits: {memory: 1Gi}
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: [ALL]
      volumeMounts:
        - {name: work, mountPath: /work}
  volumes:
    - name: work
      emptyDir: {}
EOF
  echo "    pod $NS/$POD created"
  kubectl -n "$NS" wait --for=condition=Ready "pod/$POD" --timeout=180s >/dev/null ||
    die 1 "pod $POD not ready in 3 minutes (kubectl -n $NS describe pod $POD)"
  # 分阶段的启动脚本复用 chaos 的 phase.sh
  put_file "$ROOT/deploy/cluster/chaos/phase.sh" /work/phase.sh
}

# 经 exec 的标准输入把本机的文件送进 Pod（不依赖镜像里有 tar，kubectl cp 要它），并核对大小：
# kubectl exec -i 偶尔在输入还没送完时就结束，留下一个空文件或半个文件（冒烟时遇到过），所以送完要量，不对就重送。
put_file() { # 本机文件 Pod 里的路径
  # 小文件直接经 exec 的标准输入送；几 MB 的订单号清单这样送会在几十到几百 KB 处断掉
  # （"Copying stdin failed ... broken pipe"，2026-11-18 实测，原因没有查），所以先试 kubectl cp（走 tar），
  # 不行再切成 48 KB 的小块逐块追加。每种办法送完都核对字节数。
  local want got try dir chunk
  want="$(wc -c <"$1" | tr -d ' ')"
  size_ok() {
    got="$(pexec wc -c "$1" 2>/dev/null | awk '{print $1}')" || got=""
    [[ "$got" == "$want" ]]
  }
  if [[ "$want" -le 32768 ]]; then
    for try in 1 2 3; do
      # shellcheck disable=SC2016 # $0 在 Pod 里展开
      kubectl -n "$NS" exec -i "$POD" -c tool -- sh -c 'cat >"$0"' "$2" <"$1" || true
      if size_ok "$2"; then return 0; fi
    done
  fi
  kubectl -n "$NS" cp -c tool "$1" "$POD:$2" >/dev/null 2>&1 || true
  if size_ok "$2"; then return 0; fi
  echo "    kubectl cp of $1 incomplete ($got of $want bytes), sending in chunks" >&2
  dir="$(mktemp -d)"
  split -b 48k "$1" "$dir/part."
  # shellcheck disable=SC2016
  pexec sh -c ': >"$0"' "$2"
  for chunk in "$dir"/part.*; do
    for try in 1 2 3 4 5; do
      before="$(pexec wc -c "$2" | awk '{print $1}')"
      # shellcheck disable=SC2016
      kubectl -n "$NS" exec -i "$POD" -c tool -- sh -c 'cat >"$0.chunk"' "$2" <"$chunk" 2>/dev/null || true
      if [[ "$(pexec wc -c "$2.chunk" 2>/dev/null | awk '{print $1}')" == "$(wc -c <"$chunk" | tr -d ' ')" ]]; then
        # shellcheck disable=SC2016
        pexec sh -c 'cat "$0.chunk" >>"$0" && rm -f "$0.chunk"' "$2"
        break
      fi
      [[ $try -lt 5 ]] || { rm -rf "$dir"; die 1 "could not upload $1 to the pod (chunk $(basename "$chunk"), file at $before bytes)"; }
    done
  done
  rm -rf "$dir"
  size_ok "$2" || die 1 "could not upload $1 to the pod ($got of $want bytes after chunked upload)"
}

# shellcheck disable=SC2329 # EXIT 的 trap 调用
stop_pod() {
  if [[ -n "$POD" ]]; then
    kubectl -n "$NS" delete pod "$POD" --wait=false >/dev/null 2>&1 || true
  fi
}

pexec() { kubectl -n "$NS" exec "$POD" -c tool -- "$@"; }

# 在 Pod 里跑一个命令并等它结束，输出存进 $OUT/<名字>.out/.err 并打到终端；退出码放在 PHASE_CODE。
PHASE_CODE=""
run_phase() { # 名字 时限秒数 命令...
  local name="$1" limit="$2" deadline last
  shift 2
  pexec sh /work/phase.sh start "$name" "$@"
  # 启动失败（脚本没起来）时不会有输出文件，别傻等到时限
  sleep 1
  pexec test -e "/work/$name.out" || die 1 "phase $name did not start in the pod"
  deadline=$((SECONDS + limit))
  last=$SECONDS
  PHASE_CODE=""
  while [[ -z "$PHASE_CODE" ]]; do
    if [[ $SECONDS -ge $deadline ]]; then
      PHASE_CODE=timeout
      break
    fi
    sleep 3
    PHASE_CODE="$(pexec sh /work/phase.sh status "$name" 2>/dev/null || true)"
    if [[ -z "$PHASE_CODE" && $((SECONDS - last)) -ge 30 ]]; then
      echo "    ... $name still running ($((SECONDS - deadline + limit))s)" >&2
      last=$SECONDS
    fi
  done
  pexec cat "/work/$name.out" >"$OUT/$name.out" 2>/dev/null || true
  pexec cat "/work/$name.err" >"$OUT/$name.err" 2>/dev/null || true
  cat "$OUT/$name.out"
  # 标准错误里是日志（JSON，可能很长）：终端上只给最后 20 行，完整的在文件里
  if [[ -s "$OUT/$name.err" ]]; then
    echo "--- stderr (last 20 lines; full: $OUT/$name.err)"
    tail -n 20 "$OUT/$name.err"
  fi
  echo "==> $name exited with $PHASE_CODE (output in $OUT)"
}

with_pod() { # 之后 trap 清理
  ensure_out
  ensure_image
  trap stop_pod EXIT
  trap 'exit 130' INT TERM
  start_pod
}

phase_result() { # PHASE_CODE → 退出码
  [[ "$PHASE_CODE" == 0 ]]
}

cmd_wait_quiet() {
  with_pod
  # 默认 20 分钟：没付款的订单要等 15 分钟的支付超时才会被关掉（order 的 DefaultPaymentTimeout），在那之前不算静止。
  local qt="${QUIET_TIMEOUT:-20m}" qs
  case "$qt" in
  *m) qs=$((${qt%m} * 60)) ;;
  *s) qs="${qt%s}" ;;
  *) die 2 "QUIET_TIMEOUT must look like 20m or 1200s, got $qt" ;;
  esac
  is_uint "$qs" || die 2 "QUIET_TIMEOUT must look like 20m or 1200s, got $qt"
  run_phase quiet $((qs + 120)) load wait-quiet -redis-addr "$GATE_REDIS" -redis-prefix "$REDIS_PREFIX" \
    -kafka-brokers "$KAFKA_BROKERS" -kafka-prefix "$KAFKA_PREFIX" -timeout "$qt"
  phase_result
}

cmd_recon() { # [recon 的参数]
  local a has_since=0 settle="${RECON_SETTLE:-0}" has_pairs=0 has_settle=0 has_recheck=0
  for a in "$@"; do
    case "$a" in
    -since | --since | -since=* | --since=*) has_since=1 ;;
    -pairs | --pairs | -pairs=* | --pairs=*) has_pairs=1 ;;
    -settle | --settle | -settle=* | --settle=*) has_settle=1 ;;
    -recheck | --recheck | -recheck=* | --recheck=*) has_recheck=1 ;;
    esac
  done
  if [[ $has_since -eq 0 && "${RECON_FULL:-}" != 1 ]]; then
    die 2 "recon without -since is a full run that reads every order (the table will hold 10M+ rows); pass -since <duration, e.g. 30m> or set RECON_FULL=1"
  fi
  local args=()
  [[ $has_pairs -eq 1 ]] || args+=(-pairs 'payment,stock,replica,index')
  [[ $has_settle -eq 1 ]] || args+=(-settle "$settle")
  [[ $has_recheck -eq 1 ]] || args+=(-recheck 30s)
  with_pod
  # shellcheck disable=SC2068 # args 可能为空；bash 3.2 下空数组加 set -u 要这样写
  run_phase recon "${RECON_TIMEOUT:-3600}" recon ${args[@]+"${args[@]}"} "$@" -json /work/recon.json
  pexec cat /work/recon.json >"$OUT/recon.json" 2>/dev/null || true
  phase_result
}

# 某个 SKU 列表对应的 -initial：先看 VERIFY_INITIAL，再读 skus 记下的文件（同一个号记过几次时取最后一次）。
initial_for() { # SKU 列表（逗号分隔）
  local sku v out=""
  if [[ -n "${VERIFY_INITIAL:-}" ]]; then
    echo "$VERIFY_INITIAL"
    return
  fi
  for sku in ${1//,/ }; do
    v=""
    if [[ -f "$SKU_INITIAL_FILE" ]]; then
      v="$(awk -F= -v s="$sku" '$1 == s {v = $2} END {print v}' "$SKU_INITIAL_FILE")"
    fi
    [[ -n "$v" ]] || die 2 "no initial stock known for sku $sku (not in $SKU_INITIAL_FILE); set VERIFY_INITIAL=$sku=<stock>,..."
    out="$out${out:+,}$sku=$v"
  done
  echo "$out"
}

cmd_verify() { # SKU 列表 orders.jsonl
  local skus="${1:-}" orders="${2:-}" initial s
  [[ -n "$skus" && -n "$orders" ]] || die 2 "usage: loadtest.sh verify <sku list, comma separated> <orders.jsonl>"
  [[ -f "$orders" ]] || die 2 "$orders not found"
  for s in ${skus//,/ }; do is_uint "$s" || die 2 "bad sku '$s'"; done
  initial="$(initial_for "$skus")"
  with_pod
  echo "==> sending $(wc -l <"$orders" | tr -d ' ') order lines ($(wc -c <"$orders" | tr -d ' ') bytes) to the pod"
  # 经 exec 的标准输入送进去：不依赖镜像里有 tar（kubectl cp 要它）
  put_file "$orders" /work/orders.jsonl
  run_phase verify 1800 load verify -redis-addr "$GATE_REDIS" -redis-prefix "$REDIS_PREFIX" \
    -kafka-brokers "$KAFKA_BROKERS" -kafka-prefix "$KAFKA_PREFIX" \
    -scope-skus "$skus" -orders-jsonl /work/orders.jsonl -initial "$initial"
  phase_result
}

# 灌历史订单。订单主库的 DSN 在 Pod 的环境变量里（带密码），所以经 sh 在 Pod 里展开，不出现在命令行上。
# 写入会经过 binlog 流到从库和 CDC，再进商家索引：灌完要等从库和索引的消费延迟追平（preflight 会看）。
cmd_seed_orders() {
  [[ $# -ge 1 ]] || die 2 "usage: loadtest.sh seed-orders -orders N -from YYYY-MM-DD -to YYYY-MM-DD [more load seed flags]"
  with_pod
  # shellcheck disable=SC2016 # 要的就是在 Pod 里展开
  run_phase seed "${SEED_TIMEOUT:-21600}" sh -c 'exec load seed -order-dsns "$LOAD_ORDER_DSNS" "$@"' seed "$@"
  phase_result
}

# ---------- 空表对照：切到空库、切回来 ----------
# 六个服务用同一个 tag、LOADTEST 那一层按现在的样子保留：切换只改“数据在哪”，别的都不动。
redeploy() { # LOADTEST_EMPTY 的值
  local tag svc t lt=0
  tag="$(image_tag_of order)"
  for svc in $SERVICES; do
    t="$(image_tag_of "$svc")"
    [[ "$t" == "$tag" ]] || die 2 "services run different image tags ($svc: $t, order: $tag); deploy one tag first"
  done
  if [[ "$(cm_value gateway RATE_LIMIT_IP)" == off ]]; then lt=1; fi
  IMAGE_TAG="$tag" LOADTEST="$lt" LOADTEST_EMPTY="$1" "$HERE/services.sh" deploy
}

cmd_empty_up() {
  [[ -z "$DB_PREFIX" ]] || die 2 "the long-running stack is already on the e2e_* databases"
  if kubectl get namespace "$E2E_NS" >/dev/null 2>&1; then
    die 2 "namespace $E2E_NS exists (state: $(e2e_ns_state "$E2E_NS")); it uses the same e2e_* databases"
  fi
  echo "==> empty databases"
  e2e_create_dbs
  e2e_wait_replicated
  redeploy 1
  detect_dataset
  echo "==> dataset: $(dataset_name)"
}

cmd_empty_down() {
  [[ -n "$DB_PREFIX" ]] || echo "    the long-running stack is not on the e2e_* databases; only cleaning up"
  redeploy 0
  detect_dataset
  [[ -z "$DB_PREFIX" ]] || die 1 "order still points at the e2e_* databases after the deploy"
  echo "==> mysql"
  e2e_drop_dbs
  echo "==> kafka"
  e2e_clean_kafka
  echo "==> redis"
  e2e_clean_redis
  echo "==> dataset: $(dataset_name)"
}

# ---------- 入口 ----------
cmd="${1:-}"
shift || true
case "$cmd" in
image | pin | unpin | "") ;;
*) detect_dataset ;;
esac
case "$cmd" in
preflight) cmd_preflight ;;
conditions) cmd_conditions "$@" ;;
skus) cmd_skus "$@" ;;
pin) cmd_pin "$@" ;;
unpin) cmd_unpin "$@" ;;
wait-quiet) cmd_wait_quiet ;;
recon) cmd_recon "$@" ;;
verify) cmd_verify "$@" ;;
seed-orders) cmd_seed_orders "$@" ;;
image) cmd_image ;;
empty-up) cmd_empty_up ;;
empty-down) cmd_empty_down ;;
*)
  sed -n '2,45p' "${BASH_SOURCE[0]}" >&2
  exit 2
  ;;
esac
