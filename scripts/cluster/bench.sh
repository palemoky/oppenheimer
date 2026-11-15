#!/usr/bin/env bash
# 在集群上重做 1a 与 1b 的下单吞吐对比（make cluster-bench；设计文档 1b 的 7.3.1、2c 的 10.3）。
# 本机的对应物是 scripts/bench.sh（make bench），结果见 docs/notes/1b-high-concurrency.md 的“压测结果”。
#
#   bench.sh [run]       跑对比（默认）
#   bench.sh images-1a   只构建并推送 1a 的四个镜像（BENCH_1A_PUSH=0 只构建不推送）
#   bench.sh render-1a   只把 1a 的清单渲染出来打印（不碰集群）
#   bench.sh down        清掉上一次 E2E_KEEP=1 留下的现场（命名空间、bench1a_* 和 e2e_* 库、e2e- 的 topic 和键）
#
# 要报的三个数（7.3.1 的“吞吐对比怎么做才公平”）：1a 的下单吞吐（同步返回 201，已落库）、1b 的受理吞吐（202）、
# 1b 的落库吞吐，外加积压排空用了多久。都用同一个工具（test/load 的 load run，同时接受 201 和 202）、
# 同一个发压 Pod、同一条入口路径来量。
#
# 做的事，按顺序：
#   1. 镜像：1b 的六个服务和 chaos 镜像（load、recon 加 busybox，发压用）照 images.sh 构建推送；1a 的四个镜像
#      从 BENCH_1A_REF（默认 732f9ab，main 上合并 PR #2 的提交）的临时 worktree 构建（deploy/docker/bench1a.Dockerfile），
#      名字 $REGISTRY_ADDR/oppenheimer/<服务>-1a:<短提交号>；仓库里已经有同名镜像就不重建（BENCH_1A_REBUILD=1 强制）。
#      E2E_SKIP_IMAGES=1 跳过 1b 和 chaos 的构建，这时必须给 IMAGE_TAG
#   2. 占用命名空间 oppenheimer-e2e（和 e2e、chaos 共用，规则见 e2e-lib.sh 的“命名空间的占用”），suite 标成 bench
#   3. services.sh e2e-prepare（Secret），建发压 Job：钉在 BENCH_LOAD_NODE 上，CPU requests BENCH_LOAD_CPU
#   4. 每种架构（BENCH_ARCH=both 时先 1a 后 1b，不同时部署）：
#        建库、部署、灌 SKU（5001 起 BENCH_SKU_COUNT 个，每个库存 BENCH_STOCK，不会卖完）、（1b）load warmup 装闸门、
#        等网关经入口就绪、预热一轮（BENCH_WARMUP，不计入结果），然后按档位各跑 BENCH_REPEAT 轮。每一轮：
#        load run → 等落库停下（落库数连续 5 次不变）→ 从库里按这一轮的幂等键前缀统计落库数和时间 → 汇总一行。
#        每一轮期间：kubectl top nodes 和 kubectl top pods -A --containers 每 BENCH_SAMPLE_INTERVAL 秒采样一次（看时间序列、峰值），
#        发压开始、结束、落库停下这三个时刻各取一次每个节点 kubelet 的累计 CPU 计数（/metrics/resource），
#        平均占用按计数的差算（比 top 的 15 秒窗口准）。能 ssh 到宿主机（cluster.env 的 HOST_SSH）时，
#        还记下这一轮宿主机有线网卡的收发字节数（见下面“流量走哪条路”）。
#        1a 跑完先删掉它的部署和 bench1a_* 库，再部署 1b。
#   5. 打印对比表（每档取几次运行的平均），原始输出都在 tmp/cluster-bench/<运行号>/
# 结束时（无论成败）：失败时收各 Pod 的日志；默认 services.sh e2e-down 删命名空间、清中间件，再删 bench1a_* 库；
# E2E_KEEP=1 保留现场（命名空间标成 kept，下一次 e2e、chaos、bench 会先清掉它；手工清用 bench.sh down）。
# 退出码：0 跑完（数字好坏不判）；1 中途失败；2 参数或环境不对；3 命名空间被占用。
#
# ---------- 两边不对等的地方（输出的 conditions.txt 和对比表的表头都写了） ----------
#   - 副本数：1a 每个服务 1 个副本（1a 的服务间直连 ClusterIP、节点号来自固定的 WORKER_ID，多副本会撞号，
#     见 deploy/cluster/bench/1a.yaml）；1b 用设计值（gateway、order、inventory、payment、orderindex 各 2，cdc 1，不开 HPA）。
#     BENCH_1B_REPLICAS=1 把 1b 也压成每个服务 1 个副本（再叠一层 deploy/cluster/services/bench-single/）。
#   - 数据库：1a 的三个库默认都在 mysql-main 一个实例上（同本机 bench 只有一个 MySQL），1b 的订单在两个分片实例上，
#     库存和支付在 mysql-main。BENCH_1A_ORDER_MYSQL=order-0 把 1a 的订单库放到 mysql-order-0 上（库存仍在 mysql-main），
#     这样 1a 也用上两个实例，单看“架构”而不是“实例数”。
#   - 1b 多了边车（每个 Pod 一个 Envoy）、Kafka、Redis；1b 不导出链路（OTEL_EXPORTER_OTLP_ENDPOINT 为空），1a 没有埋点。
#   - 1b 是当前分支的代码（含 1c 的分片、1d 的保护、2c 的改动），不是 1b 刚做完时的代码：比的是“1a 对现在”。
#
# ---------- 发压模式（BENCH_MODE） ----------
#   fire（默认）  闭环，-poll-timeout 1ms：虚拟用户提交后只查一次就下一单。1a 返回 201（订单已落库），不轮询，
#                和 closed 完全一样；1b 量出来的是受理吞吐。这就是本机对比表的口径（本机 BENCH_MODE 的默认是 closed，
#                但表里 1b 的受理吞吐是 fire 量的），集群上默认用它，数字可以直接和本机那张表对照。
#                为什么不默认开环：开环要先知道容量才能定速率档，速率高于容量时在途请求涨到 -max-in-flight，
#                之后的请求被丢弃，实际上退化成并发 max-in-flight 的闭环；闭环按并发档天然把系统压到饱和，
#                直接给出“这个并发下最多多快”。开环的价值在“给定到达速率下延迟是否稳住、何时开始被背压拒绝”，
#                用 BENCH_MODE=open 另跑。
#   closed        闭环，轮询到落库（-poll-interval BENCH_POLL_INTERVAL）：用户视角，1b 的受理吞吐被落库速度卡住。
#   open          开环，档位是 BENCH_OPEN_RATES（每秒请求数）而不是并发数，-max-in-flight BENCH_OPEN_MAX_IN_FLIGHT。
#
# ---------- 发压和被测系统抢 CPU、入口是否先成为瓶颈 ----------
# 发压 Pod 钉在一个工作节点上、给足 CPU requests（deploy/cluster/bench/load-job.yaml 的说明）。看采样判断：
#   - 发压 Pod 的 CPU（cpu_load）接近它所在节点剩下的全部、或者节点 CPU（cpu_node_max_pct）接近 100%：
#     发压一侧可能是瓶颈，换 BENCH_LOAD_NODE 或调大 BENCH_LOAD_CPU 复测，数字变了就说明之前被它限住了。
#   - 入口 Envoy（envoy-gateway-system 的 2 个副本，一个在控制面节点上，那里还有 etcd、一个 Kafka broker 和 Redis）：
#     cpu_ingress_max 是占用最高的那个副本的核数。它涨到所在节点空闲 CPU 的上限、而被测服务和 MySQL 都没满，
#     入口就是瓶颈。用 BENCH_VIA=service 绕过入口（直接打 gateway 的 ClusterIP Service）对照：绕过后吞吐明显上升，
#     就说明入口限住了；两边一样，入口不是瓶颈。
#   - MySQL：cpu_mysql_max 是占用最高的 MySQL Pod。1a 在本机是卡在 MySQL 的（容器 135% 到 160%）。
#   - 长期运行的那一套（命名空间 oppenheimer）照常在跑，cpu.txt 里有它的占用；它空闲时只有几十毫核。
#
# ---------- 流量走哪条路 ----------
# 三个节点是同一台宿主机上的虚拟机，接在宿主机的网桥 br0 上。发压 Pod 打的是 http://e2e.oppenheimer.test（1b）
# 或 http://bench1a.oppenheimer.test（1a），hostAliases 指到入口的 LoadBalancer 地址 192.168.50.51。Pod 访问
# LoadBalancer 地址时，kube-proxy 在 Pod 所在的节点上就把它 DNAT 到入口 Envoy 的某个 Pod（集群内的地址，
# 节点之间走 flannel 的 VXLAN，经宿主机的网桥），包不经过宿主机的有线网卡，更不经过 MacBook 和宿主机之间的百兆网线。
# 百兆只影响从 MacBook 推镜像（1a 四个镜像加 1b 七个，每个十几 MB）和 kubectl 本身的流量。
# 每一轮记下的宿主机网卡字节数（nic_mb）用来核实：压测流量走了网卡的话，每轮会是几十到几百 MB；
# 只剩 kubectl 的流量时是零点几 MB。
#
# ---------- 环境变量 ----------
#   BENCH_ARCH            1a、1b 或 both（默认 both，先 1a 后 1b）
#   BENCH_MODE            fire（默认）、closed 或 open，见上
#   BENCH_CONCURRENCY     并发档（fire、closed），空格分隔，默认 "50 200 800"（同本机）
#   BENCH_OPEN_RATES      开环的速率档（open），默认 "1000 2000 4000 8000"
#   BENCH_OPEN_MAX_IN_FLIGHT  开环在途上限，默认 2000（同本机）
#   BENCH_REPEAT          每档运行次数，默认 2（同本机）
#   BENCH_DURATION        每轮的提交窗口，默认 60s（本机 30s：kubelet 和 metrics-server 的 CPU 数据 15 秒一更新，
#                         30 秒的一轮只有一两个点；Pod 刚起来时 Go 运行时和连接池也要一阵才稳）
#   BENCH_WARMUP          每种架构正式轮次之前的预热时长（并发取第一档，不计入结果），默认 15s，0 跳过
#   BENCH_SKU_COUNT       SKU 个数（id 从 5001 起），默认 10（同本机）
#   BENCH_STOCK           每个 SKU 的库存，默认 100000000（同本机）
#   BENCH_USERS           用户 id 范围，默认 100000（同本机）
#   BENCH_QTY             每单数量，默认 1
#   BENCH_POLL_INTERVAL   closed 的轮询间隔、open 收尾的轮询间隔，默认 50ms
#   BENCH_QUIET_TIMEOUT   每轮之后等落库停下的上限（秒），默认 closed 180，fire、open 1200
#   BENCH_LOAD_ARGS       追加给 load run 的参数
#   BENCH_LOAD_NODE       发压 Pod 钉在哪个节点，默认 k3s-agent-1（两个工作节点都有 MySQL 主库：agent-1 上是
#                         mysql-order-0 的主库，agent-2 上是 mysql-main 和 mysql-order-1 的主库，1a 的三个库默认都在 mysql-main，
#                         所以默认放在 agent-1，不和 1a 的瓶颈抢同一个节点）
#   BENCH_LOAD_CPU        发压 Pod 的 CPU requests，默认 2（核）
#   BENCH_LOAD_MEMORY     发压 Pod 的内存 requests，默认 512Mi
#   BENCH_VIA             ingress（默认，经入口 Envoy，同 e2e、chaos）或 service（直接打 gateway 的 ClusterIP Service）
#   BENCH_1B_REPLICAS     2（默认，设计值）或 1（每个服务 1 个副本，和 1a 对等）
#   BENCH_1B_ADMIT_MAX_QUEUE  不设就用服务的默认值（1 万，受理端背压，ADR-0011）；0 关掉按队列长度的背压
#   BENCH_1A_ORDER_MYSQL  main（默认）或 order-0，1a 的订单库建在哪个实例上，见上
#   BENCH_1A_REF          1a 的提交，默认 732f9ab
#   BENCH_1A_REBUILD      设为 1 时即使仓库里已有 1a 的镜像也重新构建推送
#   BENCH_1A_PUSH         images-1a 子命令用：0 只构建不推送，默认 1
#   BENCH_SAMPLE_INTERVAL kubectl top 的采样间隔（秒），默认 15（metrics-server 的分辨率）
#   BENCH_HOST_NIC        auto（默认，ssh 到宿主机读网卡计数，读不到就记 n/a）或 off
#   LOG_LEVEL             load run 的日志级别，默认 warn（服务的日志级别在 values 和 1a.yaml 里，都是 warn）
#   IMAGE_TAG、E2E_SKIP_IMAGES、E2E_KEEP、E2E_RUN_ID（默认当前 Unix 时间）、E2E_NAMESPACE（默认 oppenheimer-e2e）：同 e2e.sh
#
# 和长期运行的那一套共用 MySQL、Kafka、Redis 和入口：跑的时候不要做别的压测，长期运行那一套也不要有流量。
# 在 macOS 自带的 bash 3.2 上运行。密码不进命令行和日志：SQL 在 mysql 的 Pod 里用 root 执行（e2e-lib.sh），
# 服务的 DSN 由 Kubernetes 从 Secret 展开。需要本机有 python3、git、docker、curl。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
ROOT="$(cd "$HERE/../.." && pwd)"

export KUBECONFIG="$KUBECONFIG_OUT"

# shellcheck source=scripts/cluster/e2e-lib.sh
source "$HERE/e2e-lib.sh"

NS="${E2E_NAMESPACE:-oppenheimer-e2e}"
export E2E_NAMESPACE="$NS"
export E2E_RUN_ID="${E2E_RUN_ID:-$(date +%s)}"
RUN_ID="$E2E_RUN_ID"

BENCH_DIR="$ROOT/deploy/cluster/bench"
JOB_TEMPLATE="$BENCH_DIR/load-job.yaml"
MANIFEST_1A="$BENCH_DIR/1a.yaml"
# 分阶段启动命令的脚本和 chaos 共用（deploy/cluster/chaos/phase.sh）。
PHASE_SCRIPT="$ROOT/deploy/cluster/chaos/phase.sh"
JOB="bench"
CONTAINER="bench"
IMAGES_1B="gateway order inventory payment orderindex cdc chaos"
SERVICES_1A="inventory order payment gateway"
PART_OF_1A="oppenheimer-bench1a"
HOST_1A="bench1a.oppenheimer.test"
HOST_1B="e2e.oppenheimer.test"
# 1a 的库（bench1a_ 前缀，和 1b 的 e2e_* 分开：表结构不同）。
DBS_1A="bench1a_inventory_db bench1a_order_db bench1a_payment_db"
# 1b 的订单分片：库 i 上是 slot % 2 == i 的那几张表（pkg/sharding：db = slot % DBs，8 个 slot，2 个库）。
SLOTS_DB0="0 2 4 6"
SLOTS_DB1="1 3 5 7"

ARCH="${BENCH_ARCH:-both}"
MODE="${BENCH_MODE:-fire}"
CONCURRENCY_LIST="${BENCH_CONCURRENCY:-50 200 800}"
OPEN_RATES="${BENCH_OPEN_RATES:-1000 2000 4000 8000}"
OPEN_MAX_IN_FLIGHT="${BENCH_OPEN_MAX_IN_FLIGHT:-2000}"
REPEAT="${BENCH_REPEAT:-2}"
DURATION="${BENCH_DURATION:-60s}"
WARMUP="${BENCH_WARMUP:-15s}"
SKU_COUNT="${BENCH_SKU_COUNT:-10}"
STOCK="${BENCH_STOCK:-100000000}"
USERS="${BENCH_USERS:-100000}"
QTY="${BENCH_QTY:-1}"
POLL_INTERVAL="${BENCH_POLL_INTERVAL:-50ms}"
QUIET_TIMEOUT="${BENCH_QUIET_TIMEOUT:-}"
LOAD_NODE="${BENCH_LOAD_NODE:-k3s-agent-1}"
LOAD_CPU="${BENCH_LOAD_CPU:-2}"
LOAD_MEMORY="${BENCH_LOAD_MEMORY:-512Mi}"
VIA="${BENCH_VIA:-ingress}"
REPLICAS_1B="${BENCH_1B_REPLICAS:-2}"
ADMIT_MAX_QUEUE="${BENCH_1B_ADMIT_MAX_QUEUE:-}"
ORDER_MYSQL_1A="${BENCH_1A_ORDER_MYSQL:-main}"
REF_1A="${BENCH_1A_REF:-732f9ab}"
SAMPLE_INTERVAL="${BENCH_SAMPLE_INTERVAL:-15}"
HOST_NIC_MODE="${BENCH_HOST_NIC:-auto}"
LOAD_LOG_LEVEL="${LOG_LEVEL:-warn}"

die() { # 退出码 消息
  local code="$1"
  shift
  echo "$*" >&2
  exit "$code"
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

is_uint() {
  case "$1" in
  '' | *[!0-9]*) return 1 ;;
  esac
  return 0
}

# ---------- 1a 的镜像 ----------
TAG_1A=""
resolve_1a_ref() {
  TAG_1A="$(git -C "$ROOT" rev-parse --short=7 --verify "$REF_1A^{commit}" 2>/dev/null)" ||
    die 2 "BENCH_1A_REF $REF_1A is not a commit in this repository"
}

image_1a() { # 服务
  echo "$REGISTRY_ADDR/oppenheimer/$1-1a:$TAG_1A"
}

# 仓库里有没有这个镜像：问 registry 的 manifest（只读的 HEAD）。仓库是集群里的 distribution，没有鉴权，明文 HTTP。
registry_has() { # 服务
  curl -fsS -o /dev/null -I -m 10 \
    -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    "http://$REGISTRY_ADDR/v2/oppenheimer/$1-1a/manifests/$TAG_1A" 2>/dev/null
}

# 从 1a 提交的临时 worktree 构建四个镜像（linux/amd64），push=1 时推送。worktree 用完就删（失败也删）。
build_1a_images() { # push(0|1)
  local push="$1" wt s
  wt="$(mktemp -d "${TMPDIR:-/tmp}/bench1a-src.XXXXXX")"
  # 子 shell 里 trap，保证 worktree 一定被删掉；主 shell 的 trap 不受影响。
  (
    trap 'git -C "$ROOT" worktree remove --force "$wt" >/dev/null 2>&1 || command rm -rf "$wt"; git -C "$ROOT" worktree prune' EXIT
    echo "==> 1a source: worktree of $REF_1A ($TAG_1A) at $wt"
    git -C "$ROOT" worktree add --detach "$wt" "$TAG_1A" >/dev/null
    for s in $SERVICES_1A; do
      echo "==> build $(image_1a "$s")"
      docker buildx build --platform linux/amd64 --load \
        -f "$ROOT/deploy/docker/bench1a.Dockerfile" \
        --build-arg "SERVICE=$s" \
        -t "$(image_1a "$s")" \
        "$wt"
    done
  )
  if [[ "$push" == 1 ]]; then
    for s in $SERVICES_1A; do
      echo "==> push $(image_1a "$s")"
      docker push "$(image_1a "$s")"
    done
  fi
}

ensure_1a_images() {
  local s missing=""
  if [[ "${BENCH_1A_REBUILD:-}" != 1 ]]; then
    for s in $SERVICES_1A; do
      registry_has "$s" || missing="$missing $s"
    done
    if [[ -z "$missing" ]]; then
      echo "==> 1a images $TAG_1A already in $REGISTRY_ADDR (BENCH_1A_REBUILD=1 to rebuild)"
      return 0
    fi
    echo "==> 1a images missing in the registry:$missing"
  fi
  build_1a_images 1
}

# ---------- 1a 的清单 ----------
# 订单库在哪个实例：决定 DSN 的主机和引用哪个密码。值不含密码（$(VAR) 由 Kubernetes 展开）。
order_1a_release() {
  case "$ORDER_MYSQL_1A" in
  main) echo mysql-main ;;
  order-0) echo mysql-order-0 ;;
  esac
}

render_1a() {
  local line var key host
  case "$ORDER_MYSQL_1A" in
  main) var=MYSQL_MAIN_PASSWORD key=main-app-password host=mysql-main-primary ;;
  order-0) var=MYSQL_ORDER_0_PASSWORD key=order-0-app-password host=mysql-order-0-primary ;;
  esac
  local dsn="app:\$($var)@tcp($host.mysql.svc:3306)/bench1a_order_db"
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line//__IMAGE_PREFIX__/$REGISTRY_ADDR/oppenheimer}"
    line="${line//__TAG__/$TAG_1A}"
    line="${line//__HOST__/$HOST_1A}"
    line="${line//__ORDER_DSN__/$dsn}"
    line="${line//__ORDER_PASSWORD_VAR__/$var}"
    line="${line//__ORDER_PASSWORD_KEY__/$key}"
    printf '%s\n' "$line"
  done <"$MANIFEST_1A"
}

render_job() { # image
  local line
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line//__IMAGE__/$1}"
    line="${line//__LB_GATEWAY_IP__/$LB_GATEWAY_IP}"
    line="${line//__RUN_ID__/$RUN_ID}"
    line="${line//__NODE__/$LOAD_NODE}"
    line="${line//__CPU__/$LOAD_CPU}"
    line="${line//__MEMORY__/$LOAD_MEMORY}"
    printf '%s\n' "$line"
  done <"$JOB_TEMPLATE"
}

# ---------- 1a 的库 ----------
# 和 e2e_create_dbs 同样的写法：GRANT 写在 CREATE 前面，授权在 DROP 之后仍留在 mysql.db 里，重跑没有副作用。
# app 要 ALL：1a 启动时自己跑迁移（goose 建表、建 goose_db_version）。
create_1a_dbs() {
  local main order_rel pod db sql=""
  main="$(e2e_mysql_pod mysql-main primary)"
  order_rel="$(order_1a_release)"
  for db in $DBS_1A; do
    if [[ "$db" == bench1a_order_db && "$order_rel" != mysql-main ]]; then continue; fi
    sql="$sql
GRANT ALL PRIVILEGES ON \`$(e2e_grant_name "$db")\`.* TO 'app'@'%';
DROP DATABASE IF EXISTS \`$db\`;
CREATE DATABASE \`$db\`;"
  done
  printf '%s\n' "$sql" | e2e_root_sql "$main"
  echo "    mysql-main ($main): bench1a_* databases"
  if [[ "$order_rel" != mysql-main ]]; then
    pod="$(e2e_mysql_pod "$order_rel" primary)"
    printf '%s\n' "GRANT ALL PRIVILEGES ON \`$(e2e_grant_name bench1a_order_db)\`.* TO 'app'@'%';
DROP DATABASE IF EXISTS \`bench1a_order_db\`;
CREATE DATABASE \`bench1a_order_db\`;" | e2e_root_sql "$pod"
    echo "    $order_rel ($pod): bench1a_order_db"
  fi
}

# 两处都删（订单库可能在 mysql-order-0 上）。失败只警告：收尾时不盖掉调用方的退出码。
drop_1a_dbs() {
  local pod db sql=""
  for db in $DBS_1A; do sql="$sql DROP DATABASE IF EXISTS \`$db\`;"; done
  if pod="$(e2e_mysql_pod mysql-main primary 2>/dev/null)" && printf '%s\n' "$sql" | e2e_root_sql "$pod" >/dev/null 2>&1; then
    echo "    dropped bench1a_* on mysql-main"
  else
    echo "    WARNING: dropping bench1a_* on mysql-main failed" >&2
  fi
  if pod="$(e2e_mysql_pod mysql-order-0 primary 2>/dev/null)" &&
    printf '%s\n' "DROP DATABASE IF EXISTS \`bench1a_order_db\`;" | e2e_root_sql "$pod" >/dev/null 2>&1; then
    echo "    dropped bench1a_order_db on mysql-order-0 (if it was there)"
  else
    echo "    WARNING: dropping bench1a_order_db on mysql-order-0 failed" >&2
  fi
}

# ---------- 子命令：images-1a、render-1a、down ----------
cmd="${1:-run}"
case "$cmd" in
images-1a)
  resolve_1a_ref
  build_1a_images "${BENCH_1A_PUSH:-1}"
  exit 0
  ;;
render-1a)
  case "$ORDER_MYSQL_1A" in main | order-0) ;; *) die 2 "BENCH_1A_ORDER_MYSQL must be main or order-0" ;; esac
  resolve_1a_ref
  render_1a
  exit 0
  ;;
down)
  [[ -f "$KUBECONFIG" ]] || die 2 "$KUBECONFIG not found; run scripts/cluster/k3s.sh kubeconfig first"
  e2e_check_free "$NS" || exit 3
  echo "==> bench1a databases"
  drop_1a_dbs
  "$HERE/services.sh" e2e-down
  exit 0
  ;;
run) ;;
*)
  sed -n '2,8p' "${BASH_SOURCE[0]}" >&2
  exit 2
  ;;
esac

# ---------- 参数检查 ----------
[[ -f "$KUBECONFIG" ]] || die 2 "$KUBECONFIG not found; run scripts/cluster/k3s.sh kubeconfig first"
case "$ARCH" in
1a) ARCHES="1a" ;;
1b) ARCHES="1b" ;;
both) ARCHES="1a 1b" ;;
*) die 2 "BENCH_ARCH must be 1a, 1b or both, got $ARCH" ;;
esac
case "$MODE" in
closed) QUIET_TIMEOUT="${QUIET_TIMEOUT:-180}" ;;
fire | open) QUIET_TIMEOUT="${QUIET_TIMEOUT:-1200}" ;;
*) die 2 "BENCH_MODE must be fire, closed or open, got $MODE" ;;
esac
if [[ "$MODE" == open ]]; then LEVELS="$OPEN_RATES"; else LEVELS="$CONCURRENCY_LIST"; fi
for v in $LEVELS; do is_uint "$v" && [[ "$v" -gt 0 ]] || die 2 "levels must be positive integers, got '$v'"; done
[[ -n "${LEVELS// /}" ]] || die 2 "no levels to run (BENCH_CONCURRENCY / BENCH_OPEN_RATES is empty)"
is_uint "$REPEAT" && [[ "$REPEAT" -gt 0 ]] || die 2 "BENCH_REPEAT must be a positive integer"
is_uint "$QUIET_TIMEOUT" || die 2 "BENCH_QUIET_TIMEOUT must be seconds"
is_uint "$SAMPLE_INTERVAL" && [[ "$SAMPLE_INTERVAL" -gt 0 ]] || die 2 "BENCH_SAMPLE_INTERVAL must be a positive integer"
dur_seconds "$DURATION" >/dev/null || die 2 "BENCH_DURATION must look like 60s or 2m, got $DURATION"
WARMUP_S="$(dur_seconds "$WARMUP")" || die 2 "BENCH_WARMUP must look like 15s or 0, got $WARMUP"
case "$VIA" in ingress | service) ;; *) die 2 "BENCH_VIA must be ingress or service" ;; esac
case "$REPLICAS_1B" in 1 | 2) ;; *) die 2 "BENCH_1B_REPLICAS must be 1 or 2" ;; esac
case "$ORDER_MYSQL_1A" in main | order-0) ;; *) die 2 "BENCH_1A_ORDER_MYSQL must be main or order-0" ;; esac
case "$HOST_NIC_MODE" in auto | off) ;; *) die 2 "BENCH_HOST_NIC must be auto or off" ;; esac
if [[ -n "$ADMIT_MAX_QUEUE" ]]; then is_uint "$ADMIT_MAX_QUEUE" || die 2 "BENCH_1B_ADMIT_MAX_QUEUE must be a non-negative integer"; fi
kubectl get node "$LOAD_NODE" >/dev/null 2>&1 || die 2 "BENCH_LOAD_NODE $LOAD_NODE is not a node of the cluster"
for t in python3 git docker curl; do command -v "$t" >/dev/null || die 2 "$t is required"; done
in_arch() { [[ " $ARCHES " == *" $1 "* ]]; }
if in_arch 1a; then resolve_1a_ref; fi

e2e_check_free "$NS" || exit 3

OUT="$ROOT/tmp/cluster-bench/$RUN_ID"
mkdir -p "$OUT"
ln -sfn "$RUN_ID" "$ROOT/tmp/cluster-bench/latest"
EVENTS="$OUT/bench.log"
SUMMARY="$OUT/summary.tsv"
log() { echo "[bench $(date +%H:%M:%S)] $*" | tee -a "$EVENTS" >&2; }

# ---------- 发压 Pod 里的阶段 ----------
POD=""
pexec() { kubectl -n "$NS" exec "$POD" -c "$CONTAINER" -- "$@"; }
phase_status() { pexec sh /phase/phase.sh status "$1" 2>/dev/null || true; }

# 启动一个阶段并等它结束（每 3 秒看一次），输出取回到目录 $2。退出码放在 PHASE_CODE（超时是 timeout）。
PHASE_CODE=""
run_phase() { # 名字 目录 时限秒数 命令...
  local name="$1" dir="$2" limit="$3" deadline
  shift 3
  pexec sh /phase/phase.sh start "$name" "$@"
  deadline=$((SECONDS + limit))
  PHASE_CODE=""
  while [[ -z "$PHASE_CODE" ]]; do
    if [[ $SECONDS -ge $deadline ]]; then
      PHASE_CODE=timeout
      break
    fi
    sleep 3
    PHASE_CODE="$(phase_status "$name")"
  done
  pexec cat "/work/$name.out" >"$dir/$name.out" 2>/dev/null || true
  pexec cat "/work/$name.err" >"$dir/$name.err" 2>/dev/null || true
}

# ---------- 采样 ----------
K8S_NODES="$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{" "}{end}')"

# 每个节点 kubelet 的累计 CPU 计数（/metrics/resource），存成 <目录>/res-<标签>-<节点>.txt。
# 计数是累计的 CPU 秒数，每条带采集时刻（毫秒），两次之差除以时刻之差就是这段时间的平均核数。
snapshot() { # 目录 标签
  local n
  for n in $K8S_NODES; do
    kubectl get --raw "/api/v1/nodes/$n/proxy/metrics/resource" >"$1/res-$2-$n.txt" 2>/dev/null || : >"$1/res-$2-$n.txt"
  done
}

# 宿主机有线网卡的收发字节数（读不到就输出 n/a）。只读：cat sysfs。
nic_bytes() {
  local out
  if [[ "$HOST_NIC_MODE" == off ]]; then
    echo n/a
    return 0
  fi
  if out="$(ssh -o BatchMode=yes -o ConnectTimeout=5 "$HOST_SSH" \
    "cat /sys/class/net/$HOST_NIC/statistics/rx_bytes /sys/class/net/$HOST_NIC/statistics/tx_bytes" 2>/dev/null)"; then
    echo "$out" | awk '{s += $1} END {print s}'
  else
    echo n/a
  fi
}

# 后台每 SAMPLE_INTERVAL 秒采一次 kubectl top，每行前面加上时刻（MacBook 的时钟）和阶段（读 <目录>/phase）。
SAMPLER_PID=""
start_sampler() { # 目录
  local dir="$1"
  (
    set +e
    while :; do
      ph="$(cat "$dir/phase" 2>/dev/null)"
      ts="$(date +%s)"
      kubectl top nodes --no-headers 2>/dev/null | awk -v t="$ts" -v p="${ph:-?}" '{print t, p, $0}' >>"$dir/top-nodes.txt"
      kubectl top pods -A --containers --no-headers 2>/dev/null | awk -v t="$ts" -v p="${ph:-?}" '{print t, p, $0}' >>"$dir/top-pods.txt"
      sleep "$SAMPLE_INTERVAL"
    done
  ) &
  SAMPLER_PID=$!
}
stop_sampler() {
  if [[ -n "$SAMPLER_PID" ]]; then
    kill "$SAMPLER_PID" 2>/dev/null || true
    wait "$SAMPLER_PID" 2>/dev/null || true
    SAMPLER_PID=""
  fi
}

# ---------- 库里的落库数 ----------
# 当前架构的订单库：每项 “实例:库:表...”。在 setup 里设好，并缓存各实例主库的 Pod 名（压测期间不做主从切换）。
ORDER_TARGETS=""
target_pod() { # 实例
  case "$1" in
  mysql-main) echo "$POD_MAIN" ;;
  mysql-order-0) echo "$POD_ORDER0" ;;
  mysql-order-1) echo "$POD_ORDER1" ;;
  esac
}

union_of() { # 库 列 条件 表...
  local db="$1" cols="$2" where="$3" t out="" sep=""
  shift 3
  for t in "$@"; do
    out="$out${sep}SELECT $cols FROM \`$db\`.\`$t\` WHERE $where"
    sep=" UNION ALL "
  done
  echo "$out"
}

# 在每个订单库上执行同一个查询（子查询是这个库里所有订单表的 UNION ALL），每个库输出一段。
# 查询文本里的 {U} 换成子查询。SET time_zone：UNIX_TIMESTAMP 按会话时区解释 DATETIME，服务写入时用的是 +00:00。
each_order_db() { # 列 条件 查询
  local cols="$1" where="$2" q="$3" spec rel db tables u
  for spec in $ORDER_TARGETS; do
    rel="${spec%%:*}"
    db="${spec#*:}"
    tables="${db#*:}"
    db="${db%%:*}"
    # shellcheck disable=SC2086 # 表名按逗号拆开是有意的
    u="$(union_of "$db" "$cols" "$where" ${tables//,/ })"
    printf '%s\n' "SET time_zone = '+00:00'; ${q//\{U\}/$u};" | e2e_root_sql "$(target_pod "$rel")" -N -B
  done
}

# 进入 pending_payment（status 2）的订单总数。不支付不取消，所以落库的订单都停在这个状态。
landed_total() {
  each_order_db status "status = 2" "SELECT COUNT(*) FROM ({U}) o" | awk '{s += $1} END {print s + 0}'
}

# 这一轮（按幂等键前缀）的落库数、最早的 created_at、最晚的 updated_at（Unix 秒），写到文件。
run_stats() { # 运行号 文件 状态分布文件
  local where="idempotency_key LIKE '$1-%'"
  each_order_db "status, created_at, updated_at" "$where AND status = 2" \
    "SELECT COUNT(*), UNIX_TIMESTAMP(MIN(created_at)), UNIX_TIMESTAMP(MAX(updated_at)) FROM ({U}) o" |
    awk '{c += $1; if ($2 != "NULL" && (lo == "" || $2 < lo)) lo = $2; if ($3 != "NULL" && (hi == "" || $3 > hi)) hi = $3}
      END {print c + 0, (lo == "" ? "NULL" : lo), (hi == "" ? "NULL" : hi)}' >"$2"
  each_order_db status "$where" "SELECT status, COUNT(*) FROM ({U}) o GROUP BY status" >"$3"
}

# 等落库停下：落库总数连续 5 次（每次间隔 2 秒）不变。1a 同步落库，立刻满足。等了多少秒放在 QUIET_S。
QUIET_S=0
wait_quiet() { # 时限秒数
  local t0=$SECONDS prev=-1 stable=0 count
  while [[ $((SECONDS - t0)) -lt $1 ]]; do
    count="$(landed_total 2>/dev/null)" || count=err
    if [[ "$count" == "$prev" && "$count" != err ]]; then stable=$((stable + 1)); else stable=0; fi
    prev="$count"
    if [[ $stable -ge 5 ]]; then break; fi
    sleep 2
  done
  QUIET_S=$((SECONDS - t0))
  if [[ $stable -lt 5 ]]; then log "WARNING: landing did not stop within $1s (last count $prev)"; fi
}

# ---------- 一轮 ----------
SKU_LIST=""
SKU_VALUES=""
for i in $(seq 1 "$SKU_COUNT"); do
  sku=$((5000 + i))
  SKU_LIST="$SKU_LIST${SKU_LIST:+,}$sku"
  SKU_VALUES="$SKU_VALUES${SKU_VALUES:+,}($sku, 'bench $sku', 1990, $STOCK)"
done

CUR_ARCH=""
CUR_URL=""
CUR_REPLICAS=""

# load run 的参数（不含 -addr 之外的每轮变量）。
load_args() { # 档位 时长 运行号 名字
  LOAD_ARGS=(load run -addr "$CUR_URL" -duration "$2" -skus "$SKU_LIST" -users "$USERS" -qty "$QTY"
    -pay 0 -cancel 0 -log-level "$LOAD_LOG_LEVEL" -progress 0 -run-id "$3"
    -ledger "/work/$4.ledger" -json "/work/$4.json")
  case "$MODE" in
  closed) LOAD_ARGS+=(-c "$1" -poll-interval "$POLL_INTERVAL" -poll-timeout 30s) ;;
  fire) LOAD_ARGS+=(-c "$1" -poll-interval 1ms -poll-timeout 1ms) ;;
  # 收尾只查一下（-drain 1s，同本机 bench 的对比轮次），落库吞吐另从库里统计。
  open) LOAD_ARGS+=(-mode open -rate "$1" -max-in-flight "$OPEN_MAX_IN_FLIGHT" -drain 1s -poll-interval "$POLL_INTERVAL") ;;
  esac
  local extra
  # shellcheck disable=SC2206 # BENCH_LOAD_ARGS 故意按空格拆分
  extra=(${BENCH_LOAD_ARGS:-})
  LOAD_ARGS+=(${extra[@]+"${extra[@]}"})
}

run_round() { # 档位 第几次 [warmup]
  local lvl="$1" n="$2" warm="${3:-}" name tok rid dir dur="$DURATION" limit nic0 nic1 t0 t1 t2
  if [[ "$MODE" == open ]]; then tok="r$lvl"; else tok="c$lvl"; fi
  name="$CUR_ARCH-$tok-n$n"
  if [[ -n "$warm" ]]; then
    name="$CUR_ARCH-warmup"
    dur="$WARMUP"
  fi
  # 幂等键前缀：只用字母和数字，库里按它统计这一轮的订单。
  rid="b${RUN_ID}x${CUR_ARCH}${tok}n${n}${warm:+w}"
  dir="$OUT/$name"
  mkdir -p "$dir"
  load_args "$lvl" "$dur" "$rid" "$name"
  printf '%s\n' "${LOAD_ARGS[*]}" >"$dir/command.txt"
  log "$name: ${LOAD_ARGS[*]:0:2} $(if [[ "$MODE" == open ]]; then echo "rate $lvl"; else echo "c=$lvl"; fi) duration $dur ($CUR_ARCH, replicas $CUR_REPLICAS, via $VIA)"

  echo load >"$dir/phase"
  start_sampler "$dir"
  nic0="$(nic_bytes)"
  snapshot "$dir" start
  t0="$(date +%s)"
  limit=$(($(dur_seconds "$dur") + 300))
  run_phase "$name" "$dir" "$limit" "${LOAD_ARGS[@]}"
  t1="$(date +%s)"
  snapshot "$dir" end
  nic1="$(nic_bytes)"
  pexec cat "/work/$name.json" >"$dir/report.json" 2>/dev/null || true
  pexec rm -f "/work/$name.ledger" >/dev/null 2>&1 || true
  if [[ "$PHASE_CODE" != 0 ]]; then log "$name: load run exited with $PHASE_CODE (see $dir/$name.err)"; fi

  echo drain >"$dir/phase"
  wait_quiet "$QUIET_TIMEOUT"
  snapshot "$dir" quiet
  t2="$(date +%s)"
  stop_sampler
  run_stats "$rid" "$dir/db-landed.txt" "$dir/db-status.txt"
  {
    echo "arch $CUR_ARCH"
    echo "replicas $CUR_REPLICAS"
    echo "mode $MODE"
    echo "level $lvl"
    echo "run $n"
    echo "load_code $PHASE_CODE"
    echo "t_load_start $t0"
    echo "t_load_end $t1"
    echo "t_quiet $t2"
    echo "quiet_wait_s $QUIET_S"
    echo "nic0 $nic0"
    echo "nic1 $nic1"
  } >"$dir/meta.txt"
  if [[ -n "$warm" ]]; then
    log "$name: warmup done ($(awk '{print $1}' "$dir/db-landed.txt") landed)"
    return 0
  fi
  summarize_round "$dir"
}

# 一轮汇总成 summary.tsv 的一行，并把每个容器的平均 CPU 写到 <目录>/cpu.txt。
summarize_round() { # 目录
  python3 -I - "$1" "$NS" >>"$SUMMARY" <<'PY'
import json, sys, datetime, re
d, ns = sys.argv[1:]
meta = dict(l.split(None, 1) for l in open(d + "/meta.txt").read().splitlines() if l.strip())
meta = {k: v.strip() for k, v in meta.items()}
try:
    r = json.load(open(d + "/report.json"))
except Exception:
    r = {}

def parse_res(path):
    # 返回 {("node", 名字): (值, 毫秒)}、{(ns, pod, container): (值, 毫秒)}
    out = {}
    try:
        lines = open(path).read().splitlines()
    except Exception:
        return out
    for l in lines:
        if l.startswith("node_cpu_usage_seconds_total"):
            p = l.split()
            out[("node",)] = (float(p[1]), float(p[2]) if len(p) > 2 else 0)
        elif l.startswith("container_cpu_usage_seconds_total{"):
            m = re.match(r'container_cpu_usage_seconds_total\{(.*)\}\s+(\S+)\s*(\S*)', l)
            if not m:
                continue
            labels = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1)))
            out[(labels.get("namespace"), labels.get("pod"), labels.get("container"))] = (float(m.group(2)), float(m.group(3) or 0))
    return out

import glob, os
def rates(a, b):
    res = {}
    nodes = {}
    for fa in glob.glob(d + "/res-%s-*.txt" % a):
        node = fa[len(d + "/res-%s-" % a):-4]
        fb = d + "/res-%s-%s.txt" % (b, node)
        if not os.path.exists(fb):
            continue
        A, B = parse_res(fa), parse_res(fb)
        for k, (v0, t0) in A.items():
            if k not in B:
                continue
            v1, t1 = B[k]
            if t1 <= t0:
                continue
            rate = (v1 - v0) / ((t1 - t0) / 1000.0)
            if k[0] == "node":
                nodes[node] = rate
            else:
                res[k] = rate
    return res, nodes

cont, nodes = rates("start", "end")
cap = {}
for l in open(os.path.dirname(d) + "/nodes-capacity.txt").read().splitlines():
    p = l.split()
    if len(p) == 2:
        cap[p[0]] = float(p[1])

def app(pod):
    # Deployment 的 Pod 名是 <名字>-<模板哈希>-<随机>，Job 的是 <名字>-<随机>
    parts = pod.split("-")
    return "-".join(parts[:-2]) if len(parts) > 2 else pod

with open(d + "/cpu.txt", "w") as f:
    f.write("# average cores between load start and load end (kubelet /metrics/resource)\n")
    for n in sorted(nodes):
        f.write("node %-14s %6.2f cores  %5.1f%% of %s\n" % (n, nodes[n], 100 * nodes[n] / cap.get(n, 1), cap.get(n, "?")))
    for k in sorted(cont, key=lambda k: -cont[k]):
        if cont[k] >= 0.005:
            f.write("%-22s %-60s %-18s %6.3f\n" % (k[0], k[1], k[2], cont[k]))

svc = sum(v for (n, p, c), v in cont.items() if n == ns and not p.startswith("bench-"))
load = sum(v for (n, p, c), v in cont.items() if n == ns and p.startswith("bench-"))
ing = {}
for (n, p, c), v in cont.items():
    if n == "envoy-gateway-system" and p.startswith("envoy-envoy-gateway-system-public") and c == "envoy":
        ing[p] = ing.get(p, 0) + v
mysql = {}
for (n, p, c), v in cont.items():
    if n == "mysql" and c == "mysql":
        mysql[p] = mysql.get(p, 0) + v
mysql_max_pod = max(mysql, key=mysql.get) if mysql else "-"
node_pct = {n: 100 * v / cap.get(n, 1) for n, v in nodes.items()}
node_max = max(node_pct, key=node_pct.get) if node_pct else "-"

o = r.get("open")
acc = r.get("accepted", {})
if o:
    oc = o.get("outcomes", {})
    accepted = oc.get("accepted", 0)
    win = o.get("send_window_seconds", 0)
    acc_rate = round(accepted / win, 1) if win else 0
    lat = o.get("accepted_from_schedule", {})
    backlogged = oc.get("backlogged", 0)
    last_accept_off = acc.get("window_seconds") or win
else:
    accepted = acc.get("count", 0)
    acc_rate = acc.get("per_second", 0)
    lat = acc.get("latency", {})
    backlogged = r.get("http_errors", {}).get("submit 429", 0)
    last_accept_off = acc.get("window_seconds", 0)
errs = sum(v for op in r.get("requests", {}).values() for k, v in op.items() if k not in ("ok", "rejected"))
sync = r.get("orders", {}).get("accepted_sync", 0)

cnt, lo, hi = (open(d + "/db-landed.txt").read().split() + ["NULL"] * 3)[:3]
cnt = int(cnt)
start = r.get("started_at")
st = datetime.datetime.fromisoformat(start.replace("Z", "+00:00")).timestamp() if start else float(meta["t_load_start"])
land = drain = 0.0
if cnt and hi != "NULL" and float(hi) > st:
    land = cnt / (float(hi) - st)
    drain = max(0.0, float(hi) - (st + last_accept_off))
nic = "n/a"
if meta.get("nic0", "n/a") != "n/a" and meta.get("nic1", "n/a") != "n/a":
    nic = round((float(meta["nic1"]) - float(meta["nic0"])) / 1e6, 2)

row = [meta["arch"], meta["replicas"], meta["mode"], meta["level"], meta["run"], meta.get("load_code", "?"),
       accepted, acc_rate, lat.get("p50_ms", 0), lat.get("p99_ms", 0), sync, backlogged, errs,
       cnt, accepted - cnt, round(land, 1), round(drain, 1), meta.get("quiet_wait_s", 0),
       round(svc, 2), round(load, 2), round(sum(ing.values()), 2), round(max(ing.values()) if ing else 0, 2),
       round(mysql.get(mysql_max_pod, 0), 2), mysql_max_pod, round(node_pct.get(node_max, 0), 1), node_max, nic]
print("\t".join(str(x) for x in row))
PY
  log "$(basename "$1"): $(tail -n 1 "$SUMMARY" | tr '\t' ' ')"
}

# ---------- 每种架构的部署和收尾 ----------
wait_ingress_ready() { # 主机名 路径
  local ok=0 url
  if [[ "$VIA" == ingress ]]; then url="http://$1$2"; else url="$CUR_URL$2"; fi
  for _ in $(seq 1 90); do
    if pexec wget -q -O /dev/null -T 3 "$url" >/dev/null 2>&1; then
      ok=$((ok + 1))
      [[ $ok -ge 3 ]] && break
    else
      ok=0
    fi
    sleep 2
  done
  [[ $ok -ge 3 ]] || die 2 "gateway not ready at $url after 3 minutes"
  log "gateway ready at $url"
}

record_placement() { # 标签
  {
    echo "# $1 $(date '+%Y-%m-%d %H:%M:%S %Z')"
    kubectl -n "$NS" get deploy -o wide 2>&1 || true
    kubectl get pods -A -o wide --sort-by=.spec.nodeName 2>&1 | grep -v -E 'Completed|backup-manual' || true
  } >"$OUT/placement-$1.txt"
}

setup_1a() {
  CUR_ARCH=1a
  CUR_REPLICAS=1
  if [[ "$VIA" == ingress ]]; then CUR_URL="http://$HOST_1A"; else CUR_URL="http://gateway-1a.$NS.svc:8080"; fi
  POD_MAIN="$(e2e_mysql_pod mysql-main primary)"
  POD_ORDER0="$(e2e_mysql_pod mysql-order-0 primary)"
  POD_ORDER1=""
  ORDER_TARGETS="$(order_1a_release):bench1a_order_db:orders"
  echo "==> 1a: databases"
  create_1a_dbs
  echo "==> 1a: deploy ($TAG_1A)"
  render_1a | kubectl -n "$NS" apply -f - >/dev/null
  # inventory 先就绪（它建表），再灌 SKU；其余三个并行起。
  local s
  for s in $SERVICES_1A; do
    kubectl -n "$NS" rollout status "deploy/$s-1a" --timeout 5m
  done
  printf '%s\n' "INSERT INTO bench1a_inventory_db.sku_stock (sku_id, name, unit_price, available) VALUES $SKU_VALUES;" |
    e2e_root_sql "$POD_MAIN"
  log "1a: skus $SKU_LIST, stock $STOCK each"
  wait_ingress_ready "$HOST_1A" /healthz
  record_placement 1a
}

teardown_1a() {
  echo "==> 1a: delete deployment and databases"
  kubectl -n "$NS" delete deploy,service,httproute -l "app.kubernetes.io/part-of=$PART_OF_1A" --wait --timeout 3m >/dev/null 2>&1 || true
  # 等 Pod 真的没了，1b 部署时不和它们抢资源。
  for _ in $(seq 1 60); do
    [[ -z "$(kubectl -n "$NS" get pods -l "app.kubernetes.io/part-of=$PART_OF_1A" -o name 2>/dev/null)" ]] && break
    sleep 2
  done
  drop_1a_dbs
}

setup_1b() {
  CUR_ARCH=1b
  CUR_REPLICAS="$REPLICAS_1B"
  if [[ "$VIA" == ingress ]]; then CUR_URL="http://$HOST_1B"; else CUR_URL="http://gateway.$NS.svc:8080"; fi
  POD_MAIN="$(e2e_mysql_pod mysql-main primary)"
  POD_ORDER0="$(e2e_mysql_pod mysql-order-0 primary)"
  POD_ORDER1="$(e2e_mysql_pod mysql-order-1 primary)"
  ORDER_TARGETS="mysql-order-0:e2e_order_db_0:$(echo "$SLOTS_DB0" | sed 's/[0-9]/orders_&/g; s/ /,/g') mysql-order-1:e2e_order_db_1:$(echo "$SLOTS_DB1" | sed 's/[0-9]/orders_&/g; s/ /,/g')"
  echo "==> 1b: databases"
  e2e_start_replicas
  e2e_create_dbs
  e2e_wait_replicated
  # values：e2e 那两层之上叠 bench/，BENCH_1B_REPLICAS=1 时再叠 bench-single/，设了 BENCH_1B_ADMIT_MAX_QUEUE 再叠一层临时生成的。
  local overlay="$ROOT/deploy/cluster/services/bench"
  if [[ "$REPLICAS_1B" == 1 ]]; then overlay="$overlay:$ROOT/deploy/cluster/services/bench-single"; fi
  if [[ -n "$ADMIT_MAX_QUEUE" ]]; then
    mkdir -p "$OUT/values-admit"
    printf '%s\n' "# 临时生成（scripts/cluster/bench.sh，BENCH_1B_ADMIT_MAX_QUEUE）。" "config:" "  ADMIT_MAX_QUEUE: \"$ADMIT_MAX_QUEUE\"" >"$OUT/values-admit/order.yaml"
    overlay="$overlay:$OUT/values-admit"
  fi
  echo "==> 1b: deploy (image tag $IMAGE_TAG, overlays ${overlay//$ROOT\//})"
  E2E_OVERLAY_DIR="$overlay" "$HERE/services.sh" e2e-up
  printf '%s\n' "INSERT INTO e2e_inventory_db.sku_stock (sku_id, name, unit_price, available) VALUES $SKU_VALUES;" |
    e2e_root_sql "$POD_MAIN"
  log "1b: skus $SKU_LIST, stock $STOCK each"
  # 直连 inventory 的地址随它的形态（MESH_ISTIO）变，e2e-lib.sh 的 e2e_inventory_addr。
  run_phase warmup-gate "$OUT" 120 load warmup -inventory-addr "$(e2e_inventory_addr)" -skus "$SKU_LIST" \
    -redis-addr "sentinel://gate@redis-gate-sentinel.redis.svc:26379" -redis-prefix "e2e-$RUN_ID:"
  [[ "$PHASE_CODE" == 0 ]] || die 1 "load warmup failed (see $OUT/warmup-gate.err)"
  wait_ingress_ready "$HOST_1B" /readyz
  record_placement 1b
}

# ---------- 收尾 ----------
# shellcheck disable=SC2329 # 由 EXIT 的 trap 调用
finish() {
  local code=$?
  trap - EXIT INT TERM
  set +e
  stop_sampler
  if [[ $code -ne 0 ]] && kubectl get namespace "$NS" >/dev/null 2>&1; then
    e2e_collect_logs "$NS" "$OUT/cluster"
  fi
  if [[ "${E2E_KEEP:-}" == 1 ]]; then
    e2e_mark_kept "$NS"
    echo "==> E2E_KEEP=1: keeping namespace $NS, the bench1a_* and e2e_* databases"
    echo "    clean up later with: scripts/cluster/bench.sh down"
  else
    echo "==> bench1a databases"
    drop_1a_dbs
    echo "==> e2e-down"
    "$HERE/services.sh" e2e-down
  fi
  if [[ $code -eq 0 ]]; then
    echo "==> cluster bench done (run $RUN_ID); output in $OUT"
  else
    echo "==> cluster bench FAILED (run $RUN_ID, exit $code); output in $OUT" >&2
  fi
  exit "$code"
}

# ---------- 1. 镜像 ----------
if [[ "${E2E_SKIP_IMAGES:-}" == 1 ]]; then
  [[ -n "${IMAGE_TAG:-}" ]] || die 2 "E2E_SKIP_IMAGES=1 needs IMAGE_TAG (the tag of the chaos and 1b images already in the registry)"
else
  IMAGE_TAG="${IMAGE_TAG:-$("$HERE/images.sh" tag)}"
  export IMAGE_TAG
  # 只跑 1a 时也要 chaos 镜像（发压）。
  if in_arch 1b; then
    # shellcheck disable=SC2086 # 镜像名按空格拆开是有意的
    "$HERE/images.sh" all $IMAGES_1B
  else
    "$HERE/images.sh" all chaos
  fi
fi
export IMAGE_TAG
if in_arch 1a; then ensure_1a_images; fi

# ---------- 2. 命名空间 ----------
if [[ -n "$(e2e_ns_state "$NS")" ]]; then
  e2e_check_free "$NS" || exit 3
  echo "==> namespace $NS exists (kept by an earlier E2E_KEEP=1 run?); tearing it down first"
  drop_1a_dbs
  "$HERE/services.sh" e2e-down
fi
e2e_claim_ns "$NS" bench "$RUN_ID" || exit 3
trap finish EXIT
trap 'exit 130' INT TERM

# ---------- 3. Secret、发压 Job ----------
"$HERE/services.sh" e2e-prepare
kubectl -n "$NS" create configmap bench-phase --from-file=phase.sh="$PHASE_SCRIPT" \
  --dry-run=client -o yaml | kubectl -n "$NS" apply -f - >/dev/null
render_job "$REGISTRY_ADDR/oppenheimer/chaos:$IMAGE_TAG" | kubectl -n "$NS" apply -f - >/dev/null
for _ in $(seq 1 90); do
  POD="$(kubectl -n "$NS" get pods -l "job-name=$JOB" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "$POD" && "$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)" == Running ]]; then
    break
  fi
  POD=""
  sleep 2
done
[[ -n "$POD" ]] || die 2 "load pod of $NS/$JOB is not running after 3 minutes (CPU requests $LOAD_CPU on $LOAD_NODE too high?)"
log "load pod $POD on $LOAD_NODE (cpu requests $LOAD_CPU)"

# ---------- 环境记录 ----------
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.capacity.cpu}{"\n"}{end}' >"$OUT/nodes-capacity.txt"
{
  echo "# cluster bench $RUN_ID, $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "repo $(git -C "$ROOT" rev-parse --short HEAD)$(if [[ -n "$(git -C "$ROOT" status --porcelain)" ]]; then echo " (dirty)"; fi)"
  echo "1b image tag ${IMAGE_TAG:-} (current branch code), 1a ref ${TAG_1A:-n/a} ($REF_1A)"
  echo "arches $ARCHES, mode $MODE, levels $LEVELS, repeat $REPEAT, duration $DURATION, warmup $WARMUP"
  echo "skus $SKU_COUNT (from 5001), stock $STOCK each, users $USERS, qty $QTY, -pay 0 -cancel 0"
  echo "via $VIA (1a $HOST_1A, 1b $HOST_1B -> ingress $LB_GATEWAY_IP; service = gateway ClusterIP)"
  echo "load pod on $LOAD_NODE, cpu requests $LOAD_CPU, memory requests $LOAD_MEMORY, no cpu limit"
  echo "replicas: 1a 1 per service (inventory, order, payment, gateway); 1b $REPLICAS_1B per service (gateway, order, inventory, payment, orderindex), cdc 1, no HPA"
  echo "1a databases: inventory and payment on mysql-main, order on $(order_1a_release); 1b: order on mysql-order-0/1 (2 shards), inventory and payment on mysql-main"
  echo "1b admission backlog limit: ${ADMIT_MAX_QUEUE:-service default (10000)}; rate limit off; PAYMENT_TIMEOUT 1h; HOLD_REAP_GRACE 1h; tracing off"
  echo "1b has an Envoy sidecar per pod, Kafka and Redis; 1a calls services directly over ClusterIP"
  echo "the long-running set in namespace oppenheimer shares MySQL, Kafka, Redis and the ingress"
  echo "nodes (vCPU):"
  sed 's/^/  /' "$OUT/nodes-capacity.txt"
} >"$OUT/conditions.txt"
tee -a "$EVENTS" <"$OUT/conditions.txt"

{
  printf 'arch\treplicas\tmode\tlevel\trun\tload_exit\taccepted\taccept_per_s\taccept_p50_ms\taccept_p99_ms\tsync_201\tbacklogged_429\terrors\t'
  printf 'landed_db\tunlanded\tland_db_per_s\tdrain_s\tquiet_wait_s\tcpu_svc\tcpu_load\tcpu_ingress\tcpu_ingress_max\tcpu_mysql_max\tmysql_max_pod\tnode_max_pct\tnode_max\tnic_mb\n'
} >"$SUMMARY"

# ---------- 4. 每种架构 ----------
for a in $ARCHES; do
  "setup_$a"
  if [[ "$WARMUP_S" -gt 0 ]]; then
    # shellcheck disable=SC2086
    set -- $LEVELS
    run_round "$1" 0 warmup
  fi
  for lvl in $LEVELS; do
    for n in $(seq 1 "$REPEAT"); do
      run_round "$lvl" "$n"
      sleep 5
    done
  done
  if [[ "$a" == 1a ]]; then teardown_1a; fi
done

# ---------- 5. 对比表 ----------
python3 -I - "$SUMMARY" "$OUT/conditions.txt" >"$OUT/compare.txt" <<'PY'
import sys
rows = [l.rstrip("\n").split("\t") for l in open(sys.argv[1])]
head, rows = rows[0], rows[1:]
ix = {k: i for i, k in enumerate(head)}
def f(r, k):
    try:
        return float(r[ix[k]])
    except Exception:
        return 0.0
groups = {}
for r in rows:
    groups.setdefault((r[ix["arch"]], r[ix["replicas"]], r[ix["mode"]], int(r[ix["level"]])), []).append(r)
print("== conditions ==")
print(open(sys.argv[2]).read().strip())
print()
print("== per level, mean of the runs (accept = 201 for 1a / 202 for 1b; land_db = orders in pending_payment per second, from the order dbs) ==")
cols = ["arch", "replicas", "mode", "level", "runs", "accept/s", "p50_ms", "p99_ms", "land_db/s", "drain_s", "unlanded", "429", "errors",
        "cpu_svc", "cpu_load", "cpu_ingress_max", "cpu_mysql_max", "node_max%", "nic_mb"]
print("\t".join(cols))
mean = {}
for k in sorted(groups):
    g = groups[k]
    m = lambda c: sum(f(r, c) for r in g) / len(g)
    mean[k] = {"accept": m("accept_per_s"), "land": m("land_db_per_s")}
    nic = [r[ix["nic_mb"]] for r in g]
    print("\t".join(str(x) for x in [k[0], k[1], k[2], k[3], len(g), round(m("accept_per_s"), 1), round(m("accept_p50_ms"), 1),
          round(m("accept_p99_ms"), 1), round(m("land_db_per_s"), 1), round(m("drain_s"), 1), int(m("unlanded")),
          int(m("backlogged_429")), int(m("errors")), round(m("cpu_svc"), 2), round(m("cpu_load"), 2),
          round(m("cpu_ingress_max"), 2), round(m("cpu_mysql_max"), 2), round(m("node_max_pct"), 1), "/".join(nic)]))
print()
print("== 1b vs 1a at the same level ==")
print("\t".join(["mode", "level", "1a accept/s", "1b accept/s", "1b/1a accept", "1b land_db/s", "1b/1a land"]))
for k in sorted(mean):
    if k[0] != "1b":
        continue
    a = [v for kk, v in mean.items() if kk[0] == "1a" and kk[2] == k[2] and kk[3] == k[3]]
    if not a:
        continue
    a = a[0]
    b = mean[k]
    ratio = lambda x, y: ("%.2fx" % (x / y)) if y else "n/a"
    print("\t".join(str(x) for x in [k[2], k[3], round(a["accept"], 1), round(b["accept"], 1), ratio(b["accept"], a["accept"]),
          round(b["land"], 1), ratio(b["land"], a["accept"])]))
print()
print("note: 1a runs 1 replica per service; 1b runs %s per service (see conditions). Results hold for these conditions only." %
      ",".join(sorted(set(k[1] for k in mean if k[0] == "1b"))) if any(k[0] == "1b" for k in mean) else "")
PY
{
  echo
  sed -n '/== per level/,$p' "$OUT/compare.txt" | column -t -s "$(printf '\t')"
} | tee -a "$EVENTS"
log "summary $SUMMARY, comparison $OUT/compare.txt"
