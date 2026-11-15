#!/usr/bin/env bash
# 在集群上做故障注入（make cluster-fault），验证 Istio 边车（设计文档 2d 的 3.4 和第 4 节，deploy/cluster/mesh/）上的
# 熔断（摘除）、并发上限、超时和不重试真的起作用。本机的对应物是 scripts/fault.sh（对本机进程、手写的 Envoy 边车），
# 场景编号和断言的写法尽量一致；集群上多了 B'（只挂住一个副本）、F（不重试）、G（重复摘除的时长）。
#
#   A  基线          查库存 20 次全 200；下一单并等它离开 creating；记 p50
#   B  下游全部挂住  两个 inventory 都 SIGSTOP，并发发几轮查库存：摘除之前的请求约 3 秒（2.5 到 4 秒）返回 503/504，
#                    之后的 500 毫秒内 503；两个 gateway 的边车里 inventory 的两个端点都出现过 FAILED；
#                    期间查订单全 200、延迟和基线相当；SIGCONT 之后 20 秒内查库存恢复。摘除前的 503、504 各多少个只打印
#   B' 只挂住一个    SIGSTOP 一个 inventory，持续成批发查库存直到一批里没有失败：坏的那个在两个 gateway 的边车里都出现过
#                    FAILED；之后连续 50 个请求全 200 且都在 500 毫秒内；打印从挂住到不再失败一共失败了几个请求
#   C  下游被杀      两个 inventory 强制删除（--grace-period=0 --force）：删除到恢复之间的失败都是 500 毫秒内的 503
#                    （删除后旧进程还活着时的 200 单独计数）；恢复是第一个失败之后连续 5 个 200 的第一个；
#                    新 Pod 就绪后 30 秒内恢复
#   D  并发上限      两个 inventory 都 SIGSTOP，同时发 400 个查库存：1 秒内被拒绝（503）的 >= 400 - 2×128，
#                    等到约 3 秒的 <= 256，没有一个超过 5 秒。边车上的溢出计数按 gateway 打印
#   E  业务错误      查不存在的 SKU 30 次：全 404，端点仍是 OK（摘除计数不增），之后查存在的 SKU 200
#   F  不重试        持续发查库存的同时在节点上 SIGKILL 两个 inventory 进程（边车还活着，会替它回 503，正是默认重试
#                    策略会重试的情况）。比三个数：MacBook 收到的响应数 N、gateway 进程交给边车的请求数 M、
#                    边车实际发往 inventory 的请求数 U。断言 M = N（gateway 进程自己不重试）、U + 本地拒绝 = M
#                    （边车没有多发）、upstream_rq_retry 增量为 0。FAULT_F_CONTROL=1 时再把路由的重试打开做一次对照
#   G  重复摘除      B 的“挂住、等摘除生效、恢复”连做 FAULT_G_CYCLES 次，只打印每次“那一批返回到摘除生效
#                    （第一个快速 503）”和“SIGCONT 到第一个 200”的时间
# 每个场景打印 PASS/FAIL 和量到的数，结束时打印结果表；任何一个 FAIL，退出码 1。
#
# 做的事，按顺序（同 scripts/cluster/chaos.sh，没有测试 Job：请求都从 MacBook 经入口发）：
#   1. 构建并推送六个服务的镜像（E2E_SKIP_IMAGES=1 跳过，这时必须给 IMAGE_TAG）
#   2. 占用命名空间 oppenheimer-e2e（和 e2e、chaos 共用，规则见 e2e-lib.sh 的“命名空间的占用”）
#   3. 建 e2e_* 库并授权；services.sh e2e-up 部署六个服务，values 在 e2e 那两层上
#      再叠 deploy/cluster/services/fault/（关限流、gateway 和 inventory 各 2 个副本、打开边车的统计项）
#   4. 灌 SKU 3001（库存很大），等网关经入口就绪
#   5. 依次跑场景；结束时（无论成败）先把挂住的进程全部 SIGCONT，失败时收日志，再 e2e-down（E2E_KEEP=1 保留现场）
#
# 怎么挂住一个进程：镜像是 distroless，容器里没有 shell，所以在节点上做。kubectl 取 Pod 所在节点和容器号，
# ssh 到节点用 crictl inspect 取进程号，sudo kill -STOP / -CONT（节点地址来自 cluster.env 的 NODES，用户 $VM_USER）。
# 挂住的进程不会自己恢复，所以三重保险：
#   - 每次挂住都同时起一个看门狗（后台的 sleep 加 ssh），FAULT_MAX_HOLD 秒（默认 18）后无条件 SIGCONT
#   - EXIT 的 trap（包括出错、Ctrl-C）把记下的进程全部 SIGCONT
#   - 记录先于信号：先把“要挂住谁”记下来再发 SIGSTOP，发到一半被中断也恢复得到
# 为什么是 18 秒：存活探针（chart 里写死：每 10 秒、超时 2 秒、连续 3 次失败）在挂住约 30 秒后会让 kubelet 重启容器，
# chart 没有放宽它的 value。所以每个场景挂住的时间都控制在 15 秒以内，看门狗是兜底。
# 就绪探针（每 3 秒、连续 2 次失败）在挂住约 4 到 8 秒后把 Pod 标成未就绪，Istio 随后可能把这个端点从 EDS 里拿掉
# （拿掉之后边车里就没有这个端点了，也就看不到 FAILED）。所以端点的状态由一个后台的观察者从挂住开始连续采样，
# 断言的是“采样期间出现过 FAILED”，完整的时间线在日志目录的 endpoints-*.txt 里。
#
# 环境变量：
#   FAULT_SCENARIOS    只跑哪些场景，空格分隔，默认 "A B B' C D E F G"（B' 也可以写成 Bp）。A 总是会跑：
#                      后面的场景要用它的订单和基线延迟。顺序固定按上面的顺序，和这里写的顺序无关
#   FAULT_SKIP_DEPLOY  设为 1 时不构建、不部署，复用上一次 E2E_KEEP=1 留下的现场（命名空间必须存在且不是 running），
#                      结束时不删命名空间（标回 kept）。调试断言用
#   FAULT_MAX_HOLD     看门狗的秒数，默认 18
#   FAULT_B_BURST      场景 B、G 第一批的并发数，默认 60（要让每个 gateway 对每个 inventory 各攒够 5 次失败）
#   FAULT_BP_BURST     场景 B' 每批的并发数，默认 60；FAULT_BP_ROUNDS 最多几批，默认 4
#   FAULT_D_N          场景 D 的并发数，默认 400
#   FAULT_F_SECS       场景 F 持续发请求的秒数，默认 15；在第 3 秒杀进程
#   FAULT_F_CONTROL    设为 1 时场景 F 再做一次对照：把 VirtualService inventory 的 from-gateway 路由改成重试 2 次，
#                      预期 upstream_rq_retry > 0；做完 kubectl apply deploy/cluster/mesh/inventory.yaml 改回来
#   FAULT_G_CYCLES     场景 G 的次数，默认 4
#   IMAGE_TAG、E2E_SKIP_IMAGES、E2E_KEEP、E2E_RUN_ID（默认当前 Unix 时间）、E2E_NAMESPACE（默认 oppenheimer-e2e）：同 chaos.sh
# 退出码：0 全部通过；1 有场景失败（脚本中途出错被 set -e 中止的也多半是 1，看输出区分）；2 参数或环境不对；3 命名空间被占用。
#
# 需要：MacBook 上有 python3、curl；能免密 ssh 到三个节点；bin/istio-<版本>/bin/istioctl（scripts/cluster/mesh.sh fetch）。
# 和长期运行的那一套共用 MySQL、Kafka、Redis（只是 e2e_ 前缀的库），不要同时做压测。
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
export E2E_OVERLAY_DIR="$ROOT/deploy/cluster/services/fault"
IMAGES="gateway order inventory payment orderindex cdc"
SKIP_DEPLOY="${FAULT_SKIP_DEPLOY:-0}"

MAX_HOLD="${FAULT_MAX_HOLD:-18}"
B_BURST="${FAULT_B_BURST:-60}"
BP_BURST="${FAULT_BP_BURST:-60}"
BP_ROUNDS="${FAULT_BP_ROUNDS:-4}"
D_N="${FAULT_D_N:-400}"
F_SECS="${FAULT_F_SECS:-15}"
F_CONTROL="${FAULT_F_CONTROL:-0}"
G_CYCLES="${FAULT_G_CYCLES:-4}"

# 和 deploy/cluster/mesh/inventory.yaml 一致：gateway 的每个 Pod 对 inventory 在途上限 128，路由超时 3 秒。
PER_GW_LIMIT=128
E2E_HOST="e2e.oppenheimer.test"
BASE_URL="http://$LB_GATEWAY_IP"
SKU=3001
MISSING_SKU=999999
USER_ID=7001
INV_CLUSTER="outbound|9082||inventory.$NS.svc.cluster.local"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=5)

for v in MAX_HOLD B_BURST BP_BURST BP_ROUNDS D_N F_SECS G_CYCLES; do
  case "${!v}" in
  '' | *[!0-9]* | 0)
    echo "$v must be a positive integer, got '${!v}'" >&2
    exit 2
    ;;
  esac
done
if [[ $F_SECS -lt 6 ]]; then
  echo "FAULT_F_SECS must be at least 6 (the kill happens at +3s), got $F_SECS" >&2
  exit 2
fi

# 场景名：B' 在环境变量里不好写，也接受 Bp、BP。内部一律叫 Bp。
SCENARIOS=""
for s in ${FAULT_SCENARIOS:-A B Bp C D E F G}; do
  case "$s" in
  "B'" | Bp | BP | bp) s=Bp ;;
  A | B | C | D | E | F | G) ;;
  *)
    echo "FAULT_SCENARIOS: unknown scenario '$s' (valid: A B B' C D E F G)" >&2
    exit 2
    ;;
  esac
  SCENARIOS="$SCENARIOS $s"
done
wanted() {
  case " $SCENARIOS " in
  *" $1 "*) return 0 ;;
  esac
  return 1
}

# ---------- 运行前检查（在占用命名空间之前，失败不留任何东西） ----------
for t in python3 curl ssh; do
  command -v "$t" >/dev/null || {
    echo "$t is required" >&2
    exit 2
  }
done
if [[ ! -x "$ROOT/bin/istio-$ISTIO_VERSION/bin/istioctl" ]]; then
  echo "istioctl not found in bin/istio-$ISTIO_VERSION; run scripts/cluster/mesh.sh fetch first" >&2
  exit 2
fi
# 三个节点都要能免密 ssh、免密 sudo（inventory 落在哪个节点事先不知道）。
for spec in "${NODES[@]}"; do
  n_name="${spec%%:*}"
  n_rest="${spec#*:}"
  n_ip="${n_rest%%:*}"
  if ! ssh "${SSH_OPTS[@]}" "$VM_USER@$n_ip" "sudo -n true" </dev/null >/dev/null 2>&1; then
    echo "cannot ssh to $VM_USER@$n_ip ($n_name) with passwordless sudo; the scenarios that freeze processes need it" >&2
    exit 2
  fi
done

if [[ "$SKIP_DEPLOY" == 1 ]]; then
  st="$(e2e_ns_state "$NS")"
  if [[ -z "$st" ]]; then
    echo "FAULT_SKIP_DEPLOY=1 but namespace $NS does not exist; run once with E2E_KEEP=1 first" >&2
    exit 2
  fi
  e2e_check_free "$NS" || exit 3
  if [[ "${st%% *}" != fault ]]; then
    echo "==> WARNING: namespace $NS was left by a '${st%% *}' run, not fault; the overlay (rate limit off, replicas, stats) may be missing" >&2
  fi
  # 运行号沿用部署时的（日志目录、Kafka/Redis 前缀都跟着它）。
  E2E_RUN_ID="$(kubectl get namespace "$NS" -o jsonpath='{.metadata.labels.oppenheimer\.test/run-id}' 2>/dev/null || true)"
  E2E_RUN_ID="${E2E_RUN_ID:-$(date +%s)}"
else
  e2e_check_free "$NS" || exit 3
fi
export E2E_RUN_ID="${E2E_RUN_ID:-$(date +%s)}"

# 日志目录按这一次运行的时间分（FAULT_SKIP_DEPLOY 时同一个运行号会跑很多次）。
LOG_DIR="$ROOT/tmp/cluster-fault-logs/$E2E_RUN_ID-$(date +%H%M%S)"
mkdir -p "$LOG_DIR/stats"
ln -sfn "$(basename "$LOG_DIR")" "$ROOT/tmp/cluster-fault-logs/latest"
EVENTS="$LOG_DIR/fault-events.log"
PY="$LOG_DIR/fault_http.py"

log() { echo "[fault $(date +%H:%M:%S)] $*" | tee -a "$EVENTS"; }
die() { # 退出码 消息
  local code="$1"
  shift
  echo "$*" >&2
  exit "$code"
}

# ---------- HTTP：一个小的 python3 程序（只用标准库） ----------
# 并发和计时都在 python 里做：400 个 curl 进程同时起来会把 MacBook 拖慢，计时就不准了；线程加一个栅栏，
# 所有请求在同一刻发出。每个请求一个新连接（同 curl），经入口的地址，Host 头指向 e2e 的主机名。
# 输出每行一个请求：“开始时刻（Unix 毫秒） 状态码 耗时毫秒”，连接失败、超时的状态码是 000（原因打到标准错误）。
cat >"$PY" <<'PYEOF'
import http.client, os, resource, sys, threading, time

ADDR = os.environ["FAULT_ADDR"]
HOST = os.environ["FAULT_HOST"]


def raise_nofile(n):
    # macOS 的 shell 默认只能开 256 个文件，400 个并发连接不够。
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        want = n + 64
        if hard != resource.RLIM_INFINITY:
            want = min(want, hard)
        want = min(want, 10240)
        if soft == resource.RLIM_INFINITY or soft >= want:
            return
        resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
    except (ValueError, OSError) as e:
        sys.stderr.write("raising RLIMIT_NOFILE failed: %s\n" % e)


def req(path, user, timeout=10.0):
    t0 = time.time()
    code = "000"
    try:
        c = http.client.HTTPConnection(ADDR, 80, timeout=timeout)
        c.request("GET", path, headers={"Host": HOST, "X-User-Id": str(user), "Connection": "close"})
        r = c.getresponse()
        r.read()
        code = str(r.status)
        c.close()
    except Exception as e:  # noqa: BLE001 记下来，不中断
        sys.stderr.write("GET %s: %s: %s\n" % (path, type(e).__name__, e))
    t1 = time.time()
    return "%d %s %d" % (int(t0 * 1000), code, int((t1 - t0) * 1000 + 0.5))


def burst(n, path, user_base):
    raise_nofile(n)
    out = [None] * n
    bar = threading.Barrier(n)

    def run(i):
        try:
            bar.wait(timeout=30)
        except threading.BrokenBarrierError:
            pass
        out[i] = req(path, user_base + i)

    ts = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    for line in sorted(out):
        print(line)


def seq(n, path, user):
    for _ in range(n):
        print(req(path, user), flush=True)


def poll(path, max_s, need_ok, user, fail_first=0):
    # 顺序发，直到连续 need_ok 个 200 或者超时。退出码 0 成功、1 超时。
    # fail_first=1 时只数第一个失败之后的 200（场景 C：强制删除之后旧进程还会活一小会儿，最早的几个仍是 200）。
    deadline = time.time() + max_s
    ok = 0
    failed = not fail_first
    while time.time() < deadline:
        line = req(path, user, timeout=min(10.0, max(1.0, deadline - time.time())))
        print(line, flush=True)
        if line.split()[1] == "200":
            ok = ok + 1 if failed else 0
        else:
            failed = True
            ok = 0
        if ok >= need_ok:
            return 0
        time.sleep(0.1)
    return 1


def until_fast(path, max_s, need, user):
    # 每 200 毫秒发一个（按开始时刻算，慢的那个返回后立刻发下一个），直到连续 need 个“500 毫秒内的 503”
    # （摘除已经生效）或者超过 max_s 秒不再发新的。退出码 0 等到了、1 没等到。
    deadline = time.time() + max_s
    n = 0
    while time.time() < deadline:
        t = time.time()
        line = req(path, user)
        print(line, flush=True)
        _, code, ms = line.split()
        n = n + 1 if code == "503" and int(ms) < 500 else 0
        if n >= need:
            return 0
        time.sleep(max(0.0, 0.2 - (time.time() - t)))
    return 1


def stream(secs, conc, path, user_base):
    raise_nofile(conc)
    deadline = time.time() + secs
    lock = threading.Lock()

    def run(i):
        while time.time() < deadline:
            line = req(path, user_base + i)
            with lock:
                print(line, flush=True)
            time.sleep(0.05)

    ts = [threading.Thread(target=run, args=(i,)) for i in range(conc)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()


def main():
    cmd, a = sys.argv[1], sys.argv[2:]
    if cmd == "burst":
        burst(int(a[0]), a[1], int(a[2]))
    elif cmd == "seq":
        seq(int(a[0]), a[1], int(a[2]))
    elif cmd == "poll":
        sys.exit(poll(a[0], float(a[1]), int(a[2]), int(a[3]), int(a[4]) if len(a) > 4 else 0))
    elif cmd == "until_fast":
        sys.exit(until_fast(a[0], float(a[1]), int(a[2]), int(a[3])))
    elif cmd == "stream":
        stream(float(a[0]), int(a[1]), a[2], int(a[3]))
    else:
        sys.exit("unknown command " + cmd)


main()
PYEOF
export FAULT_ADDR="$LB_GATEWAY_IP" FAULT_HOST="$E2E_HOST"

# http <命令> 参数...：标准错误（连接失败的原因）追加到 http-errors.log。
http() { python3 -I "$PY" "$@" 2>>"$LOG_DIR/http-errors.log"; }
now_ms() { python3 -I -S -c 'import time; print(int(time.time()*1000))'; }
stock_path() { echo "/api/v1/skus/$1/stock"; }

# 结果文件（每行“开始 状态码 毫秒”）的几个统计。
count_lines() { wc -l <"$1" | tr -d ' '; }
count_not() { awk -v c="$2" '$2 != c' "$1" | wc -l | tr -d ' '; }
# 状态码的分布，如 "200x18 503x2"。
code_hist() { awk '{n[$2]++} END {for (c in n) printf "%s%sx%d", (s++ ? " " : ""), c, n[c]; print ""}' "$1"; }
# percentile <p> <文件>：耗时列的第 p 百分位（最近秩法）。
percentile() { awk '{print $3}' "$2" | sort -n | awk -v p="$1" '{a[NR] = $1} END {i = int((p * NR + 99) / 100); if (i < 1) i = 1; print a[i] + 0}'; }
max_ms() { awk 'BEGIN {m = 0} $3 > m {m = $3} END {print m}' "$1"; }

# ---------- Pod ----------
# 就绪、没有在终止的 gateway 和 inventory Pod。INV_TABLE 每行“名字 IP 节点”。
GW_PODS=""
INV_PODS=""
INV_IPS=""
INV_TABLE=""
refresh_pods() {
  local out app name del ready ip node
  GW_PODS=""
  INV_PODS=""
  INV_IPS=""
  INV_TABLE=""
  out="$(kubectl -n "$NS" get pods -l 'app.kubernetes.io/name in (gateway,inventory)' -o jsonpath='{range .items[*]}{.metadata.labels.app\.kubernetes\.io/name}{"|"}{.metadata.name}{"|"}{.metadata.deletionTimestamp}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"|"}{.status.podIP}{"|"}{.spec.nodeName}{"\n"}{end}' 2>/dev/null)" || return 0
  while IFS='|' read -r app name del ready ip node; do
    [[ -n "$name" && -z "$del" && "$ready" == True ]] || continue
    case "$app" in
    gateway) GW_PODS="$GW_PODS $name" ;;
    inventory)
      INV_PODS="$INV_PODS $name"
      INV_IPS="$INV_IPS $ip"
      INV_TABLE="$INV_TABLE$name $ip $node
"
      ;;
    esac
  done <<<"$out"
}
words() { # 按空格数个数
  # shellcheck disable=SC2086
  set -- $1
  echo $#
}
inv_ip() { printf '%s' "$INV_TABLE" | awk -v p="$1" '$1 == p {print $2}'; }
first_word() {
  # shellcheck disable=SC2086
  set -- $1
  echo "${1:-}"
}

# ---------- 节点上的进程：挂住、恢复、杀掉 ----------
node_ip() { # 节点名
  local spec rest
  for spec in "${NODES[@]}"; do
    if [[ "${spec%%:*}" == "$1" ]]; then
      rest="${spec#*:}"
      echo "${rest%%:*}"
      return 0
    fi
  done
  return 1
}
rsh() { # 地址 远端命令
  local ip="$1"
  shift
  # shellcheck disable=SC2029 # 远端命令里的进程号、容器号就是要在本机展开
  ssh "${SSH_OPTS[@]}" "$VM_USER@$ip" "$@" </dev/null
}

# 某个 inventory Pod 里 inventory 容器的进程：结果放在 RP_IP（节点地址）、RP_PID。每次现取：容器重启过进程号就变了。
RP_IP=""
RP_PID=""
resolve_proc() { # Pod
  local node cid
  RP_IP=""
  RP_PID=""
  node="$(kubectl -n "$NS" get pod "$1" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
  cid="$(kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.containerStatuses[?(@.name=="inventory")].containerID}' 2>/dev/null || true)"
  cid="${cid#containerd://}"
  if [[ -z "$node" || -z "$cid" ]]; then
    log "ERROR: pod $1: node '$node', container id '$cid' (pod gone or container not started?)"
    return 1
  fi
  RP_IP="$(node_ip "$node")" || {
    log "ERROR: node $node of pod $1 is not in cluster.env NODES"
    return 1
  }
  RP_PID="$(rsh "$RP_IP" "sudo k3s crictl inspect -o go-template --template '{{.info.pid}}' $cid" 2>>"$EVENTS" | tr -d '\r\n ' || true)"
  case "$RP_PID" in
  '' | *[!0-9]* | 0 | 1)
    log "ERROR: pod $1: could not get the pid of container $cid on $node ($RP_IP), got '$RP_PID'"
    return 1
    ;;
  esac
}

# 挂住的进程，每项“Pod:节点地址:进程号”。先记下再发信号（发到一半被中断也恢复得到）。
HUNG=""
HANG_T0=0
WATCHDOGS=""
hang() { # Pod...
  local p e entries="" pids="" pid ip rc=0
  for p in "$@"; do
    resolve_proc "$p" || return 1
    entries="$entries $p:$RP_IP:$RP_PID"
  done
  HUNG="$HUNG$entries"
  # 几个 Pod 的 SIGSTOP 并行发，挂住的时刻尽量一致。
  for e in $entries; do
    ip="${e#*:}"
    pid="${ip#*:}"
    ip="${ip%%:*}"
    rsh "$ip" "sudo kill -STOP $pid" >>"$EVENTS" 2>&1 &
    pids="$pids $!"
  done
  for pid in $pids; do wait "$pid" || rc=1; done
  HANG_T0="$(now_ms)"
  # 看门狗：到时间无条件 SIGCONT。它自己是后台的子 shell，不继承 EXIT 的 trap。
  # 被 resume_all 取消时收到 TERM：自己的 trap 杀掉 sleep 并以 0 退出。sleep 放到后台再 wait，
  # 不让 bash 对“前台的 sleep 被信号杀掉”打印 Terminated 的作业提示。
  (
    sp=""
    trap 'if [[ -n "$sp" ]]; then kill "$sp" 2>/dev/null; fi; exit 0' TERM
    sleep "$MAX_HOLD" &
    sp=$!
    wait "$sp"
    for e in $entries; do
      ip="${e#*:}"
      pid="${ip#*:}"
      ip="${ip%%:*}"
      rsh "$ip" "sudo kill -CONT $pid" >/dev/null 2>&1 || true
    done
    echo "[fault $(date +%H:%M:%S)] WATCHDOG: sent SIGCONT after ${MAX_HOLD}s to$entries (the scenario held them too long)" | tee -a "$EVENTS" >&2
  ) &
  WATCHDOGS="$WATCHDOGS $!"
  log "  SIGSTOP$entries (pod:node:pid)"
  if [[ $rc -ne 0 ]]; then
    log "ERROR: sending SIGSTOP failed for some of$entries (see $EVENTS)"
    return 1
  fi
}

# 全部恢复。每个进程恢复后打印它的状态（ps 的 STAT：T 是还停着，S/R 是在跑），排查时用。
resume_all() {
  local e ip pid pids="" w held
  [[ -n "${HUNG// /}" ]] || return 0
  held=$(($(now_ms) - HANG_T0))
  for e in $HUNG; do
    ip="${e#*:}"
    pid="${ip#*:}"
    ip="${ip%%:*}"
    (
      st="$(rsh "$ip" "sudo kill -CONT $pid && ps -o stat= -p $pid" 2>&1 | tr -d '\r\n')" || st="FAILED: $st"
      echo "[fault $(date +%H:%M:%S)]   SIGCONT ${e%%:*} (pid $pid on $ip): state '$st'" | tee -a "$EVENTS"
    ) &
    pids="$pids $!"
  done
  for pid in $pids; do wait "$pid" || true; done
  HUNG=""
  for w in $WATCHDOGS; do
    kill "$w" 2>/dev/null || true
    wait "$w" 2>/dev/null || true
  done
  WATCHDOGS=""
  log "  resumed after ${held}ms held"
}

kill_procs() { # Pod...
  local p e ip pid pids="" entries=""
  for p in "$@"; do
    resolve_proc "$p" || return 1
    entries="$entries $p:$RP_IP:$RP_PID"
  done
  for e in $entries; do
    ip="${e#*:}"
    pid="${ip#*:}"
    ip="${ip%%:*}"
    rsh "$ip" "sudo kill -KILL $pid" >>"$EVENTS" 2>&1 &
    pids="$pids $!"
  done
  for pid in $pids; do wait "$pid" || true; done
  log "  SIGKILL$entries (pod:node:pid)"
}

# ---------- 边车：端点状态和统计 ----------
ISTIOCTL=("$HERE/mesh.sh" istioctl)

# 某个 gateway 的边车里 inventory 的端点，每行“IP 状态 摘除检查”（HEALTHY OK、UNHEALTHY FAILED 这样）。
# istioctl 的输出：ENDPOINT STATUS OUTLIER CHECK CLUSTER，第一行是表头。取不到时输出一行“- ERROR -”，原文存档。
ep_snapshot() { # gateway Pod
  local raw
  raw="$("${ISTIOCTL[@]}" proxy-config endpoints "$1.$NS" --cluster "$INV_CLUSTER" 2>&1)" || {
    printf '%s\n' "$raw" >>"$LOG_DIR/istioctl-errors.log"
    echo "- ERROR -"
    return 0
  }
  printf '%s\n' "$raw" | awk 'NR > 1 && $1 ~ /:9082$/ {sub(/:9082$/, "", $1); print $1, $2, $3}'
}

# 所有 gateway 并行取一次，每行“gateway IP 状态 摘除检查”。
ep_all() {
  local gw pids="" pid tmp="$LOG_DIR/.ep.$$"
  for gw in $GW_PODS; do
    ep_snapshot "$gw" | sed "s/^/$gw /" >"$tmp.$gw" &
    pids="$pids $!"
  done
  for pid in $pids; do wait "$pid" || true; done
  for gw in $GW_PODS; do
    cat "$tmp.$gw" 2>/dev/null || true
    command rm -f "$tmp.$gw"
  done
}

# 后台连续采样 <秒数>，每行“Unix 毫秒 gateway IP 状态 摘除检查”追加到文件。PID 在 EP_WATCH_PID。
EP_WATCH_PID=""
ep_watch_start() { # 秒数 文件
  local secs="$1" file="$2"
  (
    end=$(($(date +%s) + secs))
    while [[ $(date +%s) -lt $end ]]; do
      t="$(now_ms)"
      ep_all | sed "s/^/$t /" >>"$file"
      sleep 0.3
    done
  ) &
  EP_WATCH_PID=$!
}
ep_watch_stop() {
  if [[ -n "$EP_WATCH_PID" ]]; then
    kill "$EP_WATCH_PID" 2>/dev/null || true
    wait "$EP_WATCH_PID" 2>/dev/null || true
    EP_WATCH_PID=""
  fi
}
# 采样文件里某个 gateway 看某个 IP 第一次出现某个摘除检查值的时刻（Unix 毫秒），没出现过输出空。
ep_first() { awk -v g="$2" -v ip="$3" -v v="$4" '$2 == g && $3 == ip && $5 == v {print $1; exit}' "$1"; }
# 某个 gateway 看某个 IP 的状态序列（去掉连续重复），如 "HEALTHY/OK UNHEALTHY/FAILED (absent)"，排查时看。
ep_timeline() {
  awk -v g="$2" -v ip="$3" '
    $2 == g { seen[$1] = 1; if ($3 == ip) st[$1] = $4 "/" $5 }
    END {
      n = 0; for (t in seen) ts[++n] = t
      for (i = 1; i <= n; i++) for (j = i + 1; j <= n; j++) if (ts[j] < ts[i]) { x = ts[i]; ts[i] = ts[j]; ts[j] = x }
      last = ""; out = ""
      for (i = 1; i <= n; i++) { s = (ts[i] in st) ? st[ts[i]] : "(absent)"; if (s != last) { out = out (out ? " " : "") s; last = s } }
      print out
    }' "$1"
}

# 统计：每个 gateway 的边车 /stats 存一份，文件名“标签.Pod.txt”。
snap_stats() { # 标签
  local gw pids="" pid
  for gw in $GW_PODS; do
    kubectl -n "$NS" exec "$gw" -c istio-proxy -- pilot-agent request GET stats >"$LOG_DIR/stats/$1.$gw.txt" 2>&1 &
    pids="$pids $!"
  done
  for pid in $pids; do wait "$pid" || true; done
}
# 一个统计项的值，没有这一项输出 NA。种类：
#   up <后缀>    cluster.<到 inventory 的集群>.<后缀>：边车发往 inventory 的
#   down <后缀>  http.outbound_...9082....<后缀>：gateway 进程交给边车的（出站监听的 HCM，可能不止一项，求和）
stat_one() { # 文件 种类 后缀
  # Istio 的统计项名字在标签和后缀之间多一个分号（cluster.outbound|9082||inventory.<ns>.svc.cluster.local;.upstream_rq_total），
  # 先去掉再比。第一次运行所有统计都是 NA，就是因为它。
  awk -F': ' -v kind="$2" -v s="$3" -v cl="cluster.$INV_CLUSTER.$3" '
    {sub(/;\./, ".", $1)}
    kind == "up" && $1 == cl {v += $2; f = 1}
    kind == "down" && index($1, "http.outbound_") == 1 && index($1, "9082") > 0 &&
      substr($1, length($1) - length(s)) == "." s {v += $2; f = 1}
    END {if (f) print v + 0; else print "NA"}' "$1" 2>/dev/null || echo NA
}
# 两个标签之间某项的增量。给了 Pod 就只算它，否则对所有 gateway 求和；任何一个 NA 就是 NA。
stat_delta() { # 标签0 标签1 种类 后缀 [Pod]
  local gw a b sum=0 list="${5:-$GW_PODS}"
  for gw in $list; do
    a="$(stat_one "$LOG_DIR/stats/$1.$gw.txt" "$3" "$4")"
    b="$(stat_one "$LOG_DIR/stats/$2.$gw.txt" "$3" "$4")"
    if [[ "$a" == NA || "$b" == NA ]]; then
      echo NA
      return 0
    fi
    sum=$((sum + b - a))
  done
  echo "$sum"
}

# ---------- 场景的记账（同本机 scripts/fault.sh） ----------
FAILED=0
SC_FAIL=0
SC_NOTE=""
RESULTS=""
expect() { # 说明 命令...
  local desc="$1"
  shift
  if "$@"; then
    log "  ok    $desc"
  else
    log "  FAIL  $desc"
    SC_FAIL=1
  fi
}
begin() {
  SC_FAIL=0
  SC_NOTE=""
  echo
  log "== scenario $1: $2"
}
end_scenario() { # 名字 标题
  local res=PASS
  if [[ $SC_FAIL -ne 0 ]]; then
    res=FAIL
    FAILED=1
  fi
  log "== scenario $1 $res: $SC_NOTE"
  RESULTS="$RESULTS$1|$2|$res|$SC_NOTE
"
}

# 场景开始前等环境干净：gateway、inventory 各 2 个就绪的 Pod；每个 gateway 的边车里恰好是这两个 inventory 的 IP，
# 都是 HEALTHY、OK（上一个场景的摘除期已过；摘除时长逐次加长，所以轮询，不 sleep 固定值）；查库存 200。
CLEAN_TIMEOUT=150
env_clean() {
  local deadline=$((SECONDS + CLEAN_TIMEOUT)) bad snap gw ip line n next_report=$((SECONDS + 15)) t0=$SECONDS
  while :; do
    refresh_pods
    bad=""
    n="$(words "$GW_PODS")"
    [[ "$n" == 2 ]] || bad="$bad gateway-ready-pods=$n"
    n="$(words "$INV_PODS")"
    [[ "$n" == 2 ]] || bad="$bad inventory-ready-pods=$n"
    if [[ -z "$bad" ]]; then
      snap="$(ep_all)"
      for gw in $GW_PODS; do
        n="$(printf '%s\n' "$snap" | awk -v g="$gw" '$1 == g' | wc -l | tr -d ' ')"
        [[ "$n" == 2 ]] || bad="$bad $gw:endpoints=$n"
        for ip in $INV_IPS; do
          line="$(printf '%s\n' "$snap" | awk -v g="$gw" -v ip="$ip" '$1 == g && $2 == ip {print $3 "/" $4}')"
          [[ "$line" == HEALTHY/OK ]] || bad="$bad $gw:$ip=${line:-absent}"
        done
      done
    fi
    if [[ -z "$bad" ]]; then
      line="$(http seq 1 "$(stock_path "$SKU")" "$USER_ID")"
      if [[ "$(echo "$line" | awk '{print $2}')" == 200 ]]; then
        log "  environment clean after $((SECONDS - t0))s (gateway:$GW_PODS; inventory:$INV_PODS /$INV_IPS)"
        return 0
      fi
      bad="stock-probe=$line"
    fi
    if [[ $SECONDS -ge $deadline ]]; then
      log "  environment NOT clean after ${CLEAN_TIMEOUT}s:$bad"
      kubectl -n "$NS" get pods -o wide 2>&1 | tee -a "$EVENTS" || true
      printf '%s\n' "${snap:-}" | sed 's/^/    endpoints: /' | tee -a "$EVENTS" || true
      return 1
    fi
    if [[ $SECONDS -ge $next_report ]]; then
      log "  waiting for a clean environment ($((SECONDS - t0))s):$bad"
      next_report=$((SECONDS + 15))
    fi
    sleep 1
  done
}

# ---------- 收尾 ----------
MESH_DIRTY=0
READY_PID=""
EP_DETAIL=""
# shellcheck disable=SC2329 # 由 EXIT 的 trap 调用
on_exit() {
  local code=$?
  trap - EXIT INT TERM
  set +e
  # 第一件事：挂住的进程全部恢复。
  resume_all
  ep_watch_stop
  if [[ -n "$READY_PID" ]]; then kill "$READY_PID" 2>/dev/null || true; fi
  if [[ $MESH_DIRTY -eq 1 ]]; then
    echo "==> restoring the inventory traffic rules (FAULT_F_CONTROL changed them)"
    kubectl apply -n "$NS" -f "$ROOT/deploy/cluster/mesh/inventory.yaml" >/dev/null ||
      echo "    WARNING: restoring failed; run: kubectl apply -n $NS -f deploy/cluster/mesh/inventory.yaml" >&2
  fi
  if [[ ($code -ne 0 || $FAILED -ne 0) ]] && kubectl get namespace "$NS" >/dev/null 2>&1; then
    e2e_collect_logs "$NS" "$LOG_DIR/cluster"
  fi
  if [[ "$SKIP_DEPLOY" == 1 || "${E2E_KEEP:-}" == 1 ]]; then
    e2e_mark_kept "$NS"
    echo "==> keeping namespace $NS (FAULT_SKIP_DEPLOY=1 or E2E_KEEP=1); rerun with FAULT_SKIP_DEPLOY=1"
    echo "    clean up later with: scripts/cluster/services.sh e2e-down"
  else
    echo "==> e2e-down"
    "$HERE/services.sh" e2e-down
  fi
  if [[ $code -eq 0 ]]; then
    echo "==> cluster fault PASSED (run $E2E_RUN_ID); logs in $LOG_DIR"
  else
    echo "==> cluster fault FAILED (run $E2E_RUN_ID, exit $code); logs in $LOG_DIR" >&2
  fi
  exit "$code"
}

# ---------- 1 到 4：部署（FAULT_SKIP_DEPLOY=1 时跳过） ----------
if [[ "$SKIP_DEPLOY" == 1 ]]; then
  e2e_mark_running "$NS" || die 2 "could not mark namespace $NS as running"
  trap on_exit EXIT
  trap 'exit 130' INT TERM
  log "cluster fault: reusing namespace $NS (run $E2E_RUN_ID), logs $LOG_DIR"
else
  if [[ "${E2E_SKIP_IMAGES:-}" == 1 ]]; then
    [[ -n "${IMAGE_TAG:-}" ]] || die 2 "E2E_SKIP_IMAGES=1 needs IMAGE_TAG (the tag of images already in the registry)"
  else
    IMAGE_TAG="${IMAGE_TAG:-$("$HERE/images.sh" tag)}"
    export IMAGE_TAG
    # shellcheck disable=SC2086 # 镜像名按空格拆开是有意的
    "$HERE/images.sh" all $IMAGES
  fi
  export IMAGE_TAG
  log "cluster fault: run $E2E_RUN_ID, image tag $IMAGE_TAG, namespace $NS, logs $LOG_DIR"

  if [[ -n "$(e2e_ns_state "$NS")" ]]; then
    e2e_check_free "$NS" || exit 3
    echo "==> namespace $NS exists (kept by an earlier E2E_KEEP=1 run?); tearing it down first"
    "$HERE/services.sh" e2e-down
  fi
  e2e_claim_ns "$NS" fault "$E2E_RUN_ID" || exit 3
  trap on_exit EXIT
  trap 'exit 130' INT TERM

  echo "==> mysql: replicas, e2e databases and grants"
  e2e_start_replicas
  e2e_create_dbs
  e2e_wait_replicated

  "$HERE/services.sh" e2e-up

  # inventory 已经就绪，表已经迁移好了。闸门里的库存由 inventory 第一次用到这个 SKU 时从 MySQL 装载（seed.sh 的说明）。
  printf '%s\n' "INSERT INTO e2e_inventory_db.sku_stock (sku_id, name, unit_price, available, purchase_limit)
VALUES ($SKU, 'fault $SKU', 100, 100000000, 0) ON DUPLICATE KEY UPDATE name = VALUES(name);" |
    e2e_root_sql "$(e2e_mysql_pod mysql-main primary)"
  log "sku $SKU seeded"
fi

# 网关经入口连续 3 次就绪（同 chaos.sh：入口刚挂上路由时可能时好时坏）。
ok=0
for _ in $(seq 1 90); do
  if [[ "$(curl -s -m 3 -o /dev/null -w '%{http_code}' -H "Host: $E2E_HOST" "$BASE_URL/readyz" || true)" == 200 ]]; then
    ok=$((ok + 1))
    [[ $ok -ge 3 ]] && break
  else
    ok=0
  fi
  sleep 2
done
[[ $ok -ge 3 ]] || die 2 "gateway not ready through the ingress ($BASE_URL/readyz, Host $E2E_HOST) after 3 minutes"
log "gateway ready through the ingress"

refresh_pods
[[ "$(words "$GW_PODS")" == 2 && "$(words "$INV_PODS")" == 2 ]] ||
  die 2 "need 2 ready gateway and 2 ready inventory pods, have gateway:$GW_PODS inventory:$INV_PODS (is the fault overlay applied?)"

# 生成出来的配置存档：路由上的超时、重试，集群上的并发上限、摘除。断言失败时先看这几份。
GW0="$(first_word "$GW_PODS")"
"${ISTIOCTL[@]}" proxy-config routes "$GW0.$NS" --name 9082 -o json >"$LOG_DIR/gateway-routes-9082.json" 2>&1 || true
"${ISTIOCTL[@]}" proxy-config cluster "$GW0.$NS" --fqdn "inventory.$NS.svc.cluster.local" -o json >"$LOG_DIR/gateway-cluster-inventory.json" 2>&1 || true
log "generated config of $GW0 saved: gateway-routes-9082.json (retryPolicy lines: $(grep -c retryPolicy "$LOG_DIR/gateway-routes-9082.json" || true)), gateway-cluster-inventory.json"

# 统计项打开了没有（fault/gateway.yaml 的 proxyStatsMatcher）。没打开的话 D、F 的统计会是 NA，F 判失败。
snap_stats probe
STATS_OK=1
for gw in $GW_PODS; do
  if [[ "$(stat_one "$LOG_DIR/stats/probe.$gw.txt" up upstream_rq_total)" == NA ||
    "$(stat_one "$LOG_DIR/stats/probe.$gw.txt" down downstream_rq_total)" == NA ]]; then
    STATS_OK=0
  fi
done
if [[ $STATS_OK -eq 1 ]]; then
  log "sidecar stats available (upstream_rq_total, downstream_rq_total on both gateways)"
else
  log "WARNING: sidecar stats for $INV_CLUSTER or http.outbound_*9082* missing on some gateway (proxyStatsMatcher not applied?);"
  log "         see $LOG_DIR/stats/probe.*.txt; D prints NA, F fails"
fi

# ---------- 场景 A：基线 ----------
BASE_ORDER=""
ORDER_P50=1
begin A "baseline"
OUT="$LOG_DIR/a-stock.txt"
http seq 20 "$(stock_path "$SKU")" "$USER_ID" >"$OUT"
BASE_P50="$(percentile 50 "$OUT")"
bad="$(count_not "$OUT" 200)"
expect "20 stock queries all 200 (codes: $(code_hist "$OUT"))" [ "$bad" = 0 ]
body="$LOG_DIR/a-submit.json"
sub_code="$(curl -s -m 10 -o "$body" -w '%{http_code}' -X POST -H "Host: $E2E_HOST" -H "X-User-Id: $USER_ID" \
  -H "Idempotency-Key: fault-$E2E_RUN_ID-$RANDOM-$RANDOM" -H 'Content-Type: application/json' \
  -d "{\"items\":[{\"sku_id\":\"$SKU\",\"quantity\":1}]}" "$BASE_URL/api/v1/orders" || true)"
BASE_ORDER="$(sed -n 's/.*"order_id" *: *"\([0-9]*\)".*/\1/p' "$body" 2>/dev/null | head -n 1 || true)"
expect "create order accepted (got $sub_code, id ${BASE_ORDER:-none})" [ "$sub_code" = 202 -a -n "$BASE_ORDER" ]
state=""
if [[ -n "$BASE_ORDER" ]]; then
  for _ in $(seq 1 60); do
    state="$(curl -s -m 5 -H "Host: $E2E_HOST" -H "X-User-Id: $USER_ID" "$BASE_URL/api/v1/orders/$BASE_ORDER" |
      sed -n 's/.*"status" *: *"\([a-z_]*\)".*/\1/p' | head -n 1 || true)"
    [[ -n "$state" && "$state" != creating ]] && break
    sleep 0.5
  done
fi
expect "order left creating within 30s (state ${state:-none})" [ -n "$state" -a "$state" != creating ]
if [[ -n "$BASE_ORDER" ]]; then
  http seq 10 "/api/v1/orders/$BASE_ORDER" "$USER_ID" >"$LOG_DIR/a-order.txt"
  ORDER_P50="$(percentile 50 "$LOG_DIR/a-order.txt")"
fi
[[ "$ORDER_P50" -ge 1 ]] || ORDER_P50=1
# 查订单“和基线相当”：p50 不超过基线的 3 倍和基线加 50 毫秒中较大的一个（从 MacBook 经局域网，几毫秒的抖动很常见）。
ORDER_LIMIT=$((ORDER_P50 * 3 > ORDER_P50 + 50 ? ORDER_P50 * 3 : ORDER_P50 + 50))
SC_NOTE="stock p50 ${BASE_P50}ms, order $BASE_ORDER -> ${state:-none}, order query p50 ${ORDER_P50}ms"
end_scenario A "baseline"

# 查订单（gateway -> order，不经过 inventory）若干次：全 200 且 p50 在 ORDER_LIMIT 以内。
order_isolation_check() { # 次数 文件
  local p50 bad
  if [[ -z "$BASE_ORDER" ]]; then
    expect "order queries during the fault (no base order from scenario A)" false
    return 0
  fi
  http seq "$1" "/api/v1/orders/$BASE_ORDER" "$USER_ID" >"$2"
  p50="$(percentile 50 "$2")"
  bad="$(count_not "$2" 200)"
  expect "$1 order queries during the fault: codes $(code_hist "$2"), p50 ${p50}ms (baseline ${ORDER_P50}ms, limit ${ORDER_LIMIT}ms)" \
    [ "$bad" = 0 -a "$p50" -le "$ORDER_LIMIT" ]
}

# 挂住全部 inventory 后的第一批：并发 B_BURST 个，全部卡到路由超时，让每个 gateway 对每个 inventory 攒够 5 次连续失败。
# 端点的观察者从挂住前开始采样。结果在 <前缀>-r1.txt、<前缀>-endpoints.txt。
# 恢复后轮询到第一个 200，恢复用时（从 SIGCONT 到 200 返回）放在 RECOVER_MS，-1 表示超时。
recover_after_resume() { # 时限秒数 文件
  local t0 first
  t0="$(now_ms)"
  RECOVER_MS=-1
  http poll "$(stock_path "$SKU")" "$1" 1 "$USER_ID" >"$2" || true
  first="$(awk '$2 == 200 {print $1 + $3; exit}' "$2")"
  if [[ -n "$first" ]]; then RECOVER_MS=$((first - t0)); fi
}

# 端点断言：每个 gateway 都看到这些 IP 出现过 FAILED。打印每个的时间线。
expect_failed_seen() { # 采样文件 挂住时刻 IP...
  local file="$1" t0="$2" gw ip first all=1 detail=""
  shift 2
  for gw in $GW_PODS; do
    for ip in "$@"; do
      first="$(ep_first "$file" "$gw" "$ip" FAILED)"
      log "    $gw sees $ip: $(ep_timeline "$file" "$gw" "$ip")"
      if [[ -n "$first" ]]; then
        detail="$detail ${gw##*-}/$ip:+$((first - t0))ms"
      else
        detail="$detail ${gw##*-}/$ip:never"
        all=0
      fi
    done
  done
  EP_DETAIL="$detail"
  expect "outlier check FAILED seen in every gateway sidecar for$(printf ' %s' "$@") (first seen after the hang:$detail)" [ "$all" = 1 ]
}

# ---------- 场景 B：下游全部挂住 ----------
if wanted B; then
  begin B "all inventory hung (SIGSTOP both)"
  env_clean || SC_FAIL=1
  EPF="$LOG_DIR/b-endpoints.txt"
  : >"$EPF"
  snap_stats b0
  ep_watch_start 14 "$EPF"
  # shellcheck disable=SC2086 # Pod 名按空格拆开
  if hang $INV_PODS; then
    t_hang="$HANG_T0"
    # 第一批：全部等到路由超时。之后不再成批发：摘除在那一批超时之后要过几百毫秒到 1 秒才生效（推断是 Envoy 的
    # update_merge_window），紧跟着的一批会整批再等 3 秒。改成每 200 毫秒一个的连续探测，直到连续 10 个快速 503，
    # 顺便量出“第一批返回到摘除生效”的间隔。最多 7 秒不再发新的（挂住的总时间要在 FAULT_MAX_HOLD 以内）。
    http burst "$B_BURST" "$(stock_path "$SKU")" 31000 >"$LOG_DIR/b-r1.txt"
    probe_ok=1
    http until_fast "$(stock_path "$SKU")" 7 10 "$USER_ID" >"$LOG_DIR/b-probe.txt" || probe_ok=0
    cat "$LOG_DIR/b-r1.txt" "$LOG_DIR/b-probe.txt" >"$LOG_DIR/b-all.txt"
    order_isolation_check 20 "$LOG_DIR/b-order.txt"
    resume_all
    recover_after_resume 20 "$LOG_DIR/b-recover.txt"
    ep_watch_stop
    snap_stats b1

    ALL="$LOG_DIR/b-all.txt"
    # 慢（>= 1 秒）的是摘除之前、等到路由超时的；快的是摘除之后边车直接拒绝的。
    slow="$(awk '$3 >= 1000' "$ALL" | wc -l | tr -d ' ')"
    slow_bad="$(awk '$3 >= 1000 && (($2 != 503 && $2 != 504) || $3 < 2500 || $3 > 4000)' "$ALL" | wc -l | tr -d ' ')"
    slow503="$(awk '$3 >= 1000 && $2 == 503' "$ALL" | wc -l | tr -d ' ')"
    slow504="$(awk '$3 >= 1000 && $2 == 504' "$ALL" | wc -l | tr -d ' ')"
    fast="$(awk '$3 < 1000' "$ALL" | wc -l | tr -d ' ')"
    fast_bad="$(awk '$3 < 1000 && ($2 != 503 || $3 >= 500)' "$ALL" | wc -l | tr -d ' ')"
    # 只打印：第一批返回（最晚的那个返回的时刻）到第一个快速 503 的间隔，其间有几个探测又等满了路由超时。
    r1_end="$(awk 'BEGIN {e = 0} $1 + $3 > e {e = $1 + $3} END {print e}' "$LOG_DIR/b-r1.txt")"
    first_fast="$(awk '$2 == 503 && $3 < 500 {print $1; exit}' "$LOG_DIR/b-probe.txt")"
    slow_probes="$(awk '$2 == 503 && $3 < 500 {exit} $3 >= 1000 {n++} END {print n + 0}' "$LOG_DIR/b-probe.txt")"
    if [[ -n "$first_fast" ]]; then eff="$((first_fast - r1_end))ms"; else eff="never"; fi
    log "  ejection took effect ${eff} after the first burst returned; $slow_probes probe(s) in between waited the full timeout"
    log "  probes: $(count_lines "$LOG_DIR/b-probe.txt"), codes $(code_hist "$LOG_DIR/b-probe.txt"); all codes: $(code_hist "$ALL")"
    log "  slow (>=1s): $slow (503: $slow503, 504: $slow504), latency min/max $(awk '$3 >= 1000' "$ALL" | sort -k3 -n | awk 'NR == 1 {a = $3} {b = $3} END {print a "/" b}')ms"
    log "  fast (<1s): $fast, max $(awk '$3 < 1000' "$ALL" | sort -k3 -n | tail -n 1 | awk '{print $3}')ms"
    expect "slow responses before ejection ($slow) are 503/504 within 2.5-4s (violations: $slow_bad)" [ "$slow" -ge 1 -a "$slow_bad" = 0 ]
    expect "fast responses after ejection ($fast) are 503 under 500ms (violations: $fast_bad; need >= 10)" [ "$fast" -ge 10 -a "$fast_bad" = 0 ]
    expect "probes reached 10 fast 503s in a row within 7s (reached: $probe_ok)" [ "$probe_ok" = 1 ]
    # shellcheck disable=SC2086
    expect_failed_seen "$EPF" "$t_hang" $INV_IPS
    expect "stock query recovered ${RECOVER_MS}ms after SIGCONT (limit 20000ms; polls: $(code_hist "$LOG_DIR/b-recover.txt"))" \
      [ "$RECOVER_MS" -ge 0 -a "$RECOVER_MS" -le 20000 ]
    log "  sidecar stats (both gateways): ejections_enforced_total +$(stat_delta b0 b1 up outlier_detection.ejections_enforced_total)," \
      "upstream_rq_timeout +$(stat_delta b0 b1 up upstream_rq_timeout), upstream_cx_none_healthy +$(stat_delta b0 b1 up upstream_cx_none_healthy)"
    SC_NOTE="slow $slow (503 $slow503 / 504 $slow504), fast $fast, ejection effective +$eff ($slow_probes slow probes), recovery ${RECOVER_MS}ms, FAILED seen:$EP_DETAIL"
  else
    resume_all
    ep_watch_stop
    SC_FAIL=1
    SC_NOTE="could not freeze the inventory processes"
  fi
  end_scenario B "all inventory hung"
fi

# ---------- 场景 B'：只挂住一个副本 ----------
if wanted Bp; then
  begin "B'" "one inventory hung (SIGSTOP one of two)"
  env_clean || SC_FAIL=1
  victim="$(first_word "$INV_PODS")"
  victim_ip="$(inv_ip "$victim")"
  log "  victim $victim ($victim_ip); the other: $(echo "$INV_PODS" | tr ' ' '\n' | grep -v "^$victim\$" | tr '\n' ' ' || true)"
  EPF="$LOG_DIR/bp-endpoints.txt"
  : >"$EPF"
  ep_watch_start 16 "$EPF"
  if hang "$victim"; then
    t_hang="$HANG_T0"
    : >"$LOG_DIR/bp-all.txt"
    settled=0
    # 成批发：坏的那个上面挂着的请求越多，LEAST_REQUEST 越不选它，所以要一批足够大，让它在每个 gateway 上都攒够 5 次失败。
    for r in $(seq 1 "$BP_ROUNDS"); do
      http burst "$BP_BURST" "$(stock_path "$SKU")" $((32000 + r * 100)) >"$LOG_DIR/bp-r$r.txt"
      cat "$LOG_DIR/bp-r$r.txt" >>"$LOG_DIR/bp-all.txt"
      log "  round $r: $(code_hist "$LOG_DIR/bp-r$r.txt"), max $(max_ms "$LOG_DIR/bp-r$r.txt")ms"
      if [[ "$(count_not "$LOG_DIR/bp-r$r.txt" 200)" == 0 ]]; then
        settled=1
        break
      fi
    done
    total_fail="$(count_not "$LOG_DIR/bp-all.txt" 200)"
    http seq 50 "$(stock_path "$SKU")" "$USER_ID" >"$LOG_DIR/bp-after.txt"
    held_ms=$(($(now_ms) - t_hang))
    resume_all
    ep_watch_stop
    after_bad="$(count_not "$LOG_DIR/bp-after.txt" 200)"
    after_max="$(max_ms "$LOG_DIR/bp-after.txt")"
    expect "a round without failures within $BP_ROUNDS rounds (reached: $settled; failed requests before that: $total_fail)" [ "$settled" = 1 ]
    expect_failed_seen "$EPF" "$t_hang" "$victim_ip"
    expect "then 50 sequential queries all 200 and under 500ms (codes: $(code_hist "$LOG_DIR/bp-after.txt"), max ${after_max}ms)" \
      [ "$after_bad" = 0 -a "$after_max" -lt 500 ]
    # 好的那个不该出现 FAILED（它在接全部流量）。只打印。
    for ip in $INV_IPS; do
      [[ "$ip" == "$victim_ip" ]] && continue
      for gw in $GW_PODS; do log "    (healthy one) $gw sees $ip: $(ep_timeline "$EPF" "$gw" "$ip")"; done
    done
    SC_NOTE="failed requests until settled: $total_fail, held ${held_ms}ms, FAILED seen:$EP_DETAIL, after: max ${after_max}ms"
  else
    resume_all
    ep_watch_stop
    SC_FAIL=1
    SC_NOTE="could not freeze $victim"
  fi
  end_scenario "B'" "one inventory hung"
  # 等坏的那个回来（就绪、摘除期满），再进入下一个场景：下一个场景开头的 env_clean 会做。
fi

# ---------- 场景 C：下游被杀 ----------
if wanted C; then
  begin C "inventory pods force-deleted"
  env_clean || SC_FAIL=1
  old="$INV_PODS"
  READY_FILE="$LOG_DIR/c-ready-at.txt"
  : >"$READY_FILE"
  t_del="$(now_ms)"
  # shellcheck disable=SC2086
  kubectl -n "$NS" delete pod $old --grace-period=0 --force --wait=false >>"$EVENTS" 2>&1 || log "  WARNING: kubectl delete returned non-zero"
  log "  force-deleted$old"
  # 新 Pod 第一次就绪的时刻（后台，1 秒一次）。
  (
    end=$((SECONDS + 240))
    while [[ $SECONDS -lt $end ]]; do
      names="$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=inventory -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.metadata.deletionTimestamp}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null || true)"
      while IFS='|' read -r name del ready; do
        [[ -n "$name" && -z "$del" && "$ready" == True ]] || continue
        case " $old " in *" $name "*) continue ;; esac
        echo "$(now_ms) $name" >"$READY_FILE"
        exit 0
      done <<<"$names"
      sleep 1
    done
  ) &
  READY_PID=$!
  # 恢复 = 第一个失败之后连续 5 个 200 里的第一个（第二个新 Pod 加入时不该再有失败）。强制删除只是立刻删掉了 API 对象，
  # 进程要等 kubelet 去杀，所以删除之后最早的几个请求可能还是 200：python 的 poll 只数第一个失败之后的 200。
  http poll "$(stock_path "$SKU")" 240 5 "$USER_ID" 1 >"$LOG_DIR/c-poll.txt" || true
  wait "$READY_PID" 2>/dev/null || true
  READY_PID=""
  POLL="$LOG_DIR/c-poll.txt"
  # 一次算出：第一个失败的行号、恢复（连续 5 个 200 的第一个）的行号、之前的 200 个数、第一个失败的开始时刻、恢复的返回时刻。
  read -r ff rec_line early ff_at rec_at <<<"$(awk -v need=5 '
    {s[NR] = $1; c[NR] = $2; m[NR] = $3}
    END {
      ff = 0; for (i = 1; i <= NR; i++) if (c[i] != 200) {ff = i; break}
      rec = 0; run = 0
      if (ff) for (i = ff; i <= NR; i++) {
        if (c[i] == 200) {run++; if (run == 1) st = i; if (run >= need) {rec = st; break}} else run = 0
      }
      printf "%d %d %d %s %s\n", ff, rec, (ff ? ff - 1 : NR), (ff ? s[ff] : "-"), (rec ? s[rec] + m[rec] : "-")
    }' "$POLL")"
  ready_at="$(awk '{print $1; exit}' "$READY_FILE")"
  # 删除到恢复之间（第一个失败起、恢复那一行之前）的请求：非 200 的都应该是 500 毫秒内的 503。
  window="$LOG_DIR/c-window.txt"
  awk -v a="$ff" -v b="$rec_line" 'a > 0 && NR >= a && (b == 0 || NR < b)' "$POLL" >"$window"
  n_fail="$(count_not "$window" 200)"
  viol="$(awk '$2 != 200 && ($2 != 503 || $3 >= 500)' "$window" | wc -l | tr -d ' ')"
  log "  200s right after the delete (old process still alive, not violations): $early"
  log "  between first failure and recovery: $(count_lines "$window") requests, codes $(code_hist "$window"), max $(max_ms "$window")ms"
  if [[ "$viol" != 0 ]]; then
    log "  violations (start relative to delete, code, ms):"
    awk -v t="$t_del" '$2 != 200 && ($2 != 503 || $3 >= 500) {printf "    +%dms %s %dms\n", $1 - t, $2, $3}' "$window" | head -n 20 | tee -a "$EVENTS"
  fi
  expect "failures after the delete ($n_fail) are all 503 under 500ms (violations: $viol; need >= 1 failure)" [ "$n_fail" -ge 1 -a "$viol" = 0 ]
  rec=NA
  outage=NA
  if [[ "$rec_at" != - && -n "$ready_at" ]]; then rec=$((rec_at - ready_at)); fi
  if [[ "$rec_at" != - && "$ff_at" != - ]]; then outage=$((rec_at - ff_at)); fi
  # 就绪的时刻是 1 秒一次轮询出来的，恢复可能比它早一点：允许 -5 秒以内的负数。
  expect "recovered ${rec}ms after the first new pod became ready (-5000..30000ms; ready at +$((${ready_at:-$t_del} - t_del))ms; outage ${outage}ms from first failure)" \
    [ "$rec" != NA -a "$rec" -ge -5000 -a "$rec" -le 30000 ]
  SC_NOTE="early 200s $early, failures $n_fail (max $(max_ms "$window")ms), outage ${outage}ms, ready->recovered ${rec}ms"
  end_scenario C "inventory force-deleted"
fi

# ---------- 场景 D：并发上限 ----------
if wanted D; then
  begin D "concurrency limit ($D_N concurrent against hung inventory)"
  # 前面的场景摘除过，摘除时长逐次加长：env_clean 轮询到端点都是 OK 才开始。
  env_clean || SC_FAIL=1
  snap_stats d0
  # shellcheck disable=SC2086
  if hang $INV_PODS; then
    OUT="$LOG_DIR/d.txt"
    http burst "$D_N" "$(stock_path "$SKU")" 40000 >"$OUT"
    snap_stats d1
    resume_all
    total="$(count_lines "$OUT")"
    fast503="$(awk '$2 == 503 && $3 < 1000' "$OUT" | wc -l | tr -d ' ')"
    fast_other="$(awk '$2 != 503 && $3 < 1000' "$OUT" | wc -l | tr -d ' ')"
    waited="$(awk '$3 >= 1000' "$OUT" | wc -l | tr -d ' ')"
    over5="$(awk '$3 >= 5000' "$OUT" | wc -l | tr -d ' ')"
    need=$((D_N - 2 * PER_GW_LIMIT))
    log "  responses $total, codes $(code_hist "$OUT"); <1s: 503 $fast503, other $fast_other; >=1s: $waited (codes $(awk '$3 >= 1000' "$OUT" | code_hist /dev/stdin)); >=5s: $over5"
    log "  latency of the waited ones min/max: $(awk '$3 >= 1000' "$OUT" | sort -k3 -n | awk 'NR == 1 {a = $3} {b = $3} END {print a "/" b}')ms"
    for gw in $GW_PODS; do
      log "    $gw: app->sidecar +$(stat_delta d0 d1 down downstream_rq_total "$gw"), sidecar->inventory +$(stat_delta d0 d1 up upstream_rq_total "$gw")," \
        "pending_overflow +$(stat_delta d0 d1 up upstream_rq_pending_overflow "$gw"), cx_none_healthy +$(stat_delta d0 d1 up upstream_cx_none_healthy "$gw")"
    done
    expect "$total responses received" [ "$total" = "$D_N" ]
    expect "$fast503 rejected immediately (503 under 1s), need >= $need" [ "$fast503" -ge "$need" ]
    expect "$waited waited (>= 1s), at most $((2 * PER_GW_LIMIT)) (2 gateways x $PER_GW_LIMIT)" [ "$waited" -le $((2 * PER_GW_LIMIT)) ]
    expect "no request took 5s or more ($over5)" [ "$over5" = 0 ]
    SC_NOTE="fast 503 $fast503/$total (need >= $need), waited $waited (<= $((2 * PER_GW_LIMIT))), >=5s $over5"
  else
    resume_all
    SC_FAIL=1
    SC_NOTE="could not freeze the inventory processes"
  fi
  end_scenario D "concurrency limit"
fi

# ---------- 场景 E：业务错误不触发摘除 ----------
if wanted E; then
  begin E "business errors (404) do not eject"
  env_clean || SC_FAIL=1
  snap_stats e0
  http seq 30 "$(stock_path "$MISSING_SKU")" "$USER_ID" >"$LOG_DIR/e-missing.txt"
  snap="$(ep_all)"
  snap_stats e1
  not404="$(count_not "$LOG_DIR/e-missing.txt" 404)"
  notok="$(printf '%s\n' "$snap" | awk '$4 != "OK"' | wc -l | tr -d ' ')"
  printf '%s\n' "$snap" | sed 's/^/    endpoints: /' | tee -a "$EVENTS"
  line="$(http seq 1 "$(stock_path "$SKU")" "$USER_ID")"
  ej="$(stat_delta e0 e1 up outlier_detection.ejections_enforced_total)"
  expect "30 queries for a missing SKU all 404 (codes: $(code_hist "$LOG_DIR/e-missing.txt"))" [ "$not404" = 0 ]
  expect "all endpoints still OK in every gateway sidecar (not OK: $notok)" [ "$notok" = 0 -a -n "$snap" ]
  expect "ejections_enforced_total +$ej (need 0; NA = stats missing, not asserted)" [ "$ej" = 0 -o "$ej" = NA ]
  expect "existing SKU still 200 (got $(echo "$line" | awk '{print $2}'))" [ "$(echo "$line" | awk '{print $2}')" = 200 ]
  SC_NOTE="404 x30, endpoints not OK $notok, ejections +$ej"
  end_scenario E "business errors"
fi

# ---------- 场景 F：不重试 ----------
# 量法（不改服务代码能做到的最直接的版本）：
#   N  MacBook 收到的响应数：每个都经过 gateway，gateway 对每个查库存请求调一次 inventory
#   M  gateway 进程交给自己边车的请求数（出站监听 http.outbound_*9082* 的 downstream_rq_total 增量，两个 gateway 求和）
#   U  边车发往 inventory 的请求数（集群的 upstream_rq_total 增量），每一次重试都会让它多 1
#   L  边车本地拒绝、没发出去的：没有健康端点（upstream_cx_none_healthy）、溢出（upstream_rq_pending_overflow）
# 断言 M = N（gateway 进程自己不重试）、U + L = M（边车没有多发）、upstream_rq_retry 增量为 0。
# 失败要由边车之后的 inventory 产生才测得到重试：SIGKILL inventory 的进程（Pod 和它的边车还在），inventory 的边车
# 连不上本机进程，回 503（gRPC 的 UNAVAILABLE），这正是 Istio 默认重试策略会重试的条件（unavailable、503）。
# SIGSTOP 造成的是超时，默认策略本来就不重试超时，测不出东西；强制删 Pod 时一部分请求在边车本地就被拒绝，也不如这个直接。
f_run() { # 标签
  local tag="$1" out="$LOG_DIR/f-$1.txt" N M U L none ovf retry fails
  env_clean || SC_FAIL=1
  snap_stats "f-$tag-0"
  http stream "$F_SECS" 4 "$(stock_path "$SKU")" 50000 >"$out" &
  local spid=$!
  sleep 3
  # shellcheck disable=SC2086
  kill_procs $INV_PODS || SC_FAIL=1
  wait "$spid" || true
  # 统计在请求全部返回之后取；边车的计数是同步更新的，不需要再等。
  snap_stats "f-$tag-1"
  N="$(count_lines "$out")"
  fails="$(count_not "$out" 200)"
  M="$(stat_delta "f-$tag-0" "f-$tag-1" down downstream_rq_total)"
  U="$(stat_delta "f-$tag-0" "f-$tag-1" up upstream_rq_total)"
  none="$(stat_delta "f-$tag-0" "f-$tag-1" up upstream_cx_none_healthy)"
  ovf="$(stat_delta "f-$tag-0" "f-$tag-1" up upstream_rq_pending_overflow)"
  retry="$(stat_delta "f-$tag-0" "f-$tag-1" up upstream_rq_retry)"
  L=NA
  if [[ "$none" != NA && "$ovf" != NA ]]; then L=$((none + ovf)); fi
  log "  [$tag] client responses N=$N (codes $(code_hist "$out")), app->sidecar M=$M, sidecar->inventory U=$U," \
    "local rejects L=$L (none_healthy $none, overflow $ovf), upstream_rq_retry +$retry"
  log "  [$tag] also: upstream_rq_503 +$(stat_delta "f-$tag-0" "f-$tag-1" up upstream_rq_503)," \
    "upstream_cx_connect_fail +$(stat_delta "f-$tag-0" "f-$tag-1" up upstream_cx_connect_fail)," \
    "upstream_rq_retry_success +$(stat_delta "f-$tag-0" "f-$tag-1" up upstream_rq_retry_success)," \
    "ejections_enforced_total +$(stat_delta "f-$tag-0" "f-$tag-1" up outlier_detection.ejections_enforced_total)"
  F_N="$N" F_M="$M" F_U="$U" F_L="$L" F_RETRY="$retry" F_FAILS="$fails"
}

if wanted F; then
  begin F "no retries (SIGKILL inventory processes under load)"
  f_run main
  expect "failures were induced ($F_FAILS non-200 of $F_N), otherwise nothing was tested" [ "$F_FAILS" -ge 1 ]
  expect "upstream_rq_retry +$F_RETRY (need 0)" [ "$F_RETRY" = 0 ]
  expect "gateway app made one call per request: M=$F_M == N=$F_N" [ "$F_M" = "$F_N" ]
  if [[ "$F_U" != NA && "$F_L" != NA ]]; then
    expect "sidecar sent no extra requests: U=$F_U + L=$F_L == M=$F_M" [ "$((F_U + F_L))" = "$F_M" ]
  else
    expect "sidecar stats available (U=$F_U, L=$F_L)" false
  fi
  SC_NOTE="N=$F_N M=$F_M U=$F_U L=$F_L retry +$F_RETRY, induced failures $F_FAILS"
  if [[ "$F_CONTROL" == 1 ]]; then
    # 对照：把 gateway -> inventory 的路由改成重试 2 次（Istio 默认的条件），同样的量法应该看到重试。
    # 网格默认已经是 attempts: 0（deploy/cluster/istio/istiod.yaml），所以“删掉 retries 那一行”不够，要显式写重试。
    log "  control: enabling 2 retries on VirtualService $NS/inventory route from-gateway"
    MESH_DIRTY=1
    kubectl -n "$NS" patch virtualservice inventory --type=json \
      -p '[{"op":"replace","path":"/spec/http/0/retries","value":{"attempts":2,"retryOn":"connect-failure,refused-stream,unavailable,cancelled,retriable-status-codes"}}]' >>"$EVENTS" 2>&1
    sleep 5
    "${ISTIOCTL[@]}" proxy-config routes "$GW0.$NS" --name 9082 -o json >"$LOG_DIR/gateway-routes-9082-control.json" 2>&1 || true
    log "  control: route now has retryPolicy lines: $(grep -c retryPolicy "$LOG_DIR/gateway-routes-9082-control.json" || true)"
    f_run control
    kubectl apply -n "$NS" -f "$ROOT/deploy/cluster/mesh/inventory.yaml" >>"$EVENTS" 2>&1 && MESH_DIRTY=0
    log "  control: traffic rules restored (MESH_DIRTY=$MESH_DIRTY)"
    expect "control: with retries on, upstream_rq_retry +$F_RETRY > 0 (the measurement can see retries)" [ "$F_RETRY" != NA -a "$F_RETRY" != 0 ]
    SC_NOTE="$SC_NOTE; control: N=$F_N M=$F_M U=$F_U L=$F_L retry +$F_RETRY"
  fi
  end_scenario F "no retries"
fi

# ---------- 场景 G：重复摘除的时长 ----------
if wanted G; then
  begin G "repeated ejection ($G_CYCLES cycles of B)"
  times=""
  effs=""
  for i in $(seq 1 "$G_CYCLES"); do
    env_clean || SC_FAIL=1
    EPF="$LOG_DIR/g$i-endpoints.txt"
    : >"$EPF"
    ep_watch_start 8 "$EPF"
    # shellcheck disable=SC2086
    if ! hang $INV_PODS; then
      resume_all
      ep_watch_stop
      SC_FAIL=1
      times="$times freeze-failed"
      effs="$effs freeze-failed"
      continue
    fi
    t_hang="$HANG_T0"
    http burst "$B_BURST" "$(stock_path "$SKU")" $((60000 + i * 100)) >"$LOG_DIR/g$i-burst.txt"
    # 摘除在那一批超时之后要过一会儿才生效（见场景 B）：等到第一个快速 503 再 SIGCONT，否则测到的不是摘除时长。
    burst_end="$(awk 'BEGIN {e = 0} $1 + $3 > e {e = $1 + $3} END {print e}' "$LOG_DIR/g$i-burst.txt")"
    http until_fast "$(stock_path "$SKU")" 5 1 "$USER_ID" >"$LOG_DIR/g$i-probe.txt" || true
    ff="$(awk '$2 == 503 && $3 < 500 {print $1; exit}' "$LOG_DIR/g$i-probe.txt")"
    if [[ -n "$ff" ]]; then eff="$((ff - burst_end))ms"; else eff="not-effective"; fi
    resume_all
    recover_after_resume 90 "$LOG_DIR/g$i-recover.txt"
    ep_watch_stop
    seen=""
    for gw in $GW_PODS; do
      for ip in $INV_IPS; do
        f="$(ep_first "$EPF" "$gw" "$ip" FAILED)"
        if [[ -n "$f" ]]; then
          seen="$seen ${gw##*-}/$ip:+$((f - t_hang))ms"
        else
          seen="$seen ${gw##*-}/$ip:never"
        fi
      done
    done
    log "  cycle $i: burst $(code_hist "$LOG_DIR/g$i-burst.txt"); burst returned -> ejection effective: $eff;" \
      "SIGCONT -> first 200: ${RECOVER_MS}ms; FAILED seen:$seen"
    effs="$effs $eff"
    times="$times ${RECOVER_MS}ms"
  done
  log "  burst returned -> ejection effective:$effs"
  log "  recovery times (SIGCONT -> first 200, expected to grow ~5/10/15/20s if ejection backoff applies):$times"
  SC_NOTE="ejection effective:$effs; SIGCONT -> first 200:$times"
  end_scenario G "repeated ejection"
fi

# ---------- 结果 ----------
echo
{
  echo "== cluster fault injection results (run $E2E_RUN_ID, $LOG_DIR) =="
  printf '%s' "$RESULTS" | awk -F'|' '{printf "  %-3s %-26s %s  %s\n", $1, $2, $3, $4}'
} | tee -a "$EVENTS"
if [[ $FAILED -ne 0 ]]; then
  log "FAULT FAIL"
  exit 1
fi
log "FAULT PASS"
exit 0
