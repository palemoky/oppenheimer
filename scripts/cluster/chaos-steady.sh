#!/usr/bin/env bash
# 故障实验的稳态：一条固定速率的压测，和“现在是不是稳态”的检查（make chaos-steady ARGS=start|stop|check）。
# 设计：docs/design/4a-chaos-prep.md 的 3.6。
#
#   chaos-steady.sh start [--dry-run]   在发压机上起 k6 mixed（后台，经 scripts/cluster/k6.sh），固定速率
#   chaos-steady.sh stop  [--dry-run]   让它收尾退出（k6.sh 收到 TERM 会让发压机上的 k6 写出 summary 再退）
#   chaos-steady.sh check [--dry-run]   四项检查，全过退出 0：
#       alerts  没有 warning 或 critical 的告警在响（Prometheus 的 ALERTS，firing；被静默的也算：静默只管通知，不代表故障不在）
#       load    gateway 的请求速率 >= 目标速率的 70%（压测真的在打）
#       probe   常驻探针最近 5 分钟失败或跳过的比例 < 10%
#       slo     五个 SLO 最近 5 分钟的错误比例都在预算以内（预算：可用性 0.1%，延迟类 1%），
#               读 deploy/cluster/monitoring/rules/slo.yaml 的记录规则 slo:<名>:error_ratio_rate5m
#     退出码：0 稳态；1 不是稳态（打印原因）；2 用法不对或者查询出了错（没有结论）。
#
# 稳态速率：RATE=570 请求/秒（mixed 场景，一次迭代一个请求）。
#   出处：docs/notes/3b-capacity.md 第 1 节的表，混合读写的拐点是 1907 请求/秒（R4 1.1），570 是它的 30%（取整）。
#   这一档在 R4 阶梯的第一档（500）和第二档（625）之间，那一次两个工作节点的 CPU 用到 36% 到 60%，没有 5xx；
#   其中下单约 46 单/秒（8%），在落库的拐点（持续 125 到 150 单/秒）以下很远。取三成是为了留出余量：
#   故障（杀 Pod、延迟）会让剩下的副本多扛活，再叠上稳态的 90% 就进了拐点，实验量到的是过载而不是故障。
#
# 前提：SKUS 要给（环境变量，逗号分隔）：mixed 的下单会扣这些 SKU 的库存，按每秒约 46 单算，
#   默认 2 小时约 33 万单，20 个 SKU 每个要备 2 万以上（scripts/cluster/loadtest.sh skus <起始号> <个数> <每个的库存>，每一轮要用新的 SKU）。
#   发压机（ssh 别名 mini）可达、有 k6。k6 在跑的时候不在本机跑测试、lint、构建镜像（AGENTS.md）。
#
# 环境变量：
#   SKUS                    必填（start）
#   CHAOS_STEADY_RATE       默认 570
#   CHAOS_STEADY_DURATION   默认 2h
#   CHAOS_NS                检查哪个命名空间，默认 oppenheimer。SLO 的规则和探针都写死在 oppenheimer 上，
#                           其他命名空间时 slo、probe 两项跳过（并且 k6 打的是 oppenheimer 的入口，不是 e2e）
#   CHAOS_STEADY_SKIP       跳过的检查项，空格分隔（alerts load probe slo）；跳过的会打印出来
#   MERCHANTS 等            透传给 k6（见 test/k6/README.md）
#
# 状态文件：tmp/chaos-steady/（pid、日志）。macOS 自带的 bash 3.2 能运行。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
export KUBECONFIG="$KUBECONFIG_OUT"

NS="${CHAOS_NS:-oppenheimer}"
RATE="${CHAOS_STEADY_RATE:-570}"
DURATION="${CHAOS_STEADY_DURATION:-2h}"
SKIP=" ${CHAOS_STEADY_SKIP:-} "
STATE="$ROOT/tmp/chaos-steady"
PIDFILE="$STATE/k6.pid"
LOGFILE="$STATE/k6.log"

cmd="${1:-}"
[[ $# -gt 0 ]] && shift
DRY=0
while [[ $# -gt 0 ]]; do
  case "$1" in
  --dry-run) DRY=1; shift ;;
  *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done

skipped() { [[ "$SKIP" == *" $1 "* ]]; }

pid_alive() {
  [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null
}

cmd_start() {
  local skus="${SKUS:-}"
  if [[ -z "$skus" ]]; then
    echo "SKUS is required (comma separated); prepare them with scripts/cluster/loadtest.sh skus <start> <count> <stock each>" >&2
    exit 2
  fi
  echo "k6 mixed at RATE=$RATE for $DURATION against SKUS=$skus (30% of the 1907 req/s knee, docs/notes/3b-capacity.md section 1)"
  if [[ $DRY -eq 1 ]]; then
    echo "dry-run: would run: RATE=$RATE DURATION=$DURATION SKUS=$skus scripts/cluster/k6.sh mixed  (background, log $LOGFILE)"
    return 0
  fi
  if pid_alive; then
    echo "already running (pid $(cat "$PIDFILE")); stop it first" >&2
    exit 1
  fi
  mkdir -p "$STATE"
  RATE="$RATE" DURATION="$DURATION" SKUS="$skus" nohup "$HERE/k6.sh" mixed >"$LOGFILE" 2>&1 &
  echo $! >"$PIDFILE"
  sleep 5
  if ! pid_alive; then
    echo "k6.sh exited within 5 seconds; last lines of $LOGFILE:" >&2
    tail -n 10 "$LOGFILE" >&2 || true
    rm -f "$PIDFILE"
    exit 1
  fi
  echo "started (pid $(cat "$PIDFILE")); log $LOGFILE. Metrics reach Prometheus about 3 minutes later; run check after that."
}

cmd_stop() {
  if [[ $DRY -eq 1 ]]; then
    echo "dry-run: would send TERM to the pid in $PIDFILE and wait for it to exit"
    return 0
  fi
  if ! pid_alive; then
    echo "not running (no live pid in $PIDFILE)"
    rm -f "$PIDFILE"
    return 0
  fi
  local pid i
  pid="$(cat "$PIDFILE")"
  # TERM：k6.sh 的 trap 收到后会让发压机上的 k6 收尾（后台任务收不到 INT，所以不用 INT）。
  kill -TERM "$pid"
  for ((i = 0; i < 60; i++)); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "k6.sh (pid $pid) did not exit in 60 seconds; see $LOGFILE" >&2
    exit 1
  fi
  rm -f "$PIDFILE"
  echo "stopped"
}

# 即时查询，只打印第一条结果的值（没有结果或者不是数字时打印空）。查询本身出错时退出 2。
promql_value() {
  local out
  if ! out="$("$HERE/monitoring.sh" promql "$1" 2>&1)"; then
    echo "promql failed: $1 -> $out" >&2
    exit 2
  fi
  printf '%s' "$out" | python3 -I -c '
import json, math, sys
r = json.load(sys.stdin)["data"]["result"]
if r:
    v = float(r[0]["value"][1])
    if not math.isnan(v):
        print(v)
'
}

FAILS=()
fail() { FAILS+=("$1"); echo "  NOT STEADY: $1"; }
# 比较两个浮点数：python 做，bash 不会。$1 op $2，op 是 lt、le、ge。
num_cmp() {
  python3 -I -c '
import sys
a, op, b = float(sys.argv[1]), sys.argv[2], float(sys.argv[3])
sys.exit(0 if {"lt": a < b, "le": a <= b, "ge": a >= b}[op] else 1)' "$1" "$2" "$3"
}

# SLO 和探针的规则写死在 oppenheimer 上。
slo_applies() { [[ "$NS" == oppenheimer ]]; }

QUERY_ALERTS='ALERTS{alertstate="firing",severity=~"warning|critical",alertname!~"Watchdog|InfoInhibitor"}'
QUERY_LOAD="sum(rate(http_server_request_duration_seconds_count{service_name=\"gateway\",k8s_namespace_name=\"$NS\"}[3m]))"
QUERY_PROBE="sum(rate(probe_cycles_total{k8s_namespace_name=\"$NS\",outcome=~\"failed|skipped\"}[5m])) or vector(0)"
QUERY_PROBE_ALL="sum(rate(probe_cycles_total{k8s_namespace_name=\"$NS\"}[5m]))"
# 名字 预算
SLOS="order_create_availability:0.001 order_create_latency:0.01 order_persist_delay:0.01 order_pay_availability:0.001 order_payment_delay:0.01"

cmd_check() {
  if [[ $DRY -eq 1 ]]; then
    echo "dry-run: would query Prometheus (namespace $NS, skip:${SKIP% })"
    echo "  alerts: $QUERY_ALERTS   (must be empty)"
    echo "  load:   $QUERY_LOAD   (>= 70% of $RATE)"
    echo "  probe:  ($QUERY_PROBE) / ($QUERY_PROBE_ALL)   (< 0.1)"
    local s
    for s in $SLOS; do echo "  slo:    slo:${s%%:*}:error_ratio_rate5m   (<= ${s##*:})"; done
    return 0
  fi
  local v n

  if skipped alerts; then echo "skipped: alerts"; else
    local out
    if ! out="$("$HERE/monitoring.sh" promql "$QUERY_ALERTS" 2>&1)"; then
      echo "promql failed: $out" >&2
      exit 2
    fi
    n="$(printf '%s' "$out" | python3 -I -c '
import json, sys
r = json.load(sys.stdin)["data"]["result"]
names = sorted({s["metric"].get("alertname", "?") + "(" + s["metric"].get("severity", "?") + ")" for s in r})
print(", ".join(names))')"
    if [[ -n "$n" ]]; then fail "alerts firing: $n"; else echo "  ok: no warning/critical alert firing"; fi
  fi

  if skipped load; then echo "skipped: load"; else
    v="$(promql_value "$QUERY_LOAD")"
    if [[ -z "$v" ]]; then
      fail "load: no gateway request metric (is the steady load running? metrics arrive about 3 minutes after start)"
    elif num_cmp "$v" ge "$((RATE * 7 / 10))"; then
      echo "  ok: gateway at $v req/s (target $RATE)"
    else
      fail "load: gateway at $v req/s, below 70% of the target $RATE"
    fi
  fi

  if skipped probe; then echo "skipped: probe"; elif ! slo_applies; then echo "skipped: probe (only exists in namespace oppenheimer)"; else
    local bad all
    all="$(promql_value "$QUERY_PROBE_ALL")"
    bad="$(promql_value "$QUERY_PROBE")"
    if [[ -z "$all" ]] || ! num_cmp "$all" ge 0.001; then
      fail "probe: no probe cycles in the last 5 minutes (the probe is not running)"
    elif num_cmp "$(python3 -I -c 'import sys; print(float(sys.argv[1]) / float(sys.argv[2]))' "${bad:-0}" "$all")" lt 0.1; then
      echo "  ok: probe failing share ${bad:-0}/$all per second"
    else
      fail "probe: failed or skipped cycles $bad/s of $all/s"
    fi
  fi

  if skipped slo; then echo "skipped: slo"; elif ! slo_applies; then echo "skipped: slo (rules are bound to namespace oppenheimer)"; else
    local s name budget
    for s in $SLOS; do
      name="${s%%:*}"
      budget="${s##*:}"
      v="$(promql_value "slo:$name:error_ratio_rate5m")"
      if [[ -z "$v" ]]; then
        fail "slo $name: no data (no traffic in the window, or the rule is not loaded)"
      elif num_cmp "$v" le "$budget"; then
        echo "  ok: slo $name error ratio $v (budget $budget)"
      else
        fail "slo $name: error ratio $v over the budget $budget"
      fi
    done
  fi

  if [[ ${#FAILS[@]} -gt 0 ]]; then
    echo "steady state: NO (${#FAILS[@]} problem(s))"
    exit 1
  fi
  echo "steady state: yes"
}

case "$cmd" in
start) cmd_start ;;
stop) cmd_stop ;;
check) cmd_check ;;
-h | --help | "") sed -n '2,35p' "${BASH_SOURCE[0]}"; [[ -n "$cmd" ]] || exit 2 ;;
*) echo "usage: chaos-steady.sh start|stop|check [--dry-run]" >&2; exit 2 ;;
esac
