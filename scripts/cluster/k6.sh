#!/usr/bin/env bash
# 从 MacBook 发起，在发压机（Mac mini）上原生运行一个 k6 压测场景，打集群的入口。
#
#   scripts/cluster/k6.sh <场景名> [k6 的额外参数]
#   场景名：order | flash | mixed | ratelimit（对应 test/k6/<场景名>.js）
#
# 例：
#   SKUS=3001 RATE=30 DURATION=20s scripts/cluster/k6.sh order
#   make k6 SCENARIO=mixed RATE=30 DURATION=20s SKUS=3001
#
# 流程：
#   1. rsync 把 test/k6/ 同步到发压机的 ~/oppenheimer-k6/scripts/（--delete）
#   2. ssh 上去跑 k6：调高 ulimit -n，用 caffeinate 防止发压机睡眠（它空闲 1 分钟就睡，没有 sudo 改不了）
#   3. k6 的指标实时 remote write 到集群的 Prometheus（经入口，按 Host 头 $PROM_WRITE_HOST 路由），
#      每条序列带标签 testid=<RUN_ID>；趋势类指标按 K6_PROM_TREND_STATS 输出成独立的序列
#   4. 跑完把结果拉回 tmp/k6/<RUN_ID>/，结构同 scripts/k6-local.sh：
#      summary.json、orders.jsonl、conditions.txt、k6.log（conditions.txt 里另有发压机的信息）
#   k6 的退出码原样返回（阈值不通过是 99）。
#
# Ctrl-C（或 SIGTERM）：经另一条 ssh 给发压机上的 k6 发 SIGINT，k6 会收尾并写出 summary.json，
# 然后照常拉回结果；不会留下还在发压的进程。
#
# 发压机上的位置：脚本在 ~/oppenheimer-k6/scripts/，每次运行的临时结果在 ~/oppenheimer-k6/runs/<RUN_ID>/；
# 结果拉回成功后会删掉这个目录，拉取失败则保留（报错里会说）。
#
# 环境变量：
#   透传给 k6 的那些和 k6-local.sh 一致（见 test/k6/README.md）；BASE_URL 默认 http://$LB_GATEWAY_IP，
#   HOST_HEADER 默认 api.oppenheimer.test。
#   LOAD_SSH           发压机的 ssh 别名，默认 mini
#   PROM_WRITE_HOST    remote write 用的 Host 头，默认 prom-write.oppenheimer.test
#   K6_PROM            设成 off 不写 Prometheus
#   K6_PROM_TREND_STATS  趋势类指标输出哪些统计，默认 p(50),p(95),p(99),max
#
# 发压机走 Wi-Fi 时客户端延迟不可信，脚本会在终端和 conditions.txt 里标出。
# 只用 macOS 自带 bash 3.2 能跑的写法。
# shellcheck disable=SC2029,SC2016,SC2329 # ssh 的命令里有意在本机展开 REMOTE_RUN；REMOTE_PRE 是要在发压机上展开的文本；stop_remote 由 trap 调用
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
ROOT="$(cd "$HERE/../.." && pwd)"

LOAD_SSH="${LOAD_SSH:-mini}"
PROM_WRITE_HOST="${PROM_WRITE_HOST:-prom-write.oppenheimer.test}"
K6_PROM="${K6_PROM:-on}"
K6_PROM_TREND_STATS="${K6_PROM_TREND_STATS:-p(50),p(95),p(99),max}"
REMOTE_BASE='oppenheimer-k6' # 相对发压机的 $HOME
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10)

if [ $# -lt 1 ]; then
  echo "用法: $0 <order|flash|mixed|ratelimit> [k6 的额外参数]" >&2
  exit 2
fi
SCENARIO="$1"
shift
case "$SCENARIO" in
  order | flash | mixed | ratelimit) ;;
  *)
    echo "未知场景: $SCENARIO（可选 order flash mixed ratelimit）" >&2
    exit 2
    ;;
esac

# 传给脚本的环境变量（没设的不传，用脚本里的默认值）。和 k6-local.sh 保持一致。
VARS="BASE_URL HOST_HEADER RUN_ID USER_BASE USER_COUNT SKUS MERCHANTS RATE DURATION STAGES MAX_VUS PRE_VUS
POLL_INTERVAL_MS POLL_TIMEOUT_MS PAY_RATIO CANCEL_RATIO REQ_TIMEOUT FLASH_SKU RAMP ZIPF_S A_USERS A_RATE B_RATE"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%S)-$SCENARIO}"
export RUN_ID
case "$RUN_ID" in
  *[!A-Za-z0-9_.-]*)
    echo "RUN_ID 只能含字母、数字、_ . -: $RUN_ID" >&2
    exit 2
    ;;
esac
OUT="$ROOT/tmp/k6/$RUN_ID"
if [ -e "$OUT" ]; then
  echo "结果目录已存在: $OUT（换一个 RUN_ID）" >&2
  exit 2
fi

# 用户号范围检查（和 test/k6/lib/config.js 一致）：>= 2^40 的订单不会被对账
USER_BASE_V="${USER_BASE:-549755813888}"
USER_COUNT_V="${USER_COUNT:-1000000}"
if [ $((USER_BASE_V + USER_COUNT_V)) -gt 1099511627776 ]; then
  echo "USER_BASE + USER_COUNT 超过 2^40（1099511627776），这样的用户号不会被对账" >&2
  exit 2
fi

for t in jq rsync ssh; do
  if ! command -v "$t" >/dev/null 2>&1; then
    echo "需要 $t" >&2
    exit 1
  fi
done

BASE_URL_V="${BASE_URL:-http://$LB_GATEWAY_IP}"
HOST_HEADER_V="${HOST_HEADER:-api.oppenheimer.test}"
# 下面按 VARS 循环取值时用这两个默认值
export BASE_URL="$BASE_URL_V" HOST_HEADER="$HOST_HEADER_V"

REMOTE_RUN="$REMOTE_BASE/runs/$RUN_ID" # 相对 $HOME，用在发压机的 shell 里时前面加 $HOME/

# 发压机上 ssh 非交互的 PATH 里没有 Homebrew；ulimit -n 默认 256，调到 kern.maxfilesperproc 以内
REMOTE_PRE='export PATH=/opt/homebrew/bin:$PATH
ulimit -n 60000 2>/dev/null || ulimit -n 10240 2>/dev/null || true'

# ---- 发压机的信息 ----
if ! INFO="$(
  ssh "${SSH_OPTS[@]}" "$LOAD_SSH" bash -s "$LB_GATEWAY_IP" <<'EOF'
export PATH=/opt/homebrew/bin:$PATH
ulimit -n 60000 2>/dev/null || ulimit -n 10240 2>/dev/null || true
ip="$1"
if ! command -v k6 >/dev/null 2>&1; then echo "error=发压机上没有 k6" ; exit 0; fi
echo "hostname=$(hostname)"
echo "os=$(sw_vers -productName) $(sw_vers -productVersion) ($(uname -m))"
echo "k6_version=$(k6 version | head -n 1)"
echo "ulimit_n=$(ulimit -n)"
iface="$(route -n get "$ip" 2>/dev/null | awk '/interface:/{print $2}')"
echo "interface=$iface"
echo "link=$(networksetup -listallhardwareports 2>/dev/null | awk -v d="$iface" '/Hardware Port:/{sub(/^Hardware Port: /,""); p=$0} /^Device:/{if ($2==d) print p}')"
EOF
)"; then
  echo "ssh $LOAD_SSH 失败" >&2
  exit 1
fi
info() { printf '%s\n' "$INFO" | sed -n "s/^$1=//p" | head -n 1; }
if [ -n "$(info error)" ]; then
  echo "$(info error)（$LOAD_SSH）" >&2
  exit 1
fi
LINK="$(info link)"
IFACE="$(info interface)"
WIFI=0
case "$LINK" in
  *[Ww]i-[Ff]i* | *WLAN*) WIFI=1 ;;
esac
if [ "$WIFI" = 1 ]; then
  printf '\033[1;33m!!! 发压机 %s 走的是 Wi-Fi（%s）：客户端测到的延迟不可信，只能看服务端的指标 !!!\033[0m\n' \
    "$(info hostname)" "$IFACE" >&2
fi

mkdir -p "$OUT"

# ---- 同步脚本 ----
ssh "${SSH_OPTS[@]}" "$LOAD_SSH" "mkdir -p \"\$HOME/$REMOTE_BASE/scripts\" \"\$HOME/$REMOTE_BASE/runs\""
rsync -a --delete -e "ssh ${SSH_OPTS[*]}" "$ROOT/test/k6/" "$LOAD_SSH:$REMOTE_BASE/scripts/"

# ---- 组装发压机上运行的脚本 ----
# 环境变量用 %q 转义后写进脚本里，不依赖 ssh 的环境变量转发。
ENVLINES=""
for v in $VARS; do
  if [ -n "${!v:-}" ]; then
    ENVLINES="$ENVLINES
export $v=$(printf '%q' "${!v}")"
  fi
done

PROM_LINES=""
PROM_ARGS=""
PROM_NOTE="off"
if [ "$K6_PROM" != off ]; then
  PROM_NOTE="on (http://$LB_GATEWAY_IP/api/v1/write, Host: $PROM_WRITE_HOST, testid=$RUN_ID, trend_stats=$K6_PROM_TREND_STATS)"
  PROM_LINES="
export K6_PROMETHEUS_RW_SERVER_URL=$(printf '%q' "http://$LB_GATEWAY_IP/api/v1/write")
export K6_PROMETHEUS_RW_HEADERS_Host=$(printf '%q' "$PROM_WRITE_HOST")
export K6_PROMETHEUS_RW_TREND_STATS=$(printf '%q' "$K6_PROM_TREND_STATS")
export K6_PROMETHEUS_RW_STALE_MARKERS=true"
  PROM_ARGS="-o experimental-prometheus-rw --tag testid=$RUN_ID"
fi

EXTRA=""
if [ $# -gt 0 ]; then
  EXTRA="$(printf '%q ' "$@")"
fi

REMOTE_SCRIPT="$REMOTE_PRE
RUN=\"\$HOME/$REMOTE_RUN\"
mkdir -p \"\$RUN\"
cd \"\$HOME/$REMOTE_BASE/scripts\"
export OUT_DIR=\"\$RUN\"$ENVLINES$PROM_LINES
# console.log 的内容（订单号清单）写到文件，拉回本机后再提取
k6 run --quiet --console-output=\"\$RUN/console.log\" $PROM_ARGS $EXTRA $SCENARIO.js >\"\$RUN/k6.log\" 2>&1 </dev/null &
K6PID=\$!
echo \$K6PID >\"\$RUN/k6.pid\"
# caffeinate 跟着 k6 的进程，k6 一退出它也退出
caffeinate -i -w \$K6PID >/dev/null 2>&1 &
tail -n +1 -f \"\$RUN/k6.log\" &
TAILPID=\$!
wait \$K6PID
RC=\$?
sleep 0.5
kill \$TAILPID 2>/dev/null
wait \$TAILPID 2>/dev/null
echo \$RC >\"\$RUN/rc\"
exit \$RC
"

# ---- 运行 ----
START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
INTERRUPTED=0
stop_remote() {
  INTERRUPTED=1
  echo "收到中断，停发压机上的 k6（它会收尾并写出 summary.json）..." >&2
  ssh "${SSH_OPTS[@]}" "$LOAD_SSH" "kill -INT \$(cat \"\$HOME/$REMOTE_RUN/k6.pid\" 2>/dev/null) 2>/dev/null; true" || true
}
trap stop_remote INT TERM

# 放到后台跑：非交互的 bash 里后台任务不收终端的 SIGINT，trap 才能先让 k6 收尾，ssh 继续把收尾的输出带回来。
set +e
ssh "${SSH_OPTS[@]}" "$LOAD_SSH" bash -s >&2 <<<"$REMOTE_SCRIPT" &
SSH_PID=$!
while kill -0 "$SSH_PID" 2>/dev/null; do
  wait "$SSH_PID"
done
wait "$SSH_PID" 2>/dev/null
SSH_RC=$?
set -e
trap - INT TERM

# k6 的退出码在发压机的 rc 文件里（ssh 自己断了也能拿到）
RC="$(ssh "${SSH_OPTS[@]}" "$LOAD_SSH" "cat \"\$HOME/$REMOTE_RUN/rc\" 2>/dev/null" || true)"
if [ -z "$RC" ]; then
  echo "没取到 k6 的退出码（ssh 退出码 $SSH_RC）" >&2
  RC=1
fi

# ---- 拉回结果 ----
if ! rsync -a -e "ssh ${SSH_OPTS[*]}" "$LOAD_SSH:$REMOTE_RUN/" "$OUT/"; then
  echo "拉取结果失败，发压机上保留在 ~/$REMOTE_RUN" >&2
  exit 1
fi
/bin/rm -f "$OUT/rc" "$OUT/k6.pid"
ssh "${SSH_OPTS[@]}" "$LOAD_SSH" "/bin/rm -rf \"\$HOME/$REMOTE_RUN\"" || true

# 提取订单号清单。k6 2.x 的 --console-output 每条一行 logfmt：time=... level=info msg="<转义后的 JSON>"，
# 用 jq 取出 msg 的字符串字面量再还原；订单号清单以外的 console 输出（如果有）留在 console-other.log。
: >"$OUT/orders.jsonl"
if [ -f "$OUT/console.log" ]; then
  jq -Rr 'capture("msg=(?<m>\"(?:[^\"\\\\]|\\\\.)*\")") | .m | fromjson | select(startswith("{\"run\":"))' \
    "$OUT/console.log" >"$OUT/orders.jsonl" || true
  grep -v 'msg="{\\"run\\":' "$OUT/console.log" >"$OUT/console-other.log" || true
  rm -f "$OUT/console.log"
  if [ ! -s "$OUT/console-other.log" ]; then
    rm -f "$OUT/console-other.log"
  fi
fi

END="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
{
  echo "scenario: $SCENARIO"
  echo "start_utc: $START"
  echo "end_utc: $END"
  echo "k6_version: $(info k6_version)"
  echo "extra_args: $*"
  echo "load_generator: $(info hostname), $(info os), ulimit -n $(info ulimit_n)"
  echo "load_generator_link: ${LINK:-未知} (interface ${IFACE:-未知})$([ "$WIFI" = 1 ] && echo ' [Wi-Fi：客户端延迟不可信]')"
  echo "prometheus_remote_write: $PROM_NOTE"
  echo "interrupted: $INTERRUPTED"
  echo "env (未列出的变量用 test/k6/lib/config.js 里的默认值):"
  echo "  BASE_URL=$BASE_URL_V"
  for v in $VARS; do
    if [ "$v" != BASE_URL ] && [ -n "${!v:-}" ]; then
      echo "  $v=${!v}"
    fi
  done
  echo "k6_exit_code: $RC"
  echo "orders_jsonl_lines: $(wc -l <"$OUT/orders.jsonl" | tr -d ' ')"
} >"$OUT/conditions.txt"

echo "结果目录: $OUT"
exit "$RC"
