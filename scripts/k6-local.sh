#!/usr/bin/env bash
# 在本机用 Docker 镜像跑一个 k6 压测场景。
#
#   scripts/k6-local.sh <场景名> [k6 的额外参数]
#   场景名：order | flash | mixed | ratelimit（对应 test/k6/<场景名>.js）
#
# 配置全部走环境变量，含义见 test/k6/README.md，例如：
#   RATE=30 DURATION=20s scripts/k6-local.sh order
#   FLASH_SKU=1001 RATE=50 DURATION=20s scripts/k6-local.sh flash
#
# 结果放在 tmp/k6/<RUN_ID>/：summary.json、orders.jsonl、conditions.txt。
# RUN_ID 没设时用 <开始时间 UTC>-<场景名>。k6 的退出码原样返回（阈值不通过是 99）。
# 只用 macOS 自带 bash 3.2 能跑的写法。
set -euo pipefail

K6_IMAGE="${K6_IMAGE:-grafana/k6:2.4.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

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

# 传给脚本的环境变量（没设的不传，用脚本里的默认值）
VARS="BASE_URL HOST_HEADER RUN_ID USER_BASE USER_COUNT SKUS MERCHANTS RATE DURATION STAGES MAX_VUS PRE_VUS
POLL_INTERVAL_MS POLL_BACKOFF POLL_MAX_INTERVAL_MS POLL_TIMEOUT_MS PAY_RATIO CANCEL_RATIO REQ_TIMEOUT FLASH_SKU RAMP ZIPF_S A_USERS A_RATE B_RATE"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%S)-$SCENARIO}"
export RUN_ID
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

if ! command -v jq >/dev/null 2>&1; then
  echo "需要 jq（brew install jq）" >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "docker 不可用，先启动 OrbStack" >&2
  exit 1
fi

mkdir -p "$OUT"

# 容器里的 127.0.0.1 是容器自己，换成宿主机的名字。conditions.txt 里记原值。
BASE_URL_V="${BASE_URL:-http://127.0.0.1:8080}"
CONTAINER_URL="$BASE_URL_V"
case "$CONTAINER_URL" in
  *://127.0.0.1* | *://localhost*)
    CONTAINER_URL="$(printf '%s' "$CONTAINER_URL" | sed -e 's#://127\.0\.0\.1#://host.docker.internal#' -e 's#://localhost#://host.docker.internal#')"
    ;;
esac

ENV_ARGS=()
for v in $VARS; do
  if [ "$v" = BASE_URL ]; then
    continue
  fi
  if [ -n "${!v:-}" ]; then
    ENV_ARGS+=(-e "$v=${!v}")
  fi
done
ENV_ARGS+=(-e "BASE_URL=$CONTAINER_URL" -e "OUT_DIR=/out")

K6_VERSION="$(docker run --rm "$K6_IMAGE" version 2>/dev/null | head -n 1 || true)"

START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
{
  echo "scenario: $SCENARIO"
  echo "start_utc: $START"
  echo "k6_image: $K6_IMAGE"
  echo "k6_version: $K6_VERSION"
  echo "host: $(uname -srm)"
  echo "extra_args: $*"
  echo "env (未列出的变量用 test/k6/lib/config.js 里的默认值):"
  echo "  BASE_URL=$BASE_URL_V"
  for v in $VARS; do
    if [ "$v" != BASE_URL ] && [ -n "${!v:-}" ]; then
      echo "  $v=${!v}"
    fi
  done
} >"$OUT/conditions.txt"

NAME="k6-$(printf '%s' "$RUN_ID" | tr -c 'A-Za-z0-9_.-' '-')"
trap 'docker stop "$NAME" >/dev/null 2>&1 || true' INT TERM

# console.log 的内容（订单号清单）用 --console-output 写到文件，下面再提取。
# 每个下单被受理的订单一行 JSON，高速率下是每秒一次小写入，开销见 README。
set +e
docker run --rm --name "$NAME" \
  --user "$(id -u):$(id -g)" \
  -v "$ROOT/test/k6:/scripts:ro" \
  -v "$OUT:/out" \
  "${ENV_ARGS[@]}" \
  "$K6_IMAGE" run \
  --console-output=/out/console.log \
  "$@" \
  "/scripts/$SCENARIO.js" 2>&1 | tee "$OUT/k6.log"
RC="${PIPESTATUS[0]}"
set -e

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
  echo "end_utc: $END"
  echo "k6_exit_code: $RC"
  echo "orders_jsonl_lines: $(wc -l <"$OUT/orders.jsonl" | tr -d ' ')"
} >>"$OUT/conditions.txt"

echo "结果目录: $OUT"
exit "$RC"
