#!/usr/bin/env bash
# 检查 Alertmanager 的配置（deploy/cluster/monitoring/alertmanager/）。
#
#   scripts/alertmanager-test.sh                       amtool check-config（带和不带 Telegram 的两种渲染结果都查），
#                                                      再按 tests/routes.txt 核对每类告警走到哪个接收端
#   scripts/alertmanager-test.sh --render <目录> [会话号]
#                                                      把 alertmanager.yaml 渲染到 <目录>（连同 *.tmpl）。给了会话号就填进
#                                                      __TELEGRAM_CHAT_ID__；没给就删掉 telegram-begin 到 telegram-end 之间的行。
#                                                      monitoring.sh alertmanager 用它生成 Secret 的内容
#
# 需要 amtool（make amtool 装进 ./bin，PATH 里已有的也行）。在 macOS 自带的 bash 3.2 上运行。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/deploy/cluster/monitoring/alertmanager"
PATH="$ROOT/bin:$PATH"

render() { # <目录> [会话号]
  local out="$1" chat="${2:-}"
  mkdir -p "$out"
  cp "$SRC"/*.tmpl "$out/"
  if [[ -n "$chat" ]]; then
    # 会话号是整数（群是负数）。不是的话在这里停下，免得把别的东西写进配置。
    if [[ ! "$chat" =~ ^-?[0-9]+$ ]]; then
      echo "the Telegram chat id must be an integer" >&2
      exit 1
    fi
    sed -e "s|__TELEGRAM_CHAT_ID__|$chat|g" "$SRC/alertmanager.yaml" >"$out/alertmanager.yaml"
  else
    sed -e '/^ *# telegram-begin$/,/^ *# telegram-end$/d' "$SRC/alertmanager.yaml" >"$out/alertmanager.yaml"
  fi
  # 注释里提到占位符的名字不算。
  if grep -v '^ *#' "$out/alertmanager.yaml" | grep -q '__[A-Z_]*__'; then
    echo "unfilled placeholder left in the rendered alertmanager.yaml" >&2
    exit 1
  fi
}

if [[ "${1:-}" == "--render" ]]; then
  render "${2:?usage: alertmanager-test.sh --render <dir> [chat id]}" "${3:-}"
  exit 0
fi

if ! command -v amtool >/dev/null 2>&1; then
  echo "amtool not found; run 'make amtool' (installs it into ./bin)" >&2
  exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp:?}"' EXIT

# 模板的路径在配置里是集群上的绝对路径（/etc/alertmanager/config/*.tmpl），本机检查时换成渲染目录。
localize() { # <目录>
  sed -i.bak -e "s|/etc/alertmanager/config/\*.tmpl|$1/*.tmpl|" "$1/alertmanager.yaml"
  rm -f "$1/alertmanager.yaml.bak"
}

render "$tmp/with" 1
render "$tmp/without"
for v in with without; do
  localize "$tmp/$v"
  echo "==> amtool check-config ($v telegram)"
  amtool check-config "$tmp/$v/alertmanager.yaml"
done
if grep -v '^ *#' "$tmp/without/alertmanager.yaml" | grep -q telegram; then
  echo "the rendering without Telegram still mentions telegram" >&2
  exit 1
fi

echo "==> routes (tests/routes.txt)"
fail=0 n=0
while IFS='|' read -r want labels; do
  want="$(echo "$want" | tr -d '[:space:]')"
  [[ -z "$want" || "$want" == \#* ]] && continue
  n=$((n + 1))
  for v in with without; do
    # shellcheck disable=SC2086 # 标签要按空格拆开
    if ! got="$(amtool config routes test --config.file="$tmp/$v/alertmanager.yaml" --verify.receivers="$want" $labels 2>&1)"; then
      echo "    FAIL ($v telegram): want $want, got $got  <-$labels"
      fail=1
    fi
  done
done <"$SRC/tests/routes.txt"
if [[ $fail -ne 0 ]]; then
  exit 1
fi
echo "    $n cases ok"
