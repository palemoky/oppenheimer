#!/usr/bin/env bash
# 检查并单元测试 Prometheus 的规则（deploy/cluster/monitoring/rules/）。
#
#   scripts/rules-test.sh            promtool check rules，再 promtool test rules
#   scripts/rules-test.sh --extract <目录>
#                                    只把规则清单里的 .spec.groups 抽成纯规则文件写到 <目录>（monitoring.sh rules 用它做下发前的检查）
#
# 仓库里的规则是 PrometheusRule 清单（规则在 .spec.groups），promtool 读的是顶层 groups: 的纯规则文件，
# 所以先用 yq 抽出来。临时目录的布局和仓库里一样：
#     <tmp>/slo.yaml           抽出来的规则，文件名同清单
#     <tmp>/tests/*.yaml       tests/ 下的测试文件的副本
# 测试文件里写 rule_files: [../slo.yaml]（相对于测试文件自己），在仓库里和临时目录里指的是同名的文件。
#
# 需要 promtool 和 yq（make promtool yq 装进 ./bin，PATH 里已有的也行）。在 macOS 自带的 bash 3.2 上运行。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RULES="$ROOT/deploy/cluster/monitoring/rules"
PATH="$ROOT/bin:$PATH"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 not found; run 'make $1' (installs it into ./bin)" >&2
    exit 1
  fi
}

# 抽取：每个清单 .spec.groups 写成 {groups: [...]}。
extract() { # <目录>
  local out="$1" f n=0
  mkdir -p "$out"
  for f in "$RULES"/*.yaml; do
    [[ -e "$f" ]] || continue
    yq '{"groups": .spec.groups}' "$f" >"$out/$(basename "$f")"
    n=$((n + 1))
  done
  if [[ $n -eq 0 ]]; then
    echo "no rule files in $RULES" >&2
    exit 1
  fi
}

need yq
if [[ "${1:-}" == "--extract" ]]; then
  extract "${2:?usage: rules-test.sh --extract <dir>}"
  exit 0
fi
need promtool

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp:?}"' EXIT

extract "$tmp"
echo "==> promtool check rules"
promtool check rules "$tmp"/*.yaml

if ls "$RULES"/tests/*.yaml >/dev/null 2>&1; then
  mkdir -p "$tmp/tests"
  cp "$RULES"/tests/*.yaml "$tmp/tests/"
  echo "==> promtool test rules"
  promtool test rules "$tmp"/tests/*.yaml
else
  echo "no tests in $RULES/tests" >&2
  exit 1
fi
