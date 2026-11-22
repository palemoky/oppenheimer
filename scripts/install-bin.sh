#!/usr/bin/env bash
# 把 promtool、amtool 或 yq 的指定版本装进 ./bin（不入库）。版本由 Makefile 通过环境变量传入（PROMTOOL_VERSION、AMTOOL_VERSION、YQ_VERSION）。
# 按本机的操作系统和架构选发布包，校验发布页的 sha256 之后才放进 ./bin；已经是这个版本就什么都不做。
#
#   scripts/install-bin.sh promtool | amtool | yq
#
# 在 macOS 自带的 bash 3.2 和 ubuntu-latest 上都能运行。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$ROOT/bin}"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
x86_64 | amd64) arch=amd64 ;;
arm64 | aarch64) arch=arm64 ;;
*)
  echo "unsupported architecture: $(uname -m)" >&2
  exit 1
  ;;
esac

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp:?}"' EXIT

fetch() { # <url> <目标文件>
  curl -fsSL --retry 3 --retry-delay 2 -o "$2" "$1"
}

verify() { # <文件> <期望的 sha256>
  local got
  if command -v sha256sum >/dev/null 2>&1; then
    got="$(sha256sum "$1" | awk '{print $1}')"
  else
    got="$(shasum -a 256 "$1" | awk '{print $1}')"
  fi
  if [[ ${#2} -ne 64 || "$got" != "$2" ]]; then
    echo "sha256 mismatch for $(basename "$1"): want '$2', got '$got'" >&2
    exit 1
  fi
}

install_promtool() {
  local v="${PROMTOOL_VERSION:?PROMTOOL_VERSION not set}"
  if [[ "$("$BIN/promtool" --version 2>/dev/null | awk 'NR==1 {print $3}')" == "$v" ]]; then
    echo "promtool $v already installed"
    return 0
  fi
  local name="prometheus-$v.$os-$arch"
  local base="https://github.com/prometheus/prometheus/releases/download/v$v"
  fetch "$base/$name.tar.gz" "$tmp/$name.tar.gz"
  fetch "$base/sha256sums.txt" "$tmp/sha256sums.txt"
  verify "$tmp/$name.tar.gz" "$(awk -v f="$name.tar.gz" '$2 == f {print $1}' "$tmp/sha256sums.txt")"
  tar -xzf "$tmp/$name.tar.gz" -C "$tmp" "$name/promtool"
  mkdir -p "$BIN"
  mv "$tmp/$name/promtool" "$BIN/promtool"
  echo "installed promtool $v ($os-$arch)"
}

install_amtool() {
  local v="${AMTOOL_VERSION:?AMTOOL_VERSION not set}"
  if [[ "$("$BIN/amtool" --version 2>&1 | awk 'NR==1 {print $3}')" == "$v" ]]; then
    echo "amtool $v already installed"
    return 0
  fi
  local name="alertmanager-$v.$os-$arch"
  local base="https://github.com/prometheus/alertmanager/releases/download/v$v"
  fetch "$base/$name.tar.gz" "$tmp/$name.tar.gz"
  fetch "$base/sha256sums.txt" "$tmp/sha256sums.txt"
  verify "$tmp/$name.tar.gz" "$(awk -v f="$name.tar.gz" '$2 == f {print $1}' "$tmp/sha256sums.txt")"
  tar -xzf "$tmp/$name.tar.gz" -C "$tmp" "$name/amtool"
  mkdir -p "$BIN"
  mv "$tmp/$name/amtool" "$BIN/amtool"
  echo "installed amtool $v ($os-$arch)"
}

install_yq() {
  local v="${YQ_VERSION:?YQ_VERSION not set}"
  if [[ "$("$BIN/yq" --version 2>/dev/null | awk '{print $NF}')" == "$v" ]]; then
    echo "yq $v already installed"
    return 0
  fi
  local name="yq_${os}_$arch"
  local base="https://github.com/mikefarah/yq/releases/download/$v"
  fetch "$base/$name" "$tmp/$name"
  fetch "$base/checksums" "$tmp/checksums"
  # checksums 每行是 文件名 加 一串算法的摘要，顺序见发布页的 checksums_hashes_order；SHA-256 是第 18 个，即第 19 列。
  verify "$tmp/$name" "$(awk -v f="$name" '$1 == f {print $19}' "$tmp/checksums")"
  chmod +x "$tmp/$name"
  mkdir -p "$BIN"
  mv "$tmp/$name" "$BIN/yq"
  echo "installed yq $v ($os-$arch)"
}

case "${1:-}" in
promtool) install_promtool ;;
amtool) install_amtool ;;
yq) install_yq ;;
*)
  echo "usage: $0 promtool | amtool | yq" >&2
  exit 2
  ;;
esac
