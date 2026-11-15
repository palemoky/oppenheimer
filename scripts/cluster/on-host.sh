#!/usr/bin/env bash
# 从 MacBook 把 scripts/cluster/ 复制到宿主机，并在那里以 root 运行其中一个脚本。
#
# 用法：scripts/cluster/on-host.sh <脚本名> [参数...]
#   scripts/cluster/on-host.sh host-setup.sh
#   scripts/cluster/on-host.sh vm.sh create
#
# 宿主机的 ssh 目标取 HOST_SSH（默认见 cluster.env），额外的 ssh 选项放在 HOST_SSH_OPTS。需要免密 sudo。
# 本机的 ssh 公钥（SSH_PUBKEY_FILE，默认 ~/.ssh/id_ed25519.pub）会传过去，vm.sh 把它写进虚拟机。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"

if [[ $# -lt 1 || ! -f "$HERE/$1" ]]; then
  echo "usage: $0 <script in scripts/cluster> [args...]" >&2
  exit 2
fi
SCRIPT="$1"
shift

read -r -a SSH_OPTS <<<"${HOST_SSH_OPTS:-}"
SSH_OPTS+=(-o BatchMode=yes)
PUBKEY_FILE="${SSH_PUBKEY_FILE:-$HOME/.ssh/id_ed25519.pub}"
REMOTE_DIR="oppenheimer-cluster"

ssh "${SSH_OPTS[@]}" "$HOST_SSH" "mkdir -p $REMOTE_DIR"
scp -q "${SSH_OPTS[@]}" "$HERE"/*.sh "$HERE"/cluster.env "$PUBKEY_FILE" "$HOST_SSH:$REMOTE_DIR/"
# 参数逐个加引号后再拼进远端的命令行
ARGS=""
for a in "$@"; do ARGS+=" $(printf '%q' "$a")"; done
ssh "${SSH_OPTS[@]}" "$HOST_SSH" \
  "sudo SSH_PUBKEY_FILE=\$HOME/$REMOTE_DIR/$(basename "$PUBKEY_FILE") bash $REMOTE_DIR/$SCRIPT$ARGS"
