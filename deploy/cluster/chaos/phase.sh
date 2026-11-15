#!/bin/sh
# 在 chaos 的测试 Pod 里分阶段运行命令（scripts/cluster/chaos.sh 用 kubectl exec 调用）。
# 由 chaos.sh 放进 ConfigMap chaos-phase，挂在 /phase/phase.sh。镜像里的 shell 是 busybox 的 sh，只用 POSIX 的写法。
#
#   phase.sh start  <名字> <命令> [参数...]   在后台启动命令，立刻返回
#   phase.sh status <名字>                    已经结束就打印退出码，还在跑（或没启动过）就什么都不打印
#
# 输出在 /work/<名字>.out（标准输出）、/work/<名字>.err（标准错误），退出码在 /work/<名字>.exit。
#
# 为什么不让 kubectl exec 一直挂着等命令结束：load run 和 wait-quiet 要跑几分钟，exec 的连接经过 API 服务器
# 和 kubelet，中途断了，命令会跟着收到 SIGPIPE 或被杀掉，账本就不完整。所以命令和 exec 的会话脱开
# （setsid、nohup，标准输入输出都重定向），脚本只是隔几秒来看一次退出码。
# 退出码先写临时文件再改名：看到 .exit 的时候，输出一定已经写完。
# 命令行上没有密码：DSN 都在 Pod 的环境变量里（deploy/cluster/chaos/load-job.yaml），load 和 recon 自己读。
set -eu

W=/work

usage() {
  echo "usage: phase.sh start <name> <command> [args...] | phase.sh status <name>" >&2
  exit 2
}

[ $# -ge 2 ] || usage
op="$1"
name="$2"
shift 2
case "$name" in
*/* | "") usage ;;
esac

case "$op" in
start)
  [ $# -ge 1 ] || usage
  if [ -e "$W/$name.out" ]; then
    echo "phase $name was already started" >&2
    exit 2
  fi
  # 内层 sh 的 $0 是输出文件的前缀，$@ 是要运行的命令。
  # shellcheck disable=SC2016 # 整段由内层的 sh 展开
  setsid nohup sh -c '"$@" >"$0.out" 2>"$0.err"; echo $? >"$0.exit.tmp"; mv "$0.exit.tmp" "$0.exit"' \
    "$W/$name" "$@" </dev/null >/dev/null 2>&1 &
  ;;
status)
  if [ -f "$W/$name.exit" ]; then cat "$W/$name.exit"; fi
  ;;
*) usage ;;
esac
