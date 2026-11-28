# shellcheck shell=bash
# method: script 的场景（chaos/scenarios/<编号>/inject/run.sh）共用的函数：在中间件的 Pod 里执行一段脚本、
# 起一个脱离终端的后台任务、撤销。不单独运行，由 run.sh source。
#
# 调用方（chaos-run.sh）给的环境变量：TARGET_NS、RUN_ID、RUN_DIR、KUBECONFIG。
#
# 为什么在中间件的 Pod 里跑，而不是在本机挂一个 kubectl exec：
#   注入成功之后 chaos-run.sh 就退出了，故障要自己撑到 duration；本机的进程会因为终端关掉、机器睡眠而没掉。
#   Pod 里的后台任务不依赖本机，到时自己结束（每个任务都带截止时刻 END），也不在集群里留对象。
#   代价：Pod 重启的话任务跟着没了（故障提前结束）；容器里没有 ps、pkill（MySQL 的镜像），所以不靠杀进程来撤销：
#     - 循环类的任务每一轮看一眼停止文件（STOP），撤销就是建这个文件；
#     - 占着 MySQL 会话的任务把自己的连接号写进文件，撤销时在库里 KILL 这些连接（事务随之回滚）。
#
# Pod 里的文件都叫 /tmp/.c-<运行号>.*，撤销时删掉。运行号本来就出现在 Chaos Mesh 的对象名里（c-<运行号>），不算新的痕迹。
# 在 macOS 自带的 bash 3.2 上运行。

CX_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/cluster/e2e-lib.sh
source "$CX_HERE/e2e-lib.sh"

cx_die() {
  echo "chaos-lib: $*" >&2
  exit 1
}

# 检查环境，定下 Pod 里文件的前缀、数据库名的前缀、截止时刻。参数：这个场景的时长（秒）。
cx_init() { # duration_seconds
  [[ -n "${TARGET_NS:-}" && -n "${RUN_ID:-}" && -n "${RUN_DIR:-}" ]] || cx_die "TARGET_NS, RUN_ID and RUN_DIR must be set (run via chaos-run.sh)"
  [[ "$RUN_ID" =~ ^[A-Za-z0-9-]+$ ]] || cx_die "unexpected RUN_ID '$RUN_ID'"
  CX_TAG="/tmp/.c-$RUN_ID"
  CX_DURATION="$1"
  # e2e 那一套和长期运行的那一套共用中间件，靠名字隔离（scripts/cluster/e2e-lib.sh 的头部）。
  case "$TARGET_NS" in
  oppenheimer) CX_DBP="" ;;
  oppenheimer-e2e) CX_DBP="e2e_" ;;
  *) cx_die "unexpected TARGET_NS '$TARGET_NS'" ;;
  esac
  export CX_TAG CX_DURATION CX_DBP
}

# 目标工作负载的一个环境变量的值（REDIS_PREFIX、KAFKA_PREFIX 这一类 e2e 里每次运行都不同的）。没设时输出空。
cx_app_env() { # workload VAR
  kubectl -n "$TARGET_NS" get deployment "$1" \
    -o jsonpath="{.spec.template.spec.containers[?(@.name==\"$1\")].env[?(@.name==\"$2\")].value}"
}

# 记下这次注入用的是哪个 Pod：撤销时用同一个（主从切换过的话，按 Service 再找一次会找到别的 Pod）。
cx_remember() { # key value
  echo "$1=$2" >>"$RUN_DIR/script-target"
}
cx_recall() { # key；没有记录时输出空
  [[ -f "$RUN_DIR/script-target" ]] || return 0
  sed -n "s/^$1=//p" "$RUN_DIR/script-target" | tail -n 1
}

# 在 Pod 里用 bash 执行标准输入里的脚本。额外的参数是 VAR=值，作为环境变量传进去。
cx_pod_sh() { # ns pod container [VAR=值...]
  local ns="$1" pod="$2" ctr="$3"
  shift 3
  kubectl -n "$ns" exec -i "$pod" -c "$ctr" -- env "$@" bash -s
}

# 在 Pod 里起一个后台任务：标准输入里的脚本存成 <前缀>.<名字>.sh，nohup 起，输出进 <前缀>.<名字>.log。
# 任务拿到的环境变量：TAG（文件前缀）、STOP（停止文件）、END（截止时刻，Unix 秒），加上调用方给的 VAR=值。
# 任务自己负责到 END 或者 STOP 出现时结束。
cx_bg() { # ns pod container name [VAR=值...]
  local ns="$1" pod="$2" ctr="$3" name="$4" end
  shift 4
  [[ "$name" =~ ^[a-z0-9]+$ ]] || cx_die "bad task name '$name'"
  end=$(($(date +%s) + CX_DURATION))
  # shellcheck disable=SC2016 # $1 在容器里展开
  kubectl -n "$ns" exec -i "$pod" -c "$ctr" -- bash -c 'cat >"$1"' _ "$CX_TAG.$name.sh" || return 1
  # shellcheck disable=SC2016
  kubectl -n "$ns" exec "$pod" -c "$ctr" -- env "TAG=$CX_TAG" "STOP=$CX_TAG.stop" "END=$end" "$@" \
    bash -c 'nohup bash "$1" >"$1.log" 2>&1 </dev/null & echo started' _ "$CX_TAG.$name.sh" >/dev/null || return 1
}

# 让 Pod 里这次运行的循环类任务停下（它们每一轮看这个文件）。
cx_stop() { # ns pod container
  # shellcheck disable=SC2016
  kubectl -n "$1" exec "$2" -c "$3" -- bash -c ': >"$1"' _ "$CX_TAG.stop"
}

# 删掉 Pod 里这次运行留下的文件。先等几秒让循环看到停止文件：太早删的话任务读不到 STOP。
cx_clean() { # ns pod container [wait_seconds]
  # shellcheck disable=SC2016
  kubectl -n "$1" exec "$2" -c "$3" -- bash -c 'sleep "$2"; rm -f "$1".*' _ "$CX_TAG" "${4:-3}"
}

# MySQL 的 Pod 里任务用的前缀：定义 M（root 连本机的实例，密码取自容器自己的环境变量，不进命令行）。
# shellcheck disable=SC2016,SC2034 # 在容器里展开；由 run.sh 使用
CX_MYSQL_PRE='M() { MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql --no-defaults -uroot -h127.0.0.1 -P3306 --connect-timeout=5 "$@"; }'

# KILL 掉这次运行记下的 MySQL 连接（<前缀>.*.cid，每个文件第一行是连接号）。连接已经不在了不算失败。
cx_mysql_kill() { # pod
  cx_pod_sh "$E2E_MYSQL_NS" "$1" mysql "TAG=$CX_TAG" <<EOF
$CX_MYSQL_PRE
for f in "\$TAG".*.cid; do
  [ -e "\$f" ] || continue
  id="\$(head -n 1 "\$f" | tr -cd '0-9')"
  [ -n "\$id" ] || continue
  M -e "KILL \$id" 2>/dev/null || true
done
EOF
}

# 后台任务起来几秒之后看一眼它的输出：有 ERROR（库名不对、连不上）就算注入失败，把输出打出来。
# 任务本身是脱离的，不看的话注入“成功”而什么都没有发生。
cx_bg_check() { # ns pod container name [wait_seconds]
  # shellcheck disable=SC2016
  kubectl -n "$1" exec "$2" -c "$3" -- bash -c 'sleep "$2"; if grep -qi "error" "$1"; then cat "$1" >&2; exit 1; fi' \
    _ "$CX_TAG.$4.sh.log" "${5:-3}"
}
